# 基础库
import asyncio
import html
import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

# 第三方库
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver
from pydantic import BaseModel, Field
from sqlalchemy import tuple_
from sqlalchemy.orm import Session

# 项目库
from app.chain.mediaserver import MediaServerChain
from app.chain.subscribe import SubscribeChain
from app.core.config import global_vars, settings
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.db.models.subscribehistory import SubscribeHistory
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.subscribe_oper import SubscribeOper
from app.db import db_query
from app.helper.cookiecloud import CookieCloudHelper
from app.helper.service import ServiceConfigHelper
from app.log import logger
from app.modules.themoviedb.tmdbapi import TmdbApi
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, NotificationType
from app.utils.http import RequestUtils


TRAKT_CALENDAR_PAGE_URL = "https://app.trakt.tv/calendar?mode=media"
TRAKT_CALENDAR_API_BASE = "https://apiz.trakt.tv"
TRAKT_WEB_API_KEY = "201dc70c5ec6af530f12f079ea1922733f6e1085ad7b02f36d8e011b75bcea7d"
TRAKT_CALENDAR_PAGE_TIMEOUT_SECONDS = 15
TRAKT_CALENDAR_PAGE_MAX_BYTES = 2 * 1024 * 1024
TRAKT_EVENT_DATA_PREFIX = "trakt_event."
TRAKT_PROCESSING_TTL_SECONDS = 15 * 60
TRAKT_OIDC_AUTH_BLOCK_PATTERN = re.compile(
    r"(?:[\"'])?oidcAuth(?:[\"'])?\s*:\s*\{(?P<block>[^{}]{1,4096})\}",
    re.IGNORECASE | re.DOTALL,
)
TRAKT_OIDC_TOKEN_PATTERN = re.compile(
    r"(?:[\"'])?token(?:[\"'])?\s*:\s*[\"'](?P<token>[^\"'\s]+)[\"']",
    re.IGNORECASE,
)
TRAKT_OIDC_EXPIRES_PATTERN = re.compile(
    r"(?:[\"'])?expiresAt(?:[\"'])?\s*:\s*(?:[\"'](?P<quoted>[^\"']+)[\"']|(?P<number>\d+(?:\.\d+)?))",
    re.IGNORECASE,
)


class TraktRequestError(RuntimeError):
    """Trakt 页面请求失败，但不携带 Cookie 或响应正文。"""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class TraktCalendarEvent:
    """已从 Trakt 个人剧集日历中验证过 TMDB ID 的单个剧集事件。"""

    tmdb_id: int
    season_number: int
    episode_number: int
    air_date: str
    title: str
    status: str

    @property
    def event_key(self) -> str:
        return (
            f"{TRAKT_EVENT_DATA_PREFIX}{MediaType.TV.value}.{self.tmdb_id}."
            f"S{self.season_number:02d}.E{self.episode_number:02d}.{self.air_date}"
        )

    def to_update(self) -> dict:
        """转换为现有电视续作处理链路使用的更新结构。"""
        return {
            "season_number": self.season_number,
            "episode_number": self.episode_number,
            "name": self.title,
            "air_date": self.air_date,
            "_followup_status": self.status,
            "_followup_source": "trakt",
            "_followup_event_key": self.event_key,
            "_followup_tmdb_id": self.tmdb_id,
        }


class SubscriptionManagerConfig(BaseModel):
    """订阅管理插件配置"""
    # 插件开关
    enabled: bool = False
    # 命中日历更新后自动添加订阅
    auto_subscribe: bool = True
    # 提前提醒天数
    after_days: int = Field(default=2, ge=1, le=30)
    # 系列检查年限
    threshold_years: int = Field(default=15, ge=2, le=50)
    # cron 表达式
    cron: str = ""
    # 单次运行开关
    onlyonce: bool = False
    # 检查订阅历史
    check_sub_history: bool = True
    # 媒体库
    libraries: List[str] = Field(default_factory=list)
    # Trakt 个人剧集日历（默认关闭，避免无登录状态的外部请求）
    trakt_calendar_enabled: bool = False
    # 单次拉取范围；实际提醒仍受 after_days 约束
    trakt_calendar_days: int = Field(default=7, ge=1, le=30)
    # 转移记录清理配置（从原 TransferCleaner 迁移）
    notify: bool = True
    dry_run: bool = True
    delay_enabled: bool = False
    delay_seconds: int = Field(default=10, ge=1, le=3600)
    monitor_dirs: str = ""
    path_mappings: str = ""
    exclude_dirs: str = ""
    exclude_keywords: str = ""
    clean_dirs: str = ""
    run_once: bool = False
    retransfer_once: bool = False
    retransfer_dirs: str = ""
    retransfer_cron: str = ""
    clean_failed: bool = False


class FileMonitorHandler(FileSystemEventHandler):
    """转移记录清理的目录事件处理器。"""

    def __init__(self, monpath: str, plugin: Any, **kwargs):
        super().__init__(**kwargs)
        self._watch_path = monpath
        self.plugin = plugin

    def on_deleted(self, event):
        if event.is_directory:
            return
        self.plugin.handle_file_event("deleted", event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        self.plugin.handle_file_event("moved", event.src_path, event.dest_path)


class TransferCleanupMixin:
    """复用转移记录清理能力，生命周期由订阅管理插件统一调度。"""

    _enabled: bool = False
    _notify: bool = True
    _dry_run: bool = True
    _delay_enabled: bool = False
    _delay_seconds: int = 10
    _monitor_dirs: str = ""
    _path_mappings: str = ""
    _exclude_dirs: str = ""
    _exclude_keywords: str = ""
    _clean_dirs: str = ""
    _run_once: bool = False
    _retransfer_once: bool = False
    _retransfer_dirs: str = ""
    _retransfer_cron: str = ""
    _clean_failed: bool = False
    _observers: List[PollingObserver] = None
    _transferhistory: Optional[TransferHistoryOper] = None
    _event_cache: Dict[str, float] = None
    _event_cache_lock: threading.Lock = None
    _exclude_keywords_list: List[str] = None
    _exclude_dirs_list: List[str] = None
    _path_mappings_dict: Dict[str, str] = None
    _reverse_mappings_dict: Dict[str, str] = None
    _dedupe_ttl: int = 3
    _temp_suffixes: List[str] = [".!qb", ".part", ".mp", ".tmp"]
    _delay_queue: Queue = None
    _delay_thread: threading.Thread = None
    _stop_event: threading.Event = None
    _notify_buffer: List[str] = None
    _notify_buffer_lock: threading.Lock = None
    _notify_timer: threading.Timer = None
    _notify_delay: int = 30

    def __update_transfer_config(self):
        """将清理线程的瞬时状态写回统一配置，避免覆盖续作设置。"""
        if hasattr(self, "_sync_transfer_config"):
            self._sync_transfer_config()
        self.update_config(self._get_config().model_dump())

    def __update_config(self):
        self.__update_transfer_config()

    def _init_transfer_cleanup(self, config: dict = None):
        """初始化插件"""
        # 初始化实例属性（避免类级可变状态共享）
        self._observers = []
        self._event_cache = {}
        self._event_cache_lock = threading.Lock()
        self._exclude_keywords_list = []
        self._exclude_dirs_list = []
        self._path_mappings_dict = {}
        self._reverse_mappings_dict = {}
        self._delay_queue = Queue()
        self._stop_event = threading.Event()
        # 通知聚合初始化
        self._notify_buffer = []
        self._notify_buffer_lock = threading.Lock()
        self._notify_timer = None

        self._transferhistory = TransferHistoryOper()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", True)
            self._dry_run = config.get("dry_run", True)
            self._delay_enabled = config.get("delay_enabled", False)
            self._delay_seconds = int(config.get("delay_seconds", 10) or 10)
            self._monitor_dirs = config.get("monitor_dirs", "")
            self._path_mappings = config.get("path_mappings", "")
            self._exclude_dirs = config.get("exclude_dirs", "")
            self._exclude_keywords = config.get("exclude_keywords", "")
            self._clean_dirs = config.get("clean_dirs", "")
            self._run_once = config.get("run_once", False)
            self._retransfer_once = config.get("retransfer_once", False)
            self._retransfer_dirs = config.get("retransfer_dirs", "")
            self._retransfer_cron = config.get("retransfer_cron", "")
            self._clean_failed = config.get("clean_failed", False)
            # 预编译排除关键词列表
            self._exclude_keywords_list = [
                k.strip() for k in self._exclude_keywords.split("\n") if k.strip()
            ]
            # 预编译不删除目录列表
            self._exclude_dirs_list = [
                d.strip() for d in self._exclude_dirs.split("\n") if d.strip()
            ]
            # 预编译路径映射
            self._path_mappings_dict = self._parse_path_mappings()
            # 预编译反向路径映射（用于清理任务）
            self._reverse_mappings_dict = {v: k for k, v in self._path_mappings_dict.items()}

        logger.info(
            f"订阅管理转移清理初始化，"
            f"enabled={self._enabled}, dry_run={self._dry_run}, "
            f"delay_enabled={self._delay_enabled}, delay_seconds={self._delay_seconds}, "
            f"path_mappings={len(self._path_mappings_dict)}个"
        )

        # 停止现有监控
        self._stop_transfer_cleanup()

        if self._enabled:
            self._start_monitoring()
            # 启动延迟删除线程
            if self._delay_enabled:
                self._start_delay_worker()

        # 检查是否需要立即运行清理任务
        if self._run_once:
            # 启动清理任务（在任务完成后重置开关）
            threading.Thread(
                target=self._run_cleanup_task_wrapper,
                daemon=True,
                name="SubscriptionManager-Cleanup"
            ).start()

        # 检查是否需要立即运行重新整理任务
        if self._retransfer_once:
            # 启动重新整理任务（在任务完成后重置开关）
            threading.Thread(
                target=self._run_retransfer_task_wrapper,
                daemon=True,
                name="SubscriptionManager-Retransfer"
            ).start()

    def _run_cleanup_task_wrapper(self):
        """清理任务包装器，完成后重置开关"""
        try:
            self._run_cleanup_task()
        finally:
            # 重置开关
            self._run_once = False
            self.__update_config()

    def _run_retransfer_task_wrapper(self):
        """重新整理任务包装器（立即执行，不重置开关）"""
        self._run_retransfer_task()

    def _parse_path_mappings(self) -> Dict[str, str]:
        """
        解析路径映射配置
        格式: 本地目录:存储路径
        例如: /media/115/转存:/115/转存
        返回: {本地路径前缀: 存储路径前缀}
        """
        mappings = {}
        if not self._path_mappings:
            return mappings

        for line in self._path_mappings.split("\n"):
            line = line.strip()
            if not line or ":" not in line:
                continue
            try:
                # 格式: 本地目录:存储类型:存储路径 或 本地目录:存储路径
                parts = line.split(":", 2)
                if len(parts) == 2:
                    # 本地目录:存储路径（存储路径可能包含存储类型前缀如 u115:）
                    local_path = parts[0].strip()
                    storage_path = parts[1].strip()
                    mappings[local_path] = storage_path
                elif len(parts) == 3:
                    # 本地目录:存储类型:存储路径
                    local_path = parts[0].strip()
                    storage_type = parts[1].strip()
                    storage_path = parts[2].strip()
                    # 组合成完整的存储路径
                    mappings[local_path] = f"{storage_type}:{storage_path}"
                else:
                    logger.warning(f"SubscriptionManager: 无效的路径映射配置: {line}")
                    continue

                logger.info(f"SubscriptionManager: 路径映射 {local_path} -> {mappings[local_path]}")

            except Exception as e:
                logger.warning(f"SubscriptionManager: 解析路径映射失败 {line}: {e}")

        return mappings

    def _convert_storage_to_local(self, storage_path: str) -> str:
        """
        将存储路径转换为本地路径（用于检查文件是否存在）

        :param storage_path: 数据库中的存储路径
        :return: 转换后的本地路径，如果没有匹配的映射则返回原路径
        """
        for storage_prefix, local_prefix in self._reverse_mappings_dict.items():
            if storage_path.startswith(storage_prefix):
                # 计算相对路径
                relative_path = storage_path[len(storage_prefix):].lstrip("/")
                # 构建本地路径
                local_path = local_prefix.rstrip("/") + "/" + relative_path
                return local_path

        # 没有匹配的映射，返回原路径
        return storage_path

    def _check_file_exists(self, src_path: str) -> bool:
        """
        检查源文件是否存在（支持 CD2 路径和本地路径）

        :param src_path: 数据库中的 src 路径
        :return: 文件是否存在
        """
        # CD2 路径（如 /115open/115/转存/xxx.mkv）走 CD2 API 检查
        if src_path.startswith("/115open/"):
            return self._check_cd2_file_exists(src_path)
        # 其他路径走本地 os.path.exists
        return os.path.exists(src_path)

    def _check_cd2_file_exists(self, storage_path: str) -> bool:
        """
        通过 CD2 储存接口检查文件是否存在
        如果找不到 CD2 储存实例或调用失败，认为文件不存在（已上传成功）

        :param storage_path: CD2 路径，如 /115open/115/转存/xxx.mkv
        :return: 文件是否存在
        """
        try:
            from app.modules.filemanager.storages import storages
            for s in storages:
                if s.__class__.__name__ == 'CloudDriveDisk':
                    try:
                        item = s.get_file_item(
                            storage='CloudDrive储存',
                            path=Path(storage_path)
                        )
                        return item is not None
                    except Exception:
                        pass
            # 找不到 CD2 storage 实例，认为文件不存在（假失败场景）
            return False
        except Exception:
            return False

    def _run_cleanup_task(self):
        """
        运行清理任务：扫描数据库中的转移记录，检查源文件是否存在，
        如果不存在则删除对应的记录
        """
        logger.info("SubscriptionManager: 开始运行清理任务...")

        # 直接使用监控目录
        clean_dirs = [d.strip() for d in self._monitor_dirs.split("\n") if d.strip()]

        if not clean_dirs:
            logger.warning("SubscriptionManager: 未配置监控目录，跳过清理任务")
            self.systemmessage.put("未配置监控目录，请先配置监控目录", title="转移记录清理")
            return

        # 将本地目录转换为存储路径前缀（用于数据库查询）
        storage_prefixes = []
        for local_dir in clean_dirs:
            storage_path = self._convert_path_to_storage(local_dir)
            storage_prefixes.append(storage_path)
            logger.info(f"SubscriptionManager: 清理目录映射 {local_dir} -> {storage_path}")

        # 统计
        total_checked = 0
        total_deleted = 0
        deleted_records = []

        try:
            from sqlalchemy import desc
            from app.db.models.transferhistory import TransferHistory
            from app.db import SessionFactory

            with SessionFactory() as db:
                # 遍历每个存储路径前缀
                for storage_prefix in storage_prefixes:
                    logger.info(f"SubscriptionManager: 扫描存储路径前缀 {storage_prefix}")

                    # 查询匹配的记录
                    records = db.query(TransferHistory).filter(
                        TransferHistory.src.like(f"{storage_prefix}%")
                    ).order_by(desc(TransferHistory.id)).all()

                    logger.info(f"SubscriptionManager: 找到 {len(records)} 条匹配记录")

                    for record in records:
                        total_checked += 1

                        # 检查文件是否存在（支持 CD2 路径）
                        file_exists = self._check_file_exists(record.src)
                        if file_exists:
                            continue

                        # 文件不存在，需要删除记录
                        if self._dry_run:
                            logger.info(
                                f"[DryRun] SubscriptionManager: 将删除记录 "
                                f"ID={record.id}, src={record.src}"
                            )
                            total_deleted += 1
                            deleted_records.append({
                                "id": record.id,
                                "src": record.src,
                                "title": getattr(record, 'title', '')
                            })
                        else:
                            self._transferhistory.delete(record.id)
                            logger.info(
                                f"SubscriptionManager: 已删除记录 "
                                f"ID={record.id}, src={record.src}"
                            )
                            total_deleted += 1
                            deleted_records.append({
                                "id": record.id,
                                "src": record.src,
                                "title": getattr(record, 'title', '')
                            })

                        # 防止删除过多
                        if total_deleted >= 1000:
                            logger.warning("SubscriptionManager: 达到单次清理上限 1000 条")
                            break

                    if total_deleted >= 1000:
                        break

        except Exception as e:
            logger.exception("SubscriptionManager: 清理任务异常")
            self.systemmessage.put(f"清理任务异常: {str(e)}", title="转移记录清理")
            return

        # 发送通知
        dry_run_tag = "[模拟] " if self._dry_run else ""
        summary = f"{dry_run_tag}清理任务完成\n"
        summary += f"扫描记录: {total_checked} 条\n"
        summary += f"{'将删除' if self._dry_run else '已删除'}: {total_deleted} 条\n"

        if deleted_records and len(deleted_records) <= 10:
            summary += "\n详情:\n"
            for r in deleted_records[:10]:
                title = r.get('title', '')
                if title:
                    summary += f"- {title}\n"
                else:
                    summary += f"- ID:{r['id']}\n"

        logger.info(f"SubscriptionManager: {summary}")

        if self._notify:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【转移记录清理】{dry_run_tag}",
                text=summary
            )

        # 如果开启了清理失败记录，继续执行
        if self._clean_failed:
            self._run_clean_failed_task()

    def _run_clean_failed_task(self):
        """
        清理/重试失败记录：
        - 源文件不存在（说明已上传成功）：删除失败记录
        - 源文件仍存在（说明确实失败了）：删除记录并重新整理
        """
        logger.info("SubscriptionManager: 开始处理失败记录...")

        total_checked = 0
        deleted_count = 0
        retry_count = 0

        try:
            from sqlalchemy import desc
            from app.db.models.transferhistory import TransferHistory
            from app.db import SessionFactory
            from app.chain.transfer import TransferChain

            transfer_chain = None
            if not self._dry_run:
                try:
                    transfer_chain = TransferChain()
                except ImportError:
                    logger.error("SubscriptionManager: 无法导入 TransferChain")

            with SessionFactory() as db:
                # 查询所有失败的记录
                records = db.query(TransferHistory).filter(
                    TransferHistory.status == False
                ).order_by(desc(TransferHistory.id)).limit(500).all()

                logger.info(f"SubscriptionManager: 找到 {len(records)} 条失败记录")

                for record in records:
                    total_checked += 1

                    # 将存储路径转换为本地路径，检查文件是否存在（支持 CD2 路径）
                    local_path = self._convert_storage_to_local(record.src)
                    file_exists = self._check_file_exists(record.src)

                    if file_exists:
                        # 源文件存在，说明确实失败了，需要重试
                        if self._dry_run:
                            logger.info(
                                f"[DryRun] SubscriptionManager: 将重试整理 "
                                f"ID={record.id}, src={record.src}"
                            )
                            retry_count += 1
                        else:
                            # 删除旧记录并重新整理
                            self._transferhistory.delete(record.id)
                            logger.info(f"SubscriptionManager: 已删除失败记录 ID={record.id}")

                            if transfer_chain:
                                try:
                                    transfer_chain.process(Path(local_path))
                                    retry_count += 1
                                    logger.info(f"SubscriptionManager: 已触发重新整理 {local_path}")
                                except Exception as e:
                                    logger.exception(f"SubscriptionManager: 重新整理失败 {local_path}")
                    else:
                        # 源文件不存在，说明实际已上传成功，删除错误记录
                        if self._dry_run:
                            logger.info(
                                f"[DryRun] SubscriptionManager: 将删除假失败记录 "
                                f"ID={record.id}, src={record.src}"
                            )
                            deleted_count += 1
                        else:
                            self._transferhistory.delete(record.id)
                            logger.info(
                                f"SubscriptionManager: 已删除假失败记录 "
                                f"ID={record.id}, src={record.src}"
                            )
                            deleted_count += 1

                    if deleted_count + retry_count >= 100:
                        logger.warning("SubscriptionManager: 达到单次处理上限 100 条")
                        break

        except Exception as e:
            logger.exception("SubscriptionManager: 处理失败记录异常")
            return

        if deleted_count > 0 or retry_count > 0:
            dry_run_tag = "[模拟] " if self._dry_run else ""
            summary = f"{dry_run_tag}处理失败记录完成\n"
            summary += f"检查失败记录: {total_checked} 条\n"
            if deleted_count > 0:
                summary += f"{'将删除' if self._dry_run else '已删除'}假失败记录: {deleted_count} 条\n"
            if retry_count > 0:
                summary += f"{'将重试' if self._dry_run else '已重试'}整理: {retry_count} 条\n"

            logger.info(f"SubscriptionManager: {summary}")

            if self._notify:
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=f"【转移记录清理】{dry_run_tag}失败记录处理",
                    text=summary
                )

    def _run_retransfer_task(self):
        """
        运行重新整理任务：扫描已有转移记录但源文件仍存在的情况，
        说明文件没有成功上传，需要重新整理
        """
        logger.info("SubscriptionManager: 开始运行重新整理检测任务...")

        # 使用用户配置的重新整理检测目录
        retransfer_dirs = [d.strip() for d in self._retransfer_dirs.split("\n") if d.strip()]
        if not retransfer_dirs:
            logger.warning("SubscriptionManager: 未配置重新整理检测目录，跳过")
            return

        # 统计
        total_checked = 0
        need_retransfer = []

        try:
            from sqlalchemy import desc
            from app.db.models.transferhistory import TransferHistory
            from app.db import SessionFactory

            with SessionFactory() as db:
                for check_dir in retransfer_dirs:
                    logger.info(f"SubscriptionManager: 检测目录 {check_dir}")

                    # 查询源路径在该目录下的记录
                    records = db.query(TransferHistory).filter(
                        TransferHistory.src.like(f"{check_dir}%")
                    ).order_by(desc(TransferHistory.id)).limit(2000).all()

                    logger.info(f"SubscriptionManager: 找到 {len(records)} 条匹配记录")

                    for record in records:
                        total_checked += 1
                        src_path = record.src

                        # 检查源文件是否仍然存在（支持 CD2 路径）
                        src_path = record.src
                        file_exists = self._check_file_exists(src_path)
                        if file_exists:
                            # 源文件仍存在，说明可能没有成功上传
                            need_retransfer.append({
                                "id": record.id,
                                "src": src_path,
                                "dest": record.dest,
                                "title": getattr(record, 'title', ''),
                            })
                            logger.info(
                                f"SubscriptionManager: 发现未上传文件 "
                                f"ID={record.id}, src={src_path}"
                            )

                        if len(need_retransfer) >= 500:
                            logger.warning("SubscriptionManager: 达到单次检测上限 500 条")
                            break

                    if len(need_retransfer) >= 500:
                        break

        except Exception as e:
            logger.exception("SubscriptionManager: 重新整理检测任务异常")
            self.systemmessage.put(f"重新整理检测异常: {str(e)}", title="转移记录清理")
            return

        # 处理需要重新整理的文件
        retransfer_count = 0
        if need_retransfer and not self._dry_run:
            try:
                from app.chain.transfer import TransferChain
                transfer_chain = TransferChain()

                for item in need_retransfer:
                    src_path = item["src"]
                    try:
                        # 先删除旧的转移记录
                        self._transferhistory.delete(item["id"])
                        logger.info(f"SubscriptionManager: 已删除旧记录 ID={item['id']}")

                        # 触发重新整理
                        transfer_chain.process(Path(src_path))
                        retransfer_count += 1
                        logger.info(f"SubscriptionManager: 已触发重新整理 {src_path}")

                    except Exception as e:
                        logger.exception(f"SubscriptionManager: 重新整理失败 {src_path}")

            except ImportError:
                logger.error("SubscriptionManager: 无法导入 TransferChain，跳过重新整理")

        # 发送通知
        dry_run_tag = "[模拟] " if self._dry_run else ""
        summary = f"{dry_run_tag}重新整理检测完成\n"
        summary += f"扫描记录: {total_checked} 条\n"
        summary += f"发现未上传: {len(need_retransfer)} 条\n"
        if not self._dry_run:
            summary += f"已重新整理: {retransfer_count} 条\n"

        if need_retransfer and len(need_retransfer) <= 10:
            summary += "\n详情:\n"
            for r in need_retransfer[:10]:
                title = r.get('title', '')
                if title:
                    summary += f"- {title}\n"
                else:
                    src = r.get('src', '')
                    summary += f"- {Path(src).name}\n"

        logger.info(f"SubscriptionManager: {summary}")

        if self._notify:
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title=f"【转移记录清理】{dry_run_tag}重新整理",
                text=summary
            )

    def _convert_path_to_storage(self, local_path: str) -> str:
        """
        将本地路径转换为存储路径（用于匹配 TransferHistory.src）

        :param local_path: 本地文件路径
        :return: 转换后的存储路径，如果没有匹配的映射则返回原路径
        """
        for local_prefix, storage_prefix in self._path_mappings_dict.items():
            if local_path.startswith(local_prefix):
                # 计算相对路径
                relative_path = local_path[len(local_prefix):].lstrip("/")
                # 构建存储路径
                storage_path = storage_prefix.rstrip("/") + "/" + relative_path
                logger.debug(f"SubscriptionManager: 路径转换 {local_path} -> {storage_path}")
                return storage_path

        # 没有匹配的映射，返回原路径
        return local_path

    def _start_monitoring(self):
        """启动目录监控"""
        monitor_dirs = [d.strip() for d in self._monitor_dirs.split("\n") if d.strip()]

        if not monitor_dirs:
            logger.warning("SubscriptionManager: 未配置监控目录")
            return

        logger.info(f"SubscriptionManager: 监控目录列表 {monitor_dirs}")

        for mon_path in monitor_dirs:
            if not os.path.isdir(mon_path):
                logger.warning(f"SubscriptionManager: 监控目录不存在 {mon_path}")
                continue

            try:
                # 使用兼容模式（轮询），适用于网络挂载目录
                observer = PollingObserver(timeout=10)

                self._observers.append(observer)
                observer.schedule(
                    FileMonitorHandler(mon_path, self),
                    mon_path,
                    recursive=True
                )
                observer.daemon = True
                observer.start()

                logger.info(f"SubscriptionManager: {mon_path} 目录监控启动 [兼容模式], observer.is_alive={observer.is_alive()}")

            except Exception as e:
                logger.exception(f"SubscriptionManager: 启动目录监控失败 {mon_path}")
                self.systemmessage.put(
                    f"启动目录监控失败：{mon_path}\n{str(e)}",
                    title="转移记录清理"
                )

    def _start_delay_worker(self):
        """启动延迟删除工作线程"""
        self._delay_thread = threading.Thread(
            target=self._delay_worker_loop,
            daemon=True,
            name="SubscriptionManager-DelayWorker"
        )
        self._delay_thread.start()
        logger.info(f"SubscriptionManager: 延迟删除线程启动，延迟 {self._delay_seconds} 秒")

    def _delay_worker_loop(self):
        """延迟删除工作线程主循环"""
        while not self._stop_event.is_set():
            try:
                # 从队列获取事件，超时1秒
                event_data = self._delay_queue.get(timeout=1)
            except Empty:
                continue

            event_type = event_data["event_type"]
            src_path = event_data["src_path"]
            dest_path = event_data.get("dest_path")
            event_time = event_data["event_time"]

            # 计算需要等待的时间
            elapsed = time.time() - event_time
            wait_time = self._delay_seconds - elapsed

            if wait_time > 0:
                # 等待剩余时间，但要检查停止信号
                if self._stop_event.wait(wait_time):
                    break

            # 检查文件是否仍然不存在（确认删除）
            if os.path.exists(src_path):
                logger.info(
                    f"SubscriptionManager: 延迟检查发现文件已恢复，跳过 {src_path}"
                )
                continue

            # 执行删除历史记录
            deleted = self._process_delete(event_type, src_path, dest_path)
            # 如果未删除任何记录，从去重缓存移除，允许后续事件重试
            if not deleted:
                self._remove_from_event_cache(src_path)

    def handle_file_event(self, event_type: str, src_path: str, dest_path: str = None):
        """
        处理文件事件

        :param event_type: 事件类型 (deleted/moved)
        :param src_path: 源路径（用于匹配历史记录）
        :param dest_path: 目标路径（仅移动事件有）
        """
        try:
            file_path = Path(src_path)

            # 过滤临时文件
            if file_path.suffix.lower() in self._temp_suffixes:
                return

            # 检查是否在不删除目录中
            if self._is_in_exclude_dirs(src_path):
                logger.debug(f"SubscriptionManager: 路径在不删除目录中，跳过 {src_path}")
                return

            # 过滤排除关键词
            if self._should_exclude(src_path):
                logger.debug(f"SubscriptionManager: 路径命中排除关键词，跳过 {src_path}")
                return

            # 事件去重
            if self._is_duplicate_event(src_path):
                logger.info(f"SubscriptionManager: 重复事件，跳过 {src_path}")
                return

            logger.info(f"SubscriptionManager: 检测到文件{event_type}事件 - {src_path}")

            if self._delay_enabled:
                # 加入延迟队列
                self._delay_queue.put({
                    "event_type": event_type,
                    "src_path": src_path,
                    "dest_path": dest_path,
                    "event_time": time.time()
                })
                logger.debug(f"SubscriptionManager: 事件加入延迟队列，{self._delay_seconds}秒后处理")
            else:
                # 立即处理
                deleted = self._process_delete(event_type, src_path, dest_path)
                # 如果未删除任何记录，从去重缓存移除，允许后续事件重试
                if not deleted:
                    self._remove_from_event_cache(src_path)

        except Exception as e:
            logger.exception(f"SubscriptionManager: 处理事件异常 {src_path}")

    def _process_delete(self, event_type: str, src_path: str, dest_path: str = None) -> bool:
        """
        实际执行删除历史记录

        :return: 是否成功删除了记录
        """
        # 规范化路径
        normalized_path = self._normalize_path(src_path)

        # 应用路径映射转换
        storage_path = self._convert_path_to_storage(normalized_path)

        # 先尝试精确匹配（快速路径）
        result = self._delete_history_by_src(storage_path, event_type)

        # 精确匹配失败，尝试原路径
        if result["deleted_count"] == 0 and storage_path != normalized_path:
            logger.debug(f"SubscriptionManager: 存储路径未匹配，尝试原路径 {normalized_path}")
            result = self._delete_history_by_src(normalized_path, event_type)

        # 精确匹配全部失败，使用 LIKE 模糊匹配（处理路径格式差异）
        if result["deleted_count"] == 0:
            logger.info(f"SubscriptionManager: 精确匹配未找到记录，尝试模糊匹配 {storage_path}")
            result = self._delete_history_by_like(storage_path, normalized_path, event_type)

        if result["deleted_count"] == 0:
            logger.warning(f"SubscriptionManager: 未找到匹配的转移记录 {src_path}")

        # 发送通知
        if result["deleted_count"] > 0 and self._notify:
            self._send_notification(event_type, storage_path, dest_path, result)

        # 异常导致的失败也视为未成功，允许重试
        return result["deleted_count"] > 0 and not result.get("error")

    def _normalize_path(self, path: str) -> str:
        """路径规范化"""
        # 转换为绝对路径
        normalized = os.path.abspath(path)
        # 统一分隔符
        normalized = normalized.replace("\\", "/")
        # 去除尾部斜杠
        normalized = normalized.rstrip("/")
        return normalized

    def _is_in_exclude_dirs(self, path: str) -> bool:
        """检查路径是否在不删除目录中"""
        if not self._exclude_dirs_list:
            return False

        for exclude_dir in self._exclude_dirs_list:
            if path.startswith(exclude_dir):
                return True
        return False

    def _should_exclude(self, path: str) -> bool:
        """检查路径是否应该排除（使用预编译的关键词列表）"""
        if not self._exclude_keywords_list:
            return False

        for keyword in self._exclude_keywords_list:
            if keyword in path:
                return True
        return False

    def _is_duplicate_event(self, path: str) -> bool:
        """检查是否为重复事件"""
        current_time = time.time()

        with self._event_cache_lock:
            # 清理过期缓存
            expired_keys = [
                k for k, v in self._event_cache.items()
                if current_time - v > self._dedupe_ttl
            ]
            for k in expired_keys:
                del self._event_cache[k]

            # 检查是否重复
            if path in self._event_cache:
                return True

            # 记录事件
            self._event_cache[path] = current_time
            return False

    def _remove_from_event_cache(self, path: str):
        """从去重缓存中移除路径，允许后续事件重试"""
        with self._event_cache_lock:
            self._event_cache.pop(path, None)

    def _delete_history_by_src(self, src_path: str, reason: str) -> dict:
        """
        根据源路径删除转移历史记录

        :return: {"deleted_count": int, "deleted_ids": list, "dry_run": bool}
        """
        result = {
            "deleted_count": 0,
            "deleted_ids": [],
            "dry_run": self._dry_run
        }

        # 删除上限保护，防止异常数据导致长循环
        max_delete_count = 100

        try:
            # 循环删除直到没有匹配记录（处理重复记录）
            while result["deleted_count"] < max_delete_count:
                history = self._transferhistory.get_by_src(src_path)
                if not history:
                    break

                result["deleted_ids"].append(history.id)
                result["deleted_count"] += 1

                if self._dry_run:
                    logger.info(
                        f"[DryRun] SubscriptionManager: 将删除历史记录 "
                        f"ID={history.id}, src={src_path}"
                    )
                    break  # Dry Run 模式只检查一次
                else:
                    self._transferhistory.delete(history.id)
                    logger.info(
                        f"SubscriptionManager: 已删除历史记录 "
                        f"ID={history.id}, src={src_path}, reason={reason}"
                    )

            if result["deleted_count"] >= max_delete_count:
                logger.warning(
                    f"SubscriptionManager: 达到删除上限 {max_delete_count}，"
                    f"src={src_path} 可能存在异常数据"
                )

        except Exception as e:
            logger.exception(f"SubscriptionManager: 删除历史记录异常 {src_path}")

        return result

    def _delete_history_by_like(self, storage_path: str, local_path: str, reason: str) -> dict:
        """
        使用 LIKE 受限模糊匹配删除转移历史记录（处理路径格式差异）
        匹配策略：目录前缀 + 文件名，避免跨目录误删

        :param storage_path: 转换后的存储路径
        :param local_path: 原始本地路径（用于构建目录约束）
        :param reason: 删除原因
        :return: {"deleted_count": int, "deleted_ids": list, "dry_run": bool}
        """
        result = {
            "deleted_count": 0,
            "deleted_ids": [],
            "dry_run": self._dry_run
        }

        try:
            from app.db.models.transferhistory import TransferHistory
            from app.db import SessionFactory

            # 提取文件名用于匹配
            filename = Path(storage_path).name
            if not filename:
                return result

            # 转义 LIKE 通配符（防止文件名中的 % _ 被当作模式字符）
            escaped_filename = filename.replace("%", "\\%").replace("_", "\\_")

            # 构建目录前缀候选列表（约束匹配范围，防止跨目录误删）
            dir_prefixes = set()
            for candidate in [storage_path, local_path]:
                parent = str(Path(candidate).parent)
                if parent and parent != ".":
                    # 取上两级目录作为前缀（兼容子目录结构差异）
                    grandparent = str(Path(parent).parent)
                    if grandparent and grandparent != ".":
                        escaped_gp = grandparent.replace("%", "\\%").replace("_", "\\_")
                        dir_prefixes.add(escaped_gp)
                    escaped_parent = parent.replace("%", "\\%").replace("_", "\\_")
                    dir_prefixes.add(escaped_parent)

            with SessionFactory() as db:
                from sqlalchemy import or_

                if dir_prefixes:
                    # 受限匹配：目录前缀 + 文件名
                    conditions = [
                        TransferHistory.src.like(f"{prefix}%{escaped_filename}", escape="\\")
                        for prefix in dir_prefixes
                    ]
                    records = db.query(TransferHistory).filter(
                        or_(*conditions)
                    ).limit(10).all()
                else:
                    # 无目录信息时的保守匹配：完整路径尾部匹配 + 严格 limit
                    records = db.query(TransferHistory).filter(
                        TransferHistory.src.like(f"%/{escaped_filename}", escape="\\")
                    ).limit(5).all()

                if not records:
                    return result

                logger.info(
                    f"SubscriptionManager: 模糊匹配找到 {len(records)} 条记录 "
                    f"(文件名={filename}, 目录约束={len(dir_prefixes)}个)"
                )

                for record in records:
                    result["deleted_ids"].append(record.id)
                    result["deleted_count"] += 1

                    if self._dry_run:
                        logger.info(
                            f"[DryRun] SubscriptionManager: 将删除历史记录 "
                            f"ID={record.id}, src={record.src}"
                        )
                    else:
                        self._transferhistory.delete(record.id)
                        logger.info(
                            f"SubscriptionManager: 已删除历史记录 "
                            f"ID={record.id}, src={record.src}, reason={reason}"
                        )

        except Exception as e:
            logger.exception(f"SubscriptionManager: 模糊匹配删除异常 {storage_path}")
            # 标记为失败，让调用方知道不是"未找到"而是"执行出错"
            result["error"] = True

        return result

    def _send_notification(self, event_type: str, src_path: str,
                          dest_path: str, result: dict):
        """发送通知"""
        dry_run_tag = "[模拟] " if result["dry_run"] else ""

        # 提取文件名并加入通知缓冲区
        file_name = Path(src_path).name
        self._add_to_notify_buffer(file_name, result["dry_run"])

    def _add_to_notify_buffer(self, file_name: str, dry_run: bool):
        """将文件名加入通知缓冲区，延迟聚合发送"""
        with self._notify_buffer_lock:
            self._notify_buffer.append(file_name)

            # 取消现有定时器
            if self._notify_timer:
                self._notify_timer.cancel()

            # 设置新的定时器
            self._notify_timer = threading.Timer(
                self._notify_delay,
                self._flush_notify_buffer,
                args=[dry_run]
            )
            self._notify_timer.daemon = True
            self._notify_timer.start()

    def _flush_notify_buffer(self, dry_run: bool = False):
        """发送聚合通知"""
        with self._notify_buffer_lock:
            if not self._notify_buffer:
                return

            files = self._notify_buffer.copy()
            self._notify_buffer.clear()

        dry_run_tag = "[模拟] " if dry_run else ""
        count = len(files)

        title = f"【转移记录清理】{dry_run_tag}已删除 {count} 条记录"

        # 按剧集/系列名分组
        groups = self._group_files_by_series(files)

        text_parts = []
        for series_name, episode_files in groups.items():
            if len(episode_files) == 1:
                text_parts.append(f"· {episode_files[0]}")
            else:
                # 提取集数信息，紧凑显示
                episodes = self._extract_episode_numbers(episode_files)
                if episodes:
                    text_parts.append(f"· {series_name} ({len(episode_files)}集: {episodes})")
                else:
                    text_parts.append(f"· {series_name} ({len(episode_files)}个文件)")

        # 限制通知长度
        if len(text_parts) > 10:
            text = "\n".join(text_parts[:10]) + f"\n... 等共 {len(text_parts)} 个系列"
        else:
            text = "\n".join(text_parts)

        self.post_message(
            mtype=NotificationType.SiteMessage,
            title=title,
            text=text
        )

    def _group_files_by_series(files: List[str]) -> Dict[str, List[str]]:
        """按剧集/系列名分组文件"""
        import re
        groups: Dict[str, List[str]] = {}

        for f in files:
            # 尝试提取系列名（匹配到 S01E01 / EP01 / E01 之前的部分）
            match = re.match(r'^(.*?)[.\s]S\d+E\d+', f, re.IGNORECASE)
            if not match:
                match = re.match(r'^(.*?)[.\s](?:EP?\d+)', f, re.IGNORECASE)

            if match:
                series = match.group(1).strip().rstrip('.')
            else:
                series = f  # 无法提取系列名，用完整文件名

            if series not in groups:
                groups[series] = []
            groups[series].append(f)

        return groups

    def _extract_episode_numbers(files: List[str]) -> str:
        """从文件名列表提取集数信息，返回紧凑的集数字符串"""
        import re
        episodes = set()
        for f in files:
            # 匹配 S01E03, E03, EP03 等
            match = re.search(r'[.\s]S\d+E(\d+)', f, re.IGNORECASE)
            if not match:
                match = re.search(r'[.\s]EP?(\d+)', f, re.IGNORECASE)
            if match:
                episodes.add(int(match.group(1)))

        if not episodes:
            return ""

        sorted_eps = sorted(episodes)
        # 生成紧凑范围表示: [1,2,3,5,7,8] -> "E01-E03, E05, E07-E08"
        ranges = []
        start = sorted_eps[0]
        end = sorted_eps[0]
        for ep in sorted_eps[1:]:
            if ep == end + 1:
                end = ep
            else:
                ranges.append(f"E{start:02d}-E{end:02d}" if start != end else f"E{start:02d}")
                start = end = ep
        ranges.append(f"E{start:02d}-E{end:02d}" if start != end else f"E{start:02d}")

        return ", ".join(ranges)

    def _run_scheduled_task(self):
        """
        定时任务：执行检测未上传和清理假失败
        """
        logger.info(
            f"SubscriptionManager: 定时任务开始，执行清理与重新整理 "
            f"(clean_failed={self._clean_failed})"
        )
        try:
            self._run_cleanup_task()
            self._run_retransfer_task()
            if self._clean_failed:
                self._run_clean_failed_task()
        finally:
            logger.info("SubscriptionManager: 定时任务结束")

    def _stop_transfer_cleanup(self):
        """停止服务"""
        # 停止延迟删除线程
        if self._stop_event:
            self._stop_event.set()
        if self._delay_thread and self._delay_thread.is_alive():
            self._delay_thread.join(timeout=5)
            logger.info("SubscriptionManager: 延迟删除线程已停止")

        # 刷新并停止通知定时器（防止停服后丢通知）
        if self._notify_timer:
            self._notify_timer.cancel()
            self._notify_timer = None
        # 发送缓冲区中残留的通知
        if self._notify_buffer and len(self._notify_buffer) > 0:
            try:
                self._flush_notify_buffer(self._dry_run)
            except Exception:
                pass

        # 停止目录监控
        if self._observers:
            for observer in self._observers:
                try:
                    observer.stop()
                    observer.join(timeout=5)
                except Exception as e:
                    logger.exception("SubscriptionManager: 停止监控异常")
            self._observers = []
            logger.info("SubscriptionManager: 目录监控已停止")

    def _get_transfer_service(self) -> List[Dict[str, Any]]:
        """
        注册定时任务
        """
        if not self._enabled:
            return []

        cron_exp = (self._retransfer_cron or "").strip()
        if not cron_exp:
            return []

        try:
            trigger = CronTrigger.from_crontab(cron_exp)
        except Exception as e:
            logger.error(f"SubscriptionManager: 无效的定时任务表达式 `{cron_exp}`: {e}")
            return []

        return [{
            "id": "SubscriptionManagerTransferCleanup",
            "name": "转移记录清理",
            "trigger": trigger,
            "func": self._run_scheduled_task,
            "kwargs": {}
        }]


class SubscriptionManager(TransferCleanupMixin, _PluginBase):
    # 插件名称
    plugin_name = "订阅管理"
    # 插件描述
    plugin_desc = "统一管理续作订阅、Trakt 日历提醒和转移记录清理"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/i-kirito/MoviePilot-SubscriptionManager/main/icons/subscriptionmanager.png"
    # 插件版本
    plugin_version = "1.0.1"
    # 插件作者
    plugin_author = "i-kirito"
    # 作者主页
    author_url = "https://github.com/i-kirito"
    # 插件配置项ID前缀
    plugin_config_prefix = "subscriptionmanager_"
    # 加载顺序
    plugin_order = 99
    # 可使用的用户级别
    auth_level = 2

    # 订阅历史保留旧版“续作跟进”归属，新记录使用统一插件名。
    subscription_owner_names = ("订阅管理", "续作跟进")

    _transfer_config_keys = (
        "notify", "dry_run", "delay_enabled", "delay_seconds",
        "monitor_dirs", "path_mappings", "exclude_dirs", "exclude_keywords",
        "clean_dirs", "run_once", "retransfer_once", "retransfer_dirs",
        "retransfer_cron", "clean_failed",
    )

    def _merge_legacy_config(self, config: Optional[dict]) -> dict:
        """合并旧 FollowUp/TransferCleaner 配置，优先保留新插件已保存值。"""
        merged = {}
        legacy_followup = self.systemconfig.get("plugin.FollowUp") or {}
        legacy_transfer = self.systemconfig.get("plugin.TransferCleaner") or {}
        if isinstance(legacy_followup, dict):
            merged.update(legacy_followup)
        if isinstance(legacy_transfer, dict):
            for key, value in legacy_transfer.items():
                if key != "enabled":
                    merged.setdefault(key, value)
            if "enabled" not in merged:
                merged["enabled"] = legacy_transfer.get("enabled", False)
        if isinstance(config, dict):
            merged.update(config)
        return merged

    def _migrate_legacy_data(self) -> None:
        """迁移 FollowUp 的插件数据，保留订阅忽略项、Trakt 状态和集合缓存。"""
        try:
            legacy_rows = self.plugindata.get_data_all("FollowUp") or []
            for row in legacy_rows:
                key = getattr(row, "key", None)
                if not isinstance(key, str):
                    continue
                if self.plugindata.get_data("SubscriptionManager", key) is None:
                    self.plugindata.save("SubscriptionManager", key, getattr(row, "value", None))
        except Exception as exc:
            logger.warning(f"迁移续作跟进插件数据失败：{exc}")

    def _sync_transfer_config(self) -> None:
        config = self._get_config()
        for key in self._transfer_config_keys:
            if hasattr(self, f"_{key}"):
                setattr(config, key, getattr(self, f"_{key}"))

    def _transfer_form_sections(self) -> list[dict]:
        def field(component: str, model: str, label: str, **props: Any) -> dict:
            field_props = {
                "model": model,
                "label": label,
                "density": "comfortable",
            }
            field_props.update(props)
            return {"component": component, "props": field_props}

        def col(content: dict, cols: int = 12, sm: int | None = None, md: int | None = None) -> dict:
            props: dict[str, Any] = {"cols": cols}
            if sm is not None:
                props["sm"] = sm
            if md is not None:
                props["md"] = md
            return {"component": "VCol", "props": props, "content": [content]}

        def section(title: str, subtitle: str, content: list[dict]) -> dict:
            return {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "text-subtitle-1 font-weight-bold pt-3 pb-1"},
                        "text": title,
                    },
                    {
                        "component": "VCardSubtitle",
                        "props": {"class": "text-caption pt-0 pb-2"},
                        "text": subtitle,
                    },
                    {"component": "VCardText", "props": {"class": "pt-1"}, "content": content},
                ],
            }

        return [
            section(
                "转移记录清理",
                "监控目录中的文件移动或删除后，自动清理对应的转移历史。与续作订阅共用插件开关。",
                [
                    {
                        "component": "VRow",
                        "props": {"class": "ga-1"},
                        "content": [
                            col(field("VSwitch", "notify", "发送清理通知"), 12, 6, 3),
                            col(field("VSwitch", "dry_run", "模拟运行"), 12, 6, 3),
                            col(field("VSwitch", "delay_enabled", "延迟删除"), 12, 6, 3),
                            col(field("VTextField", "delay_seconds", "延迟秒数", type="number", min=1, max=3600), 12, 6, 3),
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "ga-1"},
                        "content": [
                            col(field("VSwitch", "run_once", "立即清理一次"), 12, 6, 4),
                            col(field("VSwitch", "clean_failed", "清理假失败记录"), 12, 6, 4),
                            col(field("VTextField", "retransfer_cron", "清理定时周期", placeholder="0 */2 * * *"), 12, 12, 4),
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"class": "ga-1"},
                        "content": [
                            col(field("VTextarea", "monitor_dirs", "监控目录（每行一个）", rows=2, placeholder="/media/待上传\n/media/downloads"), 12, 12, 6),
                            col(field("VTextarea", "path_mappings", "路径映射（每行一个）", rows=2, placeholder="/media/115/转存:u115:/115/转存"), 12, 12, 6),
                            col(field("VTextarea", "exclude_dirs", "排除目录", rows=2), 12, 12, 6),
                            col(field("VTextarea", "exclude_keywords", "排除关键词", rows=2), 12, 12, 6),
                            col(field("VTextarea", "retransfer_dirs", "重新整理检测目录", rows=2, placeholder="/media/待上传"), 12, 12, 6),
                        ],
                    },
                ],
            ),
        ]

    # 私有属性
    _last_request_time = 0
    _request_lock = asyncio.Lock()
    _min_interval = 0.025
    # 配置
    config: Optional[SubscriptionManagerConfig] = None

    def init_plugin(self, config: dict = None):

        # 停止现有任务
        self.stop_service()
        merged_config = self._merge_legacy_config(config)
        self.load_config(merged_config)
        self._migrate_legacy_data()
        self._init_transfer_cleanup(self._get_config().model_dump())
        self.tmdbapi = TmdbApi()
        self._sync_transfer_config()
        if merged_config != (config or {}):
            self.update_config(self._get_config().model_dump())


        if self.config.onlyonce:
            self.schedule_once()
            # 关闭一次性开关
            self.config.onlyonce = False
            self.__update_config()

    def load_config(self, config: dict):
        """加载配置"""
        self.config = SubscriptionManagerConfig(**config) if config else SubscriptionManagerConfig()

    def _get_config(self) -> SubscriptionManagerConfig:
        """Return a usable config even when a page or event is queried early."""
        if not isinstance(self.config, SubscriptionManagerConfig):
            self.config = SubscriptionManagerConfig()
        return self.config

    def schedule_once(self):
        logger.info("订阅管理，立即运行一次")
        loop = getattr(global_vars, "loop", None)
        if loop is None or not loop.is_running():
            logger.warning("订阅管理无法立即运行：MoviePilot 主事件循环未运行")
            return None
        return asyncio.run_coroutine_threadsafe(self.follow_up(), loop)

    def __update_config(self):
        """更新配置"""
        self.update_config(self._get_config().model_dump())

    def _get_trakt_cookie_from_cookiecloud(self) -> Optional[str]:
        """
        从 MoviePilot 的全局 CookieCloud 读取 ``trakt.tv`` 的 Cookie。

        CookieCloud 的宿主可指向 Mac；本插件不读取浏览器资料、Keychain 或
        CookieCloud 配置本身，也绝不记录 Cookie 内容。
        """
        try:
            cookies, error = CookieCloudHelper().download()
        except Exception:
            logger.warning("CookieCloud 同步失败，已跳过本轮 Trakt 日历读取")
            return None

        if error or not isinstance(cookies, dict):
            logger.warning("CookieCloud 未提供可用数据，已跳过本轮 Trakt 日历读取")
            return None

        cookie = cookies.get("trakt.tv")
        if not isinstance(cookie, str) or not cookie.strip():
            logger.warning(
                "CookieCloud 中未找到 trakt.tv Cookie，已跳过本轮 Trakt 日历读取"
            )
            return None
        return cookie.strip()

    def _request_trakt_calendar_page(self, cookie: str) -> str:
        """用全局代理和 CookieCloud 登录态只读获取 Trakt 日历页面。"""
        response = RequestUtils(
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "Referer": "https://app.trakt.tv/",
                "User-Agent": (
                    settings.USER_AGENT
                    or f"MoviePilot-SubscriptionManager/{self.plugin_version}"
                ),
            },
            cookies=cookie,
            proxies=settings.PROXY,
            timeout=TRAKT_CALENDAR_PAGE_TIMEOUT_SECONDS,
        ).get_res(
            url=TRAKT_CALENDAR_PAGE_URL,
            allow_redirects=False,
        )
        if response is None:
            raise TraktRequestError("Trakt 网络请求失败")

        try:
            if response.status_code != 200:
                raise TraktRequestError(
                    f"Trakt 返回 HTTP {response.status_code}",
                    status_code=response.status_code,
                )
            body = response.content or b""
            if len(body) > TRAKT_CALENDAR_PAGE_MAX_BYTES:
                raise TraktRequestError("Trakt 日历页面过大")
            return body.decode("utf-8", errors="replace")
        finally:
            response.close()

    @staticmethod
    def _parse_trakt_oidc_expiry(value: str) -> Optional[float]:
        """返回 OIDC 过期时间戳；未知格式一律视为不可用。"""
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except (TypeError, ValueError):
                return None
            if parsed.tzinfo is None:
                return None
            return parsed.timestamp()

        # JavaScript 页面通常使用毫秒时间戳；兼容秒级时间戳。
        return numeric_value / 1000 if numeric_value >= 10_000_000_000 else numeric_value

    @classmethod
    def parse_trakt_calendar_oidc_token(
        cls,
        page: str,
        now: Optional[float] = None,
    ) -> Optional[str]:
        """
        仅从 Trakt 页面内嵌 ``oidcAuth`` 对象取得当前短时 token。

        token 从不写入配置、数据库或日志；缺失、过期、未知格式均返回空，
        由调用方安全跳过本轮日历检查。
        """
        if not isinstance(page, str) or not page.strip():
            return None

        current_time = now if now is not None else time.time()
        normalized_page = html.unescape(page).replace(r'\"', '"')
        for match in TRAKT_OIDC_AUTH_BLOCK_PATTERN.finditer(normalized_page):
            block = match.group("block")
            token_match = TRAKT_OIDC_TOKEN_PATTERN.search(block)
            expires_match = TRAKT_OIDC_EXPIRES_PATTERN.search(block)
            if not token_match or not expires_match:
                continue

            expires_value = expires_match.group("quoted") or expires_match.group("number")
            expires_at = cls._parse_trakt_oidc_expiry(expires_value)
            if expires_at is None or expires_at <= current_time + 30:
                continue
            return token_match.group("token")
        return None

    def _request_trakt_calendar_json(
        self,
        cookie: str,
        oidc_token: str,
        start_date: str,
        days: int,
    ) -> Any:
        """通过 Trakt Web App 公共日历接口读取 JSON，不保存短时 token。"""
        url = f"{TRAKT_CALENDAR_API_BASE}/calendars/my/shows/{start_date}/{days}"
        response = RequestUtils(
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {oidc_token}",
                "Origin": "https://app.trakt.tv",
                "Referer": "https://app.trakt.tv/",
                "trakt-api-key": TRAKT_WEB_API_KEY,
                "trakt-api-version": "2",
                "User-Agent": (
                    settings.USER_AGENT
                    or f"MoviePilot-SubscriptionManager/{self.plugin_version}"
                ),
            },
            cookies=cookie,
            proxies=settings.PROXY,
            timeout=TRAKT_CALENDAR_PAGE_TIMEOUT_SECONDS,
        ).get_res(
            url=url,
            params={"extended": "full,images", "group": "day"},
            allow_redirects=False,
        )
        if response is None:
            raise TraktRequestError("Trakt 日历 API 网络请求失败")

        try:
            if response.status_code != 200:
                raise TraktRequestError(
                    f"Trakt 日历 API 返回 HTTP {response.status_code}",
                    status_code=response.status_code,
                )
            try:
                payload = response.json()
            except Exception as exc:
                raise TraktRequestError("Trakt 日历 API 返回了无效 JSON") from exc
            if not isinstance(payload, list):
                raise TraktRequestError("Trakt 日历 API 返回了不支持的数据布局")
            return payload
        finally:
            response.close()

    @staticmethod
    def parse_trakt_calendar_events(
        payload: Any,
        now: Optional[datetime] = None,
    ) -> List[TraktCalendarEvent]:
        """
        纯解析 Trakt ``calendars/my/shows`` 响应。

        只接受 ``show.ids.tmdb``，不会拿标题、年份或 TVDB/Trakt ID 猜测
        MoviePilot 媒体，从源头避免同名剧被误订阅。
        """
        if not isinstance(payload, list):
            return []

        reference_date = (now or datetime.now(timezone.utc)).date()
        normalized: dict[str, TraktCalendarEvent] = {}
        for item in payload:
            if not isinstance(item, dict):
                continue
            show = item.get("show")
            episode = item.get("episode")
            if not isinstance(show, dict) or not isinstance(episode, dict):
                continue
            ids = show.get("ids")
            if not isinstance(ids, dict):
                continue
            try:
                tmdb_id = int(ids.get("tmdb"))
                season_number = int(episode.get("season"))
                episode_number = int(episode.get("number"))
            except (TypeError, ValueError):
                continue
            if tmdb_id <= 0 or season_number <= 0 or episode_number <= 0:
                continue

            air_value = item.get("first_aired") or episode.get("first_aired")
            if not isinstance(air_value, str) or len(air_value) < 10:
                continue
            air_date = air_value[:10]
            try:
                event_date = datetime.strptime(air_date, "%Y-%m-%d").date()
            except ValueError:
                continue

            title = episode.get("title")
            event = TraktCalendarEvent(
                tmdb_id=tmdb_id,
                season_number=season_number,
                episode_number=episode_number,
                air_date=air_date,
                title=title.strip() if isinstance(title, str) else "",
                status="upcoming" if event_date >= reference_date else "recent",
            )
            current = normalized.get(event.event_key)
            # 同一 Trakt 事件偶尔会重复出现；保留标题更完整的那一个。
            if current is None or (event.title and not current.title):
                normalized[event.event_key] = event

        return sorted(
            normalized.values(),
            key=lambda event: (
                event.tmdb_id,
                event.season_number,
                event.episode_number,
                event.air_date,
            ),
        )

    @staticmethod
    def _trakt_calendar_page_problem(page: str) -> Optional[str]:
        """识别需要在 CookieCloud/Trakt 侧处理的常见安全失败页。"""
        normalized = page.lower()
        cloudflare_markers = (
            "just a moment...",
            "checking your browser",
        )
        if any(marker in normalized for marker in cloudflare_markers):
            return "Cloudflare 验证页"
        return None

    async def _fetch_trakt_calendar_events(self) -> List[TraktCalendarEvent]:
        """通过 CookieCloud 只读拉取 Trakt 个人剧集日历。"""
        config = self._get_config()
        if not config.trakt_calendar_enabled:
            return []

        cookie = await asyncio.to_thread(self._get_trakt_cookie_from_cookiecloud)
        if not cookie:
            return []

        try:
            page = await asyncio.to_thread(self._request_trakt_calendar_page, cookie)
        except TraktRequestError as exc:
            if exc.status_code in {401, 403}:
                logger.warning(
                    "Trakt CookieCloud 日历访问被拒绝，请在 Mac CookieCloud 更新 Trakt 登录状态"
                )
            else:
                logger.warning(f"Trakt CookieCloud 日历请求失败：{exc}")
            return []

        if problem := self._trakt_calendar_page_problem(page):
            logger.warning(
                f"Trakt CookieCloud 日历返回{problem}，请在 Mac CookieCloud 更新登录状态"
            )
            return []

        oidc_token = self.parse_trakt_calendar_oidc_token(page)
        if not oidc_token:
            logger.warning(
                "Trakt CookieCloud 页面未识别到有效登录 token，"
                "请在 Mac CookieCloud 更新 Trakt 登录状态"
            )
            return []

        now = datetime.now(timezone.utc)
        start_date, window_days = self._trakt_calendar_window(now)
        try:
            payload = await asyncio.to_thread(
                self._request_trakt_calendar_json,
                cookie,
                oidc_token,
                start_date,
                window_days,
            )
        except TraktRequestError as exc:
            if exc.status_code in {401, 403}:
                logger.warning(
                    "Trakt CookieCloud 日历 API 访问被拒绝，请在 Mac CookieCloud 更新 Trakt 登录状态"
                )
            else:
                logger.warning(f"Trakt CookieCloud 日历 API 请求失败：{exc}")
            return []

        events = self.parse_trakt_calendar_events(payload, now=now)
        if not events:
            logger.warning("Trakt CookieCloud 日历 API 未返回可用剧集数据，已跳过本轮")
            return []

        window_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        window_end = window_start + timedelta(days=window_days)
        return [
            event
            for event in events
            if (
                window_start
                <= datetime.strptime(event.air_date, "%Y-%m-%d").date()
                < window_end
            )
            if self.is_date_in_range(
                event.air_date,
                now,
                threshold_days=config.after_days,
            )
        ]

    def _trakt_calendar_window(
        self,
        now: Optional[datetime] = None,
    ) -> tuple[str, int]:
        """Return a UTC calendar range with enough look-back for missed runs."""
        reference = now or datetime.now(timezone.utc)
        utc_today = (
            reference.date()
            if reference.tzinfo is None
            else reference.astimezone(timezone.utc).date()
        )
        config = self._get_config()
        lookback_days = config.after_days
        return (
            (utc_today - timedelta(days=lookback_days)).isoformat(),
            config.trakt_calendar_days + lookback_days,
        )

    @staticmethod
    def _trakt_event_priority(event: TraktCalendarEvent) -> tuple:
        """
        同一剧在窗口内有多集时，优先更高的新季；同季取更早的集数，
        使订阅从正确的季起点接续。
        """
        return (
            -event.season_number,
            event.episode_number,
            event.air_date,
            event.event_key,
        )

    def _merge_trakt_calendar_candidates(
        self,
        candidates: set[tuple[str, int]],
        events: List[TraktCalendarEvent],
    ) -> Dict[tuple[str, int], dict]:
        """
        将 Trakt 事件限制在媒体库/订阅历史已有候选内，再让精确日历事件
        覆盖同条目的 TMDB 推测更新。
        """
        selected: Dict[tuple[str, int], TraktCalendarEvent] = {}
        for event in events:
            key = (MediaType.TV.value, event.tmdb_id)
            if key not in candidates:
                logger.debug(
                    f"Trakt 日历 {event.tmdb_id} 不在媒体库或订阅历史候选中，跳过"
                )
                continue
            current = selected.get(key)
            if current is None or self._trakt_event_priority(event) < self._trakt_event_priority(current):
                selected[key] = event

        return {
            key: event.to_update()
            for key, event in selected.items()
        }

    def _is_trakt_event_recorded(self, event_key: Optional[str]) -> bool:
        if not event_key:
            return False
        record = self.get_data(event_key)
        if not isinstance(record, dict):
            return bool(record)

        status = record.get("status")
        if status == "failed":
            # 自动订阅失败需要保留下一轮重试机会；同一轮的重复项已在
            # 纯解析和候选合并阶段折叠，不会因此重复调用。
            return False
        if status != "processing":
            return status in {"subscribed", "already_subscribed", "notified"}

        recorded_at = record.get("recorded_at")
        try:
            processing_at = datetime.fromisoformat(recorded_at)
        except (TypeError, ValueError, OverflowError):
            return True
        # 防止进程在 SubscribeChain 执行中异常退出后永久卡住一个事件。
        if processing_at.tzinfo is None:
            current_time = datetime.now()
        else:
            current_time = datetime.now(timezone.utc)
            processing_at = processing_at.astimezone(timezone.utc)
        return (current_time - processing_at).total_seconds() < TRAKT_PROCESSING_TTL_SECONDS

    def _record_trakt_event(self, update: dict, status: str) -> None:
        """保存单事件状态，避免已成功/已通知日历项重复处理并保留失败重试。"""
        event_key = update.get("_followup_event_key")
        if not event_key:
            return
        self.save_data(
            event_key,
            {
                "status": status,
                "tmdbid": update.get("_followup_tmdb_id"),
                "season": update.get("season_number"),
                "episode": update.get("episode_number"),
                "air_date": update.get("air_date"),
                "recorded_at": datetime.now().isoformat(timespec="seconds"),
            },
        )

    def _get_followup_form(self):
        # 获取所有启用的媒体服务器及其库信息
        mediaservers = ServiceConfigHelper.get_mediaserver_configs() or []
        libraryitems = []
        serverchain = MediaServerChain()
        for mediaserver in mediaservers:
            if not mediaserver or not getattr(mediaserver, "enabled", False):
                continue
            server_name = getattr(mediaserver, "name", None)
            if not server_name:
                continue
            try:
                libraries = serverchain.librarys(server_name) or []
            except Exception as exc:
                logger.warning(f"获取 {server_name} 媒体库失败：{exc}")
                continue
            for library in libraries:
                library_id = getattr(library, "id", None)
                if library_id is None:
                    continue
                libraryitems.append(
                    {
                        "title": getattr(library, "name", "未命名媒体库"),
                        "value": str(library_id),
                        "subtitle": server_name,
                    }
                )

        form = [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 4},
                                'content': [
                                    {
                                        # 'component': 'VTextField', # 组件替换为VCronField
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式，留空自动',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'auto_subscribe',
                                            'label': '命中自动订阅',
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'trakt_calendar_enabled',
                                            'label': '接入 CookieCloud Trakt 个人剧集日历',
                                            'hint': '只读取 MoviePilot 全局 CookieCloud 中的 trakt.tv Cookie，并仅匹配明确的 TMDB ID',
                                            'persistent-hint': True,
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'trakt_calendar_days',
                                            'label': 'Trakt 日历拉取天数',
                                            'type': 'number',
                                            'min': 1,
                                            'max': 30,
                                            'step': 1,
                                            'hint': '用于筛选日历页面范围；提醒窗口仍使用“提前提醒天数”',
                                            'persistent-hint': True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'check_sub_history',
                                            'label': '检查订阅历史',
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'after_days',
                                            'label': '提前提醒(天)',
                                            'type': 'number',
                                            'min': 1,
                                            'max': 30,
                                            'step': 1,
                                        },
                                    }
                                ],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 5},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'threshold_years',
                                            'label': '检查年限',
                                            'placeholder': '播出或上映时间超出年限则不再检查',
                                            'type': 'number',
                                            'min': 2,
                                            'max': 50,
                                            'step': 1,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 12},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'libraries',
                                            'label': '选择媒体库',
                                            'chips': True,
                                            'multiple': True,
                                            'clearable': True,
                                            'items': libraryitems,
                                            'item-props': True
                                        }
                                    }
                                ],
                            },
                        ],
                    },
                ],
            },
        ]
        defaults = {
            "enabled": False,
            "auto_subscribe": True,
            "after_days": 2,
            "threshold_years": 15,
            "cron": "",
            "onlyonce": False,
            "check_sub_history": True,
            "libraries": [],
            "trakt_calendar_enabled": False,
            "trakt_calendar_days": 7,
        }

        def section(title: str, subtitle: str, content: list[dict]) -> dict:
            return {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "text-subtitle-1 font-weight-bold pt-3 pb-1"},
                        "text": title,
                    },
                    {
                        "component": "VCardSubtitle",
                        "props": {"class": "text-caption pt-0 pb-2"},
                        "text": subtitle,
                    },
                    {"component": "VCardText", "props": {"class": "pt-1"}, "content": content},
                ],
            }

        core, trakt, scan, libraries = form[0]["content"]
        form[0]["content"] = [
            section("核心开关", "控制插件启停、执行周期和命中订阅后的处理方式。", [core]),
            section("Trakt 日历", "从 MoviePilot 全局 CookieCloud 读取个人剧集日历，仅匹配明确的 TMDB ID。", [trakt]),
            section("订阅扫描", "控制订阅历史检查范围与提醒窗口。", [scan, libraries]),
        ]
        return form, defaults

    def get_form(self):
        forms, defaults = self._get_followup_form()
        if forms and isinstance(forms[0], dict):
            forms[0].setdefault("content", []).extend(self._transfer_form_sections())
        defaults.update({
            "notify": True,
            "dry_run": True,
            "delay_enabled": False,
            "delay_seconds": 10,
            "monitor_dirs": "",
            "path_mappings": "",
            "exclude_dirs": "",
            "exclude_keywords": "",
            "clean_dirs": "",
            "run_once": False,
            "retransfer_once": False,
            "retransfer_dirs": "",
            "retransfer_cron": "",
            "clean_failed": False,
        })
        return forms, defaults

    def get_service(self) -> List[Dict[str, Any]]:
        """注册续作订阅和转移记录清理两个公共服务。"""
        services = []
        config = self._get_config()
        if config.enabled:
            try:
                trigger = CronTrigger.from_crontab(config.cron) if config.cron else "interval"
            except (TypeError, ValueError) as exc:
                logger.error(f"订阅管理 cron 配置无效，续作服务未注册：{exc}")
            else:
                services.append({
                    "id": "SubscriptionManagerFollowUp",
                    "name": "续作自动订阅",
                    "trigger": trigger,
                    "func": self.follow_up,
                    "kwargs": {"hours": 24} if not config.cron else {},
                })
        transfer_service = self._get_transfer_service()
        if transfer_service:
            services.extend(transfer_service)
        return services

    def stop_service(self):
        """停止续作任务和转移记录清理监控。"""
        self._stop_transfer_cleanup()

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_command(self):
        return [
            {
                "cmd": "/follow_up",
                "event": EventType.PluginAction,
                "desc": "订阅管理",
                "category": "",
                "data": {"action": "follow_up"}
            }
        ]

    def get_page(self) -> List[dict]:
        """插件详情页：用概览卡片和分组表格展示续作处理链路。"""
        config = self._get_config()
        plugin_status_label = "运行中" if config.enabled else "已停用"
        plugin_status_color = "success" if config.enabled else "warning"
        trakt_status_label = (
            "Trakt 日历已接入"
            if config.trakt_calendar_enabled
            else "Trakt 日历未接入"
        )
        trakt_status_color = "success" if config.trakt_calendar_enabled else "info"

        # 自动订阅成功后，插件自身的临时数据会被清理；历史记录保存在
        # SubscribeHistory 中，因此详情页必须从历史表读取，而不是只看 get_data()。
        history_records = self.get_followup_subscription_history(limit=60)
        # 详情页只承担运行概览和最近记录预览，避免把整张卡片撑成长列表。
        # 完整历史仍保留在 SubscribeHistory 中，数量徽标用于提示总量。
        preview_limit = 5
        try:
            active_records = [
                item
                for item in (SubscribeOper().list() or [])
                if getattr(item, "username", None) in self.subscription_owner_names
            ]
        except Exception as exc:
            logger.warning(f"读取订阅管理当前订阅失败：{exc}")
            active_records = []

        pending_records = []
        event_counts = {"failed": 0, "processing": 0}
        try:
            data_entries = self.get_data() or []
        except Exception as exc:
            logger.warning(f"读取订阅管理待处理数据失败：{exc}")
            data_entries = []
        for entry in data_entries:
            entry_key = getattr(entry, "key", None)
            if not isinstance(entry_key, str):
                continue
            if entry_key.startswith(TRAKT_EVENT_DATA_PREFIX):
                event = self.get_data(entry_key)
                if isinstance(event, dict) and event.get("status") in event_counts:
                    event_counts[event["status"]] += 1
                continue
            parsed_key = self.parse_key(entry_key)
            if not parsed_key:
                continue
            data = self.get_data(entry_key)
            if isinstance(data, dict):
                pending_records.append((entry_key, data))

        def record_title(item: Any) -> str:
            title = getattr(item, "name", None)
            year = getattr(item, "year", None)
            if not title and isinstance(item, dict):
                title = item.get("title") or item.get("name")
                year = item.get("year")
            title = str(title or "未命名媒体")
            return f"{title} ({year})" if year else title

        def record_season(item: Any) -> str:
            season = getattr(item, "season", None)
            if isinstance(item, dict):
                season = item.get("season", season)
            try:
                return f"S{int(season):02d}" if season is not None else "全剧"
            except (TypeError, ValueError):
                return str(season or "全剧")

        def record_date(item: Any) -> str:
            value = getattr(item, "date", None)
            if isinstance(item, dict):
                value = item.get("date", value)
            return str(value or "—")

        def record_identity(item: Any) -> tuple[Optional[str], Optional[int], Optional[int]]:
            media_type = getattr(item, "type", None)
            tmdbid = getattr(item, "tmdbid", None)
            season = getattr(item, "season", None)
            if isinstance(item, dict):
                media_type = item.get("type", media_type)
                tmdbid = item.get("tmdbid", tmdbid)
                season = item.get("season", season)
            media_type = getattr(media_type, "value", media_type)
            try:
                tmdbid = int(tmdbid) if tmdbid is not None else None
            except (TypeError, ValueError):
                tmdbid = None
            try:
                season = int(season) if season is not None else None
            except (TypeError, ValueError):
                season = None
            return (
                str(media_type) if media_type is not None else None,
                tmdbid,
                season,
            )

        active_keys = {
            record_identity(item)
            for item in active_records
        }

        def history_row(item: Any) -> dict:
            key = record_identity(item)
            status = "当前订阅" if key in active_keys else "已完成添加"
            return {
                "component": "tr",
                "content": [
                    {"component": "td", "text": record_title(item)},
                    {"component": "td", "text": record_season(item)},
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "variant": "tonal",
                                    "color": "success" if status == "当前订阅" else "info",
                                },
                                "text": status,
                            }
                        ],
                    },
                    {
                        "component": "td",
                        "props": {"style": "white-space: nowrap;"},
                        "text": record_date(item),
                    },
                ],
            }

        def pending_row(entry: tuple[str, dict]) -> dict:
            key, data = entry
            title = data.get("title") or key
            year = data.get("year")
            title_text = f"{title} ({year})" if year else str(title)
            return {
                "component": "tr",
                "content": [
                    {"component": "td", "text": title_text},
                    {"component": "td", "text": record_season(data)},
                    {
                        "component": "td",
                        "content": [
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "variant": "tonal",
                                    "color": "warning",
                                },
                                "text": "待处理提醒",
                            }
                        ],
                    },
                    {
                        "component": "td",
                        "props": {"style": "white-space: nowrap;"},
                        "text": str(data.get("air_date") or data.get("date") or "—"),
                    },
                ],
            }

        def table_card(
            title: str,
            headers: list[str],
            rows: list[dict],
            empty_text: str,
            subtitle: str,
            total: Optional[int] = None,
        ) -> dict:
            table_content = [
                {
                    "component": "VTable",
                    "props": {
                        "density": "compact",
                        "hover": True,
                        "fixed-header": True,
                        "style": "min-width: 0; font-size: 12px;",
                    },
                    "content": [
                        {
                            "component": "thead",
                            "content": [
                                {
                                    "component": "tr",
                                    "content": [
                                        {"component": "th", "text": header}
                                        for header in headers
                                    ],
                                }
                            ],
                        },
                        {
                            "component": "tbody",
                            "content": rows,
                        },
                    ],
                }
            ]
            if not rows:
                table_content = [
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "density": "compact",
                            "text": empty_text,
                        },
                    }
                ]
            return {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "mt-4",
                },
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center pt-4 pb-1"},
                        "content": [
                            {
                                "component": "span",
                                "props": {"class": "text-subtitle-1 font-weight-bold"},
                                "text": title,
                            },
                            {
                                "component": "VSpacer",
                            },
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "variant": "tonal",
                                    "color": "primary",
                                },
                                "text": str(total if total is not None else len(rows)),
                            },
                        ],
                    },
                    {
                        "component": "VCardSubtitle",
                        "props": {"class": "pt-0 pb-3"},
                        "text": subtitle,
                    },
                    {"component": "VDivider"},
                    {
                        "component": "div",
                        "props": {
                            "style": (
                                "max-height: 205px; overflow: auto; "
                                "scrollbar-width: thin;"
                            ),
                        },
                        "content": table_content,
                    },
                ],
            }

        def metric_card(label: str, value: str, hint: str, color: str) -> dict:
            """Overview metric card styled like the newer plugin pages."""
            return {
                "component": "VCol",
                "props": {"cols": 12, "sm": 6, "md": 3},
                "content": [
                    {
                        "component": "VCard",
                        "props": {
                            "variant": "tonal",
                            "color": color,
                            "class": "h-100",
                        },
                        "content": [
                            {
                                "component": "VCardText",
                                "props": {"class": "py-4"},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {
                                            "class": "text-caption text-medium-emphasis",
                                        },
                                        "text": label,
                                    },
                                    {
                                        "component": "div",
                                        "props": {
                                            "class": "text-h4 font-weight-bold mt-1",
                                        },
                                        "text": value,
                                    },
                                    {
                                        "component": "div",
                                        "props": {
                                            "class": "text-caption text-medium-emphasis mt-1",
                                        },
                                        "text": hint,
                                    },
                                ],
                            }
                        ],
                    }
                ],
            }

        def summary_item(label: str, value: str) -> dict:
            return {
                "component": "VCol",
                "props": {"cols": 12, "sm": 6, "md": 4},
                "content": [
                    {
                        "component": "div",
                        "props": {"class": "text-caption text-medium-emphasis"},
                        "text": label,
                    },
                    {
                        "component": "div",
                        "props": {"class": "text-body-2 font-weight-medium mt-1"},
                        "text": value,
                    },
                ],
            }

        issue_total = event_counts["failed"] + event_counts["processing"]
        library_label = f"{len(config.libraries)} 个媒体库" if config.libraries else "全部媒体库"
        issue_hint = "无未完成 Trakt 事件" if issue_total == 0 else "需要关注的 Trakt 事件"
        issue_color = "success" if issue_total == 0 else "warning"
        transfer_dirs = [d.strip() for d in (self._monitor_dirs or "").split("\n") if d.strip()]
        transfer_status = (
            f"已开启 · {len(transfer_dirs)} 个监控目录"
            if self._enabled
            else "已停用"
        )
        transfer_mode = "模拟运行" if self._dry_run else "实际清理"

        alerts = []
        if event_counts["failed"]:
            alerts.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "error",
                        "variant": "tonal",
                        "density": "compact",
                        "class": "mt-4",
                        "text": f"Trakt 有 {event_counts['failed']} 条失败事件，下一轮运行会继续保留重试机会。",
                    },
                }
            )
        if event_counts["processing"]:
            alerts.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "density": "compact",
                        "class": "mt-3",
                        "text": f"Trakt 有 {event_counts['processing']} 条事件正在处理中。",
                    },
                }
            )

        return [
            {
                "component": "VCard",
                "props": {"class": "mx-auto", "max-width": 1180},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center flex-wrap ga-2 pt-5"},
                        "content": [
                            {
                                "component": "span",
                                "props": {"class": "text-h6 font-weight-bold"},
                                "text": "订阅管理记录",
                            },
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "variant": "tonal",
                                    "color": plugin_status_color,
                                },
                                "text": plugin_status_label,
                            },
                            {
                                "component": "VChip",
                                "props": {
                                    "size": "small",
                                    "variant": "tonal",
                                    "color": trakt_status_color,
                                },
                                "text": trakt_status_label,
                            },
                        ],
                    },
                    {
                        "component": "VCardSubtitle",
                        "props": {"class": "pt-1"},
                        "text": "续作订阅、Trakt 提醒和转移清理 · 只读运行概览",
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "props": {"class": "mt-2"},
                                "content": [
                                    metric_card(
                                        "当前订阅",
                                        str(len(active_records)),
                                        "仍由订阅管理维护",
                                        "primary",
                                    ),
                                    metric_card(
                                        "历史记录",
                                        str(len(history_records)),
                                        "最近自动订阅记录",
                                        "info",
                                    ),
                                    metric_card(
                                        "待处理提醒",
                                        str(len(pending_records)),
                                        "等待下一次处理",
                                        "warning",
                                    ),
                                    metric_card(
                                        "Trakt 事件",
                                        str(issue_total),
                                        issue_hint,
                                        issue_color,
                                    ),
                                ],
                            },
                            {
                                "component": "VDivider",
                                "props": {"class": "my-4"},
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-subtitle-2 font-weight-bold mb-2"},
                                "text": "运行概况",
                            },
                            {
                                "component": "VRow",
                                "props": {"class": "mb-1"},
                                "content": [
                                    summary_item(
                                        "自动订阅",
                                        "已开启" if config.auto_subscribe else "已关闭",
                                    ),
                                    summary_item(
                                        "检查订阅历史",
                                        "已开启" if config.check_sub_history else "已关闭",
                                    ),
                                    summary_item(
                                        "检查周期",
                                        config.cron or "每 24 小时",
                                    ),
                                    summary_item(
                                        "提醒窗口",
                                        f"提前 {config.after_days} 天",
                                    ),
                                    summary_item(
                                        "Trakt 拉取范围",
                                        f"{config.trakt_calendar_days} 天",
                                    ),
                                    summary_item("扫描范围", library_label),
                                    summary_item("转移记录清理", transfer_status),
                                    summary_item("清理模式", transfer_mode),
                                ],
                            },
                            *alerts,
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "props": {"class": "mt-1", "dense": True},
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6, "class": "py-0"},
                        "content": [
                            table_card(
                                "最近自动订阅",
                                ["媒体", "季", "状态", "时间"],
                                [history_row(item) for item in history_records[:preview_limit]],
                                "暂无自动订阅记录",
                                f"预览最近 {preview_limit} 条 · 共 {len(history_records)} 条",
                                total=len(history_records),
                            )
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6, "class": "py-0"},
                        "content": [
                            table_card(
                                "待处理提醒",
                                ["媒体", "季", "状态", "提醒日期"],
                                [pending_row(item) for item in pending_records[:preview_limit]],
                                "暂无待处理的续作提醒",
                                f"预览最近 {preview_limit} 条 · 共 {len(pending_records)} 条",
                                total=len(pending_records),
                            )
                        ],
                    },
                ],
            },
        ]

    def get_state(self):
        return self._get_config().enabled

    @eventmanager.register(EventType.PluginAction)
    def action_event_handler(self, event: Event):
        """
        远程命令处理
        """
        event_data = event.event_data if event else {}
        if not event_data or event_data.get("action") != "follow_up":
            return

        userid = event_data.get("user") or event_data.get("userid")
        self.post_message(
            channel=event_data.get("channel"),
            title="【订阅管理】开始执行 ...",
            userid=userid,
        )

        try:
            future = self.schedule_once()
        except Exception as exc:
            logger.error(f"提交订阅管理任务出错: {exc}", exc_info=True)
            self.post_message(
                channel=event_data.get("channel"),
                userid=userid,
                title="【订阅管理】执行失败",
                text=f"错误信息: {exc}",
            )
            return

        if future is None:
            self.post_message(
                channel=event_data.get("channel"),
                userid=userid,
                title="【订阅管理】执行失败",
                text="MoviePilot 主事件循环未运行",
            )
            return

        # 事件处理器运行在线程池中；不要在这里阻塞等待整个 TMDB/媒体库扫描。
        event_snapshot = dict(event_data)
        future.add_done_callback(
            lambda completed: self._follow_up_done_callback(completed, event_snapshot)
        )

    def _follow_up_done_callback(self, future, event_data: dict) -> None:
        """在后台任务完成后回报结果，不阻塞广播事件线程。"""
        userid = event_data.get("user") or event_data.get("userid")
        try:
            future.result()
        except Exception as exc:
            logger.error(f"执行订阅管理任务出错: {exc}", exc_info=True)
            result_msg = {
                "title": "【订阅管理】执行失败",
                "text": f"错误信息: {exc}",
            }
        else:
            result_msg = {"title": "【订阅管理】执行完成"}

        try:
            self.post_message(
                channel=event_data.get("channel"),
                userid=userid,
                **result_msg,
            )
        except Exception as exc:
            logger.error(f"发送订阅管理结果通知失败: {exc}", exc_info=True)

    async def _fetch_tmdb_info(self, mtype: str, tmdbid: int) -> Optional[dict]:
        # 频率限制
        async with self._request_lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._last_request_time = time.time()

        try:
            if mtype == MediaType.MOVIE.value:
                if details := await self.tmdbapi.movie.async_details(tmdbid):
                    return {
                        "title_year": f"{details.get('title')} ({(details.get('release_date') or '未知')[:4]})",
                        "type": MediaType.MOVIE,
                        "tmdb_id": tmdbid,
                        "release_date": details.get("release_date"),
                        "belongs_to_collection": details.get("belongs_to_collection")
                    }
            elif mtype == MediaType.TV.value:
                if details := await self.tmdbapi.tv.async_details(tmdbid):
                    return {
                        "title_year": f"{details.get('name')} ({(details.get('first_air_date') or '未知')[:4]})",
                        "type": MediaType.TV,
                        "tmdb_id": tmdbid,
                        "last_air_date": details.get("last_air_date"),
                        "next_episode_to_air": details.get("next_episode_to_air"),
                        "last_episode_to_air": details.get("last_episode_to_air"),
                        "seasons": details.get("seasons") or [],
                    }
            return None
        except Exception as e:
            logger.debug(f"获取TMDB信息失败 ({mtype} {tmdbid}): {e}")
            return None

    def _get_tv_update(self, min_info: dict) -> Optional[dict]:
        """
        从 TMDB 详情中提取日历式更新。

        TMDB 在剧集已播出后可能清空 ``next_episode_to_air``，而 Trakt
        日历仍会显示最近播出的集数。因此同时接受即将播出、最近播出和
        最近开播的季三种信号，且统一使用同一个提前提醒窗口。
        """
        if not isinstance(min_info, dict):
            return None

        next_episode = min_info.get("next_episode_to_air")
        next_episode = next_episode if isinstance(next_episode, dict) else {}
        next_air_date = next_episode.get("air_date")
        if next_air_date and self.is_date_in_range(
            next_air_date, threshold_days=self._get_config().after_days
        ):
            return {**next_episode, "_followup_status": "upcoming"}

        last_episode = min_info.get("last_episode_to_air")
        last_episode = last_episode if isinstance(last_episode, dict) else {}
        last_air_date = last_episode.get("air_date")
        if last_air_date and self.is_date_in_range(
            last_air_date, datetime.now(), threshold_days=self._get_config().after_days
        ):
            return {**last_episode, "_followup_status": "recent"}

        seasons = min_info.get("seasons") or []
        if not isinstance(seasons, (list, tuple)):
            seasons = []

        def season_number(item: dict) -> int:
            try:
                value = int(item.get("season_number"))
            except (TypeError, ValueError):
                return 0
            return value if value > 0 else 0

        for season in sorted(
            (
                item for item in seasons
                if isinstance(item, dict) and season_number(item) > 0
            ),
            key=season_number,
            reverse=True,
        ):
            air_date = season.get("air_date")
            if air_date and self.is_date_in_range(
                air_date, datetime.now(), threshold_days=self._get_config().after_days
            ):
                return {
                    "season_number": season_number(season),
                    "episode_number": 1,
                    "name": season.get("name"),
                    "air_date": air_date,
                    "_followup_status": "season",
                }
        return None

    async def follow_up(self):
        config = self._get_config()
        # 获取忽略列表
        _ignore = self.get_ignore_keys()
        # 获取系列合集
        collections = self.get_collections()
        # 获取需要跟进的媒体
        his = self._need_follow_up(_ignore, collections)
        if not his:
            logger.info("没有需要跟进的媒体项。")
            return

        trakt_updates = self._merge_trakt_calendar_candidates(
            his,
            await self._fetch_trakt_calendar_events(),
        )
        if trakt_updates:
            logger.info(f"Trakt 个人日历命中 {len(trakt_updates)} 个已有候选剧集。")

        await self._filter_media(his, _ignore, collections, trakt_updates)

        if collections:
            self.collection_follow_up(collections, _ignore)

        self.save_collections(collections)
        logger.info("订阅管理执行完成。")

    async def _filter_media(
        self,
        his: set[tuple[str, int]],
        _ignore: set[tuple[str, int]],
        collections: dict[str, dict],
        trakt_updates: Optional[Dict[tuple[str, int], dict]] = None,
    ):
        # 外部媒体库/历史数据可能包含旧版本写入的错误形状，逐项过滤后
        # 再解包，避免一个脏条目中断整轮扫描。
        valid_his = set()
        try:
            candidates = his or set()
            for item in candidates:
                if not isinstance(item, (tuple, list)) or len(item) != 2:
                    continue
                mtype, tmdbid = item
                mtype = getattr(mtype, "value", mtype)
                try:
                    tmdbid = int(tmdbid)
                except (TypeError, ValueError):
                    continue
                if (
                    isinstance(mtype, str)
                    and mtype in {MediaType.TV.value, MediaType.MOVIE.value}
                    and tmdbid > 0
                ):
                    valid_his.add((mtype, tmdbid))
        except TypeError:
            pass
        his = valid_his
        if not his:
            logger.info("没有可预检的有效媒体项。")
            return

        logger.info(f"开始对 {len(his)} 个条目进行预检...")

        tasks = [self._fetch_tmdb_info(mtype, tmdbid) for mtype, tmdbid in his]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items_for_full_recognition = []
        _collection_ids = set()
        his_list = list(his)
        trakt_updates = trakt_updates if isinstance(trakt_updates, dict) else {}

        for i, min_info in enumerate(results):
            key = his_list[i]
            trakt_update = trakt_updates.get(key)

            if isinstance(min_info, Exception):
                logger.debug(f"获取TMDB信息失败 ({key}): {min_info}")
                if trakt_update:
                    items_for_full_recognition.append((key, trakt_update))
                continue

            if not isinstance(min_info, dict):
                if trakt_update:
                    items_for_full_recognition.append((key, trakt_update))
                continue

            # 电视剧或非系列电影检查年限
            if not min_info.get("belongs_to_collection") and not trakt_update:
                air_date = min_info.get("last_air_date") or min_info.get("release_date")
                config = self._get_config()
                if air_date and not self.is_date_in_range(
                    air_date,
                    datetime.now(),
                    365 * config.threshold_years,
                ):
                    logger.info(
                        f"{key} {min_info.get('title_year') or ''} 已超过设定年限: "
                        f"{config.threshold_years} 年，不再跟进"
                    )
                    _ignore.add(key)
                    self.update_ignore_keys(key)
                    continue

            # 检查具体更新
            media_type = min_info.get("type")
            media_type_value = getattr(media_type, "value", media_type)
            if media_type_value == MediaType.TV.value:
                # Trakt 只会提供已经通过 TMDB ID 命中的精确季/集；若没有
                # Trakt 项则保留原有 TMDB 日期推测作为完整兜底。
                update = trakt_update or self._get_tv_update(min_info)
                if update:
                    items_for_full_recognition.append((key, update))

            elif media_type_value == MediaType.MOVIE.value:
                collection = min_info.get("belongs_to_collection")
                collection_id = (
                    collection.get("id")
                    if isinstance(collection, dict)
                    else None
                )
                try:
                    collection_id = int(collection_id)
                except (TypeError, ValueError):
                    collection_id = None
                if collection_id and collection_id not in _collection_ids:
                    _collection_ids.add(collection_id)
                    items_for_full_recognition.append((key, None))

        logger.info(f"发现 {len(items_for_full_recognition)} 个有价值的新条目。")

        if not items_for_full_recognition:
            return

        for (mtype, tmdbid), update in items_for_full_recognition:

            try:
                mediainfo = self.chain.recognize_media(
                    mtype=MediaType(mtype),
                    tmdbid=tmdbid,
                )
            except Exception as exc:
                logger.warning(
                    f"识别媒体失败 ({mtype} {tmdbid})，跳过该条目：{exc}"
                )
                continue
            if not mediainfo:
                continue

            try:
                if mediainfo.type == MediaType.MOVIE:
                    self._handle_movie(mediainfo, collections)
                else:
                    self._handle_tv_show(mediainfo, update)
            except Exception as exc:
                logger.warning(
                    f"处理媒体续作信息失败 ({mtype} {tmdbid})，跳过该条目：{exc}",
                    exc_info=True,
                )

    def _handle_movie(self, mediainfo: MediaInfo, collections: dict):
        """处理电影逻辑"""
        collection_id, collection_name = self._get_collection_id(mediainfo)
        if not collection_id:
            return

        if str(collection_id) not in collections:
            collections[str(collection_id)] = {"follow_up": True, "name": collection_name}
            logger.info(f"{mediainfo.tmdb_id} {mediainfo.title_year} 添加至系列合集 {collection_id} {collection_name}")

    def _handle_tv_show(self, mediainfo: MediaInfo, update: Optional[dict] = None):
        """处理电视剧逻辑"""
        if not isinstance(update, dict) or not update:
            fallback_update = getattr(mediainfo, "next_episode_to_air", None)
            update = fallback_update if isinstance(fallback_update, dict) else {}
        event_key = update.get("_followup_event_key")

        air_date = update.get("air_date")
        if not isinstance(air_date, str) or not air_date.strip():
            logger.info(
                f"{getattr(mediainfo, 'tmdb_id', '')} "
                f"{getattr(mediainfo, 'title_year', '')} 没有新集或播出日期"
            )
            return

        # 获取季号和集号
        try:
            default_season = int(getattr(mediainfo, "number_of_seasons", 1) or 1)
        except (TypeError, ValueError):
            default_season = 1
        if default_season <= 0:
            default_season = 1
        try:
            season_number = int(update.get("season_number") or default_season)
        except (TypeError, ValueError):
            season_number = default_season
        if season_number <= 0:
            season_number = default_season
        try:
            episode_number = int(update.get("episode_number") or 1)
        except (TypeError, ValueError):
            episode_number = 1
        if episode_number <= 0:
            episode_number = 1

        if self._is_trakt_event_recorded(event_key):
            logger.debug(
                f"{mediainfo.title_year} {event_key} 已处理，跳过重复日历事件"
            )
            return

        if self._has_subscription(mediainfo, season_number):
            self.del_data(
                self.build_key(
                    getattr(mediainfo.type, "value", mediainfo.type),
                    mediainfo.tmdb_id,
                )
            )
            self._record_trakt_event(update, "already_subscribed")
            logger.info(
                f"{getattr(mediainfo, 'title_year', '')} {season_number=}: "
                "已存在订阅或订阅历史，跳过重复添加"
            )
            return

        # 补零格式化
        season_number_str = f"S{season_number:02d}"
        episode_number_str = f"E{episode_number:02d}"

        is_upcoming = update.get("_followup_status") == "upcoming"
        status_text = "即将播出" if is_upcoming else "日历已更新"
        msg_title = (
            f"🆕 {getattr(mediainfo, 'title_year', '')} "
            f"{season_number_str}{episode_number_str} {status_text}"
        )
        msg_text = (
            f"🎬 标题：{update.get('name') or '暂无标题'}\n"
            f"📅 播出日期：{air_date[:10]}\n"
            f"👉 是否订阅该系列的最新作品？\n"
        )

        if self._get_config().auto_subscribe:
            data = self.clean_media_info(mediainfo, season=season_number)
            # 先记录处理中状态，避免两个调度任务同时对同一日历事件重复调用
            # SubscribeChain；无论成功或失败均保留单事件结果供排查。
            self._record_trakt_event(update, "processing")
            if self._auto_subscribe(mediainfo, data):
                self._record_trakt_event(update, "subscribed")
                return
            self._record_trakt_event(update, "failed")
            logger.error(
                f"{mediainfo.title_year} {season_number_str} 自动订阅失败，保留该 Trakt 事件供下一轮重试"
            )
            return

        self._send_menu_message(mediainfo, msg_title, msg_text, season=season_number)
        self._record_trakt_event(update, "notified")

    def collection_follow_up(self, collections: dict[str, dict], ignore: set[tuple]):
        from app.chain.tmdb import TmdbChain

        tmdbchain = TmdbChain()
        collections = collections if isinstance(collections, dict) else {}
        if not isinstance(ignore, set):
            ignore = set(ignore or [])
        config = self._get_config()

        logger.info(f"开始检查 {len(collections)} 个电影合集...")
        for raw_collection_id, followinfo in list(collections.items()):
            if not isinstance(followinfo, dict) or not followinfo.get("follow_up"):
                continue

            try:
                collection_id = int(raw_collection_id)
            except (TypeError, ValueError):
                logger.warning(f"跳过无效电影合集 ID：{raw_collection_id}")
                continue

            try:
                collection_info = tmdbchain.tmdb_collection(collection_id=collection_id)
            except Exception as exc:
                logger.warning(f"获取电影合集 {collection_id} 失败：{exc}")
                continue

            if not isinstance(collection_info, (list, tuple)):
                continue
            collection_info = [
                part
                for part in collection_info
                if part
                and getattr(part, "tmdb_id", None)
                and getattr(part, "type", None)
            ]
            if not collection_info:
                continue

            def release_date(part) -> str:
                return str(getattr(part, "release_date", None) or "0000-00-00")

            latest_part = max(collection_info, key=release_date)
            media_type = getattr(latest_part, "type", None)
            media_type_value = getattr(media_type, "value", media_type)
            try:
                tmdbid = int(getattr(latest_part, "tmdb_id"))
            except (TypeError, ValueError):
                logger.warning(f"电影合集 {collection_id} 最新条目缺少有效 TMDB ID")
                continue

            latest_release_date = str(
                followinfo.get("latest_release_date") or "0000-00-00"
            )
            latest_release = release_date(latest_part)
            latest_air_date = followinfo.get("air_date")

            if latest_release > latest_release_date:
                # 更新系列信息，同时过滤掉历史脏条目。
                parts = []
                for part in collection_info:
                    part_type = getattr(getattr(part, "type", None), "value", getattr(part, "type", None))
                    part_id = getattr(part, "tmdb_id", None)
                    if not part_type or part_id is None:
                        continue
                    try:
                        normalized_part_id = int(part_id)
                    except (TypeError, ValueError):
                        continue
                    if normalized_part_id > 0:
                        parts.append(self.build_key(part_type, normalized_part_id))
                followinfo["parts"] = parts
                followinfo["latest_release_date"] = latest_release

            try:
                still_tracking = self._should_track_media(latest_part)
            except Exception as exc:
                logger.warning(f"判断电影合集 {collection_id} 跟进状态失败：{exc}")
                still_tracking = False
            if not still_tracking:
                followinfo["follow_up"] = False

            if latest_air_date and not self.is_date_in_range(
                latest_air_date,
                threshold_days=config.after_days,
            ):
                logger.info(
                    f"{followinfo.get('name') or collection_id} 没有新的系列电影上映"
                )
                continue

            if not followinfo.get("follow_up") or (
                media_type_value,
                tmdbid,
            ) in ignore:
                continue

            # 获取数字发行日期。
            next_air_date = None
            release_message = "📼 暂无数字发行信息"
            if media_type == MediaType.MOVIE or media_type_value == MediaType.MOVIE.value:
                try:
                    next_air_date, release_message = self.find_earliest_date(tmdbid)
                except Exception as exc:
                    logger.warning(f"获取 {tmdbid} 发行日期失败：{exc}")
                    continue
                followinfo["air_date"] = next_air_date

            if next_air_date is None or not self.is_date_in_range(
                next_air_date,
                threshold_days=config.after_days,
            ):
                logger.info(
                    f"{getattr(latest_part, 'title', tmdbid)} 非院线发行日期: "
                    f"{next_air_date if next_air_date else '暂无'}，不符合提醒条件"
                )
                continue

            title_year = getattr(latest_part, "title_year", None) or getattr(
                latest_part, "title", tmdbid
            )
            msg_title = f"🆕 {followinfo.get('name') or title_year} 有新的电影即将上线！"
            next_air_date_text = str(next_air_date)
            msg_text = (
                f"🎬 最新电影：{title_year}\n"
                f"{release_message or '📼 暂无数字发行信息'}\n"
                f"📅 日期：{next_air_date_text[:10]}\n\n"
                f"👉 是否订阅该系列的最新作品？"
            )

            if config.auto_subscribe:
                try:
                    data = self.clean_media_info(latest_part)
                    if self._auto_subscribe(latest_part, data):
                        continue
                except Exception as exc:
                    logger.warning(f"{title_year} 自动订阅处理异常：{exc}", exc_info=True)
                logger.error(f"{title_year} 自动订阅失败，保留后续检查机会")
                continue

            try:
                self._send_menu_message(latest_part, msg_title, msg_text)
            except Exception as exc:
                logger.warning(f"{title_year} 发送续作提醒失败：{exc}", exc_info=True)

    def _get_collection_id(self, mediainfo: MediaInfo) -> tuple[Optional[int], Optional[str]]:
        """获取媒体的合集ID"""
        tmdb_info = getattr(mediainfo, "tmdb_info", None)
        collection = tmdb_info.get("belongs_to_collection") if isinstance(tmdb_info, dict) else None
        if not isinstance(collection, dict):
            logger.warning(
                f"{getattr(mediainfo, 'tmdb_id', '')} "
                f"{getattr(mediainfo, 'title_year', '')} 未获取到所属合集信息"
            )
            return None, None

        try:
            collection_id = int(collection.get("id"))
        except (TypeError, ValueError):
            collection_id = None
        if not collection_id:
            logger.warning(
                f"{getattr(mediainfo, 'tmdb_id', '')} "
                f"{getattr(mediainfo, 'title_year', '')} 未获取到所属合集ID, 等待下次检查"
            )

        name = collection.get("name")
        return collection_id, name.strip() if isinstance(name, str) and name.strip() else None

    def _should_track_media(self, mediainfo: MediaInfo) -> bool:
        """判断是否在跟进时间范围内"""
        config = self._get_config()
        air_date = getattr(mediainfo, "last_air_date", None) or getattr(
            mediainfo, "release_date", None
        )
        if not air_date or not self.is_date_in_range(
            air_date,
            datetime.now(),
            365 * config.threshold_years,
        ):
            logger.info(
                f"{getattr(mediainfo, 'title_year', '')} 已超过设定年限: "
                f"{config.threshold_years} 年，不再跟进"
            )
            return False

        return True

    def find_earliest_date(self, tmdbid: int):
        try:
            results = TmdbApi().movie.release_dates(tmdbid) or []
        except Exception as exc:
            logger.warning(f"获取 TMDB 发行日期失败 ({tmdbid})：{exc}")
            return None, self.movie_release_info("", None, None)
        if not isinstance(results, (list, tuple)):
            results = []
        _release_date, iso_3166_1, note, _type = "9999-12-31T23:59:59.999Z", "", "", 4
        for result in results:
            if not isinstance(result, dict):
                continue
            release_dates = result.get("release_dates") or []
            if not isinstance(release_dates, (list, tuple)):
                continue
            for release in release_dates:
                if not isinstance(release, dict):
                    continue
                try:
                    release_type = int(release.get("type") or 0)
                except (TypeError, ValueError):
                    continue
                release_date = release.get("release_date")
                if (
                    release_type > 3
                    and isinstance(release_date, str)
                    and release_date
                    and release_date < _release_date
                ):
                    _release_date = release_date
                    iso_3166_1 = result.get("iso_3166_1") or ""
                    note = release.get("note")
                    _type = release_type
        return _release_date if _release_date != "9999-12-31T23:59:59.999Z" else None, self.movie_release_info(iso_3166_1, note, _type)

    def _need_follow_up(self, ignore: set[tuple[str, int]], collections: dict[str, dict]) -> set[tuple[str, int]]:
        config = self._get_config()
        normalized_ignore = {
            key
            for key in (ignore or set())
            if isinstance(key, tuple)
            and len(key) == 2
            and isinstance(key[0], str)
            and isinstance(key[1], int)
        }
        # 保留调用方传入 set 的对象身份，确保本轮新增忽略项也能被
        # collection_follow_up() 看到；旧版这里重新绑定局部变量会丢失更新。
        if isinstance(ignore, set):
            ignore.clear()
            ignore.update(normalized_ignore)
        else:
            ignore = normalized_ignore
        collections = collections if isinstance(collections, dict) else {}

        # 订阅
        subscriptions = {
            (getattr(sub, "type", None), getattr(sub, "tmdbid", None))
            for sub in (SubscribeOper().list() or [])
            if isinstance(getattr(sub, "type", None), str)
            and isinstance(getattr(sub, "tmdbid", None), int)
            and getattr(sub, "tmdbid", 0) > 0
        }
        # 已发送跟进通知
        notified_items = {
            key
            for data in (self.get_data() or [])
            if (key := self.parse_key(getattr(data, "key", None)))
        }

        # 移除已订阅
        # 自动模式按季判断订阅状态，不能用整部剧的 key 清掉新季待处理记录。
        already_subscribed_items = (
            (ignore | notified_items) & subscriptions
            if not config.auto_subscribe
            else ignore & subscriptions
        )
        if already_subscribed_items:
            logger.debug(f"清理 {len(already_subscribed_items)} 个已订阅的忽略项")
            for item in already_subscribed_items:
                ignore.discard(item)
                self.del_data(self.build_key(*item))
            self.save_ignore_keys(ignore)

        # 手动模式排除已订阅和已发送未处理条目；自动模式保留这些条目，
        # 由 _handle_tv_show() 按季判断，才能识别同一剧的新季。
        if not config.auto_subscribe:
            ignore |= subscriptions | notified_items

        # 检索排除包含合集中的条目
        excluded_items = {
            _key
            for collection in collections.values()
            if isinstance(collection, dict)
            and isinstance(parts := collection.get("parts"), (list, tuple, set))
            for k in parts
            if (_key := self.parse_key(k))
        }.union(ignore)

        # 媒体服务器
        serveritems = self.get_media_server_items(exclude=excluded_items)
        serveritems = serveritems if isinstance(serveritems, set) else set()
        subscribehis = set()
        if config.check_sub_history:
            subscribehis = {
                (getattr(sub, "type", None), getattr(sub, "tmdbid", None))
                for sub in (self.get_subscribe_history(exclude=excluded_items) or [])
                if isinstance(getattr(sub, "type", None), str)
                and isinstance(getattr(sub, "tmdbid", None), int)
                and getattr(sub, "tmdbid", 0) > 0
            }
        if config.auto_subscribe:
            return serveritems.union(subscribehis).union(notified_items)
        return serveritems.union(subscribehis)

    @eventmanager.register(EventType.MessageAction)
    def message_action(self, event: Event):
        """
        处理消息按钮回调
        """
        event_data = getattr(event, "event_data", {})
        if not isinstance(event_data, dict) or not event_data:
            return
        if event_data.get("plugin_id") != self.__class__.__name__:
            return

        text = event_data.get("text")
        if not isinstance(text, str):
            return
        text_parts = text.split("|", 1)
        if len(text_parts) < 2:
            return
        action, _key = text_parts

        handler_map = {"add": self._handle_add, "ignore": self._handle_ignore}
        if handler := handler_map.get(action):
            handler(event_data.get("channel"), event_data.get("source"), event_data.get("userid"),
                    event_data.get("original_message_id"), event_data.get("original_chat_id"), _key)

    def _send_menu_message(
        self,
        mediainfo: MediaInfo,
        title: str,
        text: str,
        season: Optional[int] = None,
    ):
        """
        发送主菜单
        """
        _key = self.build_key(mediainfo.type.value, mediainfo.tmdb_id)
        buttons = [[
            {"text": "📼 追加订阅", "callback_data": f"[PLUGIN]{self.__class__.__name__}|add|{_key}"},
            {"text": "💤 不再提醒", "callback_data": f"[PLUGIN]{self.__class__.__name__}|ignore|{_key}"}
        ]]
        self.post_message(title=title, text=text, mtype=NotificationType.Plugin, buttons=buttons)
        self.save_data(_key, self.clean_media_info(mediainfo, season=season))

    def _has_subscription(self, mediainfo: MediaInfo, season: Optional[int] = None) -> bool:
        """
        判断目标媒体/季是否已有活动订阅或已完成订阅历史。

        同一剧的旧季不能阻止新季自动跟进；只有目标季（或未指定季的
        全剧订阅）才视为已处理。
        """
        tmdb_id = getattr(mediainfo, "tmdb_id", None)
        media_type = getattr(mediainfo, "type", None)
        target_type = getattr(media_type, "value", media_type)
        if not mediainfo or not tmdb_id or not isinstance(target_type, str):
            return False

        try:
            subscriptions = SubscribeOper().list_by_tmdbid(tmdb_id) or []
        except Exception as exc:
            logger.warning(f"读取 {tmdb_id} 活动订阅失败：{exc}")
            subscriptions = []
        if any(
            getattr(sub, "type", None) == target_type
            and (
                target_type == MediaType.MOVIE.value
                or season is None
                or getattr(sub, "season", None) in (None, season)
            )
            for sub in subscriptions
        ):
            return True

        history = self.get_subscribe_history(tmdbid=tmdb_id, type=media_type) or []
        return any(
            getattr(item, "type", None) == target_type
            and (
                target_type == MediaType.MOVIE.value
                or season is None
                or getattr(item, "season", None) in (None, season)
            )
            for item in history
        )

    def _auto_subscribe(self, mediainfo: MediaInfo, data: dict) -> bool:
        """通过 MoviePilot 原生订阅链添加续作，成功后清理待处理记录。"""
        if not data or not mediainfo:
            return False

        key = self.build_key(mediainfo.type.value, mediainfo.tmdb_id)
        try:
            sid, msg = SubscribeChain().add(
                **data,
                username=self.plugin_name,
                message=True,
            )
        except Exception as exc:
            logger.error(f"{mediainfo.title_year} 自动添加订阅异常: {exc}", exc_info=True)
            self.save_data(key, data)
            return False

        if sid:
            self.del_data(key)
            logger.info(f"{mediainfo.title_year} 自动订阅成功: {msg}")
            return True

        self.save_data(key, data)
        logger.error(f"{mediainfo.title_year} 自动添加订阅失败: {msg}")
        return False

    def _handle_add(self, channel, source, userid, original_message_id, original_chat_id, _key: str):
        data = self.get_data(_key) or {}
        if not data:
            msg, buttons = "信息已过时", None
        else:
            try:
                sid, msg = SubscribeChain().add(**data, username=self.plugin_name)
            except Exception as exc:
                logger.error(f"手动添加续作订阅异常: {exc}", exc_info=True)
                sid, msg = None, str(exc)
            if sid:
                self.del_data(_key)
                try:
                    self.chain.delete_message(
                        channel,
                        source,
                        original_message_id,
                        original_chat_id,
                    )
                except Exception as exc:
                    logger.warning(f"删除续作提醒消息失败: {exc}")
                return
            buttons = [[
                {"text": "📼 重试", "callback_data": f"[PLUGIN]{self.__class__.__name__}|add|{_key}"},
                {"text": "💤 忽略", "callback_data": f"[PLUGIN]{self.__class__.__name__}|ignore|{_key}"}
            ]]
        self.post_message(channel=channel, title="添加订阅失败", text=f"原因: {msg}", userid=userid, buttons=buttons,
                          original_message_id=original_message_id, original_chat_id=original_chat_id)

    def _handle_ignore(self, channel, source, userid, original_message_id, original_chat_id, _key):

        data = self.get_data(_key) or {}
        self.del_data(_key)
        self.update_ignore_keys(_key)

        title = data.get("title") if isinstance(data, dict) else None
        year = data.get("year") if isinstance(data, dict) else None
        ignored_title = (
            f"已忽略订阅 {title} ({year})"
            if title and year
            else f"已忽略订阅 {_key}"
        )
        self.post_message(
            channel=channel,
            source=source,
            title=ignored_title,
            userid=userid,
            original_message_id=original_message_id,
            original_chat_id=original_chat_id
        )

    def get_media_server_items(
        self,
        exclude: Optional[set[tuple]] = None,
    ) -> set[tuple[str, int]]:
        # 获取所有媒体服务器
        mediaservers = ServiceConfigHelper.get_mediaserver_configs() or []
        if not mediaservers:
            return set()
        items = set()
        serverchain = MediaServerChain()
        selected_libraries = {
            str(value)
            for value in (self._get_config().libraries or [])
            if value is not None and str(value).strip()
        }
        excluded = exclude if isinstance(exclude, set) else set(exclude or [])
        # 遍历媒体服务器
        for mediaserver in mediaservers:
            if not mediaserver:
                continue
            if not getattr(mediaserver, "enabled", False):
                continue
            server_name = getattr(mediaserver, "name", None)
            if not server_name:
                continue
            try:
                libraries = serverchain.librarys(server_name) or []
            except Exception as exc:
                logger.warning(f"获取 {server_name} 媒体库失败：{exc}")
                continue
            if not libraries:
                continue
            for library in libraries:
                library_id = getattr(library, "id", None)
                if library_id is None or str(library_id) not in selected_libraries:
                    continue
                library_name = getattr(library, "name", "未命名媒体库")
                logger.info(f"正在获取 {server_name} 媒体库 {library_name} ...")

                try:
                    library_items = serverchain.items(
                        server=server_name,
                        library_id=library_id,
                    ) or []
                except Exception as exc:
                    logger.warning(
                        f"获取 {server_name}/{library_name} 媒体项失败：{exc}"
                    )
                    continue

                for item in library_items:
                    tmdbid_value = getattr(item, "tmdbid", None)
                    if not item or not tmdbid_value:
                        continue
                    # 类型
                    item_type = (
                        MediaType.TV.value
                        if getattr(item, "item_type", None) in ["Series", "show"]
                        else MediaType.MOVIE.value
                    )
                    try:
                        tmdbid = int(tmdbid_value)
                    except (TypeError, ValueError):
                        continue
                    # 插入数据
                    if (_key := (item_type, tmdbid)) not in excluded:
                        items.add(_key)
        return items

    def clean_media_info(self, mediainfo: MediaInfo, season: Optional[int] = None) -> dict:
        """
        清洗 mediainfo 对象，仅保留关键字段用于存储或传输。

        ``season`` 是本次命中的目标季。电视剧的 ``number_of_seasons``
        代表整部剧当前季数，不能代替日历命中的季号，否则历史集数和
        ``start_episode`` 会被计算到错误的季。
        """
        if not mediainfo:
            return {}
        target_season = (
            season
            if season is not None
            else getattr(mediainfo, "number_of_seasons", None)
        )
        try:
            target_season = int(target_season) if target_season is not None else None
        except (TypeError, ValueError):
            target_season = None
        # 查询订阅历史
        history = self.get_subscribe_history(
            tmdbid=getattr(mediainfo, "tmdb_id", None),
            type=getattr(mediainfo, "type", None),
        ) or []
        total_episode = next(
            (
                int(item.total_episode or 0)
                for item in history
                if getattr(item, "season", None) == target_season
                and str(getattr(item, "total_episode", "")).isdigit()
            ),
            0,
        )
        return {
            'title': getattr(mediainfo, "title", ""),
            'year': getattr(mediainfo, "year", ""),
            "tmdbid": getattr(mediainfo, "tmdb_id", None),
            "doubanid": getattr(mediainfo, "douban_id", None),
            "bangumiid": getattr(mediainfo, "bangumi_id", None),
            "episode_group": getattr(mediainfo, "episode_group", None),
            "season": target_season,
            "start_episode": total_episode + 1 if total_episode else 0,
            }

    def get_ignore_keys(self) -> set[tuple[str, int]]:
        _keys = self.get_data("ignore_keys") or []
        if not isinstance(_keys, (list, tuple, set)):
            return set()
        return {
            parsed
            for key in _keys
            if (parsed := self.parse_key(key))
        }

    def save_ignore_keys(self, keys: Optional[set[tuple[str, int]]]):
        normalized = {
            key
            for key in (keys or set())
            if isinstance(key, tuple)
            and len(key) == 2
            and isinstance(key[0], str)
            and isinstance(key[1], int)
            and key[1] > 0
        }
        _keys = sorted(self.build_key(*key) for key in normalized)
        self.save_data("ignore_keys", _keys)

    def update_ignore_keys(self, key: Union[tuple[str, int], str]):
        """将 key 添加到忽略列表中"""
        if isinstance(key, str):
            key = self.parse_key(key)
            if not key:
                return
        if (
            not isinstance(key, tuple)
            or len(key) != 2
            or not isinstance(key[0], str)
            or not isinstance(key[1], int)
            or key[1] <= 0
        ):
            return
        self.save_ignore_keys(self.get_ignore_keys().union({key}))

    def get_collections(self) -> dict[str, dict]:
        collections = self.get_data("collections") or {}
        return collections if isinstance(collections, dict) else {}

    def save_collections(self, collections: dict):
        self.save_data("collections", collections)

    @db_query
    def get_subscribe_history(
        self,
        db: Session = None,
        tmdbid: int = None,
        type: MediaType = None,
        exclude: Optional[set[tuple]] = None,
    ) -> list[SubscribeHistory]:
        query = db.query(SubscribeHistory)
        conditions = []
        if tmdbid is not None:
            conditions.append(SubscribeHistory.tmdbid == tmdbid)
        if type:
            type_value = getattr(type, "value", type)
            if isinstance(type_value, str):
                conditions.append(SubscribeHistory.type == type_value)
        normalized_exclude = {
            key
            for key in (exclude or set())
            if isinstance(key, tuple) and len(key) == 2
        }
        if normalized_exclude:
            conditions.append(
                tuple_(
                    SubscribeHistory.type,
                    SubscribeHistory.tmdbid,
                ).notin_(normalized_exclude)
            )
        try:
            return query.filter(*conditions).all()
        except Exception as e:
            logger.error(f"获取订阅历史失败: {str(e)}")
            return []

    @db_query
    def get_followup_subscription_history(
        self,
        db: Session = None,
        limit: int = 60,
    ) -> list[SubscribeHistory]:
        """读取本插件创建的订阅历史，用于详情页展示。"""
        try:
            safe_limit = max(1, min(int(limit), 200))
        except (TypeError, ValueError):
            safe_limit = 60
        try:
            return (
                db.query(SubscribeHistory)
                .filter(SubscribeHistory.username.in_(self.subscription_owner_names))
                .order_by(SubscribeHistory.date.desc(), SubscribeHistory.id.desc())
                .limit(safe_limit)
                .all()
            )
        except Exception as exc:
            logger.warning(f"读取订阅管理订阅历史失败：{exc}")
            return []

    @staticmethod
    def build_key(mtype: str, tmdbid: int) -> str:
        return f"{mtype}.{tmdbid}"

    @staticmethod
    def parse_key(key_str: str) -> Optional[tuple[str, int]]:
        if not isinstance(key_str, str):
            return None
        try:
            type_str, tmdbid_str = key_str.split(".", 1)
            if type_str not in {MediaType.MOVIE.value, MediaType.TV.value}:
                return None
            tmdbid = int(tmdbid_str)
            if tmdbid <= 0:
                return None
            return type_str, tmdbid
        except (TypeError, ValueError):
            return None
        except Exception as e:
            logger.warn(f"解析key失败: {key_str}, 错误: {str(e)}")
            return None

    @staticmethod
    def movie_release_info(iso_code: str, note, type_id) -> str:
        type_name = {4: "数字发行", 5: "实体发行", 6: "电视播放"}
        iso_to_country_cn = {"US": "美国", "GB": "英国", "FR": "法国", "DE": "德国", "JP": "日本", "KR": "韩国", "CN": "中国", "HK": "中国香港", "TW": "中国台湾"}
        normalized_iso = str(iso_code or "").strip().upper()
        try:
            normalized_type = int(type_id)
        except (TypeError, ValueError):
            normalized_type = 0
        return (
            f"🌍 地区：{iso_to_country_cn.get(normalized_iso, '未知地区')}\n"
            f"📼 渠道：{note or '未知'}\n"
            f"🏷️ 类型：{type_name.get(normalized_type, '未知发行渠道')}"
        )

    @staticmethod
    def is_date_in_range(air_date: Union[datetime, str], reference_date: Optional[Union[datetime, str]] = None, threshold_days: int = 2) -> bool:
        """
        两个日期接近或在未来指定天数内

        :param air_date: 目标日期
        :param reference_date: 参考日期
        :param threshold_days: 阈值天数
        :return: bool

        只传入 target_date 时，判断是否在未来 threshold_days 天内
        传入 target_date 和 reference_date 时，判断两个日期是否接近
        """
        def parse_date(value: Union[datetime, str]) -> Optional[Any]:
            if isinstance(value, datetime):
                return value.date()
            if not isinstance(value, str) or not value.strip():
                return None
            value = value.strip()
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
            except ValueError:
                try:
                    return datetime.strptime(value[:10], "%Y-%m-%d").date()
                except ValueError:
                    return None

        try:
            date1 = parse_date(air_date)
            if date1 is None:
                return False
            try:
                threshold = int(threshold_days)
            except (TypeError, ValueError):
                return False
            if threshold < 0:
                return False

            # 单日期模式：是否在未来threshold_days内
            if reference_date is None:
                today = datetime.now().date()
                delta = (date1 - today).days
                return 0 <= delta <= threshold

            # 双日期模式：两个日期是否接近
            date2 = parse_date(reference_date)
            if date2 is None:
                return False
            # 天数差
            delta = (date1 - date2).days
            return abs(delta) <= threshold

        except (ValueError, TypeError, OverflowError) as e:
            logger.error(f"日期格式错误: {str(e)}")
            return False
