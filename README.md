# MoviePilot 订阅管理

`订阅管理` 是一个 MoviePilot v2 插件，将续作自动订阅、Trakt 个人日历提醒和转移记录清理合并到一个配置入口。

这个仓库是 `i-kirito` 的独立维护版，当前版本为 `1.0.0`。它会迁移旧 `FollowUp` 与 `TransferCleaner` 的配置和插件数据，并保留旧订阅历史归属。

- 顶部显示插件、Trakt 日历和转移清理状态；
- 用概览卡片展示当前订阅、历史记录、待处理提醒和 Trakt 事件；
- 将运行周期、提醒窗口、媒体库范围和清理模式整理成运行概况；
- 自动订阅记录与待处理提醒采用单屏双列预览；
- 合并原转移记录清理的监控、路径映射、延迟删除、模拟运行和失败记录处理能力。

## 目录

```text
package.v2.json
plugins.v2/subscriptionmanager/__init__.py
icons/subscriptionmanager.png
```

## 配置要点

- `启用插件`：开启每日或自定义 cron 检查。
- `命中自动订阅`：命中续作时自动创建订阅；关闭后仍可保留检查与提醒。
- `检查订阅历史`：把历史订阅纳入续作扫描范围。
- `接入 CookieCloud Trakt 个人剧集日历`：只读取 MoviePilot 全局 CookieCloud 中的 `trakt.tv` Cookie，并仅处理能够确认 TMDB ID 的事件。
- `选择媒体库`：为空时扫描所有已配置媒体库。

## 许可与来源

本插件代码基于 MoviePilot 社区插件实现并保留 GPL-3.0 许可。请同时保留本仓库的 [LICENSE](LICENSE) 文件。
