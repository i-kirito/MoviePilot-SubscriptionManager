<script setup>
import { computed, reactive, ref, watch } from 'vue'

const props = defineProps({
  initialConfig: { type: Object, default: () => ({}) },
  api: { type: Object, default: null },
})

const emit = defineEmits(['save', 'close', 'layout'])
emit('layout', { maxWidth: '78rem' })

const defaults = {
  enabled: false,
  auto_subscribe: true,
  after_days: 2,
  threshold_years: 15,
  cron: '',
  onlyonce: false,
  check_sub_history: true,
  libraries: [],
  trakt_calendar_enabled: false,
  trakt_calendar_days: 7,
  notify: true,
  dry_run: true,
  delay_enabled: false,
  delay_seconds: 10,
  monitor_dirs: '',
  path_mappings: '',
  exclude_dirs: '',
  exclude_keywords: '',
  clean_dirs: '',
  run_once: false,
  retransfer_once: false,
  retransfer_dirs: '',
  retransfer_cron: '',
  clean_failed: false,
}

const normalize = (source) => {
  const incoming = source && typeof source === 'object' ? source : {}
  const result = { ...defaults, ...incoming }
  result.libraries = Array.isArray(incoming.libraries) ? [...incoming.libraries] : [...defaults.libraries]
  return result
}

const draft = reactive(normalize(props.initialConfig))
const baseline = ref(normalize(props.initialConfig))
const activeGroup = ref('global')
const headerScrolled = ref(false)
const mobileGroupSheet = ref(false)
const headerSentinel = ref(null)

const groups = [
  {
    key: 'global',
    title: '全局运行',
    icon: 'mdi-tune-variant',
    summary: '插件开关、订阅扫描和公共执行周期',
    sections: [
      {
        title: '运行状态',
        fields: [
          ['enabled', '启用插件', '开启后启用续作订阅、Trakt 提醒和转移清理。', 'switch'],
          ['auto_subscribe', '命中自动订阅', '发现新的续作集数后自动添加订阅。', 'switch'],
          ['check_sub_history', '检查订阅历史', '根据历史订阅记录识别需要继续追更的条目。', 'switch'],
          ['onlyonce', '立即运行一次', '保存后立即执行一次续作订阅检查，完成后自动复位。', 'switch'],
        ],
      },
      {
        title: '公共周期',
        fields: [
          ['cron', '执行周期', '留空时按默认的每日周期运行，也可填写 5 位 cron。', 'cron'],
          ['after_days', '提前提醒', '在预计播出前多少天开始处理提醒。', 'number', '天'],
          ['threshold_years', '检查年限', '播出或上映时间超出年限的条目不再检查。', 'number', '年'],
        ],
      },
    ],
  },
  {
    key: 'trakt',
    title: 'Trakt 日历',
    icon: 'mdi-calendar-sync-outline',
    summary: '读取 CookieCloud 中的个人剧集日历并生成提醒',
    sections: [
      {
        title: '个人剧集日历',
        fields: [
          ['trakt_calendar_enabled', '接入 Trakt 日历', '只读取 MoviePilot 全局 CookieCloud 中的 trakt.tv Cookie。', 'switch'],
          ['trakt_calendar_days', '日历拉取天数', '用于筛选 Trakt 日历页面范围，提醒窗口仍由“提前提醒”控制。', 'number', '天'],
        ],
      },
    ],
  },
  {
    key: 'subscriptions',
    title: '订阅扫描',
    icon: 'mdi-magnify-scan',
    summary: '选择参与扫描的媒体库，保留原有订阅历史归属',
    sections: [
      {
        title: '媒体库范围',
        fields: [
          ['libraries', '选择媒体库', '留空表示检查全部媒体库；可输入媒体库 ID 进行补充。', 'combo'],
        ],
      },
    ],
  },
  {
    key: 'transfer',
    title: '转移清理',
    icon: 'mdi-file-sync-outline',
    summary: '监控文件移动或删除并清理对应的转移历史',
    sections: [
      {
        title: '清理策略',
        fields: [
          ['notify', '发送清理通知', '清理转移记录后发送通知。', 'switch'],
          ['dry_run', '模拟运行', '只记录将要清理的记录，不执行删除。', 'switch'],
          ['delay_enabled', '延迟删除', '文件事件发生后等待一段时间再处理。', 'switch'],
          ['delay_seconds', '延迟秒数', '延迟删除的等待时间。', 'number', '秒'],
          ['run_once', '立即清理一次', '保存后立即扫描并清理一次，完成后自动复位。', 'switch'],
          ['clean_failed', '清理假失败记录', '清理历史中的假失败转移记录。', 'switch'],
          ['retransfer_cron', '清理定时周期', '例如 0 */2 * * *，留空则不单独执行定时清理。', 'cron'],
        ],
      },
      {
        title: '监控目录',
        fields: [
          ['monitor_dirs', '监控目录', '每行一个，例如 /media/待上传。', 'textarea'],
          ['path_mappings', '路径映射', '每行一个，格式为本地路径:存储路径。', 'textarea'],
          ['retransfer_dirs', '重新整理检测目录', '每行一个，需要重新整理检测的目录。', 'textarea'],
        ],
      },
      {
        title: '排除规则',
        fields: [
          ['exclude_dirs', '排除目录', '命中的目录不会触发清理。', 'textarea'],
          ['exclude_keywords', '排除关键词', '每行一个，命中关键词的路径会跳过。', 'textarea'],
          ['clean_dirs', '清理目录', '可选的定期清理目录，每行一个。', 'textarea'],
          ['retransfer_once', '立即重新整理一次', '保存后执行一次重新整理检测，完成后自动复位。', 'switch'],
        ],
      },
    ],
  },
]

const activeGroupMeta = computed(() => groups.find((group) => group.key === activeGroup.value) || groups[0])
const allFields = computed(() => groups.flatMap((group) => group.sections.flatMap((section) => section.fields)))
const changedKeys = computed(() =>
  allFields.value.map((field) => field[0]).filter((key) => JSON.stringify(draft[key]) !== JSON.stringify(baseline.value[key])),
)
const changedCount = computed(() => changedKeys.value.length)
const activeFieldCount = computed(() => allFields.value.filter((field) => field[3] === 'switch' && draft[field[0]]).length)
const transferMode = computed(() => (draft.dry_run ? '模拟运行' : '实际清理'))
const monitorCount = computed(() => String(draft.monitor_dirs || '').split('\n').filter((item) => item.trim()).length)

watch(
  () => props.initialConfig,
  (value) => {
    const next = normalize(value)
    Object.assign(draft, next)
    baseline.value = next
  },
  { deep: true },
)

function setGroup(key) {
  activeGroup.value = key
  mobileGroupSheet.value = false
}

function valueLabel(field) {
  const key = field[0]
  if (field[3] === 'number') return `${draft[key]} ${field[4] || ''}`.trim()
  if (field[3] === 'switch') return draft[key] ? '已开启' : '已关闭'
  return ''
}

function updateNumber(key, event) {
  const value = Number(event)
  if (Number.isFinite(value)) draft[key] = value
}

function cloneDraft() {
  return JSON.parse(JSON.stringify(draft))
}

function saveConfig() {
  const payload = cloneDraft()
  baseline.value = normalize(payload)
  emit('save', payload)
}

function runOnce() {
  const payload = cloneDraft()
  payload.onlyonce = true
  draft.onlyonce = true
  baseline.value = normalize(payload)
  emit('save', payload)
}
</script>

<template>
  <section class="sm-config">
    <form class="sm-config__form" @submit.prevent="saveConfig">
      <div ref="headerSentinel" class="sm-header-sentinel" aria-hidden="true" />
      <header :class="['sm-header', { 'sm-header--scrolled': headerScrolled }]">
        <div class="sm-header__brand">
          <div class="sm-header__logo"><VIcon icon="mdi-calendar-sync-outline" size="27" /></div>
          <div class="sm-header__identity">
            <div class="sm-header__crumbs"><span>MoviePilot</span><VIcon icon="mdi-chevron-right" size="14" /><span>插件</span></div>
            <div class="sm-header__title-row"><h1>订阅管理</h1><VChip color="primary" size="x-small" variant="tonal">i-kirito</VChip></div>
          </div>
        </div>
        <div class="sm-header__actions">
          <VBtn color="primary" variant="tonal" type="button" @click="runOnce"><VIcon icon="mdi-play" start />运行一次</VBtn>
          <VBtn color="primary" variant="flat" type="submit" :disabled="changedCount === 0"><VIcon icon="mdi-content-save" start />保存修改</VBtn>
          <VBtn class="sm-header__close" color="default" variant="outlined" type="button" @click="emit('close')"><VIcon icon="mdi-close" start />关闭</VBtn>
        </div>
      </header>

      <div class="sm-config__body">
        <div class="sm-layout">
          <nav class="sm-group-nav" aria-label="选择配置分组">
            <div class="sm-group-nav__heading">插件设置</div>
            <VList class="sm-group-nav__list" density="compact" nav>
              <VListItem v-for="group in groups" :key="group.key" :active="activeGroup === group.key" :prepend-icon="group.icon" :title="group.title" color="primary" rounded="lg" @click="setGroup(group.key)" />
            </VList>
            <section class="sm-group-nav__help"><strong>关于订阅管理</strong><p>续作订阅、Trakt 日历和转移清理共用一个配置入口。</p></section>
          </nav>

          <main class="sm-field-surface">
            <div class="sm-field-surface__heading">
              <div class="sm-field-surface__heading-copy"><VIcon :icon="activeGroupMeta.icon" color="primary" size="22" /><div><h2>{{ activeGroupMeta.title }}</h2><p>{{ activeGroupMeta.summary }}</p></div></div>
              <VBtn class="sm-mobile-group-action" icon size="small" type="button" variant="tonal" @click="mobileGroupSheet = true"><VIcon icon="mdi-view-list-outline" /></VBtn>
            </div>

            <VAlert class="sm-info" type="info" variant="tonal" density="compact" text="所有功能共用“启用插件”开关；修改后点击保存修改才会生效。" />

            <section v-for="(section, sectionIndex) in activeGroupMeta.sections" :key="section.title" class="sm-field-section">
              <h3>{{ sectionIndex + 1 }}. {{ section.title }}</h3>
              <div class="sm-field-section__rows">
                <div v-for="field in section.fields" :key="field[0]" :class="['sm-field-row', { 'sm-field-row--switch': field[3] === 'switch' }]">
                  <div class="sm-field-row__copy"><div class="sm-field-row__label">{{ field[1] }}</div><p>{{ field[2] }}</p></div>
                  <div class="sm-field-control">
                    <VSwitch v-if="field[3] === 'switch'" v-model="draft[field[0]]" color="primary" density="compact" hide-details :aria-label="field[1]" />
                    <VTextField v-else-if="field[3] === 'number'" :model-value="draft[field[0]]" type="number" density="compact" hide-details variant="outlined" :suffix="field[4]" @update:model-value="updateNumber(field[0], $event)" />
                    <VCronField v-else-if="field[3] === 'cron'" v-model="draft[field[0]]" density="compact" hide-details variant="outlined" />
                    <VTextarea v-else-if="field[3] === 'textarea'" v-model="draft[field[0]]" rows="2" density="compact" hide-details variant="outlined" />
                    <VCombobox v-else-if="field[3] === 'combo'" v-model="draft[field[0]]" :items="draft[field[0]]" multiple chips closable-chips clearable density="compact" hide-details variant="outlined" />
                  </div>
                </div>
              </div>
            </section>
          </main>

          <aside class="sm-preview">
            <div class="sm-preview__title"><VIcon color="primary" icon="mdi-clock-outline" size="20" /><h2>运行节奏</h2></div>
            <ul class="sm-preview__list">
              <li><VIcon icon="mdi-calendar-clock-outline" size="18" /><span>执行周期</span><strong>{{ draft.cron || '默认每日' }}</strong></li>
              <li><VIcon icon="mdi-folder-eye-outline" size="18" /><span>监控目录</span><strong>{{ monitorCount }} 个</strong></li>
              <li><VIcon icon="mdi-delete-sweep-outline" size="18" /><span>清理模式</span><strong>{{ transferMode }}</strong></li>
              <li><VIcon icon="mdi-toggle-switch-outline" size="18" /><span>已启用能力</span><strong>{{ activeFieldCount }} / {{ allFields.length }}</strong></li>
            </ul>
            <section class="sm-change-summary"><div class="sm-preview__title"><VIcon color="warning" icon="mdi-format-list-checks" size="19" /><h3>当前配置</h3></div><p v-if="changedCount === 0">配置与已保存内容一致。</p><p v-else>有 {{ changedCount }} 项修改待保存。</p></section>
          </aside>
        </div>
      </div>
    </form>

    <VBottomSheet v-model="mobileGroupSheet">
      <VCard><VCardTitle>选择配置分组</VCardTitle><VList nav>
        <VListItem v-for="group in groups" :key="group.key" :active="activeGroup === group.key" :prepend-icon="group.icon" :title="group.title" @click="setGroup(group.key)" />
      </VList></VCard>
    </VBottomSheet>
  </section>
</template>

<style scoped>
.sm-config { container-type: inline-size; min-inline-size: 0; color: rgb(var(--v-theme-on-surface)); letter-spacing: 0; }
.sm-config, .sm-config * { box-sizing: border-box; }
.sm-config__form { min-inline-size: 0; }
.sm-header-sentinel { block-size: 1px; margin-block-end: -1px; pointer-events: none; }
.sm-header { position: sticky; z-index: 20; inset-block-start: 0; display: flex; align-items: center; justify-content: space-between; min-block-size: 72px; padding: 10px 16px; border-block-end: 1px solid rgba(var(--v-theme-on-surface), .1); gap: 16px; }
.sm-header--scrolled { background: rgba(var(--v-theme-surface), .82); backdrop-filter: blur(18px); box-shadow: 0 8px 24px rgba(0,0,0,.08); }
.sm-header__brand, .sm-header__title-row, .sm-header__crumbs { display: flex; align-items: center; min-inline-size: 0; }
.sm-header__brand { flex: 1 1 auto; gap: 10px; }
.sm-header__logo { display: grid; flex: 0 0 40px; block-size: 40px; inline-size: 40px; place-items: center; border-radius: 12px; color: rgb(var(--v-theme-primary)); background: rgba(var(--v-theme-primary), .12); }
.sm-header__identity { min-inline-size: 0; }
.sm-header__crumbs { margin-block-end: 3px; color: rgba(var(--v-theme-on-surface), .55); font-size: .6875rem; line-height: 1rem; gap: 2px; }
.sm-header__title-row { gap: 8px; }
.sm-header h1 { margin: 0; font-size: 1.0625rem; font-weight: 700; line-height: 1.4rem; }
.sm-header__actions { display: flex; flex: 0 0 auto; align-items: center; gap: 8px; }
.sm-header__actions :deep(.v-btn) { font-weight: 600; }
.sm-config__body { min-inline-size: 0; padding: 12px; }
.sm-layout { display: grid; min-inline-size: 0; margin-block-start: 12px; gap: 12px; grid-template-areas: 'navigation content preview'; grid-template-columns: 190px minmax(0, 1fr) 230px; }
.sm-group-nav, .sm-preview, .sm-field-section { border: 1px solid rgba(var(--v-theme-on-surface), .12); border-radius: 12px; background: rgba(var(--v-theme-surface), .4); box-shadow: 0 5px 18px rgba(0,0,0,.05); }
.sm-group-nav { display: flex; flex-direction: column; min-inline-size: 0; grid-area: navigation; }
.sm-group-nav__heading { padding: 8px 10px 7px; color: rgba(var(--v-theme-on-surface), .55); font-size: .75rem; font-weight: 600; }
.sm-group-nav__list { flex: 1 1 auto; padding: 0 4px; background: transparent; }
.sm-group-nav__list :deep(.v-list-item) { min-block-size: 44px; margin-block: 3px; }
.sm-group-nav__list :deep(.v-list-item-title) { font-size: .8rem; font-weight: 600; }
.sm-group-nav__list :deep(.v-list-item--active) { color: rgb(var(--v-theme-primary)); background: rgba(var(--v-theme-primary), .1); }
.sm-group-nav__help { padding: 11px; margin: 10px; border: 1px solid rgba(var(--v-theme-on-surface), .1); border-radius: 9px; background: rgba(var(--v-theme-on-surface), .025); }
.sm-group-nav__help strong { display: block; font-size: .78rem; }
.sm-group-nav__help p { margin: 5px 0 0; color: rgba(var(--v-theme-on-surface), .56); font-size: .68rem; line-height: 1rem; }
.sm-field-surface { min-inline-size: 0; grid-area: content; }
.sm-field-surface__heading { display: flex; align-items: flex-start; justify-content: space-between; min-inline-size: 0; padding: 2px 2px 12px; gap: 12px; }
.sm-field-surface__heading-copy { display: flex; align-items: flex-start; gap: 9px; }
.sm-field-surface h2, .sm-preview h2 { margin: 0; font-size: 1rem; font-weight: 700; line-height: 1.25rem; }
.sm-field-surface__heading p { margin: 3px 0 0; color: rgba(var(--v-theme-on-surface), .62); font-size: .75rem; line-height: 1.05rem; }
.sm-info { margin-block-end: 12px; }
.sm-field-section { overflow: hidden; min-inline-size: 0; }
.sm-field-section + .sm-field-section { margin-block-start: 12px; }
.sm-field-section > h3 { padding: 14px 16px 10px; margin: 0; font-size: .9375rem; font-weight: 700; line-height: 1.25rem; }
.sm-field-section__rows { padding-inline: 16px; }
.sm-field-row { display: grid; align-items: start; min-inline-size: 0; padding-block: 12px; border-block-start: 1px solid rgba(var(--v-theme-on-surface), .08); gap: 18px; grid-template-columns: minmax(180px, 1.35fr) minmax(160px, .85fr); }
.sm-field-row--switch { align-items: center; }
.sm-field-row__copy { min-inline-size: 0; }
.sm-field-row__label { color: rgb(var(--v-theme-on-surface)); font-size: .8125rem; font-weight: 600; line-height: 1.15rem; }
.sm-field-row__copy p { margin: 4px 0 0; color: rgba(var(--v-theme-on-surface), .57); font-size: .6875rem; line-height: 1rem; }
.sm-field-control, .sm-field-control :deep(.v-input) { min-inline-size: 0; max-inline-size: 100%; }
.sm-field-control :deep(.v-input) { width: 100%; }
.sm-preview { align-self: start; padding: 14px; grid-area: preview; }
.sm-preview__title { display: flex; align-items: center; gap: 7px; }
.sm-preview__title h3 { margin: 0; font-size: .86rem; font-weight: 700; }
.sm-preview__list { padding: 0; margin: 13px 0 0; list-style: none; }
.sm-preview__list li { display: grid; align-items: center; padding: 11px 0; border-block-start: 1px solid rgba(var(--v-theme-on-surface), .08); gap: 7px; grid-template-columns: 20px 1fr auto; }
.sm-preview__list span { color: rgba(var(--v-theme-on-surface), .62); font-size: .7rem; }
.sm-preview__list strong { max-inline-size: 105px; overflow: hidden; color: rgb(var(--v-theme-on-surface)); font-size: .72rem; text-align: end; text-overflow: ellipsis; white-space: nowrap; }
.sm-change-summary { padding-block-start: 15px; margin-block-start: 8px; border-block-start: 1px solid rgba(var(--v-theme-on-surface), .1); }
.sm-change-summary p { margin: 8px 0 0; color: rgba(var(--v-theme-on-surface), .58); font-size: .7rem; line-height: 1.1rem; }
.sm-mobile-group-action { display: none; }
@container (max-width: 980px) { .sm-layout { grid-template-areas: 'navigation content' 'navigation preview'; grid-template-columns: 178px minmax(0, 1fr); } .sm-preview { margin-block-start: 0; } }
@container (max-width: 720px) { .sm-header { align-items: flex-start; padding: 10px 12px; } .sm-header__actions { gap: 4px; } .sm-header__actions :deep(.v-btn) { min-inline-size: 0; padding-inline: 9px; } .sm-header__close { display: none; } .sm-layout { display: block; margin-block-start: 6px; } .sm-group-nav { display: none; } .sm-mobile-group-action { display: inline-flex; } .sm-preview { margin-block-start: 12px; } .sm-field-row { grid-template-columns: minmax(0, 1fr); gap: 7px; } .sm-field-row--switch { grid-template-columns: minmax(0, 1fr) auto; } .sm-field-control { justify-self: stretch; } .sm-field-row--switch .sm-field-control { justify-self: end; } }
</style>
