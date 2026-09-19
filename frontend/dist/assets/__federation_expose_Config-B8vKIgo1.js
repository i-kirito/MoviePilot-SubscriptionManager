import { importShared } from './__federation_fn_import-JrT3xvdd.js';

const _export_sfc = (sfc, props) => {
  const target = sfc.__vccOpts || sfc;
  for (const [key, val] of props) {
    target[key] = val;
  }
  return target;
};

const {createElementVNode:_createElementVNode,resolveComponent:_resolveComponent,createVNode:_createVNode,createTextVNode:_createTextVNode,withCtx:_withCtx,normalizeClass:_normalizeClass,renderList:_renderList,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,toDisplayString:_toDisplayString,createBlock:_createBlock,createCommentVNode:_createCommentVNode,withModifiers:_withModifiers} = await importShared('vue');


const _hoisted_1 = { class: "sm-config" };
const _hoisted_2 = { class: "sm-header__brand" };
const _hoisted_3 = { class: "sm-header__logo" };
const _hoisted_4 = { class: "sm-header__identity" };
const _hoisted_5 = { class: "sm-header__crumbs" };
const _hoisted_6 = { class: "sm-header__title-row" };
const _hoisted_7 = { class: "sm-header__actions" };
const _hoisted_8 = { class: "sm-config__body" };
const _hoisted_9 = { class: "sm-layout" };
const _hoisted_10 = {
  class: "sm-group-nav",
  "aria-label": "选择配置分组"
};
const _hoisted_11 = { class: "sm-field-surface" };
const _hoisted_12 = { class: "sm-field-surface__heading" };
const _hoisted_13 = { class: "sm-field-surface__heading-copy" };
const _hoisted_14 = { class: "sm-field-section__rows" };
const _hoisted_15 = { class: "sm-field-row__copy" };
const _hoisted_16 = { class: "sm-field-row__label" };
const _hoisted_17 = { class: "sm-field-control" };
const _hoisted_18 = { class: "sm-preview" };
const _hoisted_19 = { class: "sm-preview__title" };
const _hoisted_20 = { class: "sm-preview__list" };
const _hoisted_21 = { class: "sm-change-summary" };
const _hoisted_22 = { class: "sm-preview__title" };
const _hoisted_23 = { key: 0 };
const _hoisted_24 = { key: 1 };

const {computed,reactive,ref,watch} = await importShared('vue');



const _sfc_main = {
  __name: 'Config',
  props: {
  initialConfig: { type: Object, default: () => ({}) },
  api: { type: Object, default: null },
},
  emits: ['save', 'close', 'layout'],
  setup(__props, { emit: __emit }) {

const props = __props;

const emit = __emit;
emit('layout', { maxWidth: '78rem' });

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
};

const normalize = (source) => {
  const incoming = source && typeof source === 'object' ? source : {};
  const result = { ...defaults, ...incoming };
  result.libraries = Array.isArray(incoming.libraries) ? [...incoming.libraries] : [...defaults.libraries];
  return result
};

const draft = reactive(normalize(props.initialConfig));
const baseline = ref(normalize(props.initialConfig));
const activeGroup = ref('global');
const headerScrolled = ref(false);
const mobileGroupSheet = ref(false);
const headerSentinel = ref(null);

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
];

const activeGroupMeta = computed(() => groups.find((group) => group.key === activeGroup.value) || groups[0]);
const allFields = computed(() => groups.flatMap((group) => group.sections.flatMap((section) => section.fields)));
const changedKeys = computed(() =>
  allFields.value.map((field) => field[0]).filter((key) => JSON.stringify(draft[key]) !== JSON.stringify(baseline.value[key])),
);
const changedCount = computed(() => changedKeys.value.length);
const activeFieldCount = computed(() => allFields.value.filter((field) => field[3] === 'switch' && draft[field[0]]).length);
const transferMode = computed(() => (draft.dry_run ? '模拟运行' : '实际清理'));
const monitorCount = computed(() => String(draft.monitor_dirs || '').split('\n').filter((item) => item.trim()).length);

watch(
  () => props.initialConfig,
  (value) => {
    const next = normalize(value);
    Object.assign(draft, next);
    baseline.value = next;
  },
  { deep: true },
);

function setGroup(key) {
  activeGroup.value = key;
  mobileGroupSheet.value = false;
}

function updateNumber(key, event) {
  const value = Number(event);
  if (Number.isFinite(value)) draft[key] = value;
}

function cloneDraft() {
  return JSON.parse(JSON.stringify(draft))
}

function saveConfig() {
  const payload = cloneDraft();
  baseline.value = normalize(payload);
  emit('save', payload);
}

function runOnce() {
  const payload = cloneDraft();
  payload.onlyonce = true;
  draft.onlyonce = true;
  baseline.value = normalize(payload);
  emit('save', payload);
}

return (_ctx, _cache) => {
  const _component_VIcon = _resolveComponent("VIcon");
  const _component_VChip = _resolveComponent("VChip");
  const _component_VBtn = _resolveComponent("VBtn");
  const _component_VListItem = _resolveComponent("VListItem");
  const _component_VList = _resolveComponent("VList");
  const _component_VAlert = _resolveComponent("VAlert");
  const _component_VSwitch = _resolveComponent("VSwitch");
  const _component_VTextField = _resolveComponent("VTextField");
  const _component_VCronField = _resolveComponent("VCronField");
  const _component_VTextarea = _resolveComponent("VTextarea");
  const _component_VCombobox = _resolveComponent("VCombobox");
  const _component_VCardTitle = _resolveComponent("VCardTitle");
  const _component_VCard = _resolveComponent("VCard");
  const _component_VBottomSheet = _resolveComponent("VBottomSheet");

  return (_openBlock(), _createElementBlock("section", _hoisted_1, [
    _createElementVNode("form", {
      class: "sm-config__form",
      onSubmit: _withModifiers(saveConfig, ["prevent"])
    }, [
      _createElementVNode("div", {
        ref_key: "headerSentinel",
        ref: headerSentinel,
        class: "sm-header-sentinel",
        "aria-hidden": "true"
      }, null, 512),
      _createElementVNode("header", {
        class: _normalizeClass(['sm-header', { 'sm-header--scrolled': headerScrolled.value }])
      }, [
        _createElementVNode("div", _hoisted_2, [
          _createElementVNode("div", _hoisted_3, [
            _createVNode(_component_VIcon, {
              icon: "mdi-calendar-sync-outline",
              size: "27"
            })
          ]),
          _createElementVNode("div", _hoisted_4, [
            _createElementVNode("div", _hoisted_5, [
              _cache[3] || (_cache[3] = _createElementVNode("span", null, "MoviePilot", -1)),
              _createVNode(_component_VIcon, {
                icon: "mdi-chevron-right",
                size: "14"
              }),
              _cache[4] || (_cache[4] = _createElementVNode("span", null, "插件", -1))
            ]),
            _createElementVNode("div", _hoisted_6, [
              _cache[6] || (_cache[6] = _createElementVNode("h1", null, "订阅管理", -1)),
              _createVNode(_component_VChip, {
                color: "primary",
                size: "x-small",
                variant: "tonal"
              }, {
                default: _withCtx(() => [...(_cache[5] || (_cache[5] = [
                  _createTextVNode("i-kirito", -1)
                ]))]),
                _: 1
              })
            ])
          ])
        ]),
        _createElementVNode("div", _hoisted_7, [
          _createVNode(_component_VBtn, {
            color: "primary",
            variant: "tonal",
            type: "button",
            onClick: runOnce
          }, {
            default: _withCtx(() => [
              _createVNode(_component_VIcon, {
                icon: "mdi-play",
                start: ""
              }),
              _cache[7] || (_cache[7] = _createTextVNode("运行一次", -1))
            ]),
            _: 1
          }),
          _createVNode(_component_VBtn, {
            color: "primary",
            variant: "flat",
            type: "submit",
            disabled: changedCount.value === 0
          }, {
            default: _withCtx(() => [
              _createVNode(_component_VIcon, {
                icon: "mdi-content-save",
                start: ""
              }),
              _cache[8] || (_cache[8] = _createTextVNode("保存修改", -1))
            ]),
            _: 1
          }, 8, ["disabled"]),
          _createVNode(_component_VBtn, {
            class: "sm-header__close",
            color: "default",
            variant: "outlined",
            type: "button",
            onClick: _cache[0] || (_cache[0] = $event => (emit('close')))
          }, {
            default: _withCtx(() => [
              _createVNode(_component_VIcon, {
                icon: "mdi-close",
                start: ""
              }),
              _cache[9] || (_cache[9] = _createTextVNode("关闭", -1))
            ]),
            _: 1
          })
        ])
      ], 2),
      _createElementVNode("div", _hoisted_8, [
        _createElementVNode("div", _hoisted_9, [
          _createElementVNode("nav", _hoisted_10, [
            _cache[10] || (_cache[10] = _createElementVNode("div", { class: "sm-group-nav__heading" }, "插件设置", -1)),
            _createVNode(_component_VList, {
              class: "sm-group-nav__list",
              density: "compact",
              nav: ""
            }, {
              default: _withCtx(() => [
                (_openBlock(), _createElementBlock(_Fragment, null, _renderList(groups, (group) => {
                  return _createVNode(_component_VListItem, {
                    key: group.key,
                    active: activeGroup.value === group.key,
                    "prepend-icon": group.icon,
                    title: group.title,
                    color: "primary",
                    rounded: "lg",
                    onClick: $event => (setGroup(group.key))
                  }, null, 8, ["active", "prepend-icon", "title", "onClick"])
                }), 64))
              ]),
              _: 1
            }),
            _cache[11] || (_cache[11] = _createElementVNode("section", { class: "sm-group-nav__help" }, [
              _createElementVNode("strong", null, "关于订阅管理"),
              _createElementVNode("p", null, "续作订阅、Trakt 日历和转移清理共用一个配置入口。")
            ], -1))
          ]),
          _createElementVNode("main", _hoisted_11, [
            _createElementVNode("div", _hoisted_12, [
              _createElementVNode("div", _hoisted_13, [
                _createVNode(_component_VIcon, {
                  icon: activeGroupMeta.value.icon,
                  color: "primary",
                  size: "22"
                }, null, 8, ["icon"]),
                _createElementVNode("div", null, [
                  _createElementVNode("h2", null, _toDisplayString(activeGroupMeta.value.title), 1),
                  _createElementVNode("p", null, _toDisplayString(activeGroupMeta.value.summary), 1)
                ])
              ]),
              _createVNode(_component_VBtn, {
                class: "sm-mobile-group-action",
                icon: "",
                size: "small",
                type: "button",
                variant: "tonal",
                onClick: _cache[1] || (_cache[1] = $event => (mobileGroupSheet.value = true))
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_VIcon, { icon: "mdi-view-list-outline" })
                ]),
                _: 1
              })
            ]),
            _createVNode(_component_VAlert, {
              class: "sm-info",
              type: "info",
              variant: "tonal",
              density: "compact",
              text: "所有功能共用“启用插件”开关；修改后点击保存修改才会生效。"
            }),
            (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(activeGroupMeta.value.sections, (section, sectionIndex) => {
              return (_openBlock(), _createElementBlock("section", {
                key: section.title,
                class: "sm-field-section"
              }, [
                _createElementVNode("h3", null, _toDisplayString(sectionIndex + 1) + ". " + _toDisplayString(section.title), 1),
                _createElementVNode("div", _hoisted_14, [
                  (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(section.fields, (field) => {
                    return (_openBlock(), _createElementBlock("div", {
                      key: field[0],
                      class: _normalizeClass(['sm-field-row', { 'sm-field-row--switch': field[3] === 'switch' }])
                    }, [
                      _createElementVNode("div", _hoisted_15, [
                        _createElementVNode("div", _hoisted_16, _toDisplayString(field[1]), 1),
                        _createElementVNode("p", null, _toDisplayString(field[2]), 1)
                      ]),
                      _createElementVNode("div", _hoisted_17, [
                        (field[3] === 'switch')
                          ? (_openBlock(), _createBlock(_component_VSwitch, {
                              key: 0,
                              modelValue: draft[field[0]],
                              "onUpdate:modelValue": $event => ((draft[field[0]]) = $event),
                              color: "primary",
                              density: "compact",
                              "hide-details": "",
                              "aria-label": field[1]
                            }, null, 8, ["modelValue", "onUpdate:modelValue", "aria-label"]))
                          : (field[3] === 'number')
                            ? (_openBlock(), _createBlock(_component_VTextField, {
                                key: 1,
                                "model-value": draft[field[0]],
                                type: "number",
                                density: "compact",
                                "hide-details": "",
                                variant: "outlined",
                                suffix: field[4],
                                "onUpdate:modelValue": $event => (updateNumber(field[0], $event))
                              }, null, 8, ["model-value", "suffix", "onUpdate:modelValue"]))
                            : (field[3] === 'cron')
                              ? (_openBlock(), _createBlock(_component_VCronField, {
                                  key: 2,
                                  modelValue: draft[field[0]],
                                  "onUpdate:modelValue": $event => ((draft[field[0]]) = $event),
                                  density: "compact",
                                  "hide-details": "",
                                  variant: "outlined"
                                }, null, 8, ["modelValue", "onUpdate:modelValue"]))
                              : (field[3] === 'textarea')
                                ? (_openBlock(), _createBlock(_component_VTextarea, {
                                    key: 3,
                                    modelValue: draft[field[0]],
                                    "onUpdate:modelValue": $event => ((draft[field[0]]) = $event),
                                    rows: "2",
                                    density: "compact",
                                    "hide-details": "",
                                    variant: "outlined"
                                  }, null, 8, ["modelValue", "onUpdate:modelValue"]))
                                : (field[3] === 'combo')
                                  ? (_openBlock(), _createBlock(_component_VCombobox, {
                                      key: 4,
                                      modelValue: draft[field[0]],
                                      "onUpdate:modelValue": $event => ((draft[field[0]]) = $event),
                                      items: draft[field[0]],
                                      multiple: "",
                                      chips: "",
                                      "closable-chips": "",
                                      clearable: "",
                                      density: "compact",
                                      "hide-details": "",
                                      variant: "outlined"
                                    }, null, 8, ["modelValue", "onUpdate:modelValue", "items"]))
                                  : _createCommentVNode("", true)
                      ])
                    ], 2))
                  }), 128))
                ])
              ]))
            }), 128))
          ]),
          _createElementVNode("aside", _hoisted_18, [
            _createElementVNode("div", _hoisted_19, [
              _createVNode(_component_VIcon, {
                color: "primary",
                icon: "mdi-clock-outline",
                size: "20"
              }),
              _cache[12] || (_cache[12] = _createElementVNode("h2", null, "运行节奏", -1))
            ]),
            _createElementVNode("ul", _hoisted_20, [
              _createElementVNode("li", null, [
                _createVNode(_component_VIcon, {
                  icon: "mdi-calendar-clock-outline",
                  size: "18"
                }),
                _cache[13] || (_cache[13] = _createElementVNode("span", null, "执行周期", -1)),
                _createElementVNode("strong", null, _toDisplayString(draft.cron || '默认每日'), 1)
              ]),
              _createElementVNode("li", null, [
                _createVNode(_component_VIcon, {
                  icon: "mdi-folder-eye-outline",
                  size: "18"
                }),
                _cache[14] || (_cache[14] = _createElementVNode("span", null, "监控目录", -1)),
                _createElementVNode("strong", null, _toDisplayString(monitorCount.value) + " 个", 1)
              ]),
              _createElementVNode("li", null, [
                _createVNode(_component_VIcon, {
                  icon: "mdi-delete-sweep-outline",
                  size: "18"
                }),
                _cache[15] || (_cache[15] = _createElementVNode("span", null, "清理模式", -1)),
                _createElementVNode("strong", null, _toDisplayString(transferMode.value), 1)
              ]),
              _createElementVNode("li", null, [
                _createVNode(_component_VIcon, {
                  icon: "mdi-toggle-switch-outline",
                  size: "18"
                }),
                _cache[16] || (_cache[16] = _createElementVNode("span", null, "已启用能力", -1)),
                _createElementVNode("strong", null, _toDisplayString(activeFieldCount.value) + " / " + _toDisplayString(allFields.value.length), 1)
              ])
            ]),
            _createElementVNode("section", _hoisted_21, [
              _createElementVNode("div", _hoisted_22, [
                _createVNode(_component_VIcon, {
                  color: "warning",
                  icon: "mdi-format-list-checks",
                  size: "19"
                }),
                _cache[17] || (_cache[17] = _createElementVNode("h3", null, "当前配置", -1))
              ]),
              (changedCount.value === 0)
                ? (_openBlock(), _createElementBlock("p", _hoisted_23, "配置与已保存内容一致。"))
                : (_openBlock(), _createElementBlock("p", _hoisted_24, "有 " + _toDisplayString(changedCount.value) + " 项修改待保存。", 1))
            ])
          ])
        ])
      ])
    ], 32),
    _createVNode(_component_VBottomSheet, {
      modelValue: mobileGroupSheet.value,
      "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((mobileGroupSheet).value = $event))
    }, {
      default: _withCtx(() => [
        _createVNode(_component_VCard, null, {
          default: _withCtx(() => [
            _createVNode(_component_VCardTitle, null, {
              default: _withCtx(() => [...(_cache[18] || (_cache[18] = [
                _createTextVNode("选择配置分组", -1)
              ]))]),
              _: 1
            }),
            _createVNode(_component_VList, { nav: "" }, {
              default: _withCtx(() => [
                (_openBlock(), _createElementBlock(_Fragment, null, _renderList(groups, (group) => {
                  return _createVNode(_component_VListItem, {
                    key: group.key,
                    active: activeGroup.value === group.key,
                    "prepend-icon": group.icon,
                    title: group.title,
                    onClick: $event => (setGroup(group.key))
                  }, null, 8, ["active", "prepend-icon", "title", "onClick"])
                }), 64))
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"])
  ]))
}
}

};
const Config = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-d6c6e9bb"]]);

export { Config as default };
