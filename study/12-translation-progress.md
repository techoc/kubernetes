# 中文注释进度

批次划分与执行细节见 `11-translation-plan.md`。本表只记录注释翻译进度，与 `06-progress.md` 的学习进度分开。

计划表按目录粗估，实际执行按"有英文注释的文件"重新盘点。已完成批次见下方提交记录。

| 批次 | 范围 | 实际文件数 | 完成 |
| --- | --- | --- | --- |
| 0 | 清理工作区 + 守卫工具 | 3 文档 + 2 文件 + 工具 | [x] |
| 1 | `cmd/` | 29 | [x] |
| 2 | `pkg/` 残余 | 1 | [x] |
| 3 | `staging/src/k8s.io/api/` 残余 | 0（58 个均为生成文件，跳过） | [x] |
| 4 | apimachinery 残余 | 7 | [x] |
| 5 | `kubectl` 小包 | 58 | [x] |
| 6 | `kubectl/pkg/cmd` | 196 | [x] |
| 7 | `test/utils/`、`test/images/`（拆 2 批） | 134 | [ ] |
| 8 | `test/` 其他小目录 | 58 | [ ] |
| 9 | `test/integration/` | 417 | [ ] |
| 10 | `test/e2e/` | 537 | [ ] |
| 11 | `test/e2e_node/`、`test/e2e_node_windows/` | 177 | [ ] |
| 12 | `hack/`、`third_party/`、`cluster/`、`build/`、小 staging 模块 | ~155 | [ ] |
| 13 | 双语残留清理 | 待枚举 | [ ] |
| — | 生成物重同步（`make update`） | 1 次以上 | [ ] |

## 提交记录

| 日期 | 提交 | 范围 |
| --- | --- | --- |
| 2026-11-24 ~ 25 | `84024b1c2b8` 及之前的 135 个提交 | 已覆盖 ~2,694 个文件，详见 `git log --grep='docs: translate'` |
| 2026-11-25 | `5e3edf4059e` | apiserver example types 与 watch cache history 注释 |
| 2026-11-25 | `0ec388cc484` | 翻译计划、进度表、仓库笔记与导航 |
| 2026-11-25 | `885a3b2c831` | 注释守卫工具 |
| 2026-11-25 | `b690bf93464` | `cmd/` 工具与 kubeadm 注释 |
| 2026-11-25 | `aa2f2a71357` | `pkg/registry` 包注释 |
| 2026-11-25 | `59261435822` | apimachinery runtime 与 sharding 注释 |
| 2026-11-25 | `4a1ac922a6c` | kubectl config、describe 与工具类注释 |
| 2026-11-25 | `c4f6b6d2b06` | kubectl 命令注释 |
