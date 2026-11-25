# 中文注释进度

批次划分与执行细节见 `11-translation-plan.md`。本表只记录注释翻译进度，与 `06-progress.md` 的学习进度分开。

批次 0 已完成：`5e3edf4059e`（2 个 apiserver 文件）、`0ec388cc484`（5 个 study 文档）、`885a3b2c831`（守卫工具）。

| 批次 | 范围 | 文件数 | 完成 |
| --- | --- | --- | --- |
| 0 | 清理工作区 | 3 文档 + 2 文件 + 守卫 | [x] |
| 1 | `cmd/`（实际有英文注释的文件） | 29 文件 / ~481 行 | [x] |
| 2 | `pkg/` 残余 | 92 | [ ] |
| 3 | `staging/src/k8s.io/api/` 残余 | 58 | [ ] |
| 4 | `staging/src/k8s.io/kubectl/pkg/`（拆 3 批） | 411 | [ ] |
| 5 | `test/` 小目录 | ~240 | [ ] |
| 6 | `test/integration/`（拆 3 批） | 417 | [ ] |
| 7 | `test/e2e/`（拆 4 批） | 537 | [ ] |
| 8 | `test/e2e_node/`、`test/e2e_node_windows/` | 177 | [ ] |
| 9 | `hack/`、`third_party/`、`cluster/`、`build/`、小 staging 模块 | ~155 | [ ] |
| 10 | 双语残留清理 | 待枚举 | [ ] |
| — | 生成物重同步（`make update`） | 1 次以上 | [ ] |

## 提交记录

| 日期 | 提交 | 范围 |
| --- | --- | --- |
| 2026-11-24 ~ 25 | `84024b1c2b8` 及之前的 135 个提交 | 已覆盖 ~2,694 个文件，详见 `git log --grep='docs: translate'` |
| 2026-11-25 | `5e3edf4059e` | apiserver example types 与 watch cache history 注释 |
| 2026-11-25 | `0ec388cc484` | 翻译计划、进度表、仓库笔记与导航 |
| 2026-11-25 | `885a3b2c831` | 注释守卫工具 |
