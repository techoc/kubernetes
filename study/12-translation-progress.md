# 中文注释进度

批次划分与执行细节见 `11-translation-plan.md`。本表只记录注释翻译进度，与 `06-progress.md` 的学习进度分开。

当前处于批次 0 之前：工作区有 2 个 apiserver 文件、3 个未跟踪的 `study/` 文档待提交。

| 批次 | 范围 | 文件数 | 完成 |
| --- | --- | --- | --- |
| 0 | 清理工作区 | 3 | [ ] |
| 1 | `cmd/` | 134 | [ ] |
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
