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
| 7 | `test/utils/`、`test/images/`（拆 2 批） | 134 | [x] |
| 8 | `test/` 其他小目录 | 58 | [x] |
| 9 | `test/integration/`（拆 3 批） | 290 | [x] |
| 10 | `test/e2e/` | 671（含 e2e_node 等，拆 5 批） | [x] |
| 11 | `test/e2e_node/`、`test/e2e_node_windows/` | 已随第 10 项完成 | [x] |
| 12 | `hack/`、`third_party/`、`cluster/`、`build/`、小 staging 模块 | 41 + staging 分批 | [x] |
| 13 | 双语残留清理 | 55 | [x] |
| — | 生成物重同步（swagger doc + openapi spec） | 122 | [x] |

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
| 2026-11-25 | `48450030fd1` | 守卫误报修正 |
| 2026-11-25 | `58ebf6c18cd` | test/utils 与 test/images 注释 |
| 2026-11-25 | `7b3ed4b9cc5` | 补漏的 agnhost 指标注释 |
| 2026-11-25 | `6030957c638` | test/ 小目录注释 |
| 2026-11-25 | `23cbe0c8ad4` | integration apimachinery 与 scheduler 注释 |
| 2026-11-25 | `75690c6f8ea` | integration 补漏注释 |
| 2026-11-25 | `d434e55a745` | integration scheduler 与 framework 注释 |
| 2026-11-26 | `3b2aff5d47e` | integration apiserver 与 storage 注释 |
| 2026-11-26 | `0318d501ec1` | e2e framework 与 node 注释 |
| 2026-11-26 | `28b77325d99` | e2e networking 与 storage 注释 |
| 2026-11-26 | `a3f409a899a` | e2e storage 与 conformance 注释 |
| 2026-11-26 | `fcd3bc91ba9` | e2e autoscaling 与 scheduling 注释 |
| 2026-11-26 | `f2021c3de23` | e2e kubectl 与 utils 注释 |
| 2026-11-26 | `6520c9e4ac3` | hack 与 third_party 注释 |
| 2026-11-26 | `c98bddb4cd6` | staging kubelet 与小模块注释 |
| 2026-11-26 | `6e39de121cb` | staging apimachinery 注释 |
| 2026-11-26 | `0c7fdcce66c` | staging cli-runtime 与 kubectl 注释 |
| 2026-11-26 | `3aea3274ed3` | staging cloud provider 与 api 注释 |
| 2026-11-26 | `3f0da287424` | staging client-go 注释 part 1 |
| 2026-11-26 | `4193b302251` | staging client-go 注释 part 2 |
| 2026-11-26 | `8e6de93f78f` | staging code-generator 注释 part 1 |
| 2026-11-26 | `ad3237bc12b` | staging code-generator 注释 part 2 |
| 2026-11-26 | `19fabf91b91` | staging apiserver 注释 part 1 |
| 2026-11-26 | `dd797675d50` | staging apiserver 注释 part 2 |
| 2026-11-26 | `b388b1d5005` | staging apiserver 注释 part 3 |
| 2026-11-26 | `3e1ede0e1a7` | staging apiserver 注释 part 4 |
| 2026-11-26 | `10574dabdac` | 残余英文正文注释 |
| 2026-11-26 | `792784a7d7b` | 重生成 swagger doc 与 openapi spec |
