# 全仓库中文注释计划

目标是给仓库里**人工维护的 Go 源码注释**补上中文。先立防线，再翻代码；生成物只通过生成器同步，不手改。

## 1. 家底

截至 `release-1.37-zh` 当前 HEAD（`84024b1c2b8`）：

| 项 | 数 |
| --- | --- |
| 目标 Go 文件（排除 `vendor/`、`_output/`、生成文件） | 12,116 |
| 目标文件里的注释行 | ~357,000 |
| 已含中文的注释行 | ~207,000 |
| 仍为英文的注释行 | ~150,000 |
| 完全没有中文的文件 | 4,671 |
| 其中带 `Code generated ... DO NOT EDIT` 标记 | 2,487 |
| 人工维护、完全没有中文的文件 | 2,184 |
| 已有翻译提交 | 135 个，触及 ~2,694 个文件 |

人工维护的零中文文件分布：

| 目录 | 文件数 |
| --- | --- |
| `test/e2e/` | 537 |
| `test/integration/` | 417 |
| `staging/src/k8s.io/kubectl/pkg/` | 411 |
| `test/e2e_node/` | 149 |
| `cmd/` | 134 |
| `pkg/` | 92 |
| `test/images/`、`test/declarative_validation/`、`test/utils/` | 231 |
| `staging/src/k8s.io/api/` | 58 |
| 其余（`hack/`、`third_party/`、`cluster/`、小 staging 模块等） | ~155 |

2,487 个带生成标记的文件（`client-go/applyconfigurations`、`clientset`、`informers`、`listers`、`code-generator` 的 `output_tests` 等）**不进入人工翻译队列**。它们的注释从 API 源类型复制而来，`make update` 会把已翻好的源注释带进去。

## 2. 边界

纳入：所有非 `vendor/`、非 `_output/`、非生成标记的 `.go` 文件。

排除：

- `vendor/`（4,460 个文件，上游代码，升级依赖时全量冲突）
- `zz_generated.*`、`generated.pb.go`、`*.gen.go`
- 带 `Code generated ... DO NOT EDIT` 标记的文件
- `.proto`（生成输入）、`.sh` / `.py` / `.c` / `.h` / `.js` / `.ts` 等非 Go 源码
- `go.mod` / `go.work`（生成文件）

生成物的漂移按「重生成」处理：`api/openapi-spec/` 里的英文 description 来自已翻译的 API 类型注释，跑生成器同步，不改 verify 规则，也不回滚源注释。

## 3. 红线

这些在 `84024b1c2b8` 里都出过事，逐条对照：

1. **只改注释，不改代码。** 日志文本、`fmt.Errorf` 消息、Ginkgo `It("...")` 描述、命令行 help 字符串都是字符串字面量，不是注释，保持英文。
2. **不重排 import。** 用 `gofmt`，不用 `goimports`。Go 按 import 声明顺序初始化包，重排分组属于代码变更。
3. **不移动声明位置。** 曾经把 `Graph` 结构体挪到 `namedVertex` 前面，属于代码变更。
4. **不补类型实参。** 上游依赖类型推断的地方不要显式写出来。
5. **机器可读的注释原样保留**：`//go:build`、`//go:generate`、`//go:linkname`、`// +k8s:*`、`// +optional`、`//nolint`、`Deprecated:`、Go Example 的 `// Output:`。
6. **license header 原样保留**，使用 `hack/boilerplate/boilerplate.go.txt`。
7. **生成文件不手改**。需要改动时跑 `make update`，生成 diff 单独提交。

## 4. 防线：非注释 token 流守卫

这是整个计划的地基，先落地再翻任何文件。原因是 `84024b1c2b8` 的提交说明里那句话：worktree-vs-HEAD 的检查在提交后就失声了。

实现：在 `study/tools/commentguard/` 放一个 Go 小程序，用 `go/scanner` 把注释 token 全部丢掉，只保留代码 token 的序列（含字面量值），对比两个版本的 token 流是否一致。额外做三项文本级比对：

- 所有指令注释（`//go:*`、`// +*`、`//nolint`、`Deprecated:`）逐字相同
- 所有 `// Output:` 及其后的期望输出逐字相同
- license header 逐字相同

用法：

```bash
# 翻译前记录基线（BASE 为待改文件的父提交）
go run ./study/tools/commentguard -base "$BASE" -files file1.go,file2.go

# 翻译后、暂存前再跑一次；提交后再对父提交跑一次
```

它能挡住上次全部四类越界：字符串被翻译（字面量 token 变了）、import 分组重排（token 顺序变了）、结构体移位（顺序变了）、显式类型实参（token 多了）。

守卫通过后：

```bash
gofmt -l <改动文件>          # 必须无输出
make test WHAT=<涉及包>       # 按批次涉及范围跑
git diff --check
```

## 5. 批次

每批一个提交，消息沿用 `docs: translate <范围> comments`。每批 100–150 个文件，按目录聚簇，不要跨目录混装。

| 批次 | 范围 | 文件数 | 风险点 |
| --- | --- | --- | --- |
| 0 | 清理工作区（2 个 apiserver 文件 + `study/09`） | 3 | 先单独提交，别混进翻译批次 |
| 1 | `cmd/` | 134 | help 文本是字符串，别翻 |
| 2 | `pkg/` 残余 | 92 | 无特殊风险 |
| 3 | `staging/src/k8s.io/api/` 残余 | 58 | 改完后生成器要把注释带进生成物 |
| 4 | `staging/src/k8s.io/kubectl/pkg/` | 411 | 拆 3 批；命令参数说明多为字符串 |
| 5 | `test/` 小目录（utils、images、declarative_validation、fuzz、typecheck、list 等） | ~240 | 测试断言消息是字符串 |
| 6 | `test/integration/` | 417 | 拆 3 批 |
| 7 | `test/e2e/` | 537 | 拆 4 批；Ginkgo 描述字符串不翻 |
| 8 | `test/e2e_node/`、`test/e2e_node_windows/` | 177 | 同上 |
| 9 | `hack/`、`third_party/`、`cluster/`、`build/`、小 staging 模块 | ~155 | `hack/` 脚本内的 Go 文件注意 boilerplate |
| 10 | 已翻译文件中的中英双语残留清理 | 待枚举 | 只合并重复行，别改语义 |

批次 3 之后插入一次生成器同步：源 API 注释变了，生成物要跟上。之后每批涉及 API 类型时也如此。

## 6. 生成物同步

```bash
git status --short                # 必须干净
make update SILENT=false
git diff --stat                   # 确认只有预期的生成物变化
```

`make update` 会启动 kube-apiserver + etcd 抓 OpenAPI 规格，耗时较长。生成物 diff 单独提交：

```
chore: regenerate generated files after comment translations
```

验证：

```bash
make verify WHAT=openapi-spec
make verify WHAT=codegen
```

`hack/lib/verify-generated.sh` 在临时 worktree 里跑 update 脚本，所以**提交前跑 verify 会失败**，这是预期行为。

## 7. 环境前置

当前机器缺 GNU sed，`make verify WHAT=openapi-spec` 直接报：

```
Failed to find GNU sed as sed or gsed. If you are on Mac: brew install gnu-sed.
```

开工前先装：

```bash
brew install gnu-sed
```

另外 `make verify` 全量检查耗时较长，日常用：

```bash
make verify WHAT="boilerplate gofmt test-code typecheck"
```

## 8. 每批的固定流程

```bash
# 1. 生成待翻清单
python3 - <<'PY'
# 按批次目录枚举零中文或含英文注释的文件
PY

# 2. 翻译，只动注释

# 3. 守卫：翻译后 vs 父提交
go run ./study/tools/commentguard -base "$BASE" -files "$FILES"

# 4. 格式与静态检查
gofmt -l $FILES
git diff --check
make test WHAT=<涉及包>

# 5. 提交
git add $FILES
git commit -m "docs: translate ... comments"

# 6. 提交后再守卫一次，堵住"提交后 gates 失声"
go run ./study/tools/commentguard -base "$BASE^" -files "$FILES"
```

## 9. 验收

- 人工维护的 Go 文件里不再有整段英文注释（允许英文专有名词、URL、代码标识符、`// Output:`、指令注释）
- 非注释 token 流守卫对全部翻译提交逐批通过
- `gofmt -l` 无输出
- `make verify` 全量通过
- 生成物与源注释一致，`make update` 不再产生 diff
