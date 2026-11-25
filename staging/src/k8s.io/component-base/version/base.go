/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package version

// 基础版本信息：go ldflags 未提供 git 版本信息时使用的回退数据，
// 为无法从 git 获取版本的临时构建（如 `go build`）提供近似的 Kubernetes 版本。
// 这些包级变量不是运行时配置，而是构建过程通过 ldflags -X 注入的目标；
// 源码中的占位值用于 "git archive"（也适用于 GitHub tar 下载）。
var (
	// TODO：弃用 gitMajor 和 gitMinor，仅使用 gitVersion。弃用第一步是保留字段但不再使用；
	// 之后可能删除它们，影响读取 kubectl version 输出的旧脚本，不过多数脚本应已使用 gitVersion。
	// 保留这两个字段仅为兼容旧脚本；新代码应只使用 gitVersion。
	gitMajor string // 主版本，始终为数字
	gitMinor string // 次版本，为数字，可能后跟 "+"

	// 由构建脚本生成的语义版本，详见：
	// https://github.com/kubernetes/community/blob/master/contributors/design-proposals/release/versioning.md
	// TODO：因历史原因该字段仍名为 "gitVersion"。预发布版本的语义版本构建元数据是 git 哈希，
	// 但版本本身已不再是 "git describe" 的直接输出，而是为符合 semver 做了少量转换。

	// 注意：$Format 字符串在 'git archive' 时由同目录 .gitattributes 的 export-subst 替换。
	// 另见 https://git-scm.com/docs/gitattributes；手工修改占位符无效。
	gitVersion   = "v0.0.0-master+$Format:%H$"
	gitCommit    = "$Format:%H$" // git 的 sha1，输出自 $(git rev-parse HEAD)
	gitTreeState = ""            // git 工作树状态，"clean" 或 "dirty"

	buildDate = "1970-01-01T00:00:00Z" // ISO8601 格式构建日期，输出自 $(date -u +'%Y-%m-%dT%H:%M:%SZ')
)

const (
	// DefaultKubeBinaryVersion 是基于最新 Kubernetes 发布版硬编码的二进制版本。
	// 除本地测试中 gitMajor 和 gitMinor 为空外，应与它们一致；每个 minor 发布都要同步更新。
	// 用于在没有注入构建信息时进行特性门控的版本比较。
	DefaultKubeBinaryVersion = "1.37"
)
