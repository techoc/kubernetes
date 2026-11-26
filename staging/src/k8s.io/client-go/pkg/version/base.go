/*
Copyright 2014 The Kubernetes Authors.

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

// 基础版本信息。
//
// 当构建时未通过 go ldflags 注入 git 版本信息时，以这里的数据兜底，为 `go build`
// 等无法获取 git 信息的临时构建提供近似的 Kubernetes 版本。
//
// 源码树中的这些字段看起来很奇怪：构建流程会用 ldflags -X 覆盖它们，仓库内的值
// 只是给 "git archive"（以及 GitHub tarball 下载）用的占位符。
//
// 中文：这些变量在发布构建中通过 -ldflags -X 覆盖；源码中的值只是归档时的占位符。
var (
	// TODO: 弃用 gitMajor/gitMinor，只保留 gitVersion。第一步是保留字段但使其
	// 不再参与逻辑；之后再移除。移除可能影响解析 kubectl version 输出的脚本，
	// 不过这些脚本大多本就该读 gitVersion。

	// 中文：gitMajor/gitMinor 已仅作兼容保留，解析版本时应以 gitVersion 为准。
	gitMajor string = "" // major version, always numeric
	gitMinor string = "" // minor version, numeric possibly followed by "+"

	// 由构建脚本生成的语义化版本，详见
	// https://github.com/kubernetes/sig-release/blob/master/release-engineering/versioning.md#kubernetes-release-versioning
	// https://kubernetes.io/releases/version-skew-policy/
	// 关于该字段的详细讨论。
	//
	// TODO: 出于历史原因该字段仍叫 "gitVersion"。预发布版本的语义化版本元数据
	// 是 git hash，但版本号本身已不是 "git describe" 的直接输出，而是为满足
	// semver 规范做过转换。

	// 注意：$Format 字符串在 'git archive' 时由同目录 .gitattributes 中的
	// 'export-subst' 替换，详见 https://git-scm.com/docs/gitattributes。
	//
	// 中文：归档时由 export-subst 替换 $Format 占位符；正常构建则由 ldflags 注入。
	gitVersion   string = "v0.0.0-master+$Format:%H$"
	gitCommit    string = "$Format:%H$" // sha1 from git, output of $(git rev-parse HEAD)
	gitTreeState string = ""            // state of git tree, either "clean" or "dirty"

	buildDate string = "1970-01-01T00:00:00Z" // build date in ISO8601 format, output of $(date -u +'%Y-%m-%dT%H:%M:%SZ')
)
