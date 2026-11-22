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

// Base version information.
//
// This is the fallback data used when version information from git is not
// provided via go ldflags. It provides an approximation of the Kubernetes
// version for ad-hoc builds (e.g. `go build`) that cannot get the version
// information from git.
//
// If you are looking at these fields in the git tree, they look
// strange. They are set by the build process with ldflags -X. The
// in-tree values are dummy values used for "git archive", which also
// works for GitHub tar downloads.
//
// 中文：这些变量在发布构建中通过 -ldflags -X 覆盖；源码中的值只是归档时的占位符。
var (
	// TODO: Deprecate gitMajor and gitMinor, use only gitVersion
	// instead. First step in deprecation, keep the fields but make
	// them irrelevant. (Next we'll take it out, which may muck with
	// scripts consuming the kubectl version output - but most of
	// these should be looking at gitVersion already anyways.)

	// 中文：gitMajor/gitMinor 已仅作兼容保留，解析版本时应以 gitVersion 为准。
	gitMajor string = "" // major version, always numeric
	gitMinor string = "" // minor version, numeric possibly followed by "+"

	// semantic version, derived by build scripts (see
	// https://github.com/kubernetes/sig-release/blob/master/release-engineering/versioning.md#kubernetes-release-versioning
	// https://kubernetes.io/releases/version-skew-policy/
	// for a detailed discussion of this field)
	//
	// TODO: This field is still called "gitVersion" for legacy
	// reasons. For prerelease versions, the build metadata on the
	// semantic version is a git hash, but the version itself is no
	// longer the direct output of "git describe", but a slight
	// translation to be semver compliant.

	// NOTE: The $Format strings are replaced during 'git archive' thanks to the
	// companion .gitattributes file containing 'export-subst' in this same
	// directory.  See also https://git-scm.com/docs/gitattributes
	//
	// 中文：归档时由 export-subst 替换 $Format 占位符；正常构建则由 ldflags 注入。
	gitVersion   string = "v0.0.0-master+$Format:%H$"
	gitCommit    string = "$Format:%H$" // sha1 from git, output of $(git rev-parse HEAD)
	gitTreeState string = ""            // state of git tree, either "clean" or "dirty"

	buildDate string = "1970-01-01T00:00:00Z" // build date in ISO8601 format, output of $(date -u +'%Y-%m-%dT%H:%M:%SZ')
)
