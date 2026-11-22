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

package metrics

import (
	"fmt"
	"regexp"

	"github.com/blang/semver/v4"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

const (
	// 只匹配 v<major>.<minor>.<patch> 前缀，忽略预发布/构建元数据，
	// 因为指标弃用判断只关心主版本号。
	versionRegexpString = `^v(\d+\.\d+\.\d+)`
)

var (
	versionRe = regexp.MustCompile(versionRegexpString)
)

// parseSemver 解析可选的语义化版本：空字符串表示“未设置”，返回 nil 而非错误。
func parseSemver(s string) *semver.Version {
	if s != "" {
		sv := semver.MustParse(s)
		return &sv
	}
	return nil
}

// parseVersion 从 apimachinery 的版本信息中提取 semver。
// 版本串不符合预期时 panic：这是构建期配置错误，不应在运行时静默降级。
func parseVersion(ver apimachineryversion.Info) semver.Version {
	matches := versionRe.FindAllStringSubmatch(ver.String(), -1)

	if len(matches) != 1 {
		panic(fmt.Sprintf("version string \"%v\" doesn't match expected regular expression: \"%v\"", ver.String(), versionRe.String()))
	}
	return semver.MustParse(matches[0][1])
}
