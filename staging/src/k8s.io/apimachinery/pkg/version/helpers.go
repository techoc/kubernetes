/*
Copyright 2018 The Kubernetes Authors.

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

import (
	"regexp"
	"strconv"
	"strings"
)

// versionType 表示版本成熟度；数值越大优先级越高，使 GA 排在 beta、beta 排在 alpha 之前。
type versionType int

const (
	// 版本类型数值越大，优先级越高
	versionTypeAlpha versionType = iota
	versionTypeBeta
	versionTypeGA
)

// kubeVersionRegex 匹配 Kubernetes 风格的版本号：v<major> 或 v<major>(alpha|beta)<minor>。
var kubeVersionRegex = regexp.MustCompile("^v([\\d]+)(?:(alpha|beta)([\\d]+))?$")

// parseKubeVersion 解析 Kubernetes 风格版本串，返回主版本号、成熟度类型和次版本号。
// 对 GA 版本次版本号为 0；任何不符合格式的输入返回 ok=false，由调用方决定如何兜底。
func parseKubeVersion(v string) (majorVersion int, vType versionType, minorVersion int, ok bool) {
	var err error
	submatches := kubeVersionRegex.FindStringSubmatch(v)
	if len(submatches) != 4 {
		return 0, 0, 0, false
	}
	switch submatches[2] {
	case "alpha":
		vType = versionTypeAlpha
	case "beta":
		vType = versionTypeBeta
	case "":
		vType = versionTypeGA
	default:
		return 0, 0, 0, false
	}
	if majorVersion, err = strconv.Atoi(submatches[1]); err != nil {
		return 0, 0, 0, false
	}
	// GA 版本没有 minor 子串，避免对空串调用 Atoi。
	if vType != versionTypeGA {
		if minorVersion, err = strconv.Atoi(submatches[3]); err != nil {
			return 0, 0, 0, false
		}
	}
	return majorVersion, vType, minorVersion, true
}

// CompareKubeAwareVersionStrings 比较两个 Kubernetes 风格版本串。
// Kubernetes 风格版本串以 v 开头，后接主版本号，可选 "alpha" 或 "beta" 再接次版本号
// （例如 v1、v2beta1）。排序时先按 GA/alpha/beta 的成熟度，再按主版本号、次版本号比较，
// 例如 v2, v1, v1beta2, v1beta1, v1alpha1。
//
// 无法识别的版本串按普通字符串逆向排序（见下方 strings.Compare 参数顺序），
// 且可识别的版本一律排在不可识别的版本之后。
func CompareKubeAwareVersionStrings(v1, v2 string) int {
	if v1 == v2 {
		return 0
	}
	v1major, v1type, v1minor, ok1 := parseKubeVersion(v1)
	v2major, v2type, v2minor, ok2 := parseKubeVersion(v2)
	switch {
	case !ok1 && !ok2:
		// 两者都不是 kube 风格版本，按字典序比较；注意参数是反的，保持既有排序约定。
		return strings.Compare(v2, v1)
	case !ok1 && ok2:
		return -1
	case ok1 && !ok2:
		return 1
	}
	// 首先比较成熟度，其次主版本号，最后次版本号。
	if v1type != v2type {
		return int(v1type) - int(v2type)
	}
	if v1major != v2major {
		return v1major - v2major
	}
	return v1minor - v2minor
}
