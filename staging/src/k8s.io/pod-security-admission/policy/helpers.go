/*
Copyright 2021 The Kubernetes Authors.

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

package policy

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// 诊断消息里统一用双引号包住列表项，空列表返回空串以免留下悬空的引号。
func joinQuote(items []string) string {
	if len(items) == 0 {
		return ""
	}
	return `"` + strings.Join(items, `", "`) + `"`
}

// 让错误详情里的单复数与实际数量一致，避免“1 containers”这种噪音。
func pluralize(singular, plural string, count int) string {
	if count == 1 {
		return singular
	}
	return plural
}

// relaxPolicyForUserNamespacePod 判断给定 PodSpec 启用用户命名空间后是否应放宽策略。
func relaxPolicyForUserNamespacePod(podSpec *corev1.PodSpec) bool {
	return podSpec != nil && podSpec.HostUsers != nil && !*podSpec.HostUsers
}
