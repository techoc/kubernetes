/*
Copyright 2025 The Kubernetes Authors.

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
	"k8s.io/pod-security-admission/api"
)

/*

默认的 /proc 掩码用于缩小攻击面，受限策略必须保留，且不因用户命名空间而放宽。

**受限字段：**
spec.containers[*].securityContext.procMount
spec.initContainers[*].securityContext.procMount

**允许值：** 未定义/null、"Default"

*/
// 本文件把基线的 procMount 检查在受限等级重新收紧：基线在 1.35 起对 user namespace Pod 放宽，
// 但受限等级不接受这种放宽，故复用旧的无条件阻断实现并覆盖基线版本。

func init() {
	addCheck(CheckProcMountRestricted)
}

// CheckProcMountRestricted 返回受限级检查，禁止未掩码的 procMount。
// 仅覆盖基线同名检查，不改基线实现，避免影响基线策略的既有判定。
func CheckProcMountRestricted() Check {
	return Check{
		ID:    "procMount_restricted",
		Level: api.LevelRestricted,
		Versions: []VersionedCheck{
			{
				// 1.35 起基线会放宽用户命名空间 Pod；受限等级仍复用无条件阻止的 procMount_1_0，
				// 并覆盖基线同名检查，避免较宽松的实现放行未掩码配置。
				MinimumVersion:   api.MajorMinorVersion(1, 35),
				CheckPod:         procMount_1_0,
				OverrideCheckIDs: []CheckID{"procMount"},
			},
		},
	}
}
