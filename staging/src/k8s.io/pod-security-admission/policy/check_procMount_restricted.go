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

The default /proc masks are set up to reduce attack surface, and should be required by the restricted profile.

**Restricted Fields:**
spec.containers[*].securityContext.procMount
spec.initContainers[*].securityContext.procMount

**Allowed Values:** undefined/null, "Default"

*/
// 本文件把基线的 procMount 检查在受限等级重新收紧：基线在 1.35 起对 user namespace Pod 放宽，
// 但受限等级不接受这种放宽，故复用旧的无条件阻断实现并覆盖基线版本。

func init() {
	addCheck(CheckProcMountRestricted)
}

// CheckProcMountRestricted returns a restricted level check that forbids unmasked procmount.
// 仅覆盖基线同名检查，不改基线实现，避免影响基线策略的既有判定。
func CheckProcMountRestricted() Check {
	return Check{
		ID:    "procMount_restricted",
		Level: api.LevelRestricted,
		Versions: []VersionedCheck{
			{
				// Prior to 1.35, the baseline "procMount" check ran procMount_1_0 to unconditionally block unmasked procMount.
				// In 1.35+, baseline conditionally relaxes for user namespace pods.
				// Starting at that version, keep running the unconditional block in the restricted profile,
				// and override the slightly weaker version of the same check from the baseline profile.
				MinimumVersion:   api.MajorMinorVersion(1, 35),
				CheckPod:         procMount_1_0,
				OverrideCheckIDs: []CheckID{"procMount"},
			},
		},
	}
}
