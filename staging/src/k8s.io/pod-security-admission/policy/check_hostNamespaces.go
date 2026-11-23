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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

/*
Sharing the host network, PID, and IPC namespaces must be disallowed.

**Restricted Fields:**

spec.hostNetwork
spec.hostPID
spec.hostIPC

**Allowed Values:** undefined, false
*/
// 本文件检查共享宿主机命名空间；三项独立判定，全部汇总到同一条原因中。

func init() {
	addCheck(CheckHostNamespaces)
}

// CheckHostNamespaces returns a baseline level check
// that prohibits host namespaces in 1.0+
// 策略自 1.0 起稳定，暂无按版本分支的必要。
func CheckHostNamespaces() Check {
	return Check{
		ID:    "hostNamespaces",
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       hostNamespaces_1_0,
			},
		},
	}
}

// 逐项收集所有违规字段，用户一次就能看到需要清理的全部命名空间共享配置。
func hostNamespaces_1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	var hostNamespaces []string

	if podSpec.HostNetwork {
		hostNamespaces = append(hostNamespaces, "hostNetwork=true")
	}

	if podSpec.HostPID {
		hostNamespaces = append(hostNamespaces, "hostPID=true")
	}

	if podSpec.HostIPC {
		hostNamespaces = append(hostNamespaces, "hostIPC=true")
	}

	if len(hostNamespaces) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "host namespaces",
			ForbiddenDetail: strings.Join(hostNamespaces, ", "),
		}
	}

	return CheckResult{Allowed: true}
}
