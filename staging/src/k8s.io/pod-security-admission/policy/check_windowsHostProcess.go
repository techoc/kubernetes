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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

/*
Pod and containers must not set securityContext.windowsOptions.hostProcess to true.

**Restricted Fields:**

spec.securityContext.windowsOptions.hostProcess
spec.containers[*].securityContext.windowsOptions.hostProcess
spec.initContainers[*].securityContext.windowsOptions.hostProcess

**Allowed Values:** undefined / false
*/

// hostProcess 让容器以宿主身份运行，等同于绕过容器隔离边界；nil 与 false 都安全，只有显式 true 才拒绝。
func init() {
	addCheck(CheckWindowsHostProcess)
}

// CheckWindowsHostProcess returns a baseline level check
// that forbids hostProcess=true in 1.0+
func CheckWindowsHostProcess() Check {
	return Check{
		ID:    "windowsHostProcess",
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       windowsHostProcess_1_0,
			},
		},
	}
}

func windowsHostProcess_1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 先逐容器再查 Pod 级，最后统一拼错误信息；分两步是为了让 “pod and containers ...” 的措辞稳定。
	var badContainers []string
	visitContainers(podSpec, func(container *corev1.Container) {
		if container.SecurityContext != nil &&
			container.SecurityContext.WindowsOptions != nil &&
			container.SecurityContext.WindowsOptions.HostProcess != nil &&
			*container.SecurityContext.WindowsOptions.HostProcess {
			badContainers = append(badContainers, container.Name)
		}
	})

	podSpecForbidden := false
	if podSpec.SecurityContext != nil &&
		podSpec.SecurityContext.WindowsOptions != nil &&
		podSpec.SecurityContext.WindowsOptions.HostProcess != nil &&
		*podSpec.SecurityContext.WindowsOptions.HostProcess {
		podSpecForbidden = true
	}

	// pod or containers explicitly set hostProcess=true
	// 即使在 Linux Pod 上这几个字段也没意义，但设置它们仍是显式的高危意图，统一拒绝避免平台差异。
	var forbiddenSetters []string
	if podSpecForbidden {
		forbiddenSetters = append(forbiddenSetters, "pod")
	}
	if len(badContainers) > 0 {
		forbiddenSetters = append(
			forbiddenSetters,
			fmt.Sprintf(
				"%s %s",
				pluralize("container", "containers", len(badContainers)),
				joinQuote(badContainers),
			),
		)
	}
	if len(forbiddenSetters) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "hostProcess",
			ForbiddenDetail: fmt.Sprintf("%s must not set securityContext.windowsOptions.hostProcess=true", strings.Join(forbiddenSetters, " and ")),
		}
	}

	return CheckResult{Allowed: true}
}
