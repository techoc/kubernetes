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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

/*
禁止使用 HostPath 卷，避免容器直接访问节点文件系统。

**受限字段：**

spec.volumes[*].hostPath

**允许值：** 未定义/null
*/

// hostPath 直接把节点文件系统挂进容器，等于把节点暴露给工作负载；这一项是基线里最硬的禁令之一，
// 没有「安全子集」可放行，所以判定只看字段有没有设置。

func init() {
	addCheck(CheckHostPathVolumes)
}

// 单独命名为常量，是为了让其它 check 能通过 OverrideCheckIDs 精确引用到这一项。
const checkHostPathVolumesID CheckID = "hostPathVolumes"

// CheckHostPathVolumes 返回基线级检查，从 1.0 起要求 hostPath 未设置或为 null，故无版本分支。
func CheckHostPathVolumes() Check {
	return Check{
		ID:    checkHostPathVolumesID,
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       hostPathVolumes_1_0,
			},
		},
	}
}

// podMetadata 用不上，仅为满足 CheckPod 的函数签名而保留。
func hostPathVolumes_1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	var hostVolumes []string

	// 报错里带卷名而不是卷定义，用户才找得到是哪个 volume 需要删掉。
	for _, volume := range podSpec.Volumes {
		if volume.HostPath != nil {
			hostVolumes = append(hostVolumes, volume.Name)
		}
	}

	if len(hostVolumes) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "hostPath volumes",
			ForbiddenDetail: fmt.Sprintf("%s %s", pluralize("volume", "volumes", len(hostVolumes)), joinQuote(hostVolumes)),
		}
	}

	return CheckResult{Allowed: true}
}
