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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/pod-security-admission/api"
)

/*
受限策略除禁止 HostPath 卷外，还只允许以下内联 Pod 卷源：
* configMap
* downwardAPI
* emptyDir
* projected
* secret
* csi
* persistentVolumeClaim
* ephemeral
* image

**受限字段：**

spec.volumes[*].hostPath
spec.volumes[*].gcePersistentDisk
spec.volumes[*].awsElasticBlockStore
spec.volumes[*].gitRepo
spec.volumes[*].nfs
spec.volumes[*].iscsi
spec.volumes[*].glusterfs
spec.volumes[*].rbd
spec.volumes[*].flexVolume
spec.volumes[*].cinder
spec.volumes[*].cephfs
spec.volumes[*].flocker
spec.volumes[*].fc
spec.volumes[*].azureFile
spec.volumes[*].vsphereVolume
spec.volumes[*].quobyte
spec.volumes[*].azureDisk
spec.volumes[*].portworxVolume
spec.volumes[*].photonPersistentDisk
spec.volumes[*].scaleIO
spec.volumes[*].storageos

**允许值：** 未定义/null
*/

func init() {
	// 通过 init 自动注册，确保策略评估器默认就能加载这条受限级别检查。
	addCheck(CheckRestrictedVolumes)
}

// CheckRestrictedVolumes 返回受限级检查，从 1.0 起限制可使用的卷类型。
// 这条检查取代 1.25 之前的 hostPathVolumes：与其只封禁 hostPath，
// 不如显式列出允许的内联卷类型，新增卷源时默认拒绝，避免白名单被动放宽。
func CheckRestrictedVolumes() Check {
	return Check{
		ID:    "restrictedVolumes",
		Level: api.LevelRestricted,
		Versions: []VersionedCheck{
			{
				// 本检查完全覆盖 hostPathVolumes 的语义，声明覆盖可避免两条检查重复报告同一违规。
				MinimumVersion:   api.MajorMinorVersion(1, 0),
				CheckPod:         restrictedVolumes_1_0,
				OverrideCheckIDs: []CheckID{checkHostPathVolumesID},
			},
		},
	}
}

// restrictedVolumes_1_0 用白名单而非黑名单判定：只放行有限的内联卷类型，
// 其余一律拒绝。这样新增卷源时默认不突破受限策略，而无需每次同步更新黑名单。
func restrictedVolumes_1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	var badVolumes []string
	badVolumeTypes := sets.NewString()

	// 按“白名单跳过、其余收集”遍历，避免在一份长长的禁止列表里漏掉新增字段。
	for _, volume := range podSpec.Volumes {
		switch {
		case volume.ConfigMap != nil,
			volume.CSI != nil,
			volume.DownwardAPI != nil,
			volume.EmptyDir != nil,
			volume.Ephemeral != nil,
			volume.Image != nil,
			volume.PersistentVolumeClaim != nil,
			volume.Projected != nil,
			volume.Secret != nil:
			continue

		default:
			badVolumes = append(badVolumes, volume.Name)

			switch {
			case volume.HostPath != nil:
				badVolumeTypes.Insert("hostPath")
			case volume.GCEPersistentDisk != nil:
				badVolumeTypes.Insert("gcePersistentDisk")
			case volume.AWSElasticBlockStore != nil:
				badVolumeTypes.Insert("awsElasticBlockStore")
			case volume.GitRepo != nil:
				badVolumeTypes.Insert("gitRepo")
			case volume.NFS != nil:
				badVolumeTypes.Insert("nfs")
			case volume.ISCSI != nil:
				badVolumeTypes.Insert("iscsi")
			case volume.Glusterfs != nil:
				badVolumeTypes.Insert("glusterfs")
			case volume.RBD != nil:
				badVolumeTypes.Insert("rbd")
			case volume.FlexVolume != nil:
				badVolumeTypes.Insert("flexVolume")
			case volume.Cinder != nil:
				badVolumeTypes.Insert("cinder")
			case volume.CephFS != nil:
				badVolumeTypes.Insert("cephfs")
			case volume.Flocker != nil:
				badVolumeTypes.Insert("flocker")
			case volume.FC != nil:
				badVolumeTypes.Insert("fc")
			case volume.AzureFile != nil:
				badVolumeTypes.Insert("azureFile")
			case volume.VsphereVolume != nil:
				badVolumeTypes.Insert("vsphereVolume")
			case volume.Quobyte != nil:
				badVolumeTypes.Insert("quobyte")
			case volume.AzureDisk != nil:
				badVolumeTypes.Insert("azureDisk")
			case volume.PhotonPersistentDisk != nil:
				badVolumeTypes.Insert("photonPersistentDisk")
			case volume.PortworxVolume != nil:
				badVolumeTypes.Insert("portworxVolume")
			case volume.ScaleIO != nil:
				badVolumeTypes.Insert("scaleIO")
			case volume.StorageOS != nil:
				badVolumeTypes.Insert("storageos")
			default:
				// 无法识别的卷源不能因“不在已知黑名单”而被放过；归入 unknown 保持默认拒绝。
				badVolumeTypes.Insert("unknown")
			}
		}
	}

	// 只在确有违规时构造详细信息；通过路径保持零分配，避免热路径上的格式化开销。
	if len(badVolumes) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "restricted volume types",
			ForbiddenDetail: fmt.Sprintf(
				"%s %s %s %s %s",
				pluralize("volume", "volumes", len(badVolumes)),
				joinQuote(badVolumes),
				pluralize("uses", "use", len(badVolumes)),
				pluralize("restricted volume type", "restricted volume types", len(badVolumeTypes)),
				joinQuote(badVolumeTypes.List()),
			),
		}
	}

	return CheckResult{Allowed: true}
}
