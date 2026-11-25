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

package test

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/pod-security-admission/api"
)

func init() {
	// 通过 init 注册到全局表，测试框架按 level/version/check 三元组查找对应夹具。
	// volumeType := "ext4"
	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "restricted volume types",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			return []*corev1.Pod{
				// 允许类型集中在一个 Pod 中，一次断言覆盖整张白名单。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{
						{Name: "volume0", VolumeSource: corev1.VolumeSource{}}, // 隐式 emptyDir
						// 空 VolumeSource 也是允许的隐式 emptyDir 形态。
						{Name: "volume1", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "volume2", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "test"}}},
						{Name: "volume3", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "test"}}},
						{Name: "volume4", VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{{Path: "labels", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels"}}}}}},
						{Name: "volume5", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "test"}}}},
						{Name: "volume6", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{}}}},

						// TODO: CSIInlineVolume 达到 GA 后启用该卷。
						// {Name: "volume7", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{Driver: "inline.storage.kubernetes.io",VolumeAttributes: map[string]string{"foo": "bar"}}}},

						// TODO: Ephemeral 达到 GA 后启用该卷。
						// {Name: "volume8", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{VolumeClaimTemplate: nil}}},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			// 每个禁用类型各造一个 Pod，便于定位具体是哪类卷触发拒绝。
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{GCEPersistentDisk: &corev1.GCEPersistentDiskVolumeSource{PDName: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{AWSElasticBlockStore: &corev1.AWSElasticBlockStoreVolumeSource{VolumeID: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{GitRepo: &corev1.GitRepoVolumeSource{Repository: "github.com/kubernetes/kubernetes"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "test", Path: "/test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{ISCSI: &corev1.ISCSIVolumeSource{TargetPortal: "test", IQN: "iqn.2001-04.com.example:storage.kube.sys1.xyz", Lun: 0}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{Glusterfs: &corev1.GlusterfsVolumeSource{Path: "test", EndpointsName: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{RBD: &corev1.RBDVolumeSource{CephMonitors: []string{"test"}, RBDImage: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{FlexVolume: &corev1.FlexVolumeSource{Driver: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{Cinder: &corev1.CinderVolumeSource{VolumeID: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{CephFS: &corev1.CephFSVolumeSource{Monitors: []string{"test"}}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{Flocker: &corev1.FlockerVolumeSource{DatasetName: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{FC: &corev1.FCVolumeSource{WWIDs: []string{"test"}}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{AzureFile: &corev1.AzureFileVolumeSource{SecretName: "test", ShareName: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{VsphereVolume: &corev1.VsphereVirtualDiskVolumeSource{VolumePath: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{Quobyte: &corev1.QuobyteVolumeSource{Registry: "localhost:1234", Volume: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{AzureDisk: &corev1.AzureDiskVolumeSource{DiskName: "test", DataDiskURI: "https://test.blob.core.windows.net/test/test.vhd"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{PortworxVolume: &corev1.PortworxVolumeSource{VolumeID: "test", FSType: "ext4"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{ScaleIO: &corev1.ScaleIOVolumeSource{VolumeName: "test", Gateway: "localhost", System: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{StorageOS: &corev1.StorageOSVolumeSource{VolumeName: "test"}}}}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{{Name: "volume1", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/null"}}}}
				}),
			}
		},
	}

	// 从 v1.0 起生效且各版本规则未变，只需注册一个版本，后续版本会复用这组夹具。
	registerFixtureGenerator(
		fixtureKey{level: api.LevelRestricted, version: api.MajorMinorVersion(1, 0), check: "restrictedVolumes"},
		fixtureData_1_0,
	)
}
