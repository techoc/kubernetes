/*
Copyright The Kubernetes Authors.

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

package inplacepodresize

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// TestMemoryBackedVolumesResizeFeatureDiscover 覆盖门控两端：
// 内存卷的原地扩缩容完全由一个特性门控制，没有额外的节点状态探测。
func TestMemoryBackedVolumesResizeFeatureDiscover(t *testing.T) {
	tests := []struct {
		name        string
		featureGate bool
		expected    bool
	}{
		{
			name:        "FeatureGateEnabled",
			featureGate: true,
			expected:    true,
		},
		{
			name:        "FeatureGateDisabled",
			featureGate: false,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &types.NodeConfiguration{FeatureGates: types.FeatureGateMap{IPPRMemoryBackedVolumesFeatureGate: tt.featureGate}}
			enabled := MemoryBackedVolumesResizeFeature.Discover(cfg)
			if want, got := tt.expected, enabled; want != got {
				t.Fatalf("want=%v,got=%v", want, got)
			}
		})
	}
}

// TestMemoryBackedVolumesResizeFeatureInferForScheduling 锁定调度推断恒为 false：
// 该特性只在更新时才有意义，调度阶段 PodSpec 里还没有“变更前”的卷可供比较。
func TestMemoryBackedVolumesResizeFeatureInferForScheduling(t *testing.T) {
	f := &memoryBackedVolumesResizeFeature{}
	podInfo := &types.PodInfo{Spec: &v1.PodSpec{}}
	if f.InferForScheduling(podInfo) {
		t.Fatalf("InferForScheduling should always be false")
	}
}

// TestMemoryBackedVolumesResizeFeatureInferForUpdate 的核心是把“真正需要该特性的变更”
// 和“应该交给常规校验处理的非法变更”区分开：
// 只有同一个内存卷的 SizeLimit 发生变化才返回 true；
// 增删卷、改名、换介质、空 SizeLimit 等都不属于这里要声明的能力，
// 否则会把非法 patch 误标成节点能力需求。
func TestMemoryBackedVolumesResizeFeatureInferForUpdate(t *testing.T) {
	quantity100Mi := resource.MustParse("100Mi")
	quantity200Mi := resource.MustParse("200Mi")

	// 各用例的语义：
	// NoChanges —— 卷完全没变，无需求；
	// SizeLimitIncreased / SizeLimitDecreased —— 内存卷大小变化，应触发；
	// DefaultMedium_Ignored —— 默认介质不是 Memory，不算内存卷；
	// NonEmptyDirVolume_Ignored —— 非 emptyDir 卷不在范围内；
	// VolumeCountMismatch_Ignored —— 卷数量变化属于增删，交给标准校验；
	// VolumeNameMismatch_Ignored —— 卷名变化属于非法修改，交给标准校验；
	// NilSizeLimit_Ignored —— 任一侧 SizeLimit 为空，不是扩容；
	// MultipleVolumes_OneResizableMemoryBackedVolumeResized —— 多卷混合，只要一个内存卷变大小就触发。
	tests := []struct {
		name       string
		oldPodInfo *types.PodInfo
		newPodInfo *types.PodInfo
		expected   bool
	}{
		{
			name: "NoChanges",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "SizeLimitIncreased",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity200Mi,
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "SizeLimitDecreased",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity200Mi,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "DefaultMedium_Ignored",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumDefault,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumDefault,
									SizeLimit: &quantity200Mi,
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "NonEmptyDirVolume_Ignored",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/tmp",
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/tmp2",
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "VolumeCountMismatch_Ignored",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
						{
							Name: "vol-2",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "VolumeNameMismatch_Ignored",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-2",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity200Mi,
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "NilSizeLimit_Ignored",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: nil,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-1",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity200Mi,
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "MultipleVolumes_OneResizableMemoryBackedVolumeResized",
			oldPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-non-resizable",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/tmp",
								},
							},
						},
						{
							Name: "vol-resizable",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity100Mi,
								},
							},
						},
					},
				},
			},
			newPodInfo: &types.PodInfo{
				Spec: &v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "vol-non-resizable",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: "/tmp",
								},
							},
						},
						{
							Name: "vol-resizable",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    v1.StorageMediumMemory,
									SizeLimit: &quantity200Mi,
								},
							},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &memoryBackedVolumesResizeFeature{}
			got := f.InferForUpdate(tt.oldPodInfo, tt.newPodInfo)
			if got != tt.expected {
				t.Errorf("InferForUpdate() = %v, want %v", got, tt.expected)
			}
		})
	}
}
