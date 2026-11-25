/*
Copyright 2015 The Kubernetes Authors.

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

package pod

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
)

// TestVisitContainers 验证 VisitContainers 按 mask 选择容器类型、保持遍历顺序，
// 并允许回调修改容器字段（这里通过清空 SecurityContext 验证修改会写回原 spec）。
func TestVisitContainers(t *testing.T) {
	setAllFeatureEnabledContainersDuringTest := ContainerType(0)
	// mask 是要访问的容器类型位掩码；wantContainers 同时断言选中类型和顺序
	//（顺序固定为 init -> regular -> ephemeral）。
	testCases := []struct {
		desc           string
		spec           *v1.PodSpec
		wantContainers []string
		mask           ContainerType
	}{
		{
			desc:           "empty podspec",
			spec:           &v1.PodSpec{},
			wantContainers: []string{},
			mask:           AllContainers,
		},
		{
			desc: "regular containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"c1", "c2"},
			mask:           Containers,
		},
		{
			desc: "init containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"i1", "i2"},
			mask:           InitContainers,
		},
		{
			desc: "ephemeral containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"e1", "e2"},
			mask:           EphemeralContainers,
		},
		{
			desc: "all container types",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"i1", "i2", "c1", "c2", "e1", "e2"},
			mask:           AllContainers,
		},
		{
			desc: "all feature enabled container types",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2", SecurityContext: &v1.SecurityContext{}},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2", SecurityContext: &v1.SecurityContext{}},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"i1", "i2", "c1", "c2", "e1", "e2"},
			mask:           setAllFeatureEnabledContainersDuringTest,
		},
		{
			desc: "dropping fields",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2", SecurityContext: &v1.SecurityContext{}},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2", SecurityContext: &v1.SecurityContext{}},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2", SecurityContext: &v1.SecurityContext{}}},
				},
			},
			wantContainers: []string{"i1", "i2", "c1", "c2", "e1", "e2"},
			mask:           AllContainers,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.mask == setAllFeatureEnabledContainersDuringTest {
				tc.mask = AllFeatureEnabledContainers()
			}

			gotContainers := []string{}
			VisitContainers(tc.spec, tc.mask, func(c *v1.Container, containerType ContainerType) bool {
				gotContainers = append(gotContainers, c.Name)
				if c.SecurityContext != nil {
					c.SecurityContext = nil
				}
				return true
			})
			if !cmp.Equal(gotContainers, tc.wantContainers) {
				t.Errorf("VisitContainers() = %+v, want %+v", gotContainers, tc.wantContainers)
			}
			for _, c := range tc.spec.Containers {
				if c.SecurityContext != nil {
					t.Errorf("VisitContainers() did not drop SecurityContext for container %q", c.Name)
				}
			}
			for _, c := range tc.spec.InitContainers {
				if c.SecurityContext != nil {
					t.Errorf("VisitContainers() did not drop SecurityContext for init container %q", c.Name)
				}
			}
			for _, c := range tc.spec.EphemeralContainers {
				if c.SecurityContext != nil {
					t.Errorf("VisitContainers() did not drop SecurityContext for ephemeral container %q", c.Name)
				}
			}
		})
	}
}

// TestContainerIter 验证 ContainerIter 迭代器与 VisitContainers 行为一致：
// 按 mask 过滤、保持顺序，并且第二个返回值报告每个容器所属的真实类型。
func TestContainerIter(t *testing.T) {
	testCases := []struct {
		desc           string
		spec           *v1.PodSpec
		wantContainers []string
		mask           ContainerType
	}{
		{
			desc:           "empty podspec",
			spec:           &v1.PodSpec{},
			wantContainers: []string{},
			mask:           AllContainers,
		},
		{
			desc: "regular containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"c1", "c2"},
			mask:           Containers,
		},
		{
			desc: "init containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"i1", "i2"},
			mask:           InitContainers,
		},
		{
			desc: "init + main containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"i1", "i2", "c1", "c2"},
			mask:           InitContainers | Containers,
		},
		{
			desc: "ephemeral containers",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"e1", "e2"},
			mask:           EphemeralContainers,
		},
		{
			desc: "all container types",
			spec: &v1.PodSpec{
				Containers: []v1.Container{
					{Name: "c1"},
					{Name: "c2"},
				},
				InitContainers: []v1.Container{
					{Name: "i1"},
					{Name: "i2"},
				},
				EphemeralContainers: []v1.EphemeralContainer{
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e1"}},
					{EphemeralContainerCommon: v1.EphemeralContainerCommon{Name: "e2"}},
				},
			},
			wantContainers: []string{"i1", "i2", "c1", "c2", "e1", "e2"},
			mask:           AllContainers,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			gotContainers := []string{}
			for c, containerType := range ContainerIter(tc.spec, tc.mask) {
				gotContainers = append(gotContainers, c.Name)

				switch containerType {
				case InitContainers:
					if c.Name[0] != 'i' {
						t.Errorf("ContainerIter() yielded container type InitContainers for container %q", c.Name)
					}
				case Containers:
					if c.Name[0] != 'c' {
						t.Errorf("ContainerIter() yielded container type Containers for container %q", c.Name)
					}
				case EphemeralContainers:
					if c.Name[0] != 'e' {
						t.Errorf("ContainerIter() yielded container type EphemeralContainers for container %q", c.Name)
					}
				default:
					t.Errorf("ContainerIter() yielded unknown container type %d", containerType)
				}
			}

			if !cmp.Equal(gotContainers, tc.wantContainers) {
				t.Errorf("ContainerIter() = %+v, want %+v", gotContainers, tc.wantContainers)
			}
		})
	}
}

// TestPodSecrets 验证 VisitPodSecretNames 能找到 Pod 中所有 Secret 引用。
// 做法是先构造一个把 Secret 名字设为“结构体路径”的 Pod，再用反射枚举
// v1.Pod 中所有名字含 secret 的字段路径，双向比对以发现漏报或多余字段。
// 这样新增字段时测试会主动失败并提示更新实现或白名单。
func TestPodSecrets(t *testing.T) {
	// 这个 stub 穷举 Pod 中可能出现的 Secret 引用位置；
	// 每个引用的名字就是反射发现的字段路径，便于精确定位漏报。
	// 包含 Pod 中全部 Secret 引用的测试桩，引用名与反射发现的字段路径一致。
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				EnvFrom: []v1.EnvFromSource{{
					SecretRef: &v1.SecretEnvSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: "Spec.Containers[*].EnvFrom[*].SecretRef"}}}},
				Env: []v1.EnvVar{{
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: "Spec.Containers[*].Env[*].ValueFrom.SecretKeyRef"}}}}}}},
			ImagePullSecrets: []v1.LocalObjectReference{{
				Name: "Spec.ImagePullSecrets"}},
			InitContainers: []v1.Container{{
				EnvFrom: []v1.EnvFromSource{{
					SecretRef: &v1.SecretEnvSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: "Spec.InitContainers[*].EnvFrom[*].SecretRef"}}}},
				Env: []v1.EnvVar{{
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: "Spec.InitContainers[*].Env[*].ValueFrom.SecretKeyRef"}}}}}}},
			Volumes: []v1.Volume{{
				VolumeSource: v1.VolumeSource{
					AzureFile: &v1.AzureFileVolumeSource{
						SecretName: "Spec.Volumes[*].VolumeSource.AzureFile.SecretName"}}}, {
				VolumeSource: v1.VolumeSource{
					CephFS: &v1.CephFSVolumeSource{
						SecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.CephFS.SecretRef"}}}}, {
				VolumeSource: v1.VolumeSource{
					Cinder: &v1.CinderVolumeSource{
						SecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.Cinder.SecretRef"}}}}, {
				VolumeSource: v1.VolumeSource{
					FlexVolume: &v1.FlexVolumeSource{
						SecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.FlexVolume.SecretRef"}}}}, {
				VolumeSource: v1.VolumeSource{
					Projected: &v1.ProjectedVolumeSource{
						Sources: []v1.VolumeProjection{{
							Secret: &v1.SecretProjection{
								LocalObjectReference: v1.LocalObjectReference{
									Name: "Spec.Volumes[*].VolumeSource.Projected.Sources[*].Secret"}}}}}}}, {
				VolumeSource: v1.VolumeSource{
					RBD: &v1.RBDVolumeSource{
						SecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.RBD.SecretRef"}}}}, {
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: "Spec.Volumes[*].VolumeSource.Secret.SecretName"}}}, {
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: "Spec.Volumes[*].VolumeSource.Secret"}}}, {
				VolumeSource: v1.VolumeSource{
					ScaleIO: &v1.ScaleIOVolumeSource{
						SecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.ScaleIO.SecretRef"}}}}, {
				VolumeSource: v1.VolumeSource{
					ISCSI: &v1.ISCSIVolumeSource{
						SecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.ISCSI.SecretRef"}}}}, {
				VolumeSource: v1.VolumeSource{
					StorageOS: &v1.StorageOSVolumeSource{
						SecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.StorageOS.SecretRef"}}}}, {
				VolumeSource: v1.VolumeSource{
					CSI: &v1.CSIVolumeSource{
						NodePublishSecretRef: &v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.CSI.NodePublishSecretRef"}}}}},
			EphemeralContainers: []v1.EphemeralContainer{{
				EphemeralContainerCommon: v1.EphemeralContainerCommon{
					EnvFrom: []v1.EnvFromSource{{
						SecretRef: &v1.SecretEnvSource{
							LocalObjectReference: v1.LocalObjectReference{
								Name: "Spec.EphemeralContainers[*].EphemeralContainerCommon.EnvFrom[*].SecretRef"}}}},
					Env: []v1.EnvVar{{
						ValueFrom: &v1.EnvVarSource{
							SecretKeyRef: &v1.SecretKeySelector{
								LocalObjectReference: v1.LocalObjectReference{
									Name: "Spec.EphemeralContainers[*].EphemeralContainerCommon.Env[*].ValueFrom.SecretKeyRef"}}}}}}}},
		},
	}
	extractedNames := sets.New[string]()
	VisitPodSecretNames(pod, func(name string) bool {
		extractedNames.Insert(name)
		return true
	})

	// 名字含 secret 但不是 Secret 对象引用的字段（如 SecretFile），需要显式排除。
	// excludedSecretPaths 保存名称含 "secret" 但并非 Secret API 对象引用的字段路径。
	excludedSecretPaths := sets.New[string](
		"Spec.Volumes[*].VolumeSource.CephFS.SecretFile",
	)
	// 期望路径清单：每条都必须在上面的 stub 中有一个以该路径为名字的引用。
	// expectedSecretPaths 保存引用 Secret API 对象且名称含 "secret" 的字段路径。
	// 每个路径都必须在上方 Pod 测试桩中有对应实例，且 Secret 名设为该路径。
	expectedSecretPaths := sets.New[string](
		"Spec.Containers[*].EnvFrom[*].SecretRef",
		"Spec.Containers[*].Env[*].ValueFrom.SecretKeyRef",
		"Spec.EphemeralContainers[*].EphemeralContainerCommon.EnvFrom[*].SecretRef",
		"Spec.EphemeralContainers[*].EphemeralContainerCommon.Env[*].ValueFrom.SecretKeyRef",
		"Spec.ImagePullSecrets",
		"Spec.InitContainers[*].EnvFrom[*].SecretRef",
		"Spec.InitContainers[*].Env[*].ValueFrom.SecretKeyRef",
		"Spec.Volumes[*].VolumeSource.AzureFile.SecretName",
		"Spec.Volumes[*].VolumeSource.CephFS.SecretRef",
		"Spec.Volumes[*].VolumeSource.Cinder.SecretRef",
		"Spec.Volumes[*].VolumeSource.FlexVolume.SecretRef",
		"Spec.Volumes[*].VolumeSource.Projected.Sources[*].Secret",
		"Spec.Volumes[*].VolumeSource.RBD.SecretRef",
		"Spec.Volumes[*].VolumeSource.Secret",
		"Spec.Volumes[*].VolumeSource.Secret.SecretName",
		"Spec.Volumes[*].VolumeSource.ScaleIO.SecretRef",
		"Spec.Volumes[*].VolumeSource.ISCSI.SecretRef",
		"Spec.Volumes[*].VolumeSource.StorageOS.SecretRef",
		"Spec.Volumes[*].VolumeSource.CSI.NodePublishSecretRef",
	)
	// 反射结果与期望清单双向比对：少了说明新增字段没被访问到，多了说明白名单该更新。
	secretPaths := collectResourcePaths(t, "secret", nil, "", reflect.TypeOf(&v1.Pod{}))
	secretPaths = secretPaths.Difference(excludedSecretPaths)
	if missingPaths := expectedSecretPaths.Difference(secretPaths); len(missingPaths) > 0 {
		t.Logf("Missing expected secret paths:\n%s", strings.Join(sets.List[string](missingPaths), "\n"))
		t.Error("Missing expected secret paths. Verify VisitPodSecretNames() is correctly finding the missing paths, then correct expectedSecretPaths")
	}
	if extraPaths := secretPaths.Difference(expectedSecretPaths); len(extraPaths) > 0 {
		t.Logf("Extra secret paths:\n%s", strings.Join(sets.List(extraPaths), "\n"))
		t.Error("Extra fields with 'secret' in the name found. Verify VisitPodSecretNames() is including these fields if appropriate, then correct expectedSecretPaths")
	}

	if missingNames := expectedSecretPaths.Difference(extractedNames); len(missingNames) > 0 {
		t.Logf("Missing expected secret names:\n%s", strings.Join(sets.List[string](missingNames), "\n"))
		t.Error("Missing expected secret names. Verify the pod stub above includes these references, then verify VisitPodSecretNames() is correctly finding the missing names")
	}
	if extraNames := extractedNames.Difference(expectedSecretPaths); len(extraNames) > 0 {
		t.Logf("Extra secret names:\n%s", strings.Join(sets.List[string](extraNames), "\n"))
		t.Error("Extra secret names extracted. Verify VisitPodSecretNames() is correctly extracting secret names")
	}

	// 空名字段必须被忽略，否则会向调用方返回无法使用的 Secret 名。
	// emptyPod 是对象名为空的测试桩。
	emptyPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				EnvFrom: []v1.EnvFromSource{{
					SecretRef: &v1.SecretEnvSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: ""}}}}}},
		},
	}
	VisitPodSecretNames(emptyPod, func(name string) bool {
		t.Fatalf("expected no empty names collected, got %q", name)
		return false
	})
}

// collectResourcePaths 递归遍历类型定义，收集名字中含 resourcename 的字段路径。
// 它服务于 TestPodSecrets/TestPodConfigmaps 的反射交叉检查：
// 处理指针解引用、map/slice 的通配下标，并跳过含环的 ObjectMeta。
func collectResourcePaths(t *testing.T, resourcename string, path *field.Path, name string, tp reflect.Type) sets.Set[string] {
	resourcename = strings.ToLower(resourcename)
	resourcePaths := sets.New[string]()

	if tp.Kind() == reflect.Pointer {
		resourcePaths.Insert(sets.List[string](collectResourcePaths(t, resourcename, path, name, tp.Elem()))...)
		return resourcePaths
	}

	if strings.Contains(strings.ToLower(name), resourcename) {
		resourcePaths.Insert(path.String())
	}

	switch tp.Kind() {
	case reflect.Pointer:
		resourcePaths.Insert(sets.List[string](collectResourcePaths(t, resourcename, path, name, tp.Elem()))...)
	case reflect.Struct:
		// ObjectMeta 是通用结构，不应包含特定资源名称字段，且存在指针环，因此直接跳过。
		if name == "ObjectMeta" {
			break
		}
		for i := 0; i < tp.NumField(); i++ {
			field := tp.Field(i)
			resourcePaths.Insert(sets.List[string](collectResourcePaths(t, resourcename, path.Child(field.Name), field.Name, field.Type))...)
		}
	case reflect.Interface:
		t.Errorf("cannot find %s fields in interface{} field %s", resourcename, path.String())
	case reflect.Map:
		resourcePaths.Insert(sets.List[string](collectResourcePaths(t, resourcename, path.Key("*"), "", tp.Elem()))...)
	case reflect.Slice:
		resourcePaths.Insert(sets.List[string](collectResourcePaths(t, resourcename, path.Key("*"), "", tp.Elem()))...)
	default:
		// 基本类型无法继续下钻，递归到此结束。
	}

	return resourcePaths
}

// TestPodConfigmaps 与 TestPodSecrets 同构：验证 VisitPodConfigmapNames
// 覆盖 Pod 中全部 ConfigMap 引用，且忽略空名字。
func TestPodConfigmaps(t *testing.T) {
	// 每个引用的名字就是其字段路径，便于反射比对时定位漏报。
	// 包含 Pod 中全部 ConfigMap 引用的测试桩，引用名与反射发现的字段路径一致。
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				EnvFrom: []v1.EnvFromSource{{
					ConfigMapRef: &v1.ConfigMapEnvSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: "Spec.Containers[*].EnvFrom[*].ConfigMapRef"}}}},
				Env: []v1.EnvVar{{
					ValueFrom: &v1.EnvVarSource{
						ConfigMapKeyRef: &v1.ConfigMapKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: "Spec.Containers[*].Env[*].ValueFrom.ConfigMapKeyRef"}}}}}}},
			EphemeralContainers: []v1.EphemeralContainer{{
				EphemeralContainerCommon: v1.EphemeralContainerCommon{
					EnvFrom: []v1.EnvFromSource{{
						ConfigMapRef: &v1.ConfigMapEnvSource{
							LocalObjectReference: v1.LocalObjectReference{
								Name: "Spec.EphemeralContainers[*].EphemeralContainerCommon.EnvFrom[*].ConfigMapRef"}}}},
					Env: []v1.EnvVar{{
						ValueFrom: &v1.EnvVarSource{
							ConfigMapKeyRef: &v1.ConfigMapKeySelector{
								LocalObjectReference: v1.LocalObjectReference{
									Name: "Spec.EphemeralContainers[*].EphemeralContainerCommon.Env[*].ValueFrom.ConfigMapKeyRef"}}}}}}}},
			InitContainers: []v1.Container{{
				EnvFrom: []v1.EnvFromSource{{
					ConfigMapRef: &v1.ConfigMapEnvSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: "Spec.InitContainers[*].EnvFrom[*].ConfigMapRef"}}}},
				Env: []v1.EnvVar{{
					ValueFrom: &v1.EnvVarSource{
						ConfigMapKeyRef: &v1.ConfigMapKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: "Spec.InitContainers[*].Env[*].ValueFrom.ConfigMapKeyRef"}}}}}}},
			Volumes: []v1.Volume{{
				VolumeSource: v1.VolumeSource{
					Projected: &v1.ProjectedVolumeSource{
						Sources: []v1.VolumeProjection{{
							ConfigMap: &v1.ConfigMapProjection{
								LocalObjectReference: v1.LocalObjectReference{
									Name: "Spec.Volumes[*].VolumeSource.Projected.Sources[*].ConfigMap"}}}}}}}, {
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: "Spec.Volumes[*].VolumeSource.ConfigMap"}}}}},
		},
	}
	extractedNames := sets.New[string]()
	VisitPodConfigmapNames(pod, func(name string) bool {
		extractedNames.Insert(name)
		return true
	})

	// 期望路径清单：每条都要在 stub 中有对应的引用实例。
	// expectedPaths 保存名称含 "ConfigMap" 且引用 ConfigMap API 对象的字段路径。
	// 每个路径都必须在上方 Pod 测试桩中有对应实例，且 ConfigMap 名设为该路径。
	expectedPaths := sets.New[string](
		"Spec.Containers[*].EnvFrom[*].ConfigMapRef",
		"Spec.Containers[*].Env[*].ValueFrom.ConfigMapKeyRef",
		"Spec.EphemeralContainers[*].EphemeralContainerCommon.EnvFrom[*].ConfigMapRef",
		"Spec.EphemeralContainers[*].EphemeralContainerCommon.Env[*].ValueFrom.ConfigMapKeyRef",
		"Spec.InitContainers[*].EnvFrom[*].ConfigMapRef",
		"Spec.InitContainers[*].Env[*].ValueFrom.ConfigMapKeyRef",
		"Spec.Volumes[*].VolumeSource.Projected.Sources[*].ConfigMap",
		"Spec.Volumes[*].VolumeSource.ConfigMap",
	)
	collectPaths := collectResourcePaths(t, "ConfigMap", nil, "", reflect.TypeOf(&v1.Pod{}))
	if missingPaths := expectedPaths.Difference(collectPaths); len(missingPaths) > 0 {
		t.Logf("Missing expected paths:\n%s", strings.Join(sets.List[string](missingPaths), "\n"))
		t.Error("Missing expected paths. Verify VisitPodConfigmapNames() is correctly finding the missing paths, then correct expectedPaths")
	}
	if extraPaths := collectPaths.Difference(expectedPaths); len(extraPaths) > 0 {
		t.Logf("Extra paths:\n%s", strings.Join(sets.List[string](extraPaths), "\n"))
		t.Error("Extra fields with resource in the name found. Verify VisitPodConfigmapNames() is including these fields if appropriate, then correct expectedPaths")
	}

	if missingNames := expectedPaths.Difference(extractedNames); len(missingNames) > 0 {
		t.Logf("Missing expected names:\n%s", strings.Join(sets.List[string](missingNames), "\n"))
		t.Error("Missing expected names. Verify the pod stub above includes these references, then verify VisitPodConfigmapNames() is correctly finding the missing names")
	}
	if extraNames := extractedNames.Difference(expectedPaths); len(extraNames) > 0 {
		t.Logf("Extra names:\n%s", strings.Join(sets.List[string](extraNames), "\n"))
		t.Error("Extra names extracted. Verify VisitPodConfigmapNames() is correctly extracting resource names")
	}

	// 空名字段必须被忽略，否则会向调用方返回无法使用的 ConfigMap 名。
	// emptyPod 是对象名为空的测试桩。
	emptyPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				EnvFrom: []v1.EnvFromSource{{
					ConfigMapRef: &v1.ConfigMapEnvSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: ""}}}}}},
		},
	}
	VisitPodConfigmapNames(emptyPod, func(name string) bool {
		t.Fatalf("expected no empty names collected, got %q", name)
		return false
	})
}

// newPod 构造只带一个 PodReady condition 的 Pod。
// beforeSec 控制条件最后一次变化距今多久，用于测试 minReadySeconds 语义。
func newPod(now metav1.Time, ready bool, beforeSec int) *v1.Pod {
	conditionStatus := v1.ConditionFalse
	if ready {
		conditionStatus = v1.ConditionTrue
	}
	return &v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:               v1.PodReady,
					LastTransitionTime: metav1.NewTime(now.Time.Add(-1 * time.Duration(beforeSec) * time.Second)),
					Status:             conditionStatus,
				},
			},
		},
	}
}

// TestIsPodAvailable 验证 IsPodAvailable 的判定：Pod 必须 Ready 且
// Ready 状态的持续时间已达到 minReadySeconds；时间戳缺失或未满时长都返回 false。
func TestIsPodAvailable(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		pod             *v1.Pod
		minReadySeconds int32
		expected        bool
	}{
		{
			pod:             newPod(now, false, 0),
			minReadySeconds: 0,
			expected:        false,
		},
		{
			pod:             newPod(now, true, 0),
			minReadySeconds: 1,
			expected:        false,
		},
		{
			// 零时间戳不能当作“已经 Ready 很久”，否则会误判为可用。
			pod: func() *v1.Pod {
				pod := newPod(now, true, 0)
				pod.Status.Conditions[0].LastTransitionTime = metav1.Time{}
				return pod
			}(),
			minReadySeconds: 1,
			expected:        false,
		},
		{
			pod:             newPod(now, true, 0),
			minReadySeconds: 0,
			expected:        true,
		},
		{
			pod:             newPod(now, true, 51),
			minReadySeconds: 50,
			expected:        true,
		},
		{
			pod:             newPod(now, true, 51),
			minReadySeconds: 51,
			expected:        true,
		},
		{
			pod:             newPod(now, true, 51),
			minReadySeconds: 52,
			expected:        false,
		},
	}

	for i, test := range tests {
		isAvailable := IsPodAvailable(test.pod, test.minReadySeconds, now)
		if isAvailable != test.expected {
			t.Errorf("[tc #%d] expected available pod: %t, got: %t", i, test.expected, isAvailable)
		}
	}
}

// TestIsPodTerminal 验证只有 Succeeded/Failed 是终态；
// Unknown/Pending/Running 以及空 phase 都不是。
func TestIsPodTerminal(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		podPhase v1.PodPhase
		expected bool
	}{
		{
			podPhase: v1.PodFailed,
			expected: true,
		},
		{
			podPhase: v1.PodSucceeded,
			expected: true,
		},
		{
			podPhase: v1.PodUnknown,
			expected: false,
		},
		{
			podPhase: v1.PodPending,
			expected: false,
		},
		{
			podPhase: v1.PodRunning,
			expected: false,
		},
		{
			expected: false,
		},
	}

	for i, test := range tests {
		pod := newPod(now, true, 0)
		pod.Status.Phase = test.podPhase
		isTerminal := IsPodTerminal(pod)
		if isTerminal != test.expected {
			t.Errorf("[tc #%d] expected terminal pod: %t, got: %t", i, test.expected, isTerminal)
		}
	}
}

// TestGetContainerStatus 验证按名字查找 ContainerStatus 的两个 API：
// GetContainerStatus 在找不到时返回零值，GetExistingContainerStatus 同理；
// 空名字和 nil 状态列表都要安全返回零值而不 panic。
func TestGetContainerStatus(t *testing.T) {
	type ExpectedStruct struct {
		status v1.ContainerStatus
		exists bool
	}

	tests := []struct {
		status   []v1.ContainerStatus
		name     string
		expected ExpectedStruct
		desc     string
	}{
		{
			status:   []v1.ContainerStatus{{Name: "test1", Ready: false, Image: "image1"}, {Name: "test2", Ready: true, Image: "image1"}},
			name:     "test1",
			expected: ExpectedStruct{status: v1.ContainerStatus{Name: "test1", Ready: false, Image: "image1"}, exists: true},
			desc:     "retrieve ContainerStatus with Name=\"test1\"",
		},
		{
			status:   []v1.ContainerStatus{{Name: "test2", Ready: false, Image: "image2"}},
			name:     "test1",
			expected: ExpectedStruct{status: v1.ContainerStatus{}, exists: false},
			desc:     "no matching ContainerStatus with Name=\"test1\"",
		},
		{
			status:   []v1.ContainerStatus{{Name: "test3", Ready: false, Image: "image3"}},
			name:     "",
			expected: ExpectedStruct{status: v1.ContainerStatus{}, exists: false},
			desc:     "retrieve an empty ContainerStatus with container name empty",
		},
		{
			status:   nil,
			name:     "",
			expected: ExpectedStruct{status: v1.ContainerStatus{}, exists: false},
			desc:     "retrieve an empty ContainerStatus with status nil",
		},
	}

	for _, test := range tests {
		resultStatus, exists := GetContainerStatus(test.status, test.name)
		assert.Equal(t, test.expected.status, resultStatus, "GetContainerStatus: "+test.desc)
		assert.Equal(t, test.expected.exists, exists, "GetContainerStatus: "+test.desc)

		// 两个函数语义一致，用同一组期望值验证，避免实现漂移。
		resultStatus = GetExistingContainerStatus(test.status, test.name)
		assert.Equal(t, test.expected.status, resultStatus, "GetExistingContainerStatus: "+test.desc)
	}
}

// TestGetIndexOfContainerStatus 验证按名字返回状态数组下标；
// 找不到时 exists 为 false 且下标无意义（约定返回 0）。
func TestGetIndexOfContainerStatus(t *testing.T) {
	testStatus := []v1.ContainerStatus{
		{
			Name:  "c1",
			Ready: false,
			Image: "image1",
		},
		{
			Name:  "c2",
			Ready: true,
			Image: "image1",
		},
	}

	tests := []struct {
		desc           string
		containerName  string
		expectedExists bool
		expectedIndex  int
	}{
		{
			desc:           "first container",
			containerName:  "c1",
			expectedExists: true,
			expectedIndex:  0,
		},
		{
			desc:           "second container",
			containerName:  "c2",
			expectedExists: true,
			expectedIndex:  1,
		},
		{
			desc:           "non-existent container",
			containerName:  "c3",
			expectedExists: false,
			expectedIndex:  0,
		},
	}

	for _, test := range tests {
		idx, exists := GetIndexOfContainerStatus(testStatus, test.containerName)
		assert.Equal(t, test.expectedExists, exists, "GetIndexOfContainerStatus: "+test.desc)
		assert.Equal(t, test.expectedIndex, idx, "GetIndexOfContainerStatus: "+test.desc)
	}
}

// TestUpdatePodCondition 验证 UpdatePodCondition 返回“是否发生了变更”：
// 条件完全相同则不更新，Type 或 Status 不同则需要更新。
func TestUpdatePodCondition(t *testing.T) {
	time := metav1.Now()

	podStatus := v1.PodStatus{
		Conditions: []v1.PodCondition{
			{
				Type:               v1.PodReady,
				Status:             v1.ConditionTrue,
				Reason:             "successfully",
				Message:            "sync pod successfully",
				LastProbeTime:      time,
				LastTransitionTime: metav1.NewTime(time.Add(1000)),
			},
		},
	}
	tests := []struct {
		status     *v1.PodStatus
		conditions v1.PodCondition
		expected   bool
		desc       string
	}{
		{
			status: &podStatus,
			conditions: v1.PodCondition{
				Type:               v1.PodReady,
				Status:             v1.ConditionTrue,
				Reason:             "successfully",
				Message:            "sync pod successfully",
				LastProbeTime:      time,
				LastTransitionTime: metav1.NewTime(time.Add(1000))},
			expected: false,
			desc:     "all equal, no update",
		},
		{
			status: &podStatus,
			conditions: v1.PodCondition{
				Type:               v1.PodScheduled,
				Status:             v1.ConditionTrue,
				Reason:             "successfully",
				Message:            "sync pod successfully",
				LastProbeTime:      time,
				LastTransitionTime: metav1.NewTime(time.Add(1000))},
			expected: true,
			desc:     "not equal Type, should get updated",
		},
		{
			status: &podStatus,
			conditions: v1.PodCondition{
				Type:               v1.PodReady,
				Status:             v1.ConditionFalse,
				Reason:             "successfully",
				Message:            "sync pod successfully",
				LastProbeTime:      time,
				LastTransitionTime: metav1.NewTime(time.Add(1000))},
			expected: true,
			desc:     "not equal Status, should get updated",
		},
	}

	for _, test := range tests {
		resultStatus := UpdatePodCondition(test.status, &test.conditions)

		assert.Equal(t, test.expected, resultStatus, test.desc)
	}
}

// TestGetContainersReadyCondition 验证从 PodStatus 中取出 ContainersReady 条件；
// 不存在时返回 nil 而不是零值条件。
func TestGetContainersReadyCondition(t *testing.T) {
	time := metav1.Now()

	containersReadyCondition := v1.PodCondition{
		Type:               v1.ContainersReady,
		Status:             v1.ConditionTrue,
		Reason:             "successfully",
		Message:            "sync pod successfully",
		LastProbeTime:      time,
		LastTransitionTime: metav1.NewTime(time.Add(1000)),
	}

	tests := []struct {
		desc              string
		podStatus         v1.PodStatus
		expectedCondition *v1.PodCondition
	}{
		{
			desc: "containers ready condition exists",
			podStatus: v1.PodStatus{
				Conditions: []v1.PodCondition{containersReadyCondition},
			},
			expectedCondition: &containersReadyCondition,
		},
		{
			desc: "containers ready condition does not exist",
			podStatus: v1.PodStatus{
				Conditions: []v1.PodCondition{},
			},
			expectedCondition: nil,
		},
	}

	for _, test := range tests {
		containersReadyCondition := GetContainersReadyCondition(test.podStatus)
		assert.Equal(t, test.expectedCondition, containersReadyCondition, test.desc)
	}
}

// TestIsContainersReadyConditionTrue 验证只有 ContainersReady=True 才返回 true；
// False 或条件缺失都返回 false。
func TestIsContainersReadyConditionTrue(t *testing.T) {
	time := metav1.Now()

	tests := []struct {
		desc      string
		podStatus v1.PodStatus
		expected  bool
	}{
		{
			desc: "containers ready condition is true",
			podStatus: v1.PodStatus{
				Conditions: []v1.PodCondition{
					{
						Type:               v1.ContainersReady,
						Status:             v1.ConditionTrue,
						Reason:             "successfully",
						Message:            "sync pod successfully",
						LastProbeTime:      time,
						LastTransitionTime: metav1.NewTime(time.Add(1000)),
					},
				},
			},
			expected: true,
		},
		{
			desc: "containers ready condition is false",
			podStatus: v1.PodStatus{
				Conditions: []v1.PodCondition{
					{
						Type:               v1.ContainersReady,
						Status:             v1.ConditionFalse,
						Reason:             "successfully",
						Message:            "sync pod successfully",
						LastProbeTime:      time,
						LastTransitionTime: metav1.NewTime(time.Add(1000)),
					},
				},
			},
			expected: false,
		},
		{
			desc: "containers ready condition is empty",
			podStatus: v1.PodStatus{
				Conditions: []v1.PodCondition{},
			},
			expected: false,
		},
	}

	for _, test := range tests {
		isContainersReady := IsContainersReadyConditionTrue(test.podStatus)
		assert.Equal(t, test.expected, isContainersReady, test.desc)
	}
}

// TestCalculatePodStatusObservedGeneration 验证 PodLevelResourcesTracking 相关逻辑：
// 未设置 observedGeneration 时回退到 metadata.Generation，已设置则保留原值。
func TestCalculatePodStatusObservedGeneration(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.Pod
		expected int64
	}{
		{
			name: "pod with no observedGeneration/PodObservedGenerationTracking=true",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 5,
				},
			},
			expected: 5,
		},
		{
			name: "pod with observedGeneration/PodObservedGenerationTracking=true",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 5,
				},
				Status: v1.PodStatus{
					ObservedGeneration: 5,
				},
			},
			expected: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, CalculatePodStatusObservedGeneration(tc.pod))
		})
	}
}

// TestCalculatePodConditionObservedGeneration 与上例对应，但针对单个 PodCondition：
// condition 未设置 observedGeneration 时使用 Pod 的 Generation。
func TestCalculatePodConditionObservedGeneration(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.Pod
		expected int64
	}{
		{
			name: "pod with no observedGeneration/PodObservedGenerationTracking=true",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 5,
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{{
						Type: v1.PodReady,
					}},
				},
			},
			expected: 5,
		},
		{
			name: "pod with observedGeneration/PodObservedGenerationTracking=true",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 5,
				},
				Status: v1.PodStatus{
					Conditions: []v1.PodCondition{{
						Type:               v1.PodReady,
						ObservedGeneration: 5,
					}},
				},
			},
			expected: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, CalculatePodConditionObservedGeneration(&tc.pod.Status, tc.pod.Generation, v1.PodReady))
		})
	}
}

// TestIsContainerRestartable 验证容器可重启性判定的优先级：
// 容器级 restartPolicyRules/restartPolicy 优先，未设置时回退到 Pod 级 RestartPolicy；
// 只有 Never 表示不可重启，空策略按可重启处理。
func TestIsContainerRestartable(t *testing.T) {
	var (
		containerRestartPolicyAlways    = v1.ContainerRestartPolicyAlways
		containerRestartPolicyOnFailure = v1.ContainerRestartPolicyOnFailure
		containerRestartPolicyNever     = v1.ContainerRestartPolicyNever
	)

	testCases := []struct {
		name      string
		podSpec   v1.PodSpec
		container v1.Container
		expected  bool
	}{
		{
			// 即使容器和 Pod 都是 Never，Restart 动作规则也强制可重启。
			name:    "Container: Rule action 'Restart' should always be true",
			podSpec: v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyNever,
				RestartPolicyRules: []v1.ContainerRestartRule{
					{Action: v1.ContainerRestartRuleActionRestart},
				},
			},
			expected: true,
		},
		{
			name:    "Container: Policy 'Always' is restartable",
			podSpec: v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyAlways,
			},
			expected: true,
		},
		{
			name:    "Container: Policy 'OnFailure' is restartable",
			podSpec: v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyOnFailure,
			},
			expected: true,
		},
		{
			name:    "Container: Policy 'Never' is not restartable",
			podSpec: v1.PodSpec{RestartPolicy: v1.RestartPolicyAlways},
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyNever,
			},
			expected: false,
		},
		{
			name:      "Pod Fallback: Policy 'Always' is restartable",
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyAlways},
			container: v1.Container{},
			expected:  true,
		},
		{
			name:      "Pod Fallback: Policy 'OnFailure' is restartable",
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyOnFailure},
			container: v1.Container{},
			expected:  true,
		},
		{
			name:      "Pod Fallback: Policy 'Never' is not restartable",
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			container: v1.Container{},
			expected:  false,
		},
		{
			name:      "Pod Fallback: Default empty policy is restartable (since it's not 'Never')",
			podSpec:   v1.PodSpec{},
			container: v1.Container{},
			expected:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsContainerRestartable(tc.podSpec, tc.container)
			if got != tc.expected {
				t.Errorf("IsContainerRestartable() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestContainerHasRestartablePolicy 验证 ContainerShouldRestart 的完整判定链：
// 1) 退出生码规则（In/NotIn + Restart/RestartAllContainers）命中即为 true；
// 2) 规则不命中时回退容器级策略；3) 再回退 Pod 级策略（最低优先级）。
// 因为规则字段受特性门控保护，测试开头需要显式打开相关特性。
func TestContainerHasRestartablePolicy(t *testing.T) {
	var (
		containerRestartPolicyAlways    = v1.ContainerRestartPolicyAlways
		containerRestartPolicyOnFailure = v1.ContainerRestartPolicyOnFailure
		containerRestartPolicyNever     = v1.ContainerRestartPolicyNever
	)

	testCases := []struct {
		name      string
		container v1.Container
		podSpec   v1.PodSpec
		podStatus v1.PodStatus
		exitCode  int32
		expected  bool
	}{
		{
			name: "Rule: 'In' operator matches with 'Restart' action",
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyNever,
				RestartPolicyRules: []v1.ContainerRestartRule{
					{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{42, 50, 60},
						},
					},
				},
			},
			exitCode: 42,
			expected: true,
		},
		{
			name: "Rule: 'NotIn' operator matches with 'Restart' action",
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyNever,
				RestartPolicyRules: []v1.ContainerRestartRule{
					{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpNotIn,
							Values:   []int32{0, 1, 2},
						},
					},
				},
			},
			exitCode: 99,
			expected: true,
		},
		{
			name: "Rule: 'In' operator matches with 'RestartAllContainers' action",
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyNever,
				RestartPolicyRules: []v1.ContainerRestartRule{
					{
						Action: v1.ContainerRestartRuleActionRestartAllContainers,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{42, 50, 60},
						},
					},
				},
			},
			exitCode: 42,
			expected: true,
		},
		{
			name: "Rule: 'In' operator does not match, should fall back to container policy",
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyNever,
				RestartPolicyRules: []v1.ContainerRestartRule{
					{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{10, 20},
						},
					},
				},
			},
			exitCode: 30,
			expected: false,
		},
		{
			name: "Rule: 'NotIn' operator does not match, should fall back to container policy",
			container: v1.Container{
				RestartPolicy: &containerRestartPolicyNever,
				RestartPolicyRules: []v1.ContainerRestartRule{
					{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpNotIn,
							Values:   []int32{10, 20},
						},
					},
				},
			},
			exitCode: 10,
			expected: false,
		},
		{
			name:      "Container Policy: 'Always'",
			container: v1.Container{RestartPolicy: &containerRestartPolicyAlways},
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			exitCode:  0,
			expected:  true,
		},
		{
			name:      "Container Policy: 'OnFailure' with exit code 0",
			container: v1.Container{RestartPolicy: &containerRestartPolicyOnFailure},
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			exitCode:  0,
			expected:  false,
		},
		{
			name:      "Container Policy: 'OnFailure' with non-zero exit code",
			container: v1.Container{RestartPolicy: &containerRestartPolicyOnFailure},
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			exitCode:  1,
			expected:  true,
		},
		{
			name:      "Container Policy: 'Never' should return false",
			container: v1.Container{RestartPolicy: &containerRestartPolicyNever},
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyAlways},
			exitCode:  1,
			expected:  false,
		},
		// 以下用例覆盖最低优先级的 Pod 级策略回退。
		// --- Pod 级重启策略场景（最低优先级） ---
		{
			name:      "Pod Policy: Fallback to pod 'Always'",
			container: v1.Container{},
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyAlways},
			exitCode:  0,
			expected:  true,
		},
		{
			name:      "Pod Policy: Fallback to pod 'OnFailure' with non-zero exit code",
			container: v1.Container{},
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyOnFailure},
			exitCode:  1,
			expected:  true,
		},
		{
			name:      "Pod Policy: Fallback to pod 'Never'",
			container: v1.Container{},
			podSpec:   v1.PodSpec{RestartPolicy: v1.RestartPolicyNever},
			exitCode:  1,
			expected:  false,
		},
		{
			name:      "Pod Policy: Fallback with no policies specified should return true",
			container: v1.Container{},
			podSpec:   v1.PodSpec{},
			exitCode:  0,
			expected:  true,
		},
	}
	// 重启规则字段默认可能关闭；显式开启三个相关特性，确保被测逻辑可达。
	// SetFeatureGatesDuringTest 会在用例结束后恢复全局状态。
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.ContainerRestartRules:                true,
		features.NodeDeclaredFeatures:                 true,
		features.RestartAllContainersOnContainerExits: true,
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 用 tc.name 作为子测试名，失败时可以直接看出是哪条规则组合。
			// 执行被测函数。
			got := ContainerShouldRestart(tc.container, tc.podSpec, tc.exitCode)

			// 断言结果。
			if got != tc.expected {
				t.Errorf("ContainerHasRestartablePolicy() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestAllContainersCouldRestart 验证 Pod 中存在 RestartAllContainers 规则时返回 true，
// 规则可位于 init 容器、sidecar 或普通容器；没有任何规则时返回 false。
func TestAllContainersCouldRestart(t *testing.T) {
	restartPolicyAlways := v1.ContainerRestartPolicyAlways

	testCases := []struct {
		name     string
		podSpec  *v1.PodSpec
		expected bool
	}{
		{
			name: "Pod with rules in init containers",
			podSpec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						RestartPolicyRules: []v1.ContainerRestartRule{
							{
								Action: v1.ContainerRestartRuleActionRestartAllContainers,
								ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
									Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
									Values:   []int32{1},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Pod with rules in sidecar containers",
			podSpec: &v1.PodSpec{
				InitContainers: []v1.Container{
					{
						RestartPolicy: &restartPolicyAlways,
						RestartPolicyRules: []v1.ContainerRestartRule{
							{
								Action: v1.ContainerRestartRuleActionRestartAllContainers,
								ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
									Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
									Values:   []int32{1},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Pod with rules in regular containers",
			podSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						RestartPolicy: &restartPolicyAlways,
						RestartPolicyRules: []v1.ContainerRestartRule{
							{
								Action: v1.ContainerRestartRuleActionRestartAllContainers,
								ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
									Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
									Values:   []int32{1},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "Pod without rules",
			podSpec:  &v1.PodSpec{},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := AllContainersCouldRestart(tc.podSpec)
			if got != tc.expected {
				t.Errorf("AllContainersRestarting() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestFindMatchingContainerRestartRule 验证按退出生码匹配规则：
// In/NotIn 操作符语义、多条规则命中时取第一条、无匹配返回 false 和零值规则。
func TestFindMatchingContainerRestartRule(t *testing.T) {
	// 三条共享规则分别代表 In 命中、NotIn 命中和更靠后的 In 规则；
	// 复用它们便于断言“返回的是哪条规则”以及“优先返回第一条”。
	ruleIn := v1.ContainerRestartRule{
		Action: v1.ContainerRestartRuleActionRestart,
		ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
			Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
			Values:   []int32{42, 50, 60},
		},
	}
	ruleNotIn := v1.ContainerRestartRule{
		Action: v1.ContainerRestartRuleActionRestart,
		ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
			Operator: v1.ContainerRestartRuleOnExitCodesOpNotIn,
			Values:   []int32{0, 1, 2},
		},
	}
	ruleIn42 := v1.ContainerRestartRule{
		Action: v1.ContainerRestartRuleActionRestart,
		ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
			Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
			Values:   []int32{42},
		},
	}
	emptyRule := v1.ContainerRestartRule{}

	testCases := []struct {
		name          string
		container     v1.Container
		exitCode      int32
		expectedFound bool
		expectedRule  v1.ContainerRestartRule
	}{
		{
			name: "a rule with op In matches",
			container: v1.Container{
				RestartPolicyRules: []v1.ContainerRestartRule{ruleIn},
			},
			exitCode:      42,
			expectedFound: true,
			expectedRule:  ruleIn,
		},
		{
			name: "a rule with op NotIn matches",
			container: v1.Container{
				RestartPolicyRules: []v1.ContainerRestartRule{ruleNotIn},
			},
			exitCode:      99,
			expectedFound: true,
			expectedRule:  ruleNotIn,
		},
		{
			// 两条规则都匹配 42 时，必须返回先声明的那条，说明匹配是短路的。
			name: "matches the first rule if two rules both matched",
			container: v1.Container{
				RestartPolicyRules: []v1.ContainerRestartRule{ruleIn, ruleIn42},
			},
			exitCode:      42,
			expectedFound: true,
			expectedRule:  ruleIn,
		},
		{
			name: "no rules matched",
			container: v1.Container{
				RestartPolicyRules: []v1.ContainerRestartRule{ruleIn},
			},
			exitCode:      100,
			expectedFound: false,
			expectedRule:  emptyRule,
		},
		{
			name:          "no rules defined",
			container:     v1.Container{},
			exitCode:      0,
			expectedFound: false,
			expectedRule:  emptyRule,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rule, found := FindMatchingContainerRestartRule(tc.container, tc.exitCode)
			if found != tc.expectedFound {
				t.Errorf("FindMatchingContainerRestartRule() found = %v, want %v", found, tc.expectedFound)
			}
			if !reflect.DeepEqual(rule, tc.expectedRule) {
				t.Errorf("FindMatchingContainerRestartRule() rule = %+v, want %+v", rule, tc.expectedRule)
			}
		})
	}
}
