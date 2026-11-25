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

package v1_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/features"

	// 仅用于触发 init：确保 v1 相关类型已注册到 scheme 中
	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

// featureGatedPodDefaults 记录仅在所有特性门控都开启时才会应用到 Pod 上的默认值。
// 这些默认值在 SetDefaults_Pod 中设置（而非 SetDefaults_PodSpec），避免读取旧版本数据时触发工作负载的无谓滚动更新。
var featureGatedPodDefaults = map[string]string{
	".Spec.Containers[0].Lifecycle.PostStart.HTTPGet.Protocol":                                           `"HTTP1"`,
	".Spec.Containers[0].Lifecycle.PreStop.HTTPGet.Protocol":                                             `"HTTP1"`,
	".Spec.Containers[0].LivenessProbe.ProbeHandler.HTTPGet.Protocol":                                    `"HTTP1"`,
	".Spec.Containers[0].ReadinessProbe.ProbeHandler.HTTPGet.Protocol":                                   `"HTTP1"`,
	".Spec.Containers[0].StartupProbe.ProbeHandler.HTTPGet.Protocol":                                     `"HTTP1"`,
	".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PostStart.HTTPGet.Protocol":         `"HTTP1"`,
	".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PreStop.HTTPGet.Protocol":           `"HTTP1"`,
	".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.ProbeHandler.HTTPGet.Protocol":  `"HTTP1"`,
	".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.ProbeHandler.HTTPGet.Protocol": `"HTTP1"`,
	".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.ProbeHandler.HTTPGet.Protocol":   `"HTTP1"`,
	".Spec.InitContainers[0].Lifecycle.PostStart.HTTPGet.Protocol":                                       `"HTTP1"`,
	".Spec.InitContainers[0].Lifecycle.PreStop.HTTPGet.Protocol":                                         `"HTTP1"`,
	".Spec.InitContainers[0].LivenessProbe.ProbeHandler.HTTPGet.Protocol":                                `"HTTP1"`,
	".Spec.InitContainers[0].ReadinessProbe.ProbeHandler.HTTPGet.Protocol":                               `"HTTP1"`,
	".Spec.InitContainers[0].StartupProbe.ProbeHandler.HTTPGet.Protocol":                                 `"HTTP1"`,
}

// TestWorkloadDefaults 检测 PodTemplateSpec 内部默认值的变化。
// 该类型中的默认值变更可能在 API Server 升级后引发工作负载的无谓滚动更新。
func TestWorkloadDefaults(t *testing.T) {
	t.Run("enabled_features", func(t *testing.T) { testWorkloadDefaults(t, true) })
	// 分别在特性门控全开与全关两种状态下运行，确保默认值集合不受门控状态影响。
	t.Run("disabled_features", func(t *testing.T) { testWorkloadDefaults(t, false) })
}
func testWorkloadDefaults(t *testing.T, featuresEnabled bool) {
	setAllFeatures(t, featuresEnabled)
	// PodTemplateSpec 下新增默认值只有在读取旧版本数据时不会被应用才是可接受的。
	// 禁止：新增字段 `MyField *bool` 并将其默认为非 nil 值。
	// 禁止：对过去不设默认值的已有字段 `MyField *bool` 新增默认值。
	// 禁止：修改已有的默认值。
	// 允许：新增字段 `MyContainer *MyType` 并对其子字段设默认值（如 `MyContainer.MyChildField`），但前提是 MyContainer 本身非 nil。
	// 期望的默认值清单按字段路径索引，值是该字段序列化后的 JSON 文本；
	// 一旦新增或改动默认值，这里就会失配，从而提醒维护者评估升级兼容性。
	expectedDefaults := map[string]string{
		".Spec.Containers[0].Env[0].ValueFrom.FieldRef.APIVersion":       `"v1"`,
		".Spec.Containers[0].Env[0].ValueFrom.FileKeyRef.Optional":       "false",
		".Spec.Containers[0].ImagePullPolicy":                            `"IfNotPresent"`,
		".Spec.Containers[0].Lifecycle.PostStart.HTTPGet.Path":           `"/"`,
		".Spec.Containers[0].Lifecycle.PostStart.HTTPGet.Scheme":         `"HTTP"`,
		".Spec.Containers[0].Lifecycle.PreStop.HTTPGet.Path":             `"/"`,
		".Spec.Containers[0].Lifecycle.PreStop.HTTPGet.Scheme":           `"HTTP"`,
		".Spec.Containers[0].LivenessProbe.FailureThreshold":             `3`,
		".Spec.Containers[0].LivenessProbe.ProbeHandler.HTTPGet.Path":    `"/"`,
		".Spec.Containers[0].LivenessProbe.ProbeHandler.HTTPGet.Scheme":  `"HTTP"`,
		".Spec.Containers[0].LivenessProbe.PeriodSeconds":                `10`,
		".Spec.Containers[0].LivenessProbe.SuccessThreshold":             `1`,
		".Spec.Containers[0].LivenessProbe.TimeoutSeconds":               `1`,
		".Spec.Containers[0].Ports[0].Protocol":                          `"TCP"`,
		".Spec.Containers[0].ReadinessProbe.FailureThreshold":            `3`,
		".Spec.Containers[0].ReadinessProbe.ProbeHandler.HTTPGet.Path":   `"/"`,
		".Spec.Containers[0].ReadinessProbe.ProbeHandler.HTTPGet.Scheme": `"HTTP"`,
		".Spec.Containers[0].ReadinessProbe.PeriodSeconds":               `10`,
		".Spec.Containers[0].ReadinessProbe.SuccessThreshold":            `1`,
		".Spec.Containers[0].ReadinessProbe.TimeoutSeconds":              `1`,
		".Spec.Containers[0].StartupProbe.FailureThreshold":              "3",
		".Spec.Containers[0].StartupProbe.ProbeHandler.HTTPGet.Path":     `"/"`,
		".Spec.Containers[0].StartupProbe.ProbeHandler.HTTPGet.Scheme":   `"HTTP"`,
		".Spec.Containers[0].StartupProbe.PeriodSeconds":                 "10",
		".Spec.Containers[0].StartupProbe.SuccessThreshold":              "1",
		".Spec.Containers[0].StartupProbe.TimeoutSeconds":                "1",
		".Spec.Containers[0].TerminationMessagePath":                     `"/dev/termination-log"`,
		".Spec.Containers[0].TerminationMessagePolicy":                   `"File"`,
		".Spec.Containers[0].LivenessProbe.ProbeHandler.GRPC.Service":    `""`,
		".Spec.Containers[0].ReadinessProbe.ProbeHandler.GRPC.Service":   `""`,
		".Spec.Containers[0].StartupProbe.ProbeHandler.GRPC.Service":     `""`,
		".Spec.DNSPolicy": `"ClusterFirst"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.ProbeHandler.HTTPGet.Path":    `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.ProbeHandler.HTTPGet.Scheme":  `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.ProbeHandler.HTTPGet.Path":   `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.ProbeHandler.HTTPGet.Scheme": `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.ProbeHandler.HTTPGet.Path":     `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.ProbeHandler.HTTPGet.Scheme":   `"HTTP"`,
		".Spec.InitContainers[0].LivenessProbe.ProbeHandler.HTTPGet.Path":                                  `"/"`,
		".Spec.InitContainers[0].LivenessProbe.ProbeHandler.HTTPGet.Scheme":                                `"HTTP"`,
		".Spec.InitContainers[0].ReadinessProbe.ProbeHandler.HTTPGet.Path":                                 `"/"`,
		".Spec.InitContainers[0].ReadinessProbe.ProbeHandler.HTTPGet.Scheme":                               `"HTTP"`,
		".Spec.InitContainers[0].StartupProbe.ProbeHandler.HTTPGet.Path":                                   `"/"`,
		".Spec.InitContainers[0].StartupProbe.ProbeHandler.HTTPGet.Scheme":                                 `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Env[0].ValueFrom.FieldRef.APIVersion":       `"v1"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Env[0].ValueFrom.FileKeyRef.Optional":       "false",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ImagePullPolicy":                            `"IfNotPresent"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PostStart.HTTPGet.Path":           `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PostStart.HTTPGet.Scheme":         `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PreStop.HTTPGet.Path":             `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PreStop.HTTPGet.Scheme":           `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.FailureThreshold":             "3",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.ProbeHandler.GRPC.Service":    `""`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.PeriodSeconds":                "10",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.SuccessThreshold":             "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.TimeoutSeconds":               "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Ports[0].Protocol":                          `"TCP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.FailureThreshold":            "3",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.ProbeHandler.GRPC.Service":   `""`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.PeriodSeconds":               "10",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.SuccessThreshold":            "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.TimeoutSeconds":              "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.FailureThreshold":              "3",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.ProbeHandler.GRPC.Service":     `""`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.PeriodSeconds":                 "10",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.SuccessThreshold":              "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.TimeoutSeconds":                "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.TerminationMessagePath":                     `"/dev/termination-log"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.TerminationMessagePolicy":                   `"File"`,
		".Spec.InitContainers[0].Env[0].ValueFrom.FieldRef.APIVersion":                                     `"v1"`,
		".Spec.InitContainers[0].Env[0].ValueFrom.FileKeyRef.Optional":                                     "false",
		".Spec.InitContainers[0].ImagePullPolicy":                                                          `"IfNotPresent"`,
		".Spec.InitContainers[0].Lifecycle.PostStart.HTTPGet.Path":                                         `"/"`,
		".Spec.InitContainers[0].Lifecycle.PostStart.HTTPGet.Scheme":                                       `"HTTP"`,
		".Spec.InitContainers[0].Lifecycle.PreStop.HTTPGet.Path":                                           `"/"`,
		".Spec.InitContainers[0].Lifecycle.PreStop.HTTPGet.Scheme":                                         `"HTTP"`,
		".Spec.InitContainers[0].LivenessProbe.FailureThreshold":                                           `3`,
		".Spec.InitContainers[0].LivenessProbe.ProbeHandler.GRPC.Service":                                  `""`,
		".Spec.InitContainers[0].LivenessProbe.PeriodSeconds":                                              `10`,
		".Spec.InitContainers[0].LivenessProbe.SuccessThreshold":                                           `1`,
		".Spec.InitContainers[0].LivenessProbe.TimeoutSeconds":                                             `1`,
		".Spec.InitContainers[0].Ports[0].Protocol":                                                        `"TCP"`,
		".Spec.InitContainers[0].ReadinessProbe.FailureThreshold":                                          `3`,
		".Spec.InitContainers[0].ReadinessProbe.ProbeHandler.GRPC.Service":                                 `""`,
		".Spec.InitContainers[0].ReadinessProbe.PeriodSeconds":                                             `10`,
		".Spec.InitContainers[0].ReadinessProbe.SuccessThreshold":                                          `1`,
		".Spec.InitContainers[0].ReadinessProbe.TimeoutSeconds":                                            `1`,
		".Spec.InitContainers[0].StartupProbe.FailureThreshold":                                            "3",
		".Spec.InitContainers[0].StartupProbe.ProbeHandler.GRPC.Service":                                   `""`,
		".Spec.InitContainers[0].StartupProbe.PeriodSeconds":                                               "10",
		".Spec.InitContainers[0].StartupProbe.SuccessThreshold":                                            "1",
		".Spec.InitContainers[0].StartupProbe.TimeoutSeconds":                                              "1",
		".Spec.InitContainers[0].TerminationMessagePath":                                                   `"/dev/termination-log"`,
		".Spec.InitContainers[0].TerminationMessagePolicy":                                                 `"File"`,
		".Spec.RestartPolicy":                                                                         `"Always"`,
		".Spec.SchedulerName":                                                                         `"default-scheduler"`,
		".Spec.SecurityContext":                                                                       `{}`,
		".Spec.TerminationGracePeriodSeconds":                                                         `30`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.CachingMode":                                         `"ReadWrite"`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.FSType":                                              `"ext4"`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.Kind":                                                `"Shared"`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.ReadOnly":                                            `false`,
		".Spec.Volumes[0].VolumeSource.ConfigMap.DefaultMode":                                         `420`,
		".Spec.Volumes[0].VolumeSource.DownwardAPI.DefaultMode":                                       `420`,
		".Spec.Volumes[0].VolumeSource.DownwardAPI.Items[0].FieldRef.APIVersion":                      `"v1"`,
		".Spec.Volumes[0].VolumeSource.EmptyDir":                                                      `{}`,
		".Spec.Volumes[0].VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.VolumeMode":                 `"Filesystem"`,
		".Spec.Volumes[0].VolumeSource.HostPath.Type":                                                 `""`,
		".Spec.Volumes[0].VolumeSource.ISCSI.ISCSIInterface":                                          `"default"`,
		".Spec.Volumes[0].VolumeSource.Image.PullPolicy":                                              `"IfNotPresent"`,
		".Spec.Volumes[0].VolumeSource.Projected.DefaultMode":                                         `420`,
		".Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items[0].FieldRef.APIVersion": `"v1"`,
		".Spec.Volumes[0].VolumeSource.Projected.Sources[0].ServiceAccountToken.ExpirationSeconds":    `3600`,
		".Spec.Volumes[0].VolumeSource.RBD.Keyring":                                                   `"/etc/ceph/keyring"`,
		".Spec.Volumes[0].VolumeSource.RBD.RBDPool":                                                   `"rbd"`,
		".Spec.Volumes[0].VolumeSource.RBD.RadosUser":                                                 `"admin"`,
		".Spec.Volumes[0].VolumeSource.ScaleIO.FSType":                                                `"xfs"`,
		".Spec.Volumes[0].VolumeSource.ScaleIO.StorageMode":                                           `"ThinProvisioned"`,
		".Spec.Volumes[0].VolumeSource.Secret.DefaultMode":                                            `420`,
	}
	t.Run("empty PodTemplateSpec", func(t *testing.T) {
		rc := &v1.ReplicationController{Spec: v1.ReplicationControllerSpec{Template: &v1.PodTemplateSpec{}}}
		// 用一个完全空的模板做探测，任何非零字段都必然来自默认化逻辑。
		template := rc.Spec.Template
		defaults := detectDefaults(t, rc, reflect.ValueOf(template))
		if !reflect.DeepEqual(expectedDefaults, defaults) {
			t.Errorf("Defaults for PodTemplateSpec changed. This can cause spurious rollouts of workloads on API server upgrade.")
			t.Log(cmp.Diff(expectedDefaults, defaults))
		}
	})
	t.Run("hostnet PodTemplateSpec with ports", func(t *testing.T) {
		// hostNetwork + 显式端口的场景：需要把“已知输入值”与默认值区分开，避免误报。
		rc := &v1.ReplicationController{
			Spec: v1.ReplicationControllerSpec{
				Template: &v1.PodTemplateSpec{
					Spec: v1.PodSpec{
						HostNetwork: true,
						Containers: []v1.Container{{
							Ports: []v1.ContainerPort{{
								ContainerPort: 12345,
								Protocol:      v1.ProtocolTCP,
							}},
						}},
					},
				},
			},
		}
		template := rc.Spec.Template
		defaults := detectDefaults(t, rc, reflect.ValueOf(template))
		expected := func() map[string]string {
			// 先登记那些由用例显式填入的值，它们不是默认值。
			m := map[string]string{
				".Spec.HostNetwork":                          "true",
				".Spec.Containers[0].Ports[0].ContainerPort": "12345",
			}
			m[".Spec.Containers"] = `[{"name":"","ports":[{"containerPort":12345,"protocol":"TCP"}],"resources":{},"terminationMessagePath":"/dev/termination-log","terminationMessagePolicy":"File","imagePullPolicy":"IfNotPresent"}]`
			m[".Spec.Containers[0].Ports"] = `[{"containerPort":12345,"protocol":"TCP"}]`
			for k, v := range expectedDefaults {
				// 已知输入优先，剩余的键才沿用空模板场景下观测到的默认值。
				if _, found := m[k]; !found {
					m[k] = v
				}
			}
			return m
		}()
		if !reflect.DeepEqual(expected, defaults) {
			t.Errorf("Defaults for PodTemplateSpec changed. This can cause spurious rollouts of workloads on API server upgrade.")
			t.Log(cmp.Diff(expected, defaults))
		}
	})
}

// TestPodDefaults 检测 PodSpec 内部默认值的变化。
// 该类型中的默认值变更（尤其是容器内部字段）可能在 API Server 升级后导致 kubelet 重启容器。
func TestPodDefaults(t *testing.T) {
	t.Run("enabled_features", func(t *testing.T) { testPodDefaults(t, true) })
	// 与 PodTemplateSpec 一样，需要在门控全开/全关两种状态下锁定默认值集合。
	t.Run("disabled_features", func(t *testing.T) { testPodDefaults(t, false) })
}
func testPodDefaults(t *testing.T, featuresEnabled bool) {
	setAllFeatures(t, featuresEnabled)
	pod := &v1.Pod{}
	// PodSpec 下新增默认值只有在读取旧版本数据时不会被应用才是可接受的。
	// 禁止：新增字段 `MyField *bool` 并将其默认为非 nil 值。
	// 禁止：对过去不设默认值的已有字段 `MyField *bool` 新增默认值。
	// 禁止：修改已有的默认值。
	// 允许：新增字段 `MyContainer *MyType` 并对其子字段设默认值（如 `MyContainer.MyChildField`），但前提是 MyContainer 本身非 nil。
	expectedDefaults := map[string]string{
		".Spec.Containers[0].Env[0].ValueFrom.FieldRef.APIVersion":       `"v1"`,
		".Spec.Containers[0].Env[0].ValueFrom.FileKeyRef.Optional":       "false",
		".Spec.Containers[0].ImagePullPolicy":                            `"IfNotPresent"`,
		".Spec.Containers[0].Lifecycle.PostStart.HTTPGet.Path":           `"/"`,
		".Spec.Containers[0].Lifecycle.PostStart.HTTPGet.Scheme":         `"HTTP"`,
		".Spec.Containers[0].Lifecycle.PreStop.HTTPGet.Path":             `"/"`,
		".Spec.Containers[0].Lifecycle.PreStop.HTTPGet.Scheme":           `"HTTP"`,
		".Spec.Containers[0].LivenessProbe.FailureThreshold":             `3`,
		".Spec.Containers[0].LivenessProbe.ProbeHandler.HTTPGet.Path":    `"/"`,
		".Spec.Containers[0].LivenessProbe.ProbeHandler.HTTPGet.Scheme":  `"HTTP"`,
		".Spec.Containers[0].LivenessProbe.PeriodSeconds":                `10`,
		".Spec.Containers[0].LivenessProbe.SuccessThreshold":             `1`,
		".Spec.Containers[0].LivenessProbe.TimeoutSeconds":               `1`,
		".Spec.Containers[0].Ports[0].Protocol":                          `"TCP"`,
		".Spec.Containers[0].ReadinessProbe.FailureThreshold":            `3`,
		".Spec.Containers[0].ReadinessProbe.ProbeHandler.HTTPGet.Path":   `"/"`,
		".Spec.Containers[0].ReadinessProbe.ProbeHandler.HTTPGet.Scheme": `"HTTP"`,
		".Spec.Containers[0].ReadinessProbe.PeriodSeconds":               `10`,
		".Spec.Containers[0].ReadinessProbe.SuccessThreshold":            `1`,
		".Spec.Containers[0].ReadinessProbe.TimeoutSeconds":              `1`,
		".Spec.Containers[0].Resources.Requests":                         `{"":"0"}`, // 该值是从 limits 字段默认推导出来的
		".Spec.Containers[0].StartupProbe.FailureThreshold":              "3",
		".Spec.Containers[0].StartupProbe.ProbeHandler.HTTPGet.Path":     `"/"`,
		".Spec.Containers[0].StartupProbe.ProbeHandler.HTTPGet.Scheme":   `"HTTP"`,
		".Spec.Containers[0].StartupProbe.PeriodSeconds":                 "10",
		".Spec.Containers[0].StartupProbe.SuccessThreshold":              "1",
		".Spec.Containers[0].StartupProbe.TimeoutSeconds":                "1",
		".Spec.Containers[0].TerminationMessagePath":                     `"/dev/termination-log"`,
		".Spec.Containers[0].TerminationMessagePolicy":                   `"File"`,
		".Spec.Containers[0].LivenessProbe.ProbeHandler.GRPC.Service":    `""`,
		".Spec.Containers[0].ReadinessProbe.ProbeHandler.GRPC.Service":   `""`,
		".Spec.Containers[0].StartupProbe.ProbeHandler.GRPC.Service":     `""`,
		".Spec.DNSPolicy":          `"ClusterFirst"`,
		".Spec.EnableServiceLinks": `true`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Env[0].ValueFrom.FieldRef.APIVersion":       `"v1"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Env[0].ValueFrom.FileKeyRef.Optional":       "false",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ImagePullPolicy":                            `"IfNotPresent"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PostStart.HTTPGet.Path":           `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PostStart.HTTPGet.Scheme":         `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PreStop.HTTPGet.Path":             `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Lifecycle.PreStop.HTTPGet.Scheme":           `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.FailureThreshold":             "3",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.PeriodSeconds":                "10",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.SuccessThreshold":             "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.TimeoutSeconds":               "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.Ports[0].Protocol":                          `"TCP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.FailureThreshold":            "3",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.PeriodSeconds":               "10",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.SuccessThreshold":            "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.TimeoutSeconds":              "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.FailureThreshold":              "3",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.PeriodSeconds":                 "10",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.SuccessThreshold":              "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.TimeoutSeconds":                "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.TerminationMessagePath":                     `"/dev/termination-log"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.TerminationMessagePolicy":                   `"File"`,
		".Spec.InitContainers[0].Env[0].ValueFrom.FieldRef.APIVersion":                                     `"v1"`,
		".Spec.InitContainers[0].Env[0].ValueFrom.FileKeyRef.Optional":                                     "false",
		".Spec.InitContainers[0].ImagePullPolicy":                                                          `"IfNotPresent"`,
		".Spec.InitContainers[0].Lifecycle.PostStart.HTTPGet.Path":                                         `"/"`,
		".Spec.InitContainers[0].Lifecycle.PostStart.HTTPGet.Scheme":                                       `"HTTP"`,
		".Spec.InitContainers[0].Lifecycle.PreStop.HTTPGet.Path":                                           `"/"`,
		".Spec.InitContainers[0].Lifecycle.PreStop.HTTPGet.Scheme":                                         `"HTTP"`,
		".Spec.InitContainers[0].LivenessProbe.FailureThreshold":                                           `3`,
		".Spec.InitContainers[0].LivenessProbe.PeriodSeconds":                                              `10`,
		".Spec.InitContainers[0].LivenessProbe.SuccessThreshold":                                           `1`,
		".Spec.InitContainers[0].LivenessProbe.TimeoutSeconds":                                             `1`,
		".Spec.InitContainers[0].Ports[0].Protocol":                                                        `"TCP"`,
		".Spec.InitContainers[0].ReadinessProbe.FailureThreshold":                                          `3`,
		".Spec.InitContainers[0].ReadinessProbe.PeriodSeconds":                                             `10`,
		".Spec.InitContainers[0].ReadinessProbe.SuccessThreshold":                                          `1`,
		".Spec.InitContainers[0].ReadinessProbe.TimeoutSeconds":                                            `1`,
		".Spec.InitContainers[0].Resources.Requests":                                                       `{"":"0"}`, // 该值是从 limits 字段默认推导出来的
		".Spec.InitContainers[0].TerminationMessagePath":                                                   `"/dev/termination-log"`,
		".Spec.InitContainers[0].TerminationMessagePolicy":                                                 `"File"`,
		".Spec.InitContainers[0].StartupProbe.FailureThreshold":                                            "3",
		".Spec.InitContainers[0].StartupProbe.PeriodSeconds":                                               "10",
		".Spec.InitContainers[0].StartupProbe.SuccessThreshold":                                            "1",
		".Spec.InitContainers[0].StartupProbe.TimeoutSeconds":                                              "1",
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.ProbeHandler.GRPC.Service":    `""`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.ProbeHandler.HTTPGet.Path":    `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.LivenessProbe.ProbeHandler.HTTPGet.Scheme":  `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.ProbeHandler.GRPC.Service":   `""`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.ProbeHandler.HTTPGet.Path":   `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.ReadinessProbe.ProbeHandler.HTTPGet.Scheme": `"HTTP"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.ProbeHandler.GRPC.Service":     `""`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.ProbeHandler.HTTPGet.Path":     `"/"`,
		".Spec.EphemeralContainers[0].EphemeralContainerCommon.StartupProbe.ProbeHandler.HTTPGet.Scheme":   `"HTTP"`,
		".Spec.InitContainers[0].LivenessProbe.ProbeHandler.GRPC.Service":                                  `""`,
		".Spec.InitContainers[0].LivenessProbe.ProbeHandler.HTTPGet.Path":                                  `"/"`,
		".Spec.InitContainers[0].LivenessProbe.ProbeHandler.HTTPGet.Scheme":                                `"HTTP"`,
		".Spec.InitContainers[0].ReadinessProbe.ProbeHandler.GRPC.Service":                                 `""`,
		".Spec.InitContainers[0].ReadinessProbe.ProbeHandler.HTTPGet.Path":                                 `"/"`,
		".Spec.InitContainers[0].ReadinessProbe.ProbeHandler.HTTPGet.Scheme":                               `"HTTP"`,
		".Spec.InitContainers[0].StartupProbe.ProbeHandler.GRPC.Service":                                   `""`,
		".Spec.InitContainers[0].StartupProbe.ProbeHandler.HTTPGet.Path":                                   `"/"`,
		".Spec.InitContainers[0].StartupProbe.ProbeHandler.HTTPGet.Scheme":                                 `"HTTP"`,
		".Spec.RestartPolicy":                                                                         `"Always"`,
		".Spec.SchedulerName":                                                                         `"default-scheduler"`,
		".Spec.SecurityContext":                                                                       `{}`,
		".Spec.TerminationGracePeriodSeconds":                                                         `30`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.CachingMode":                                         `"ReadWrite"`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.FSType":                                              `"ext4"`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.Kind":                                                `"Shared"`,
		".Spec.Volumes[0].VolumeSource.AzureDisk.ReadOnly":                                            `false`,
		".Spec.Volumes[0].VolumeSource.ConfigMap.DefaultMode":                                         `420`,
		".Spec.Volumes[0].VolumeSource.DownwardAPI.DefaultMode":                                       `420`,
		".Spec.Volumes[0].VolumeSource.DownwardAPI.Items[0].FieldRef.APIVersion":                      `"v1"`,
		".Spec.Volumes[0].VolumeSource.EmptyDir":                                                      `{}`,
		".Spec.Volumes[0].VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.VolumeMode":                 `"Filesystem"`,
		".Spec.Volumes[0].VolumeSource.HostPath.Type":                                                 `""`,
		".Spec.Volumes[0].VolumeSource.ISCSI.ISCSIInterface":                                          `"default"`,
		".Spec.Volumes[0].VolumeSource.Image.PullPolicy":                                              `"IfNotPresent"`,
		".Spec.Volumes[0].VolumeSource.Projected.DefaultMode":                                         `420`,
		".Spec.Volumes[0].VolumeSource.Projected.Sources[0].DownwardAPI.Items[0].FieldRef.APIVersion": `"v1"`,
		".Spec.Volumes[0].VolumeSource.Projected.Sources[0].ServiceAccountToken.ExpirationSeconds":    `3600`,
		".Spec.Volumes[0].VolumeSource.RBD.Keyring":                                                   `"/etc/ceph/keyring"`,
		".Spec.Volumes[0].VolumeSource.RBD.RBDPool":                                                   `"rbd"`,
		".Spec.Volumes[0].VolumeSource.RBD.RadosUser":                                                 `"admin"`,
		".Spec.Volumes[0].VolumeSource.ScaleIO.FSType":                                                `"xfs"`,
		".Spec.Volumes[0].VolumeSource.ScaleIO.StorageMode":                                           `"ThinProvisioned"`,
		".Spec.Volumes[0].VolumeSource.Secret.DefaultMode":                                            `420`,
	}
	if featuresEnabled {
		// 门控开启时，特性相关的默认值会额外出现在 Pod 上，需要并入期望集合。
		maps.Copy(expectedDefaults, featureGatedPodDefaults)
	}
	defaults := detectDefaults(t, pod, reflect.ValueOf(pod))
	if !reflect.DeepEqual(expectedDefaults, defaults) {
		t.Errorf("Defaults for PodSpec changed. This can cause spurious restarts of containers on API server upgrade.")
		t.Log(cmp.Diff(expectedDefaults, defaults))
	}
}

// TestPodResourcesDefaults 验证 Pod 级别 resources 与容器级别 resources 的默认值推导关系，
// 重点覆盖 PodLevelResources 特性门控开启时的请求值汇总与回退逻辑。
func TestPodResourcesDefaults(t *testing.T) {
	cases := []struct {
		// 表格字段：podLevelResourcesEnabled 控制是否开启 PodLevelResources 门控；
		// podResources 为 Pod 级输入，containers 为各容器输入，expectedPodSpec 为默认化后的期望结果。
		name                     string
		podLevelResourcesEnabled bool
		containers               []v1.Container
		podResources             *v1.ResourceRequirements
		expectedPodSpec          v1.PodSpec
	}{
		{
			// 双方均未设置资源：默认情况下什么都不应被填充。
			name:       "pod resources=unset, container resources=unset",
			containers: []v1.Container{{Resources: v1.ResourceRequirements{}}, {Resources: v1.ResourceRequirements{}}},
			expectedPodSpec: v1.PodSpec{
				Containers: []v1.Container{{Resources: v1.ResourceRequirements{}}, {Resources: v1.ResourceRequirements{}}},
			},
		}, {
			// 容器仅设置 limits：期望 requests 按 limits 复制（传统默认行为）。
			name: "pod resources=unset, container requests=unset limits=set",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 容器仅设置 requests：期望保持原样，不反向推导 limits。
			name: "pod resources=unset, container requests=set limits=unset",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 容器 requests 与 limits 均显式设置：默认值不应覆盖任何一方。
			name: "pod resources=unset, container requests=set limits=set",
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("1Mi"),
						},
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("1Mi"),
						},
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 门控开启且仅 Pod 设置 limits：Pod 级 requests 应由 limits 推导。
			name:                     "pod requests=unset limits=set, container resources=unset",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("2m"),
					"memory": resource.MustParse("1Mi"),
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("1Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("1Mi"),
					},
				},
			},
		}, {
			// Pod 级 limits 为 nil 时退化为容器级默认：容器 limits 仍推导各自的 requests。
			name:                     "pod limits=nil, container requests=unset limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: nil,
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 空 map 与 nil 等价：同样不应干扰容器级资源的默认化。
			name:                     "pod limits=empty map, container requests=unset limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// Pod 级 requests 为空 map：期望由 Pod limits 与各容器 requests 汇总计算。
			name:                     "pod requests=empty map limits=set, container requests=unset limits=set",
			podLevelResourcesEnabled: true,
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			podResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{},
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("5m"),
					"memory": resource.MustParse("7Mi"),
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("3m"),
						"memory": resource.MustParse("6Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("5m"),
						"memory": resource.MustParse("7Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// Pod 级 requests 为 nil：与空 map 场景应有相同的汇总结果。
			name:                     "pod requests=nil limits=set, container requests=unset limits=set",
			podLevelResourcesEnabled: true,
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			podResources: &v1.ResourceRequirements{
				Requests: nil,
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("5m"),
					"memory": resource.MustParse("7Mi"),
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("3m"),
						"memory": resource.MustParse("6Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("5m"),
						"memory": resource.MustParse("7Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// Pod 与容器同时设置 limits：Pod 级 requests 取各容器 requests 之和。
			name:                     "pod requests=unset limits=set, container requests=unset limits=set",
			podLevelResourcesEnabled: true,
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("5m"),
					"memory": resource.MustParse("7Mi"),
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("3m"),
						"memory": resource.MustParse("6Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("5m"),
						"memory": resource.MustParse("7Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 容器已有 requests 时，Pod 级 requests 应累加各容器的显式 requests。
			name:                     "pod requests=unset limits=set, container requests=set limits=unset",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("4m"),
					"memory": resource.MustParse("8Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("2Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("3Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("4m"),
						"memory": resource.MustParse("8Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("2Mi"),
							},
						},
					},
				},
			},
		}, {
			// 仅 CPU 有 limit：只推导 CPU 的 requests，不应凭空生成内存字段。
			name:                     "pod requests=unset cpu limits=set, container resources=unset",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu": resource.MustParse("4m"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{},
				}, {
					Resources: v1.ResourceRequirements{},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("4m"),
					},
					Limits: v1.ResourceList{
						"cpu": resource.MustParse("4m"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{},
					}, {
						Resources: v1.ResourceRequirements{},
					},
				},
			},
		}, {
			// Pod 与容器双方均显式设置 requests/limits：验证优先级与汇总行为。
			name:                     "pod requests=unset limits=set, container requests=set limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("4m"),
					"memory": resource.MustParse("8Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("1Mi"),
						},
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("2m"),
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("2Mi"),
						},
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1m"),
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("3Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("4m"),
						"memory": resource.MustParse("8Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("2Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 容器仅设置内存 requests：CPU 部分仍需由 Pod limits 推导，内存部分保持显式值。
			name:                     "pod requests=unset limits=set, container memory requests=set limits=unset",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("4m"),
					"memory": resource.MustParse("8Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"memory": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"memory": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("4m"),
						"memory": resource.MustParse("6Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("4m"),
						"memory": resource.MustParse("8Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2m"),
								"memory": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1m"),
								"memory": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 容器出现非 CPU/内存资源：验证这些受限资源不参与默认推导。
			name:                     "pod limits=set, container unsupported requests=set limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":    resource.MustParse("2m"),
					"memory": resource.MustParse("1Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"storage": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"ephemeral-storage": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("1Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("1Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"storage": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"storage": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"ephemeral-storage": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"ephemeral-storage": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// Pod 与容器均含非标准资源：验证默认逻辑对这类资源的处理策略。
			name:                     "pod unsupported resources limits=set, container unsupported requests=set limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"storage": resource.MustParse("1Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"storage": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"ephemeral-storage": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{},
					Limits: v1.ResourceList{
						"storage": resource.MustParse("1Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"storage": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"storage": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"ephemeral-storage": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"ephemeral-storage": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// 标准资源与非标准资源混用：验证只对受支持资源做汇总。
			name:                     "pod supported and unsupported resources limits=set, container unsupported requests=set limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":     resource.MustParse("2m"),
					"memory":  resource.MustParse("1Mi"),
					"storage": resource.MustParse("1Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"storage": resource.MustParse("1Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"ephemeral-storage": resource.MustParse("5Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("1Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":    resource.MustParse("2m"),
						"memory": resource.MustParse("1Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"storage": resource.MustParse("1Mi"),
							},
							Limits: v1.ResourceList{
								"storage": resource.MustParse("1Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"ephemeral-storage": resource.MustParse("5Mi"),
							},
							Limits: v1.ResourceList{
								"ephemeral-storage": resource.MustParse("5Mi"),
							},
						},
					},
				},
			},
		}, {
			// HugePages 场景一：Pod 仅有 CPU limit，容器 HugePages limit 不应被误汇总。
			name:                     "pod has cpu limit with hugepages requests=unset limits=unset, container hugepages requests=unset limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu": resource.MustParse("5m"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("2m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("1m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":                              resource.MustParse("3m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("6Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("6Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
						},
					},
				},
			},
		}, {
			// HugePages 场景二：Pod 仅有 CPU request，同样不应波及 HugePages。
			name:                     "pod has cpu request with hugepages requests=unset limits=unset, container hugepages requests=unset limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu": resource.MustParse("5m"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("2m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("1m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("6Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("6Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
						},
					},
				},
			},
		}, {
			// Pod 与容器同时设置同规格 HugePages limit：验证默认与汇总行为。
			name:                     "pod hugepages requests=unset limits=set, container hugepages requests=unset limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":                              resource.MustParse("5m"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("2m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("1m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":                              resource.MustParse("3m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
						},
					},
				},
			},
		}, {
			// HugePages 已有 requests：验证已有值不被 limits 覆盖。
			name:                     "pod hugepages requests=set limits=set, container hugepages requests=unset limits=set",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":                              resource.MustParse("5m"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
				Requests: v1.ResourceList{
					"cpu":                              resource.MustParse("5m"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("2m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("1m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("4Mi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
						},
					},
				},
			},
		}, {
			// 容器无 HugePages 设置而 Pod 有：验证 Pod 级值如何向下传递。
			name:                     "pod hugepages requests=set limits=set, container hugepages requests=unset limits=unset",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":                              resource.MustParse("5m"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
				Requests: v1.ResourceList{
					"cpu":                              resource.MustParse("5m"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					},
					Limits: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{},
					},
				},
			},
		}, {
			// Pod 与容器使用不同 HugePages 页大小：验证两者互不干扰。
			name:                     "pod hugepages requests=unset limits=set, container hugepages requests=unset limits=set different hugepagesizes between pod and container level",
			podLevelResourcesEnabled: true,
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":                              resource.MustParse("5m"),
					v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
				},
			},
			containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("2m"),
							v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
						},
					},
				}, {
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":                              resource.MustParse("1m"),
							v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
						},
					},
				},
			},
			expectedPodSpec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":                              resource.MustParse("3m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
						v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
					},
					Limits: v1.ResourceList{
						"cpu":                              resource.MustParse("5m"),
						v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("10Mi"),
						v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
					},
				},
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("2m"),
								v1.ResourceHugePagesPrefix + "1Gi": resource.MustParse("1Gi"),
							},
						},
					}, {
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
							Limits: v1.ResourceList{
								"cpu":                              resource.MustParse("1m"),
								v1.ResourceHugePagesPrefix + "2Mi": resource.MustParse("2Mi"),
							},
						},
					},
				},
			},
		}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.podLevelResourcesEnabled {
				// 仅对该用例开启门控；SetFeatureGateDuringTest 会在子测试结束时自动恢复。
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, true)
			}
			spec := v1.PodSpec{
				Containers: tc.containers,
				Resources:  tc.podResources,
			}
			p := v1.Pod{Spec: *spec.DeepCopy()}
			// 深拷贝避免用例输入被默认化逻辑就地修改，影响后续断言。
			corev1.SetDefaults_Pod(&p)
			// 逐个容器校验 requests：只检查期望集合中出现的资源键。
			for i, container := range p.Spec.Containers {
				for resource, quantity := range container.Resources.Requests {
					if quantity.Cmp(tc.expectedPodSpec.Containers[i].Resources.Requests[resource]) != 0 {
						t.Errorf("got: %v, expected: %v", quantity, tc.expectedPodSpec.Containers[i].Resources.Requests[resource])
					}
				}
			}

			if tc.podResources != nil {
				// 仅在 Pod 级资源存在时校验其默认值，避免空指针解引用。
				for resource, quantity := range p.Spec.Resources.Requests {
					if quantity.Cmp(tc.expectedPodSpec.Resources.Requests[resource]) != 0 {
						t.Errorf("got: %v, expected: %v", quantity, tc.expectedPodSpec.Resources.Requests[resource])
					}
				}
			}
		})
	}
}

// TestPodHostNetworkDefaults 验证 hostNetwork 开启时 HostPort 的默认值设置位置：
// 只应由 SetDefaults_Pod 处理，而不应在 SetDefaults_PodSpec 中生效。
func TestPodHostNetworkDefaults(t *testing.T) {
	cases := []struct {
		// expectPodDefault 与 expectPodSpecDefault 分别记录两条默认化路径上是否期望 HostPort 被填充。
		name                 string
		hostNet              bool
		expectPodDefault     bool
		expectPodSpecDefault bool
	}{{
		name:                 "hostNetwork=false",
		hostNet:              false,
		expectPodDefault:     false,
		expectPodSpecDefault: false,
	}, {
		name:                 "hostNetwork=true",
		hostNet:              true,
		expectPodDefault:     true,
		expectPodSpecDefault: false,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const portNum = 12345
			spec := v1.PodSpec{
				HostNetwork: tc.hostNet,
				Containers: []v1.Container{{
					Ports: []v1.ContainerPort{{
						ContainerPort: portNum,
						Protocol:      v1.ProtocolTCP,
						// 有意留空 HostPort，这样才能观察默认化是否发生。
					}},
				}},
			}

			// 走 Pod 级别的默认化路径。
			p := v1.Pod{Spec: *spec.DeepCopy()}
			corev1.SetDefaults_Pod(&p)
			// hostNetwork=true 时 HostPort 应等于 ContainerPort；为 false 时应保持 0。
			if got := p.Spec.Containers[0].Ports[0].HostPort; tc.expectPodDefault && got == 0 {
				t.Errorf("expected Pod HostPort to be defaulted, got %v", got)
			}
			if got := p.Spec.Containers[0].Ports[0].HostPort; !tc.expectPodDefault && got != 0 {
				t.Errorf("expected Pod HostPort to be 0, got %v", got)
			}

			// 走 PodSpec 级别的默认化路径。
			s := spec.DeepCopy()
			corev1.SetDefaults_PodSpec(s)
			// 该路径不应设置 HostPort，两个用例均期望保持为 0。
			if got := s.Containers[0].Ports[0].HostPort; tc.expectPodSpecDefault && got == 0 {
				t.Errorf("expected PodSpec HostPort to be defaulted, got %v", got)
			}
			if got := s.Containers[0].Ports[0].HostPort; !tc.expectPodSpecDefault && got != 0 {
				t.Errorf("expected PodSpec HostPort to be 0, got %v", got)
			}
		})
	}
}

type testPath struct {
	path  string
	value reflect.Value
}

// detectDefaults 通过反射遍历对象字段，把每个非零字段的路径和序列化值记录下来，
// 从而穷举出当前版本实际施加的所有默认值。
func detectDefaults(t *testing.T, obj runtime.Object, v reflect.Value) map[string]string {
	defaults := map[string]string{}
	toVisit := []testPath{{path: "", value: v}}

	for len(toVisit) > 0 {
		visit := toVisit[0]
		toVisit = toVisit[1:]

		legacyscheme.Scheme.Default(obj)
		// 每次访问都重新默认化，使后续字段能观察到默认值带来的连带影响。
		defaultedV := visit.value
		zeroV := reflect.Zero(visit.value.Type())

		switch {
		case visit.value.Kind() == reflect.Struct:
			for fi := 0; fi < visit.value.NumField(); fi++ {
				structField := visit.value.Type().Field(fi)
				valueField := visit.value.Field(fi)
				if valueField.CanSet() {
					toVisit = append(toVisit, testPath{path: visit.path + "." + structField.Name, value: valueField})
				}
			}

		case visit.value.Kind() == reflect.Slice:
			if !visit.value.IsNil() {
				// 切片已有值：可能是默认值也可能是显式输入，先记录再尝试下钻到元素。
				marshaled, _ := json.Marshal(defaultedV.Interface())
				defaults[visit.path] = string(marshaled)
				toVisit = append(toVisit, testPath{path: visit.path + "[0]", value: visit.value.Index(0)})
			} else if visit.value.Type().Elem().Kind() == reflect.Struct {
				if strings.HasPrefix(visit.path, ".ObjectMeta.ManagedFields[") {
					// 元数据 managedFields 由系统维护，不属于 API 默认值范畴，直接跳过。
					break
				}
				// 对 nil 的结构体切片，临时追加一个零值元素以便递归发现其子字段默认值；
				// ManagedFields 是元数据内部实现，跳过以免产生噪声。
				item := reflect.New(visit.value.Type().Elem()).Elem()
				visit.value.Set(reflect.Append(visit.value, item))
				toVisit = append(toVisit, testPath{path: visit.path + "[0]", value: visit.value.Index(0)})
			} else if !isPrimitive(visit.value.Type().Elem().Kind()) {
				t.Logf("unhandled non-primitive slice type %s: %s", visit.path, visit.value.Type().Elem())
			}

		case visit.value.Kind() == reflect.Map:
			if !visit.value.IsNil() {
				// map 已有值说明被默认化，记录其 JSON 表示。
				marshaled, _ := json.Marshal(defaultedV.Interface())
				defaults[visit.path] = string(marshaled)
			} else if visit.value.Type().Key().Kind() == reflect.String && visit.value.Type().Elem().Kind() == reflect.Struct {
				if strings.HasPrefix(visit.path, ".ObjectMeta.ManagedFields[") {
					// 同样跳过 managedFields。
					break
				}
				// 对 nil 的结构体 map 注入一个零值条目，递归探测其结构内部。
				item := reflect.New(visit.value.Type().Elem()).Elem()
				visit.value.Set(reflect.MakeMap(visit.value.Type()))
				visit.value.SetMapIndex(reflect.New(visit.value.Type().Key()).Elem(), item)
				toVisit = append(toVisit, testPath{path: visit.path + "[*]", value: item})
			} else if !isPrimitive(visit.value.Type().Elem().Kind()) {
				t.Logf("unhandled non-primitive map type %s: %s", visit.path, visit.value.Type().Elem())
			}

		case visit.value.Kind() == reflect.Pointer:
			if visit.value.IsNil() {
				if visit.value.Type().Elem().Kind() == reflect.Struct {
					visit.value.Set(reflect.New(visit.value.Type().Elem()))
					toVisit = append(toVisit, testPath{path: visit.path, value: visit.value.Elem()})
				} else if !isPrimitive(visit.value.Type().Elem().Kind()) {
					t.Errorf("unhandled non-primitive nil ptr: %s: %s", visit.path, visit.value.Type())
				}
			} else {
				if visit.path != "" {
					marshaled, _ := json.Marshal(defaultedV.Interface())
					defaults[visit.path] = string(marshaled)
				}
				toVisit = append(toVisit, testPath{path: visit.path, value: visit.value.Elem()})
			}

		case isPrimitive(visit.value.Kind()):
			// 基本类型只有非零值才算被默认化。
			if !reflect.DeepEqual(defaultedV.Interface(), zeroV.Interface()) {
				marshaled, _ := json.Marshal(defaultedV.Interface())
				defaults[visit.path] = string(marshaled)
			}

		default:
			t.Errorf("unhandled kind: %s: %s", visit.path, visit.value.Type())
		}

	}
	return defaults
}

// isPrimitive 判断某种类是否属于需要比较零值的基本类型。
func isPrimitive(k reflect.Kind) bool {
	switch k {
	case reflect.String, reflect.Bool, reflect.Int32, reflect.Int64, reflect.Int:
		return true
	default:
		return false
	}
}

// roundTrip 将对象编码为 wire 格式再解码并转换回内部版本，
// 借此触发默认化链条，模拟对象经过 API Server 往返后的状态。
func roundTrip(t *testing.T, obj runtime.Object) runtime.Object {
	codec := legacyscheme.Codecs.LegacyCodec(corev1.SchemeGroupVersion)
	data, err := runtime.Encode(codec, obj)
	if err != nil {
		t.Errorf("%v\n %#v", err, obj)
		return nil
	}
	obj2, err := runtime.Decode(codec, data)
	if err != nil {
		t.Errorf("%v\nData: %s\nSource: %#v", err, string(data), obj)
		return nil
	}
	obj3 := reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
	err = legacyscheme.Scheme.Convert(obj2, obj3, nil)
	if err != nil {
		t.Errorf("%v\nSource: %#v", err, obj2)
		return nil
	}
	return obj3
}

// TestSetDefaultServiceAccountAlias 验证废弃字段 deprecatedServiceAccount 向 serviceAccountName 的别名同步，
// 包括两者冲突时新字段优先的规则。
func TestSetDefaultServiceAccountAlias(t *testing.T) {
	tests := []struct {
		// 覆盖新旧字段的各种组合：都为空、相同、冲突以及仅设置其中之一。
		name                     string
		serviceAccountName       string
		deprecatedServiceAccount string
		expected                 string
	}{
		{name: "neither set", serviceAccountName: "", deprecatedServiceAccount: "", expected: ""},
		{name: "both set to the same value", serviceAccountName: "a", deprecatedServiceAccount: "a", expected: "a"},
		{name: "both set, serviceAccountName wins", serviceAccountName: "a", deprecatedServiceAccount: "b", expected: "a"},
		{name: "only serviceAccountName set", serviceAccountName: "a", deprecatedServiceAccount: "", expected: "a"},
		{name: "only deprecated alias set", serviceAccountName: "", deprecatedServiceAccount: "a", expected: "a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{
				Spec: v1.PodSpec{
					ServiceAccountName:       tc.serviceAccountName,
					DeprecatedServiceAccount: tc.deprecatedServiceAccount,
				},
			}
			obj2 := roundTrip(t, runtime.Object(pod))
			pod2 := obj2.(*v1.Pod)
			if pod2.Spec.ServiceAccountName != tc.expected {
				// 两个字段在往返后都应收敛到同一个期望值。
				t.Errorf("expected serviceAccountName %q, got %q", tc.expected, pod2.Spec.ServiceAccountName)
			}
			if pod2.Spec.DeprecatedServiceAccount != tc.expected {
				t.Errorf("expected deprecatedServiceAccount %q, got %q", tc.expected, pod2.Spec.DeprecatedServiceAccount)
			}
		})
	}
}

// TestSetDefaultReplicationController 验证 RC 的 selector 和 labels 从 Pod 模板标签复制的默认化规则，
// 以及已有值不被覆盖的行为。
func TestSetDefaultReplicationController(t *testing.T) {
	tests := []struct {
		// expectLabels / expectSelector 标记 roundTrip 后 RC 的 Labels / Selector 是否应等于模板标签。
		rc             *v1.ReplicationController
		expectLabels   bool
		expectSelector bool
	}{
		{
			rc: &v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectLabels:   true,
			expectSelector: true,
		},
		{
			rc: &v1.ReplicationController{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectLabels:   false,
			expectSelector: true,
		},
		{
			rc: &v1.ReplicationController{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: v1.ReplicationControllerSpec{
					Selector: map[string]string{
						"some": "other",
					},
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectLabels:   false,
			expectSelector: false,
		},
		{
			rc: &v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Selector: map[string]string{
						"some": "other",
					},
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectLabels:   true,
			expectSelector: false,
		},
	}

	for _, test := range tests {
		rc := test.rc
		// 通过 roundTrip 触发默认化，再比较复制关系。
		obj2 := roundTrip(t, runtime.Object(rc))
		rc2, ok := obj2.(*v1.ReplicationController)
		if !ok {
			t.Errorf("unexpected object: %v", rc2)
			t.FailNow()
		}
		if test.expectSelector != reflect.DeepEqual(rc2.Spec.Selector, rc2.Spec.Template.Labels) {
			if test.expectSelector {
				t.Errorf("expected: %v, got: %v", rc2.Spec.Template.Labels, rc2.Spec.Selector)
			} else {
				t.Errorf("unexpected equality: %v", rc.Spec.Selector)
			}
		}
		if test.expectLabels != reflect.DeepEqual(rc2.Labels, rc2.Spec.Template.Labels) {
			if test.expectLabels {
				t.Errorf("expected: %v, got: %v", rc2.Spec.Template.Labels, rc2.Labels)
			} else {
				t.Errorf("unexpected equality: %v", rc.Labels)
			}
		}
	}
}

// TestSetDefaultReplicationControllerReplicas 验证副本数未设置时默认为 1，
// 而已设置的值（包括 0）必须原样保留。
func TestSetDefaultReplicationControllerReplicas(t *testing.T) {
	tests := []struct {
		// 三个用例分别覆盖 nil（未设置）、显式 0 与显式 3。
		rc             v1.ReplicationController
		expectReplicas int32
	}{
		{
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectReplicas: 1,
		},
		{
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Replicas: ptr.To[int32](0),
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectReplicas: 0,
		},
		{
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Replicas: ptr.To[int32](3),
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectReplicas: 3,
		},
	}

	for _, test := range tests {
		rc := &test.rc
		obj2 := roundTrip(t, runtime.Object(rc))
		rc2, ok := obj2.(*v1.ReplicationController)
		if !ok {
			t.Errorf("unexpected object: %v", rc2)
			t.FailNow()
		}
		if rc2.Spec.Replicas == nil {
			t.Errorf("unexpected nil Replicas")
		} else if test.expectReplicas != *rc2.Spec.Replicas {
			t.Errorf("expected: %d replicas, got: %d", test.expectReplicas, *rc2.Spec.Replicas)
		}
	}
}

// InitContainerValidator 是校验 init container 某个字段子集的断言函数类型。
type InitContainerValidator func(got, expected *v1.Container) error

// TestSetDefaultReplicationControllerInitContainers 验证 init container 经过默认化后，
// 镜像拉取策略、字段引用、端口协议、资源、探针和生命周期钩子等均被正确填充。
func TestSetDefaultReplicationControllerInitContainers(t *testing.T) {
	// 各 validator 只比较用例关心的字段，避免被其他默认化差异干扰。
	assertEnvFieldRef := func(got, expected *v1.Container) error {
		if len(got.Env) != len(expected.Env) {
			return fmt.Errorf("different number of env: got <%v>, expected <%v>", len(got.Env), len(expected.Env))
		}

		for j := range got.Env {
			ge := &got.Env[j]
			ee := &expected.Env[j]

			if ge.Name != ee.Name {
				return fmt.Errorf("different name of env: got <%v>, expected <%v>", ge.Name, ee.Name)
			}

			if ge.ValueFrom.FieldRef.APIVersion != ee.ValueFrom.FieldRef.APIVersion {
				return fmt.Errorf("different api version of FieldRef <%v>: got <%v>, expected <%v>",
					ge.Name, ge.ValueFrom.FieldRef.APIVersion, ee.ValueFrom.FieldRef.APIVersion)
			}
		}
		return nil
	}

	assertImagePullPolicy := func(got, expected *v1.Container) error {
		if got.ImagePullPolicy != expected.ImagePullPolicy {
			return fmt.Errorf("different image pull policy: got <%v>, expected <%v>", got.ImagePullPolicy, expected.ImagePullPolicy)
		}
		return nil
	}

	assertContainerPort := func(got, expected *v1.Container) error {
		if len(got.Ports) != len(expected.Ports) {
			return fmt.Errorf("different number of ports: got <%v>, expected <%v>", len(got.Ports), len(expected.Ports))
		}

		for i := range got.Ports {
			gp := &got.Ports[i]
			ep := &expected.Ports[i]

			if gp.Name != ep.Name {
				return fmt.Errorf("different name of port: got <%v>, expected <%v>", gp.Name, ep.Name)
			}

			if gp.Protocol != ep.Protocol {
				return fmt.Errorf("different port protocol <%v>: got <%v>, expected <%v>", gp.Name, gp.Protocol, ep.Protocol)
			}
		}

		return nil
	}

	assertResource := func(got, expected *v1.Container) error {
		if len(got.Resources.Limits) != len(expected.Resources.Limits) {
			return fmt.Errorf("different number of resources.Limits: got <%v>, expected <%v>", len(got.Resources.Limits), (expected.Resources.Limits))
		}

		for k, v := range got.Resources.Limits {
			if ev, found := expected.Resources.Limits[v1.ResourceName(k)]; !found {
				return fmt.Errorf("failed to find resource <%v> in expected resources.Limits.", k)
			} else {
				if ev.Value() != v.Value() {
					return fmt.Errorf("different resource.Limits: got <%v>, expected <%v>.", v.Value(), ev.Value())
				}
			}
		}

		if len(got.Resources.Requests) != len(expected.Resources.Requests) {
			return fmt.Errorf("different number of resources.Requests: got <%v>, expected <%v>", len(got.Resources.Requests), (expected.Resources.Requests))
		}

		for k, v := range got.Resources.Requests {
			if ev, found := expected.Resources.Requests[v1.ResourceName(k)]; !found {
				return fmt.Errorf("failed to find resource <%v> in expected resources.Requests.", k)
			} else {
				if ev.Value() != v.Value() {
					return fmt.Errorf("different resource.Requests: got <%v>, expected <%v>.", v.Value(), ev.Value())
				}
			}
		}

		return nil
	}

	assertProb := func(got, expected *v1.Container) error {
		// 校验存活探针的默认路径、协议与各阈值。
		if got.LivenessProbe.ProbeHandler.HTTPGet.Path != expected.LivenessProbe.ProbeHandler.HTTPGet.Path ||
			got.LivenessProbe.ProbeHandler.HTTPGet.Scheme != expected.LivenessProbe.ProbeHandler.HTTPGet.Scheme ||
			got.LivenessProbe.FailureThreshold != expected.LivenessProbe.FailureThreshold ||
			got.LivenessProbe.SuccessThreshold != expected.LivenessProbe.SuccessThreshold ||
			got.LivenessProbe.PeriodSeconds != expected.LivenessProbe.PeriodSeconds ||
			got.LivenessProbe.TimeoutSeconds != expected.LivenessProbe.TimeoutSeconds {
			return fmt.Errorf("different LivenessProbe: got <%v>, expected <%v>", got.LivenessProbe, expected.LivenessProbe)
		}

		// 校验就绪探针的默认路径、协议与各阈值。
		if got.ReadinessProbe.ProbeHandler.HTTPGet.Path != expected.ReadinessProbe.ProbeHandler.HTTPGet.Path ||
			got.ReadinessProbe.ProbeHandler.HTTPGet.Scheme != expected.ReadinessProbe.ProbeHandler.HTTPGet.Scheme ||
			got.ReadinessProbe.FailureThreshold != expected.ReadinessProbe.FailureThreshold ||
			got.ReadinessProbe.SuccessThreshold != expected.ReadinessProbe.SuccessThreshold ||
			got.ReadinessProbe.PeriodSeconds != expected.ReadinessProbe.PeriodSeconds ||
			got.ReadinessProbe.TimeoutSeconds != expected.ReadinessProbe.TimeoutSeconds {
			return fmt.Errorf("different ReadinessProbe: got <%v>, expected <%v>", got.ReadinessProbe, expected.ReadinessProbe)
		}

		return nil
	}

	assertLifeCycle := func(got, expected *v1.Container) error {
		if got.Lifecycle.PostStart.HTTPGet.Path != expected.Lifecycle.PostStart.HTTPGet.Path ||
			got.Lifecycle.PostStart.HTTPGet.Scheme != expected.Lifecycle.PostStart.HTTPGet.Scheme {
			return fmt.Errorf("different LifeCycle: got <%v>, expected <%v>", got.Lifecycle, expected.Lifecycle)
		}

		return nil
	}

	cpu, _ := resource.ParseQuantity("100m")
	// 预解析数量值，供期望资源列表复用。
	mem, _ := resource.ParseQuantity("100Mi")

	tests := []struct {
		// 每个用例只设置 init container 的某一个方面，并用对应 validator 校验默认化结果。
		name       string
		rc         v1.ReplicationController
		expected   []v1.Container
		validators []InitContainerValidator
	}{
		{
			name: "imagePullIPolicy",
			// 期望未指定拉取策略时默认为 PullAlways（由镜像名无 tag 推导）。
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							InitContainers: []v1.Container{
								{
									Name:  "install",
									Image: "busybox",
								},
							},
						},
					},
				},
			},
			expected: []v1.Container{
				{
					ImagePullPolicy: v1.PullAlways,
				},
			},
			validators: []InitContainerValidator{assertImagePullPolicy},
		},
		{
			name: "FieldRef",
			// 期望 FieldRef 的 APIVersion 被默认填充为 v1。
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							InitContainers: []v1.Container{
								{
									Name:  "fun",
									Image: "alpine",
									Env: []v1.EnvVar{
										{
											Name: "MY_POD_IP",
											ValueFrom: &v1.EnvVarSource{
												FieldRef: &v1.ObjectFieldSelector{
													APIVersion: "",
													FieldPath:  "status.podIP",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []v1.Container{
				{
					Env: []v1.EnvVar{
						{
							Name: "MY_POD_IP",
							ValueFrom: &v1.EnvVarSource{
								FieldRef: &v1.ObjectFieldSelector{
									APIVersion: "v1",
									FieldPath:  "status.podIP",
								},
							},
						},
					},
				},
			},
			validators: []InitContainerValidator{assertEnvFieldRef},
		},
		{
			name: "ContainerPort",
			// 期望容器端口协议被默认填充为 TCP。
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							InitContainers: []v1.Container{
								{
									Name:  "fun",
									Image: "alpine",
									Ports: []v1.ContainerPort{
										{
											Name: "default",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []v1.Container{
				{
					Ports: []v1.ContainerPort{
						{
							Name:     "default",
							Protocol: v1.ProtocolTCP,
						},
					},
				},
			},
			validators: []InitContainerValidator{assertContainerPort},
		},
		{
			name: "Resources",
			// 已显式设置的 limits/requests 应保持原值。
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							InitContainers: []v1.Container{
								{
									Name:  "fun",
									Image: "alpine",
									Resources: v1.ResourceRequirements{
										Limits: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("100m"),
											v1.ResourceMemory: resource.MustParse("100Mi"),
										},
										Requests: v1.ResourceList{
											v1.ResourceCPU:    resource.MustParse("100m"),
											v1.ResourceMemory: resource.MustParse("100Mi"),
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    cpu,
							v1.ResourceMemory: mem,
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU:    cpu,
							v1.ResourceMemory: mem,
						},
					},
				},
			},
			validators: []InitContainerValidator{assertResource},
		},
		{
			name: "Probe",
			// 期望探针的默认路径、协议与四个阈值全部补齐。
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							InitContainers: []v1.Container{
								{
									Name:  "fun",
									Image: "alpine",
									LivenessProbe: &v1.Probe{
										ProbeHandler: v1.ProbeHandler{
											HTTPGet: &v1.HTTPGetAction{
												Host: "localhost",
											},
										},
									},
									ReadinessProbe: &v1.Probe{
										ProbeHandler: v1.ProbeHandler{
											HTTPGet: &v1.HTTPGetAction{
												Host: "localhost",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []v1.Container{
				{
					LivenessProbe: &v1.Probe{
						ProbeHandler: v1.ProbeHandler{
							HTTPGet: &v1.HTTPGetAction{
								Path:   "/",
								Scheme: v1.URISchemeHTTP,
							},
						},
						TimeoutSeconds:   1,
						PeriodSeconds:    10,
						SuccessThreshold: 1,
						FailureThreshold: 3,
					},
					ReadinessProbe: &v1.Probe{
						ProbeHandler: v1.ProbeHandler{
							HTTPGet: &v1.HTTPGetAction{
								Path:   "/",
								Scheme: v1.URISchemeHTTP,
							},
						},
						TimeoutSeconds:   1,
						PeriodSeconds:    10,
						SuccessThreshold: 1,
						FailureThreshold: 3,
					},
				},
			},
			validators: []InitContainerValidator{assertProb},
		},
		{
			name: "LifeCycle",
			// 期望生命周期钩子的 HTTPGet 路径与协议被默认填充。
			rc: v1.ReplicationController{
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							InitContainers: []v1.Container{
								{
									Name:  "fun",
									Image: "alpine",
									Ports: []v1.ContainerPort{
										{
											Name: "default",
										},
									},
									Lifecycle: &v1.Lifecycle{
										PostStart: &v1.LifecycleHandler{
											HTTPGet: &v1.HTTPGetAction{
												Host: "localhost",
											},
										},
										PreStop: &v1.LifecycleHandler{
											HTTPGet: &v1.HTTPGetAction{
												Host: "localhost",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []v1.Container{
				{
					Lifecycle: &v1.Lifecycle{
						PostStart: &v1.LifecycleHandler{
							HTTPGet: &v1.HTTPGetAction{
								Path:   "/",
								Scheme: v1.URISchemeHTTP,
							},
						},
						PreStop: &v1.LifecycleHandler{
							HTTPGet: &v1.HTTPGetAction{
								Path:   "/",
								Scheme: v1.URISchemeHTTP,
							},
						},
					},
				},
			},
			validators: []InitContainerValidator{assertLifeCycle},
		},
	}

	assertInitContainers := func(got, expected []v1.Container, validators []InitContainerValidator) error {
		// 统一比较容器数量并对每个容器运行该用例指定的 validators。
		if len(got) != len(expected) {
			return fmt.Errorf("different number of init container: got <%d>, expected <%d>",
				len(got), len(expected))
		}

		for i := range got {
			g := &got[i]
			e := &expected[i]

			for _, validator := range validators {
				if err := validator(g, e); err != nil {
					return err
				}
			}
		}

		return nil
	}

	for _, test := range tests {
		rc := &test.rc
		obj2 := roundTrip(t, runtime.Object(rc))
		rc2, ok := obj2.(*v1.ReplicationController)
		if !ok {
			t.Errorf("unexpected object: %v", rc2)
			t.FailNow()
		}

		if err := assertInitContainers(rc2.Spec.Template.Spec.InitContainers, test.expected, test.validators); err != nil {
			t.Errorf("test %v failed: %v", test.name, err)
		}
	}
}

// TestSetDefaultService 验证空 Service 的默认 type 为 ClusterIP、sessionAffinity 为 None，
// 且不应生成空的 sessionAffinityConfig。
func TestSetDefaultService(t *testing.T) {
	svc := &v1.Service{}
	obj2 := roundTrip(t, runtime.Object(svc))
	svc2 := obj2.(*v1.Service)
	if svc2.Spec.SessionAffinity != v1.ServiceAffinityNone {
		t.Errorf("Expected default session affinity type:%s, got: %s", v1.ServiceAffinityNone, svc2.Spec.SessionAffinity)
	}
	if svc2.Spec.SessionAffinityConfig != nil {
		t.Errorf("Expected empty session affinity config when session affinity type: %s, got: %v", v1.ServiceAffinityNone, svc2.Spec.SessionAffinityConfig)
	}
	if svc2.Spec.Type != v1.ServiceTypeClusterIP {
		t.Errorf("Expected default type:%s, got: %s", v1.ServiceTypeClusterIP, svc2.Spec.Type)
	}
}

// TestSetDefaultServiceSessionAffinityConfig 验证 ClientIP 会话亲和场景下，
// 无论配置缺失到哪一层，超时时间都会被填充为默认值。
func TestSetDefaultServiceSessionAffinityConfig(t *testing.T) {
	testCases := map[string]v1.Service{
		// 三种残缺程度：整个 config 缺失、ClientIP 缺失、TimeoutSeconds 缺失。
		"SessionAffinityConfig is empty": {
			Spec: v1.ServiceSpec{
				SessionAffinity:       v1.ServiceAffinityClientIP,
				SessionAffinityConfig: nil,
			},
		},
		"ClientIP is empty": {
			Spec: v1.ServiceSpec{
				SessionAffinity: v1.ServiceAffinityClientIP,
				SessionAffinityConfig: &v1.SessionAffinityConfig{
					ClientIP: nil,
				},
			},
		},
		"TimeoutSeconds is empty": {
			Spec: v1.ServiceSpec{
				SessionAffinity: v1.ServiceAffinityClientIP,
				SessionAffinityConfig: &v1.SessionAffinityConfig{
					ClientIP: &v1.ClientIPConfig{
						TimeoutSeconds: nil,
					},
				},
			},
		},
	}
	for name, test := range testCases {
		obj2 := roundTrip(t, runtime.Object(&test))
		svc2 := obj2.(*v1.Service)
		if svc2.Spec.SessionAffinityConfig == nil || svc2.Spec.SessionAffinityConfig.ClientIP == nil || svc2.Spec.SessionAffinityConfig.ClientIP.TimeoutSeconds == nil {
			// 任一层级缺失都意味着默认化未完整执行，直接致命失败。
			t.Fatalf("Case: %s, unexpected empty SessionAffinityConfig/ClientIP/TimeoutSeconds when session affinity type: %s, got: %v", name, v1.ServiceAffinityClientIP, svc2.Spec.SessionAffinityConfig)
		}
		if *svc2.Spec.SessionAffinityConfig.ClientIP.TimeoutSeconds != v1.DefaultClientIPServiceAffinitySeconds {
			t.Errorf("Case: %s, default TimeoutSeconds should be %d when session affinity type: %s, got: %d", name, v1.DefaultClientIPServiceAffinitySeconds, v1.ServiceAffinityClientIP, *svc2.Spec.SessionAffinityConfig.ClientIP.TimeoutSeconds)
		}
	}
}

// TestSetDefaultServiceLoadbalancerIPMode 验证 LoadBalancer 类型 Service 的 Ingress IPMode：
// 未设置时默认为 VIP，已设置时保持原值。
func TestSetDefaultServiceLoadbalancerIPMode(t *testing.T) {
	modeVIP := v1.LoadBalancerIPModeVIP
	modeProxy := v1.LoadBalancerIPModeProxy
	testCases := []struct {
		// 分别覆盖仅设置 IP（期望默认 VIP）与 IP + IPMode 均已设置（期望保持 Proxy）两种输入。
		name           string
		svc            *v1.Service
		expectedIPMode []*v1.LoadBalancerIPMode
	}{
		{
			name: "Set IP but not set IPMode",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{{
							IP: "1.2.3.4",
						}},
					},
				}},
			expectedIPMode: []*v1.LoadBalancerIPMode{&modeVIP},
		}, {
			name: "Both IP and IPMode are set",
			svc: &v1.Service{
				Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{
						Ingress: []v1.LoadBalancerIngress{{
							IP:     "1.2.3.4",
							IPMode: &modeProxy,
						}},
					},
				}},
			expectedIPMode: []*v1.LoadBalancerIPMode{&modeProxy},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			obj := roundTrip(t, runtime.Object(tc.svc))
			svc := obj.(*v1.Service)
			for i, s := range svc.Status.LoadBalancer.Ingress {
				got := s.IPMode
				expected := tc.expectedIPMode[i]
				if !reflect.DeepEqual(got, expected) {
					t.Errorf("Expected IPMode %v, got %v", tc.expectedIPMode[i], s.IPMode)
				}
			}
		})
	}
}

// TestSetDefaultSecretVolumeSource 验证 Secret 卷的 DefaultMode 被填充为默认权限。
func TestSetDefaultSecretVolumeSource(t *testing.T) {
	s := v1.PodSpec{}
	s.Volumes = []v1.Volume{
		{
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	output := roundTrip(t, runtime.Object(pod))
	pod2 := output.(*v1.Pod)
	defaultMode := pod2.Spec.Volumes[0].VolumeSource.Secret.DefaultMode
	expectedMode := v1.SecretVolumeSourceDefaultMode
	// roundTrip 触发默认化后再读取 DefaultMode，与常量默认值比较。

	if defaultMode == nil || *defaultMode != expectedMode {
		t.Errorf("Expected secret DefaultMode %v, got %v", expectedMode, defaultMode)
	}
}

// TestSetDefaultConfigMapVolumeSource 验证 ConfigMap 卷的 DefaultMode 被填充为默认权限。
func TestSetDefaultConfigMapVolumeSource(t *testing.T) {
	s := v1.PodSpec{}
	s.Volumes = []v1.Volume{
		{
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	output := roundTrip(t, runtime.Object(pod))
	pod2 := output.(*v1.Pod)
	defaultMode := pod2.Spec.Volumes[0].VolumeSource.ConfigMap.DefaultMode
	expectedMode := v1.ConfigMapVolumeSourceDefaultMode
	// roundTrip 触发默认化后再读取 DefaultMode，与常量默认值比较。

	if defaultMode == nil || *defaultMode != expectedMode {
		t.Errorf("Expected v1.ConfigMap DefaultMode %v, got %v", expectedMode, defaultMode)
	}
}

// TestSetDefaultDownwardAPIVolumeSource 验证 DownwardAPI 卷的 DefaultMode 被填充为默认权限。
func TestSetDefaultDownwardAPIVolumeSource(t *testing.T) {
	s := v1.PodSpec{}
	s.Volumes = []v1.Volume{
		{
			VolumeSource: v1.VolumeSource{
				DownwardAPI: &v1.DownwardAPIVolumeSource{},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	output := roundTrip(t, runtime.Object(pod))
	pod2 := output.(*v1.Pod)
	defaultMode := pod2.Spec.Volumes[0].VolumeSource.DownwardAPI.DefaultMode
	expectedMode := v1.DownwardAPIVolumeSourceDefaultMode
	// roundTrip 触发默认化后再读取 DefaultMode，与常量默认值比较。

	if defaultMode == nil || *defaultMode != expectedMode {
		t.Errorf("Expected DownwardAPI DefaultMode %v, got %v", expectedMode, defaultMode)
	}
}

// TestSetDefaultProjectedVolumeSource 验证 Projected 卷的 DefaultMode 被填充为默认权限。
func TestSetDefaultProjectedVolumeSource(t *testing.T) {
	s := v1.PodSpec{}
	s.Volumes = []v1.Volume{
		{
			VolumeSource: v1.VolumeSource{
				Projected: &v1.ProjectedVolumeSource{},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	output := roundTrip(t, runtime.Object(pod))
	pod2 := output.(*v1.Pod)
	defaultMode := pod2.Spec.Volumes[0].VolumeSource.Projected.DefaultMode
	expectedMode := v1.ProjectedVolumeSourceDefaultMode
	// roundTrip 触发默认化后再读取 DefaultMode，与常量默认值比较。

	if defaultMode == nil || *defaultMode != expectedMode {
		t.Errorf("Expected v1.ProjectedVolumeSource DefaultMode %v, got %v", expectedMode, defaultMode)
	}
}

// TestSetDefaultSecret 验证空 Secret 的类型被默认填充为 Opaque。
func TestSetDefaultSecret(t *testing.T) {
	s := &v1.Secret{}
	obj2 := roundTrip(t, runtime.Object(s))
	s2 := obj2.(*v1.Secret)

	if s2.Type != v1.SecretTypeOpaque {
		t.Errorf("Expected secret type %v, got %v", v1.SecretTypeOpaque, s2.Type)
	}
}

// TestSetDefaultPersistentVolume 验证 PV 的默认 phase、回收策略，以及 VolumeMode 未设置时默认为 Filesystem。
func TestSetDefaultPersistentVolume(t *testing.T) {
	fsMode := v1.PersistentVolumeFilesystem
	blockMode := v1.PersistentVolumeBlock

	tests := []struct {
		// 覆盖 VolumeMode 为 nil（期望默认 Filesystem）以及显式 Filesystem/Block 三种输入。
		name               string
		volumeMode         *v1.PersistentVolumeMode
		expectedVolumeMode v1.PersistentVolumeMode
	}{
		{
			name:               "volume mode nil",
			volumeMode:         nil,
			expectedVolumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			name:               "volume mode filesystem",
			volumeMode:         &fsMode,
			expectedVolumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			name:               "volume mode block",
			volumeMode:         &blockMode,
			expectedVolumeMode: v1.PersistentVolumeBlock,
		},
	}

	for _, test := range tests {
		pv := &v1.PersistentVolume{
			Spec: v1.PersistentVolumeSpec{
				VolumeMode: test.volumeMode,
			},
		}
		obj1 := roundTrip(t, runtime.Object(pv))
		pv1 := obj1.(*v1.PersistentVolume)
		if pv1.Status.Phase != v1.VolumePending {
			t.Errorf("Expected claim phase %v, got %v", v1.ClaimPending, pv1.Status.Phase)
		}
		if pv1.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
			t.Errorf("Expected pv reclaim policy %v, got %v", v1.PersistentVolumeReclaimRetain, pv1.Spec.PersistentVolumeReclaimPolicy)
		}
		if *pv1.Spec.VolumeMode != test.expectedVolumeMode {
			t.Errorf("Test %s failed, Expected VolumeMode: %v, but got %v", test.name, test.volumeMode, *pv1.Spec.VolumeMode)
		}
	}
}

// TestSetDefaultPersistentVolumeClaim 验证 PVC 的默认 phase 与 VolumeMode 默认化规则。
func TestSetDefaultPersistentVolumeClaim(t *testing.T) {
	fsMode := v1.PersistentVolumeFilesystem
	blockMode := v1.PersistentVolumeBlock

	tests := []struct {
		// 覆盖 VolumeMode 为 nil（期望默认 Filesystem）以及显式 Filesystem/Block 三种输入。
		name               string
		volumeMode         *v1.PersistentVolumeMode
		expectedVolumeMode v1.PersistentVolumeMode
	}{
		{
			name:               "volume mode nil",
			volumeMode:         nil,
			expectedVolumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			name:               "volume mode filesystem",
			volumeMode:         &fsMode,
			expectedVolumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			name:               "volume mode block",
			volumeMode:         &blockMode,
			expectedVolumeMode: v1.PersistentVolumeBlock,
		},
	}

	for _, test := range tests {
		pvc := &v1.PersistentVolumeClaim{
			Spec: v1.PersistentVolumeClaimSpec{
				VolumeMode: test.volumeMode,
			},
		}
		obj1 := roundTrip(t, runtime.Object(pvc))
		pvc1 := obj1.(*v1.PersistentVolumeClaim)
		if pvc1.Status.Phase != v1.ClaimPending {
			t.Errorf("Expected claim phase %v, got %v", v1.ClaimPending, pvc1.Status.Phase)
		}
		if *pvc1.Spec.VolumeMode != test.expectedVolumeMode {
			t.Errorf("Test %s failed, Expected VolumeMode: %v, but got %v", test.name, test.volumeMode, *pvc1.Spec.VolumeMode)
		}
	}
}

// TestSetDefaultEphemeral 验证临时卷（Ephemeral）中 PVC 模板的 VolumeMode 默认化规则。
func TestSetDefaultEphemeral(t *testing.T) {
	fsMode := v1.PersistentVolumeFilesystem
	blockMode := v1.PersistentVolumeBlock

	tests := []struct {
		// 覆盖 VolumeMode 为 nil（期望默认 Filesystem）以及显式 Filesystem/Block 三种输入。
		name               string
		volumeMode         *v1.PersistentVolumeMode
		expectedVolumeMode v1.PersistentVolumeMode
	}{
		{
			name:               "volume mode nil",
			volumeMode:         nil,
			expectedVolumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			name:               "volume mode filesystem",
			volumeMode:         &fsMode,
			expectedVolumeMode: v1.PersistentVolumeFilesystem,
		},
		{
			name:               "volume mode block",
			volumeMode:         &blockMode,
			expectedVolumeMode: v1.PersistentVolumeBlock,
		},
	}

	for _, test := range tests {
		pod := &v1.Pod{
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						VolumeSource: v1.VolumeSource{
							Ephemeral: &v1.EphemeralVolumeSource{
								VolumeClaimTemplate: &v1.PersistentVolumeClaimTemplate{
									Spec: v1.PersistentVolumeClaimSpec{
										VolumeMode: test.volumeMode,
									},
								},
							},
						},
					},
				},
			},
		}
		obj1 := roundTrip(t, runtime.Object(pod))
		pod1 := obj1.(*v1.Pod)
		if *pod1.Spec.Volumes[0].VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.VolumeMode != test.expectedVolumeMode {
			t.Errorf("Test %s failed, Expected VolumeMode: %v, but got %v", test.name, test.volumeMode, *pod1.Spec.Volumes[0].VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.VolumeMode)
		}
	}
}

// TestSetDefaultEndpointsProtocol 验证 Endpoints 端口协议未设置时默认为 TCP，已设置时原样保留。
func TestSetDefaultEndpointsProtocol(t *testing.T) {
	in := &v1.Endpoints{Subsets: []v1.EndpointSubset{
		// 刻意混入空协议与显式 UDP 两种端口，验证“空则填充、非空则保留”。
		{Ports: []v1.EndpointPort{{}, {Protocol: "UDP"}, {}}},
	}}
	obj := roundTrip(t, runtime.Object(in))
	out := obj.(*v1.Endpoints)

	for i := range out.Subsets {
		for j := range out.Subsets[i].Ports {
			if in.Subsets[i].Ports[j].Protocol == "" {
				if out.Subsets[i].Ports[j].Protocol != v1.ProtocolTCP {
					t.Errorf("Expected protocol %s, got %s", v1.ProtocolTCP, out.Subsets[i].Ports[j].Protocol)
				}
			} else {
				if out.Subsets[i].Ports[j].Protocol != in.Subsets[i].Ports[j].Protocol {
					t.Errorf("Expected protocol %s, got %s", in.Subsets[i].Ports[j].Protocol, out.Subsets[i].Ports[j].Protocol)
				}
			}
		}
	}
}

// TestSetDefaultServiceTargetPort 验证 Service 端口的 TargetPort 未设置时默认等于 Port，已设置时不被覆盖。
func TestSetDefaultServiceTargetPort(t *testing.T) {
	in := &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{Port: 1234}}}}
	obj := roundTrip(t, runtime.Object(in))
	out := obj.(*v1.Service)
	if out.Spec.Ports[0].TargetPort != intstr.FromInt32(1234) {
		t.Errorf("Expected TargetPort to be defaulted, got %v", out.Spec.Ports[0].TargetPort)
	}

	in = &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{Port: 1234, TargetPort: intstr.FromInt32(5678)}}}}
	// 第二段：显式 TargetPort 必须保持不变。
	obj = roundTrip(t, runtime.Object(in))
	out = obj.(*v1.Service)
	if out.Spec.Ports[0].TargetPort != intstr.FromInt32(5678) {
		t.Errorf("Expected TargetPort to be unchanged, got %v", out.Spec.Ports[0].TargetPort)
	}
}

// TestSetDefaultServicePort 验证 Service 端口的协议与 TargetPort：显式设置时保留，未设置时分别默认为 TCP 和 Port。
func TestSetDefaultServicePort(t *testing.T) {
	// 第一段：显式设置的协议和 TargetPort 不应被更改。
	in := &v1.Service{Spec: v1.ServiceSpec{
		Ports: []v1.ServicePort{
			{Protocol: "UDP", Port: 9376, TargetPort: intstr.FromString("p")},
			{Protocol: "UDP", Port: 8675, TargetPort: intstr.FromInt32(309)},
		},
	}}
	out := roundTrip(t, runtime.Object(in)).(*v1.Service)
	if out.Spec.Ports[0].Protocol != v1.ProtocolUDP {
		t.Errorf("Expected protocol %s, got %s", v1.ProtocolUDP, out.Spec.Ports[0].Protocol)
	}
	if out.Spec.Ports[0].TargetPort != intstr.FromString("p") {
		t.Errorf("Expected port %v, got %v", in.Spec.Ports[0].Port, out.Spec.Ports[0].TargetPort)
	}
	if out.Spec.Ports[1].Protocol != v1.ProtocolUDP {
		t.Errorf("Expected protocol %s, got %s", v1.ProtocolUDP, out.Spec.Ports[1].Protocol)
	}
	if out.Spec.Ports[1].TargetPort != intstr.FromInt32(309) {
		t.Errorf("Expected port %v, got %v", in.Spec.Ports[1].Port, out.Spec.Ports[1].TargetPort)
	}

	// 第二段：空值应被默认化为 TCP 以及与 Port 相同的 TargetPort。
	in = &v1.Service{Spec: v1.ServiceSpec{
		Ports: []v1.ServicePort{
			{Protocol: "", Port: 9376, TargetPort: intstr.FromString("")},
			{Protocol: "", Port: 8675, TargetPort: intstr.FromInt32(0)},
		},
	}}
	out = roundTrip(t, runtime.Object(in)).(*v1.Service)
	if out.Spec.Ports[0].Protocol != v1.ProtocolTCP {
		t.Errorf("Expected protocol %s, got %s", v1.ProtocolTCP, out.Spec.Ports[0].Protocol)
	}
	if out.Spec.Ports[0].TargetPort != intstr.FromInt32(in.Spec.Ports[0].Port) {
		t.Errorf("Expected port %v, got %v", in.Spec.Ports[0].Port, out.Spec.Ports[0].TargetPort)
	}
	if out.Spec.Ports[1].Protocol != v1.ProtocolTCP {
		t.Errorf("Expected protocol %s, got %s", v1.ProtocolTCP, out.Spec.Ports[1].Protocol)
	}
	if out.Spec.Ports[1].TargetPort != intstr.FromInt32(in.Spec.Ports[1].Port) {
		t.Errorf("Expected port %v, got %v", in.Spec.Ports[1].Port, out.Spec.Ports[1].TargetPort)
	}
}

// TestSetDefaultServiceExternalTraffic 验证 ExternalTrafficPolicy 只在 NodePort/LoadBalancer
// 等对外暴露类型上默认填充为 Cluster，其余类型保持为空。
func TestSetDefaultServiceExternalTraffic(t *testing.T) {
	in := &v1.Service{}
	obj := roundTrip(t, runtime.Object(in))
	out := obj.(*v1.Service)
	if out.Spec.ExternalTrafficPolicy != "" {
		t.Errorf("Expected ExternalTrafficPolicy to be empty, got %v", out.Spec.ExternalTrafficPolicy)
	}

	in = &v1.Service{Spec: v1.ServiceSpec{Type: v1.ServiceTypeNodePort}}
	// NodePort 类型应默认填充 Cluster。
	obj = roundTrip(t, runtime.Object(in))
	out = obj.(*v1.Service)
	if out.Spec.ExternalTrafficPolicy != v1.ServiceExternalTrafficPolicyCluster {
		t.Errorf("Expected ExternalTrafficPolicy to be %v, got %v", v1.ServiceExternalTrafficPolicyCluster, out.Spec.ExternalTrafficPolicy)
	}

	in = &v1.Service{Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer}}
	// LoadBalancer 类型同样应默认填充 Cluster。
	obj = roundTrip(t, runtime.Object(in))
	out = obj.(*v1.Service)
	if out.Spec.ExternalTrafficPolicy != v1.ServiceExternalTrafficPolicyCluster {
		t.Errorf("Expected ExternalTrafficPolicy to be %v, got %v", v1.ServiceExternalTrafficPolicyCluster, out.Spec.ExternalTrafficPolicy)
	}

	in = &v1.Service{Spec: v1.ServiceSpec{Type: v1.ServiceTypeClusterIP, ExternalIPs: []string{"1.2.3.4"}}}
	// 带 ExternalIP 的 ClusterIP 也视为对外暴露，应默认填充 Cluster。
	obj = roundTrip(t, runtime.Object(in))
	out = obj.(*v1.Service)
	if out.Spec.ExternalTrafficPolicy != v1.ServiceExternalTrafficPolicyCluster {
		t.Errorf("Expected ExternalTrafficPolicy to be %v, got %v", v1.ServiceExternalTrafficPolicyCluster, out.Spec.ExternalTrafficPolicy)
	}

	in = &v1.Service{Spec: v1.ServiceSpec{Type: v1.ServiceTypeClusterIP}}
	// 普通 ClusterIP 不填充该字段。
	obj = roundTrip(t, runtime.Object(in))
	out = obj.(*v1.Service)
	if out.Spec.ExternalTrafficPolicy != "" {
		t.Errorf("Expected ExternalTrafficPolicy to be empty, got %v", out.Spec.ExternalTrafficPolicy)
	}

	in = &v1.Service{Spec: v1.ServiceSpec{Type: v1.ServiceTypeExternalName}}
	// ExternalName 也不填充该字段。
	obj = roundTrip(t, runtime.Object(in))
	out = obj.(*v1.Service)
	if out.Spec.ExternalTrafficPolicy != "" {
		t.Errorf("Expected ExternalTrafficPolicy to be empty, got %v", out.Spec.ExternalTrafficPolicy)
	}
}

// TestSetDefaultNamespace 验证空 Namespace 的状态被默认填充为 Active。
func TestSetDefaultNamespace(t *testing.T) {
	s := &v1.Namespace{}
	obj2 := roundTrip(t, runtime.Object(s))
	s2 := obj2.(*v1.Namespace)

	if s2.Status.Phase != v1.NamespaceActive {
		t.Errorf("Expected phase %v, got %v", v1.NamespaceActive, s2.Status.Phase)
	}
}

// TestSetDefaultNamespaceLabels 验证 Namespace 的 metadata.name 标签被默认设置为其名称。
func TestSetDefaultNamespaceLabels(t *testing.T) {
	theNs := "default-ns-labels-are-great"
	s := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: theNs,
		},
	}
	obj2 := roundTrip(t, runtime.Object(s))
	s2 := obj2.(*v1.Namespace)

	if s2.ObjectMeta.Labels[v1.LabelMetadataName] != theNs {
		t.Errorf("Expected default namespace label value of %v, but got %v", theNs, s2.ObjectMeta.Labels[v1.LabelMetadataName])
	}
}

// TestSetDefaultPodSpecHostNetwork 验证 hostNetwork 开启时，普通容器与 init container 的 HostPort
// 都会被默认设置为对应的 ContainerPort。
func TestSetDefaultPodSpecHostNetwork(t *testing.T) {
	portNum := int32(8080)
	// 用 ContainerPort 与 HostPort 的相等关系来验证默认化是否生效。
	s := v1.PodSpec{}
	s.HostNetwork = true
	s.Containers = []v1.Container{
		{
			Ports: []v1.ContainerPort{
				{
					ContainerPort: portNum,
				},
			},
		},
	}
	s.InitContainers = []v1.Container{
		{
			Ports: []v1.ContainerPort{
				{
					ContainerPort: portNum,
				},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	obj2 := roundTrip(t, runtime.Object(pod))
	pod2 := obj2.(*v1.Pod)
	s2 := pod2.Spec

	hostPortNum := s2.Containers[0].Ports[0].HostPort
	if hostPortNum != portNum {
		t.Errorf("Expected container port to be defaulted, was made %d instead of %d", hostPortNum, portNum)
	}

	hostPortNum = s2.InitContainers[0].Ports[0].HostPort
	if hostPortNum != portNum {
		t.Errorf("Expected container port to be defaulted, was made %d instead of %d", hostPortNum, portNum)
	}
}

// TestSetDefaultNodeStatusAllocatable 验证 Node 状态中 Allocatable 的默认化：
// 未设置时回退到 Capacity，已设置时保持原值。
func TestSetDefaultNodeStatusAllocatable(t *testing.T) {
	capacity := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("1000m"),
		v1.ResourceMemory: resource.MustParse("10G"),
	}
	allocatable := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("500m"),
		v1.ResourceMemory: resource.MustParse("5G"),
	}
	tests := []struct {
		// 四个用例覆盖 capacity/allocatable 的有无组合，验证回退与保留规则。
		capacity            v1.ResourceList
		allocatable         v1.ResourceList
		expectedAllocatable v1.ResourceList
	}{{ // 两者都设置，无需默认化。
		capacity:            capacity,
		allocatable:         allocatable,
		expectedAllocatable: allocatable,
	}, { // 仅 Allocatable 设置，保持原值。
		capacity:            nil,
		allocatable:         allocatable,
		expectedAllocatable: allocatable,
	}, { // 仅 Capacity 设置，Allocatable 应回退为 Capacity。
		capacity:            capacity,
		allocatable:         nil,
		expectedAllocatable: capacity,
	}, { // 两者都未设置，Allocatable 最终也为 nil。
		capacity:            nil,
		allocatable:         nil,
		expectedAllocatable: nil,
	}}

	copyResourceList := func(rl v1.ResourceList) v1.ResourceList {
		// 深拷贝资源列表，避免默认化修改输入后污染同一结构体中的共享引用。
		if rl == nil {
			return nil
		}
		copy := make(v1.ResourceList, len(rl))
		for k, v := range rl {
			copy[k] = v.DeepCopy()
		}
		return copy
	}

	resourceListsEqual := func(a v1.ResourceList, b v1.ResourceList) bool {
		// 用 Cmp 比较数量值，避免 Quantity 内部表示差异导致误判。
		if len(a) != len(b) {
			return false
		}
		for k, v := range a {
			vb, found := b[k]
			if !found {
				return false
			}
			if v.Cmp(vb) != 0 {
				return false
			}
		}
		return true
	}

	for i, testcase := range tests {
		node := v1.Node{
			Status: v1.NodeStatus{
				Capacity:    copyResourceList(testcase.capacity),
				Allocatable: copyResourceList(testcase.allocatable),
			},
		}
		node2 := roundTrip(t, runtime.Object(&node)).(*v1.Node)
		actual := node2.Status.Allocatable
		expected := testcase.expectedAllocatable
		if !resourceListsEqual(expected, actual) {
			t.Errorf("[%d] Expected v1.NodeStatus.Allocatable: %+v; Got: %+v", i, expected, actual)
		}
	}
}

// TestSetDefaultObjectFieldSelectorAPIVersion 验证 FieldRef 的 APIVersion 未设置时默认为 v1。
func TestSetDefaultObjectFieldSelectorAPIVersion(t *testing.T) {
	s := v1.PodSpec{
		Containers: []v1.Container{
			{
				Env: []v1.EnvVar{
					{
						ValueFrom: &v1.EnvVarSource{
							FieldRef: &v1.ObjectFieldSelector{},
						},
					},
				},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	obj2 := roundTrip(t, runtime.Object(pod))
	pod2 := obj2.(*v1.Pod)
	s2 := pod2.Spec

	apiVersion := s2.Containers[0].Env[0].ValueFrom.FieldRef.APIVersion
	// 读取往返后的字段，与期望的 v1 比较。
	if apiVersion != "v1" {
		t.Errorf("Expected default APIVersion v1, got: %v", apiVersion)
	}
}

// TestSetMinimumScalePod 验证资源请求的“取整”默认行为：极小的 requests 会被提升到最小可表示单位。
func TestSetMinimumScalePod(t *testing.T) {
	// 有 limits 时会触发默认化；同时验证 request=0 这类显式值不被覆盖。
	s := v1.PodSpec{}
	s.Containers = []v1.Container{
		{
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("1n"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("2n"),
				},
			},
		},
	}
	s.InitContainers = []v1.Container{
		{
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("1n"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("2n"),
				},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	corev1.SetObjectDefaults_Pod(pod)

	if expect := resource.MustParse("1m"); expect.Cmp(pod.Spec.Containers[0].Resources.Requests[v1.ResourceMemory]) != 0 {
		t.Errorf("did not round resources: %#v", pod.Spec.Containers[0].Resources)
	}
	if expect := resource.MustParse("1m"); expect.Cmp(pod.Spec.InitContainers[0].Resources.Requests[v1.ResourceMemory]) != 0 {
		t.Errorf("did not round resources: %#v", pod.Spec.InitContainers[0].Resources)
	}
}

// TestSetDefaultRequestsPod 验证容器 requests 默认从 limits 推导，且显式设置为零的值被保留。
func TestSetDefaultRequestsPod(t *testing.T) {
	// 有 limits 时会触发默认化；同时验证 request=0 这类显式值不被覆盖。
	// 第一段：limits 存在时，缺失的 CPU request 应从 limits 复制，而显式为 0 的 memory request 保持为 0。
	s := v1.PodSpec{}
	s.Containers = []v1.Container{
		{
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("0"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
	}
	s.InitContainers = []v1.Container{
		{
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("0"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("1Gi"),
				},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	output := roundTrip(t, runtime.Object(pod))
	pod2 := output.(*v1.Pod)
	defaultRequest := pod2.Spec.Containers[0].Resources.Requests
	if requestValue := defaultRequest[v1.ResourceCPU]; requestValue.String() != "100m" {
		t.Errorf("Expected request cpu: %s, got: %s", "100m", requestValue.String())
	}
	if requestValue := defaultRequest[v1.ResourceMemory]; requestValue.String() != "0" {
		t.Errorf("Expected request memory: %s, got: %s", "0", requestValue.String())
	}
	defaultRequest = pod2.Spec.InitContainers[0].Resources.Requests
	if requestValue := defaultRequest[v1.ResourceCPU]; requestValue.String() != "100m" {
		t.Errorf("Expected request cpu: %s, got: %s", "100m", requestValue.String())
	}
	if requestValue := defaultRequest[v1.ResourceMemory]; requestValue.String() != "0" {
		t.Errorf("Expected request memory: %s, got: %s", "0", requestValue.String())
	}

	// 第二段：没有任何 limits 时，不应凭空生成 requests。
	s = v1.PodSpec{}
	s.Containers = []v1.Container{{}}
	s.InitContainers = []v1.Container{{}}
	pod = &v1.Pod{
		Spec: s,
	}
	output = roundTrip(t, runtime.Object(pod))
	pod2 = output.(*v1.Pod)
	defaultRequest = pod2.Spec.Containers[0].Resources.Requests
	if requestValue := defaultRequest[v1.ResourceCPU]; requestValue.String() != "0" {
		t.Errorf("Expected 0 request value, got: %s", requestValue.String())
	}
	defaultRequest = pod2.Spec.InitContainers[0].Resources.Requests
	if requestValue := defaultRequest[v1.ResourceCPU]; requestValue.String() != "0" {
		t.Errorf("Expected 0 request value, got: %s", requestValue.String())
	}
}

// TestDefaultRequestIsNotSetForReplicationController 验证 PodTemplateSpec（RC 模板）中的容器不会
// 像独立 Pod 那样从 limits 推导 requests，以免升级时改变工作负载的配置。
func TestDefaultRequestIsNotSetForReplicationController(t *testing.T) {
	s := v1.PodSpec{}
	s.Containers = []v1.Container{
		{
			Resources: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("100m"),
				},
			},
		},
	}
	rc := &v1.ReplicationController{
		Spec: v1.ReplicationControllerSpec{
			Replicas: ptr.To[int32](3),
			Template: &v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: s,
			},
		},
	}
	output := roundTrip(t, runtime.Object(rc))
	rc2 := output.(*v1.ReplicationController)
	defaultRequest := rc2.Spec.Template.Spec.Containers[0].Resources.Requests
	// 即使容器设置了 CPU limit，模板中的 request 也应保持为 0。
	requestValue := defaultRequest[v1.ResourceCPU]
	if requestValue.String() != "0" {
		t.Errorf("Expected 0 request value, got: %s", requestValue.String())
	}
}

// TestSetDefaultLimitRangeItem 验证 LimitRange 条目的默认 default/defaultRequest 推导规则。
func TestSetDefaultLimitRangeItem(t *testing.T) {
	limitRange := &v1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-defaults",
		},
		Spec: v1.LimitRangeSpec{
			Limits: []v1.LimitRangeItem{{
				Type: v1.LimitTypeContainer,
				Max: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("100m"),
				},
				Min: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Default:        v1.ResourceList{},
				DefaultRequest: v1.ResourceList{},
			}},
		},
	}

	output := roundTrip(t, runtime.Object(limitRange))
	limitRange2 := output.(*v1.LimitRange)
	defaultLimit := limitRange2.Spec.Limits[0].Default
	defaultRequest := limitRange2.Spec.Limits[0].DefaultRequest

	// default 取自 max 中与容器相关的 CPU 上限。
	defaultValue := defaultLimit[v1.ResourceCPU]
	if defaultValue.String() != "100m" {
		t.Errorf("Expected default cpu: %s, got: %s", "100m", defaultValue.String())
	}
	// defaultRequest 的 CPU 值取自同一 max。
	requestValue := defaultRequest[v1.ResourceCPU]
	if requestValue.String() != "100m" {
		t.Errorf("Expected request cpu: %s, got: %s", "100m", requestValue.String())
	}
	// 若某资源在 max 中没有对应值而 min 中提供了，则 defaultRequest 取 min 的值。
	requestMinValue := defaultRequest[v1.ResourceMemory]
	if requestMinValue.String() != "100Mi" {
		t.Errorf("Expected request memory: %s, got: %s", "100Mi", requestMinValue.String())
	}
}

// TestSetDefaultProbe 验证空探针的超时、周期、成功/失败阈值被填充为默认值。
func TestSetDefaultProbe(t *testing.T) {
	originalProbe := v1.Probe{}
	expectedProbe := v1.Probe{
		// 期望值：初始延迟为 0，其余四个阈值使用官方默认值。
		InitialDelaySeconds: 0,
		TimeoutSeconds:      1,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}

	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{LivenessProbe: &originalProbe}},
		},
	}

	output := roundTrip(t, runtime.Object(pod)).(*v1.Pod)
	actualProbe := *output.Spec.Containers[0].LivenessProbe
	if actualProbe != expectedProbe {
		t.Errorf("Expected probe: %+v\ngot: %+v\n", expectedProbe, actualProbe)
	}
}

// TestSetDefaultProbeGRPCMode 验证 gRPC 探针的 Mode 字段默认化行为：
// 显式设置的模式保留，未设置时保持 nil。
func TestSetDefaultProbeGRPCMode(t *testing.T) {
	tlsMode := v1.GRPCProbeModeTLS
	plaintextMode := v1.GRPCProbeModePlaintext
	tests := []struct {
		// 覆盖显式 TLS、显式 Plaintext 与未设置（nil）三种输入。
		name         string
		grpc         *v1.GRPCAction
		expectedMode *v1.GRPCProbeMode
	}{
		{
			name: "mode TLS is preserved",
			grpc: &v1.GRPCAction{
				Port: 8443,
				Mode: &tlsMode,
			},
			expectedMode: &tlsMode,
		},
		{
			name: "mode Plaintext is preserved",
			grpc: &v1.GRPCAction{
				Port: 8443,
				Mode: &plaintextMode,
			},
			expectedMode: &plaintextMode,
		},
		{
			name: "nil mode stays nil",
			grpc: &v1.GRPCAction{
				Port: 8443,
			},
			expectedMode: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						LivenessProbe: &v1.Probe{
							ProbeHandler: v1.ProbeHandler{
								GRPC: tc.grpc,
							},
						},
					}},
				},
			}
			output := roundTrip(t, runtime.Object(pod)).(*v1.Pod)
			actualMode := output.Spec.Containers[0].LivenessProbe.GRPC.Mode
			// 同时比较 nil 性与非 nil 时的具体值。
			if (actualMode == nil) != (tc.expectedMode == nil) || (actualMode != nil && *actualMode != *tc.expectedMode) {
				t.Errorf("expected Mode %v, got %v", tc.expectedMode, actualMode)
			}
		})
	}
}

// TestSetDefaultSchedulerName 验证空 Pod 的调度器名称被默认设置为 default-scheduler。
func TestSetDefaultSchedulerName(t *testing.T) {
	pod := &v1.Pod{}

	output := roundTrip(t, runtime.Object(pod)).(*v1.Pod)
	if output.Spec.SchedulerName != v1.DefaultSchedulerName {
		t.Errorf("Expected scheduler name: %+v\ngot: %+v\n", v1.DefaultSchedulerName, output.Spec.SchedulerName)
	}
}

// TestSetDefaultHostPathVolumeSource 验证 HostPath 卷的 Type 未设置时默认为空字符串（HostPathUnset）。
func TestSetDefaultHostPathVolumeSource(t *testing.T) {
	s := v1.PodSpec{}
	s.Volumes = []v1.Volume{
		{
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "foo"},
			},
		},
	}
	pod := &v1.Pod{
		Spec: s,
	}
	output := roundTrip(t, runtime.Object(pod))
	pod2 := output.(*v1.Pod)
	defaultType := pod2.Spec.Volumes[0].VolumeSource.HostPath.Type
	expectedType := v1.HostPathUnset

	if defaultType == nil || *defaultType != expectedType {
		t.Errorf("Expected v1.HostPathVolumeSource default type %v, got %v", expectedType, defaultType)
	}
}

// TestSetDefaultEnableServiceLinks 验证 enableServiceLinks 缺省时默认为 true。
func TestSetDefaultEnableServiceLinks(t *testing.T) {
	pod := &v1.Pod{}
	output := roundTrip(t, runtime.Object(pod)).(*v1.Pod)
	if output.Spec.EnableServiceLinks == nil || *output.Spec.EnableServiceLinks != v1.DefaultEnableServiceLinks {
		t.Errorf("Expected enableServiceLinks value: %+v\ngot: %+v\n", v1.DefaultEnableServiceLinks, *output.Spec.EnableServiceLinks)
	}
}

// TestSetDefaultServiceInternalTrafficPolicy 验证 Service 的 internalTrafficPolicy：
// 普通 Service 默认设为 Cluster；ExternalName 类型及已显式设置的场景保持不变。
func TestSetDefaultServiceInternalTrafficPolicy(t *testing.T) {
	cluster := v1.ServiceInternalTrafficPolicyCluster
	local := v1.ServiceInternalTrafficPolicyLocal
	testCases := []struct {
		// 覆盖默认填充、已设置 Cluster、ExternalName 类型与已设置 Local 四种场景。
		name                          string
		expectedInternalTrafficPolicy *v1.ServiceInternalTrafficPolicy
		svc                           v1.Service
	}{
		{
			name:                          "must set default internalTrafficPolicy",
			expectedInternalTrafficPolicy: &cluster,
			svc:                           v1.Service{},
		},
		{
			name:                          "must not set default internalTrafficPolicy when it's cluster",
			expectedInternalTrafficPolicy: &cluster,
			svc: v1.Service{
				Spec: v1.ServiceSpec{
					InternalTrafficPolicy: &cluster,
				},
			},
		},
		{
			name:                          "must not set default internalTrafficPolicy when type is ExternalName",
			expectedInternalTrafficPolicy: nil,
			svc: v1.Service{
				Spec: v1.ServiceSpec{
					Type: v1.ServiceTypeExternalName,
				},
			},
		},
		{
			name:                          "must not set default internalTrafficPolicy when it's local",
			expectedInternalTrafficPolicy: &local,
			svc: v1.Service{
				Spec: v1.ServiceSpec{
					InternalTrafficPolicy: &local,
				},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			obj := roundTrip(t, runtime.Object(&test.svc))
			svc := obj.(*v1.Service)

			if !reflect.DeepEqual(svc.Spec.InternalTrafficPolicy, test.expectedInternalTrafficPolicy) {
				t.Errorf("expected .spec.internalTrafficPolicy: %v got %v", test.expectedInternalTrafficPolicy, svc.Spec.InternalTrafficPolicy)
			}
		})
	}
}

// TestSetDefaults_Volume 验证卷的默认化：空卷源回退为 emptyDir，
// Image 卷的 PullPolicy 依据镜像 tag 推导；依赖 ImageVolume 门控开启。
func TestSetDefaults_Volume(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ImageVolume, true)
	// 必须开启 ImageVolume 门控，否则 Image 卷字段在编解码阶段会被裁剪。
	for desc, tc := range map[string]struct {
		// 表格涵盖空卷回退、带 tag、latest tag 与无 tag 四种镜像引用形式。
		given, expected *v1.Volume
	}{
		"defaults to emptyDir": {
			given:    &v1.Volume{},
			expected: &v1.Volume{VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
		},
		"default image volume source pull policy is IfNotPresent": {
			given:    &v1.Volume{VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: "image:v1"}}},
			expected: &v1.Volume{VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: "image:v1", PullPolicy: v1.PullIfNotPresent}}},
		},
		"default image volume source pull policy Always if 'latest' tag is used": {
			given:    &v1.Volume{VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: "image:latest"}}},
			expected: &v1.Volume{VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: "image:latest", PullPolicy: v1.PullAlways}}},
		},
		"default image volume source pull policy Always if no tag is used": {
			given:    &v1.Volume{VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: "image"}}},
			expected: &v1.Volume{VolumeSource: v1.VolumeSource{Image: &v1.ImageVolumeSource{Reference: "image", PullPolicy: v1.PullAlways}}},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			corev1.SetDefaults_Volume(tc.given)
			if !cmp.Equal(tc.given, tc.expected) {
				t.Errorf("expected volume %+v, but got %+v", tc.expected, tc.given)
			}
		})
	}
}

// TestSetDefaults_PodLogOptions 验证 Pod 日志选项的 Stream 默认为 All，且已指定的流不被覆盖；
// 依赖 PodLogsQuerySplitStreams 门控开启。
func TestSetDefaults_PodLogOptions(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLogsQuerySplitStreams, true)
	// 门控关闭时该字段不会被编解码保留，因此测试中显式开启。
	for desc, tc := range map[string]struct {
		// 分别覆盖未设置（应默认 All）与显式指定 Stdout（应保留）两种输入。
		given, expected *v1.PodLogOptions
	}{
		"defaults to All": {
			given:    &v1.PodLogOptions{},
			expected: &v1.PodLogOptions{Stream: ptr.To(v1.LogStreamAll)},
		},
		"the specified stream should not be overridden": {
			given:    &v1.PodLogOptions{Stream: ptr.To(v1.LogStreamStdout)},
			expected: &v1.PodLogOptions{Stream: ptr.To(v1.LogStreamStdout)},
		},
	} {
		t.Run(desc, func(t *testing.T) {
			corev1.SetDefaults_PodLogOptions(tc.given)
			if !cmp.Equal(tc.given, tc.expected) {
				t.Errorf("expected volume %+v, but got %+v", tc.expected, tc.given)
			}
		})
	}
}

// setAllFeatures 将所有未锁定到默认值的特性门控统一打开或关闭，
// 用于在门控全开/全关两种状态下运行同一组默认值探测。
func setAllFeatures(t *testing.T, featuresEnabled bool) {
	features := featuregatetesting.FeatureOverrides{}
	for feature, featureSpec := range utilfeature.DefaultFeatureGate.DeepCopy().GetAll() {
		if !featureSpec.LockToDefault {
			// 锁定到默认值的门控不允许覆盖，否则 SetFeatureGatesDuringTest 会报错。
			features[feature] = featuresEnabled
		}
	}
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, features)
}

// TestSetDefaultPodStatusPodIPs 验证 PodIP 与 PodIPs 的一致性默认化：
// 仅有其一会从另一方补全，两者冲突时以单值的 PodIP 为准。
func TestSetDefaultPodStatusPodIPs(t *testing.T) {
	tests := []struct {
		// 覆盖单边设置、冲突、双栈一致以及两边都为空的场景。
		name      string
		podIP     string
		podIPs    []v1.PodIP
		expectIP  string
		expectIPs []v1.PodIP
	}{
		{name: "only podIP, podIPs synthesized", podIP: "10.0.0.1", expectIP: "10.0.0.1", expectIPs: []v1.PodIP{{IP: "10.0.0.1"}}},
		{name: "mismatched, podIP authoritative", podIP: "10.0.0.2", podIPs: []v1.PodIP{{IP: "10.0.0.1"}, {IP: "2000::"}}, expectIP: "10.0.0.2", expectIPs: []v1.PodIP{{IP: "10.0.0.2"}}},
		{name: "only podIPs, podIP filled", podIPs: []v1.PodIP{{IP: "10.0.0.1"}, {IP: "2000::"}}, expectIP: "10.0.0.1", expectIPs: []v1.PodIP{{IP: "10.0.0.1"}, {IP: "2000::"}}},
		{name: "consistent dual-stack preserved", podIP: "10.0.0.1", podIPs: []v1.PodIP{{IP: "10.0.0.1"}, {IP: "2000::"}}, expectIP: "10.0.0.1", expectIPs: []v1.PodIP{{IP: "10.0.0.1"}, {IP: "2000::"}}},
		{name: "neither set", expectIP: "", expectIPs: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{Status: v1.PodStatus{PodIP: tc.podIP, PodIPs: tc.podIPs}}
			obj2 := roundTrip(t, runtime.Object(pod))
			pod2 := obj2.(*v1.Pod)
			if pod2.Status.PodIP != tc.expectIP {
				t.Errorf("expected podIP %q, got %q", tc.expectIP, pod2.Status.PodIP)
			}
			if !reflect.DeepEqual(pod2.Status.PodIPs, tc.expectIPs) {
				t.Errorf("expected podIPs %#v, got %#v", tc.expectIPs, pod2.Status.PodIPs)
			}
		})
	}
}

// TestSetDefaultPodTerminationGracePeriodSeconds 验证终止宽限期：负值被钳制为 1，零值与正值原样保留。
func TestSetDefaultPodTerminationGracePeriodSeconds(t *testing.T) {
	tests := []struct {
		// 覆盖负值钳制、零值保留与正值保留三种输入。
		name     string
		grace    *int64
		expected *int64
	}{
		{name: "negative clamped to 1", grace: ptr.To[int64](-1), expected: ptr.To[int64](1)},
		{name: "zero preserved", grace: ptr.To[int64](0), expected: ptr.To[int64](0)},
		{name: "positive preserved", grace: ptr.To[int64](42), expected: ptr.To[int64](42)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{Spec: v1.PodSpec{TerminationGracePeriodSeconds: tc.grace}}
			obj2 := roundTrip(t, runtime.Object(pod))
			pod2 := obj2.(*v1.Pod)
			if !reflect.DeepEqual(pod2.Spec.TerminationGracePeriodSeconds, tc.expected) {
				t.Errorf("expected %v, got %v", *tc.expected, *pod2.Spec.TerminationGracePeriodSeconds)
			}
		})
	}
}
