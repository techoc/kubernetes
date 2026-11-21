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

// 使用外部测试包（v1beta1_test）：只通过导出的 API 与 scheme 行为测试默认值，
// 避免测试代码依赖包内未导出的实现细节。
package v1beta1_test

// 测试的关键依赖是 install 包的副作用注册：只有把类型与默认值函数注册进
// legacyscheme，下面的 encode/decode 往返才会真正触发默认化逻辑。
import (
	"math"
	"reflect"
	"testing"

	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	// 空白导入 core/install：PodTemplateSpec 内的 PodSpec 字段也需要 core 的默认值注册。
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	// 空白导入 extensions/install：本文件要验证的 DaemonSet/Deployment/ReplicaSet/NetworkPolicy
	// 默认值函数正是随这个包注册进 scheme 的。
	_ "k8s.io/kubernetes/pkg/apis/extensions/install"
	. "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
	"k8s.io/utils/ptr"
)

// TestSetDefaultDaemonSetSpec 验证 DaemonSet 的默认值：从 Pod 模板推导 Selector 与对象标签、
// 默认更新策略为 OnDelete、修订历史上限为 10。
// 用表格驱动覆盖「用户已设置」与「用户未设置」两类输入，确认默认值不覆盖显式配置。
func TestSetDefaultDaemonSetSpec(t *testing.T) {
	// defaultLabels 既作为模板标签，也作为期望的 Selector/Labels 来源：
	// 默认值逻辑会在字段为空时从模板标签复制一份。
	defaultLabels := map[string]string{"foo": "bar"}
	// 将常量转为变量后取地址，供各模板共享同一个终止宽限期。
	period := int64(v1.DefaultTerminationGracePeriodSeconds)
	// 预先补齐 Pod 层级的默认字段（DNS、重启策略、安全上下文、优雅终止时间、调度器），
	// 因为这些由 core/v1 的默认值函数负责；本测试只关心 extensions/v1beta1 这一层补的字段。
	defaultTemplate := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			DNSPolicy:                     v1.DNSClusterFirst,
			RestartPolicy:                 v1.RestartPolicyAlways,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &period,
			SchedulerName:                 v1.DefaultSchedulerName,
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels: defaultLabels,
		},
	}
	// 无标签的模板：用于验证模板没有标签时不会凭空生成 Selector/Labels。
	templateNoLabel := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			DNSPolicy:                     v1.DNSClusterFirst,
			RestartPolicy:                 v1.RestartPolicyAlways,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &period,
			SchedulerName:                 v1.DefaultSchedulerName,
		},
	}
	// 每行给出一组输入对象与期望对象，期望里只写默认化后应当出现的变化。
	tests := []struct {
		original *extensionsv1beta1.DaemonSet
		expected *extensionsv1beta1.DaemonSet
	}{
		// 场景：模板带标签、Selector 与对象标签均为空，期望两者都从模板标签推导，
		// 同时更新策略与历史上限被补上默认值。
		{ // 标签变更/默认值测试：从模板标签推导 Selector 与对象标签。
			original: &extensionsv1beta1.DaemonSet{
				Spec: extensionsv1beta1.DaemonSetSpec{
					Template: defaultTemplate,
				},
			},
			expected: &extensionsv1beta1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: defaultLabels,
				},
				Spec: extensionsv1beta1.DaemonSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: defaultLabels,
					},
					Template: defaultTemplate,
					UpdateStrategy: extensionsv1beta1.DaemonSetUpdateStrategy{
						Type: extensionsv1beta1.OnDeleteDaemonSetStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		// 场景：用户已在对象上设置自己的标签，默认化不得覆盖它；
		// Selector 仍从模板标签推导，且显式设置的 RevisionHistoryLimit=1 被保留。
		{ // 标签变更/默认值测试：已有的对象标签不被覆盖。
			original: &extensionsv1beta1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: extensionsv1beta1.DaemonSetSpec{
					Template:             defaultTemplate,
					RevisionHistoryLimit: ptr.To[int32](1),
				},
			},
			expected: &extensionsv1beta1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: extensionsv1beta1.DaemonSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: defaultLabels,
					},
					Template: defaultTemplate,
					UpdateStrategy: extensionsv1beta1.DaemonSetUpdateStrategy{
						Type: extensionsv1beta1.OnDeleteDaemonSetStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](1),
				},
			},
		},
		// 场景：完全空的对象，验证无标签路径下的默认化结果。
		{ // 更新策略：未指定时默认使用 OnDelete。
			original: &extensionsv1beta1.DaemonSet{},
			expected: &extensionsv1beta1.DaemonSet{
				Spec: extensionsv1beta1.DaemonSetSpec{
					Template: templateNoLabel,
					UpdateStrategy: extensionsv1beta1.DaemonSetUpdateStrategy{
						Type: extensionsv1beta1.OnDeleteDaemonSetStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
		// 场景：Spec 为空但对象非空，与上一例一起覆盖标签缺失时的默认化分支。
		{ // 自定义唯一标签键：覆盖 Spec 为空但对象非空的情形。
			original: &extensionsv1beta1.DaemonSet{
				Spec: extensionsv1beta1.DaemonSetSpec{},
			},
			expected: &extensionsv1beta1.DaemonSet{
				Spec: extensionsv1beta1.DaemonSetSpec{
					Template: templateNoLabel,
					UpdateStrategy: extensionsv1beta1.DaemonSetUpdateStrategy{
						Type: extensionsv1beta1.OnDeleteDaemonSetStrategyType,
					},
					RevisionHistoryLimit: ptr.To[int32](10),
				},
			},
		},
	}

	// 逐用例执行往返并比较 Spec：默认值是通过 scheme 编码/解码注册的，
	// 直接调用 SetDefaults_* 会漏掉「解码时才应用默认值」这一真实路径。
	for i, test := range tests {
		original := test.original
		expected := test.expected
		// roundTrip 会触发 scheme 中注册的全部默认化逻辑。
		obj2 := roundTrip(t, runtime.Object(original))
		// 类型断言失败要立即终止：后续比较依赖具体类型，继续只会产生误导性错误。
		got, ok := obj2.(*extensionsv1beta1.DaemonSet)
		if !ok {
			t.Errorf("(%d) unexpected object: %v", i, got)
			t.FailNow()
		}
		// 用语义相等而非 reflect.DeepEqual：nil 与空切片/空 map 在这里被视为等价，
		// 避免把无意义的表示差异当成默认值错误。
		if !apiequality.Semantic.DeepEqual(got.Spec, expected.Spec) {
			t.Errorf("(%d) got different than expected\ngot:\n\t%+v\nexpected:\n\t%+v", i, got.Spec, expected.Spec)
		}
	}
}

// TestSetDefaultDeployment 验证 Deployment 的默认值：副本数为 1、策略为 RollingUpdate、
// MaxSurge/MaxUnavailable 为 1、ProgressDeadlineSeconds 与 RevisionHistoryLimit 为 MaxInt32。
// 后两者取 MaxInt32 是因为该版本用它表示「不限制」，语义等同于未设置。
func TestSetDefaultDeployment(t *testing.T) {
	// 1 是 MaxSurge/MaxUnavailable 的默认值，单独取地址以便被多个用例共享。
	defaultIntOrString := intstr.FromInt32(1)
	// 5 表示用户显式指定的值，用来验证默认化不会覆盖已有配置。
	differentIntOrString := intstr.FromInt32(5)
	period := int64(v1.DefaultTerminationGracePeriodSeconds)
	// 这里不带标签，因此只验证副本数、策略与超时/历史上限相关默认值。
	defaultTemplate := v1.PodTemplateSpec{
		Spec: v1.PodSpec{
			DNSPolicy:                     v1.DNSClusterFirst,
			RestartPolicy:                 v1.RestartPolicyAlways,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &period,
			SchedulerName:                 v1.DefaultSchedulerName,
		},
	}
	// 表格中的 original 是输入，expected 是默认化后期望得到的完整 Spec。
	tests := []struct {
		original *extensionsv1beta1.Deployment
		expected *extensionsv1beta1.Deployment
	}{
		// 用例 1：完全空的 Deployment —— 所有字段都应被补上默认值。
		{
			original: &extensionsv1beta1.Deployment{},
			expected: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type: extensionsv1beta1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &extensionsv1beta1.RollingUpdateDeployment{
							MaxSurge:       &defaultIntOrString,
							MaxUnavailable: &defaultIntOrString,
						},
					},
					Template:                defaultTemplate,
					ProgressDeadlineSeconds: ptr.To[int32](math.MaxInt32),
					RevisionHistoryLimit:    ptr.To[int32](math.MaxInt32),
				},
			},
		},
		// 用例 2：用户显式设置副本数、MaxSurge，只应补上缺失的 MaxUnavailable，
		// 并保持已有的自定义值。
		{
			original: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						RollingUpdate: &extensionsv1beta1.RollingUpdateDeployment{
							MaxSurge: &differentIntOrString,
						},
					},
				},
			},
			expected: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type: extensionsv1beta1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &extensionsv1beta1.RollingUpdateDeployment{
							MaxSurge:       &differentIntOrString,
							MaxUnavailable: &defaultIntOrString,
						},
					},
					Template:                defaultTemplate,
					ProgressDeadlineSeconds: ptr.To[int32](math.MaxInt32),
					RevisionHistoryLimit:    ptr.To[int32](math.MaxInt32),
				},
			},
		},
		// 用例 3：策略类型为 RollingUpdate 但 RollingUpdate 结构为 nil，
		// 默认化需要创建该结构并填充两个默认参数。
		{
			original: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](3),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type:          extensionsv1beta1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: nil,
					},
				},
			},
			expected: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](3),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type: extensionsv1beta1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &extensionsv1beta1.RollingUpdateDeployment{
							MaxSurge:       &defaultIntOrString,
							MaxUnavailable: &defaultIntOrString,
						},
					},
					Template:                defaultTemplate,
					ProgressDeadlineSeconds: ptr.To[int32](math.MaxInt32),
					RevisionHistoryLimit:    ptr.To[int32](math.MaxInt32),
				},
			},
		},
		// 用例 4：Recreate 策略不需要 RollingUpdate 参数，默认化不应额外创建它。
		{
			original: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type: extensionsv1beta1.RecreateDeploymentStrategyType,
					},
				},
			},
			expected: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type: extensionsv1beta1.RecreateDeploymentStrategyType,
					},
					Template:                defaultTemplate,
					ProgressDeadlineSeconds: ptr.To[int32](math.MaxInt32),
					RevisionHistoryLimit:    ptr.To[int32](math.MaxInt32),
				},
			},
		},
		// 用例 5：Recreate 且用户显式设置 ProgressDeadlineSeconds=30，
		// 该值应被保留，只有其他未设置字段才补默认值。
		{
			original: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type: extensionsv1beta1.RecreateDeploymentStrategyType,
					},
					ProgressDeadlineSeconds: ptr.To[int32](30),
				},
			},
			expected: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Replicas: ptr.To[int32](5),
					Strategy: extensionsv1beta1.DeploymentStrategy{
						Type: extensionsv1beta1.RecreateDeploymentStrategyType,
					},
					Template:                defaultTemplate,
					ProgressDeadlineSeconds: ptr.To[int32](30),
					RevisionHistoryLimit:    ptr.To[int32](math.MaxInt32),
				},
			},
		},
	}

	// 与 DaemonSet 测试相同，走完整的编码/解码往返以激活 scheme 中的默认值注册。
	for _, test := range tests {
		original := test.original
		expected := test.expected
		obj2 := roundTrip(t, runtime.Object(original))
		got, ok := obj2.(*extensionsv1beta1.Deployment)
		if !ok {
			t.Errorf("unexpected object: %v", got)
			t.FailNow()
		}
		// 只比较 Spec：元数据里的 ResourceVersion 等字段由编解码过程产生，与本测试无关。
		if !apiequality.Semantic.DeepEqual(got.Spec, expected.Spec) {
			t.Errorf("object mismatch!\nexpected:\n\t%+v\ngot:\n\t%+v", got.Spec, expected.Spec)
		}
	}
}

// TestSetDefaultReplicaSet 验证标签推导规则：当模板带标签时，
// Selector 与对象 Labels 在缺失时才从模板标签复制，用户显式设置的值必须保留。
func TestSetDefaultReplicaSet(t *testing.T) {
	// expectLabels/expectSelector 表示期望「对象标签」和「Selector 标签」是否与
	// 模板标签相等，用来区分「被默认化补齐」和「保持用户原值」两种情况。
	tests := []struct {
		rs             *extensionsv1beta1.ReplicaSet
		expectLabels   bool
		expectSelector bool
	}{
		// 场景 1：Selector 与对象标签均为空，期望两者都被推导为模板标签。
		{
			rs: &extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Template: v1.PodTemplateSpec{
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
		// 场景 2：对象标签已设置为 bar=foo，期望它保持不变；Selector 仍从模板推导。
		{
			rs: &extensionsv1beta1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Template: v1.PodTemplateSpec{
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
		// 场景 3：Selector 与对象标签都已显式设置，期望两者都不被模板标签覆盖。
		{
			rs: &extensionsv1beta1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bar": "foo",
					},
				},
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"some": "other",
						},
					},
					Template: v1.PodTemplateSpec{
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
		// 场景 4：对象标签为空但 Selector 已显式设置，期望只补齐对象标签。
		{
			rs: &extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"some": "other",
						},
					},
					Template: v1.PodTemplateSpec{
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

	// 逐用例往返后再检查两组相等关系，覆盖「未设置→推导」和「已设置→保留」的组合。
	for _, test := range tests {
		rs := test.rs
		obj2 := roundTrip(t, runtime.Object(rs))
		rs2, ok := obj2.(*extensionsv1beta1.ReplicaSet)
		if !ok {
			t.Errorf("unexpected object: %v", rs2)
			t.FailNow()
		}
		// Selector 相等性：expectSelector 为 true 表示期望与模板标签一致。
		if test.expectSelector != reflect.DeepEqual(rs2.Spec.Selector.MatchLabels, rs2.Spec.Template.Labels) {
			if test.expectSelector {
				t.Errorf("expected: %v, got: %v", rs2.Spec.Template.Labels, rs2.Spec.Selector)
			} else {
				t.Errorf("unexpected equality: %v", rs.Spec.Selector)
			}
		}
		// 对象标签相等性：expectLabels 为 true 表示期望与模板标签一致。
		if test.expectLabels != reflect.DeepEqual(rs2.Labels, rs2.Spec.Template.Labels) {
			if test.expectLabels {
				t.Errorf("expected: %v, got: %v", rs2.Spec.Template.Labels, rs2.Labels)
			} else {
				t.Errorf("unexpected equality: %v", rs.Labels)
			}
		}
	}
}

// TestSetDefaultReplicaSetReplicas 验证副本数默认值：未设置时补 1，
// 而显式设置的 0 是有意义的值（缩容到零），必须原样保留。
func TestSetDefaultReplicaSetReplicas(t *testing.T) {
	// 用指针区分「未设置(nil)」与「显式设为 0」，这正是该默认值逻辑的关键。
	tests := []struct {
		rs             extensionsv1beta1.ReplicaSet
		expectReplicas int32
	}{
		// 场景 1：Replicas 为 nil，期望默认化为 1。
		{
			rs: extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Template: v1.PodTemplateSpec{
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
		// 场景 2：显式设为 0，期望保持 0，不能被误判为未设置。
		{
			rs: extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Replicas: ptr.To[int32](0),
					Template: v1.PodTemplateSpec{
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
		// 场景 3：显式设为 3，期望保持自定义值。
		{
			rs: extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Replicas: ptr.To[int32](3),
					Template: v1.PodTemplateSpec{
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

	// 先确认字段非空（默认化必须生效），再核对具体数值。
	for _, test := range tests {
		rs := &test.rs
		obj2 := roundTrip(t, runtime.Object(rs))
		rs2, ok := obj2.(*extensionsv1beta1.ReplicaSet)
		if !ok {
			t.Errorf("unexpected object: %v", rs2)
			t.FailNow()
		}
		// nil 说明默认值没有注册或没被应用，属于最严重的失败。
		if rs2.Spec.Replicas == nil {
			t.Errorf("unexpected nil Replicas")
		} else if test.expectReplicas != *rs2.Spec.Replicas {
			t.Errorf("expected: %d replicas, got: %d", test.expectReplicas, *rs2.Spec.Replicas)
		}
	}
}

// TestDefaultRequestIsNotSetForReplicaSet 防止默认化把 CPU limits 自动变成 requests：
// 该行为应由其他准入环节决定，自动补齐会改变工作负载的 QoS 类别与调度结果。
func TestDefaultRequestIsNotSetForReplicaSet(t *testing.T) {
	// 构造一个只设置 CPU limits、不设置 requests 的 PodSpec。
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
	rs := &extensionsv1beta1.ReplicaSet{
		Spec: extensionsv1beta1.ReplicaSetSpec{
			Replicas: ptr.To[int32](3),
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: s,
			},
		},
	}
	// 通过往返触发默认化，再直接读取容器资源字段验证副作用没有发生。
	output := roundTrip(t, runtime.Object(rs))
	rs2 := output.(*extensionsv1beta1.ReplicaSet)
	// 取出 requests 中 CPU 的值：不存在时返回零值，这正是本测试期望的结果。
	defaultRequest := rs2.Spec.Template.Spec.Containers[0].Resources.Requests
	requestValue := defaultRequest[v1.ResourceCPU]
	// 断言 request 为零，即默认化没有从 limits 推断出 requests。
	if requestValue.String() != "0" {
		t.Errorf("Expected 0 request value, got: %s", requestValue.String())
	}
}

// TestSetDefaultNetworkPolicy 验证 PolicyTypes 推断：未显式指定时至少包含 Ingress，
// 存在 Egress 规则时还要加上 Egress；显式指定的值则原样保留。
// 这是旧版 API 没有 PolicyTypes 字段时的兼容行为。
func TestSetDefaultNetworkPolicy(t *testing.T) {
	tests := []struct {
		original *extensionsv1beta1.NetworkPolicy
		expected *extensionsv1beta1.NetworkPolicy
	}{
		// 场景 1：空的 NetworkPolicy（无 Ingress/Egress 规则）默认只有 Ingress。
		{ // 空的 NetworkPolicy 应被默认设置为 PolicyTypes=Ingress。
			original: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
				},
			},
			expected: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					PolicyTypes: []extensionsv1beta1.PolicyType{extensionsv1beta1.PolicyTypeIngress},
				},
			},
		},
		// 场景 2：显式给出空的 Ingress 列表，语义仍等同于未指定方向，默认 Ingress。
		{ // 空的 Ingress NetworkPolicy 应被默认设置为 PolicyTypes=Ingress。
			original: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Ingress: []extensionsv1beta1.NetworkPolicyIngressRule{},
				},
			},
			expected: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Ingress:     []extensionsv1beta1.NetworkPolicyIngressRule{},
					PolicyTypes: []extensionsv1beta1.PolicyType{extensionsv1beta1.PolicyTypeIngress},
				},
			},
		},
		// 场景 3：同时定义 Ingress 与 Egress 规则，未指定 PolicyTypes，期望推断出两者。
		{ // 同时定义了 Ingress 与 Egress 时，应推断为 Ingress、Egress。
			original: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Ingress: []extensionsv1beta1.NetworkPolicyIngressRule{
						{
							From: []extensionsv1beta1.NetworkPolicyPeer{
								{
									PodSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
								},
							},
						},
					},
					Egress: []extensionsv1beta1.NetworkPolicyEgressRule{
						{
							To: []extensionsv1beta1.NetworkPolicyPeer{
								{
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
								},
							},
						},
					},
				},
			},
			expected: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Ingress: []extensionsv1beta1.NetworkPolicyIngressRule{
						{
							From: []extensionsv1beta1.NetworkPolicyPeer{
								{
									PodSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
								},
							},
						},
					},
					Egress: []extensionsv1beta1.NetworkPolicyEgressRule{
						{
							To: []extensionsv1beta1.NetworkPolicyPeer{
								{
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
								},
							},
						},
					},
					PolicyTypes: []extensionsv1beta1.PolicyType{extensionsv1beta1.PolicyTypeIngress, extensionsv1beta1.PolicyTypeEgress},
				},
			},
		},
		// 场景 4：只有 Egress 规则且未指定 PolicyTypes，期望同时推断出 Ingress 和 Egress。
		{ // 只有 Egress 且未设置 PolicyTypes 时，应设为 Ingress 与 Egress。
			original: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Egress: []extensionsv1beta1.NetworkPolicyEgressRule{
						{
							To: []extensionsv1beta1.NetworkPolicyPeer{
								{
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
								},
							},
						},
					},
				},
			},
			expected: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Egress: []extensionsv1beta1.NetworkPolicyEgressRule{
						{
							To: []extensionsv1beta1.NetworkPolicyPeer{
								{
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"c": "d"},
									},
								},
							},
						},
					},
					PolicyTypes: []extensionsv1beta1.PolicyType{extensionsv1beta1.PolicyTypeIngress, extensionsv1beta1.PolicyTypeEgress},
				},
			},
		},
		// 场景 5：只有 Egress 规则但用户显式指定 PolicyTypes=[Egress]，期望不被改写。
		{ // 只有 Egress 且 PolicyTypes 已设为 Egress 时，应只保留 Egress。
			original: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Egress: []extensionsv1beta1.NetworkPolicyEgressRule{
						{
							To: []extensionsv1beta1.NetworkPolicyPeer{
								{
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"Egress": "only"},
									},
								},
							},
						},
					},
					PolicyTypes: []extensionsv1beta1.PolicyType{extensionsv1beta1.PolicyTypeEgress},
				},
			},
			expected: &extensionsv1beta1.NetworkPolicy{
				Spec: extensionsv1beta1.NetworkPolicySpec{
					PodSelector: metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
					Egress: []extensionsv1beta1.NetworkPolicyEgressRule{
						{
							To: []extensionsv1beta1.NetworkPolicyPeer{
								{
									NamespaceSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"Egress": "only"},
									},
								},
							},
						},
					},
					PolicyTypes: []extensionsv1beta1.PolicyType{extensionsv1beta1.PolicyTypeEgress},
				},
			},
		},
	}

	// 逐用例往返后用语义相等比较 Spec，重点检查 PolicyTypes 推断结果。
	for i, test := range tests {
		original := test.original
		expected := test.expected
		obj2 := roundTrip(t, runtime.Object(original))
		got, ok := obj2.(*extensionsv1beta1.NetworkPolicy)
		if !ok {
			t.Errorf("(%d) unexpected object: %v", i, got)
			t.FailNow()
		}
		if !apiequality.Semantic.DeepEqual(got.Spec, expected.Spec) {
			t.Errorf("(%d) got different than expected\ngot:\n\t%+v\nexpected:\n\t%+v", i, got.Spec, expected.Spec)
		}
	}
}

// roundTrip 模拟 APIServer 的真实处理过程：外部版本编码 → 内部版本解码（此时触发默认化）
// → 再转回外部版本。测试默认值必须经过这条路径，直接调用 SetDefaults_* 无法覆盖
// 「默认值由 scheme 注册并在解码时应用」这一关键行为。
func roundTrip(t *testing.T, obj runtime.Object) runtime.Object {
	// 用 LegacyCodec 以本版本 GroupVersion 编码；输出是带 apiVersion/kind 的序列化数据。
	data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(SchemeGroupVersion), obj)
	if err != nil {
		t.Errorf("%v\n %#v", err, obj)
		return nil
	}
	// UniversalDecoder 根据数据中的 apiVersion 选择编解码器，解码到内部版本时应用默认值。
	obj2, err := runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), data)
	if err != nil {
		t.Errorf("%v\nData: %s\nSource: %#v", err, string(data), obj)
		return nil
	}
	// 用反射创建与输入相同具体类型的新对象，避免把内部版本对象直接返回给调用方。
	obj3 := reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
	// 转回外部版本对象，使调用方拿到的类型与传入时一致。
	err = legacyscheme.Scheme.Convert(obj2, obj3, nil)
	if err != nil {
		t.Errorf("%v\nSource: %#v", err, obj2)
		return nil
	}
	return obj3
}
