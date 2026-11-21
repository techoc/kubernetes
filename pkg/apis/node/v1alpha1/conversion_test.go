/*
Copyright 2019 The Kubernetes Authors.

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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1alpha1 "k8s.io/api/node/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core "k8s.io/kubernetes/pkg/apis/core"
	node "k8s.io/kubernetes/pkg/apis/node"
)

// TestRuntimeClassConversion 验证 RuntimeClass 在 v1alpha1 外部类型与内部类型之间
// 双向转换后字段保持一致，重点覆盖 spec 展开（unnest）带来的字段映射。
func TestRuntimeClassConversion(t *testing.T) {
	// 测试用例共享的常量：name 是对象名，handler 是运行时 handler，
	// cpuOverhead 是固定 CPU 开销的字符串表示。
	const (
		name        = "puppy"
		handler     = "heidi"
		cpuOverhead = "100"
	)
	// 使用 map 组成表驱动测试：每个场景同时给出期望的内部对象与外部对象，
	// 双向转换都要得到对应的期望值。
	tests := map[string]struct {
		internal *node.RuntimeClass
		external *v1alpha1.RuntimeClass
	}{
		// 所有字段都填写的场景：验证 handler、overhead、scheduling 及其子字段
		// 在双向转换中都不丢失。
		"fully-specified": {
			internal: &node.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Handler:    handler,
				Overhead: &node.Overhead{
					PodFixed: core.ResourceList{
						core.ResourceCPU: resource.MustParse(cpuOverhead),
					},
				},
				Scheduling: &node.Scheduling{
					NodeSelector: map[string]string{"extra-soft": "true"},
					Tolerations: []core.Toleration{{
						Key:      "stinky",
						Operator: core.TolerationOpExists,
						Effect:   core.TaintEffectNoSchedule,
					}},
				},
			},
			external: &v1alpha1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: v1alpha1.RuntimeClassSpec{
					RuntimeHandler: handler,
					Overhead: &v1alpha1.Overhead{
						PodFixed: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse(cpuOverhead),
						},
					},
					Scheduling: &v1alpha1.Scheduling{
						NodeSelector: map[string]string{"extra-soft": "true"},
						Tolerations: []corev1.Toleration{{
							Key:      "stinky",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						}},
					},
				},
			},
		},
		// overhead 非空但内容为空：验证空的 Overhead 对象不会被当成 nil 丢弃，
		// 否则往返转换后字段的有无会发生变化。
		"empty-overhead": {
			internal: &node.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Handler:    handler,
				Overhead:   &node.Overhead{},
			},
			external: &v1alpha1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: v1alpha1.RuntimeClassSpec{
					RuntimeHandler: handler,
					Overhead:       &v1alpha1.Overhead{},
				},
			},
		},
		// scheduling 非空但内容为空：与空 overhead 同理，
		// 确认空调度对象也会被原样保留。
		"empty-scheduling": {
			internal: &node.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Handler:    handler,
				Scheduling: &node.Scheduling{},
			},
			external: &v1alpha1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: v1alpha1.RuntimeClassSpec{
					RuntimeHandler: handler,
					Scheduling:     &v1alpha1.Scheduling{},
				},
			},
		},
		// overhead 与 scheduling 都为 nil 的最小对象：
		// 验证转换不会凭空创建空的 Overhead/Scheduling 对象。
		"empty": {
			internal: &node.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Handler:    handler,
			},
			external: &v1alpha1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: v1alpha1.RuntimeClassSpec{
					RuntimeHandler: handler,
				},
			},
		},
	}

	// 每个场景都做双向转换，确保两个方向的转换函数互逆，
	// 这也是发现字段映射遗漏或空对象处理错误的关键。
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			convertedInternal := &node.RuntimeClass{}
			require.NoError(t,
				Convert_v1alpha1_RuntimeClass_To_node_RuntimeClass(test.external, convertedInternal, nil))
			assert.Equal(t, test.internal, convertedInternal, "external -> internal")

			convertedV1alpha1 := &v1alpha1.RuntimeClass{}
			require.NoError(t,
				Convert_node_RuntimeClass_To_v1alpha1_RuntimeClass(test.internal, convertedV1alpha1, nil))
			assert.Equal(t, test.external, convertedV1alpha1, "internal -> external")
		})
	}
}
