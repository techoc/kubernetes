/*
Copyright 2024 The Kubernetes Authors.

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

package podtopologylabels

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	admissiontesting "k8s.io/apiserver/pkg/admission/testing"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	kubefeatures "k8s.io/kubernetes/pkg/features"
)

// TestPodTopology 验证拓扑标签准入插件在 Pod 与 Binding 两种路径下的行为。
func TestPodTopology(t *testing.T) {
	tests := []struct {
		name                  string               // 测试用例名称。
		bindingTarget         *api.ObjectReference // 绑定目标；为 nil 时使用带指定标签的默认节点。
		targetNodeLabels      map[string]string    // 目标节点上的标签。
		existingBindingLabels map[string]string    // 准入前 Binding 上已有的标签（由客户端/调度器设置）。
		expectedBindingLabels map[string]string    // 准入后期望 Binding 上存在的标签。
		featureDisabled       bool                 // 是否禁用 SetPodTopologyLabels 特性门控。
	}{
		{
			name: "copies topology.kubernetes.io/zone and region labels to binding labels",
			targetNodeLabels: map[string]string{
				"topology.kubernetes.io/zone":      "zone1",
				"topology.kubernetes.io/region":    "region1",
				"topology.kubernetes.io/arbitrary": "something",
				"non-topology.kubernetes.io/label": "something", // 非 topology.kubernetes.io 前缀的标签不应被复制。
			},
			expectedBindingLabels: map[string]string{
				"topology.kubernetes.io/zone":   "zone1",
				"topology.kubernetes.io/region": "region1",
			},
		},
		{
			name: "does not copy arbitrary topology labels",
			targetNodeLabels: map[string]string{
				"topology.kubernetes.io/zone":      "zone1",
				"topology.kubernetes.io/arbitrary": "something",
			},
			expectedBindingLabels: map[string]string{
				"topology.kubernetes.io/zone": "zone1",
			},
		},
		{
			name: "does not copy topology labels that use a subdomain",
			targetNodeLabels: map[string]string{
				"topology.kubernetes.io/region":   "region1",
				"sub.topology.kubernetes.io/zone": "value",
			},
			expectedBindingLabels: map[string]string{
				"topology.kubernetes.io/region": "region1",
			},
		},
		{
			name: "does not copy label keys that don't contain a / character",
			targetNodeLabels: map[string]string{
				"topology.kubernetes.io": "value",
			},
			existingBindingLabels: map[string]string{},
		},
		{
			name: "overwrites existing topology labels",
			existingBindingLabels: map[string]string{
				"topology.kubernetes.io/zone": "oldValue",
			},
			targetNodeLabels: map[string]string{
				"topology.kubernetes.io/zone": "newValue",
			},
			expectedBindingLabels: map[string]string{
				"topology.kubernetes.io/zone": "newValue",
			},
		},
		{
			name: "does nothing if the SetPodTopologyLabels feature gate is disabled",
			targetNodeLabels: map[string]string{
				"topology.kubernetes.io/zone":   "zone1",
				"topology.kubernetes.io/region": "region1",
			},
			expectedBindingLabels: map[string]string{},
			featureDisabled:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			namespace := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: "test-ns"},
			}
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "valid-test-node",
					Labels: test.targetNodeLabels,
				},
			}

			featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, kubefeatures.PodTopologyLabelsAdmission, !test.featureDisabled)

			t.Run("using Pod directly (update)", func(t *testing.T) {
				// 准备客户端与 informer。
				mockClient := fake.NewSimpleClientset(namespace, node)
				handler, informerFactory, err := newHandlerForTest(mockClient)
				if err != nil {
					t.Fatalf("unexpected error initializing handler: %v", err)
				}
				stopCh := make(chan struct{})
				defer close(stopCh)
				informerFactory.Start(stopCh)

				oldPod := &api.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "testPod", Namespace: namespace.Name, Labels: test.existingBindingLabels},
					Spec:       api.PodSpec{},
				}
				pod := oldPod.DeepCopy()
				pod.Spec.NodeName = node.Name

				if err := admissiontesting.WithReinvocationTesting(t, handler).
					Admit(context.TODO(), admission.NewAttributesRecord(pod, oldPod,
						api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name,
						api.Resource("pods").WithVersion("version"), "", admission.Update, &metav1.UpdateOptions{},
						false, nil), nil); err != nil {
					t.Errorf("failed running admission plugin: %v", err)
				}
			})

			t.Run("using Pod directly (create)", func(t *testing.T) {
				// 准备客户端与 informer。
				mockClient := fake.NewSimpleClientset(namespace, node)
				handler, informerFactory, err := newHandlerForTest(mockClient)
				if err != nil {
					t.Fatalf("unexpected error initializing handler: %v", err)
				}
				stopCh := make(chan struct{})
				defer close(stopCh)
				informerFactory.Start(stopCh)

				pod := &api.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "testPod", Namespace: namespace.Name, Labels: test.existingBindingLabels},
					Spec: api.PodSpec{
						NodeName: node.Name,
					},
				}
				if err := admissiontesting.WithReinvocationTesting(t, handler).
					Admit(context.TODO(), admission.NewAttributesRecord(pod, nil,
						api.Kind("Pod").WithVersion("version"), pod.Namespace, pod.Name,
						api.Resource("pods").WithVersion("version"), "", admission.Create, &metav1.UpdateOptions{},
						false, nil), nil); err != nil {
					t.Errorf("failed running admission plugin: %v", err)
				}
			})

			t.Run("using Binding subresource", func(t *testing.T) {
				// 测试中被绑定的 Pod。
				existingPod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "testPod", Namespace: namespace.Name},
					Spec:       corev1.PodSpec{},
				}
				mockClient := fake.NewSimpleClientset(namespace, node, existingPod)
				handler, informerFactory, err := newHandlerForTest(mockClient)
				if err != nil {
					t.Fatalf("unexpected error initializing handler: %v", err)
				}
				stopCh := make(chan struct{})
				defer close(stopCh)
				informerFactory.Start(stopCh)

				// 构造并提交 Binding 对象。
				target := test.bindingTarget
				if target == nil {
					target = &api.ObjectReference{
						Kind: "Node",
						Name: node.Name,
					}
				}
				binding := &api.Binding{
					ObjectMeta: metav1.ObjectMeta{
						Name:      existingPod.Name,
						Namespace: existingPod.Namespace,
						Labels:    test.existingBindingLabels,
					},
					Target: *target,
				}
				if err := admissiontesting.WithReinvocationTesting(t, handler).
					Admit(context.TODO(), admission.NewAttributesRecord(binding, nil, api.Kind("Binding").WithVersion("version"), existingPod.Namespace, existingPod.Name, api.Resource("pods").WithVersion("version"), "binding", admission.Create, &metav1.CreateOptions{}, false, nil), nil); err != nil {
					t.Errorf("failed running admission plugin: %v", err)
				}
				updatedBindingLabels := binding.Labels
				if !apiequality.Semantic.DeepEqual(updatedBindingLabels, test.expectedBindingLabels) {
					t.Errorf("Unexpected label values: %v", cmp.Diff(updatedBindingLabels, test.expectedBindingLabels))
				}
			})
		})
	}
}

// newHandlerForTest 创建并完成测试用准入控制器的初始化。
func newHandlerForTest(c kubernetes.Interface) (*Plugin, informers.SharedInformerFactory, error) {
	factory := informers.NewSharedInformerFactory(c, 5*time.Minute)
	handler := NewPodTopologyPlugin(defaultConfig) // todo: write additional test cases with non-default config.
	pluginInitializer := genericadmissioninitializer.New(c, nil, factory, nil, feature.DefaultFeatureGate, nil, nil, nil)
	pluginInitializer.Initialize(handler)
	return handler, factory, admission.ValidateInitialization(handler)
}
