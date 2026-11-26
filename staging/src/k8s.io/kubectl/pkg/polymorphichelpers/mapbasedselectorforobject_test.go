/*
Copyright 2018 The Kubernetes Authors.

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

package polymorphichelpers

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestMapBasedSelectorForObject(t *testing.T) {
	tests := []struct {
		object         runtime.Object
		expectSelector string
		expectErr      bool
	}{
		{
			object: &corev1.ReplicationController{
				Spec: corev1.ReplicationControllerSpec{
					Selector: map[string]string{
						"foo": "bar",
					},
				},
			},
			expectSelector: "foo=bar",
		},
		{
			object:    &corev1.Pod{},
			expectErr: true,
		},
		{
			object: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
			},
			expectSelector: "foo=bar",
		},
		{
			object: &corev1.Service{
				Spec: corev1.ServiceSpec{
					Selector: map[string]string{
						"foo": "bar",
					},
				},
			},
			expectSelector: "foo=bar",
		},
		{
			object:    &corev1.Service{},
			expectErr: true,
		},
		// extensions/v1beta1 Deployment：同时有标签和选择器。
		{
			object: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// extensions/v1beta1 Deployment：只有标签没有选择器，使用标签。
		{
			object: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// extensions/v1beta1 Deployment：选择器非法。
		{
			object: &extensionsv1beta1.Deployment{
				Spec: extensionsv1beta1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key: "foo",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		// apps/v1 Deployment：同时有标签和选择器。
		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// apps/v1 Deployment：只有标签没有选择器，应报错。
		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		// apps/v1 Deployment：既无标签也无选择器，应报错。
		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{},
			},
			expectErr: true,
		},
		// apps/v1 Deployment：标签为空，应报错。
		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{}, // Empty labels map
						},
					},
				},
			},
			expectErr: true,
		},
		// apps/v1beta2 Deployment：同时有标签和选择器。
		{
			object: &appsv1beta2.Deployment{
				Spec: appsv1beta2.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// apps/v1beta2 Deployment：只有标签没有选择器，应报错。
		{
			object: &appsv1beta2.Deployment{
				Spec: appsv1beta2.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		// apps/v1beta2 Deployment：既无标签也无选择器，应报错。
		{
			object: &appsv1beta2.Deployment{
				Spec: appsv1beta2.DeploymentSpec{},
			},
			expectErr: true,
		},
		// apps/v1beta1 Deployment：同时有标签和选择器。
		{
			object: &appsv1beta1.Deployment{
				Spec: appsv1beta1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// apps/v1beta1 Deployment：只有标签没有选择器，应报错。
		{
			object: &appsv1beta1.Deployment{
				Spec: appsv1beta1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		// apps/v1beta1 Deployment：既无标签也无选择器，应报错。
		{
			object: &appsv1beta1.Deployment{
				Spec: appsv1beta1.DeploymentSpec{},
			},
			expectErr: true,
		},
		// extensions/v1beta1 ReplicaSet：同时有标签和选择器。
		{
			object: &extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// extensions/v1beta1 ReplicaSet：只有标签没有选择器，使用标签。
		{
			object: &extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// extensions/v1beta1 ReplicaSet：标签选择器非法，应报错。
		{
			object: &extensionsv1beta1.ReplicaSet{
				Spec: extensionsv1beta1.ReplicaSetSpec{
					Selector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key: "foo",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		// apps/v1 ReplicaSet：同时有标签和选择器。
		{
			object: &appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// apps/v1 ReplicaSet：只有标签没有选择器，应报错。
		{
			object: &appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		// apps/v1beta2 ReplicaSet：同时有标签和选择器。
		{
			object: &appsv1beta2.ReplicaSet{
				Spec: appsv1beta2.ReplicaSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		// apps/v1beta2 ReplicaSet：只有标签没有选择器，应报错。
		{
			object: &appsv1beta2.ReplicaSet{
				Spec: appsv1beta2.ReplicaSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		// Node 不支持 expose，应报错。
		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key: "foo",
							},
						},
					},
				},
			},
			expectErr: true,
		},
		{
			object: &appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"foo": "bar",
						},
					},
				},
			},
			expectSelector: "foo=bar",
		},
		{
			object: &appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Selector: &metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{
							{
								Key: "foo",
							},
						},
					},
				},
			},
			expectErr: true,
		},

		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: nil,
				},
			},
			expectErr: true,
		},
		{
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: nil,
				},
			},
			expectErr: true,
		},
		{
			object: &appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Selector: nil,
				},
			},
			expectErr: true,
		},
		{
			object: &appsv1.ReplicaSet{
				Spec: appsv1.ReplicaSetSpec{
					Selector: nil,
				},
			},
			expectErr: true,
		},

		{
			object:    &corev1.Node{},
			expectErr: true,
		},
	}

	for _, test := range tests {
		actual, err := mapBasedSelectorForObject(test.object)
		if test.expectErr && err == nil {
			t.Error("unexpected non-error")
		}
		if !test.expectErr && err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if actual != test.expectSelector {
			t.Errorf("expected selector %q, but got %q", test.expectSelector, actual)
		}
	}
}
