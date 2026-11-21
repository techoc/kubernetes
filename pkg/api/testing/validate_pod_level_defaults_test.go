/*
Copyright 2025 The Kubernetes Authors.

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

package testing

import (
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	apisapps1 "k8s.io/kubernetes/pkg/apis/apps/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
)

// TestPodLevelResourcesDefaults 验证开启 PodLevelResources 特性后，hugepages 在不同层级（Pod/容器）
// 的请求与限制组合下，默认值填充与校验结果是否符合预期。
// 每个用例走三条路径：直接 Pod、Deployment 模板、以及从 Deployment 模板还原的 Pod，
// 确保默认值逻辑在嵌套场景下也一致。
func TestPodLevelResourcesDefaults(t *testing.T) {
	resCPU := v1.ResourceName(v1.ResourceCPU)
	valCPU1 := resource.MustParse("1")
	resMemory := v1.ResourceName(v1.ResourceMemory)
	valMem100Mi := resource.MustParse("100Mi")
	resHugepage2Mi := v1.ResourceName("hugepages-2Mi")
	valhugepage2Mi := resource.MustParse("2Mi")
	valhugepage10Mi := resource.MustParse("10Mi")
	resHugepage1Gi := v1.ResourceName("hugepages-1Gi")
	valHugepage1Gi := resource.MustParse("1Gi")

	// R 表示 request、L 表示 limit；用例覆盖 Pod 与容器之间 hugepages 的各种组合。
	testCases := []struct {
		name               string
		podResources       *v1.ResourceRequirements
		containerResources []v1.ResourceRequirements
		expectErr          bool
	}{
		{
			name: "no pod-level resources, container hugepages-2Mi:R",
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:    valCPU1,
						resMemory: valMem100Mi,
					},
					Requests: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
				},
			},
			expectErr: true,
		},
		{
			name: "no pod-level resources, container hugepages-2Mi:L",
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
					Requests: v1.ResourceList{
						resCPU:    valCPU1,
						resMemory: valMem100Mi,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "no pod-level resources, container hugepages-2Mi:R/L",
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
					Requests: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "no pod-level resources, container hugepages-2Mi:R/L hugepages-1Gi:R/L",
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
						resHugepage1Gi: valHugepage1Gi,
					},
					Requests: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
						resHugepage1Gi: valHugepage1Gi,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "pod-level resources hugepages-2Mi:R, container hugepages-2Mi:none",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					resCPU:    valCPU1,
					resMemory: valMem100Mi,
				},
				Requests: v1.ResourceList{
					resCPU:         valCPU1,
					resMemory:      valMem100Mi,
					resHugepage2Mi: valhugepage10Mi,
				},
			},
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:    valCPU1,
						resMemory: valMem100Mi,
					},
					Requests: v1.ResourceList{
						resCPU:    valCPU1,
						resMemory: valMem100Mi,
					},
				},
			},
			expectErr: true,
		},
		{
			name: "pod-level resources hugepages-2Mi:L, container hugepages-2Mi:L",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					resCPU:         valCPU1,
					resMemory:      valMem100Mi,
					resHugepage2Mi: valhugepage10Mi,
				},
				Requests: v1.ResourceList{
					resCPU:    valCPU1,
					resMemory: valMem100Mi,
				},
			},
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
					Requests: v1.ResourceList{
						resCPU:    valCPU1,
						resMemory: valMem100Mi,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "pod-level resources hugepages-2Mi:R/L, container hugepages-2Mi:R/L",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					resCPU:         valCPU1,
					resMemory:      valMem100Mi,
					resHugepage2Mi: valhugepage10Mi,
				},
				Requests: v1.ResourceList{
					resCPU:         valCPU1,
					resMemory:      valMem100Mi,
					resHugepage2Mi: valhugepage10Mi,
				},
			},
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
					Requests: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "pod-level resources hugepages-2Mi:R/L hugepages-1Gi:R/L, container hugepages-2Mi:R/L hugepages-1Gi:R/L",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					resCPU:         valCPU1,
					resMemory:      valMem100Mi,
					resHugepage2Mi: valhugepage10Mi,
					resHugepage1Gi: valHugepage1Gi,
				},
				Requests: v1.ResourceList{
					resCPU:         valCPU1,
					resMemory:      valMem100Mi,
					resHugepage2Mi: valhugepage10Mi,
					resHugepage1Gi: valHugepage1Gi,
				},
			},
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
						resHugepage1Gi: valHugepage1Gi,
					},
					Requests: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
						resHugepage1Gi: valHugepage1Gi,
					},
				},
			},
			expectErr: false,
		},
		{
			name: "pod-level resources hugepages-2Mi:R, container hugepages-2Mi:L",
			podResources: &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					resCPU:    valCPU1,
					resMemory: valMem100Mi,
				},
				Requests: v1.ResourceList{
					resCPU:         valCPU1,
					resMemory:      valMem100Mi,
					resHugepage2Mi: valhugepage10Mi,
				},
			},
			containerResources: []v1.ResourceRequirements{
				{
					Limits: v1.ResourceList{
						resCPU:         valCPU1,
						resMemory:      valMem100Mi,
						resHugepage2Mi: valhugepage2Mi,
					},
					Requests: v1.ResourceList{
						resCPU:    valCPU1,
						resMemory: valMem100Mi,
					},
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 该特性默认可能关闭，测试中显式开启并传入对应校验选项。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, true)
			opts := validation.PodValidationOptions{PodLevelResourcesEnabled: true}

			// Step 1: 独立的 Pod 经过默认值填充与校验。
			podForPodValidation1 := makePod(tc.podResources, tc.containerResources)

			// 先填默认值再转 internal：校验器只接受无版本的 core 类型。
			corev1.SetDefaults_Pod(podForPodValidation1)
			corev1.SetDefaults_PodSpec(&podForPodValidation1.Spec)

			internalPod := &core.Pod{}
			if err := legacyscheme.Scheme.Convert(podForPodValidation1, internalPod, nil); err != nil {
				t.Fatalf("Step 1: Failed to convert v1.Pod to core.Pod: %v", err)
			}

			// 将校验结果换算成布尔值，与用例预期比较。
			podErrs := validation.ValidatePodSpec(&internalPod.Spec, &internalPod.ObjectMeta, field.NewPath("spec"), opts)
			if len(podErrs) > 0 != tc.expectErr {
				t.Errorf("Step 1: Pod validation failed. expectErr=%v, got errs: %v", tc.expectErr, podErrs.ToAggregate())
			}

			// Step 2: 嵌入式场景：Deployment 的 Pod 模板经过同样的默认值与校验流程。
			podForPodValidation2 := makePod(tc.podResources, tc.containerResources)
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "test-deploy", Namespace: "test-ns"},
				Spec: appsv1.DeploymentSpec{
					Template: v1.PodTemplateSpec{
						Spec: podForPodValidation2.Spec,
					},
				},
			}

			// Deployment 与 Pod 有各自的默认值函数，两者都要执行，顺序与 API server 一致。
			apisapps1.SetDefaults_Deployment(deployment)
			corev1.SetDefaults_PodSpec(&deployment.Spec.Template.Spec)
			internalPodTemplateSpec := &core.PodTemplateSpec{}
			if err := legacyscheme.Scheme.Convert(&deployment.Spec.Template, internalPodTemplateSpec, nil); err != nil {
				t.Fatalf("Step 2: Failed to convert v1.PodTemplateSpec to core.PodTemplateSpec: %v", err)
			}

			// 模板有自己的校验入口，用它确认结果与普通 Pod 一致。
			podErrs = validation.ValidatePodTemplateSpec(internalPodTemplateSpec, field.NewPath("template"), opts)
			if len(podErrs) > 0 != tc.expectErr {
				t.Errorf("Step 2: Pod Template validation failed. expectErr=%v, got errs: %v", tc.expectErr, podErrs.ToAggregate())
			}

			// Step 3: 把 Deployment 中已填过默认值的 PodSpec 再当作独立 Pod 校验，
			// 防止默认值只在某条填充路径上生效。
			podForPodValidation3 := &v1.Pod{
				Spec: deployment.Spec.Template.Spec,
			}
			corev1.SetDefaults_Pod(podForPodValidation3)
			corev1.SetDefaults_PodSpec(&podForPodValidation3.Spec)
			internalPod = &core.Pod{}
			if err := legacyscheme.Scheme.Convert(podForPodValidation3, internalPod, nil); err != nil {
				t.Fatalf("Step 3: Failed to convert v1.Pod to core.Pod: %v", err)
			}

			podErrs = validation.ValidatePodSpec(&internalPod.Spec, &internalPod.ObjectMeta, field.NewPath("spec"), opts)
			if len(podErrs) > 0 != tc.expectErr {
				t.Errorf("Step 3: Pod validation failed. expectErr=%v, got errs: %v", tc.expectErr, podErrs.ToAggregate())
			}
		})
	}
}

// makePod 根据给定的 Pod 级与容器级资源构造测试用 Pod；podResources 为 nil 时不设置 Pod 级资源。
func makePod(podResources *v1.ResourceRequirements, containersResources []v1.ResourceRequirements) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
		Spec: v1.PodSpec{
			Containers: makeContainers(containersResources),
		},
	}

	if podResources != nil {
		pod.Spec.Resources = podResources
	}

	return pod
}

// makeContainers 为每个资源规格生成一个命名唯一的容器，便于在错误信息中定位。
func makeContainers(resourceRequirements []v1.ResourceRequirements) []v1.Container {
	containers := []v1.Container{}
	for idx, containerResources := range resourceRequirements {
		container := v1.Container{
			Name:  fmt.Sprintf("container-%d", idx),
			Image: "img", ImagePullPolicy: "Never", TerminationMessagePolicy: "File",
			Resources: containerResources,
		}
		containers = append(containers, container)
	}

	return containers
}
