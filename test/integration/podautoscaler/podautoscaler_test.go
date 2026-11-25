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

package podautoscaler

import (
	"strings"
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/utils/ptr"
)

// TestHPAScaleUpToMin 集成测试创建一个 Deployment 和 HPA，然后通过使
// Deployment 的副本数处于 HPA 的 min 和 max 范围内，验证 HPA 存在
// 且被调谐。由于指标没有被 mock，副本数只会增长到 min。
func TestHPAScaleUpToMin(t *testing.T) {
	ctx := t.Context()

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)
	clients, _ := createClients(t, server.ClientConfig)
	startHPAControllerAndWaitForCaches(t, clients)

	cs := clients.apiServer
	ns := createTestNamespace(t, cs)
	deployment := createDeployment(t, cs, ns.Name, 1)

	metricSpec := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name: "cpu",
			Target: autoscalingv2.MetricTarget{
				Type:               autoscalingv2.UtilizationMetricType,
				AverageUtilization: ptr.To[int32](50),
			},
		},
	}

	createHPA(t, cs, deployment, metricSpec, withHPAMinMaxReplicas(2, 10))

	err := waitForDeploymentCondition(ctx, cs, deployment, atLeastReplicas(2))
	if err != nil {
		t.Fatalf("HPA did not reconcile: %v", err)
	}
}

// TestHPAScaleToZero 验证 HPA 使用外部指标时可以先扩容到五个副本，再缩容到零，然后再扩回来。
func TestHPAScaleToZero(t *testing.T) {
	ctx := t.Context()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HPAScaleToZero, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)
	clients, metricValue := createClients(t, server.ClientConfig)
	startHPAControllerAndWaitForCaches(t, clients)

	cs := clients.apiServer
	ns := createTestNamespace(t, cs)
	deployment := createDeployment(t, cs, ns.Name, 1)

	// 使用外部指标创建 HPA。
	metricSpec := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ExternalMetricSourceType,
		External: &autoscalingv2.ExternalMetricSource{
			Metric: autoscalingv2.MetricIdentifier{
				Name: externalMetricName,
			},
			Target: autoscalingv2.MetricTarget{
				Type:         autoscalingv2.ValueMetricType,
				AverageValue: new(resource.MustParse("100")),
			},
		},
	}
	createHPA(t, cs, deployment, metricSpec, withHPAMinMaxReplicas(0, 10))

	// 阶段 1：metric=500，期望扩容到 5 个 Pod。
	metricValue.Store(resource.MustParse("500"))
	err := waitForDeploymentCondition(ctx, cs, deployment, atLeastReplicas(5))
	if err != nil {
		t.Fatalf("Phase 1: HPA did not scale up: %v", err)
	}

	// 验证 HPA 上的 ScaledToZero condition 被设置为 false。
	gotHPA, err := cs.AutoscalingV2().HorizontalPodAutoscalers(ns.Name).Get(ctx, hpaName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Phase 3: failed to get HPA: %v", err)
	}
	foundScaledToZero := false
	for _, c := range gotHPA.Status.Conditions {
		if c.Type == autoscalingv2.ScaledToZero {
			foundScaledToZero = true
			if c.Status != corev1.ConditionFalse {
				t.Fatalf("Phase 1: ScaledToZero condition status = %v, want False", c.Status)
			}
			break
		}
	}
	if !foundScaledToZero {
		t.Fatal("Phase 1: ScaledToZero condition not found on HPA status")
	}

	// 阶段 2：metric=0，期望缩容到 0 并设置 ScaledToZero condition。
	metricValue.Store(resource.MustParse("0"))
	err = waitForDeploymentCondition(ctx, cs, deployment, equalReplicas(0))
	if err != nil {
		t.Fatalf("Phase 2: HPA did not scale to zero: %v", err)
	}

	// 验证 HPA 上设置了 ScaledToZero condition。
	gotHPA, err = cs.AutoscalingV2().HorizontalPodAutoscalers(ns.Name).Get(ctx, hpaName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Phase 2: failed to get HPA: %v", err)
	}
	foundScaledToZero = false
	for _, c := range gotHPA.Status.Conditions {
		if c.Type == autoscalingv2.ScaledToZero {
			foundScaledToZero = true
			if c.Status != corev1.ConditionTrue {
				t.Fatalf("Phase 2: ScaledToZero condition status = %v, want True", c.Status)
			}
			break
		}
	}
	if !foundScaledToZero {
		t.Fatal("Phase 2: ScaledToZero condition not found on HPA status")
	}

	// 阶段 3：metric=400，期望再次扩容（副本数 >= 4）。
	metricValue.Store(resource.MustParse("400"))
	err = waitForDeploymentCondition(ctx, cs, deployment, atLeastReplicas(4))
	if err != nil {
		t.Fatalf("Phase 3: HPA did not scale back up: %v", err)
	}

	// 验证 HPA 上的 ScaledToZero condition 被设置为 false。
	gotHPA, err = cs.AutoscalingV2().HorizontalPodAutoscalers(ns.Name).Get(ctx, hpaName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Phase 3: failed to get HPA: %v", err)
	}
	foundScaledToZero = false
	for _, c := range gotHPA.Status.Conditions {
		if c.Type == autoscalingv2.ScaledToZero {
			foundScaledToZero = true
			if c.Status != corev1.ConditionFalse {
				t.Fatalf("Phase 3: ScaledToZero condition status = %v, want False", c.Status)
			}
			break
		}
	}
	if !foundScaledToZero {
		t.Fatal("Phase 3: ScaledToZero condition not found on HPA status")
	}
}

// TestHPAScaleToZeroRejected 验证缩容到零仅限于 object 和
// external 指标，并且受 HPAScaleToZero feature gate 控制。
// 以下两种情况中，API server 都必须拒绝 minReplicas: 0 的 HPA：
//   - 即使启用了 feature gate，resource（CPU）指标也永远
//     不适合缩容到零；
//   - 当 feature gate 禁用时，即使 external 指标本身符合
//     条件，也禁止 minReplicas: 0。
func TestHPAScaleToZeroRejected(t *testing.T) {
	cpuMetric := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricSource{
			Name: corev1.ResourceCPU,
			Target: autoscalingv2.MetricTarget{
				Type:               autoscalingv2.UtilizationMetricType,
				AverageUtilization: ptr.To[int32](50),
			},
		},
	}
	externalMetric := autoscalingv2.MetricSpec{
		Type: autoscalingv2.ExternalMetricSourceType,
		External: &autoscalingv2.ExternalMetricSource{
			Metric: autoscalingv2.MetricIdentifier{
				Name: externalMetricName,
			},
			Target: autoscalingv2.MetricTarget{
				Type:         autoscalingv2.ValueMetricType,
				AverageValue: new(resource.MustParse("100")),
			},
		},
	}

	testCases := []struct {
		name               string
		featureGateEnabled bool
		metricSpec         autoscalingv2.MetricSpec
		wantErrSubstring   string
	}{
		{
			name:               "resource metric is not eligible to scale to zero",
			featureGateEnabled: true,
			metricSpec:         cpuMetric,
			wantErrSubstring:   "at least one Object or External metric",
		},
		{
			name:               "minReplicas zero is forbidden while the feature gate is disabled",
			featureGateEnabled: false,
			metricSpec:         externalMetric,
			wantErrSubstring:   "minReplicas",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HPAScaleToZero, tc.featureGateEnabled)

			server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
			t.Cleanup(server.TearDownFn)
			clients, _ := createClients(t, server.ClientConfig)

			cs := clients.apiServer
			ns := createTestNamespace(t, cs)
			deployment := createDeployment(t, cs, ns.Name, 1)

			hpa := newHPA(deployment, tc.metricSpec, withHPAMinMaxReplicas(0, 10))
			_, err := cs.AutoscalingV2().HorizontalPodAutoscalers(ns.Name).Create(t.Context(), hpa, metav1.CreateOptions{})
			if err == nil {
				t.Fatal("expected creating an HPA with minReplicas: 0 to be rejected, but it succeeded")
			}
			if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), tc.wantErrSubstring) {
				t.Fatalf("expected an invalid error containing %q, got: %v", tc.wantErrSubstring, err)
			}
		})
	}
}
