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
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	acappsv1 "k8s.io/client-go/applyconfigurations/apps/v1"
	"k8s.io/client-go/kubernetes"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
)

// TestHPAWithTolerance 验证配置了容差的 HPA 的扩缩容行为。
//
// 测试环境：
//  1. HPA 的扩容容差为 5%、缩容容差为 50%，基于外部指标，目标值为每 Pod 100。
//  2. 初始 35 个 Pod 的 Deployment。
//  3. 外部指标服务上报指标值 3500。
//
// 测试用例：
//  1. 指标升到 3700（增幅 6%），应扩容到 37 副本。
//  2. 指标升到 3800（增幅 3%），不应扩容。
//  3. 指标降到 1000（降幅 74%），应缩容到 10 副本。
//  4. 指标降到 800（降幅 20%），不应缩容。
func TestHPAWithTolerance(t *testing.T) {
	ctx := t.Context()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HPAConfigurableTolerance, true)

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)
	clients, metricValue := createClients(t, server.ClientConfig)
	startHPAControllerAndWaitForCaches(t, clients)

	cs := clients.apiServer
	ns := createTestNamespace(t, cs)
	deployment := createDeployment(t, cs, ns.Name, 35)

	metricValue.Store(resource.MustParse("3500"))

	// 创建使用外部指标、扩容容差 5%/缩容容差 50% 的 HPA。
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
	behavior := &autoscalingv2.HorizontalPodAutoscalerBehavior{
		ScaleUp: &autoscalingv2.HPAScalingRules{
			Tolerance: new(resource.MustParse("0.05")),
		},
		ScaleDown: &autoscalingv2.HPAScalingRules{
			Tolerance: new(resource.MustParse("0.5")),
		},
	}
	createHPA(t, cs, deployment, metricSpec, withHPAMinMaxReplicas(1, 1000), withHPABehavior(behavior))

	testcases := []struct {
		name       string
		metric     resource.Quantity
		expected   deploymentCondition
		unexpected deploymentCondition
	}{
		{
			name:     "scale_up",
			metric:   resource.MustParse("3700"),
			expected: atLeastReplicas(37),
		},
		{
			name:       "scale_up_in_tolerance",
			metric:     resource.MustParse("3800"),
			unexpected: atLeastReplicas(38),
		},
		{
			name:     "scale_down",
			metric:   resource.MustParse("1000"),
			expected: noMoreThanReplicas(10),
		},
		{
			name:       "scale_down_in_tolerance",
			metric:     resource.MustParse("800"),
			unexpected: noMoreThanReplicas(9),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			metricValue.Store(tc.metric)
			if tc.expected != nil {
				if err := waitForDeploymentCondition(ctx, cs, deployment, tc.expected); err != nil {
					t.Fatalf("Deployment did not reach the expected number of replicas: %v", err)
				}
			}
			if tc.unexpected != nil {
				if err := waitForDeploymentCondition(ctx, cs, deployment, tc.unexpected); err == nil {
					t.Fatal("Deployment reached an unexpected number of replicas")
				}
			}
			simulatePodsScheduled(t, cs, deployment)
		})
	}
}

// simulatePodsScheduled 将 Deployment 的 status.replicas 设为 spec.replicas，
// 模拟 Pod 已完成调度。
func simulatePodsScheduled(t *testing.T, cs *kubernetes.Clientset, d *appsv1.Deployment) {
	t.Helper()
	ud, err := cs.AppsV1().Deployments(d.Namespace).Get(t.Context(), d.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Cannot retrieve deployment: %v", err)
	}
	replicas := *ud.Spec.Replicas
	statusCfg := acappsv1.Deployment(d.Name, d.Namespace).
		WithStatus(acappsv1.DeploymentStatus().
			WithReplicas(replicas))
	opts := metav1.ApplyOptions{FieldManager: "podautoscaler-test"}
	if _, err = cs.AppsV1().Deployments(d.Namespace).ApplyStatus(t.Context(), statusCfg, opts); err != nil {
		t.Fatalf("Cannot set deployment status: %v", err)
	}

	// 等待 HPA controller 感知 status 更新。
	time.Sleep(hpaControllerResyncPeriod * 2)
}
