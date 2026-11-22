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

package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	nodeconfigv1alpha1 "k8s.io/cloud-provider/controllers/node/config/v1alpha1"
	nodelifecycleconfigv1alpha1 "k8s.io/cloud-provider/controllers/nodelifecycle/config/v1alpha1"
	serviceconfigv1alpha1 "k8s.io/cloud-provider/controllers/service/config/v1alpha1"
	cmconfigv1alpha1 "k8s.io/controller-manager/config/v1alpha1"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 把本包手写的默认化函数注册到 scheme；
// 生成的默认化函数在 zz_generated.defaults.go 中单独注册，二者互不覆盖。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_CloudControllerManagerConfiguration 为新构造的 CCM 配置补齐默认值。
// 默认化发生在转换与读取外部配置时，因此这里只处理“零值即未设置”的字段。
func SetDefaults_CloudControllerManagerConfiguration(obj *CloudControllerManagerConfiguration) {
	zero := metav1.Duration{}
	if obj.NodeStatusUpdateFrequency == zero {
		// 节点状态回写过频会放大云 API 压力，5 分钟是与历史行为一致的保守值。
		obj.NodeStatusUpdateFrequency = metav1.Duration{Duration: 5 * time.Minute}
	}

	// These defaults override the recommended defaults from the apimachineryconfigv1alpha1 package that are applied automatically
	// These client-connection defaults are specific to the cloud-controller-manager
	// CCM 的并发控制器数量少于 kube-controller-manager，因此放宽 QPS/Burst 上限，
	// 但仍保留独立取值以避免与通用控制器管理器共享同一组默认值。
	if obj.Generic.ClientConnection.QPS == 0 {
		obj.Generic.ClientConnection.QPS = 20
	}
	if obj.Generic.ClientConnection.Burst == 0 {
		obj.Generic.ClientConnection.Burst = 30
	}

	// Use the default RecommendedDefaultGenericControllerManagerConfiguration options
	cmconfigv1alpha1.RecommendedDefaultGenericControllerManagerConfiguration(&obj.Generic)
	// Use the default RecommendedDefaultServiceControllerConfiguration options
	serviceconfigv1alpha1.RecommendedDefaultServiceControllerConfiguration(&obj.ServiceController)
	// Use the default RecommendedDefaultNodeControllerConfiguration options
	nodeconfigv1alpha1.RecommendedDefaultNodeControllerConfiguration(&obj.NodeController)
	// Use the default RecommendedDefaultNodeLifecycleControllerConfiguration options
	nodelifecycleconfigv1alpha1.RecommendedDefaultNodeLifecycleControllerConfiguration(&obj.NodeLifecycleController)
}

// SetDefaults_KubeCloudSharedConfiguration 为 CCM 与 kube-controller-manager 共享的
// 配置段补齐默认值，确保两边在未显式配置时行为一致。
func SetDefaults_KubeCloudSharedConfiguration(obj *KubeCloudSharedConfiguration) {
	zero := metav1.Duration{}
	if obj.ClusterName == "" {
		// 空 cluster name 会使路由与负载均衡资源无法归属到集群，必须给默认值。
		obj.ClusterName = "kubernetes"
	}
	if obj.ConfigureCloudRoutes == nil {
		// 指针类型区分“未设置”与“显式 false”；默认开启以兼容传统云路由模式。
		obj.ConfigureCloudRoutes = ptr.To(true)
	}
	if obj.RouteReconciliationPeriod == zero {
		// 10 秒在路由变更及时性与云 API 调用量之间取平衡。
		obj.RouteReconciliationPeriod = metav1.Duration{Duration: 10 * time.Second}
	}
}
