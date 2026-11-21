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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// RecommendedDefaultNodeLifecycleControllerConfiguration 为
// NodeLifecycleControllerConfiguration 指针设置推荐的默认值；这些默认值可能随 API 版本变化。
// 该函数刻意没有作为普通的 `SetDefaults_Foo` 注册到 scheme 中：
// scheme 的 defaulting 发生在 conversion 过程中，消费方无法方便地选择退出，
// 强制使用这些默认值会给嵌套自己的配置带来问题。
// 因此需要这些默认值时，应在自己的包装结构体的 `SetDefaults_` 方法中显式调用本函数。
func RecommendedDefaultNodeLifecycleControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.NodeLifecycleControllerConfiguration) {
	zero := metav1.Duration{}
	if obj.PodEvictionTimeout == zero {
		obj.PodEvictionTimeout = metav1.Duration{Duration: 5 * time.Minute}
	}
	// NodeMonitorGracePeriod 默认 50 秒。
	// 该值必须大于 http2 健康检查中 HTTP2_PING_TIMEOUT_SECONDS（30s）与
	// HTTP2_READ_IDLE_TIMEOUT_SECONDS（15s）之和，以确保 server 有足够时间
	// 正确处理慢连接或空闲连接，再判定节点不健康。
	if obj.NodeMonitorGracePeriod == zero {
		obj.NodeMonitorGracePeriod = metav1.Duration{Duration: 50 * time.Second}
	}
	if obj.NodeStartupGracePeriod == zero {
		obj.NodeStartupGracePeriod = metav1.Duration{Duration: 60 * time.Second}
	}
	if obj.NodeMonitorPeriod == zero {
		obj.NodeMonitorPeriod = metav1.Duration{Duration: 5 * time.Second}
	}
}
