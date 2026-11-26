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

package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RecommendedDefaultNodeLifecycleControllerConfiguration 为配置填入推荐默认值。
// 只对零值字段填充默认值，显式配置（包括显式的 0）不会被覆盖。
func RecommendedDefaultNodeLifecycleControllerConfiguration(obj *NodeLifecycleControllerConfiguration) {
	zero := metav1.Duration{}
	// 默认每 5s 扫描一次节点，兼顾及时性与 API 负载。
	if obj.NodeMonitorPeriod == zero {
		obj.NodeMonitorPeriod = metav1.Duration{Duration: 5 * time.Second}
	}
	// 单 worker 足够处理节点生命周期事件，也避免并发删除节点带来的竞争。
	if obj.ConcurrentNodeLifecycleSyncs == 0 {
		obj.ConcurrentNodeLifecycleSyncs = 1
	}
}
