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

package endpoint

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestStaleEndpointsTracker 验证 StaleEndpointsTracker 的生命周期语义：
// 标记过期后 IsStale 应该返回 true；Endpoints 的 ResourceVersion 更新后
// 说明控制器已重新处理，过期标记应当失效；删除后相关状态应被清理。
func TestStaleEndpointsTracker(t *testing.T) {
	ns := metav1.NamespaceDefault
	tracker := newStaleEndpointsTracker()

	endpoints := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000}},
		}},
	}

	// 初始状态不应被判定为过期。
	assert.False(t, tracker.IsStale(endpoints), "IsStale should return false before the endpoint is staled")

	// 标记过期后应能被识别，防止控制器基于陈旧 Endpoints 做出错误决策。
	tracker.Stale(endpoints)
	assert.True(t, tracker.IsStale(endpoints), "IsStale should return true after the endpoint is staled")

	// 更新 ResourceVersion 模拟 Endpoints 变化，旧的过期标记必须自动失效。
	endpoints.ResourceVersion = "2"
	assert.False(t, tracker.IsStale(endpoints), "IsStale should return false after the endpoint is updated")

	// 删除操作应彻底移除内部记录，避免长期运行下状态泄漏。
	tracker.Delete(endpoints.Namespace, endpoints.Name)
	assert.Empty(t, tracker.staleResourceVersionByEndpoints)
}
