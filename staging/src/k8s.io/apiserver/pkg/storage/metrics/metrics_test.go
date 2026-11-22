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

package metrics

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
)

// TestRecordStorageListMetrics 验证 LIST 路径的四类指标被正确打点。
// 中文说明：分别用 etcd 与 watchcache 两种后端，覆盖 index 标签存在与缺失的场景
// （etcd 直读时索引为空，watchcache 路径携带字段选择器索引）。
// 断言采用完整文本比对而非逐项取值，目的是同时锁定指标名、标签集与 HELP 文本，
// 防止不经意的重命名破坏监控契约。
func TestRecordStorageListMetrics(t *testing.T) {
	// 使用独立 registry，避免与其他测试共享全局状态；defer Reset 保证用例间隔离。
	registry := k8smetrics.NewKubeRegistry()
	defer registry.Reset()

	registry.MustRegister(listStorageCount)
	registry.MustRegister(listStorageNumFetched)
	registry.MustRegister(listStorageNumSelectorEvals)
	registry.MustRegister(listStorageNumReturned)

	groupResource := schema.GroupResource{Group: "apps", Resource: "deployments"}

	// 两条记录分别对应 etcd 直读与 watchcache 读取，因此 index 标签不同。
	RecordStorageListMetrics(groupResource, StorageBackendEtcd, "", 4, 3, 1)
	RecordStorageListMetrics(groupResource, StorageBackendWatchCache, "f:spec.nodeName", 2, 0, 2)

	expected := `# HELP apiserver_storage_list_evaluated_objects_total [ALPHA] Number of objects tested in the course of serving a LIST request from storage
# TYPE apiserver_storage_list_evaluated_objects_total counter
apiserver_storage_list_evaluated_objects_total{group="apps",resource="deployments",storage="etcd"} 3
apiserver_storage_list_evaluated_objects_total{group="apps",resource="deployments",storage="watchcache"} 0
# HELP apiserver_storage_list_fetched_objects_total [ALPHA] Number of objects read from storage in the course of serving a LIST request
# TYPE apiserver_storage_list_fetched_objects_total counter
apiserver_storage_list_fetched_objects_total{group="apps",index="",resource="deployments",storage="etcd"} 4
apiserver_storage_list_fetched_objects_total{group="apps",index="f:spec.nodeName",resource="deployments",storage="watchcache"} 2
# HELP apiserver_storage_list_returned_objects_total [ALPHA] Number of objects returned for a LIST request from storage
# TYPE apiserver_storage_list_returned_objects_total counter
apiserver_storage_list_returned_objects_total{group="apps",resource="deployments",storage="etcd"} 1
apiserver_storage_list_returned_objects_total{group="apps",resource="deployments",storage="watchcache"} 2
# HELP apiserver_storage_list_total [ALPHA] Number of LIST requests served from storage
# TYPE apiserver_storage_list_total counter
apiserver_storage_list_total{group="apps",index="",resource="deployments",storage="etcd"} 1
apiserver_storage_list_total{group="apps",index="f:spec.nodeName",resource="deployments",storage="watchcache"} 1
`

	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected),
		"apiserver_storage_list_total",
		"apiserver_storage_list_fetched_objects_total",
		"apiserver_storage_list_evaluated_objects_total",
		"apiserver_storage_list_returned_objects_total",
	); err != nil {
		t.Fatal(err)
	}
}
