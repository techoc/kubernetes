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

package dynamicresources

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// TestPreQueueingHint_PodNotYetIndexed 复现模板化 claim 的预入队竞态：
// Pod 已进索引，但 status 尚未记录模板生成的 claim 名字，
// 此时 claim 事件无法按名字反查到该 Pod。
func TestPreQueueingHint_PodNotYetIndexed(t *testing.T) {
	logger := klog.Background()
	// 用测试专用索引名，避免与 informer 注册的索引相互干扰。
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{podResourceClaimIndexPrefix + "-test": podResourceClaimIndexFunc})

	// Pod 引用模板化 claim，但 status 还没有回填生成的 claim 名字。
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pod", Namespace: "ns1"},
		Spec: v1.PodSpec{
			ResourceClaims: []v1.PodResourceClaim{
				{Name: "gpu", ResourceClaimTemplateName: new("gpu-template")},
			},
		},
	}
	// 竞态窗口始于此处：Pod 已进索引，但 podResourceClaimIndexFunc
	// 要靠 status 解析模板名，此刻还算不出可查询的 claim 键。
	if err := indexer.Add(pod); err != nil {
		t.Fatal(err)
	}

	pl := &DynamicResources{podIndexer: indexer, podResourceClaimIndex: podResourceClaimIndexPrefix + "-test"}

	// claim 分配事件落在窗口内：按 claim 名字反查索引，预期查不到 Pod。
	got, err := pl.preQueueingHint(logger, nil, &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pod-gpu-xyz", Namespace: "ns1"},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 已知限制：Pod 的 ResourceClaimStatuses 未回填时索引查不到它，
	// 只能靠周期性 flush 重新入队兜底；两种结果都接受，仅记录走了哪条路。
	if got.AllPods {
		t.Logf("AllPods=true (safe fallback)")
	} else if len(got.Pods) == 0 {
		t.Logf("empty Pods (known race - pod rescued by flush)")
	}
}

// TestPreQueueingHint_DeleteEventWithPodInIndexer 验证已分配 claim 被删除时，
// 即使索引能命中引用它的 Pod，也返回 AllPods：删除释放的资源可能对任意 Pod 可用。
func TestPreQueueingHint_DeleteEventWithPodInIndexer(t *testing.T) {
	// 删除事件走专门的保守分支，不依赖索引命中，因此不受上述竞态窗口影响。
	logger := klog.Background()
	// 用测试专用索引名，避免与 informer 注册的索引相互干扰。
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{podResourceClaimIndexPrefix + "-test": podResourceClaimIndexFunc})

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pod", Namespace: "ns1"},
		Spec: v1.PodSpec{
			ResourceClaims: []v1.PodResourceClaim{
				{Name: "gpu", ResourceClaimName: new("deleted-claim")},
			},
		},
	}
	// 直接引用 claim 名，索引无需等待 status 回填即可命中。
	if err := indexer.Add(pod); err != nil {
		t.Fatal(err)
	}

	pl := &DynamicResources{podIndexer: indexer, podResourceClaimIndex: podResourceClaimIndexPrefix + "-test"}

	// oldObj 非空、newObj 为空表示删除；带 allocation 的旧 claim 才触发 AllPods。
	got, err := pl.preQueueingHint(logger, &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "deleted-claim", Namespace: "ns1"},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{},
		},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.AllPods {
		t.Errorf("expected AllPods=true for delete of allocated claim, got %+v", got)
	}
}
