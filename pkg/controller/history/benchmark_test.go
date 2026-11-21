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

package history

import (
	"fmt"
	"testing"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"
)

// BenchmarkListControllerRevisions 对 ListControllerRevisions 做基准测试，
// 覆盖不同“自有/孤儿/其他控制器所有”的 revision 占比，衡量索引与过滤成本。
func BenchmarkListControllerRevisions(b *testing.B) {
	// 被查询的父对象；自有 revision 通过 controller ref 指向它。
	parent := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-parent",
			Namespace: "default",
			UID:       types.UID("test-parent-uid"),
		},
	}
	parentKind := schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "StatefulSet",
	}

	scenarios := []struct {
		name             string
		totalRevisions   int
		ownedPercentage  float64 // 自有 revision 占总数的百分比
		orphanPercentage float64 // 孤儿 revision 占总数的百分比

	}{
		{
			name:             "0.1pct_Owned_0pct_Orphan",
			totalRevisions:   50000,
			ownedPercentage:  0.1,
			orphanPercentage: 0,
		},
		{
			name:             "0.1pct_Owned_0.1pct_Orphan",
			totalRevisions:   50000,
			ownedPercentage:  0.1,
			orphanPercentage: 0.1,
		},
		{
			name:             "1pct_Owned_0pct_Orphan",
			totalRevisions:   50000,
			ownedPercentage:  1,
			orphanPercentage: 0,
		},
		{
			name:             "1pct_Owned_1pct_Orphan",
			totalRevisions:   50000,
			ownedPercentage:  1,
			orphanPercentage: 1,
		},
		{
			name:             "10pct_Owned_0pct_Orphan",
			totalRevisions:   50000,
			ownedPercentage:  10,
			orphanPercentage: 0,
		},
		{
			name:             "10pct_Owned_10pct_Orphan",
			totalRevisions:   50000,
			ownedPercentage:  10,
			orphanPercentage: 10,
		},
		{
			name:             "100pct_Owned_0pct_Orphan",
			totalRevisions:   50000,
			ownedPercentage:  100,
			orphanPercentage: 0,
		},
	}

	// 固定 5 万条 revision，仅改变所有权分布，隔离过滤成本。
	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			// 三类 revision 相加恰好等于总数，其余归为其他控制器所有。
			ownedRevisions := int(float64(s.totalRevisions) * s.ownedPercentage / 100)
			orphans := int(float64(s.totalRevisions) * s.orphanPercentage / 100)
			otherOwned := s.totalRevisions - ownedRevisions - orphans

			client := fake.NewClientset()
			informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
			revisionInformer := informerFactory.Apps().V1().ControllerRevisions()

			// 必须按 controller ref 建索引，被测函数依赖该索引快速过滤。
			if err := AddControllerRevisionControllerIndexer(revisionInformer.Informer()); err != nil {
				b.Fatalf("failed to add indexer: %v", err)
			}

			revisions := make([]runtime.Object, 0, s.totalRevisions)
			for i := 0; i < ownedRevisions; i++ {
				revisions = append(revisions, createRevision(s.name, i, parent, parentKind))
			}

			// 创建孤儿 revision（无 controller ref），用于验证不会被误判为自有。
			for i := 0; i < orphans; i++ {
				rev := createRevision(s.name, ownedRevisions+i, nil, schema.GroupVersionKind{})
				// 清空 owner ref，确保没有 controller ref 影响过滤结果。
				rev.OwnerReferences = nil
				revisions = append(revisions, rev)
			}

			// 创建由其他控制器拥有的 revision，验证按 UID 过滤的正确性。
			otherParent := &apps.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "other-parent",
					Namespace: "default",
					UID:       types.UID("other-parent-uid"),
				},
			}
			for i := 0; i < otherOwned; i++ {
				revisions = append(revisions, createRevision(s.name, ownedRevisions+orphans+i, otherParent, parentKind))
			}

			// 直接灌入索引器，跳过 API Server，使基准只测量查询路径。
			indexer := revisionInformer.Informer().GetIndexer()
			for _, obj := range revisions {
				if err := indexer.Add(obj); err != nil {
					b.Fatalf("failed to add object to indexer: %v", err)
				}
			}

			history := NewHistory(client, revisionInformer.Lister(), indexer)
			selector, _ := labels.Parse("name=" + s.name)

			// 排除数据准备时间，只统计 ListControllerRevisions 的耗时。
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := history.ListControllerRevisions(parent, parentKind, selector)
				if err != nil {
					b.Fatalf("ListControllerRevisions failed: %v", err)
				}
			}
		})
	}
}

// createRevision 构造一条带唯一名称/UID 的 ControllerRevision；
// owner 非空时附加 controller ref，用于模拟不同所有权分布。
func createRevision(scenarioName string, index int, owner metav1.Object, ownerKind schema.GroupVersionKind) *apps.ControllerRevision {
	rev := &apps.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", scenarioName, index),
			Namespace: "default",
			Labels: map[string]string{
				"name": scenarioName,
			},
			UID: types.UID(fmt.Sprintf("%s-%d", scenarioName, index)), // 保证 UID 唯一，避免索引合并
		},
		Data:     runtime.RawExtension{Raw: []byte("{}")},
		Revision: int64(index),
	}

	if owner != nil {
		rev.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(owner, ownerKind)}
	}

	return rev
}
