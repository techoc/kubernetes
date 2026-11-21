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

package daemon

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
)

// exclusionNodeAffinity 模拟集群级 DaemonSet（如日志采集、节点 agent）常见的
// 节点亲和性配置：单个 term 内包含多个 NotIn 表达式，用于避开几类特殊节点。
// 这种结构在评测中代表最典型的“排除型”亲和性匹配开销。
func exclusionNodeAffinity() *v1.Affinity {
	return &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{Key: "example.com/papaya", Operator: v1.NodeSelectorOpNotIn, Values: []string{"true"}},
						{Key: "example.com/kumquat", Operator: v1.NodeSelectorOpNotIn, Values: []string{"true"}},
						{Key: "quince", Operator: v1.NodeSelectorOpNotIn, Values: []string{"dragonfruit"}},
						{Key: "lychee", Operator: v1.NodeSelectorOpNotIn, Values: []string{"rambutan"}},
						{Key: "example.com/durian", Operator: v1.NodeSelectorOpNotIn, Values: []string{"mangosteen", "tamarind", "persimmon"}},
						{Key: "example.com/guava", Operator: v1.NodeSelectorOpNotIn, Values: []string{"starfruit"}},
					},
				}},
			},
		},
	}
}

// BenchmarkUpdateDaemonSetStatus 测量 DaemonSet 控制器各同步路径共用的逐节点评估循环。
// updateDaemonSetStatus、manage、rollingUpdate 都会对集群中每个节点调用
// NodeShouldRunDaemonPod；单节点主要开销就是判断 daemon pod 是否适合该节点。
// 因此该基准近似反映稳态 DaemonSet 下控制器每次同步的 CPU 成本。
// 用例覆盖有无亲和性、1000/10000 节点组合，以评估不同规模与表达式复杂度的影响。
func BenchmarkUpdateDaemonSetStatus(b *testing.B) {
	for _, tc := range []struct {
		name     string
		affinity *v1.Affinity
	}{
		{name: "no-affinity", affinity: nil},
		{name: "exclusion-affinity", affinity: exclusionNodeAffinity()},
	} {
		for _, numNodes := range []int{1000, 10000} {
			b.Run(fmt.Sprintf("affinity=%s/nodes=%d", tc.name, numNodes), func(b *testing.B) {
				// 关闭每次迭代的日志输出，使基准只测量评估循环本身，而非测试 logger 的开销。
				logger := ktesting.NewLogger(b, ktesting.NewConfig(ktesting.Verbosity(0)))
				ctx := klog.NewContext(context.Background(), logger)
				ds := newDaemonSet("bench")
				ds.Spec.Template.Spec.Affinity = tc.affinity
				// 预先把 status 设置为循环将计算出的值，使 storeDaemonSetStatus 走短路路径，
				// 从而每次迭代只测量逐节点评估循环，不包含 fake client 的 status 更新开销。
				ds.Status.DesiredNumberScheduled = int32(numNodes)
				ds.Status.NumberUnavailable = int32(numNodes)
				manager, _, _, err := newTestController(ctx, ds)
				if err != nil {
					b.Fatalf("error creating DaemonSets controller: %v", err)
				}
				if err := manager.dsStore.Add(ds); err != nil {
					b.Fatal(err)
				}
				nodeList := make([]*v1.Node, 0, numNodes)
				// 构造指定数量的节点并写入 nodeStore；hostname/zone/region 标签用于模拟真实拓扑，
				// 让亲和性评估走与生产环境相近的路径。
				for i := range numNodes {
					node := newNode(fmt.Sprintf("node-%d", i), map[string]string{
						"kubernetes.io/hostname":        fmt.Sprintf("node-%d", i),
						"topology.kubernetes.io/zone":   fmt.Sprintf("zone-%d", i%3),
						"topology.kubernetes.io/region": "region-1",
					})
					if err := manager.nodeStore.Add(node); err != nil {
						b.Fatal(err)
					}
					nodeList = append(nodeList, node)
				}

				b.ReportAllocs()
				// 计时前完成全部 fixture 搭建，避免把节点/控制器初始化计入基准结果。
				b.ResetTimer()
				for range b.N {
					if err := manager.updateDaemonSetStatus(ctx, ds, nodeList, "hash", false); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
