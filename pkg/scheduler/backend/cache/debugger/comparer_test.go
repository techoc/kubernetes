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

package debugger

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// TestCompareNodes 验证 CompareNodes 能找出快照缓存相对实际节点集合的缺失冗余。
// 三张表分别覆盖：缓存多出节点、缓存缺少节点、两边完全一致。
func TestCompareNodes(t *testing.T) {
	// actual 为集群实际节点，cached 为快照缓存中的节点，missing/redundant 为预期差异。
	tests := []struct {
		name      string
		actual    []string
		cached    []string
		missing   []string
		redundant []string
	}{
		{
			name:      "redundant cached value",
			actual:    []string{"foo", "bar"},
			cached:    []string{"bar", "foo", "foobar"},
			missing:   []string{},
			redundant: []string{"foobar"},
		},
		{
			name:      "missing cached value",
			actual:    []string{"foo", "bar", "foobar"},
			cached:    []string{"bar", "foo"},
			missing:   []string{"foobar"},
			redundant: []string{},
		},
		{
			name:      "proper cache set",
			actual:    []string{"foo", "bar", "foobar"},
			cached:    []string{"bar", "foobar", "foo"},
			missing:   []string{},
			redundant: []string{},
		},
	}

	// 缓存与实际的差异与切片顺序无关，逐个子用例独立校验。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCompareNodes(test.actual, test.cached, test.missing, test.redundant, t)
		})
	}
}

// testCompareNodes 把纯字符串用例喂给 CompareNodes，并核对两组差异列表。
// 只需构造带 Name 的 Node 和以名称为键的 NodeInfo，比较器不关心其他字段。
func testCompareNodes(actual, cached, missing, redundant []string, t *testing.T) {
	compare := CacheComparer{}
	nodes := []*v1.Node{}
	// 把实际节点名称转成 Node 列表。
	for _, nodeName := range actual {
		node := &v1.Node{}
		node.Name = nodeName
		nodes = append(nodes, node)
	}

	nodeInfo := make(map[string]*framework.NodeInfo)
	// 缓存侧只需要占位 NodeInfo，名称即键。
	for _, nodeName := range cached {
		nodeInfo[nodeName] = &framework.NodeInfo{}
	}

	m, r := compare.CompareNodes(nodes, nodeInfo)

	// 缺失列表以实际集合为准，冗余列表以缓存集合为准。
	if diff := cmp.Diff(missing, m); diff != "" {
		t.Errorf("Unexpected missing (-want, +got):\n%s", diff)
	}

	if diff := cmp.Diff(redundant, r); diff != "" {
		t.Errorf("Unexpected redundant (-want, +got):\n%s", diff)
	}
}

// TestComparePods 验证 ComparePods 对缓存差异的判定，并覆盖排队中的 Pod 不算缺失的情形。
// queued 中的 Pod 虽不在缓存里，但已在调度队列，故不计入 missing。
func TestComparePods(t *testing.T) {
	// actual 为 API 中的 Pod，cached 为快照缓存，queued 为待调度队列中的 Pod。
	tests := []struct {
		name      string
		actual    []string
		cached    []string
		queued    []string
		missing   []string
		redundant []string
	}{
		{
			name:      "redundant cached value",
			actual:    []string{"foo", "bar"},
			cached:    []string{"bar", "foo", "foobar"},
			queued:    []string{},
			missing:   []string{},
			redundant: []string{"foobar"},
		},
		{
			name:      "redundant and queued values",
			actual:    []string{"foo", "bar"},
			cached:    []string{"foo", "foobar"},
			queued:    []string{"bar"},
			missing:   []string{},
			redundant: []string{"foobar"},
		},
		{
			name:      "missing cached value",
			actual:    []string{"foo", "bar", "foobar"},
			cached:    []string{"bar", "foo"},
			queued:    []string{},
			missing:   []string{"foobar"},
			redundant: []string{},
		},
		{
			name:      "missing and queued values",
			actual:    []string{"foo", "bar", "foobar"},
			cached:    []string{"foo"},
			queued:    []string{"bar"},
			missing:   []string{"foobar"},
			redundant: []string{},
		},
		{
			name:      "correct cache set",
			actual:    []string{"foo", "bar", "foobar"},
			cached:    []string{"bar", "foobar", "foo"},
			queued:    []string{},
			missing:   []string{},
			redundant: []string{},
		},
		{
			name:      "queued cache value",
			actual:    []string{"foo", "bar", "foobar"},
			cached:    []string{"foobar", "foo"},
			queued:    []string{"bar"},
			missing:   []string{},
			redundant: []string{},
		},
	}

	// 多组用例分别验证：纯冗余、冗余加排队、纯缺失、缺失加排队、一致、仅排队。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testComparePods(test.actual, test.cached, test.queued, test.missing, test.redundant, t)
		})
	}
}

// testComparePods 把字符串用例转成 Pod 列表和缓存映射，驱动 ComparePods 并校验差异。
// Pod 以 UID 标识，缓存侧需用 NewNodeInfo 构造带 Pod 的条目。
func testComparePods(actual, cached, queued, missing, redundant []string, t *testing.T) {
	compare := CacheComparer{}
	pods := []*v1.Pod{}
	// API 中的 Pod 以 UID 参与比较，命名空间和名称无关紧要。
	for _, uid := range actual {
		pod := &v1.Pod{}
		pod.UID = types.UID(uid)
		pods = append(pods, pod)
	}

	queuedPods := []*v1.Pod{}
	// 队列中的 Pod 同样只需 UID，用来从缺失集合中排除。
	for _, uid := range queued {
		pod := &v1.Pod{}
		pod.UID = types.UID(uid)
		queuedPods = append(queuedPods, pod)
	}

	nodeInfo := make(map[string]*framework.NodeInfo)
	// 缓存键是 UID，Pod 需带命名空间和名称才能放进 NodeInfo。
	for _, uid := range cached {
		pod := &v1.Pod{}
		pod.UID = types.UID(uid)
		pod.Namespace = "ns"
		pod.Name = uid

		nodeInfo[uid] = framework.NewNodeInfo(pod)
	}

	m, r := compare.ComparePods(pods, queuedPods, nodeInfo)

	// 排队中的 Pod 不应被报为缺失；冗余则来自缓存多出的条目。
	if diff := cmp.Diff(missing, m); diff != "" {
		t.Errorf("Unexpected missing (-want, +got):\n%s", diff)
	}

	if diff := cmp.Diff(redundant, r); diff != "" {
		t.Errorf("Unexpected redundant (-want, +got):\n%s", diff)
	}
}
