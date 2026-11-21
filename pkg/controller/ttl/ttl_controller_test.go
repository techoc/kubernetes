/*
Copyright 2017 The Kubernetes Authors.

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

package ttl

import (
	"context"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	listers "k8s.io/client-go/listers/core/v1"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2/ktesting"

	"github.com/stretchr/testify/assert"
)

// TestPatchNode 验证 patchNodeWithAnnotation 生成的 JSON patch：
// 仅当注解值确实变化时才产生 patch，且不影响已有其他注解。
func TestPatchNode(t *testing.T) {
	testCases := []struct {
		node       *v1.Node
		ttlSeconds int
		patch      string
	}{
		{
			// 空节点设置 TTL=0：注解不存在，仍需新增。
			node:       &v1.Node{},
			ttlSeconds: 0,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"0\"}}}",
		},
		{
			// 空节点设置非零 TTL。
			node:       &v1.Node{},
			ttlSeconds: 10,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"10\"}}}",
		},
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "name"}},
			ttlSeconds: 10,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"10\"}}}",
		},
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}},
			ttlSeconds: 10,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"10\"}}}",
		},
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"node.alpha.kubernetes.io/ttl": "0"}}},
			ttlSeconds: 10,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"10\"}}}",
		},
		{
			// 同时存在其他注解时，patch 只覆盖 TTL 键。
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"node.alpha.kubernetes.io/ttl": "0", "a": "b"}}},
			ttlSeconds: 10,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"10\"}}}",
		},
		{
			// TTL 已是目标值，不应产生任何修改（空 patch）。
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"node.alpha.kubernetes.io/ttl": "10", "a": "b"}}},
			ttlSeconds: 10,
			patch:      "{}",
		},
	}

	for i, testCase := range testCases {
		fakeClient := &fake.Clientset{}
		ttlController := &Controller{
			kubeClient: fakeClient,
		}
		// 调用后检查 fake client 收到的 patch action 内容与预期完全一致。
		err := ttlController.patchNodeWithAnnotation(context.TODO(), testCase.node, v1.ObjectTTLAnnotationKey, testCase.ttlSeconds)
		if err != nil {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}
		actions := fakeClient.Actions()
		// 每次调用应恰好产生一个 patch action；断言其 payload 与预期字符串一致。
		assert.Len(t, actions, 1, "unexpected actions: %#v", actions)
		patchAction := actions[0].(core.PatchActionImpl)
		assert.Equal(t, testCase.patch, string(patchAction.Patch), "%d: unexpected patch: %s", i, string(patchAction.Patch))
	}
}

// TestUpdateNodeIfNeeded 验证 updateNodeIfNeeded 仅在 TTL 注解需要变化时才打 patch：
// 目标值与当前值相同则不产生任何 action。
func TestUpdateNodeIfNeeded(t *testing.T) {
	testCases := []struct {
		node       *v1.Node
		desiredTTL int
		patch      string
	}{
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "name"}},
			desiredTTL: 0,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"0\"}}}",
		},
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "name"}},
			desiredTTL: 15,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"15\"}}}",
		},
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "name"}},
			desiredTTL: 30,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"30\"}}}",
		},
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "name", Annotations: map[string]string{"node.alpha.kubernetes.io/ttl": "0"}}},
			desiredTTL: 60,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"60\"}}}",
		},
		{
			// 当前 TTL 已等于目标值：不产生 patch。
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "name", Annotations: map[string]string{"node.alpha.kubernetes.io/ttl": "60"}}},
			desiredTTL: 60,
			patch:      "",
		},
		{
			node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "name", Annotations: map[string]string{"node.alpha.kubernetes.io/ttl": "60"}}},
			desiredTTL: 30,
			patch:      "{\"metadata\":{\"annotations\":{\"node.alpha.kubernetes.io/ttl\":\"30\"}}}",
		},
	}

	for i, testCase := range testCases {
		fakeClient := &fake.Clientset{}
		nodeStore := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		nodeStore.Add(testCase.node)
		ttlController := &Controller{
			kubeClient:        fakeClient,
			nodeStore:         listers.NewNodeLister(nodeStore),
			desiredTTLSeconds: testCase.desiredTTL,
		}
		if err := ttlController.updateNodeIfNeeded(context.TODO(), testCase.node.Name); err != nil {
			t.Errorf("%d: unexpected error: %v", i, err)
			continue
		}
		actions := fakeClient.Actions()
		// patch 为空表示期望无动作，否则断言恰好一次 patch 且内容匹配。
		if testCase.patch == "" {
			assert.Empty(t, actions, "unexpected actions")
		} else {
			assert.Len(t, actions, 1, "unexpected actions: %#v", actions)
			patchAction := actions[0].(core.PatchActionImpl)
			assert.Equal(t, testCase.patch, string(patchAction.Patch), "%d: unexpected patch: %s", i, string(patchAction.Patch))
		}
	}
}

// TestDesiredTTL 验证节点数跨过阈值时 getDesiredTTLSeconds 的 TTL 计算结果：
// 覆盖新增/删除节点以及多个 boundaryStep 下的边界行为。
func TestDesiredTTL(t *testing.T) {
	testCases := []struct {
		addNode      bool
		deleteNode   bool
		nodeCount    int
		desiredTTL   int
		boundaryStep int
		expectedTTL  int
	}{
		{
			addNode:      true,
			nodeCount:    0,
			desiredTTL:   0,
			boundaryStep: 0,
			expectedTTL:  0,
		},
		{
			addNode:      true,
			nodeCount:    99,
			desiredTTL:   0,
			boundaryStep: 0,
			expectedTTL:  0,
		},
		{
			// 节点数达到 100 的阈值：TTL 从 0 升到 15。
			addNode:      true,
			nodeCount:    100,
			desiredTTL:   0,
			boundaryStep: 0,
			expectedTTL:  15,
		},
		{
			deleteNode:   true,
			nodeCount:    101,
			desiredTTL:   15,
			boundaryStep: 1,
			expectedTTL:  15,
		},
		{
			deleteNode:   true,
			nodeCount:    91,
			desiredTTL:   15,
			boundaryStep: 1,
			expectedTTL:  15,
		},
		{
			addNode:      true,
			nodeCount:    91,
			desiredTTL:   15,
			boundaryStep: 1,
			expectedTTL:  15,
		},
		{
			// 节点数跌破 90：TTL 应回落到 0。
			deleteNode:   true,
			nodeCount:    90,
			desiredTTL:   15,
			boundaryStep: 1,
			expectedTTL:  0,
		},
		{
			deleteNode:   true,
			nodeCount:    1800,
			desiredTTL:   300,
			boundaryStep: 4,
			expectedTTL:  60,
		},
		{
			deleteNode:   true,
			nodeCount:    10000,
			desiredTTL:   300,
			boundaryStep: 4,
			expectedTTL:  300,
		},
	}

	for i, testCase := range testCases {
		logger, _ := ktesting.NewTestContext(t)
		ttlController := &Controller{
			queue:             workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
			nodeCount:         testCase.nodeCount,
			desiredTTLSeconds: testCase.desiredTTL,
			boundaryStep:      testCase.boundaryStep,
		}
		if testCase.addNode {
			ttlController.addNode(logger, &v1.Node{})
		}
		if testCase.deleteNode {
			ttlController.deleteNode(logger, &v1.Node{})
		}
		// 触发一次增删节点后，断言重新计算出的 TTL 符合预期。
		assert.Equal(t, testCase.expectedTTL, ttlController.getDesiredTTLSeconds(),
			"%d: unexpected ttl: %d", i, ttlController.getDesiredTTLSeconds())
	}
}
