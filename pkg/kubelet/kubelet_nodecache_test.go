/*
Copyright 2025 The Kubernetes Authors.

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

package kubelet

import (
	"fmt"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestGetCachedNode 验证 GetCachedNode 在 informer 节点与缓存节点之间
// 选择较新 ResourceVersion 的逻辑。
func TestGetCachedNode(t *testing.T) {
	// 用例覆盖版本比较、版本解析失败、缓存为 nil、lister 报错等边界。
	tests := []struct {
		name               string
		informerNode       *v1.Node
		cachedNode         *v1.Node
		nodeListerErr      error
		expectedNode       *v1.Node
		expectedErr        bool
		expectedCachedNode *v1.Node
	}{
		{
			name:         "informer node is newer",
			informerNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
			cachedNode:   &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},

			expectedNode:       &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
			expectedCachedNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
		},
		{
			name:         "cached node is newer",
			informerNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			cachedNode:   &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},

			expectedNode:       &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
			expectedCachedNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
		},
		{
			name:         "resource versions are the same",
			informerNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			cachedNode:   &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},

			expectedNode:       &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			expectedCachedNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
		},
		{
			name:         "informer node cannot be parsed, default to informer node",
			informerNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "abc"}},
			cachedNode:   &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},

			expectedNode:       &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "abc"}},
			expectedCachedNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "abc"}},
		},
		{
			name:         "cached node cannot be parsed, default to informer node",
			informerNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			cachedNode:   &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "abc"}},

			expectedNode:       &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			expectedCachedNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
		},
		{
			name:         "cached node is nil, use informer node",
			informerNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			cachedNode:   nil,

			expectedNode:       &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			expectedCachedNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
		},
		{
			name:          "node lister returns error, use cached node",
			informerNode:  nil,
			cachedNode:    &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			nodeListerErr: fmt.Errorf("test error"),

			expectedNode:       &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
			expectedErr:        false,
			expectedCachedNode: &v1.Node{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
		},
		{
			name:          "node lister returns error, cached node is nil, default to initialNode",
			informerNode:  nil,
			cachedNode:    nil,
			nodeListerErr: fmt.Errorf("test error"),

			expectedNode:       nil, // This will be filled in by the test logic below.
			expectedErr:        false,
			expectedCachedNode: nil,
		},
	}

	// 逐用例构造 Kubelet 并比对返回节点与更新后的缓存。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			kl := Kubelet{
				cachedNode: test.cachedNode,
				nodeLister: newFakeNodeLister(test.nodeListerErr, test.informerNode),
				kubeClient: &fake.Clientset{},
			}
			// expectedNode 为 nil 时用 initialNode 的结果填充，模拟"无任何可用节点"的基线。
			if test.expectedNode == nil {
				var err error
				test.expectedNode, err = kl.initialNode(tCtx)
				if err != nil {
					test.expectedErr = true
				}
			}

			// 调用被测方法，然后同时校验返回值与缓存状态。
			actualNode, err := kl.GetCachedNode(tCtx, true)

			if (err != nil) != test.expectedErr {
				t.Errorf("GetCachedNode() unexpected error status: %v, expected error: %v", err, test.expectedErr)
			}
			if !reflect.DeepEqual(actualNode, test.expectedNode) {
				t.Errorf("GetCachedNode() = %v, expected %v", actualNode, test.expectedNode)
			}
			if !reflect.DeepEqual(kl.cachedNode, test.expectedCachedNode) {
				t.Errorf("kl.cachedNode after GetCachedNode() = %v, expected %v", kl.cachedNode, test.expectedCachedNode)
			}
		})
	}
}

// fakeNodeLister 返回预设节点与错误。
type fakeNodeLister struct {
	node *v1.Node
	err  error
}

// newFakeNodeLister 创建 fake 节点 lister。
func newFakeNodeLister(err error, node *v1.Node) *fakeNodeLister {
	ret := &fakeNodeLister{}
	ret.node = node
	ret.err = err
	return ret
}

// List 返回包含预设节点的列表。
func (l *fakeNodeLister) List(selector labels.Selector) (ret []*v1.Node, err error) {
	return []*v1.Node{l.node}, l.err
}

// Get 返回预设节点与错误，忽略名称。
func (l *fakeNodeLister) Get(name string) (*v1.Node, error) {
	return l.node, l.err
}
