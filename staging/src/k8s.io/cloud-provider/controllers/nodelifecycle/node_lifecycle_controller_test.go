/*
Copyright 2016 The Kubernetes Authors.

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

package nodelifecycle

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"
	fakecloud "k8s.io/cloud-provider/fake"
	k8stest "k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
)

// Test_NodesDeleted 覆盖 NotReady 节点的清理逻辑：只有云侧确认实例不存在时才删除
// Node；云 API 出错、实例仍存在、Ready=True 时都必须保留。用例同时覆盖 Instances
// 与 InstancesV2 两套接口，确保两条实现路径行为一致。
func Test_NodesDeleted(t *testing.T) {
	testcases := []struct {
		name            string
		fakeCloud       *fakecloud.Cloud
		existingNode    *v1.Node
		expectedNode    *v1.Node
		expectedDeleted bool
	}{
		{
			name: "node is not ready and does not exist",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				ExistsByProviderID: false,
			},
		},
		{
			name: "node is not ready and provider returns err",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				ExistsByProviderID: false,
				ErrByProviderID:    errors.New("err!"),
			},
		},
		{
			name: "node is not ready but still exists",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				ExistsByProviderID: true,
			},
		},
		{
			name: "node ready condition is unknown, node doesn't exist",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				ExistsByProviderID: false,
			},
		},
		{
			name: "node ready condition is unknown, node exists",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				NodeShutdown:       false,
				ExistsByProviderID: true,
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "foo://12345",
				},
			},
		},
		{
			name: "node is ready, but provider said it is deleted (maybe a bug in provider)",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				ExistsByProviderID: false,
			},
		},
		{
			name: "[instance2] node is not ready and does not exist",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2:  true,
				ExistsByProviderID: false,
			},
		},
		{
			name: "[instancev2] node is not ready and provider returns err",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2:  true,
				ExistsByProviderID: false,
				ErrByProviderID:    errors.New("err!"),
			},
		},
		{
			name: "[instancev2] node is not ready but still exists",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2:  true,
				ExistsByProviderID: true,
			},
		},
		{
			name: "[instancev2] node ready condition is unknown, node doesn't exist",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2:  true,
				ExistsByProviderID: false,
			},
		},
		{
			name: "[instancev2] node ready condition is unknown, node exists",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2:  true,
				NodeShutdown:       false,
				ExistsByProviderID: true,
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "foo://12345",
				},
			},
		},
		{
			name: "[instancev2] node is ready, but provider said it is deleted (maybe a bug in provider)",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2:  true,
				ExistsByProviderID: false,
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			clientset := fake.NewSimpleClientset(testcase.existingNode)
			informer := informers.NewSharedInformerFactory(clientset, time.Second)
			nodeInformer := informer.Core().V1().Nodes()

			// 绕过 informer 的异步启动，直接把 API 中的节点灌入缓存，保证 MonitorNodes 看到确定输入。
			if err := syncNodeStore(ctx, nodeInformer, clientset); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
			cloudNodeLifecycleController := &CloudNodeLifecycleController{
				nodeLister:                   nodeInformer.Lister(),
				kubeClient:                   clientset,
				cloud:                        testcase.fakeCloud,
				recorder:                     eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cloud-node-lifecycle-controller"}),
				nodeMonitorPeriod:            1 * time.Second,
				concurrentNodeLifecycleSyncs: 1,
			}

			w := eventBroadcaster.StartLogging(klog.Infof)
			defer w.Stop()
			cloudNodeLifecycleController.MonitorNodes(ctx)

			// 用 IsNotFound 判断删除结果，同时校验剩余节点的内容未被意外修改。
			updatedNode, err := clientset.CoreV1().Nodes().Get(ctx, testcase.existingNode.Name, metav1.GetOptions{})
			if testcase.expectedDeleted != apierrors.IsNotFound(err) {
				t.Fatalf("unexpected error happens when getting the node: %v", err)
			}
			if !reflect.DeepEqual(updatedNode, testcase.expectedNode) {
				t.Logf("actual nodes: %v", updatedNode)
				t.Logf("expected nodes: %v", testcase.expectedNode)
				t.Error("unexpected updated nodes")
			}
		})
	}
}

// Test_NodesShutdown 覆盖关机污点的添加/移除，以及各种“无法判断”场景下的保守行为：
// 实例不存在、providerID 查询报错、云厂商未实现关机检测等。
func Test_NodesShutdown(t *testing.T) {
	testcases := []struct {
		name            string
		fakeCloud       *fakecloud.Cloud
		existingNode    *v1.Node
		expectedNode    *v1.Node
		expectedDeleted bool
	}{
		{
			name: "node is not ready and was shutdown, but exists",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "node0",
					Taints: []v1.Taint{
						*ShutdownTaint,
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				NodeShutdown:            true,
				ExistsByProviderID:      true,
				ErrShutdownByProviderID: nil,
			},
		},
		{
			name: "node with empty spec providerID is not ready and was shutdown, but exists",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "",
					Taints: []v1.Taint{
						*ShutdownTaint,
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				NodeShutdown:            true,
				ExistsByProviderID:      true,
				ErrShutdownByProviderID: nil,
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "foo://12345",
				},
			},
		},
		{
			name: "node with non-existing providerID (missing in cloud provider) gets deleted",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				ErrShutdownByProviderID: nil,
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "",
				},
			},
		},
		{
			name: "node with error when getting providerID does not have shutdown taint",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				ErrShutdownByProviderID: nil,
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): errors.New("err!"),
				},
			},
		},
		{
			name: "node with InstanceID returning InstanceNotFound gets deleted",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Spec: v1.NodeSpec{
					ProviderID: "",
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				ErrShutdownByProviderID: nil,
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): cloudprovider.InstanceNotFound,
				},
			},
		},
		{
			name: "node is not ready, but there is error checking if node is shutdown",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				NodeShutdown:            false,
				ErrShutdownByProviderID: errors.New("err!"),
			},
		},
		{
			name: "node is not ready and is not shutdown",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				NodeShutdown:            false,
				ErrShutdownByProviderID: nil,
			},
		},
		{
			name: "node is ready but provider says it's shutdown (maybe a bug by provider)",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedDeleted: false,
			fakeCloud: &fakecloud.Cloud{
				NodeShutdown:            true,
				ErrShutdownByProviderID: nil,
			},
		},
		{
			name: "node is shutdown but provider says it does not exist",
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.Local),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.Local),
						},
					},
				},
			},
			expectedNode:    &v1.Node{},
			expectedDeleted: true,
			fakeCloud: &fakecloud.Cloud{
				NodeShutdown:            true,
				ExistsByProviderID:      false,
				ErrShutdownByProviderID: nil,
			},
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			clientset := fake.NewSimpleClientset(testcase.existingNode)
			informer := informers.NewSharedInformerFactory(clientset, time.Second)
			nodeInformer := informer.Core().V1().Nodes()

			if err := syncNodeStore(ctx, nodeInformer, clientset); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
			cloudNodeLifecycleController := &CloudNodeLifecycleController{
				nodeLister:                   nodeInformer.Lister(),
				kubeClient:                   clientset,
				cloud:                        testcase.fakeCloud,
				recorder:                     eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cloud-node-lifecycle-controller"}),
				nodeMonitorPeriod:            1 * time.Second,
				concurrentNodeLifecycleSyncs: 1,
			}

			// 事件 watcher 既输出事件内容，也顺带校验 involvedObject 的 API 版本存在，
			// 防止 recorder 因对象引用不完整而丢弃事件。
			e := eventBroadcaster.StartEventWatcher(func(e *v1.Event) {
				loggerV := klog.FromContext(t.Context()).V(0)
				loggerV.Info("Event occurred", "object", klog.KRef(e.InvolvedObject.Namespace, e.InvolvedObject.Name), "fieldPath", e.InvolvedObject.FieldPath, "kind", e.InvolvedObject.Kind, "apiVersion", e.InvolvedObject.APIVersion, "type", e.Type, "reason", e.Reason, "message", e.Message)
				if e.InvolvedObject.APIVersion == "" {
					t.Fatalf("event involvedObject.apiVersion is empty")
				}
			})
			defer e.Stop()
			cloudNodeLifecycleController.MonitorNodes(ctx)

			updatedNode, err := clientset.CoreV1().Nodes().Get(ctx, testcase.existingNode.Name, metav1.GetOptions{})
			if testcase.expectedDeleted != apierrors.IsNotFound(err) {
				t.Fatalf("unexpected error happens when getting the node: %v", err)
			}
			if !reflect.DeepEqual(updatedNode, testcase.expectedNode) {
				t.Logf("actual nodes: %v", updatedNode)
				t.Logf("expected nodes: %v", testcase.expectedNode)
				t.Error("unexpected updated nodes")
			}
		})
	}
}

// Test_GetProviderID 覆盖 ProviderID 的取值来源：Node.spec 已有值、InstancesV2
// 返回、Instances 按节点名解析，以及实例不存在/未知错误的错误传播。
func Test_GetProviderID(t *testing.T) {
	testcases := []struct {
		name               string
		fakeCloud          *fakecloud.Cloud
		existingNode       *v1.Node
		expectedProviderID string
		expectedErr        error
	}{
		{
			name: "node initialized with provider ID",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				Err:               nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
				},
			},
			expectedProviderID: "fake://12345",
			expectedErr:        nil,
		},
		{
			name: "node initialized with provider ID with InstancesV2",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				Err:               nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
				},
			},
			expectedProviderID: "fake://12345",
			expectedErr:        nil,
		},
		{
			name: "cloud implemented with Instances",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
			},
			expectedProviderID: "fake://12345",
			expectedErr:        nil,
		},
		{
			name: "cloud implemented with InstancesV2",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				ProviderID: map[types.NodeName]string{
					types.NodeName("node0"): "fake://12345",
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
			},
			expectedProviderID: "fake://12345",
			expectedErr:        nil,
		},
		{
			name: "cloud implemented with InstancesV2 (without providerID)",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				Err:               nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
			},
			expectedProviderID: "",
			expectedErr:        nil,
		},
		{
			name: "cloud implemented with Instances with instance missing",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): cloudprovider.InstanceNotFound,
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
			},
			expectedProviderID: "",
			expectedErr:        cloudprovider.InstanceNotFound,
		},
		{
			name: "cloud implemented with Instances with unknown error",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): errors.New("unknown error"),
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node0",
				},
			},
			expectedProviderID: "",
			expectedErr:        errors.New("failed to get instance ID from cloud provider: unknown error"),
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			cloudNodeLifecycleController := &CloudNodeLifecycleController{
				cloud:                        testcase.fakeCloud,
				concurrentNodeLifecycleSyncs: 1,
			}

			providerID, err := cloudNodeLifecycleController.getProviderID(ctx, testcase.existingNode)

			if err != nil && testcase.expectedErr == nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err == nil && testcase.expectedErr != nil {
				t.Fatalf("did not get expected error %q", testcase.expectedErr)
			}
			if err != nil && err.Error() != testcase.expectedErr.Error() {
				t.Fatalf("expected error %q, got %q", testcase.expectedErr.Error(), err.Error())
			}

			if !cmp.Equal(providerID, testcase.expectedProviderID) {
				t.Errorf("unexpected providerID %s", cmp.Diff(providerID, testcase.expectedProviderID))
			}
		})
	}
}

// syncNodeStore 用 fake clientset 中的节点直接替换 informer 缓存内容。
// 测试不启动 informer 的 Run 循环，必须手动填充 store，否则 Lister 为空。
func syncNodeStore(ctx context.Context, nodeinformer coreinformers.NodeInformer, f *fake.Clientset) error {
	nodes, err := f.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	newElems := make([]interface{}, 0, len(nodes.Items))
	for i := range nodes.Items {
		// 取切片元素地址而不是循环变量，避免所有项指向同一个变量。
		newElems = append(newElems, &nodes.Items[i])
	}
	return nodeinformer.Informer().GetStore().Replace(newElems, "newRV")
}

// TestNewCloudNodeLifecycleController 校验构造期的参数校验：并发度必须为正数。
func TestNewCloudNodeLifecycleController(t *testing.T) {
	_, _ = ktesting.NewTestContext(t)
	clientset := fake.NewSimpleClientset()
	informer := informers.NewSharedInformerFactory(clientset, time.Second)
	nodeInformer := informer.Core().V1().Nodes()
	fakeCloud := &fakecloud.Cloud{}

	testcases := []struct {
		name            string
		workers         int
		expectedWorkers int
		expectErr       bool
	}{
		{
			name:            "valid workers count",
			workers:         5,
			expectedWorkers: 5,
			expectErr:       false,
		},
		{
			name:      "workers count is 0, expect error",
			workers:   0,
			expectErr: true,
		},
		{
			name:      "workers count is negative, expect error",
			workers:   -5,
			expectErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewCloudNodeLifecycleController(
				nodeInformer,
				clientset,
				fakeCloud,
				1*time.Second,
				tc.workers,
			)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error creating controller, but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error creating controller: %v", err)
			}
			if c.concurrentNodeLifecycleSyncs != tc.expectedWorkers {
				t.Errorf("expected concurrentNodeLifecycleSyncs to be %d, but got %d", tc.expectedWorkers, c.concurrentNodeLifecycleSyncs)
			}
		})
	}
}

// TestMonitorNodesMetrics 验证一轮 MonitorNodes 后云 API 调用指标被正确计数：
// 两个 Ready 节点不触发云调用，唯一的 NotReady 节点各产生一次 exists 与 shutdown 查询。
func TestMonitorNodesMetrics(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	clientset := fake.NewSimpleClientset()
	informer := informers.NewSharedInformerFactory(clientset, time.Second)
	nodeInformer := informer.Core().V1().Nodes()
	fakeCloud := &fakecloud.Cloud{}

	// 创建两个 Ready 节点与一个 NotReady 节点。
	node0 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node0"},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
		},
	}
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node1"},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
		},
	}
	node2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node2"},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionFalse}},
		},
	}

	// 通过 clientset 把节点写入 API Server。
	for _, node := range []*v1.Node{node0, node1, node2} {
		_, err := clientset.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create node: %v", err)
		}
	}

	// 等待 informer 缓存同步到这些节点。
	if err := syncNodeStore(ctx, nodeInformer, clientset); err != nil {
		t.Fatalf("failed to sync node store: %v", err)
	}

	// 实例化控制器（自动注册指标）。
	c, err := NewCloudNodeLifecycleController(
		nodeInformer,
		clientset,
		fakeCloud,
		1*time.Second,
		1, // workers = 1
	)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	c.broadcaster = record.NewBroadcaster(record.WithContext(ctx))
	c.recorder = c.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cloud-node-lifecycle-controller"})

	// 让 fake cloud 报告 node2 仍存在，使其走“云侧存在”路径而不被删除。
	fakeCloud.Exists = true
	fakeCloud.ExistsByProviderID = true

	// 运行循环前重置指标注册表，避免其他测试影响断言。
	cloudProviderCalls.Reset()

	// 执行一次 MonitorNodes 循环。
	c.MonitorNodes(ctx)

	// 1. 校验 cloudProviderCalls {"operation": "instance_exists", "result": "success"} 恰好为 1。
	existsCount, err := k8stest.GetCounterMetricValue(cloudProviderCalls.WithLabelValues(instanceExistsOp, "success"))
	if err != nil {
		t.Errorf("unexpected error getting %s count: %v", instanceExistsOp, err)
	}
	if existsCount != 1.0 {
		t.Errorf("expected %s calls count to be 1.0, but got %f", instanceExistsOp, existsCount)
	}

	shutdownCount, err := k8stest.GetCounterMetricValue(cloudProviderCalls.WithLabelValues(instanceShutdownOp, "success"))
	if err != nil {
		t.Errorf("unexpected error getting %s count: %v", instanceShutdownOp, err)
	}
	if shutdownCount != 1.0 {
		t.Errorf("expected %s calls count to be 1.0, but got %f", instanceShutdownOp, shutdownCount)
	}
}
