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

package nodelifecycle

import (
	"context"
	"fmt"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	coordv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coordinformers "k8s.io/client-go/informers/coordination/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	testcore "k8s.io/client-go/testing"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/nodelifecycle/scheduler"
	"k8s.io/kubernetes/pkg/controller/testutil"
	controllerutil "k8s.io/kubernetes/pkg/controller/util/node"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/util/node"
	taintutils "k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

const (
	// testNodeMonitorGracePeriod 是测试中节点心跳超时判定为不健康所容忍的时长。
	testNodeMonitorGracePeriod = 50 * time.Second
	// testNodeStartupGracePeriod 是测试中刚创建的节点在建立状态前的启动宽限期。
	testNodeStartupGracePeriod = 60 * time.Second
	// testNodeMonitorPeriod 是测试中节点健康监控的轮询周期。
	testNodeMonitorPeriod = 5 * time.Second
	// testRateLimiterQPS 设置足够高的驱逐速率，避免测试因限流而出现不确定的等待。
	testRateLimiterQPS = float32(100000)
	// testLargeClusterThreshold 是判断是否启用大规模集群限流策略的节点数阈值。
	testLargeClusterThreshold = 20
	// testUnhealthyThreshold 是 zone 中不健康节点比例超过该值时进入部分中断状态。
	testUnhealthyThreshold = float32(0.55)
)

// alwaysReady 作为 informer 的 sync 判定函数，测试中不依赖真实的缓存同步，
// 直接返回 true 让控制器认为所有 informer 均已就绪。
func alwaysReady() bool { return true }

// fakeGetPodsAssignedToNode 返回一个按节点名查询 Pod 的回调，
// 用于替代控制器中依赖 list-watch 缓存的实现，直接走 fake clientset 查询。
func fakeGetPodsAssignedToNode(c *fake.Clientset) func(string) ([]*v1.Pod, error) {
	return func(nodeName string) ([]*v1.Pod, error) {
		selector := fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName})
		pods, err := c.CoreV1().Pods(v1.NamespaceAll).List(context.TODO(), metav1.ListOptions{
			FieldSelector: selector.String(),
			LabelSelector: labels.Everything().String(),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get Pods assigned to node %v", nodeName)
		}
		// 返回指针切片而非值拷贝，保证调用方拿到的是 fake clientset 中的对象引用。
		rPods := make([]*v1.Pod, len(pods.Items))
		for i := range pods.Items {
			rPods[i] = &pods.Items[i]
		}
		return rPods, nil
	}
}

// nodeLifecycleController 在真实控制器之上保存测试所需的 informer 引用，
// 便于测试直接向各 informer 的本地 store 写入/同步对象。
type nodeLifecycleController struct {
	*Controller
	// leaseInformer 保存节点租约（node lease）的 informer。
	leaseInformer coordinformers.LeaseInformer
	// nodeInformer 保存节点对象的 informer。
	nodeInformer coreinformers.NodeInformer
	// daemonSetInformer 保存 DaemonSet 的 informer，用于判定 DaemonSet Pod。
	daemonSetInformer appsinformers.DaemonSetInformer
	// podInformer 保存 Pod 的 informer。
	podInformer coreinformers.PodInformer
}

// createNodeLease 构造一个持有者为 nodeName、续约时间为 renewTime 的租约对象，
// 模拟 kubelet 定期上报的心跳。
func createNodeLease(nodeName string, renewTime metav1.MicroTime) *coordv1.Lease {
	return &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
			Namespace: v1.NamespaceNodeLease,
		},
		Spec: coordv1.LeaseSpec{
			HolderIdentity: ptr.To(nodeName),
			RenewTime:      &renewTime,
		},
	}
}

// syncLeaseStore 将指定租约写入租约 informer 的本地 store，
// 模拟 informer 从 API Server 收到租约更新事件。
func (nc *nodeLifecycleController) syncLeaseStore(lease *coordv1.Lease) error {
	if lease == nil {
		return nil
	}
	// Replace 会整体替换 store 内容；测试场景下只需放入单个租约即可。
	newElems := make([]interface{}, 0, 1)
	newElems = append(newElems, lease)
	return nc.leaseInformer.Informer().GetStore().Replace(newElems, "newRV")
}

// syncNodeStore 从 FakeNodeHandler 读取全部节点并写入节点 informer 的本地 store，
// 使控制器通过缓存看到测试设置的节点列表。
func (nc *nodeLifecycleController) syncNodeStore(fakeNodeHandler *testutil.FakeNodeHandler) error {
	nodes, err := fakeNodeHandler.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	newElems := make([]interface{}, 0, len(nodes.Items))
	for i := range nodes.Items {
		newElems = append(newElems, &nodes.Items[i])
	}
	return nc.nodeInformer.Informer().GetStore().Replace(newElems, "newRV")
}

// syncPodStore 将指定 Pod 写入 Pod informer 的本地 store，
// 用于模拟 Pod 被调度到节点后 informer 收到通知。
func (nc *nodeLifecycleController) syncPodStore(pod *v1.Pod) error {
	if pod == nil {
		return nil
	}
	newElems := make([]interface{}, 0, 1)
	newElems = append(newElems, pod)
	return nc.podInformer.Informer().GetStore().Replace(newElems, "newRV")
}

// newNodeLifecycleControllerFromClient 基于 fake clientset 构建节点生命周期控制器及其 informer。
// 所有 informer 的同步判定都被替换为 alwaysReady，避免测试等待缓存同步。
func newNodeLifecycleControllerFromClient(
	ctx context.Context,
	kubeClient clientset.Interface,
	evictionLimiterQPS float32,
	secondaryEvictionLimiterQPS float32,
	largeClusterThreshold int32,
	unhealthyZoneThreshold float32,
	nodeMonitorGracePeriod time.Duration,
	nodeStartupGracePeriod time.Duration,
	nodeMonitorPeriod time.Duration,
) (*nodeLifecycleController, error) {

	factory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

	leaseInformer := factory.Coordination().V1().Leases()
	nodeInformer := factory.Core().V1().Nodes()
	daemonSetInformer := factory.Apps().V1().DaemonSets()
	podInformer := factory.Core().V1().Pods()

	nc, err := NewNodeLifecycleController(
		ctx,
		leaseInformer,
		podInformer,
		nodeInformer,
		daemonSetInformer,
		kubeClient,
		nodeMonitorPeriod,
		nodeStartupGracePeriod,
		nodeMonitorGracePeriod,
		evictionLimiterQPS,
		secondaryEvictionLimiterQPS,
		largeClusterThreshold,
		unhealthyZoneThreshold,
	)
	if err != nil {
		return nil, err
	}

	// fake informer 不会真正启动，直接把同步判定置为 ready，跳过等待逻辑。
	nc.leaseInformerSynced = alwaysReady
	nc.podInformerSynced = alwaysReady
	nc.nodeInformerSynced = alwaysReady
	nc.daemonSetInformerSynced = alwaysReady

	return &nodeLifecycleController{nc, leaseInformer, nodeInformer, daemonSetInformer, podInformer}, nil
}

// TestMonitorNodeHealth 验证 monitorNodeHealth 在节点心跳变化时对各 zone 状态（正常/部分中断/完全中断）的推导是否正确。
func TestMonitorNodeHealth(t *testing.T) {
	// 固定“当前时间”，避免真实时钟导致断言不稳定；timeToPass 模拟心跳间隔后的一次观察。
	fakeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)
	timeToPass := 60 * time.Minute
	healthyNodeNewStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:               v1.NodeReady,
				Status:             v1.ConditionTrue,
				LastHeartbeatTime:  metav1.NewTime(fakeNow.Add(timeToPass)),
				LastTransitionTime: fakeNow,
			},
		},
	}
	unhealthyNodeNewStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:   v1.NodeReady,
				Status: v1.ConditionUnknown,
				// 心跳停留在 fakeNow，经过 timeToPass 后即被视为陈旧状态。
				LastHeartbeatTime:  fakeNow,
				LastTransitionTime: fakeNow,
			},
		},
	}

	tests := map[string]struct {
		// nodeList 是首次监控时的初始节点集合。
		nodeList []*v1.Node
		// updatedNodeStatuses 是第二次监控时依次写入各节点的新状态。
		updatedNodeStatuses []v1.NodeStatus
		// expectedInitialStates 是第一次监控后期望的 zone 状态。
		expectedInitialStates map[string]ZoneState
		// expectedFollowingStates 是心跳推进并更新状态后期望的 zone 状态。
		expectedFollowingStates map[string]ZoneState
	}{
		"No Disruption: Node created recently without failure domain labels (happens only at cluster startup)": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionTrue,
								LastHeartbeatTime:  fakeNow,
								LastTransitionTime: fakeNow,
							},
						},
					},
				},
			},
			updatedNodeStatuses: []v1.NodeStatus{
				healthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				"": stateNormal,
			},
			expectedFollowingStates: map[string]ZoneState{
				"": stateNormal,
			},
		},
		"No Disruption: Initially both zones down, one comes back": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node1",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone2",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone2",
						},
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
			},
			updatedNodeStatuses: []v1.NodeStatus{
				unhealthyNodeNewStatus,
				healthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
				testutil.CreateZoneID("region1", "zone2"): stateFullDisruption,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
				testutil.CreateZoneID("region1", "zone2"): stateNormal,
			},
		},
		"Partial Disruption: Nodes created recently without status conditions (happens only at cluster startup)": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node1",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node2",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node3",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
					},
				},
			},
			updatedNodeStatuses: []v1.NodeStatus{
				unhealthyNodeNewStatus,
				unhealthyNodeNewStatus,
				unhealthyNodeNewStatus,
				healthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				// 尚未收到任何节点状态，控制器保守地假定该 zone 完全中断。
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): statePartialDisruption,
			},
		},
		"Partial Disruption: one Node failed leading to the number of healthy Nodes to exceed the configured threshold": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node1",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node2",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node3",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node4",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
			},
			updatedNodeStatuses: []v1.NodeStatus{
				unhealthyNodeNewStatus,
				unhealthyNodeNewStatus,
				unhealthyNodeNewStatus,
				healthyNodeNewStatus,
				healthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateNormal,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): statePartialDisruption,
			},
		},
		"Full Disruption: the zone has less than 2 Nodes down, the last healthy Node has failed": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node1",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node2",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
			},
			updatedNodeStatuses: []v1.NodeStatus{
				unhealthyNodeNewStatus,
				unhealthyNodeNewStatus,
				unhealthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				// 不健康节点数不超过 2 时，控制器忽略健康/不健康比例，仍视该 zone 为正常。
				testutil.CreateZoneID("region1", "zone1"): stateNormal,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
			},
		},
		"Full Disruption: all the Nodes in one zone are down": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node1",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone2",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone2",
						},
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
			},
			updatedNodeStatuses: []v1.NodeStatus{
				unhealthyNodeNewStatus,
				healthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
				testutil.CreateZoneID("region1", "zone2"): stateNormal,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
				testutil.CreateZoneID("region1", "zone2"): stateNormal,
			},
		},
		"Full Disruption: all the Nodes in both the zones are down": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node1",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region2",
							v1.LabelTopologyZone:            "zone2",
							v1.LabelFailureDomainBetaRegion: "region2",
							v1.LabelFailureDomainBetaZone:   "zone2",
						},
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
			},

			updatedNodeStatuses: []v1.NodeStatus{
				unhealthyNodeNewStatus,
				unhealthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
				testutil.CreateZoneID("region2", "zone2"): stateFullDisruption,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
				testutil.CreateZoneID("region2", "zone2"): stateFullDisruption,
			},
		},
		"Full Disruption: Ready condition removed from the Node": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
			},

			updatedNodeStatuses: []v1.NodeStatus{
				{
					Conditions: []v1.NodeCondition{},
				},
			},
			expectedInitialStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateNormal,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
			},
		},
		"Full Disruption: the only available Node has the node.kubernetes.io/exclude-disruption label": {
			nodeList: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
						},
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node-master",
						CreationTimestamp: fakeNow,
						Labels: map[string]string{
							v1.LabelTopologyRegion:          "region1",
							v1.LabelTopologyZone:            "zone1",
							v1.LabelFailureDomainBetaRegion: "region1",
							v1.LabelFailureDomainBetaZone:   "zone1",
							labelNodeDisruptionExclusion:    "",
						},
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
			},
			updatedNodeStatuses: []v1.NodeStatus{
				unhealthyNodeNewStatus,
				healthyNodeNewStatus,
			},
			expectedInitialStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
			},
			expectedFollowingStates: map[string]ZoneState{
				testutil.CreateZoneID("region1", "zone1"): stateFullDisruption,
			},
		},
	}

	for testName, tt := range tests {
		t.Run(testName, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			// 用 FakeNodeHandler 提供节点列表，控制器仍通过 informer store 读取节点。
			fakeNodeHandler := &testutil.FakeNodeHandler{
				Existing:  tt.nodeList,
				Clientset: fake.NewSimpleClientset(),
			}
			nodeController, _ := newNodeLifecycleControllerFromClient(
				tCtx,
				fakeNodeHandler,
				testRateLimiterQPS,
				testRateLimiterQPS,
				testLargeClusterThreshold,
				testUnhealthyThreshold,
				testNodeMonitorGracePeriod,
				testNodeStartupGracePeriod,
				testNodeMonitorPeriod)
			// 替换事件记录器与限流函数，避免测试依赖真实事件管道和动态限流计算。
			nodeController.recorder = testutil.NewFakeRecorder()
			nodeController.enterPartialDisruptionFunc = func(nodeNum int) float32 {
				return testRateLimiterQPS
			}
			nodeController.enterFullDisruptionFunc = func(nodeNum int) float32 {
				return testRateLimiterQPS
			}

			// syncAndDiffZoneState 把 fake 节点同步到 informer store 后运行一次监控并校验 zone 状态。
			syncAndDiffZoneState := func(wanted map[string]ZoneState) {
				if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if err := nodeController.monitorNodeHealth(tCtx); err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if diff := cmp.Diff(wanted, nodeController.zoneStates); diff != "" {
					t.Errorf("unexpected zone state (-want +got):\n%s", diff)
				}
			}

			// 第一次监控：此时“当前时间”与心跳时间一致，节点被视为健康。
			nodeController.now = func() metav1.Time { return fakeNow }
			syncAndDiffZoneState(tt.expectedInitialStates)

			// 第二次监控：把时钟推进 timeToPass，使未按新心跳时间更新的节点变为陈旧。
			nodeController.now = func() metav1.Time { return metav1.Time{Time: fakeNow.Add(timeToPass)} }
			for i := range tt.updatedNodeStatuses {
				fakeNodeHandler.Existing[i].Status = tt.updatedNodeStatuses[i]
			}
			syncAndDiffZoneState(tt.expectedFollowingStates)
		})
	}
}

// TestPodStatusChange 验证节点被判定不可达后，节点控制器为 Pod 写入的终止原因
// 是否包含 NodeUnreachablePodReason；同时覆盖只有一个 NotReady 节点时不驱逐的保护逻辑。
func TestPodStatusChange(t *testing.T) {
	fakeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)

	// 全部节点 NotReady 时控制器不会驱逐 Pod，因此用例中额外放置一个健康节点，
	// 并在第二次监控时更新它的状态以模拟其恢复心跳。
	healthyNodeNewStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:   v1.NodeReady,
				Status: v1.ConditionTrue,
				// 心跳刚更新且已连续 10 分钟处于 NotReady，触发污点替换。
				LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 9, 0, 0, time.UTC),
				LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	}

	// 节点创建时间很早且长期上报 Unknown，测试期望控制器将其上的 Pod 状态标记为终止。
	table := []struct {
		// fakeNodeHandler 持有初始节点、Pod 列表并记录客户端动作。
		fakeNodeHandler *testutil.FakeNodeHandler
		// timeToPass 是推进到第二次监控前经过的时长，0 表示不推进。
		timeToPass time.Duration
		// newNodeStatus 是第二次监控时写入首个节点的状态。
		newNodeStatus v1.NodeStatus
		// secondNodeNewStatus 是第二次监控时写入健康节点的状态。
		secondNodeNewStatus v1.NodeStatus
		// expectedPodUpdate 表示是否期望出现 Pod 更新动作。
		expectedPodUpdate bool
		// expectedReason 是期望写入 Pod 状态的终止原因。
		expectedReason string
		// description 描述用例场景，用于错误信息。
		description string
	}{
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
							Labels: map[string]string{
								v1.LabelTopologyRegion:          "region1",
								v1.LabelTopologyZone:            "zone1",
								v1.LabelFailureDomainBetaRegion: "region1",
								v1.LabelFailureDomainBetaZone:   "zone1",
							},
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
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node1",
							CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
							Labels: map[string]string{
								v1.LabelFailureDomainBetaRegion: "region1",
								v1.LabelFailureDomainBetaZone:   "zone1",
							},
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
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			timeToPass: 60 * time.Minute,
			newNodeStatus: v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{
						Type:   v1.NodeReady,
						Status: v1.ConditionUnknown,
						// 心跳时间停留在 1 小时前，使节点在推进时钟后被判定为不可达。
						LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
					},
				},
			},
			secondNodeNewStatus: healthyNodeNewStatus,
			expectedPodUpdate:   true,
			expectedReason:      node.NodeUnreachablePodReason,
			description: "Node created long time ago, node controller posted Unknown for a " +
				"long period of time, the pod status must include reason for termination.",
		},
	}

	tCtx := ktesting.Init(t)
	for _, item := range table {
		// 替换 Pod 查询实现，让 tainter 的回调能通过 fake client 找到节点上的 Pod。
		nodeController, _ := newNodeLifecycleControllerFromClient(
			tCtx,
			item.fakeNodeHandler,
			testRateLimiterQPS,
			testRateLimiterQPS,
			testLargeClusterThreshold,
			testUnhealthyThreshold,
			testNodeMonitorGracePeriod,
			testNodeStartupGracePeriod,
			testNodeMonitorPeriod,
		)
		nodeController.now = func() metav1.Time { return fakeNow }
		nodeController.recorder = testutil.NewFakeRecorder()
		nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(item.fakeNodeHandler.Clientset)
		if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.monitorNodeHealth(tCtx); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		// 推进时钟并更新两个节点的状态：首个节点保持陈旧心跳，健康节点恢复心跳。
		// 推进时钟并更新节点状态，触发第二次监控以覆盖心跳超时场景。
		if item.timeToPass > 0 {
			nodeController.now = func() metav1.Time { return metav1.Time{Time: fakeNow.Add(item.timeToPass)} }
			item.fakeNodeHandler.Existing[0].Status = item.newNodeStatus
			item.fakeNodeHandler.Existing[1].Status = item.secondNodeNewStatus
		}
		if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.monitorNodeHealth(tCtx); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		// 直接触发每个 zone 的 tainter 回调，驱动 Pod 删除流程，无需等待限速队列的真实定时器。
		zones := testutil.GetZones(item.fakeNodeHandler)
		for _, zone := range zones {
			nodeController.zoneNoExecuteTainter[zone].Try(tCtx.Logger(), func(value scheduler.TimedValue) (bool, time.Duration) {
				nodeUID, _ := value.UID.(string)
				pods, err := nodeController.getPodsAssignedToNode(value.Value)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				_, err = controllerutil.DeletePods(tCtx, item.fakeNodeHandler, pods, nodeController.recorder, value.Value, nodeUID, nodeController.daemonSetStore)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return true, 0
			})
		}

		// 检查 fake client 是否记录到了带正确 reason 的 Pod 更新动作。
		podReasonUpdate := false
		for _, action := range item.fakeNodeHandler.Actions() {
			if action.GetVerb() == "update" && action.GetResource().Resource == "pods" {
				updateReason := action.(testcore.UpdateActionImpl).GetObject().(*v1.Pod).Status.Reason
				podReasonUpdate = true
				if updateReason != item.expectedReason {
					t.Errorf("expected pod status reason: %+v, got %+v for %+v", item.expectedReason, updateReason, item.description)
				}
			}
		}

		if podReasonUpdate != item.expectedPodUpdate {
			t.Errorf("expected pod update: %+v, got %+v for %+v", item.expectedPodUpdate, podReasonUpdate, item.description)
		}
	}
}

// TestMonitorNodeHealthUpdateStatus 验证不同节点创建/心跳时间下，控制器是否
// 正确地给节点写入 Unknown 状态、更新 Pod 状态，以及是否产生多余的请求。
func TestMonitorNodeHealthUpdateStatus(t *testing.T) {
	// 固定当前时间，使宽限期判断和期望时间戳可预测。
	fakeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)
	table := []struct {
		// fakeNodeHandler 提供初始节点和 Pod，并记录客户端请求。
		fakeNodeHandler *testutil.FakeNodeHandler
		// timeToPass 是第二次监控前推进的时钟时长。
		timeToPass time.Duration
		// newNodeStatus 是推进时钟后写入节点的新状态。
		newNodeStatus v1.NodeStatus
		// expectedRequestCount 是期望的 fake 客户端请求总数。
		expectedRequestCount int
		// expectedNodes 是期望写入的节点状态（nil 表示不应更新节点）。
		expectedNodes []*v1.Node
		// expectedPodStatusUpdate 表示是否期望更新 Pod 的 status 子资源。
		expectedPodStatusUpdate bool
	}{
		// 节点创建时间久远且没有任何状态：期望控制器上报 Unknown。
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedRequestCount: 2, // List 加 Update 两次请求
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodeMemoryPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodeDiskPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodePIDPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
								LastTransitionTime: fakeNow,
							},
						},
					},
				},
			},
			expectedPodStatusUpdate: false, // Pod 从未被调度过
		},
		// 节点刚创建且没有状态：处于启动宽限期内，期望控制器不采取任何动作。
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: fakeNow,
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedRequestCount:    1, // List 一次请求
			expectedNodes:           nil,
			expectedPodStatusUpdate: false,
		},
		// 节点创建时间久远且 kubelet 心跳已超过监控宽限期：期望上报 Unknown。
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:   v1.NodeReady,
									Status: v1.ConditionTrue,
									// 心跳时间停留在 1 小时前，推进时钟后即超过监控宽限期。
									LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
									LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedRequestCount: 3, // （List+）List 加 Update
			timeToPass:           time.Hour,
			newNodeStatus: v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{
						Type:   v1.NodeReady,
						Status: v1.ConditionTrue,
						// 心跳时间停留在 1 小时前，推进时钟后即超过监控宽限期。
						LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			},
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusUnknown",
								Message:            "Kubelet stopped posting node status.",
								LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
								LastTransitionTime: metav1.Time{Time: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC).Add(time.Hour)},
							},
							{
								Type:    v1.NodeMemoryPressure,
								Status:  v1.ConditionUnknown,
								Reason:  "NodeStatusNeverUpdated",
								Message: "Kubelet never posted node status.",
								// 条件从未更新过，心跳时间默认回退到节点创建时间。
								LastHeartbeatTime:  metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
								LastTransitionTime: metav1.Time{Time: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC).Add(time.Hour)},
							},
							{
								Type:    v1.NodeDiskPressure,
								Status:  v1.ConditionUnknown,
								Reason:  "NodeStatusNeverUpdated",
								Message: "Kubelet never posted node status.",
								// 条件从未更新过，心跳时间默认回退到节点创建时间。
								LastHeartbeatTime:  metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
								LastTransitionTime: metav1.Time{Time: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC).Add(time.Hour)},
							},
							{
								Type:    v1.NodePIDPressure,
								Status:  v1.ConditionUnknown,
								Reason:  "NodeStatusNeverUpdated",
								Message: "Kubelet never posted node status.",
								// 条件从未更新过，心跳时间默认回退到节点创建时间。
								LastHeartbeatTime:  metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
								LastTransitionTime: metav1.Time{Time: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC).Add(time.Hour)},
							},
						},
						Capacity: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
						},
					},
				},
			},
			expectedPodStatusUpdate: true,
		},
		// 节点创建时间久远但心跳新鲜：处于监控宽限期内，期望控制器不采取任何动作。
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:   v1.NodeReady,
									Status: v1.ConditionTrue,
									// 心跳刚更新过，处于监控宽限期内，不应被判定为不健康。
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedRequestCount:    1, // List 一次请求
			expectedNodes:           nil,
			expectedPodStatusUpdate: false,
		},
	}
	tCtx := ktesting.Init(t)
	for i, item := range table {
		// 用 fake 客户端查询节点上的 Pod，避免依赖真实 informer 缓存。
		nodeController, _ := newNodeLifecycleControllerFromClient(
			tCtx,
			item.fakeNodeHandler,
			testRateLimiterQPS,
			testRateLimiterQPS,
			testLargeClusterThreshold,
			testUnhealthyThreshold,
			testNodeMonitorGracePeriod,
			testNodeStartupGracePeriod,
			testNodeMonitorPeriod,
		)
		nodeController.now = func() metav1.Time { return fakeNow }
		nodeController.recorder = testutil.NewFakeRecorder()
		nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(item.fakeNodeHandler.Clientset)
		if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.monitorNodeHealth(tCtx); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		// 推进时钟并更新节点状态，触发第二次监控以覆盖心跳超时场景。
		if item.timeToPass > 0 {
			nodeController.now = func() metav1.Time { return metav1.Time{Time: fakeNow.Add(item.timeToPass)} }
			item.fakeNodeHandler.Existing[0].Status = item.newNodeStatus
			if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err := nodeController.monitorNodeHealth(tCtx); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
		if item.expectedRequestCount != item.fakeNodeHandler.RequestCount {
			t.Errorf("expected %v call, but got %v.", item.expectedRequestCount, item.fakeNodeHandler.RequestCount)
		}
		if len(item.fakeNodeHandler.UpdatedNodes) > 0 && !apiequality.Semantic.DeepEqual(item.expectedNodes, item.fakeNodeHandler.UpdatedNodes) {
			t.Errorf("Case[%d] unexpected nodes: %s", i, cmp.Diff(item.expectedNodes[0], item.fakeNodeHandler.UpdatedNodes[0]))
		}
		if len(item.fakeNodeHandler.UpdatedNodeStatuses) > 0 && !apiequality.Semantic.DeepEqual(item.expectedNodes, item.fakeNodeHandler.UpdatedNodeStatuses) {
			t.Errorf("Case[%d] unexpected nodes: %s", i, cmp.Diff(item.expectedNodes[0], item.fakeNodeHandler.UpdatedNodeStatuses[0]))
		}

		// 检查是否产生了 Pod status 子资源更新动作。
		podStatusUpdated := false
		for _, action := range item.fakeNodeHandler.Actions() {
			if action.GetVerb() == "update" && action.GetResource().Resource == "pods" && action.GetSubresource() == "status" {
				podStatusUpdated = true
			}
		}
		if podStatusUpdated != item.expectedPodStatusUpdate {
			t.Errorf("Case[%d] expect pod status updated to be %v, but got %v", i, item.expectedPodStatusUpdate, podStatusUpdated)
		}
	}
}

// TestMonitorNodeHealthUpdateNodeAndPodStatusWithLease 在启用 NodeControllerLeaseCircuitBreaker 特性门控的前提下，
// 验证节点 Lease 的续约/过期（含缓存与 API Server 不一致的双检场景）如何影响节点状态上报与 Pod 状态更新。
func TestMonitorNodeHealthUpdateNodeAndPodStatusWithLease(t *testing.T) {
	nodeCreationTime := metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC)
	fakeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)

	// 必须显式打开特性门控，才能覆盖 Lease 断路器（先查缓存再回源 API Server 双检）的逻辑。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeControllerLeaseCircuitBreaker, true)

	testcases := []struct {
		// description 描述用例场景，同时作为子测试名称。
		description string
		// fakeNodeHandler 提供节点与 Pod 列表，并记录客户端请求。
		fakeNodeHandler *testutil.FakeNodeHandler
		// lease 是初始写入 lease informer 缓存的租约。
		lease *coordv1.Lease
		// timeToPass 是第二次监控前推进的时钟时长。
		timeToPass time.Duration
		// newNodeStatus 是推进时钟后写入节点的新状态。
		newNodeStatus v1.NodeStatus
		// newLease 是推进时钟后写入缓存的新租约，用于模拟续约或过期。
		newLease *coordv1.Lease
		// apiServerLease 是首次监控时 API Server 侧（fake client）持有的租约。
		apiServerLease *coordv1.Lease
		// newAPIServerLease 是推进时钟后 API Server 侧的新租约，用于构造缓存与服务端不一致的双检场景。
		newAPIServerLease *coordv1.Lease
		// expectedRequestCount 是期望的客户端请求总数。
		expectedRequestCount int
		// expectedNodes 是期望写入的节点对象（nil 表示不应更新节点）。
		expectedNodes []*v1.Node
		// expectedPodStatusUpdate 表示是否期望更新 Pod 的 status 子资源。
		expectedPodStatusUpdate bool
	}{
		// 节点刚创建且无状态，同时缺少租约：处于启动宽限期内，期望控制器不采取任何动作。
		// 处于启动宽限期内，期望控制器不采取任何动作。
		{
			description: "Node created recently, without status. Node lease is missing.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: fakeNow,
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedRequestCount:    1, // List 一次请求
			expectedNodes:           nil,
			expectedPodStatusUpdate: false,
		},
		// 节点刚创建且无状态，但租约最近续约过：仍处于启动宽限期内，期望不采取任何动作。
		// 处于启动宽限期内，期望控制器不采取任何动作。
		{
			description: "Node created recently, without status. Node lease is renewed recently.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: fakeNow,
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease:                   createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)),
			expectedRequestCount:    1, // List 一次请求
			expectedNodes:           nil,
			expectedPodStatusUpdate: false,
		},
		// 节点创建时间久远且无状态，也没有租约：期望控制器上报 Unknown。
		// 期望控制器上报 Unknown 状态。
		{
			description: "Node created long time ago, without status. Node lease is missing.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedRequestCount: 2, // List 加 Update 两次请求
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodeMemoryPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodeDiskPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodePIDPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: fakeNow,
							},
						},
					},
				},
			},
			expectedPodStatusUpdate: false, // 节点从未就绪，Pod 也就从未被调度，因此不应有 Pod 状态更新。
		},
		// 节点创建时间久远且无状态，但租约最近续约过：处于监控宽限期内，期望不采取动作。
		// 处于监控宽限期内，期望控制器不采取任何动作。
		{
			description: "Node created long time ago, without status. Node lease is renewed recently.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease:                createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)),
			timeToPass:           time.Hour,
			newLease:             createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time.Add(time.Hour))), // 1 小时后租约续约。
			expectedRequestCount: 2,                                                                          // 两次 List 请求
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
				},
			},
			expectedPodStatusUpdate: false,
		},
		// 节点创建时间久远且无状态，租约已过期：期望控制器上报 Unknown。
		// 期望控制器上报 Unknown 状态。
		{
			description: "Node created long time ago, without status. Node lease is expired.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease:                createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)),
			timeToPass:           time.Hour,
			newLease:             createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)), // 1 小时后租约未续约。
			expectedRequestCount: 3,                                                           // 两次 List 加一次 Update 请求
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
							{
								Type:               v1.NodeMemoryPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
							{
								Type:               v1.NodeDiskPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
							{
								Type:               v1.NodePIDPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime,
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
						},
					},
				},
			},
			expectedPodStatusUpdate: false,
		},
		// kubelet 心跳已超过宽限期，但租约仍在续约：期望控制器不采取动作。
		// 处于监控宽限期内，期望控制器不采取任何动作。
		{
			description: "Node created long time ago, with status updated by kubelet exceeds grace period. Node lease is renewed.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionTrue,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
								{
									Type:               v1.NodeDiskPressure,
									Status:             v1.ConditionFalse,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease:                createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)),
			expectedRequestCount: 2, // 两次 List 请求
			timeToPass:           time.Hour,
			newNodeStatus: v1.NodeStatus{
				// 节点状态停留在 1 小时前，用于构造心跳超时场景。
				Conditions: []v1.NodeCondition{
					{
						Type:               v1.NodeReady,
						Status:             v1.ConditionTrue,
						LastHeartbeatTime:  fakeNow,
						LastTransitionTime: fakeNow,
					},
					{
						Type:               v1.NodeDiskPressure,
						Status:             v1.ConditionFalse,
						LastHeartbeatTime:  fakeNow,
						LastTransitionTime: fakeNow,
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			},
			newLease: createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time.Add(time.Hour))), // 1 小时后租约续约。
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionTrue,
								LastHeartbeatTime:  fakeNow,
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodeDiskPressure,
								Status:             v1.ConditionFalse,
								LastHeartbeatTime:  fakeNow,
								LastTransitionTime: fakeNow,
							},
						},
						Capacity: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
						},
					},
				},
			},
			expectedPodStatusUpdate: false,
		},
		// kubelet 心跳新鲜但租约已过期：处于监控宽限期内，期望控制器不采取动作。
		// 处于监控宽限期内，期望控制器不采取任何动作。
		{
			description: "Node created long time ago, with status updated by kubelet recently. Node lease is expired.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionTrue,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
								{
									Type:               v1.NodeDiskPressure,
									Status:             v1.ConditionFalse,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease:                createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)),
			expectedRequestCount: 2, // 两次 List 请求
			timeToPass:           time.Hour,
			newNodeStatus: v1.NodeStatus{
				// 1 小时后节点状态被更新，心跳时间随之推进。
				Conditions: []v1.NodeCondition{
					{
						Type:               v1.NodeReady,
						Status:             v1.ConditionTrue,
						LastHeartbeatTime:  metav1.Time{Time: fakeNow.Add(time.Hour)},
						LastTransitionTime: fakeNow,
					},
					{
						Type:               v1.NodeDiskPressure,
						Status:             v1.ConditionFalse,
						LastHeartbeatTime:  metav1.Time{Time: fakeNow.Add(time.Hour)},
						LastTransitionTime: fakeNow,
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			},
			newLease: createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)), // 1 小时后租约未续约。
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionTrue,
								LastHeartbeatTime:  metav1.Time{Time: fakeNow.Add(time.Hour)},
								LastTransitionTime: fakeNow,
							},
							{
								Type:               v1.NodeDiskPressure,
								Status:             v1.ConditionFalse,
								LastHeartbeatTime:  metav1.Time{Time: fakeNow.Add(time.Hour)},
								LastTransitionTime: fakeNow,
							},
						},
						Capacity: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
						},
					},
				},
			},
			expectedPodStatusUpdate: false,
		},
		// kubelet 心跳与租约均已超过宽限期：期望控制器上报 Unknown 并更新 Pod 状态。
		// 期望控制器上报 Unknown 状态。
		{
			description: "Node created long time ago, with status updated by kubelet exceeds grace period. Node lease is also expired.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionTrue,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease:                createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)),
			expectedRequestCount: 3, // 两次 List 加一次 Update 请求
			timeToPass:           time.Hour,
			newNodeStatus: v1.NodeStatus{
				// 节点状态停留在 1 小时前，用于构造心跳超时场景。
				Conditions: []v1.NodeCondition{
					{
						Type:               v1.NodeReady,
						Status:             v1.ConditionTrue,
						LastHeartbeatTime:  fakeNow,
						LastTransitionTime: fakeNow,
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			},
			newLease: createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)), // 1 小时后租约未续约。
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusUnknown",
								Message:            "Kubelet stopped posting node status.",
								LastHeartbeatTime:  fakeNow,
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
							{
								Type:               v1.NodeMemoryPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime, // 条件从未更新时默认取节点创建时间
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
							{
								Type:               v1.NodeDiskPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime, // 条件从未更新时默认取节点创建时间
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
							{
								Type:               v1.NodePIDPressure,
								Status:             v1.ConditionUnknown,
								Reason:             "NodeStatusNeverUpdated",
								Message:            "Kubelet never posted node status.",
								LastHeartbeatTime:  nodeCreationTime, // 条件从未更新时默认取节点创建时间
								LastTransitionTime: metav1.Time{Time: fakeNow.Add(time.Hour)},
							},
						},
						Capacity: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
						},
					},
				},
			},
			expectedPodStatusUpdate: true,
		},
		// kubelet 心跳已超过监控宽限期。
		// 租约在缓存中已过期，但 API Server 侧仍然新鲜。
		// 由于控制器会回源 API Server 二次确认，期望不采取任何动作。
		{
			description: "Node created long time ago, with status updated by kubelet exceeds grace period. Node lease is expired in cache, but fresh in API server.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionTrue,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease:      createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time)),
			timeToPass: time.Hour,
			newNodeStatus: v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{
						Type:               v1.NodeReady,
						Status:             v1.ConditionTrue,
						LastHeartbeatTime:  fakeNow,
						LastTransitionTime: fakeNow,
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			},
			newLease: func() *coordv1.Lease {
				l := createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time))
				l.ResourceVersion = "50"
				return l
			}(),
			apiServerLease: func() *coordv1.Lease {
				l := createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time))
				l.ResourceVersion = "50"
				return l
			}(),
			newAPIServerLease: func() *coordv1.Lease {
				l := createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time.Add(time.Hour)))
				l.ResourceVersion = "100"
				return l
			}(),
			expectedRequestCount: 2, // 两次 List 节点；GET lease 不计入 FakeNodeHandler.RequestCount。
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionTrue,
								LastHeartbeatTime:  fakeNow,
								LastTransitionTime: fakeNow,
							},
						},
						Capacity: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
						},
					},
				},
			},
			expectedPodStatusUpdate: false,
		},
		// 缓存中的租约过期但 API Server 侧新鲜：EnsureReady 在第二次检查时应跳过节点状态更新。
		{
			description: "Node lease expired in cache but fresh in API server. EnsureReady should skip on second run.",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: nodeCreationTime,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionTrue,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			lease: func() *coordv1.Lease {
				l := createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time.Add(-60*time.Second)))
				l.ResourceVersion = "50"
				return l
			}(),
			timeToPass: 1 * time.Second,
			newNodeStatus: v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{
						Type:               v1.NodeReady,
						Status:             v1.ConditionTrue,
						LastHeartbeatTime:  fakeNow,
						LastTransitionTime: fakeNow,
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			},
			newLease: func() *coordv1.Lease {
				l := createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time.Add(-60*time.Second)))
				l.ResourceVersion = "50"
				return l
			}(),
			apiServerLease: func() *coordv1.Lease {
				l := createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time))
				l.ResourceVersion = "100"
				return l
			}(),
			newAPIServerLease: func() *coordv1.Lease {
				l := createNodeLease("node0", metav1.NewMicroTime(fakeNow.Time))
				l.ResourceVersion = "100"
				return l
			}(),
			expectedRequestCount: 2, // 两次 List 节点；回源 GET lease 不计入 RequestCount。
			expectedNodes: []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "node0",
						CreationTimestamp: nodeCreationTime,
					},
					Status: v1.NodeStatus{
						Conditions: []v1.NodeCondition{
							{
								Type:               v1.NodeReady,
								Status:             v1.ConditionTrue,
								LastHeartbeatTime:  fakeNow,
								LastTransitionTime: fakeNow,
							},
						},
						Capacity: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
						},
					},
				},
			},
			expectedPodStatusUpdate: false,
		},
	}

	for _, item := range testcases {
		t.Run(item.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			nodeController, _ := newNodeLifecycleControllerFromClient(
				tCtx,
				item.fakeNodeHandler,
				testRateLimiterQPS,
				testRateLimiterQPS,
				testLargeClusterThreshold,
				testUnhealthyThreshold,
				testNodeMonitorGracePeriod,
				testNodeStartupGracePeriod,
				testNodeMonitorPeriod,
			)
			// 固定当前时间，让心跳新旧判断完全由 timeToPass 控制。
			nodeController.now = func() metav1.Time { return fakeNow }
			nodeController.recorder = testutil.NewFakeRecorder()
			nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(item.fakeNodeHandler.Clientset)
			if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 把租约写入 informer 缓存；断路器会先查这里判断心跳是否新鲜。
			if err := nodeController.syncLeaseStore(item.lease); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 在 fake client 中创建租约，供控制器缓存未命中时回源二次确认。
			if item.apiServerLease != nil {
				_, err := item.fakeNodeHandler.CoordinationV1().Leases(v1.NamespaceNodeLease).Create(tCtx, item.apiServerLease, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("unexpected error creating lease in fake client: %v", err)
				}
			}
			if err := nodeController.monitorNodeHealth(tCtx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 推进时钟后同时更新节点状态、缓存租约和服务端租约，模拟一次心跳周期后的变化。
			if item.timeToPass > 0 {
				nodeController.now = func() metav1.Time { return metav1.Time{Time: fakeNow.Add(item.timeToPass)} }
				item.fakeNodeHandler.Existing[0].Status = item.newNodeStatus
				if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				// 更新缓存中的租约，构造续约或过期的差异。
				if err := nodeController.syncLeaseStore(item.newLease); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				// 同步更新 API Server 侧租约，使双检结果可与缓存不同。
				if item.newAPIServerLease != nil {
					_, err := item.fakeNodeHandler.CoordinationV1().Leases(v1.NamespaceNodeLease).Update(tCtx, item.newAPIServerLease, metav1.UpdateOptions{})
					if err != nil {
						t.Fatalf("unexpected error updating lease in fake client: %v", err)
					}
				}
				if err := nodeController.monitorNodeHealth(tCtx); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if item.expectedRequestCount != item.fakeNodeHandler.RequestCount {
				t.Errorf("expected %v call, but got %v.", item.expectedRequestCount, item.fakeNodeHandler.RequestCount)
			}
			if len(item.fakeNodeHandler.UpdatedNodes) > 0 && !apiequality.Semantic.DeepEqual(item.expectedNodes, item.fakeNodeHandler.UpdatedNodes) {
				t.Errorf("unexpected nodes: %s", cmp.Diff(item.expectedNodes[0], item.fakeNodeHandler.UpdatedNodes[0]))
			}
			if len(item.fakeNodeHandler.UpdatedNodeStatuses) > 0 && !apiequality.Semantic.DeepEqual(item.expectedNodes, item.fakeNodeHandler.UpdatedNodeStatuses) {
				t.Errorf("unexpected nodes: %s", cmp.Diff(item.expectedNodes[0], item.fakeNodeHandler.UpdatedNodeStatuses[0]))
			}

			podStatusUpdated := false
			for _, action := range item.fakeNodeHandler.Actions() {
				if action.GetVerb() == "update" && action.GetResource().Resource == "pods" && action.GetSubresource() == "status" {
					podStatusUpdated = true
				}
			}
			if podStatusUpdated != item.expectedPodStatusUpdate {
				t.Errorf("expect pod status updated to be %v, but got %v", item.expectedPodStatusUpdate, podStatusUpdated)
			}
		})
	}
}

// TestMonitorNodeHealthMarkPodsNotReady 验证节点心跳超时后，控制器是否把节点上的 Pod 标记为 NotReady。
func TestMonitorNodeHealthMarkPodsNotReady(t *testing.T) {
	fakeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)
	table := []struct {
		// fakeNodeHandler 提供节点与 Pod 列表，并记录客户端请求。
		fakeNodeHandler *testutil.FakeNodeHandler
		// timeToPass 是第二次监控前推进的时钟时长。
		timeToPass time.Duration
		// newNodeStatus 是推进时钟后写入节点的新状态。
		newNodeStatus v1.NodeStatus
		// expectedPodStatusUpdate 表示是否期望更新 Pod 的 status 子资源。
		expectedPodStatusUpdate bool
	}{
		// 节点刚创建且没有状态：处于启动宽限期内，期望控制器不采取任何动作。
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: fakeNow,
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedPodStatusUpdate: false,
		},
		// 节点创建时间久远但心跳新鲜：处于监控宽限期内，期望控制器不采取任何动作。
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:   v1.NodeReady,
									Status: v1.ConditionTrue,
									// 心跳刚更新过，处于监控宽限期内，不应被判定为不健康。
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			expectedPodStatusUpdate: false,
		},
		// 节点心跳已超过宽限期：期望控制器更新 Pod 状态并把节点标记为 Unknown。
		// 节点心跳已超过宽限期：期望控制器更新 Pod 状态并将节点状态置为 Unknown。
		{
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:   v1.NodeReady,
									Status: v1.ConditionTrue,
									// 心跳时间停留在 1 小时前，推进时钟后即超过监控宽限期。
									LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
									LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
								},
							},
							Capacity: v1.ResourceList{
								v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
								v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			timeToPass: 1 * time.Minute,
			newNodeStatus: v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{
						Type:   v1.NodeReady,
						Status: v1.ConditionTrue,
						// 心跳时间停留在 1 小时前，推进时钟后即超过监控宽限期。
						LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			},
			expectedPodStatusUpdate: true,
		},
	}

	tCtx := ktesting.Init(t)
	for i, item := range table {
		// 用 fake 客户端查询节点上的 Pod，避免依赖真实 informer 缓存。
		nodeController, _ := newNodeLifecycleControllerFromClient(
			tCtx,
			item.fakeNodeHandler,
			testRateLimiterQPS,
			testRateLimiterQPS,
			testLargeClusterThreshold,
			testUnhealthyThreshold,
			testNodeMonitorGracePeriod,
			testNodeStartupGracePeriod,
			testNodeMonitorPeriod,
		)
		nodeController.now = func() metav1.Time { return fakeNow }
		nodeController.recorder = testutil.NewFakeRecorder()
		nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(item.fakeNodeHandler.Clientset)
		if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.monitorNodeHealth(tCtx); err != nil {
			t.Errorf("Case[%d] unexpected error: %v", i, err)
		}
		// 推进时钟并更新节点状态，触发第二次监控以覆盖心跳超时场景。
		if item.timeToPass > 0 {
			nodeController.now = func() metav1.Time { return metav1.Time{Time: fakeNow.Add(item.timeToPass)} }
			item.fakeNodeHandler.Existing[0].Status = item.newNodeStatus
			if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err := nodeController.monitorNodeHealth(tCtx); err != nil {
				t.Errorf("Case[%d] unexpected error: %v", i, err)
			}
		}

		// 检查是否产生了 Pod status 子资源更新动作。
		podStatusUpdated := false
		for _, action := range item.fakeNodeHandler.Actions() {
			if action.GetVerb() == "update" && action.GetResource().Resource == "pods" && action.GetSubresource() == "status" {
				podStatusUpdated = true
			}
		}
		if podStatusUpdated != item.expectedPodStatusUpdate {
			t.Errorf("Case[%d] expect pod status updated to be %v, but got %v", i, item.expectedPodStatusUpdate, podStatusUpdated)
		}
	}
}

// TestMonitorNodeHealthMarkPodsNotReadyWithWorkerSize 在大量节点/Pod（50 节点、每节点 100 Pod）下，
// 使用不同数量的 worker 验证标记 Pod NotReady 的正常路径，确保结果不依赖并发度。
func TestMonitorNodeHealthMarkPodsNotReadyWithWorkerSize(t *testing.T) {
	const numNodes = 50
	const podsPerNode = 100
	// makeNodes 构造心跳已超时的节点集合，用于触发 NotReady 标记。
	makeNodes := func() []*v1.Node {
		nodes := make([]*v1.Node, numNodes)
		// 节点心跳已超过宽限期：期望控制器更新 Pod 状态并将节点状态置为 Unknown。
		for i := 0; i < numNodes; i++ {
			nodes[i] = &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              fmt.Sprintf("node%d", i),
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:   v1.NodeReady,
							Status: v1.ConditionTrue,
							// 心跳时间停留在 1 小时前，推进时钟后即超过监控宽限期。
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
					Capacity: v1.ResourceList{
						v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
						v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
					},
				},
			}
		}
		return nodes
	}
	// makePods 为每个节点生成 podsPerNode 个 Pod，使总数量足以覆盖并发处理路径。
	makePods := func() []v1.Pod {
		pods := make([]v1.Pod, numNodes*podsPerNode)
		for i := 0; i < numNodes*podsPerNode; i++ {
			pods[i] = *testutil.NewPod(fmt.Sprintf("pod%d", i), fmt.Sprintf("node%d", i%numNodes))
		}
		return pods
	}

	table := []struct {
		// 用 0 表示采用默认 worker 数，1 表示显式单 worker，覆盖两种并发配置。
		workers int
	}{
		{workers: 0}, // 0 表示使用默认的 scheduler.UpdateWorkerSize
		{workers: 1},
	}

	tCtx := ktesting.Init(t)
	for i, item := range table {
		fakeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)

		fakeNodeHandler := &testutil.FakeNodeHandler{
			Existing:  makeNodes(),
			Clientset: fake.NewSimpleClientset(&v1.PodList{Items: makePods()}),
		}

		nodeController, _ := newNodeLifecycleControllerFromClient(
			tCtx,
			fakeNodeHandler,
			testRateLimiterQPS,
			testRateLimiterQPS,
			testLargeClusterThreshold,
			testUnhealthyThreshold,
			testNodeMonitorGracePeriod,
			testNodeStartupGracePeriod,
			testNodeMonitorPeriod)
		nodeController.now = func() metav1.Time { return fakeNow }
		nodeController.recorder = testutil.NewFakeRecorder()
		nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)
		// 仅在显式指定时覆盖默认 worker 数量。
		if item.workers != 0 {
			nodeController.nodeUpdateWorkerSize = item.workers
		}
		if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.monitorNodeHealth(tCtx); err != nil {
			t.Errorf("Case[%d] unexpected error: %v", i, err)
		}

		nodeController.now = func() metav1.Time { return metav1.Time{Time: fakeNow.Add(1 * time.Minute)} }
		for i := range fakeNodeHandler.Existing {
			fakeNodeHandler.Existing[i].Status = v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{
						Type:   v1.NodeReady,
						Status: v1.ConditionTrue,
						// 心跳时间停留在 1 小时前，推进时钟后即超过监控宽限期。
						LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
					},
				},
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse("10"),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("10G"),
				},
			}
		}

		if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.monitorNodeHealth(tCtx); err != nil {
			t.Errorf("Case[%d] unexpected error: %v", i, err)
		}

		podStatusUpdates := 0
		for _, action := range fakeNodeHandler.Actions() {
			if action.GetVerb() == "update" && action.GetResource().Resource == "pods" && action.GetSubresource() == "status" {
				podStatusUpdates++
			}
		}
		const expectedPodStatusUpdates = numNodes * podsPerNode
		if podStatusUpdates != expectedPodStatusUpdates {
			t.Errorf("Case[%d] expect pod status updated to be %v, but got %v", i, expectedPodStatusUpdates, podStatusUpdates)
		}
	}
}

// TestMonitorNodeHealthMarkPodsNotReadyRetry 验证当 Pod 状态更新或 Pod 列表查询失败时，
// 控制器能否在下一轮监控中重试并最终把 Pod 标记为 NotReady。
func TestMonitorNodeHealthMarkPodsNotReadyRetry(t *testing.T) {
	// nodeIteration 描述一轮监控所采用的时钟偏移与节点状态。
	type nodeIteration struct {
		// timeToPass 是第二次监控前推进的时钟时长。
		timeToPass time.Duration
		// newNodes 是本轮监控写入 fake 处理器的节点状态。
		newNodes []*v1.Node
	}
	timeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)
	timePlusTwoMinutes := metav1.Date(2015, 1, 1, 12, 0, 2, 0, time.UTC)
	makeNodes := func(status v1.ConditionStatus, lastHeartbeatTime, lastTransitionTime metav1.Time) []*v1.Node {
		return []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: timeNow,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             status,
							LastHeartbeatTime:  lastHeartbeatTime,
							LastTransitionTime: lastTransitionTime,
						},
					},
				},
			},
		}
	}
	table := []struct {
		// desc 描述用例场景，同时作为子测试名称。
		desc string
		// fakeNodeHandler 提供节点与 Pod 列表，并记录客户端请求。
		fakeNodeHandler *testutil.FakeNodeHandler
		// updateReactor 注入到 fake client，用于模拟 Pod 状态更新失败。
		updateReactor func(action testcore.Action) (bool, runtime.Object, error)
		// fakeGetPodsAssignedToNode 可被用例替换，以模拟查询 Pod 列表失败。
		fakeGetPodsAssignedToNode func(c *fake.Clientset) func(string) ([]*v1.Pod, error)
		// nodeIterations 定义多轮监控的时钟推进与节点状态变化。
		nodeIterations []nodeIteration
		// expectedPodStatusUpdates 是期望的 Pod 状态更新次数（含失败重试）。
		expectedPodStatusUpdates int
	}{
		// 节点心跳已超过宽限期：第一次监控会成功将 Pod 标记为 NotReady，第二次监控不应重复更新（无需重试）。
		{
			desc: "successful pod status update, no retry required",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			fakeGetPodsAssignedToNode: fakeGetPodsAssignedToNode,
			nodeIterations: []nodeIteration{
				{
					timeToPass: 0,
					newNodes:   makeNodes(v1.ConditionTrue, timeNow, timeNow),
				},
				{
					timeToPass: 1 * time.Minute,
					newNodes:   makeNodes(v1.ConditionTrue, timeNow, timeNow),
				},
				{
					timeToPass: 1 * time.Minute,
					newNodes:   makeNodes(v1.ConditionFalse, timePlusTwoMinutes, timePlusTwoMinutes),
				},
			},
			expectedPodStatusUpdates: 1,
		},
		// 第一次监控更新 Pod 状态失败，期望第二次监控重试并成功。
		{
			desc: "unsuccessful pod status update, retry required",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			updateReactor: func() func(action testcore.Action) (bool, runtime.Object, error) {
				i := 0
				return func(action testcore.Action) (bool, runtime.Object, error) {
					if action.GetVerb() == "update" && action.GetResource().Resource == "pods" && action.GetSubresource() == "status" {
						i++
						switch i {
						case 1:
							return true, nil, fmt.Errorf("fake error")
						default:
							return true, testutil.NewPod("pod0", "node0"), nil
						}
					}

					return true, nil, fmt.Errorf("unsupported action")
				}
			}(),
			fakeGetPodsAssignedToNode: fakeGetPodsAssignedToNode,
			nodeIterations: []nodeIteration{
				{
					timeToPass: 0,
					newNodes:   makeNodes(v1.ConditionTrue, timeNow, timeNow),
				},
				{
					timeToPass: 1 * time.Minute,
					newNodes:   makeNodes(v1.ConditionTrue, timeNow, timeNow),
				},
				{
					timeToPass: 1 * time.Minute,
					newNodes:   makeNodes(v1.ConditionFalse, timePlusTwoMinutes, timePlusTwoMinutes),
				},
			},
			expectedPodStatusUpdates: 2, // 一次失败加一次重试。
		},
		// 第一次监控查询 Pod 列表失败，期望第二次监控重试并成功更新状态。
		{
			desc: "unsuccessful pod list, retry required",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			fakeGetPodsAssignedToNode: func(c *fake.Clientset) func(string) ([]*v1.Pod, error) {
				i := 0
				f := fakeGetPodsAssignedToNode(c)
				return func(nodeName string) ([]*v1.Pod, error) {
					i++
					if i == 1 {
						return nil, fmt.Errorf("fake error")
					}
					return f(nodeName)
				}
			},
			nodeIterations: []nodeIteration{
				{
					timeToPass: 0,
					newNodes:   makeNodes(v1.ConditionTrue, timeNow, timeNow),
				},
				{
					timeToPass: 1 * time.Minute,
					newNodes:   makeNodes(v1.ConditionTrue, timeNow, timeNow),
				},
				{
					timeToPass: 1 * time.Minute,
					newNodes:   makeNodes(v1.ConditionFalse, timePlusTwoMinutes, timePlusTwoMinutes),
				},
			},
			expectedPodStatusUpdates: 1,
		},
	}

	for _, item := range table {
		t.Run(item.desc, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			nodeController, _ := newNodeLifecycleControllerFromClient(
				tCtx,
				item.fakeNodeHandler,
				testRateLimiterQPS,
				testRateLimiterQPS,
				testLargeClusterThreshold,
				testUnhealthyThreshold,
				testNodeMonitorGracePeriod,
				testNodeStartupGracePeriod,
				testNodeMonitorPeriod,
			)
			// 注入 reactor 以模拟更新 Pod 状态时的 API 错误。
			if item.updateReactor != nil {
				item.fakeNodeHandler.Clientset.PrependReactor("update", "pods", item.updateReactor)
			}
			nodeController.now = func() metav1.Time { return timeNow }
			nodeController.recorder = testutil.NewFakeRecorder()
			nodeController.getPodsAssignedToNode = item.fakeGetPodsAssignedToNode(item.fakeNodeHandler.Clientset)
			// 逐轮推进时钟并更新节点状态，观察重试行为。
			for _, itertion := range item.nodeIterations {
				nodeController.now = func() metav1.Time { return metav1.Time{Time: timeNow.Add(itertion.timeToPass)} }
				item.fakeNodeHandler.Existing = itertion.newNodes
				if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if err := nodeController.monitorNodeHealth(tCtx); err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}

			podStatusUpdates := 0
			for _, action := range item.fakeNodeHandler.Actions() {
				if action.GetVerb() == "update" && action.GetResource().Resource == "pods" && action.GetSubresource() == "status" {
					podStatusUpdates++
				}
			}
			if podStatusUpdates != item.expectedPodStatusUpdates {
				t.Errorf("expect pod status updated to happen %d times, but got %d", item.expectedPodStatusUpdates, podStatusUpdates)
			}
		})
	}
}

// TestApplyNoExecuteTaints 验证 NoExecute 污点能正确施加到节点上。
// 节点控制器只负责把节点放入污点队列，真正驱逐 Pod 的工作由 taint manager 完成。
func TestApplyNoExecuteTaints(t *testing.T) {
	// TODO: Remove skip once https://github.com/kubernetes/kubernetes/pull/114607 merges.
	if goruntime.GOOS == "windows" {
		t.Skip("Skipping test on Windows.")
	}
	fakeNow := metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)

	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			// 该节点应被施加 NoExecute 效果的 Unreachable 污点。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
			// 全部节点 NotReady 时控制器不会驱逐 Pod，因此需要一个健康节点作为对照。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node1",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			// 该节点应被施加 NoExecute 效果的 NotReady 污点。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node2",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
			// 若节点在控制器入队与 taint manager 处理之间将 NodeReady 条件移除，则不应施加 NotReady 污点。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node3",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2016, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2016, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}
	healthyNodeNewStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:               v1.NodeReady,
				Status:             v1.ConditionTrue,
				LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 10, 0, 0, time.UTC),
				LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	unhealthyNodeNewStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:               v1.NodeReady,
				Status:             v1.ConditionFalse,
				LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 10, 0, 0, time.UTC),
				LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	overrideNodeNewStatusConditions := []v1.NodeCondition{
		{
			Type:               "MemoryPressure",
			Status:             v1.ConditionUnknown,
			LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 10, 0, 0, time.UTC),
			LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}
	originalTaint := UnreachableTaintTemplate
	tCtx := ktesting.Init(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		tCtx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	nodeController.now = func() metav1.Time { return fakeNow }
	nodeController.recorder = testutil.NewFakeRecorder()
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(tCtx)
	node0, err := fakeNodeHandler.Get(tCtx, "node0", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node0...")
		return
	}
	if !taintutils.TaintExists(node0.Spec.Taints, UnreachableTaintTemplate) {
		t.Errorf("Can't find taint %v in %v", originalTaint, node0.Spec.Taints)
	}
	node2, err := fakeNodeHandler.Get(tCtx, "node2", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node2...")
		return
	}
	if !taintutils.TaintExists(node2.Spec.Taints, NotReadyTaintTemplate) {
		t.Errorf("Can't find taint %v in %v", NotReadyTaintTemplate, node2.Spec.Taints)
	}

	// 将 node2 恢复为健康状态，验证污点应被移除。
	node2.Status = healthyNodeNewStatus
	_, err = fakeNodeHandler.UpdateStatus(tCtx, node2, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(tCtx)

	node2, err = fakeNodeHandler.Get(tCtx, "node2", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node2...")
		return
	}
	// 节点不应残留任何污点，尤其是带有 NoExecute 效果的 NotReady 污点。
	if taintutils.TaintExists(node2.Spec.Taints, NotReadyTaintTemplate) || len(node2.Spec.Taints) > 0 {
		t.Errorf("Found taint %v in %v, which should not be present", NotReadyTaintTemplate, node2.Spec.Taints)
	}

	node3, err := fakeNodeHandler.Get(tCtx, "node3", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node3...")
		return
	}
	node3.Status = unhealthyNodeNewStatus
	_, err = fakeNodeHandler.UpdateStatus(tCtx, node3, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// 在 taint manager 处理前，用其他条件覆盖节点状态，验证污点判断基于当前条件而非入队时的快照。
	node3.Status.Conditions = overrideNodeNewStatusConditions
	_, err = fakeNodeHandler.UpdateStatus(tCtx, node3, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(tCtx)
	node3, err = fakeNodeHandler.Get(tCtx, "node3", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node3...")
		return
	}
	// 节点不应残留任何污点，尤其是带有 NoExecute 效果的 NotReady 污点。
	if taintutils.TaintExists(node3.Spec.Taints, NotReadyTaintTemplate) || len(node3.Spec.Taints) > 0 {
		t.Errorf("Found taint %v in %v, which should not be present", NotReadyTaintTemplate, node3.Spec.Taints)
	}
}

// TestApplyNoExecuteTaintsToUnreachableNode 验证超过 nodeMonitorGracePeriod 未上报状态的节点会被加上 NoExecute 污点，
// 并在恢复心跳后污点被移除。
func TestApplyNoExecuteTaintsToUnreachableNode(t *testing.T) {
	// TODO: Remove skip once https://github.com/kubernetes/kubernetes/pull/114607 merges.
	if goruntime.GOOS == "windows" {
		t.Skip("Skipping test on Windows.")
	}
	time1 := metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)
	// time2 恰好超过监控宽限期 1 秒，用于触发污点判定。
	time2 := metav1.Time{Time: time1.Add(testNodeMonitorGracePeriod + time.Second)}

	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			// 超过宽限期后，该节点应被加上 NoExecute 效果的 Unreachable 污点。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  time1,
							LastTransitionTime: time1,
						},
					},
				},
			},
			// 全部节点 NotReady 时控制器不会驱逐 Pod，因此需要一个健康节点作为对照。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node1",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  time1,
							LastTransitionTime: time1,
						},
					},
				},
			},
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}
	_, ctx := ktesting.NewTestContext(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		ctx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	// NodeReady 条件尚未超过宽限期时，不应施加任何污点。
	nodeController.now = func() metav1.Time { return time1 }
	nodeController.recorder = testutil.NewFakeRecorder()
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(ctx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(ctx)
	node0, err := fakeNodeHandler.Get(ctx, "node0", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node0...")
		return
	}
	if len(node0.Spec.Taints) > 0 {
		t.Errorf("Got unexpected taints %v", node0.Spec.Taints)
	}
	node1, err := fakeNodeHandler.Get(ctx, "node1", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node1...")
		return
	}
	if len(node1.Spec.Taints) > 0 {
		t.Errorf("Got unexpected taints %v", node1.Spec.Taints)
	}

	// 将时钟推进到 time2，使先前的 NodeReady 心跳过期。
	nodeController.now = func() metav1.Time { return time2 }
	// 仅 node1 在 time2 上报新心跳，作为未超时节点的对照。
	node1.Status = v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:               v1.NodeReady,
				Status:             v1.ConditionTrue,
				LastHeartbeatTime:  time2,
				LastTransitionTime: time1,
			},
		},
	}
	_, err = fakeNodeHandler.UpdateStatus(ctx, node1, metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	// node0 的心跳已超过宽限期应被污点化；node1 心跳新鲜则不应被污点化。
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(ctx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(ctx)

	node0, err = fakeNodeHandler.Get(ctx, "node0", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node0...")
		return
	}
	if !taintutils.TaintExists(node0.Spec.Taints, UnreachableTaintTemplate) {
		t.Errorf("Can't find taint %v in %v", UnreachableTaintTemplate, node0.Spec.Taints)
	}
	node1, err = fakeNodeHandler.Get(ctx, "node1", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node1...")
		return
	}
	if len(node1.Spec.Taints) > 0 {
		t.Errorf("Got unexpected taints %v", node1.Spec.Taints)
	}

	// node0 恢复心跳后，Unreachable 污点应被移除。
	node0.Status = v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:               v1.NodeReady,
				Status:             v1.ConditionTrue,
				LastHeartbeatTime:  time2,
				LastTransitionTime: time2,
			},
		},
	}
	_, err = fakeNodeHandler.UpdateStatus(ctx, node0, metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(ctx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(ctx)

	node0, err = fakeNodeHandler.Get(ctx, "node0", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node0...")
		return
	}
	// 节点不应残留任何污点。
	if len(node0.Spec.Taints) > 0 {
		t.Errorf("Got unexpected taints %v", node0.Spec.Taints)
	}
}

// TestApplyNoExecuteTaintsToNodesEnqueueTwice 验证节点即使被重复入队，仍能保证每个应被污点化的节点都带上 NoExecute 污点。
func TestApplyNoExecuteTaintsToNodesEnqueueTwice(t *testing.T) {
	// TODO: Remove skip once https://github.com/kubernetes/kubernetes/pull/114607 merges.
	if goruntime.GOOS == "windows" {
		t.Skip("Skipping test on Windows.")
	}
	fakeNow := metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)

	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			// 该节点应被施加 NoExecute 效果的 Unreachable 污点。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
			// 全部节点 NotReady 时控制器不会驱逐 Pod，因此需要一个健康节点作为对照。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node1",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			// 该节点应被施加 NoExecute 效果的 NotReady 污点。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node2",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}
	healthyNodeNewStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:               v1.NodeReady,
				Status:             v1.ConditionTrue,
				LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 10, 0, 0, time.UTC),
				LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	tCtx := ktesting.Init(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		tCtx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	nodeController.now = func() metav1.Time { return fakeNow }
	nodeController.recorder = testutil.NewFakeRecorder()
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// 1. 连续两次监控，验证节点只被加入队列一次。
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 2. 将 node0 标记为健康。
	node0, err := fakeNodeHandler.Get(tCtx, "node0", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node0...")
		return
	}
	node0.Status = healthyNodeNewStatus
	_, err = fakeNodeHandler.UpdateStatus(tCtx, node0, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err.Error())
		return
	}

	// 追加其他不健康节点，扩大重复入队的规模。
	fakeNodeHandler.Existing = append(fakeNodeHandler.Existing, []*v1.Node{
		// 该节点应被施加 NoExecute 效果的 Unreachable 污点。
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "node3",
				CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				Labels: map[string]string{
					v1.LabelTopologyRegion:          "region1",
					v1.LabelTopologyZone:            "zone1",
					v1.LabelFailureDomainBetaRegion: "region1",
					v1.LabelFailureDomainBetaZone:   "zone1",
				},
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
		// 全部节点 NotReady 时控制器不会驱逐 Pod，因此需要一个健康节点作为对照。
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "node4",
				CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				Labels: map[string]string{
					v1.LabelTopologyRegion:          "region1",
					v1.LabelTopologyZone:            "zone1",
					v1.LabelFailureDomainBetaRegion: "region1",
					v1.LabelFailureDomainBetaZone:   "zone1",
				},
			},
			Status: v1.NodeStatus{
				Conditions: []v1.NodeCondition{
					{
						Type:               v1.NodeReady,
						Status:             v1.ConditionTrue,
						LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
						LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		// 该节点应被施加 NoExecute 效果的 NotReady 污点。
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "node5",
				CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				Labels: map[string]string{
					v1.LabelTopologyRegion:          "region1",
					v1.LabelTopologyZone:            "zone1",
					v1.LabelFailureDomainBetaRegion: "region1",
					v1.LabelFailureDomainBetaZone:   "zone1",
				},
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
	}...)
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// 3. 再次监控，构造 UniqueQueue 中出现重复节点的情况。
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 4. 执行 NoExecute 污点处理：node0 已恢复 Ready，应在默认分支返回并同时清理集合与队列中的值，避免任务卡住。
	nodeController.doNoExecuteTaintingPass(tCtx)

	// 5. 检查 node3 和 node5 是否都已正确获得 NoExecute 污点。
	node3, err := fakeNodeHandler.Get(tCtx, "node3", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node3...")
		return
	}
	if !taintutils.TaintExists(node3.Spec.Taints, UnreachableTaintTemplate) || len(node3.Spec.Taints) == 0 {
		t.Errorf("Not found taint %v in %v, which should be present in %s", UnreachableTaintTemplate, node3.Spec.Taints, node3.Name)
	}
	node5, err := fakeNodeHandler.Get(tCtx, "node5", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node5...")
		return
	}
	if !taintutils.TaintExists(node5.Spec.Taints, NotReadyTaintTemplate) || len(node5.Spec.Taints) == 0 {
		t.Errorf("Not found taint %v in %v, which should be present in %s", NotReadyTaintTemplate, node5.Spec.Taints, node5.Name)
	}
}

// TestSwapUnreachableNotReadyTaints 验证节点从未上报状态（Unreachable）转为明确 NotReady 时，
// 污点会从 UnreachableTaintTemplate 替换为 NotReadyTaintTemplate。
func TestSwapUnreachableNotReadyTaints(t *testing.T) {
	fakeNow := metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)

	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
			// 全部节点 NotReady 时控制器不会驱逐 Pod，因此需要一个健康节点作为对照，并在后续更新它的状态。
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node1",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}
	newNodeStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:   v1.NodeReady,
				Status: v1.ConditionFalse,
				// 心跳刚更新且已连续 10 分钟处于 NotReady，触发污点替换。
				LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 9, 0, 0, time.UTC),
				LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	healthyNodeNewStatus := v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:               v1.NodeReady,
				Status:             v1.ConditionTrue,
				LastHeartbeatTime:  metav1.Date(2017, 1, 1, 12, 10, 0, 0, time.UTC),
				LastTransitionTime: metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	originalTaint := UnreachableTaintTemplate
	updatedTaint := NotReadyTaintTemplate

	tCtx := ktesting.Init(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		tCtx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	nodeController.now = func() metav1.Time { return fakeNow }
	nodeController.recorder = testutil.NewFakeRecorder()
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)
	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(tCtx)

	node0, err := fakeNodeHandler.Get(tCtx, "node0", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node0...")
		return
	}
	node1, err := fakeNodeHandler.Get(tCtx, "node1", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node1...")
		return
	}

	if originalTaint != nil && !taintutils.TaintExists(node0.Spec.Taints, originalTaint) {
		t.Errorf("Can't find taint %v in %v", originalTaint, node0.Spec.Taints)
	}

	nodeController.now = func() metav1.Time { return metav1.Time{Time: fakeNow.Time} }

	node0.Status = newNodeStatus
	node1.Status = healthyNodeNewStatus
	_, err = fakeNodeHandler.UpdateStatus(tCtx, node0, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err.Error())
		return
	}
	_, err = fakeNodeHandler.UpdateStatus(tCtx, node1, metav1.UpdateOptions{})
	if err != nil {
		t.Error(err.Error())
		return
	}

	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	nodeController.doNoExecuteTaintingPass(tCtx)

	node0, err = fakeNodeHandler.Get(tCtx, "node0", metav1.GetOptions{})
	if err != nil {
		t.Errorf("Can't get current node0...")
		return
	}
	if updatedTaint != nil {
		if !taintutils.TaintExists(node0.Spec.Taints, updatedTaint) {
			t.Errorf("Can't find taint %v in %v", updatedTaint, node0.Spec.Taints)
		}
	}
}

// TestTaintsNodeByCondition 验证根据节点条件（NetworkUnavailable/Ready=False/Ready=Unknown）
// 施加对应 NoSchedule 污点的逻辑。
func TestTaintsNodeByCondition(t *testing.T) {
	fakeNow := metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)

	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}

	tCtx := ktesting.Init(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		tCtx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	nodeController.now = func() metav1.Time { return fakeNow }
	nodeController.recorder = testutil.NewFakeRecorder()
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)

	networkUnavailableTaint := &v1.Taint{
		Key:    v1.TaintNodeNetworkUnavailable,
		Effect: v1.TaintEffectNoSchedule,
	}
	notReadyTaint := &v1.Taint{
		Key:    v1.TaintNodeNotReady,
		Effect: v1.TaintEffectNoSchedule,
	}
	unreachableTaint := &v1.Taint{
		Key:    v1.TaintNodeUnreachable,
		Effect: v1.TaintEffectNoSchedule,
	}

	tests := []struct {
		// Name 描述用例场景。
		Name string
		// Node 是待处理的节点及其条件。
		Node *v1.Node
		// ExpectedTaints 是期望施加到节点上的污点集合。
		ExpectedTaints []*v1.Taint
	}{
		{
			Name: "NetworkUnavailable is true",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
						{
							Type:               v1.NodeNetworkUnavailable,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			ExpectedTaints: []*v1.Taint{networkUnavailableTaint},
		},
		{
			Name: "NetworkUnavailable is true",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
						{
							Type:               v1.NodeNetworkUnavailable,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC),
						},
					},
				},
			},
			ExpectedTaints: []*v1.Taint{networkUnavailableTaint},
		},
		{
			Name: "Ready is false",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
			ExpectedTaints: []*v1.Taint{notReadyTaint},
		},
		{
			Name: "Ready is unknown",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
			ExpectedTaints: []*v1.Taint{unreachableTaint},
		},
	}

	// 逐用例更新节点、执行污点处理并校验结果。
	for _, test := range tests {
		if _, err := fakeNodeHandler.Update(tCtx, test.Node, metav1.UpdateOptions{}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.doNoScheduleTaintingPass(tCtx, test.Node.Name); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		node0, err := nodeController.nodeLister.Get("node0")
		if err != nil {
			t.Errorf("Can't get current node0...")
			return
		}
		if len(node0.Spec.Taints) != len(test.ExpectedTaints) {
			t.Errorf("%s: Unexpected number of taints: expected %d, got %d",
				test.Name, len(test.ExpectedTaints), len(node0.Spec.Taints))
		}
		for _, taint := range test.ExpectedTaints {
			if !taintutils.TaintExists(node0.Spec.Taints, taint) {
				t.Errorf("%s: Can't find taint %v in %v", test.Name, taint, node0.Spec.Taints)
			}
		}
	}
}

// TestNodeEventGeneration 验证新节点注册时生成的 RegisteredNode 事件，
// 且事件的 InvolvedObject 携带正确的节点 UID。
func TestNodeEventGeneration(t *testing.T) {
	fakeNow := metav1.Date(2016, 9, 10, 12, 0, 0, 0, time.UTC)
	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					UID:               "1234567890",
					CreationTimestamp: metav1.Date(2015, 8, 10, 0, 0, 0, 0, time.UTC),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  metav1.Date(2015, 8, 10, 0, 0, 0, 0, time.UTC),
							LastTransitionTime: metav1.Date(2015, 8, 10, 0, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}

	tCtx := ktesting.Init(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		tCtx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	nodeController.now = func() metav1.Time { return fakeNow }
	// 用 FakeRecorder 捕获控制器产生的事件，便于断言事件原因与对象 UID。
	fakeRecorder := testutil.NewFakeRecorder()
	nodeController.recorder = fakeRecorder
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)

	if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := nodeController.monitorNodeHealth(tCtx); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(fakeRecorder.Events) != 1 {
		t.Fatalf("unexpected events, got %v, expected %v: %+v", len(fakeRecorder.Events), 1, fakeRecorder.Events)
	}
	if fakeRecorder.Events[0].Reason != "RegisteredNode" {
		var reasons []string
		for _, event := range fakeRecorder.Events {
			reasons = append(reasons, event.Reason)
		}
		t.Fatalf("unexpected events generation: %v", strings.Join(reasons, ","))
	}
	for _, event := range fakeRecorder.Events {
		involvedObject := event.InvolvedObject
		actualUID := string(involvedObject.UID)
		if actualUID != "1234567890" {
			t.Fatalf("unexpected event uid: %v", actualUID)
		}
	}
}

// TestReconcileNodeLabels 验证稳定标签（stable）与 beta 标签（OS/arch）之间的一致性调和：
// 缺失时补齐，冲突时以稳定标签为准。
func TestReconcileNodeLabels(t *testing.T) {
	fakeNow := metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)

	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion:          "region1",
						v1.LabelTopologyZone:            "zone1",
						v1.LabelFailureDomainBetaRegion: "region1",
						v1.LabelFailureDomainBetaZone:   "zone1",
					},
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
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}

	tCtx := ktesting.Init(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		tCtx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	nodeController.now = func() metav1.Time { return fakeNow }
	nodeController.recorder = testutil.NewFakeRecorder()
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)

	tests := []struct {
		// Name 描述用例场景。
		Name string
		// Node 是待处理的节点及其条件。
		Node *v1.Node
		// ExpectedLabels 是调和后期望的节点标签。
		ExpectedLabels map[string]string
	}{
		{
			Name: "No-op if node has no labels",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			ExpectedLabels: nil,
		},
		{
			Name: "No-op if no target labels present",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelTopologyRegion: "region1",
					},
				},
			},
			ExpectedLabels: map[string]string{
				v1.LabelTopologyRegion: "region1",
			},
		},
		{
			Name: "Create OS/arch beta labels when they don't exist",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						v1.LabelOSStable:   "linux",
						v1.LabelArchStable: "amd64",
					},
				},
			},
			ExpectedLabels: map[string]string{
				kubeletapis.LabelOS:   "linux",
				kubeletapis.LabelArch: "amd64",
				v1.LabelOSStable:      "linux",
				v1.LabelArchStable:    "amd64",
			},
		},
		{
			Name: "Reconcile OS/arch beta labels to match stable labels",
			Node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						kubeletapis.LabelOS:   "windows",
						kubeletapis.LabelArch: "arm",
						v1.LabelOSStable:      "linux",
						v1.LabelArchStable:    "amd64",
					},
				},
			},
			ExpectedLabels: map[string]string{
				kubeletapis.LabelOS:   "linux",
				kubeletapis.LabelArch: "amd64",
				v1.LabelOSStable:      "linux",
				v1.LabelArchStable:    "amd64",
			},
		},
	}

	for _, test := range tests {
		if _, err := fakeNodeHandler.Update(tCtx, test.Node, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := nodeController.reconcileNodeLabels(tCtx, test.Node.Name); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := nodeController.syncNodeStore(fakeNodeHandler); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		node0, err := nodeController.nodeLister.Get("node0")
		if err != nil {
			t.Fatalf("Can't get current node0...")
		}
		if len(node0.Labels) != len(test.ExpectedLabels) {
			t.Errorf("%s: Unexpected number of taints: expected %d, got %d",
				test.Name, len(test.ExpectedLabels), len(node0.Labels))
		}
		for key, expectedValue := range test.ExpectedLabels {
			actualValue, ok := node0.Labels[key]
			if !ok {
				t.Errorf("%s: Can't find label %v in %v", test.Name, key, node0.Labels)
			}
			if actualValue != expectedValue {
				t.Errorf("%s: label %q: expected value %q, got value %q", test.Name, key, expectedValue, actualValue)
			}
		}
	}
}

// TestTryUpdateNodeHealth 验证在节点心跳新鲜或超过宽限期时，tryUpdateNodeHealth 对 NodeReady 状态的推导，
// 并保证返回的状态与 nodeHealthMap 中保存的一致。
func TestTryUpdateNodeHealth(t *testing.T) {
	fakeNow := metav1.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)
	fakeOld := metav1.Date(2016, 1, 1, 12, 0, 0, 0, time.UTC)

	fakeNodeHandler := &testutil.FakeNodeHandler{
		Existing: []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeNow,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  fakeNow,
							LastTransitionTime: fakeNow,
						},
					},
				},
			},
		},
		Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
	}

	tCtx := ktesting.Init(t)
	nodeController, _ := newNodeLifecycleControllerFromClient(
		tCtx,
		fakeNodeHandler,
		testRateLimiterQPS,
		testRateLimiterQPS,
		testLargeClusterThreshold,
		testUnhealthyThreshold,
		testNodeMonitorGracePeriod,
		testNodeStartupGracePeriod,
		testNodeMonitorPeriod,
	)
	nodeController.now = func() metav1.Time { return fakeNow }
	nodeController.recorder = testutil.NewFakeRecorder()
	nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(fakeNodeHandler.Clientset)

	// getStatus 安全地从条件指针中取出状态值，条件不存在时返回 nil。
	getStatus := func(cond *v1.NodeCondition) *v1.ConditionStatus {
		if cond == nil {
			return nil
		}
		return &cond.Status
	}

	tests := []struct {
		// name 描述用例场景，同时作为子测试名称。
		name string
		// node 是包含初始状态与时间戳的测试节点。
		node *v1.Node
	}{
		{
			name: "Status true",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeNow,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  fakeNow,
							LastTransitionTime: fakeNow,
						},
					},
				},
			},
		},
		{
			name: "Status false",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeNow,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  fakeNow,
							LastTransitionTime: fakeNow,
						},
					},
				},
			},
		},
		{
			name: "Status unknown",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeNow,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  fakeNow,
							LastTransitionTime: fakeNow,
						},
					},
				},
			},
		},
		{
			name: "Status nil",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeNow,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{},
				},
			},
		},
		{
			name: "Status true - after grace period",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeOld,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionTrue,
							LastHeartbeatTime:  fakeOld,
							LastTransitionTime: fakeOld,
						},
					},
				},
			},
		},
		{
			name: "Status false - after grace period",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeOld,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionFalse,
							LastHeartbeatTime:  fakeOld,
							LastTransitionTime: fakeOld,
						},
					},
				},
			},
		},
		{
			name: "Status unknown - after grace period",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeOld,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:               v1.NodeReady,
							Status:             v1.ConditionUnknown,
							LastHeartbeatTime:  fakeOld,
							LastTransitionTime: fakeOld,
						},
					},
				},
			},
		},
		{
			name: "Status nil - after grace period",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: fakeOld,
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 直接将节点状态写入健康缓存，跳过 monitorNodeHealth 的前置流程。
			nodeController.nodeHealthMap.set(test.node.Name, &nodeHealthData{
				status:                   &test.node.Status,
				probeTimestamp:           test.node.CreationTimestamp,
				readyTransitionTimestamp: test.node.CreationTimestamp,
			})
			_, _, currentReadyCondition, err := nodeController.tryUpdateNodeHealth(tCtx, test.node)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验返回状态与缓存中保存的状态一致，防止两者出现偏差。
			_, savedReadyCondition := controllerutil.GetNodeCondition(nodeController.nodeHealthMap.getDeepCopy(test.node.Name).status, v1.NodeReady)
			savedStatus := getStatus(savedReadyCondition)
			currentStatus := getStatus(currentReadyCondition)
			if !apiequality.Semantic.DeepEqual(currentStatus, savedStatus) {
				t.Errorf("expected %v, got %v", savedStatus, currentStatus)
			}
		})
	}
}

// Test_isNodeExcludedFromDisruptionChecks 验证带有排除中断标签的节点会被识别，
// 普通节点（包括名字以 master 开头的节点）不会被误判。
func Test_isNodeExcludedFromDisruptionChecks(t *testing.T) {
	validNodeStatus := v1.NodeStatus{Conditions: []v1.NodeCondition{{Type: "Test"}}}
	tests := []struct {
		// name 描述用例场景，同时作为子测试名称。
		name string

		// input 是待判定的节点。
		input *v1.Node
		// want 是期望的判定结果。
		want bool
	}{
		{want: false, input: &v1.Node{Status: validNodeStatus, ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}},
		{want: false, input: &v1.Node{Status: validNodeStatus, ObjectMeta: metav1.ObjectMeta{Name: "master-abc"}}},
		{want: true, input: &v1.Node{Status: validNodeStatus, ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelNodeDisruptionExclusion: ""}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := isNodeExcludedFromDisruptionChecks(tt.input); result != tt.want {
				t.Errorf("isNodeExcludedFromDisruptionChecks() = %v, want %v", result, tt.want)
			}
		})
	}
}

// TestProcessPodMarkPodNotReady 验证 processPod 仅在节点健康状态已收集且节点不健康时，
// 才把 Pod 标记为 NotReady。
func TestProcessPodMarkPodNotReady(t *testing.T) {
	fakeNow := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)

	table := []struct {
		// desc 描述用例场景，同时作为子测试名称。
		desc string
		// fakeNodeHandler 提供节点与 Pod 列表，并记录客户端请求。
		fakeNodeHandler *testutil.FakeNodeHandler
		// pod 是待处理的 Pod。
		pod *v1.Pod
		// expectedPodStatusUpdate 表示是否期望更新 Pod 的 status 子资源。
		expectedPodStatusUpdate bool
		// monitorNodeHealth 表示是否先执行一次监控以收集节点健康状态。
		monitorNodeHealth bool
	}{
		{
			desc: "Do not mark pod as NotReady when the scheduled node's healthy is not gathered yet",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: fakeNow,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionFalse,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			pod:                     testutil.NewPod("pod0", "node0"),
			monitorNodeHealth:       false,
			expectedPodStatusUpdate: false,
		},
		{
			desc: "Do not mark pod as NotReady when the scheduled node is ready",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: fakeNow,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionTrue,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			pod:                     testutil.NewPod("pod0", "node0"),
			monitorNodeHealth:       true,
			expectedPodStatusUpdate: false,
		},
		{
			desc: "Pod marked as NotReady when the scheduled node is not ready",
			fakeNodeHandler: &testutil.FakeNodeHandler{
				Existing: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "node0",
							CreationTimestamp: fakeNow,
						},
						Status: v1.NodeStatus{
							Conditions: []v1.NodeCondition{
								{
									Type:               v1.NodeReady,
									Status:             v1.ConditionFalse,
									LastHeartbeatTime:  fakeNow,
									LastTransitionTime: fakeNow,
								},
							},
						},
					},
				},
				Clientset: fake.NewSimpleClientset(&v1.PodList{Items: []v1.Pod{*testutil.NewPod("pod0", "node0")}}),
			},
			pod:                     testutil.NewPod("pod0", "node0"),
			monitorNodeHealth:       true,
			expectedPodStatusUpdate: true,
		},
	}

	for _, item := range table {
		t.Run(item.desc, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			nodeController, _ := newNodeLifecycleControllerFromClient(
				tCtx,
				item.fakeNodeHandler,
				testRateLimiterQPS,
				testRateLimiterQPS,
				testLargeClusterThreshold,
				testUnhealthyThreshold,
				testNodeMonitorGracePeriod,
				testNodeStartupGracePeriod,
				testNodeMonitorPeriod,
			)
			nodeController.now = func() metav1.Time { return fakeNow }
			nodeController.recorder = testutil.NewFakeRecorder()
			nodeController.getPodsAssignedToNode = fakeGetPodsAssignedToNode(item.fakeNodeHandler.Clientset)
			if err := nodeController.syncNodeStore(item.fakeNodeHandler); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// 某些场景需要先收集节点健康状态，否则 processPod 不应做标记。
			if item.monitorNodeHealth {
				if err := nodeController.monitorNodeHealth(tCtx); err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}

			if err := nodeController.syncPodStore(item.pod); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// 模拟 Pod 更新事件进入队列。
			nodeController.podUpdated(nil, item.pod)
			// 执行待测试的 Pod 处理逻辑。
			nodeController.processPod(tCtx, podUpdateItem{name: item.pod.Name, namespace: item.pod.Namespace})

			podStatusUpdated := false
			for _, action := range item.fakeNodeHandler.Actions() {
				if action.GetVerb() == "update" && action.GetResource().Resource == "pods" && action.GetSubresource() == "status" {
					podStatusUpdated = true
				}
			}
			if podStatusUpdated != item.expectedPodStatusUpdate {
				t.Errorf("expect pod status updated to be %v, but got %v", item.expectedPodStatusUpdate, podStatusUpdated)
			}
		})
	}
}
