/*
Copyright 2022 The Kubernetes Authors.

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
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	goruntime "runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	cgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/draoptionalnodeoperations"
	"k8s.io/dynamic-resource-allocation/deviceclass/extendedresourcecache"
	resourceslicetracker "k8s.io/dynamic-resource-allocation/resourceslice/tracker"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/dynamic-resource-allocation/structured/schedulerapi"
	"k8s.io/klog/v2"
	kubeschedulerconfigv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	configv1 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

func init() {
	metrics.InitMetrics()
}

// TestPostFilterNoOpStatusHasNoUserVisibleReason 验证 PostFilter 无操作路径返回的状态
// 不带用户可见的 Reason：内部 no-op 不应污染 API 返回的原因列表。
func TestPostFilterNoOpStatusHasNoUserVisibleReason(t *testing.T) {
	pod := st.MakePod().Name("pod").Namespace("default").Obj()

	t.Run("plugin disabled", func(t *testing.T) {
		_, status := (&DynamicResources{}).PostFilter(context.Background(), framework.NewCycleState(), pod, nil)

		require.Equal(t, fwk.Unschedulable, status.Code())
		assert.Empty(t, status.Reasons())
	})

	t.Run("no claims", func(t *testing.T) {
		state := framework.NewCycleState()
		state.Write(stateKey, &stateData{})

		_, status := (&DynamicResources{enabled: true}).PostFilter(context.Background(), state, pod, nil)

		require.Equal(t, fwk.Unschedulable, status.Code())
		assert.Empty(t, status.Reasons())
	})
}

// 测试中复用的对象类型、名称及预设对象。
// 大量用例共享这些变量，便于通过 DeepCopy 做局部修改后比较。
var (
	// Pod 与 PodGroup 的 GVK，用于构造消费者引用。
	podKind      = v1.SchemeGroupVersion.WithKind("Pod")
	podGroupKind = schedulingapi.SchemeGroupVersion.WithKind("PodGroup")

	nodeName                            = "worker"
	node2Name                           = "worker-2"
	node3Name                           = "worker-3"
	driver                              = "some-driver"
	driver2                             = "some-driver-2"
	sharedDeviceName                    = "shared-instance"
	podName                             = "my-pod"
	podUID                              = "1234"
	podGroupName                        = "my-podgroup"
	podGroupUID                         = "5678"
	resourceName                        = "my-resource"
	resourceName2                       = resourceName + "-2"
	capacityName                        = resourceapi.QualifiedName("my-cap")
	claimName                           = podName + "-" + resourceName
	claimName2                          = podName + "-" + resourceName2
	className                           = "my-resource-class"
	namespace                           = "default"
	attrName                            = resourceapi.QualifiedName("healthy") // 仅非默认节点具备该设备属性
	extendedResourceName                = "example.com/gpu"
	extendedResourceName2               = "example.com/gpu2"
	implicitExtendedResourceName        = "deviceclass.resource.kubernetes.io/my-resource-class"
	nodeAllocatableResourceDriver       = "node-allocatable-resource-driver"
	nodeAllocatableResourceCapacityName = resourceapi.QualifiedName("example.com/cpus")

	deviceClass = &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
	}
	deviceClassWithExtendResourceName = &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: &extendedResourceName,
		},
	}
	deviceClassWithExtendResourceName2 = &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className + "2",
		},
		Spec: resourceapi.DeviceClassSpec{
			ExtendedResourceName: &extendedResourceName2,
		},
	}
	podWithClaimName = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
				Obj()
	podWithClaimTemplate = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimTemplateName: &claimName}).
				Obj()
	podWithClaimTemplateInStatus = func() *v1.Pod {
		pod := podWithClaimTemplate.DeepCopy()
		pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
			{
				Name:              pod.Spec.ResourceClaims[0].Name,
				ResourceClaimName: &claimName,
			},
		}
		return pod
	}()
	podWithTwoClaimTemplates = st.MakePod().Name(podName).Namespace(namespace).
					UID(podUID).
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimTemplateName: &claimName}).
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName2, ResourceClaimTemplateName: &claimName}).
					Obj()
	podWithTwoClaimNames = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName2, ResourceClaimName: &claimName2}).
				Obj()
	podWithExtendedResourceName = st.MakePod().Name(podName).Namespace(namespace).
					UID(podUID).
					Req(map[v1.ResourceName]string{
			v1.ResourceName(extendedResourceName): "1",
		}).
		Obj()
	podWithExtendedResourceName2 = st.MakePod().Name(podName).Namespace(namespace).
					UID(podUID).
					Req(map[v1.ResourceName]string{
			v1.ResourceName(extendedResourceName):  "1",
			v1.ResourceName(extendedResourceName2): "1",
		}).
		Obj()
	podWithImplicitExtendedResourceName = st.MakePod().Name(podName).Namespace(namespace).
						UID(podUID).
						Req(map[v1.ResourceName]string{
			v1.ResourceName(implicitExtendedResourceName): "1",
			v1.ResourceName(extendedResourceName):         "2",
		}).
		Obj()
	podWithImplicitExtendedResourceNameTwoContainers = st.MakePod().Name(podName).Namespace(namespace).
								UID(podUID).
								Req(map[v1.ResourceName]string{
			v1.ResourceName(implicitExtendedResourceName): "1",
		}).
		Req(map[v1.ResourceName]string{
			v1.ResourceName(extendedResourceName): "2",
		}).
		Obj()
	podWithClaimReferenceInContainer = func() *v1.Pod {
		pod := podWithClaimName.DeepCopy()
		container := v1.Container{
			Name: "c1",
			Resources: v1.ResourceRequirements{
				Claims: []v1.ResourceClaim{
					{
						Name: resourceName,
					},
				},
			},
		}
		pod.Spec.Containers = append(pod.Spec.Containers, container)
		return pod
	}()
	groupedPodWithClaimName = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
				PodGroupName(podGroupName).
				Obj()
	groupedPodWithClaimName2 = st.MakePod().Name(podName + "-2").Namespace(namespace).
					UID(podUID + "-2").
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
					PodGroupName(podGroupName).
					Obj()
	groupedPodWithPodLevelClaim = st.MakePod().Name(podName + "-podlevel").Namespace(namespace).
					UID(podUID + "-podlevel").
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName2, ResourceClaimName: &claimName2}).
					PodGroupName(podGroupName).
					Obj()
	groupedPodWithClaimTemplateInStatus = func() *v1.Pod {
		pod := podWithClaimTemplateInStatus.DeepCopy()
		pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
			PodGroupName: new(podGroupName),
		}
		return pod
	}()
	podGroupReservation = resourceapi.ResourceClaimConsumerReference{
		APIGroup: schedulingapi.GroupName,
		Resource: "podgroups",
		Name:     podGroupName,
		UID:      types.UID(podGroupUID),
	}

	podGroupWithClaimName = st.MakePodGroup().Name(podGroupName).Namespace(namespace).
				UID(types.UID(podGroupUID)).
				ResourceClaims(schedulingapi.PodGroupResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
				Obj()
	podGroupWithClaimNameAndConstraints = st.MakePodGroup().Name(podGroupName).Namespace(namespace).
						UID(types.UID(podGroupUID)).
						ResourceClaims(schedulingapi.PodGroupResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
						TopologyKey("kubernetes.io/hostname").
						Obj()
	podGroupWithClaimTemplate = st.MakePodGroup().Name(podGroupName).Namespace(namespace).
					UID(types.UID(podGroupUID)).
					ResourceClaims(schedulingapi.PodGroupResourceClaim{Name: resourceName, ResourceClaimTemplateName: &claimName}).
					Obj()

	// 带 "instance-1" 设备且无设备属性的节点
	workerNode                                 = &st.MakeNode().Name(nodeName).Label("kubernetes.io/hostname", nodeName).Node
	workerNodeSlice                            = st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
	largeWorkerNodeSlice                       = st.MakeResourceSlice(nodeName, driver).Device("instance-1").Device("instance-2").Device("instance-3").Device("instance-4").Obj()
	workerNodeWithCapacity                     = &st.MakeNode().Name(nodeName).Label("kubernetes.io/hostname", nodeName).Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "10", v1.ResourceMemory: "10Gi"}).Node
	workerNodeSliceWithNodeAllocatableResource = func() *resourceapi.ResourceSlice {
		nativeSlice := st.MakeResourceSlice(nodeName, nodeAllocatableResourceDriver).Device("numa-0-cpus",
			map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				nodeAllocatableResourceCapacityName: {Value: apiresource.MustParse("10")},
			},
		).Obj()
		nativeSlice.Spec.Devices[0].AllowMultipleAllocations = ptr.To(true)
		nativeSlice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
			v1.ResourceCPU: {
				Mapping: &resourceapi.NodeAllocatableMapping{
					CapacityKey: &nodeAllocatableResourceCapacityName,
				},
			},
		}
		return nativeSlice
	}

	// 设备相同，但多了 "healthy" 布尔属性
	workerNode2      = &st.MakeNode().Name(node2Name).Label("kubernetes.io/hostname", node2Name).Node
	workerNode2Slice = st.MakeResourceSlice(node2Name, driver).Device("instance-1", map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{attrName: {BoolValue: ptr.To(true)}}).Obj()

	// 又一个节点，与第二个节点相同
	workerNode3      = &st.MakeNode().Name(node3Name).Label("kubernetes.io/hostname", node3Name).Node
	workerNode3Slice = st.MakeResourceSlice(node3Name, driver).Device("instance-1", map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{attrName: {BoolValue: ptr.To(true)}}).Obj()

	workerNodeWithExtendedResource                = &st.MakeNode().Name(nodeName).Label("kubernetes.io/hostname", nodeName).Capacity(map[v1.ResourceName]string{v1.ResourceName(extendedResourceName): "1"}).Node
	workerNodeWithExtendedResourceZeroAllocatable = &st.MakeNode().Name(nodeName).Label("kubernetes.io/hostname", nodeName).Capacity(map[v1.ResourceName]string{v1.ResourceName(extendedResourceName): "0"}).Node
	workerNodeWithOptionalNodeOperations          = func() *v1.Node {
		node := workerNode.DeepCopy()
		node.Status.DeclaredFeatures = []string{draoptionalnodeoperations.DRAOptionalNodeOperationsFeatureGate}
		return node
	}()
	brokenSelector = resourceapi.DeviceSelector{
		CEL: &resourceapi.CELDeviceSelector{
			// workerNode 没有该属性，此选择器在它上面求值会失败
			Expression: fmt.Sprintf(`device.attributes["%s"].%s`, driver, attrName),
		},
	}

	claim = st.MakeResourceClaim().
		Name(claimName).
		Namespace(namespace).
		Request(className).
		Obj()
	largeClaim = st.MakeResourceClaim().
			Name(claimName).
			Namespace(namespace).
			Request(className).
			Request(className).
			Request(className).
			Request(className).
			Request(className).
			Obj()
	claim2 = st.MakeResourceClaim().
		Name(claimName2).
		Namespace(namespace).
		Request(className).
		Obj()
	claimWithPrioritzedList = st.MakeResourceClaim().
				Name(claimName).
				Namespace(namespace).
				RequestWithPrioritizedList(
			st.SubRequest("subreq-1", className, 1),
		).
		Obj()
	claimWithPrioritizedListAndSelector = st.MakeResourceClaim().
						Name(claimName).
						Namespace(namespace).
						RequestWithPrioritizedList(
			st.SubRequestWithSelector("subreq-1", className, fmt.Sprintf(`device.attributes["%s"].%s`, driver, attrName)),
			st.SubRequest("subreq-2", className, 1),
		).
		Obj()
	claimWithMultiplePrioritizedListRequests = st.MakeResourceClaim().
							Name(claimName).
							Namespace(namespace).
							RequestWithPrioritizedList(
			st.SubRequest("subreq-1", className, 2),
			st.SubRequest("subreq-2", className, 1),
		).
		RequestWithPrioritizedList(
			st.SubRequest("subreq-1", className, 2),
			st.SubRequest("subreq-2", className, 1),
		).Obj()
	claim2WithPrioritizedListAndMultipleSubrequests = st.MakeResourceClaim().
							Name(claimName2).
							Namespace(namespace).
							RequestWithPrioritizedList(
			st.SubRequest("subreq-1", className, 4),
			st.SubRequest("subreq-2", className, 3),
			st.SubRequest("subreq-3", className, 2),
			st.SubRequest("subreq-4", className, 1),
		).Obj()
	nodeAllocatableClaimWithCapacity = func() *resourceapi.ResourceClaim {
		claim := st.MakeResourceClaim().
			Name(claimName).
			Namespace(namespace).
			Request(className).
			Obj()
		claim.Spec.Devices.Requests[0].Exactly.Capacity = &resourceapi.CapacityRequirements{
			Requests: map[resourceapi.QualifiedName]apiresource.Quantity{
				nodeAllocatableResourceCapacityName: apiresource.MustParse("1"),
			},
		}
		return claim
	}()
	pendingNodeAllocatableClaimWithCapacity = st.FromResourceClaim(nodeAllocatableClaimWithCapacity).
						OwnerReference(podName, podUID, podKind).
						Obj()

	pendingClaim = st.FromResourceClaim(claim).
			OwnerReference(podName, podUID, podKind).
			Obj()
	pendingClaim2 = st.FromResourceClaim(claim2).
			OwnerReference(podName, podUID, podKind).
			Obj()
	pendingPodGroupClaim = st.FromResourceClaim(claim).
				OwnerReference(podGroupName, podGroupUID, podGroupKind).
				Obj()

	pendingClaimWithPrioritizedList = st.FromResourceClaim(claimWithPrioritzedList).
					OwnerReference(podName, podUID, podKind).
					Obj()
	pendingClaimWithPrioritizedListAndSelector = st.FromResourceClaim(claimWithPrioritizedListAndSelector).
							OwnerReference(podName, podUID, podKind).
							Obj()
	pendingClaim2WithPrioritizedListAndMultipleSubrequests = st.FromResourceClaim(claim2WithPrioritizedListAndMultipleSubrequests).
								OwnerReference(podName, podUID, podKind).
								Obj()
	pendingClaimWithMultiplePrioritizedListRequests = st.FromResourceClaim(claimWithMultiplePrioritizedListRequests).
							OwnerReference(podName, podUID, podKind).
							Obj()
	allocationResult = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  "instance-1",
				Request: "req-1",
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResultWithSharedDevice = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  sharedDeviceName,
				Request: "req-1",
				ShareID: ptr.To(types.UID("share-123")), // Shared device allocation
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResultWithConsumedCapacity = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  sharedDeviceName,
				Request: "req-1",
				ShareID: ptr.To(types.UID("share-123")), // Shared device allocation
				ConsumedCapacity: map[resourceapi.QualifiedName]apiresource.Quantity{
					capacityName: apiresource.MustParse("1"),
				},
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResultWithConsumedCapacity2 = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  sharedDeviceName,
				Request: "req-1",
				ShareID: ptr.To(types.UID("share-456")), // Shared device allocation
				ConsumedCapacity: map[resourceapi.QualifiedName]apiresource.Quantity{
					capacityName: apiresource.MustParse("1"),
				},
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResult2 = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver2,
				Pool:    nodeName,
				Device:  "instance-2",
				Request: "req-2",
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	extendedResourceAllocationResult = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  "instance-1",
				Request: "container-0-request-0",
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	extendedResourceAllocationResult2 = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  "instance-1",
				Request: "container-0-request-1",
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	implicitExtendedResourceAllocationResult = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-1",
					Request: "container-0-request-0",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-2",
					Request: "container-0-request-1",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-3",
					Request: "container-0-request-1",
				},
			},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	implicitExtendedResourceAllocationResultTwoContainers = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-1",
					Request: "container-0-request-0",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-2",
					Request: "container-1-request-0",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-3",
					Request: "container-1-request-0",
				},
			},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	extendedResourceAllocationResultNode2 = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  "instance-1",
				Request: "container-0-request-0",
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{node2Name}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}

	allocationResultWithPrioritizedList = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  "instance-1",
				Request: "req-1/subreq-1",
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResultWithPrioritizedListAndSelector = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  "instance-1",
				Request: "req-1/subreq-1",
			}},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResultWithPrioritizedListAndMultipleSubrequests = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-1",
					Request: "req-1/subreq-2",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-2",
					Request: "req-1/subreq-2",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-3",
					Request: "req-1/subreq-2",
				},
			},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResultWithMultiplePrioritizedListRequests = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-1",
					Request: "req-1/subreq-1",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-2",
					Request: "req-1/subreq-1",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-1",
					Request: "req-2/subreq-1",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-2",
					Request: "req-2/subreq-1",
				},
			},
		},
		NodeSelector: func() *v1.NodeSelector {
			return st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		}(),
	}
	allocationResultWithSkipNodeOperations = func() *resourceapi.AllocationResult {
		res := allocationResult.DeepCopy()
		res.Devices.Results[0].SkipNodeOperations = []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationAll}
		return res
	}()
	inUseClaim = st.FromResourceClaim(pendingClaim).
			Allocation(allocationResult).
			ReservedForPod(podName, types.UID(podUID)).
			Obj()
	inUseClaimForPodLevel = st.FromResourceClaim(claim2).
				OwnerReference(podName+"-podlevel", podUID+"-podlevel", podKind).
				Allocation(allocationResult).
				ReservedForPod(podName+"-podlevel", types.UID(podUID+"-podlevel")).
				Obj()
	inUseClaimByPodGroup = st.FromResourceClaim(pendingPodGroupClaim).
				Allocation(allocationResult).
				ReservedForPodGroup(podGroupName, types.UID(podGroupUID)).
				Obj()
	inUseClaimWithPrioritizedList = st.FromResourceClaim(pendingClaimWithPrioritizedList).
					Allocation(allocationResultWithPrioritizedList).
					ReservedForPod(podName, types.UID(podUID)).
					Obj()
	inUseClaimWithPrioritizedListAndSelector = st.FromResourceClaim(pendingClaimWithPrioritizedListAndSelector).
							Allocation(allocationResultWithPrioritizedListAndSelector).
							ReservedForPod(podName, types.UID(podUID)).
							Obj()
	inUseClaim2WithPrioritizedListAndMultipleSubrequests = st.FromResourceClaim(pendingClaim2WithPrioritizedListAndMultipleSubrequests).
								Allocation(allocationResultWithPrioritizedListAndMultipleSubrequests).
								ReservedForPod(podName, types.UID(podUID)).
								Obj()
	inUseClaimWithMultiplePrioritizedListRequests = st.FromResourceClaim(pendingClaimWithMultiplePrioritizedListRequests).
							Allocation(allocationResultWithMultiplePrioritizedListRequests).
							ReservedForPod(podName, types.UID(podUID)).
							Obj()
	allocatedClaim = st.FromResourceClaim(pendingClaim).
			Allocation(allocationResult).
			Obj()
	allocatedClaim2 = st.FromResourceClaim(pendingClaim2).
			Allocation(allocationResult2).
			Obj()
	allocatedPodGroupClaim = st.FromResourceClaim(pendingPodGroupClaim).
				Allocation(allocationResult).
				Obj()
	allocatedClaimWithPrioritizedList = st.FromResourceClaim(pendingClaimWithPrioritizedList).
						Allocation(allocationResultWithPrioritizedList).
						Obj()
	allocatedClaimWithPrioritizedListAndSelector = st.FromResourceClaim(pendingClaimWithPrioritizedListAndSelector).
							Allocation(allocationResultWithPrioritizedListAndSelector).
							Obj()
	allocatedClaim2WithPrioritizedListAndMultipleSubrequests = st.FromResourceClaim(pendingClaim2WithPrioritizedListAndMultipleSubrequests).
									Allocation(allocationResultWithPrioritizedListAndMultipleSubrequests).
									Obj()
	allocatedClaimWithMultiplePrioritizedListRequests = st.FromResourceClaim(pendingClaimWithMultiplePrioritizedListRequests).
								Allocation(allocationResultWithMultiplePrioritizedListRequests).
								Obj()
	allocatedClaimWithWrongTopology = st.FromResourceClaim(allocatedClaim).
					Allocation(&resourceapi.AllocationResult{NodeSelector: st.MakeNodeSelector().In("no-such-label", []string{"no-such-value"}, st.NodeSelectorTypeMatchExpressions).Obj()}).
					Obj()
	allocatedPodGroupClaimWithWrongTopology = st.FromResourceClaim(allocatedPodGroupClaim).
						Allocation(&resourceapi.AllocationResult{NodeSelector: st.MakeNodeSelector().In("no-such-label", []string{"no-such-value"}, st.NodeSelectorTypeMatchExpressions).Obj()}).
						ReservedForPodGroup(podGroupName, types.UID(podGroupUID)).
						Obj()
	allocatedClaimWithGoodTopology = st.FromResourceClaim(allocatedClaim).
					Allocation(&resourceapi.AllocationResult{NodeSelector: st.MakeNodeSelector().In("kubernetes.io/hostname", []string{nodeName}, st.NodeSelectorTypeMatchExpressions).Obj()}).
					Obj()
	allocatedClaimWithSharedDevice = st.FromResourceClaim(pendingClaim).
					Allocation(allocationResultWithSharedDevice).
					Obj()
	allocatedClaimWithConsumedCapacity = st.FromResourceClaim(pendingClaim).
						Allocation(allocationResultWithConsumedCapacity).
						Obj()
	allocatedClaimWithConsumedCapacity2 = st.FromResourceClaim(pendingClaim).
						Allocation(allocationResultWithConsumedCapacity2).
						Obj()
	allocatedClaimWithSkipNodeOperations = st.FromResourceClaim(pendingClaim).
						Allocation(allocationResultWithSkipNodeOperations).
						Obj()
	otherClaim = st.MakeResourceClaim().
			Name("not-my-claim").
			Namespace(namespace).
			Request(className).
			Obj()
	otherAllocatedClaim = st.FromResourceClaim(otherClaim).
				Allocation(allocationResult).
				Obj()
	otherAllocatedClaimOtherDevice = func() *resourceapi.ResourceClaim {
		claim := otherAllocatedClaim.DeepCopy()
		claim.Status.Allocation.Devices.Results[0].Device += "-other"
		return claim
	}()
	extendedResourceClaim = st.MakeResourceClaim().
				Name("my-pod-extended-resources-0").
				GenerateName("my-pod-extended-resources-").
				Namespace(namespace).
				Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
				OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-0", className).
		Allocation(extendedResourceAllocationResult).
		Obj()
	extendedResourceClaim2 = st.MakeResourceClaim().
				Name("my-pod-extended-resources-0").
				GenerateName("my-pod-extended-resources-").
				Namespace(namespace).
				Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
				OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-1", className+"2").
		Allocation(extendedResourceAllocationResult2).
		Obj()
	extendedResourceClaimNoName = st.MakeResourceClaim().
					Name(specialClaimInMemName).
					GenerateName("my-pod-extended-resources-").
					Namespace(namespace).
					Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
					OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-0", className).
		Allocation(extendedResourceAllocationResult).
		Obj()
	extendedResourceClaimNoName2 = st.MakeResourceClaim().
					Name(specialClaimInMemName).
					GenerateName("my-pod-extended-resources-").
					Namespace(namespace).
					Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
					OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-1", className+"2").
		Allocation(extendedResourceAllocationResult2).
		Obj()
	implicitExtendedResourceClaim = st.MakeResourceClaim().
					Name("my-pod-extended-resources-0").
					GenerateName("my-pod-extended-resources-").
					Namespace(namespace).
					Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
					OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-0", className).
		RequestWithNameCount("container-0-request-1", className, 2).
		Allocation(implicitExtendedResourceAllocationResult).
		Obj()
	implicitExtendedResourceClaimNoName = st.MakeResourceClaim().
						Name(specialClaimInMemName).
						GenerateName("my-pod-extended-resources-").
						Namespace(namespace).
						Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
						OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-0", className).
		RequestWithNameCount("container-0-request-1", className, 2).
		Allocation(implicitExtendedResourceAllocationResult).
		Obj()
	implicitExtendedResourceClaimTwoContainers = st.MakeResourceClaim().
							Name("my-pod-extended-resources-0").
							GenerateName("my-pod-extended-resources-").
							Namespace(namespace).
							Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
							OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-0", className).
		RequestWithNameCount("container-1-request-0", className, 2).
		Allocation(implicitExtendedResourceAllocationResultTwoContainers).
		Obj()
	implicitExtendedResourceClaimNoNameTwoContainers = st.MakeResourceClaim().
								Name(specialClaimInMemName).
								GenerateName("my-pod-extended-resources-").
								Namespace(namespace).
								Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
								OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-0", className).
		RequestWithNameCount("container-1-request-0", className, 2).
		Allocation(implicitExtendedResourceAllocationResultTwoContainers).
		Obj()
	extendedResourceClaimNode2 = st.MakeResourceClaim().
					Name("my-pod-extended-resources-0").
					GenerateName("my-pod-extended-resources-").
					Namespace(namespace).
					Annotations(map[string]string{"resource.kubernetes.io/extended-resource-claim": "true"}).
					OwnerRef(
			metav1.OwnerReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       podName,
				UID:        types.UID(podUID),
				Controller: ptr.To(true),
			}).
		RequestWithName("container-0-request-0", className).
		Allocation(extendedResourceAllocationResultNode2).
		Obj()

	deviceTaint = resourceapi.DeviceTaint{
		Key:    "taint-key",
		Value:  "taint-value",
		Effect: resourceapi.DeviceTaintEffectNoSchedule,
	}

	// DRA 设备绑定条件相关
	bindingConditions        = []string{"condition"}
	bindingFailureConditions = []string{"failed"}

	fabricSlice = func() *resourceapi.ResourceSlice {
		res := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
		res.Spec.Devices[0].BindsToNode = ptr.To(true)
		res.Spec.Devices[0].BindingConditions = bindingConditions
		res.Spec.Devices[0].BindingFailureConditions = bindingFailureConditions
		res.Spec.NodeSelector = st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		return res
	}()

	fabricSlice2 = func() *resourceapi.ResourceSlice {
		res := st.MakeResourceSlice(nodeName, driver2).Device("instance-2").Obj()
		res.Spec.Devices[0].BindsToNode = ptr.To(true)
		res.Spec.Devices[0].BindingConditions = bindingConditions
		res.Spec.Devices[0].BindingFailureConditions = bindingFailureConditions
		res.Spec.NodeSelector = st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj()
		return res
	}()

	allocationResultWithBindingConditions = &resourceapi.AllocationResult{
		AllocationTimestamp: new(metav1.Time), // 只需非 nil，具体值不校验
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:                   driver,
				Pool:                     nodeName,
				Device:                   "instance-1",
				Request:                  "req-1",
				BindingConditions:        bindingConditions,
				BindingFailureConditions: bindingFailureConditions,
			}},
		},
		NodeSelector: st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj(),
	}

	allocationResultWithBindingConditions2 = &resourceapi.AllocationResult{
		AllocationTimestamp: new(metav1.Time), // 只需非 nil，具体值不校验
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:                   driver2,
				Pool:                     nodeName,
				Device:                   "instance-2",
				Request:                  "req-2",
				BindingConditions:        bindingConditions,
				BindingFailureConditions: bindingFailureConditions,
			}},
		},
		NodeSelector: st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj(),
	}

	bindClaim = st.FromResourceClaim(allocatedClaim).
			Allocation(allocationResultWithBindingConditions).
			Obj()

	boundClaim = st.FromResourceClaim(allocatedClaim).
			Allocation(allocationResultWithBindingConditions).
			AllocatedDeviceStatuses([]resourceapi.AllocatedDeviceStatus{
			{
				Driver: driver,
				Pool:   nodeName,
				Device: "instance-1",
				Conditions: []metav1.Condition{
					{Type: "condition", Status: metav1.ConditionTrue},
					{Type: "failed", Status: metav1.ConditionFalse},
				},
			},
		}).
		Obj()

	boundClaim2 = st.FromResourceClaim(allocatedClaim2).
			Allocation(allocationResultWithBindingConditions2).
			AllocatedDeviceStatuses([]resourceapi.AllocatedDeviceStatus{
			{
				Driver: driver2,
				Pool:   nodeName,
				Device: "instance-2",
				Conditions: []metav1.Condition{
					{Type: "condition", Status: metav1.ConditionTrue},
					{Type: "failed", Status: metav1.ConditionFalse},
				},
			},
		}).
		Obj()

	failedBindingClaim = st.FromResourceClaim(allocatedClaim).
				Allocation(allocationResultWithBindingConditions).
				AllocatedDeviceStatuses([]resourceapi.AllocatedDeviceStatus{
			{
				Driver: driver,
				Pool:   nodeName,
				Device: "instance-1",
				Conditions: []metav1.Condition{
					{Type: "condition", Status: metav1.ConditionFalse},
					{Type: "failed", Status: metav1.ConditionTrue},
				},
			},
		}).
		Obj()

	failedBindingClaim2 = st.FromResourceClaim(allocatedClaim2).
				Allocation(allocationResultWithBindingConditions2).
				AllocatedDeviceStatuses([]resourceapi.AllocatedDeviceStatus{
			{
				Driver: driver2,
				Pool:   nodeName,
				Device: "instance-2",
				Conditions: []metav1.Condition{
					{Type: "condition", Status: metav1.ConditionFalse},
					{Type: "failed", Status: metav1.ConditionTrue},
				},
			},
		}).
		Obj()

	nodeAllocatableResAllocationResult = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  nodeAllocatableResourceDriver,
				Pool:    nodeName,
				Device:  "numa-0-cpus",
				Request: "req-1",
				ShareID: ptr.To(types.UID("share-123")),
				ConsumedCapacity: map[resourceapi.QualifiedName]apiresource.Quantity{
					nodeAllocatableResourceCapacityName: apiresource.MustParse("1"),
				},
			}},
		},
		NodeSelector: st.MakeNodeSelector().In("metadata.name", []string{nodeName}, st.NodeSelectorTypeMatchFields).Obj(),
	}
)

// taintDevices 给 ResourceSlice 中所有设备加上污点，返回深拷贝。
func taintDevices(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
	slice = slice.DeepCopy()
	for i := range slice.Spec.Devices {
		slice.Spec.Devices[i].Taints = append(slice.Spec.Devices[i].Taints, deviceTaint)
	}
	return slice
}

// reserve 将 claim 标记为已被指定 Pod 预留。
func reserve(claim *resourceapi.ResourceClaim, pod *v1.Pod) *resourceapi.ResourceClaim {
	return st.FromResourceClaim(claim).
		ReservedForPod(pod.Name, types.UID(pod.UID)).
		Obj()
}

// reserveFor 将 claim 标记为已被给定的一组消费者预留。
func reserveFor(claim *resourceapi.ResourceClaim, bindings ...resourceapi.ResourceClaimConsumerReference) *resourceapi.ResourceClaim {
	return st.FromResourceClaim(claim).
		ReservedFor(bindings...).
		Obj()
}

// addAllocationTimestamp 为 claim 添加 AllocationTimestamp。
// go-cmp 比较时只关心其是否为非 nil，具体值无关。
// 涉及绑定条件的用例中，从 PreBind 阶段开始的期望 claim 必须带上该时间戳，
// 因为特性开启时 PreBind 会自动写入。
func addAllocationTimestamp(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	claim = claim.DeepCopy()
	claim.Status.Allocation.AllocationTimestamp = new(metav1.Time)
	return claim
}

// adminAccess 将 claim 的请求与分配结果都标记为管理员访问。
func adminAccess(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	claim = claim.DeepCopy()
	for i := range claim.Spec.Devices.Requests {
		claim.Spec.Devices.Requests[i].Exactly.AdminAccess = ptr.To(true)
	}
	if claim.Status.Allocation != nil {
		for i := range claim.Status.Allocation.Devices.Results {
			claim.Status.Allocation.Devices.Results[i].AdminAccess = ptr.To(true)
		}
	}
	return claim
}

// breakCELInClaim 故意破坏 claim 中的 CEL 选择器，制造运行时错误场景。
func breakCELInClaim(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
	claim = claim.DeepCopy()
	for i := range claim.Spec.Devices.Requests {
		for e := range claim.Spec.Devices.Requests[i].Exactly.Selectors {
			claim.Spec.Devices.Requests[i].Exactly.Selectors[e] = brokenSelector
		}
		if len(claim.Spec.Devices.Requests[i].Exactly.Selectors) == 0 {
			claim.Spec.Devices.Requests[i].Exactly.Selectors = []resourceapi.DeviceSelector{brokenSelector}
		}
	}
	return claim
}

// breakCELInClass 用会出错的选择器替换 DeviceClass 中的所有选择器。
func breakCELInClass(class *resourceapi.DeviceClass) *resourceapi.DeviceClass {
	class = class.DeepCopy()
	for i := range class.Spec.Selectors {
		class.Spec.Selectors[i] = brokenSelector
	}
	if len(class.Spec.Selectors) == 0 {
		class.Spec.Selectors = []resourceapi.DeviceSelector{brokenSelector}
	}

	return class
}

// updateDeviceClassName 替换 claim 中请求使用的设备类名，返回深拷贝。
func updateDeviceClassName(claim *resourceapi.ResourceClaim, deviceClassName string) *resourceapi.ResourceClaim {
	claim = claim.DeepCopy()
	for i := range claim.Spec.Devices.Requests {
		// 没有 firstAvailable 子请求时，改基础请求的类名
		if len(claim.Spec.Devices.Requests[i].FirstAvailable) == 0 {
			claim.Spec.Devices.Requests[i].Exactly.DeviceClassName = deviceClassName
		} else {
			// 有子请求时逐个替换，否则基础请求的类名不会生效
			for j := range claim.Spec.Devices.Requests[i].FirstAvailable {
				claim.Spec.Devices.Requests[i].FirstAvailable[j].DeviceClassName = deviceClassName
			}
		}
	}
	return claim
}

// getDefaultDynamicResourcesArgs 返回经过 v1 默认化与转换的内部配置参数。
func getDefaultDynamicResourcesArgs() *config.DynamicResourcesArgs {
	v1dra := &kubeschedulerconfigv1.DynamicResourcesArgs{}
	configv1.SetDefaults_DynamicResourcesArgs(v1dra)
	dra := &config.DynamicResourcesArgs{}
	_ = configv1.Convert_v1_DynamicResourcesArgs_To_config_DynamicResourcesArgs(v1dra, dra, nil)
	return dra
}

// result 定义某次操作的期望结果，涵盖操作状态与世界状态（即各对象）。
type result struct {
	status *fwk.Status
	// 对象名到更新函数的映射。函数对操作前对象的副本施加期望的变更。
	changes change

	// 操作创建的对象。
	added []metav1.Object

	// 操作删除的对象。
	removed []metav1.Object

	// 期望被假定的 claim，没有则为 nil。
	assumedClaim *resourceapi.ResourceClaim

	// 期望被跟踪为执行中的 claim 列表，没有则为 nil。
	inFlightClaims []metav1.Object
}

// change 保存按对象类型修改对象的函数。
// 这些函数会对该类型的所有对象调用；若只想改特定实例，函数内部需自行检查名称。
type change struct {
	claim func(*resourceapi.ResourceClaim) *resourceapi.ResourceClaim
	pod   func(*v1.Pod) *v1.Pod
}

// perNodeResult 是按节点名索引的期望结果。
type perNodeResult map[string]result

// forNode 返回指定节点的期望结果；nil 映射返回零值结果。
func (p perNodeResult) forNode(nodeName string) result {
	if p == nil {
		return result{}
	}
	return p[nodeName]
}

// perNodeScoreResult 是按节点名索引的期望得分。
type perNodeScoreResult map[string]int64

// forNode 返回指定节点的期望得分；nil 映射返回 0。
func (p perNodeScoreResult) forNode(nodeName string) int64 {
	if p == nil {
		return 0
	}
	return p[nodeName]
}

// want 汇总一次调度流程中各扩展点的期望结果。
type want struct {
	preenqueue               result
	preFilterResult          *fwk.PreFilterResult
	prefilter                result
	filter                   perNodeResult
	prescore                 result
	scoreResult              perNodeScoreResult
	score                    perNodeResult
	normalizeScoreResult     fwk.NodeScoreList
	normalizeScore           result
	reserve                  result
	unreserve                result
	preBindPreFlightStatus   *fwk.Status
	prebind                  result
	postbind                 result
	postFilterResult         *fwk.PostFilterResult
	podGroupPostFilterResult *fwk.PodGroupPostFilterResult
	postfilter               result

	// 非空时在 PreBind 之后调用 Unreserve，模拟真正 Bind 失败的场景。
	unreserveAfterBindFailure *result

	// 非空时在 PreBind 之前调用 Unreserve，模拟其他 PreBind 插件失败的场景。
	unreserveBeforePreBind *result
}

// prepare 定义执行各扩展点前对 API server 中对象的变更。
// 用于模拟资源驱动等其他实体并发修改对象的场景。
type prepare struct {
	filter     change
	prescore   change
	reserve    change
	unreserve  change
	prebind    change
	postbind   change
	postfilter change
}

// testPluginCase 是 TestPlugin 表驱动测试的单个用例。
type testPluginCase struct {
	// 在用例执行前调用，用于更新对当前时间敏感的时间戳。
	patchTestCase func(tc *testPluginCase)
	// 已在节点上运行或假定的 Pod。
	// 调度被测 Pod 前，它们会被预先填充到各自节点的 NodeInfo 缓存中。
	existingPods []*v1.Pod

	args *config.DynamicResourcesArgs
	// 未设置时默认为 workerNode
	nodes     []*v1.Node
	pod       *v1.Pod
	claims    []*resourceapi.ResourceClaim
	classes   []*resourceapi.DeviceClass
	podGroups []*schedulingapi.PodGroup

	inFlightClaims []*resourceapi.ResourceClaim

	// 与上面的类型不同，objs 直接存入 fake client，不经过 reactors 处理。
	objs []apiruntime.Object

	prepare prepare
	want    want

	// 为 true 时用已取消的 context 调用 Filter。
	cancelFilter bool

	// 为 true 时模拟 DRAAdminAccess 门控关闭（v1.35 行为）。
	disableDRAAdminAccess bool
	// 为 true 时模拟 DRADeviceBindingConditions 门控关闭。
	disableDRADeviceBindingConditions bool
	// 为 true 时模拟 DRAResourceClaimDeviceStatus 门控关闭。
	disableDRAResourceClaimDeviceStatus bool
	// 为 true 时模拟 DRANodeAllocatableResources 门控开启。
	enableDRANodeAllocatableResources bool
	// 为 true 时模拟 DRAConsumableCapacity 门控开启。
	enableDRAConsumableCapacity bool
	// 以下门控默认取 false，这样少见的关闭情形无需显式设置。
	disableDRA bool

	enableDRAExtendedResource        bool
	enableDRAPrioritizedList         bool
	enableDRADeviceTaints            bool
	enableDRAWorkloadResourceClaims  bool
	disableDRASchedulerFilterTimeout bool
	skipOnWindows                    string
	failPatch                        bool
	reactors                         []cgotesting.Reactor
	metrics                          func(ktesting.TContext, compbasemetrics.Gatherer)
}

// TestPlugin 运行 DRA 插件各扩展点的主表驱动测试。
func TestPlugin(t *testing.T) {
	testPlugin(ktesting.Init(t))
}

// TestPreFilterReusesPendingAllocationWithNilNodeSelector 验证 PreFilter 能复用
// NodeSelector 为 nil 的 pending allocation，不会因空选择器而触发 panic。
func TestPreFilterReusesPendingAllocationWithNilNodeSelector(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGatesDuringTest(tCtx, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.DRAWorkloadResourceClaims: true,
		features.GenericWorkload:           true,
	})

	feats := feature.Features{
		EnableDRAAdminAccess:               true,
		EnableDRADeviceBindingConditions:   true,
		EnableDRAResourceClaimDeviceStatus: true,
		EnableDRASchedulerFilterTimeout:    true,
		EnableDynamicResourceAllocation:    true,
		EnableDRAWorkloadResourceClaims:    true,
	}
	testCtx := setup(tCtx, nil, []*v1.Node{workerNode}, []*resourceapi.ResourceClaim{pendingPodGroupClaim}, []*resourceapi.DeviceClass{deviceClass}, []*schedulingapi.PodGroup{podGroupWithClaimName}, []apiruntime.Object{workerNodeSlice}, feats, false, nil)

	pendingClaim, err := testCtx.draManager.ResourceClaims().Get(namespace, claimName)
	if err != nil {
		t.Fatalf("Get claim: %v", err)
	}
	allocatedClaim := pendingClaim.DeepCopy()
	allocatedClaim.Status.Allocation = allocationResult.DeepCopy()
	allocatedClaim.Status.Allocation.NodeSelector = nil
	tCtx.ExpectNoError(testCtx.draManager.ResourceClaims().SignalClaimPendingAllocation(allocatedClaim.UID, allocatedClaim))

	podGroupCycleState := framework.NewCycleState()
	podGroupCycleState.Write(stateKey, &podGroupStateData{
		pendingAllocations: map[types.UID]sets.Set[types.UID]{
			allocatedClaim.UID: sets.New[types.UID](groupedPodWithClaimName.UID),
		},
	})
	cycleState := framework.NewCycleState()
	cycleState.SetPodGroupSchedulingCycle(podGroupCycleState)
	testCtx.state = cycleState

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(workerNode)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("PreFilter panicked: %v", r)
			}
		}()
		_, status := testCtx.p.PreFilter(tCtx, testCtx.state, groupedPodWithClaimName, []fwk.NodeInfo{nodeInfo})
		if !status.IsSuccess() {
			t.Errorf("PreFilter status: %v", status)
		}
	}()

	pluginState, err := getStateData(testCtx.state)
	if err != nil {
		t.Fatalf("getStateData: %v", err)
	}
	if pluginState.informationsForClaim[0].allocation != allocatedClaim.Status.Allocation {
		t.Errorf("allocation pointer mismatch: got %p, want %p",
			pluginState.informationsForClaim[0].allocation, allocatedClaim.Status.Allocation)
	}
	if pluginState.informationsForClaim[0].availableOnNodes != nil {
		t.Errorf("availableOnNodes should be nil, got %v", pluginState.informationsForClaim[0].availableOnNodes)
	}
}

// TestFilterReusesPendingAllocationRequiresDRAOptionalNodeOperations 验证复用 pending allocation 后，
// Filter 只在节点声明了 DRAOptionalNodeOperations 特性时才允许调度。
func TestFilterReusesPendingAllocationRequiresDRAOptionalNodeOperations(t *testing.T) {
	tCtx := ktesting.Init(t)
	featuregatetesting.SetFeatureGatesDuringTest(tCtx, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.DRAWorkloadResourceClaims: true,
		features.GenericWorkload:           true,
	})

	feats := feature.Features{
		EnableDynamicResourceAllocation: true,
		EnableDRAWorkloadResourceClaims: true,
	}
	testCtx := setup(tCtx, &config.DynamicResourcesArgs{}, []*v1.Node{workerNode, workerNodeWithOptionalNodeOperations}, []*resourceapi.ResourceClaim{pendingPodGroupClaim}, []*resourceapi.DeviceClass{deviceClass}, []*schedulingapi.PodGroup{podGroupWithClaimName}, []apiruntime.Object{workerNodeSlice}, feats, false, nil)

	pendingClaim, err := testCtx.draManager.ResourceClaims().Get(namespace, claimName)
	if err != nil {
		t.Fatalf("Get claim: %v", err)
	}
	allocatedClaim := pendingClaim.DeepCopy()
	allocatedClaim.Status.Allocation = allocationResultWithSkipNodeOperations.DeepCopy()
	allocatedClaim.Status.Allocation.NodeSelector = nil
	tCtx.ExpectNoError(testCtx.draManager.ResourceClaims().SignalClaimPendingAllocation(allocatedClaim.UID, allocatedClaim))

	podGroupCycleState := framework.NewCycleState()
	podGroupCycleState.Write(stateKey, &podGroupStateData{
		pendingAllocations: map[types.UID]sets.Set[types.UID]{
			allocatedClaim.UID: sets.New(groupedPodWithClaimName.UID),
		},
	})
	cycleState := framework.NewCycleState()
	cycleState.SetPodGroupSchedulingCycle(podGroupCycleState)
	testCtx.state = cycleState

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(workerNode)
	nodeInfoWithFeature := framework.NewNodeInfo()
	nodeInfoWithFeature.SetNode(workerNodeWithOptionalNodeOperations)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("PreFilter panicked: %v", r)
			}
		}()
		_, status := testCtx.p.PreFilter(tCtx, testCtx.state, groupedPodWithClaimName, []fwk.NodeInfo{nodeInfo, nodeInfoWithFeature})
		if !status.IsSuccess() {
			t.Errorf("PreFilter status: %v", status)
		}
	}()

	status := testCtx.p.Filter(context.Background(), testCtx.state, groupedPodWithClaimName, nodeInfo)
	assert.Equal(t, fwk.UnschedulableAndUnresolvable, status.Code())
	assert.Equal(t, fmt.Sprintf("resource claim %s allocation requires DRAOptionalNodeOperations feature on the node", klog.KObj(allocatedClaim)), status.Message())

	statusWithFeature := testCtx.p.Filter(context.Background(), testCtx.state, groupedPodWithClaimName, nodeInfoWithFeature)
	assert.True(t, statusWithFeature.IsSuccess(), "expected success when node has DRAOptionalNodeOperations feature")
}

// testPlugin 执行 TestPlugin 的用例表，逐一驱动插件的各个扩展点并校验期望结果。
func testPlugin(tCtx ktesting.TContext) {
	testcases := map[string]testPluginCase{
		"empty": {
			pod: st.MakePod().Name("foo").Namespace("default").Obj(),
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.Skip),
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
				preBindPreFlightStatus: fwk.NewStatus(fwk.Skip),
			},
		},
		"empty-with-extended-resources-enabled": {
			enableDRAExtendedResource: true,
			pod:                       st.MakePod().Name("foo").Namespace("default").Obj(),
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.Skip),
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
				preBindPreFlightStatus: fwk.NewStatus(fwk.Skip),
			},
		},
		"claim-reference": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{allocatedClaim, otherClaim},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"claim-template": {
			pod:    podWithClaimTemplateInStatus,
			claims: []*resourceapi.ResourceClaim{allocatedClaim, otherClaim},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimTemplateInStatus)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"podgroup-claim-reference-feature-disabled-reserves-for-pod": {
			enableDRAWorkloadResourceClaims: false,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{allocatedPodGroupClaim, otherClaim},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedPodGroupClaim, groupedPodWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"podgroup-claim-reference": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{allocatedPodGroupClaim, otherClaim},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserveFor(allocatedPodGroupClaim, podGroupReservation)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaimByPodGroup.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"podgroup-claim-template": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimTemplateInStatus,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimTemplate},
			claims:                          []*resourceapi.ResourceClaim{allocatedPodGroupClaim, otherClaim},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserveFor(allocatedPodGroupClaim, podGroupReservation)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaimByPodGroup.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"missing-claim": {
			pod:    podWithClaimTemplate, // status not set
			claims: []*resourceapi.ResourceClaim{allocatedClaim, otherClaim},
			want: want{
				preenqueue: result{
					status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `pod "default/my-pod": ResourceClaim not created yet`),
				},
			},
		},
		"deleted-claim": {
			pod: podWithClaimTemplateInStatus,
			claims: func() []*resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.DeletionTimestamp = &metav1.Time{Time: time.Now()}
				return []*resourceapi.ResourceClaim{claim}
			}(),
			want: want{
				preenqueue: result{
					status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim "my-pod-my-resource" is being deleted`),
				},
			},
		},
		"wrong-claim": {
			pod: podWithClaimTemplateInStatus,
			claims: func() []*resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.OwnerReferences[0].UID += "123"
				return []*resourceapi.ResourceClaim{claim}
			}(),
			want: want{
				preenqueue: result{
					status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "ResourceClaim default/my-pod-my-resource was not created for Pod default/my-pod (Pod is not owner)"),
				},
			},
		},
		"no-resources": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot allocate all claims`),
					},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"with-resources": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},
		"podgroup-with-resources": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{pendingPodGroupClaim},
			classes:                         []*resourceapi.DeviceClass{deviceClass},
			objs:                            []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedPodGroupClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserveFor(allocatedPodGroupClaim, podGroupReservation)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedPodGroupClaim.Finalizers
								claim.Status = inUseClaimByPodGroup.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"podgroup-with-resources-feature-disabled-reserves-for-pod": {
			enableDRAWorkloadResourceClaims: false,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{pendingPodGroupClaim},
			classes:                         []*resourceapi.DeviceClass{deviceClass},
			objs:                            []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedPodGroupClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedPodGroupClaim, groupedPodWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"with-resources-has-finalizer": {
			// 同上，但 finalizer 已设置。调度器被中断后重试时会出现这种情况。
			pod: podWithClaimName,
			claims: func() []*resourceapi.ResourceClaim {
				claim := pendingClaim
				claim.Finalizers = allocatedClaim.Finalizers
				return []*resourceapi.ResourceClaim{claim}
			}(),
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},
		"with-resources-finalizer-gets-removed": {
			// 同上，finalizer 已设置，但在调度器到达 PreBind 前被移除。
			pod: podWithClaimName,
			claims: func() []*resourceapi.ResourceClaim {
				claim := pendingClaim
				claim.Finalizers = allocatedClaim.Finalizers
				return []*resourceapi.ResourceClaim{claim}
			}(),
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			prepare: prepare{
				prebind: change{
					claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
						claim.Finalizers = nil
						return claim
					},
				},
			},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},
		"with-resources-finalizer-gets-added": {
			// 起初没有 finalizer，随后在调度器到达 PreBind 前被加上。
			// 正常流程不应出现，这里覆盖异常时序。
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			prepare: prepare{
				prebind: change{
					claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
						claim.Finalizers = allocatedClaim.Finalizers
						return claim
					},
				},
			},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},
		"skip-bind": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				unreserveBeforePreBind: &result{},
			},
		},
		"exhausted-resources-in-informer-cache": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim, otherAllocatedClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot allocate all claims`),
					},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"exhausted-resources-in-flight-claim": {
			pod:            podWithClaimName,
			claims:         []*resourceapi.ResourceClaim{pendingClaim, otherClaim},
			inFlightClaims: []*resourceapi.ResourceClaim{otherAllocatedClaim},
			classes:        []*resourceapi.DeviceClass{deviceClass},
			objs:           []apiruntime.Object{workerNodeSlice},
			want: want{
				preenqueue: result{
					inFlightClaims: []metav1.Object{otherAllocatedClaim},
				},
				prefilter: result{
					inFlightClaims: []metav1.Object{otherAllocatedClaim},
				},
				filter: perNodeResult{
					workerNode.Name: {
						inFlightClaims: []metav1.Object{otherAllocatedClaim},
						status:         fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot allocate all claims`),
					},
				},
				postfilter: result{
					inFlightClaims: []metav1.Object{otherAllocatedClaim},
					status:         fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"other-resources-in-flight-claim": {
			pod:            podWithClaimName,
			claims:         []*resourceapi.ResourceClaim{pendingClaim, otherClaim},
			inFlightClaims: []*resourceapi.ResourceClaim{otherAllocatedClaimOtherDevice},
			classes:        []*resourceapi.DeviceClass{deviceClass},
			objs:           []apiruntime.Object{workerNodeSlice},
			want: want{
				preenqueue: result{
					inFlightClaims: []metav1.Object{otherAllocatedClaimOtherDevice},
				},
				prefilter: result{
					inFlightClaims: []metav1.Object{otherAllocatedClaimOtherDevice},
				},
				filter: perNodeResult{
					workerNode.Name: {
						inFlightClaims: []metav1.Object{otherAllocatedClaimOtherDevice},
					},
				},
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim, otherAllocatedClaimOtherDevice},
				},
				prebind: result{
					inFlightClaims: []metav1.Object{otherAllocatedClaimOtherDevice},
					assumedClaim:   addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					inFlightClaims: []metav1.Object{allocatedClaim, otherAllocatedClaimOtherDevice},
					assumedClaim:   reserve(allocatedClaim, podWithClaimName),
				},
			},
		},

		// 设备污点的两个用例只需验证特性门控是否正确传给 allocator。
		// 污点与分配的实际逻辑在 allocator 中。
		"tainted-device-disabled": {
			enableDRADeviceTaints: false,
			pod:                   podWithClaimName,
			claims:                []*resourceapi.ResourceClaim{pendingClaim},
			classes:               []*resourceapi.DeviceClass{deviceClass},
			objs:                  []apiruntime.Object{taintDevices(workerNodeSlice)},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},
		"tainted-device-enabled": {
			enableDRADeviceTaints: true,
			pod:                   podWithClaimName,
			claims:                []*resourceapi.ResourceClaim{pendingClaim},
			classes:               []*resourceapi.DeviceClass{deviceClass},
			objs:                  []apiruntime.Object{taintDevices(workerNodeSlice)},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot allocate all claims`),
					},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},

		"request-admin-access-with-DRAAdminAccess-featuregate": {
			// DRAAdminAccess 门控开启时，pending claim 请求管理员访问，
			// 即使资源已被其他 claim 占满也能分配成功。
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{adminAccess(pendingClaim), otherAllocatedClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{adminAccess(allocatedClaim)},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(adminAccess(allocatedClaim), podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = adminAccess(inUseClaim).Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(adminAccess(allocatedClaim), podWithClaimName),
				},
			},
		},
		"request-admin-access-without-DRAAdminAccess-featuregate": {
			// DRAAdminAccess 门控关闭时，即使 pending claim 请求管理员访问，
			// 调度器也返回不可调度状态。
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{adminAccess(pendingClaim), otherAllocatedClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `claim default/my-pod-my-resource, request req-1: admin access is requested, but the feature is disabled`),
					},
				},
			},
			disableDRAAdminAccess: true,
		},

		"structured-ignore-allocated-admin-access": {
			// 已分配的 claim 使用了管理员访问，因此第二个 claim 可以共用同一设备。
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim, adminAccess(otherAllocatedClaim)},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},

		"claim-parameters-CEL-runtime-error": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{breakCELInClaim(pendingClaim)},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.AsStatus(errors.New(`claim default/my-pod-my-resource: selector #0 on device some-driver/worker/instance-1: CEL runtime error: no such key: ` + string(attrName) + `. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields`)),
					},
				},
			},
		},

		"class-parameters-CEL-runtime-error": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{breakCELInClass(deviceClass)},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.AsStatus(errors.New(`class my-resource-class: selector #0 on device some-driver/worker/instance-1: CEL runtime error: no such key: ` + string(attrName) + `. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields`)),
					},
				},
			},
		},

		// 部分节点出现 CEL 运行时错误时也不应调度 Pod：
		// 调度成功会掩盖 Pod 自身的问题，增加排查难度。
		// 对应 E2E 用例 "keeps pod pending because of CEL runtime errors"。
		"CEL-runtime-error-for-one-of-two-nodes": {
			nodes:   []*v1.Node{workerNode, workerNode2},
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{breakCELInClaim(pendingClaim)},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice, workerNode2Slice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.AsStatus(errors.New(`claim default/my-pod-my-resource: selector #0 on device some-driver/worker/instance-1: CEL runtime error: no such key: ` + string(attrName) + `. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields`)),
					},
				},
			},
		},

		// 找到两个可行节点时会调用 PreScore。
		"CEL-runtime-error-for-one-of-three-nodes": {
			nodes:   []*v1.Node{workerNode, workerNode2, workerNode3},
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{breakCELInClaim(pendingClaim)},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice, workerNode2Slice, workerNode3Slice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `claim default/my-pod-my-resource: selector #0 on device some-driver/worker/instance-1: CEL runtime error: no such key: `+string(attrName)+`. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields`),
					},
				},
				prescore: result{
					// 这是 Filter 阶段发现的错误
					status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `filter node worker: claim default/my-pod-my-resource: selector #0 on device some-driver/worker/instance-1: CEL runtime error: no such key: healthy. consider using CEL optional chaining (.? followed by orValue()) or guarding the check with has() for optional fields`),
				},
			},
		},

		"missing-class": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{pendingClaim},
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("request req-1: device class %s does not exist", className)),
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"wrong-topology": {
			// PostFilter 尝试通过释放 claim 让 Pod 变得可调度。
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{allocatedClaimWithWrongTopology},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					// claim 会被立即释放
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								Allocation(nil).
								Obj()
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation of ResourceClaim completed`),
				},
			},
		},
		"good-topology": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{allocatedClaimWithGoodTopology},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithGoodTopology, podWithClaimName)),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return addAllocationTimestamp(st.FromResourceClaim(in).
								ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: types.UID(podUID)}).
								Obj())
						},
					},
				},
			},
		},
		"allocated-skip-node-operations-missing-feature": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{allocatedClaimWithSkipNodeOperations},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								Allocation(nil).
								Obj()
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation of ResourceClaim completed`),
				},
			},
		},
		"allocated-skip-node-operations-with-feature": {
			nodes:  []*v1.Node{workerNodeWithOptionalNodeOperations},
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{allocatedClaimWithSkipNodeOperations},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithSkipNodeOperations, podWithClaimName)),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return addAllocationTimestamp(st.FromResourceClaim(in).
								ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: types.UID(podUID)}).
								Obj())
						},
					},
				},
			},
		},
		"bind-failure": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{allocatedClaimWithGoodTopology},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithGoodTopology, podWithClaimName)),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return addAllocationTimestamp(st.FromResourceClaim(in).
								ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: types.UID(podUID)}).
								Obj())
						},
					},
				},
				unreserveAfterBindFailure: &result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithGoodTopology, podWithClaimName)),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							out := in.DeepCopy()
							out.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{}
							return out
						},
					},
				},
			},
		},
		"bind-failure-preserves-inflight-until-unreserve": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					inFlightClaims: []metav1.Object{addAllocationTimestamp(allocatedClaim)},
					status:         fwk.NewStatus(fwk.Unschedulable, `claim bind error`),
				},
				unreserveAfterBindFailure: &result{},
			},
			reactors: []cgotesting.Reactor{
				&cgotesting.SimpleReactor{
					Verb:     "update",
					Resource: "resourceclaims",
					Reaction: func(action cgotesting.Action) (handled bool, ret apiruntime.Object, err error) {
						return true, nil, apierrors.NewBadRequest("claim bind error")
					},
				},
			},
		},
		"bind-podgroup-failure": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{allocatedPodGroupClaim},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserveFor(allocatedPodGroupClaim, podGroupReservation)),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							claim := st.FromResourceClaim(in).
								ReservedFor(podGroupReservation).
								Obj()
							claim = addAllocationTimestamp(claim)
							return claim
						},
					},
				},
				unreserveAfterBindFailure: &result{
					assumedClaim: addAllocationTimestamp(reserveFor(allocatedPodGroupClaim, podGroupReservation)),
					// PodGroup 仍保留在 ReservedFor 中
				},
			},
		},
		"postfilter-deallocate-reserved-podgroup": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			objs: []apiruntime.Object{
				// PodGroup 中的 Pod
				groupedPodWithClaimName,
			},
			classes: []*resourceapi.DeviceClass{deviceClass},
			claims:  []*resourceapi.ResourceClaim{allocatedPodGroupClaimWithWrongTopology},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							claim = claim.DeepCopy()
							claim.Status.ReservedFor = nil
							claim.Status.Allocation = nil
							return claim
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation of PodGroup ResourceClaim completed`),
				},
			},
		},
		"postfilter-skip-deallocate-reserved-podgroup-feature-disabled": {
			enableDRAWorkloadResourceClaims: false,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			objs: []apiruntime.Object{
				// PodGroup 中的 Pod
				groupedPodWithClaimName,
			},
			classes: []*resourceapi.DeviceClass{deviceClass},
			claims:  []*resourceapi.ResourceClaim{allocatedPodGroupClaimWithWrongTopology},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"postfilter-deallocate-unreserved-podgroup": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			objs: []apiruntime.Object{
				// PodGroup 中的 Pod
				groupedPodWithClaimName,
			},
			classes: []*resourceapi.DeviceClass{deviceClass},
			claims: []*resourceapi.ResourceClaim{
				func() *resourceapi.ResourceClaim {
					claim := allocatedPodGroupClaimWithWrongTopology.DeepCopy()
					claim.Status.ReservedFor = nil
					return claim
				}(),
			},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							claim = claim.DeepCopy()
							claim.Status.ReservedFor = nil
							claim.Status.Allocation = nil
							return claim
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation and deletion of ResourceClaims completed`),
				},
			},
		},
		"postfilter-skip-deallocate-reserved-podgroup-when-pods-active": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			objs: []apiruntime.Object{
				// PodGroup 中的 Pod
				groupedPodWithClaimName,
				st.MakePod().Name(podName + "-2").Namespace(namespace).
					Node(nodeName). // 已调度，该 Pod 仍需要 PodGroup 的 claim
					PodGroupName(podGroupName).
					Obj(),
			},
			classes: []*resourceapi.DeviceClass{deviceClass},
			claims:  []*resourceapi.ResourceClaim{allocatedPodGroupClaimWithWrongTopology},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"reserved-okay": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{inUseClaim},
		},
		"reserved-okay-podgroup": {
			enableDRAWorkloadResourceClaims: true,
			pod:                             groupedPodWithClaimName,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{inUseClaimByPodGroup},
		},
		"DRA-disabled": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{inUseClaim},
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.Skip),
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
				preBindPreFlightStatus: fwk.NewStatus(fwk.Skip),
			},
			disableDRA: true,
		},
		"claim-with-request-with-unknown-device-class": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{updateDeviceClassName(claim, "does-not-exist")},
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.Unschedulable, `request req-1: device class does-not-exist does not exist`),
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"claim-with-prioritized-list-feature-disabled": {
			enableDRAPrioritizedList: false,
			pod:                      podWithClaimName,
			claims:                   []*resourceapi.ResourceClaim{claimWithPrioritzedList},
			classes:                  []*resourceapi.DeviceClass{deviceClass},
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `claim default/my-pod-my-resource, request req-1: has subrequests, but the DRAPrioritizedList feature is disabled`),
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"claim-with-prioritized-list-unknown-device-class": {
			enableDRAPrioritizedList: true,
			pod:                      podWithClaimName,
			claims:                   []*resourceapi.ResourceClaim{updateDeviceClassName(claimWithPrioritzedList, "does-not-exist")},
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.Unschedulable, `request req-1/subreq-1: device class does-not-exist does not exist`),
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"claim-with-prioritized-list": {
			enableDRAPrioritizedList: true,
			pod:                      podWithClaimName,
			claims:                   []*resourceapi.ResourceClaim{pendingClaimWithPrioritizedList},
			classes:                  []*resourceapi.DeviceClass{deviceClass},
			objs:                     []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaimWithPrioritizedList},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithPrioritizedList, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaimWithPrioritizedList.Finalizers
								claim.Status = inUseClaimWithPrioritizedList.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"extended-resource-name-with-node-resource": {
			enableDRAExtendedResource: true,
			nodes:                     []*v1.Node{workerNodeWithExtendedResource},
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			want:                      want{},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				_, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.ErrorContains(tCtx, err, "not found")
			},
		},
		"extended-resource-one-device-plugin-one-dra": {
			enableDRAExtendedResource: true,
			nodes:                     []*v1.Node{workerNodeWithExtendedResource},
			pod:                       podWithExtendedResourceName2,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName, deviceClassWithExtendResourceName2},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName2},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName2},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(extendedResourceClaim2, podWithExtendedResourceName2)),
					added:        []metav1.Object{addAllocationTimestamp(reserve(extendedResourceClaim2, podWithExtendedResourceName2))},
				},
				postbind: result{
					assumedClaim: addAllocationTimestamp(reserve(extendedResourceClaim2, podWithExtendedResourceName2)),
				},
			},
		},
		"extended-resource-name-with-zero-allocatable": {
			enableDRAExtendedResource: true,
			nodes:                     []*v1.Node{workerNodeWithExtendedResourceZeroAllocatable},
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName)),
					added:        []metav1.Object{addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName))},
				},
				postbind: result{
					assumedClaim: reserve(extendedResourceClaim, podWithExtendedResourceName),
				},
			},
		},
		"non-DRA-extended-resource-name-with-zero-allocatable": {
			enableDRAExtendedResource: true,
			nodes:                     []*v1.Node{workerNodeWithExtendedResourceZeroAllocatable},
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClass},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				prefilter: result{
					status: fwk.NewStatus(fwk.Skip),
				},
				preBindPreFlightStatus: fwk.NewStatus(fwk.Skip),
			},
		},
		"extended-resource-name-no-resource": {
			enableDRAExtendedResource: true,
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot allocate all claims`),
					},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				_, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.ErrorContains(tCtx, err, "not found")
			},
		},
		"extended-resource-name-with-resources": {
			enableDRAExtendedResource: true,
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName)),
					added:        []metav1.Object{addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName))},
				},
				postbind: result{
					assumedClaim: reserve(extendedResourceClaim, podWithExtendedResourceName),
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				metric, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.NoError(tCtx, err)
				require.Equal(tCtx, 1, int(metric["success"]))
			},
		},
		"implicit-extended-resource-name-with-resources": {
			enableDRAExtendedResource: true,
			pod:                       podWithImplicitExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{largeWorkerNodeSlice, podWithImplicitExtendedResourceName},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{implicitExtendedResourceClaimNoName},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(implicitExtendedResourceClaim, podWithImplicitExtendedResourceName)),
					added:        []metav1.Object{addAllocationTimestamp(reserve(implicitExtendedResourceClaim, podWithImplicitExtendedResourceName))},
				},
				postbind: result{
					assumedClaim: reserve(implicitExtendedResourceClaim, podWithImplicitExtendedResourceName),
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				metric, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.NoError(tCtx, err)
				require.Equal(tCtx, 1, int(metric["success"]))
			},
		},
		"implicit-extended-resource-name-two-containers-with-resources": {
			enableDRAExtendedResource: true,
			pod:                       podWithImplicitExtendedResourceNameTwoContainers,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{largeWorkerNodeSlice, podWithImplicitExtendedResourceNameTwoContainers},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{implicitExtendedResourceClaimNoNameTwoContainers},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(implicitExtendedResourceClaimTwoContainers, podWithImplicitExtendedResourceNameTwoContainers)),
					added:        []metav1.Object{addAllocationTimestamp(reserve(implicitExtendedResourceClaimTwoContainers, podWithImplicitExtendedResourceNameTwoContainers))},
				},
				postbind: result{
					assumedClaim: reserve(implicitExtendedResourceClaimTwoContainers, podWithImplicitExtendedResourceNameTwoContainers),
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				metric, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.NoError(tCtx, err)
				require.Equal(tCtx, 1, int(metric["success"]))
			},
		},
		"extended-resource-name-with-resources-fail-patch": {
			enableDRAExtendedResource: true,
			failPatch:                 true,
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName},
				},
				prebind: result{
					inFlightClaims: []metav1.Object{addAllocationTimestamp(extendedResourceClaimNoName)},
					assumedClaim:   addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName)),
					added:          []metav1.Object{addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName))},
					status:         fwk.NewStatus(fwk.Unschedulable, `patch error`),
				},
				postbind: result{
					assumedClaim: reserve(extendedResourceClaim, podWithExtendedResourceName),
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				metric, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.NoError(tCtx, err)
				require.Equal(tCtx, 1, int(metric["success"]))
			},
		},
		"extended-resource-name-with-resources-has-claim": {
			enableDRAExtendedResource: true,
			pod:                       podWithExtendedResourceName,
			claims:                    []*resourceapi.ResourceClaim{extendedResourceClaim},
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot schedule extended resource claim`),
					},
				},
				postfilter: result{
					status:  fwk.NewStatus(fwk.Unschedulable, `deletion of ResourceClaim completed`),
					removed: []metav1.Object{extendedResourceClaim},
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				_, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.ErrorContains(tCtx, err, "not found")
			},
		},
		"extended-resource-name-with-resources-delete-claim": {
			enableDRAExtendedResource: true,
			pod:                       podWithExtendedResourceName,
			claims:                    []*resourceapi.ResourceClaim{extendedResourceClaimNode2},
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot schedule extended resource claim`),
					},
				},
				postfilter: result{
					status:  fwk.NewStatus(fwk.Unschedulable, `deletion of ResourceClaim completed`),
					removed: []metav1.Object{extendedResourceClaimNode2},
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				_, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.ErrorContains(tCtx, err, "not found")
			},
		},
		"extended-resource-name-bind-failure": {
			enableDRAExtendedResource: true,
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName)),
					added:        []metav1.Object{addAllocationTimestamp(reserve(extendedResourceClaim, podWithExtendedResourceName))},
				},
				unreserveAfterBindFailure: &result{
					removed: []metav1.Object{reserve(extendedResourceClaim, podWithExtendedResourceName)},
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				metric, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.NoError(tCtx, err)
				require.Equal(tCtx, 1, int(metric["success"]))
			},
		},
		"extended-resource-name-skip-bind": {
			enableDRAExtendedResource: true,
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName},
				},
				unreserveBeforePreBind: &result{},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				metric, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.NoError(tCtx, err)
				require.Equal(tCtx, 1, int(metric["success"]))
			},
		},
		"extended-resource-name-claim-creation-failure": {
			enableDRAExtendedResource: true,
			pod:                       podWithExtendedResourceName,
			classes:                   []*resourceapi.DeviceClass{deviceClassWithExtendResourceName},
			objs:                      []apiruntime.Object{workerNodeSlice, podWithExtendedResourceName},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName},
				},
				prebind: result{
					inFlightClaims: []metav1.Object{extendedResourceClaimNoName},
					status:         fwk.NewStatus(fwk.Unschedulable, `claim creation errors`),
				},
				unreserveAfterBindFailure: &result{
					removed: []metav1.Object{reserve(extendedResourceClaim, podWithExtendedResourceName)},
				},
			},
			reactors: []cgotesting.Reactor{
				&cgotesting.SimpleReactor{
					Verb:     "create",
					Resource: "resourceclaims",
					Reaction: func(action cgotesting.Action) (handled bool, ret apiruntime.Object, err error) {
						return true, nil, apierrors.NewBadRequest("claim creation errors")
					},
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				metric, err := testutil.GetCounterValuesFromGatherer(g, "dynamic_resource_allocation_resourceclaim_creates_total", map[string]string{}, "status")
				require.NoError(tCtx, err)
				require.Equal(tCtx, 1, int(metric["failure"]))
			},
		},
		"canceled": {
			cancelFilter: true,
			args: &config.DynamicResourcesArgs{
				FilterTimeout: &metav1.Duration{Duration: time.Nanosecond},
			},
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{largeClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{largeWorkerNodeSlice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `asked by caller to stop allocating devices: test canceling Filter`),
					},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"timeout": {
			args: &config.DynamicResourcesArgs{
				FilterTimeout: &metav1.Duration{Duration: time.Nanosecond},
			},
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{largeClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{largeWorkerNodeSlice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						// 超时返回 Error，使 Pod 通过退避重试
						status: fwk.AsStatus(fmt.Errorf("node %s: timed out trying to allocate devices", workerNode.Name)),
					},
				},
				// 不设 postfilter：Error 会立即中止调度
			},
			// Windows 定时器粒度较粗（通常 0.5~15.6ms），1ns 超时无法保证立即到期，
			// 因此跳过该用例。
			skipOnWindows: "coarse timer granularity",
		},
		"timeout_disabled": {
			// 该变体使用常规测试对象以避免运行时间过长。
			// 理论上，即便 1ns 限制被强制执行（本不该如此）用例也可能通过（假阳性），
			// 但概率很低。
			disableDRASchedulerFilterTimeout: true,
			args:                             &config.DynamicResourcesArgs{},
			pod:                              podWithClaimName,
			claims:                           []*resourceapi.ResourceClaim{pendingClaim},
			classes:                          []*resourceapi.DeviceClass{deviceClass},
			objs:                             []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},
		"timeout_zero": {
			args: &config.DynamicResourcesArgs{
				FilterTimeout: &metav1.Duration{Duration: 0},
			},
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaim.Finalizers
								claim.Status = inUseClaim.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
				postbind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
				},
			},
		},
		"dont-add-allocation-timestamp": {
			disableDRADeviceBindingConditions: true,
			pod:                               podWithClaimName,
			claims:                            []*resourceapi.ResourceClaim{pendingClaim},
			classes:                           []*resourceapi.DeviceClass{deviceClass},
			objs:                              []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: reserve(allocatedClaim, podWithClaimName),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return reserve(allocatedClaim, podWithClaimName)
						},
					},
				},
			},
		},
		"add-allocation-timestamp": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSlice},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName))
						},
					},
				},
			},
		},
		"add-allocation-timestamp-failure": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{allocatedClaim},
			prepare: prepare{
				prebind: change{
					claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
						// 模拟 PreBind 执行前 claim 在别处被释放
						return st.FromResourceClaim(in).
							Allocation(nil).
							Obj()
					},
				},
			},
			want: want{
				prebind: result{
					status: fwk.AsStatus(fmt.Errorf("claim %s got deallocated elsewhere in the meantime", klog.KObj(allocatedClaim))),
				},
			},
		},
		"bind-claim-with-binding-conditions": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{fabricSlice},
			args: &config.DynamicResourcesArgs{
				// PreBind 中快速超时：测试环境没有控制器去设置绑定条件
				BindingTimeout: &metav1.Duration{Duration: time.Second},
			},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{func() *resourceapi.ResourceClaim {
						claim := bindClaim.DeepCopy()
						// 该字段会在 PreBind 中写入，这里先清空
						claim.Status.Allocation.AllocationTimestamp = nil
						return claim
					}()},
				},
				prebind: result{
					inFlightClaims: []metav1.Object{bindClaim},
					assumedClaim:   reserve(bindClaim, podWithClaimName),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return reserve(bindClaim, podWithClaimName)
						},
					},
					// 来自 PreBind 自身：检查 isPodReadyForBinding 时超时
					status: fwk.AsStatus(errors.New("device binding timeout")),
				},
			},
		},
		"bind-failure-concurrent-deallocation": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{fabricSlice},
			args: &config.DynamicResourcesArgs{
				// PreBind 中快速超时：测试环境没有控制器去设置绑定条件
				BindingTimeout: &metav1.Duration{Duration: time.Second},
			},
			want: want{
				reserve: result{
					inFlightClaims: []metav1.Object{func() *resourceapi.ResourceClaim {
						claim := bindClaim.DeepCopy()
						// 该字段会在 PreBind 中写入，这里先清空
						claim.Status.Allocation.AllocationTimestamp = nil
						return claim
					}()},
				},
				prebind: result{
					inFlightClaims: []metav1.Object{bindClaim},
					assumedClaim:   reserve(bindClaim, podWithClaimName),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return reserve(bindClaim, podWithClaimName)
						},
					},
					// 来自 PreBind 自身：检查 isPodReadyForBinding 时超时
					status: fwk.AsStatus(errors.New("device binding timeout")),
				},
			},
		},
		"bound-claim-with-succeeded-binding-conditions": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{boundClaim},
			want: want{
				prebind: result{
					assumedClaim: reserve(boundClaim, podWithClaimName),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: types.UID(podUID)}).
								Obj()
						},
					},
					status: nil,
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				// Counter：allocations_total 应恰好有一个事件
				allocs, err := testutil.GetCounterValuesFromGatherer(
					g,
					"scheduler_dra_bindingconditions_allocations_total",
					map[string]string{
						"status": "success",
					},
					"driver", // 按 driver 标签分组
				)
				require.NoError(tCtx, err)

				var totalAllocs float64
				for _, v := range allocs {
					totalAllocs += v
				}
				require.InEpsilon(tCtx, float64(1), totalAllocs, 0.1, "expected exactly one successful allocation with BindingConditions")

				// Histogram：requires_bindingconditions=true 的成功样本应有一个
				hist, err := testutil.GetHistogramVecFromGatherer(
					g,
					"scheduler_dra_bindingconditions_wait_duration_seconds",
					map[string]string{
						"status": "success",
					},
				)
				require.NoError(tCtx, err)
				require.Equal(tCtx, uint64(1), hist.GetAggregatedSampleCount(), "expected one success sample in wait duration histogram")
			},
		},
		"bound-claim-with-failed-binding": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{failedBindingClaim},
			objs:   []apiruntime.Object{workerNodeSlice},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								Allocation(nil).
								AllocatedDeviceStatuses(nil).
								Obj()
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation of ResourceClaim completed`),
				},
			},
		},
		"bound-claim-with-timed-out-binding": {
			pod: podWithClaimName,
			claims: func() []*resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.Status.Allocation = allocationResultWithBindingConditions.DeepCopy()
				// 该 claim 带绑定条件，但已超时
				claim.Status.Allocation.AllocationTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-10 * time.Minute)))
				claim.Status.Devices = []resourceapi.AllocatedDeviceStatus{
					{
						Driver: driver,
						Pool:   nodeName,
						Device: "instance-1",
					},
				}
				return []*resourceapi.ResourceClaim{claim}
			}(),
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								Allocation(nil).
								AllocatedDeviceStatuses(nil).
								Obj()
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation of ResourceClaim completed`),
				},
			},
		},
		"prebind-fail-with-binding-timeout": {
			patchTestCase: func(tc *testPluginCase) {
				// 时间戳必须在用例即将开始执行时才注入，避免测试运行时流逝导致过期
				now := time.Now()

				// 设置分配时间：测试开始时还未超时，但到 PreBind 执行时（约 10 秒后）很快超时
				// 这样能稳定命中超时路径。
				bindingTimeout := tc.args.BindingTimeout.Duration
				timeoutAfter := 10 * time.Second
				allocatedAt := now.Add(-bindingTimeout).Add(timeoutAfter)

				claim := allocatedClaim.DeepCopy()
				claim.Status.Allocation = allocationResultWithBindingConditions.DeepCopy()
				// 该 claim 带绑定条件，但此刻尚未超时
				claim.Status.Allocation.AllocationTimestamp = ptr.To(metav1.NewTime(allocatedAt))
				claim.Status.Devices = []resourceapi.AllocatedDeviceStatus{
					{
						Driver: driver,
						Pool:   nodeName,
						Device: "instance-1",
					},
				}
				tc.claims = []*resourceapi.ResourceClaim{claim}

				claim = claim.DeepCopy()
				claim.Status.Devices = []resourceapi.AllocatedDeviceStatus{
					{
						Driver: driver,
						Pool:   nodeName,
						Device: "instance-1",
					},
				}
				tc.want.prebind.assumedClaim = reserve(claim, podWithClaimName)
			},

			args: &config.DynamicResourcesArgs{
				BindingTimeout: &metav1.Duration{Duration: 600 * time.Second},
			},
			pod:    podWithClaimName,
			claims: nil, // 由 patchTestCase 设置
			want: want{
				prebind: result{
					assumedClaim: nil, // 由 patchTestCase 设置
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: types.UID(podUID)}).
								Obj()
						},
					},
					// 来自 isPodReadyForBinding 的错误
					status: fwk.AsStatus(fmt.Errorf("%w: claim=%s", ErrDeviceBindingTimeout, claim.Name)),
				},
			},
			metrics: func(tCtx ktesting.TContext, g compbasemetrics.Gatherer) {
				// Counter：timeouts_total 应恰好有一个事件
				timeouts, err := testutil.GetCounterValuesFromGatherer(
					g,
					"scheduler_dra_bindingconditions_allocations_total",
					map[string]string{
						"status": "timeout",
					},
					"driver",
				)
				require.NoError(tCtx, err)

				var totalTimeouts float64
				for _, v := range timeouts {
					totalTimeouts += v
				}
				require.InEpsilon(tCtx, float64(1), totalTimeouts, 0.1, "expected exactly one timeout with BindingConditions")

				// Histogram：requires_bindingconditions=true 的超时样本应有一个
				hist, err := testutil.GetHistogramVecFromGatherer(
					g,
					"scheduler_dra_bindingconditions_wait_duration_seconds",
					map[string]string{
						"status": "timeout",
					},
				)
				require.NoError(tCtx, err)
				require.Equal(tCtx, uint64(1), hist.GetAggregatedSampleCount(), "expected one timeout sample in wait duration histogram")
			},
		},
		"bound-claim-with-mixed-binding-conditions": {
			pod: podWithClaimName,
			claims: func() []*resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.Status.Allocation = allocationResultWithBindingConditions.DeepCopy()
				// 该 claim 带绑定条件，但已超时
				claim.Status.Allocation.AllocationTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-10 * time.Minute)))
				claim.Status.Devices = []resourceapi.AllocatedDeviceStatus{
					{
						Driver: driver,
						Pool:   nodeName,
						Device: "instance-1",
						Conditions: []metav1.Condition{
							{Type: "condition1", Status: metav1.ConditionTrue},
							{Type: "condition2", Status: metav1.ConditionFalse},
						},
					},
				}
				return []*resourceapi.ResourceClaim{claim}
			}(),
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								Allocation(nil).
								AllocatedDeviceStatuses(nil).
								Obj()
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation of ResourceClaim completed`),
				},
			},
		},
		"bound-claim-without-binding-conditions": {
			// 验证 DRADeviceBindingConditions 开启但 claim 没有绑定条件或失败条件时，
			// 插件按"条件全部满足"继续处理。
			pod:    podWithClaimTemplateInStatus,
			claims: []*resourceapi.ResourceClaim{allocatedClaim, otherClaim},
			want: want{
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimTemplateInStatus)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
					status: nil,
				},
			},
		},
		"multi-claims-binding-conditions-all-success": {
			pod:     podWithTwoClaimNames,
			claims:  []*resourceapi.ResourceClaim{boundClaim, boundClaim2},
			classes: []*resourceapi.DeviceClass{deviceClass},
			nodes:   []*v1.Node{workerNode},
			objs:    []apiruntime.Object{fabricSlice, fabricSlice2},
			want: want{
				prebind: result{
					assumedClaim: reserve(boundClaim, podWithTwoClaimNames),
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							return st.FromResourceClaim(in).
								ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: types.UID(podUID)}).
								Obj()
						},
					},
					status: nil,
				},
			},
		},
		"multi-claims-binding-conditions-one-fail": {
			pod:     podWithTwoClaimNames,
			claims:  []*resourceapi.ResourceClaim{boundClaim, failedBindingClaim2},
			classes: []*resourceapi.DeviceClass{deviceClass},
			nodes:   []*v1.Node{workerNode},
			objs:    []apiruntime.Object{fabricSlice, fabricSlice2},
			want: want{
				filter: perNodeResult{
					workerNode.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `resourceclaim not available on the node`),
					},
				},
				postfilter: result{
					changes: change{
						claim: func(in *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if in.Name == claimName2 {
								return st.FromResourceClaim(in).
									Allocation(nil).
									AllocatedDeviceStatuses(nil).
									Obj()
							} else {
								return in
							}
						},
					},
					status: fwk.NewStatus(fwk.Unschedulable, `deallocation of ResourceClaim completed`),
				},
			},
		},
		"single-claim-prioritized-list-scoring": {
			enableDRAPrioritizedList: true,
			pod:                      podWithClaimName,
			claims:                   []*resourceapi.ResourceClaim{pendingClaimWithPrioritizedListAndSelector},
			classes:                  []*resourceapi.DeviceClass{deviceClass},
			nodes:                    []*v1.Node{workerNode, workerNode2},
			objs: []apiruntime.Object{
				st.MakeResourceSlice(nodeName, driver).Device("instance-1", map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{attrName: {BoolValue: ptr.To(true)}}).Obj(),
				st.MakeResourceSlice(node2Name, driver).Device("instance-1", map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{attrName: {BoolValue: ptr.To(false)}}).Obj(),
			},
			want: want{
				scoreResult: perNodeScoreResult{
					nodeName:  8,
					node2Name: 7,
				},
				normalizeScoreResult: fwk.NodeScoreList{
					{
						Name:  nodeName,
						Score: 100,
					},
					{
						Name:  node2Name,
						Score: 87,
					},
				},
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaimWithPrioritizedListAndSelector},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithPrioritizedListAndSelector, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = allocatedClaimWithPrioritizedListAndSelector.Finalizers
								claim.Status = inUseClaimWithPrioritizedListAndSelector.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"multiple-claims-prioritized-list-scoring": {
			enableDRAPrioritizedList: true,
			pod:                      podWithTwoClaimNames,
			claims:                   []*resourceapi.ResourceClaim{pendingClaimWithPrioritizedList, pendingClaim2WithPrioritizedListAndMultipleSubrequests},
			classes:                  []*resourceapi.DeviceClass{deviceClass},
			nodes:                    []*v1.Node{workerNode, workerNode2, workerNode3},
			objs: []apiruntime.Object{
				st.MakeResourceSlice(nodeName, driver).
					Device("instance-1").
					Device("instance-2").
					Device("instance-3").
					Device("instance-4").Obj(),
				st.MakeResourceSlice(node2Name, driver).
					Device("instance-1").
					Device("instance-2").Obj(),
				st.MakeResourceSlice(node3Name, driver).
					Device("instance-1").Obj(),
			},
			want: want{
				filter: perNodeResult{
					workerNode3.Name: {
						status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, `cannot allocate all claims`),
					},
				},
				scoreResult: perNodeScoreResult{
					workerNode.Name:  15,
					workerNode2.Name: 13,
				},
				normalizeScoreResult: fwk.NodeScoreList{
					{
						Name:  workerNode.Name,
						Score: 100,
					},
					{
						Name:  workerNode2.Name,
						Score: 86,
					},
				},
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaimWithPrioritizedList, allocatedClaim2WithPrioritizedListAndMultipleSubrequests},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithPrioritizedList, podWithTwoClaimNames)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = inUseClaimWithPrioritizedList.Finalizers
								claim.Status = inUseClaimWithPrioritizedList.Status
								claim = addAllocationTimestamp(claim)
							}
							if claim.Name == claimName2 {
								claim = claim.DeepCopy()
								claim.Finalizers = inUseClaim2WithPrioritizedListAndMultipleSubrequests.Finalizers
								claim.Status = inUseClaim2WithPrioritizedListAndMultipleSubrequests.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"multiple-requests-prioritized-list-scoring": {
			enableDRAPrioritizedList: true,
			pod:                      podWithClaimName,
			claims:                   []*resourceapi.ResourceClaim{pendingClaimWithMultiplePrioritizedListRequests},
			classes:                  []*resourceapi.DeviceClass{deviceClass},
			nodes:                    []*v1.Node{workerNode, workerNode2, workerNode3},
			objs: []apiruntime.Object{
				st.MakeResourceSlice(nodeName, driver).
					Device("instance-1").
					Device("instance-2").
					Device("instance-3").
					Device("instance-4").Obj(),
				st.MakeResourceSlice(node2Name, driver).
					Device("instance-1").
					Device("instance-2").
					Device("instance-3").Obj(),
				st.MakeResourceSlice(node3Name, driver).
					Device("instance-1").
					Device("instance-2").Obj(),
			},
			want: want{
				scoreResult: perNodeScoreResult{
					workerNode.Name:  16,
					workerNode2.Name: 15,
					workerNode3.Name: 14,
				},
				normalizeScoreResult: fwk.NodeScoreList{
					{
						Name:  workerNode.Name,
						Score: 100,
					},
					{
						Name:  workerNode2.Name,
						Score: 93,
					},
					{
						Name:  workerNode3.Name,
						Score: 87,
					},
				},
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaimWithMultiplePrioritizedListRequests},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaimWithMultiplePrioritizedListRequests, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Finalizers = inUseClaimWithMultiplePrioritizedListRequests.Finalizers
								claim.Status = inUseClaimWithMultiplePrioritizedListRequests.Status
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
					},
				},
			},
		},
		"node-allocatable-resource-multiplier": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimReferenceInContainer,
			patchTestCase: func(tc *testPluginCase) {
				// 真实调度周期中，Assume 阶段会在 PreBind 之前把 Pod 的
				// NodeAllocatableResourceClaimStatuses 预填充到缓存。
				// 该单元测试跳过 Assume，因此必须手动向 mock Pod 注入期望状态，
				// 才能通过 patchNodeAllocatableResourceClaimStatus 的相等性检查。
				tc.pod = tc.pod.DeepCopy()
				tc.pod.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: claimName,
						Containers:        []string{tc.pod.Spec.Containers[0].Name},
						Mapping: []v1.NodeAllocatableMappedResources{{
							Name:     v1.ResourceCPU,
							Quantity: new(apiresource.MustParse("1")),
						}, {
							Name:     v1.ResourceMemory,
							Quantity: new(apiresource.MustParse("1Gi")),
						}},
					},
				}
			},
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU:    {Mapping: &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(apiresource.MustParse("1"))}},
					v1.ResourceMemory: {Mapping: &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(apiresource.MustParse("1Gi"))}},
				}
				return []apiruntime.Object{slice, podWithClaimReferenceInContainer}
			}(),
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: nil}, // 期望成功
				},
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimReferenceInContainer)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.Allocation = allocationResult
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim.Finalizers = allocatedClaim.Finalizers
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
						pod: func(pod *v1.Pod) *v1.Pod {
							if pod.Name == podName {
								p := pod.DeepCopy()
								p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
									{
										ResourceClaimName: claimName,
										Containers:        []string{podWithClaimReferenceInContainer.Spec.Containers[0].Name},
										Mapping: []v1.NodeAllocatableMappedResources{{
											Name:     v1.ResourceCPU,
											Quantity: new(apiresource.MustParse("1")),
										}, {
											Name:     v1.ResourceMemory,
											Quantity: new(apiresource.MustParse("1Gi")),
										}},
									},
								}
								return p
							}
							return pod
						},
					},
				},
			},
		},
		"node-allocatable-mapping-and-overhead-in-same-device": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimReferenceInContainer,
			patchTestCase: func(tc *testPluginCase) {
				tc.pod = tc.pod.DeepCopy()
				tc.pod.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: claimName,
						Containers:        []string{tc.pod.Spec.Containers[0].Name},
						Mapping: []v1.NodeAllocatableMappedResources{{
							Name:     v1.ResourceCPU,
							Quantity: new(apiresource.MustParse("1")),
						}},
						Overhead: []v1.NodeAllocatableOverheadResources{{
							Name:   v1.ResourceMemory,
							PerPod: apiresource.NewQuantity(100, apiresource.DecimalSI),
						}},
					},
				}
			},
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU:    {Mapping: &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(apiresource.MustParse("1"))}},
					v1.ResourceMemory: {Overhead: &resourceapi.NodeAllocatableOverhead{PerPod: apiresource.NewQuantity(100, apiresource.DecimalSI)}},
				}
				return []apiruntime.Object{slice, podWithClaimReferenceInContainer}
			}(),
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: nil},
				},
				reserve: result{
					inFlightClaims: []metav1.Object{allocatedClaim},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimReferenceInContainer)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.Allocation = allocationResult
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim.Finalizers = allocatedClaim.Finalizers
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
						pod: func(pod *v1.Pod) *v1.Pod {
							if pod.Name == podName {
								p := pod.DeepCopy()
								p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
									{
										ResourceClaimName: claimName,
										Containers:        []string{podWithClaimReferenceInContainer.Spec.Containers[0].Name},
										Mapping: []v1.NodeAllocatableMappedResources{{
											Name:     v1.ResourceCPU,
											Quantity: new(apiresource.MustParse("1")),
										}},
										Overhead: []v1.NodeAllocatableOverheadResources{{
											Name:   v1.ResourceMemory,
											PerPod: apiresource.NewQuantity(100, apiresource.DecimalSI),
										}},
									},
								}
								return p
							}
							return pod
						},
					},
				},
			},
		},
		"node-allocatable-resource-capacitykey": {
			enableDRANodeAllocatableResources: true,
			enableDRAConsumableCapacity:       true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimReferenceInContainer,
			patchTestCase: func(tc *testPluginCase) {
				// 模拟 Assume 阶段：预填充状态
				tc.pod = tc.pod.DeepCopy()
				tc.pod.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: claimName,
						Containers:        []string{tc.pod.Spec.Containers[0].Name},
						Mapping: []v1.NodeAllocatableMappedResources{{
							Name:     v1.ResourceCPU,
							Quantity: new(apiresource.MustParse("1")),
						}},
					},
				}
			},
			claims:  []*resourceapi.ResourceClaim{st.FromResourceClaim(nodeAllocatableClaimWithCapacity).OwnerReference(podName, podUID, podKind).Obj()},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSliceWithNodeAllocatableResource(), podWithClaimReferenceInContainer},
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: nil},
				},
				reserve: result{
					inFlightClaims: []metav1.Object{
						st.FromResourceClaim(pendingNodeAllocatableClaimWithCapacity).
							Allocation(nodeAllocatableResAllocationResult).
							Obj(),
					},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(st.FromResourceClaim(pendingNodeAllocatableClaimWithCapacity).
						Allocation(nodeAllocatableResAllocationResult).
						ReservedForPod(podName, types.UID(podUID)).
						Obj()),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								c := claim.DeepCopy()
								c.Finalizers = []string{resourceapi.Finalizer}
								alloc := nodeAllocatableResAllocationResult.DeepCopy()
								c.Status.Allocation = alloc
								c.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{{Resource: "pods", Name: podName, UID: types.UID(podUID)}}
								c = addAllocationTimestamp(c)
								return c
							}
							return claim
						},
						pod: func(pod *v1.Pod) *v1.Pod {
							if pod.Name == podName {
								p := pod.DeepCopy()
								p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
									{
										ResourceClaimName: claimName,
										Containers:        []string{podWithClaimReferenceInContainer.Spec.Containers[0].Name},
										Mapping: []v1.NodeAllocatableMappedResources{{
											Name:     v1.ResourceCPU,
											Quantity: new(apiresource.MustParse("1")),
										}},
									},
								}
								return p
							}
							return pod
						},
					},
				},
			},
		},
		"node-allocatable-resource-capacitykey-and-multiplier": {
			enableDRANodeAllocatableResources: true,
			enableDRAConsumableCapacity:       true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimReferenceInContainer,
			patchTestCase: func(tc *testPluginCase) {
				// 模拟 Assume 阶段：预填充状态
				tc.pod = tc.pod.DeepCopy()
				tc.pod.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: claimName,
						Containers:        []string{tc.pod.Spec.Containers[0].Name},
						Mapping: []v1.NodeAllocatableMappedResources{{
							Name:     v1.ResourceCPU,
							Quantity: new(apiresource.MustParse("1")),
						}, {
							Name:     v1.ResourceMemory,
							Quantity: new(apiresource.MustParse("1Gi")),
						}},
					},
				}
			},
			claims:  []*resourceapi.ResourceClaim{st.FromResourceClaim(nodeAllocatableClaimWithCapacity).OwnerReference(podName, podUID, podKind).Obj()},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs: func() []apiruntime.Object {
				slice := workerNodeSliceWithNodeAllocatableResource()
				slice.Spec.Devices[0].NodeAllocatableResources[v1.ResourceMemory] = resourceapi.NodeAllocatableResource{
					Mapping: &resourceapi.NodeAllocatableMapping{
						DeviceMultiplier: new(apiresource.MustParse("1Gi")),
					},
				}
				return []apiruntime.Object{slice, podWithClaimReferenceInContainer}
			}(),
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: nil},
				},
				reserve: result{
					inFlightClaims: []metav1.Object{
						st.FromResourceClaim(pendingNodeAllocatableClaimWithCapacity).
							Allocation(nodeAllocatableResAllocationResult).
							Obj(),
					},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(st.FromResourceClaim(pendingNodeAllocatableClaimWithCapacity).
						Allocation(nodeAllocatableResAllocationResult).
						ReservedForPod(podName, types.UID(podUID)).
						Obj()),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								c := claim.DeepCopy()
								c.Finalizers = []string{resourceapi.Finalizer}
								alloc := nodeAllocatableResAllocationResult.DeepCopy()
								c.Status.Allocation = alloc
								c.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{{Resource: "pods", Name: podName, UID: types.UID(podUID)}}
								c = addAllocationTimestamp(c)
								return c
							}
							return claim
						},
						pod: func(pod *v1.Pod) *v1.Pod {
							if pod.Name == podName {
								p := pod.DeepCopy()
								p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
									{
										ResourceClaimName: claimName,
										Containers:        []string{podWithClaimReferenceInContainer.Spec.Containers[0].Name},
										Mapping: []v1.NodeAllocatableMappedResources{{
											Name:     v1.ResourceCPU,
											Quantity: new(apiresource.MustParse("1")),
										}, {
											Name:     v1.ResourceMemory,
											Quantity: new(apiresource.MustParse("1Gi")),
										}},
									},
								}
								return p
							}
							return pod
						},
					},
				},
			},
		},
		"node-allocatable-resource-insufficient-resources-with-multiplier": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimReferenceInContainer,
			claims:                            []*resourceapi.ResourceClaim{pendingClaim},
			classes:                           []*resourceapi.DeviceClass{deviceClass},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {Mapping: &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(apiresource.MustParse("11"))}}, // Exceeds node capacity of 10
				}
				return []apiruntime.Object{slice, podWithClaimReferenceInContainer}
			}(),
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: fwk.NewStatus(fwk.Unschedulable, `Insufficient cpu`)},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"node-allocatable-resource-insufficient-with-capacityKey": {
			enableDRANodeAllocatableResources: true,
			enableDRAConsumableCapacity:       true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimReferenceInContainer,
			claims: func() []*resourceapi.ResourceClaim {
				claim := nodeAllocatableClaimWithCapacity.DeepCopy()
				claim.Spec.Devices.Requests[0].Exactly.Capacity.Requests[nodeAllocatableResourceCapacityName] = apiresource.MustParse("11") // Exceeds node capacity of 10
				return []*resourceapi.ResourceClaim{st.FromResourceClaim(claim).OwnerReference(podName, podUID, podKind).Obj()}
			}(),
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs:    []apiruntime.Object{workerNodeSliceWithNodeAllocatableResource(), podWithClaimReferenceInContainer},
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: fwk.NewStatus(fwk.Unschedulable, `cannot allocate all claims`)},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"node-allocatable-direct-mapped-sharing-rejected": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNode},
			pod:                               podWithClaimReferenceInContainer,
			claims: func() []*resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
					{
						UID: "other-pod-uid",
					},
				}
				return []*resourceapi.ResourceClaim{claim}
			}(),
			classes: []*resourceapi.DeviceClass{deviceClass},
			existingPods: []*v1.Pod{
				st.MakePod().Name("other-pod").Namespace("default").
					Node(nodeName).
					PodResourceClaims(v1.PodResourceClaim{
						Name:              "my-claim-ref",
						ResourceClaimName: new(claimName),
					}).
					Obj(),
			},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {Mapping: &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(apiresource.MustParse("1"))}},
				}
				// 同一命名空间下已有其他 Pod 在该节点使用同一个 claim
				otherPod := st.MakePod().Name("other-pod").Namespace("default").
					Node(nodeName).
					PodResourceClaims(v1.PodResourceClaim{
						Name:              "my-claim-ref",
						ResourceClaimName: new(claimName),
					}).
					Obj()
				return []apiruntime.Object{slice, podWithClaimReferenceInContainer, otherPod}
			}(),
			want: want{
				filter: perNodeResult{
					workerNode.Name: {status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node allocatable resource claim %s has a mapped device and cannot be shared across pods", claimName))},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"node-allocatable-mapping-and-overhead-sharing-rejected": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNode},
			pod:                               podWithClaimReferenceInContainer,
			claims: func() []*resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
					{
						UID: "other-pod-uid",
					},
				}
				return []*resourceapi.ResourceClaim{claim}
			}(),
			classes: []*resourceapi.DeviceClass{deviceClass},
			existingPods: []*v1.Pod{
				st.MakePod().Name("other-pod").Namespace("default").
					Node(nodeName).
					PodResourceClaims(v1.PodResourceClaim{
						Name:              "my-claim-ref",
						ResourceClaimName: new(claimName),
					}).
					Obj(),
			},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping:  &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(apiresource.MustParse("1"))},
						Overhead: &resourceapi.NodeAllocatableOverhead{PerPod: apiresource.NewQuantity(100, apiresource.DecimalSI)},
					},
				}
				// 同一命名空间下已有其他 Pod 在该节点使用同一个 claim
				otherPod := st.MakePod().Name("other-pod").Namespace("default").
					Node(nodeName).
					PodResourceClaims(v1.PodResourceClaim{
						Name:              "my-claim-ref",
						ResourceClaimName: new(claimName),
					}).
					Obj()
				return []apiruntime.Object{slice, podWithClaimReferenceInContainer, otherPod}
			}(),
			want: want{
				filter: perNodeResult{
					workerNode.Name: {status: fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node allocatable resource claim %s has a mapped device and cannot be shared across pods", claimName))},
				},
				postfilter: result{
					status: fwk.NewStatus(fwk.Unschedulable),
				},
			},
		},
		"node-allocatable-overhead-only-sharing-allowed": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimReferenceInContainer,
			patchTestCase: func(tc *testPluginCase) {
				tc.pod = tc.pod.DeepCopy()
				tc.pod.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: claimName,
						Containers:        []string{"c1"},
						Overhead: []v1.NodeAllocatableOverheadResources{
							{
								Name:   v1.ResourceMemory,
								PerPod: new(apiresource.MustParse("100")),
							},
						},
					},
				}
			},
			claims:  []*resourceapi.ResourceClaim{allocatedClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			existingPods: []*v1.Pod{
				st.MakePod().Name("other-pod").Namespace("default").
					Node(nodeName).
					PodResourceClaims(v1.PodResourceClaim{
						Name:              "my-claim-ref",
						ResourceClaimName: new(claimName),
					}).
					Obj(),
			},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceMemory: {Overhead: &resourceapi.NodeAllocatableOverhead{PerPod: apiresource.NewQuantity(100, apiresource.DecimalSI)}},
				}
				// 同一命名空间下已有其他 Pod 在该节点使用同一个 claim
				otherPod := st.MakePod().Name("other-pod").Namespace("default").
					Node(nodeName).
					PodResourceClaims(v1.PodResourceClaim{
						Name:              "my-claim-ref",
						ResourceClaimName: new(claimName),
					}).
					Obj()
				return []apiruntime.Object{slice, podWithClaimReferenceInContainer, otherPod}
			}(),
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: nil},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimReferenceInContainer)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
						pod: func(pod *v1.Pod) *v1.Pod {
							if pod.Name == podName {
								p := pod.DeepCopy()
								p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
									{
										ResourceClaimName: claimName,
										Containers:        []string{"c1"},
										Overhead: []v1.NodeAllocatableOverheadResources{
											{
												Name:   v1.ResourceMemory,
												PerPod: new(apiresource.MustParse("100")),
											},
										},
									},
								}
								return p
							}
							return pod
						},
					},
				},
			},
		},
		"node-allocatable-unreferenced-claim-direct-mapped": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimName, // claim not referenced in any container
			patchTestCase: func(tc *testPluginCase) {
				tc.pod = tc.pod.DeepCopy()
				tc.pod.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: claimName,
						Containers:        []string{},
						Mapping: []v1.NodeAllocatableMappedResources{
							{
								Name:     v1.ResourceCPU,
								Quantity: new(apiresource.MustParse("1")),
							},
						},
					},
				}
			},
			claims:  []*resourceapi.ResourceClaim{allocatedClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {Mapping: &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(apiresource.MustParse("1"))}},
				}
				return []apiruntime.Object{slice, podWithClaimName}
			}(),
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: nil},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
						pod: func(pod *v1.Pod) *v1.Pod {
							if pod.Name == podName {
								p := pod.DeepCopy()
								p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
									{
										ResourceClaimName: claimName,
										Containers:        []string{},
										Mapping: []v1.NodeAllocatableMappedResources{
											{
												Name:     v1.ResourceCPU,
												Quantity: new(apiresource.MustParse("1")),
											},
										},
									},
								}
								return p
							}
							return pod
						},
					},
				},
			},
		},
		"node-allocatable-unreferenced-claim-overhead": {
			enableDRANodeAllocatableResources: true,
			nodes:                             []*v1.Node{workerNodeWithCapacity},
			pod:                               podWithClaimName, // claim not referenced in any container
			patchTestCase: func(tc *testPluginCase) {
				tc.pod = tc.pod.DeepCopy()
				tc.pod.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: claimName,
						Containers:        []string{},
						Overhead: []v1.NodeAllocatableOverheadResources{
							{
								Name:   v1.ResourceMemory,
								PerPod: new(apiresource.MustParse("1Gi")),
							},
						},
					},
				}
			},
			claims:  []*resourceapi.ResourceClaim{allocatedClaim},
			classes: []*resourceapi.DeviceClass{deviceClass},
			objs: func() []apiruntime.Object {
				slice := st.MakeResourceSlice(nodeName, driver).Device("instance-1").Obj()
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceMemory: {Overhead: &resourceapi.NodeAllocatableOverhead{PerPod: apiresource.NewQuantity(1024*1024*1024, apiresource.BinarySI)}}, // 1Gi
				}
				return []apiruntime.Object{slice, podWithClaimName}
			}(),
			want: want{
				filter: perNodeResult{
					workerNodeWithCapacity.Name: {status: nil},
				},
				prebind: result{
					assumedClaim: addAllocationTimestamp(reserve(allocatedClaim, podWithClaimName)),
					changes: change{
						claim: func(claim *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
							if claim.Name == claimName {
								claim = claim.DeepCopy()
								claim.Status.ReservedFor = inUseClaim.Status.ReservedFor
								claim = addAllocationTimestamp(claim)
							}
							return claim
						},
						pod: func(pod *v1.Pod) *v1.Pod {
							if pod.Name == podName {
								p := pod.DeepCopy()
								p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
									{
										ResourceClaimName: claimName,
										Containers:        []string{},
										Overhead: []v1.NodeAllocatableOverheadResources{
											{
												Name:   v1.ResourceMemory,
												PerPod: new(apiresource.MustParse("1Gi")),
											},
										},
									},
								}
								return p
							}
							return pod
						},
					},
				},
			},
		},
	}
	for name, tc := range testcases {
		if len(tc.skipOnWindows) > 0 && goruntime.GOOS == "windows" {
			tCtx.Skipf("Skipping '%s' test case on Windows, reason: %s", name, tc.skipOnWindows)
		}
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			if tc.patchTestCase != nil {
				tc.patchTestCase(&tc)
			}

			nodes := tc.nodes
			if nodes == nil {
				nodes = []*v1.Node{workerNode}
			}
			feats := feature.Features{
				EnableDRAAdminAccess:               !tc.disableDRAAdminAccess,
				EnableDRADeviceBindingConditions:   !tc.disableDRADeviceBindingConditions,
				EnableDRAResourceClaimDeviceStatus: !tc.disableDRAResourceClaimDeviceStatus,
				EnableDRADeviceTaints:              tc.enableDRADeviceTaints,
				EnableDRASchedulerFilterTimeout:    !tc.disableDRASchedulerFilterTimeout,
				EnableDynamicResourceAllocation:    !tc.disableDRA,
				EnableDRAPrioritizedList:           tc.enableDRAPrioritizedList,
				EnableDRAExtendedResource:          tc.enableDRAExtendedResource,
				EnableDRANodeAllocatableResources:  tc.enableDRANodeAllocatableResources,
				EnableDRAConsumableCapacity:        tc.enableDRAConsumableCapacity,
				EnableDRAWorkloadResourceClaims:    tc.enableDRAWorkloadResourceClaims,
			}
			if !tc.enableDRAExtendedResource {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(tCtx, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
			}
			featuregatetesting.SetFeatureGateDuringTest(tCtx, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, tc.enableDRAExtendedResource)
			if tc.disableDRAAdminAccess {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(tCtx, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
				featuregatetesting.SetFeatureGateDuringTest(tCtx, utilfeature.DefaultFeatureGate, features.DRAAdminAccess, false)

				require.False(tCtx, tc.enableDRAWorkloadResourceClaims, "DRAWorkloadResourceClaims cannot be enabled when DRAAdminAccess is disabled")
			} else {
				// 1.36 之前的模拟版本不支持设置这些特性门控
				featuregatetesting.SetFeatureGatesDuringTest(tCtx, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
					features.DRAWorkloadResourceClaims: tc.enableDRAWorkloadResourceClaims,
					features.GenericWorkload:           tc.enableDRAWorkloadResourceClaims, // DRAWorkloadResourceClaims 的依赖门控
				})
			}
			if tc.disableDRADeviceBindingConditions {
				featuregatetesting.SetFeatureGateDuringTest(tCtx, utilfeature.DefaultFeatureGate, features.DRADeviceBindingConditions, false)
			}
			featuregatetesting.SetFeatureGateDuringTest(tCtx, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, tc.enableDRAExtendedResource)
			testCtx := setup(tCtx, tc.args, nodes, tc.claims, tc.classes, tc.podGroups, tc.objs, feats, tc.failPatch, tc.reactors)
			for _, pod := range tc.existingPods {
				for _, nodeInfo := range testCtx.nodeInfos {
					if nodeInfo.Node().Name == pod.Spec.NodeName {
						if concreteNodeInfo, ok := nodeInfo.(*framework.NodeInfo); ok {
							concreteNodeInfo.AddPod(pod)
						} else {
							tCtx.Fatalf("nodeInfo is not concrete *framework.NodeInfo: %T", nodeInfo)
						}
					}
				}
			}
			for _, claim := range tc.inFlightClaims {
				tCtx.ExpectNoError(testCtx.draManager.ResourceClaims().SignalClaimPendingAllocation(claim.UID, claim))
			}

			initialObjects := testCtx.listAll(tCtx)
			var registry compbasemetrics.KubeRegistry
			if tc.metrics != nil {
				registry = setupMetrics(feats)
			}

			status := testCtx.p.PreEnqueue(tCtx, tc.pod)
			tCtx.Run("PreEnqueue", func(tCtx ktesting.TContext) {
				testCtx.verify(tCtx, tc.want.preenqueue, initialObjects, tc.pod, nil, status)
			})
			if !status.IsSuccess() {
				return
			}

			nodeInfo := framework.NewNodeInfo()
			result, status := testCtx.p.PreFilter(tCtx, testCtx.state, tc.pod, []fwk.NodeInfo{nodeInfo})
			tCtx.Run("prefilter", func(tCtx ktesting.TContext) {
				assert.Equal(tCtx, tc.want.preFilterResult, result)
				testCtx.verify(tCtx, tc.want.prefilter, initialObjects, tc.pod, result, status)
			})
			unschedulable := status.IsRejected()

			var potentialNodes []fwk.NodeInfo

			initialObjects = testCtx.listAll(tCtx)
			testCtx.updateAPIServer(tCtx, initialObjects, tc.prepare.filter)
			if !unschedulable {
				for _, nodeInfo := range testCtx.nodeInfos {
					var status *fwk.Status
					tCtx.Run(fmt.Sprintf("filter/%s", nodeInfo.Node().Name), func(tCtx ktesting.TContext) {
						initialObjects = testCtx.listAll(tCtx)
						ctx := context.Context(tCtx)
						if tc.cancelFilter {
							c, cancel := context.WithCancelCause(ctx)
							ctx = c
							cancel(errors.New("test canceling Filter"))
						}
						status = testCtx.p.Filter(ctx, testCtx.state, tc.pod, nodeInfo)
						nodeName := nodeInfo.Node().Name
						testCtx.verify(tCtx, tc.want.filter.forNode(nodeName), initialObjects, tc.pod, nil, status)
					})
					if status.Code() == fwk.Success {
						potentialNodes = append(potentialNodes, nodeInfo)
					}
					if status.Code() == fwk.Error {
						// Error 会立即中止调度
						return
					}
				}
				if len(potentialNodes) == 0 {
					unschedulable = true
				}
			}

			var scores fwk.NodeScoreList
			if !unschedulable && len(potentialNodes) > 1 {
				initialObjects = testCtx.listAll(tCtx)
				initialObjects = testCtx.updateAPIServer(tCtx, initialObjects, tc.prepare.prescore)

				for _, potentialNode := range potentialNodes {
					initialObjects = testCtx.listAll(tCtx)
					score, status := testCtx.p.Score(tCtx, testCtx.state, tc.pod, potentialNode)
					nodeName := potentialNode.Node().Name
					tCtx.Run(fmt.Sprintf("score/%s", nodeName), func(tCtx ktesting.TContext) {
						assert.Equal(tCtx, tc.want.scoreResult.forNode(nodeName), score)
						testCtx.verify(tCtx, tc.want.score.forNode(nodeName), initialObjects, tc.pod, nil, status)
					})
					scores = append(scores, fwk.NodeScore{Name: nodeName, Score: score})
				}

				initialObjects = testCtx.listAll(tCtx)
				status := testCtx.p.NormalizeScore(tCtx, testCtx.state, tc.pod, scores)
				tCtx.Run("normalizeScore", func(tCtx ktesting.TContext) {
					assert.Equal(tCtx, tc.want.normalizeScoreResult, scores)
					testCtx.verify(tCtx, tc.want.normalizeScore, initialObjects, tc.pod, nil, status)
				})
			}

			var selectedNodeName string
			if !unschedulable && len(potentialNodes) > 0 {
				if len(scores) > 0 {
					nodeScore := scores[0]
					for _, score := range scores {
						if score.Score > nodeScore.Score {
							nodeScore = score
						}
					}
					selectedNodeName = nodeScore.Name
				} else {
					selectedNodeName = potentialNodes[0].Node().Name
				}

				initialObjects = testCtx.listAll(tCtx)
				initialObjects = testCtx.updateAPIServer(tCtx, initialObjects, tc.prepare.reserve)
				status := testCtx.p.Reserve(tCtx, testCtx.state, tc.pod, selectedNodeName)
				tCtx.Run("reserve", func(tCtx ktesting.TContext) {
					testCtx.verify(tCtx, tc.want.reserve, initialObjects, tc.pod, nil, status)
				})
				if status.Code() != fwk.Success {
					unschedulable = true
				}
			}

			if selectedNodeName != "" {
				if unschedulable {
					initialObjects = testCtx.listAll(tCtx)
					initialObjects = testCtx.updateAPIServer(tCtx, initialObjects, tc.prepare.unreserve)
					testCtx.p.Unreserve(tCtx, testCtx.state, tc.pod, selectedNodeName)
					tCtx.Run("unreserve", func(tCtx ktesting.TContext) {
						testCtx.verify(tCtx, tc.want.unreserve, initialObjects, tc.pod, nil, status)
					})
				} else {
					// 异步绑定前清空 PodGroup cycle state
					testCtx.state.(*framework.CycleState).SetPodGroupSchedulingCycle(nil)

					if tc.want.unreserveBeforePreBind != nil {
						initialObjects = testCtx.listAll(tCtx)
						testCtx.p.Unreserve(tCtx, testCtx.state, tc.pod, selectedNodeName)
						tCtx.Run("unreserveBeforePreBind", func(tCtx ktesting.TContext) {
							testCtx.verify(tCtx, *tc.want.unreserveBeforePreBind, initialObjects, tc.pod, nil, status)
						})
						return
					}

					initialObjects = testCtx.listAll(tCtx)
					initialObjects = testCtx.updateAPIServer(tCtx, initialObjects, tc.prepare.prebind)
					preBindPreFlightResult, preBindPreFlightStatus := testCtx.p.PreBindPreFlight(tCtx, testCtx.state, tc.pod, selectedNodeName)
					tCtx.Run("preBindPreFlightStatus", func(tContext ktesting.TContext) {
						assert.Equal(tCtx, tc.want.preBindPreFlightStatus, preBindPreFlightStatus)
					})
					tCtx.Run("preBindPreFlightResult", func(tContext ktesting.TContext) {
						assert.Equal(tCtx, &fwk.PreBindPreFlightResult{AllowParallel: true}, preBindPreFlightResult)
					})
					preBindStatus := testCtx.p.PreBind(tCtx, testCtx.state, tc.pod, selectedNodeName)
					tCtx.Run("prebind", func(tCtx ktesting.TContext) {
						testCtx.verify(tCtx, tc.want.prebind, initialObjects, tc.pod, nil, preBindStatus)
					})
					if tc.want.unreserveAfterBindFailure != nil {
						initialObjects = testCtx.listAll(tCtx)
						testCtx.p.Unreserve(tCtx, testCtx.state, tc.pod, selectedNodeName)
						tCtx.Run("unreserverAfterBindFailure", func(tCtx ktesting.TContext) {
							testCtx.verify(tCtx, *tc.want.unreserveAfterBindFailure, initialObjects, tc.pod, nil, status)
						})
					} else if status.IsSuccess() {
						initialObjects = testCtx.listAll(tCtx)
						initialObjects = testCtx.updateAPIServer(tCtx, initialObjects, tc.prepare.postbind)
					}
				}
			} else if len(potentialNodes) == 0 {
				initialObjects = testCtx.listAll(tCtx)
				initialObjects = testCtx.updateAPIServer(tCtx, initialObjects, tc.prepare.postfilter)
				if len(tc.podGroups) > 0 {
					pgInfo := &framework.PodGroupInfo{
						Name:            tc.podGroups[0].Name,
						Namespace:       tc.podGroups[0].Namespace,
						UnscheduledPods: []*v1.Pod{tc.pod},
						PodGroup:        tc.podGroups[0],
					}
					mockSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
						return nil, fwk.NewStatus(fwk.Unschedulable)
					}
					podGroupCycleState := testCtx.state.GetPodGroupSchedulingCycle()
					result, status := testCtx.p.PodGroupPostFilter(tCtx, podGroupCycleState, pgInfo, mockSchedulingFunc)
					tCtx.Run("postfilter", func(tCtx ktesting.TContext) {
						assert.Equal(tCtx, tc.want.podGroupPostFilterResult, result)
						testCtx.verify(tCtx, tc.want.postfilter, initialObjects, tc.pod, nil, status)
					})
				} else {
					result, status := testCtx.p.PostFilter(tCtx, testCtx.state, tc.pod, nil /* 插件不使用 filteredNodeStatusMap */)
					tCtx.Run("postfilter", func(tCtx ktesting.TContext) {
						assert.Equal(tCtx, tc.want.postFilterResult, result)
						testCtx.verify(tCtx, tc.want.postfilter, initialObjects, tc.pod, nil, status)
					})
				}
			}
			if tc.metrics != nil {
				tc.metrics(tCtx, registry)
			}
		})
	}
}

// setupMetrics 为测试创建独立的指标注册表。
// 由于特性门控未全局设置，不能使用 metrics.Register()，
// 故新建 registry 而非复用全局注册表。
func setupMetrics(features feature.Features) compbasemetrics.KubeRegistry {
	// 特性门控未全局设置，不能使用 metrics.Register()；
	// 这里用独立 registry 避免污染全局状态
	testRegistry := compbasemetrics.NewKubeRegistry()
	if features.EnableDRAExtendedResource {
		testRegistry.MustRegister(metrics.ResourceClaimCreatesTotal)
		metrics.ResourceClaimCreatesTotal.Reset()
	}
	// DRA 设备绑定条件相关指标
	if features.EnableDRADeviceBindingConditions {
		testRegistry.MustRegister(metrics.DRABindingConditionsAllocationsTotal)
		testRegistry.MustRegister(metrics.DRABindingConditionsPreBindDuration)

		metrics.DRABindingConditionsAllocationsTotal.Reset()
		metrics.DRABindingConditionsPreBindDuration.Reset()
	}
	return testRegistry
}

// testContext 汇总测试所需的客户端、管理器、插件实例与状态。
type testContext struct {
	client          *fake.Clientset
	informerFactory informers.SharedInformerFactory
	draManager      *DefaultDRAManager
	podGroupManager internalcache.Cache
	p               *DynamicResources
	nodeInfos       []fwk.NodeInfo
	state           fwk.CycleState
}

// verify 校验一次扩展点调用的状态与对象变更是否符合期望。
func (tc *testContext) verify(tCtx ktesting.TContext, expected result, initialObjects []metav1.Object, testPod *v1.Pod, result interface{}, status *fwk.Status) {
	tCtx.Helper()
	if expected.status == nil {
		assert.Nil(tCtx, status, status.AsError())
	} else if actualErr := status.AsError(); actualErr != nil {
		// 只比较错误字符串，错误类型细节不在测试范围
		assert.ErrorContains(tCtx, actualErr, expected.status.AsError().Error())
	} else {
		assert.Equal(tCtx, expected.status, status)
	}
	objects := tc.listAll(tCtx)
	wantObjects := update(initialObjects, expected.changes)
	wantObjects = append(wantObjects, expected.added...)
	for _, remove := range expected.removed {
		for i, obj := range wantObjects {
			// 这里故意宽松：不比较 GVR 和 UID，简化用例编写
			if obj.GetName() == remove.GetName() && obj.GetNamespace() == remove.GetNamespace() {
				wantObjects = append(wantObjects[0:i], wantObjects[i+1:]...)
				break
			}
		}
	}
	sortObjects(wantObjects)
	if wantObjects == nil {
		wantObjects = []metav1.Object{}
	}
	if objects == nil {
		objects = []metav1.Object{}
	}

	// 按对象类型分开比较，差异输出更清晰
	var wantClaims, actualClaims []metav1.Object
	var wantPods, actualPods []metav1.Object

	for _, obj := range wantObjects {
		switch obj.(type) {
		case *resourceapi.ResourceClaim:
			wantClaims = append(wantClaims, obj)
		case *v1.Pod:
			wantPods = append(wantPods, obj)
		}
	}

	for _, obj := range objects {
		switch obj.(type) {
		case *resourceapi.ResourceClaim:
			actualClaims = append(actualClaims, obj)
		case *v1.Pod:
			actualPods = append(actualPods, obj)
		}
	}

	if diff := cmp.Diff(wantPods, actualPods); diff != "" {
		tCtx.Errorf("Stored objects are different (- expected, + actual):\n%s", diff)
	}

	// assert 生成的 diff 有时过于简略，这里自己用 cmp 输出完整差异
	ignoreFieldsInResourceClaims := []cmp.Option{
		cmpopts.IgnoreFields(metav1.ObjectMeta{}, "UID", "ResourceVersion"),
		cmp.Transformer("AllocationTimestamp", func(result resourceapi.AllocationResult) resourceapi.AllocationResult {
			// 比较前把分配时间戳统一替换为空值：
			// 测试不在 synctest bubble 中运行，实际时间戳无法预测。
			if result.AllocationTimestamp != nil {
				result.AllocationTimestamp = new(metav1.Time)
			}
			return result
		}),
		// 测试不关心具体分配了哪个设备
		cmpopts.IgnoreFields(resourceapi.DeviceRequestAllocationResult{}, "Device"),
		// 共享设备的 ShareID 由 allocator 动态生成，无法写进期望值
		cmpopts.IgnoreFields(resourceapi.DeviceRequestAllocationResult{}, "ShareID"),
	}
	if diff := cmp.Diff(wantClaims, actualClaims, ignoreFieldsInResourceClaims...); diff != "" {
		tCtx.Errorf("Stored objects are different (- expected, + actual):\n%s", diff)
	}

	if expected.changes.pod != nil && testPod != nil {
		wantPod := expected.changes.pod(testPod.DeepCopy())
		actualPod, err := tc.client.CoreV1().Pods(testPod.Namespace).Get(tCtx, testPod.Name, metav1.GetOptions{})
		if err != nil {
			tCtx.Fatalf("Failed to get pod %s/%s: %v", testPod.Namespace, testPod.Name, err)
		}
		ignorePodFields := []cmp.Option{
			cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion", "UID", "CreationTimestamp", "ManagedFields"),
			cmpopts.EquateEmpty(),
		}
		if diff := cmp.Diff(wantPod, actualPod, ignorePodFields...); diff != "" {
			tCtx.Errorf("Stored Pod %s/%s is different (- expected, + actual):\n%s", testPod.Namespace, testPod.Name, diff)
		}
	}

	var expectAssumedClaims []metav1.Object
	if expected.assumedClaim != nil {
		expectAssumedClaims = append(expectAssumedClaims, expected.assumedClaim)
	}
	// actualAssumedClaims 是 assumed cache 中 latest 与 api 对象不同的 claim
	// sameAssumedClaims 是两者相同的 claim
	actualAssumedClaims, sameAssumedClaims := tc.listAssumedClaims()

	// 期望没有"latest 与 api 不同"的 claim，但实际存在时报错
	if len(expectAssumedClaims) == 0 && len(actualAssumedClaims) != 0 {
		// 删除 claim API 对象后，等待 assumed cache 与 informer 同步，
		// 之后 assumed cache 应为空。
		err := wait.PollUntilContextTimeout(tCtx, 200*time.Millisecond, time.Minute, true,
			func(ctx context.Context) (bool, error) {
				actualAssumedClaims, sameAssumedClaims = tc.listAssumedClaims()
				return len(actualAssumedClaims) == 0, nil
			})
		if err != nil || len(actualAssumedClaims) != 0 {
			tCtx.Errorf("Assumed claims are different, err=%v, expected: nil, actual:\n%v", err, actualAssumedClaims)
		}
	}
	if len(expectAssumedClaims) > 0 {
		// 只要期望的 claim 出现在 assumed cache 中即可，
		// 不关心其 latest 与 api 对象是否相同。
		for _, expected := range expectAssumedClaims {
			seen := false
			for _, actual := range actualAssumedClaims {
				if cmp.Equal(expected, actual, ignoreFieldsInResourceClaims...) {
					seen = true
				}
			}
			for _, same := range sameAssumedClaims {
				if cmp.Equal(expected, same, ignoreFieldsInResourceClaims...) {
					seen = true
				}
			}
			if !seen {
				tCtx.Errorf("Assumed claims are different, expected: %v not found", expected)
			}
		}
	}

	actualInFlightClaims := tc.listInFlightClaims()
	if diff := cmp.Diff(expected.inFlightClaims, actualInFlightClaims, ignoreFieldsInResourceClaims...); diff != "" {
		tCtx.Errorf("In-flight claims are different (- expected, + actual):\n%s", diff)
	}
}

// listAll 列出 API server 中所有的 ResourceClaim，并排序。
func (tc *testContext) listAll(tCtx ktesting.TContext) (objects []metav1.Object) {
	tCtx.Helper()
	claims, err := tc.client.ResourceV1().ResourceClaims("").List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, "list claims")
	for _, claim := range claims.Items {
		objects = append(objects, &claim)
	}
	sortObjects(objects)
	return
}

// listAssumedClaims 返回 assumed cache 中的 claim，
// 分别收集 latest 与 api 对象不同和相同的两组。
func (tc *testContext) listAssumedClaims() ([]metav1.Object, []metav1.Object) {
	var assumedClaims []metav1.Object
	var sameClaims []metav1.Object
	for _, obj := range tc.draManager.resourceClaimTracker.cache.List(nil) {
		claim := obj.(*resourceapi.ResourceClaim)
		obj, _ := tc.draManager.resourceClaimTracker.cache.Get(claim.Namespace + "/" + claim.Name)
		apiObj, _ := tc.draManager.resourceClaimTracker.cache.GetAPIObj(claim.Namespace + "/" + claim.Name)
		if obj != apiObj {
			assumedClaims = append(assumedClaims, claim)
		} else {
			sameClaims = append(sameClaims, claim)
		}
	}
	sortObjects(assumedClaims)
	sortObjects(sameClaims)
	return assumedClaims, sameClaims
}

// listInFlightClaims 返回所有跟踪中的执行中 allocation 对应的 claim，并排序。
func (tc *testContext) listInFlightClaims() []metav1.Object {
	var inFlightClaims []metav1.Object
	for _, inFlight := range tc.draManager.resourceClaimTracker.allInFlightAllocationsRLocked() {
		inFlightClaims = append(inFlightClaims, inFlight.claim)
	}
	sortObjects(inFlightClaims)
	return inFlightClaims
}

// updateAPIServer 按 updates 修改对象，并把发生变化的对象写回 API server。
func (tc *testContext) updateAPIServer(tCtx ktesting.TContext, objects []metav1.Object, updates change) []metav1.Object {
	modified := update(objects, updates)
	for i := range modified {
		obj := modified[i]
		if diff := cmp.Diff(objects[i], obj); diff != "" {
			tCtx.Logf("Updating %T %q, diff (-old, +new):\n%s", obj, obj.GetName(), diff)
			switch obj := obj.(type) {
			case *resourceapi.ResourceClaim:
				obj, err := tc.client.ResourceV1().ResourceClaims(obj.Namespace).Update(tCtx, obj, metav1.UpdateOptions{})
				tCtx.ExpectNoError(err, "prepare update")
				modified[i] = obj
			case *v1.Pod:
				obj, err := tc.client.CoreV1().Pods(obj.Namespace).Update(tCtx, obj, metav1.UpdateOptions{})
				tCtx.ExpectNoError(err, "prepare update")
				modified[i] = obj
			default:
				tCtx.Fatalf("unsupported object type %T", obj)
			}
		}
	}
	return modified
}

// sortObjects 按命名空间、名称对对象排序，便于稳定比较。
func sortObjects(objects []metav1.Object) {
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].GetNamespace() < objects[j].GetNamespace() {
			return true
		}
		return objects[i].GetName() < objects[j].GetName()
	})
}

// update 遍历所有对象，按类型找到对应的更新函数并替换对象；
// 没有更新函数的对象保持不变。
func update(objects []metav1.Object, updates change) []metav1.Object {
	var updated []metav1.Object

	for _, obj := range objects {
		switch in := obj.(type) {
		case *resourceapi.ResourceClaim:
			if updates.claim != nil {
				obj = updates.claim(in)
			}
		case *v1.Pod:
			if updates.pod != nil {
				obj = updates.pod(in)
			}
		}
		updated = append(updated, obj)
	}

	return updated
}

// setup 搭建测试环境：创建 fake client、informer、DRA 管理器与插件实例，
// 并等待所有 informer 同步完成。
func setup(tCtx ktesting.TContext, args *config.DynamicResourcesArgs, nodes []*v1.Node, claims []*resourceapi.ResourceClaim, classes []*resourceapi.DeviceClass, podGroups []*schedulingapi.PodGroup, objs []apiruntime.Object, features feature.Features, failPatch bool, apiReactors []cgotesting.Reactor) (result *testContext) {
	tCtx.Helper()

	tc := &testContext{}
	tc.client = fake.NewSimpleClientset(objs...)
	reactor := createReactor(tc.client.Tracker(), failPatch)
	tc.client.PrependReactor("*", "*", reactor)
	// 将用例提供的 reactors 前插到 client，使其先于默认 reactor 处理请求
	tc.client.ReactionChain = append(apiReactors, tc.client.ReactionChain...)

	tc.informerFactory = informers.NewSharedInformerFactory(tc.client, 0)
	var doneCheckers []cache.DoneChecker
	resourceSliceTrackerOpts := resourceslicetracker.Options{
		EnableDeviceTaintRules: true,
		SliceInformer:          tc.informerFactory.Resource().V1().ResourceSlices(),
		TaintInformer:          tc.informerFactory.Resource().V1().DeviceTaintRules(),
		KubeClient:             tc.client,
	}
	resourceSliceTracker, err := resourceslicetracker.StartTracker(tCtx, resourceSliceTrackerOpts)
	require.NoError(tCtx, err, "couldn't start resource slice tracker")
	doneCheckers = append(doneCheckers, resourceSliceTracker.HasSyncedChecker())

	claimsCache := assumecache.NewAssumeCache(tCtx.Logger(), tc.informerFactory.Resource().V1().ResourceClaims().Informer(), "resource claim", "", nil)
	// NewAssumeCache 会通过 informer 的 AddEventHandler 注册处理器以保持同步，
	// 但不返回 ResourceEventHandlerRegistration。
	// 这里给 assume cache 额外注册一个空处理器，借机拿到 informer 返回的注册对象。
	//
	// 这不是 DRA manager 实际使用的处理器，但足够接近：
	// assume cache 对所有处理器共用一个"已同步"布尔值。
	doneCheckers = append(doneCheckers, claimsCache.AddEventHandler(cache.ResourceEventHandlerFuncs{}).HasSyncedChecker())

	tc.draManager = NewDRAManager(tCtx, claimsCache, resourceSliceTracker, tc.informerFactory)
	if features.EnableDRAExtendedResource {
		cache := tc.draManager.DeviceClassResolver().(*extendedresourcecache.ExtendedResourceCache)
		deviceClassHandlerRegistration, err := tc.informerFactory.Resource().V1().DeviceClasses().Informer().AddEventHandler(cache)
		require.NoError(tCtx, err, "failed to add device class informer event handler")
		doneCheckers = append(doneCheckers, deviceClassHandlerRegistration.HasSyncedChecker())
	}

	tc.podGroupManager = internalcache.New(tCtx, nil, true, false)
	for _, obj := range objs {
		if pod, ok := obj.(*v1.Pod); ok {
			tc.podGroupManager.AddPodGroupMember(pod)
		}
	}
	for _, podGroup := range podGroups {
		tc.podGroupManager.AddPodGroup(podGroup)
	}
	snapshot := internalcache.NewTestSnapshotWithPodGroups(nil, nil, podGroups)

	opts := []runtime.Option{
		runtime.WithClientSet(tc.client),
		runtime.WithInformerFactory(tc.informerFactory),
		runtime.WithEventRecorder(&events.FakeRecorder{}),
		runtime.WithSharedDRAManager(tc.draManager),
		runtime.WithPodGroupManager(tc.podGroupManager),
		runtime.WithSnapshotSharedLister(snapshot),
	}
	fh, err := runtime.NewFramework(tCtx, nil, nil, opts...)
	tCtx.ExpectNoError(err, "create scheduler framework")
	tCtx.Cleanup(func() {
		tCtx.Cancel("test has completed")
		runtime.WaitForShutdown(fh)
	})

	if args == nil {
		args = getDefaultDynamicResourcesArgs()
	}
	pl, err := New(tCtx, args, fh, features)
	tCtx.ExpectNoError(err, "create plugin")
	tc.p = pl.(*DynamicResources)

	// 通过 API 创建对象，以便触发 reactors
	for _, claim := range claims {
		_, err := tc.client.ResourceV1().ResourceClaims(claim.Namespace).Create(tCtx, claim, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create resource claim")
	}
	for _, class := range classes {
		_, err := tc.client.ResourceV1().DeviceClasses().Create(tCtx, class, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create resource class")
	}
	for _, podGroup := range podGroups {
		_, err := tc.client.SchedulingV1beta1().PodGroups(podGroup.Namespace).Create(tCtx, podGroup, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create pod group")
	}

	tc.informerFactory.Start(tCtx.Done())
	tCtx.Cleanup(func() {
		// 必须先取消，再等待关闭，否则 Shutdown 会一直阻塞
		tCtx.Cancel("test is done")
		// 此时可以等待所有 goroutine 停止
		tc.informerFactory.Shutdown()
	})

	tc.informerFactory.WaitForCacheSync(tCtx.Done())
	// WaitForCacheSync 不保证已注册的处理器（如 NewAssumeCache 注册的）也已同步，
	// 因此还要等事件处理器自己确认同步。
	// 这确保 assume cache 与 informer store 一致，且 store 至少经历过一次全量 LIST。
	cache.WaitFor(tCtx, "event handlers", doneCheckers...)

	for _, node := range nodes {
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(node)
		tc.nodeInfos = append(tc.nodeInfos, nodeInfo)
	}
	state := framework.NewCycleState()
	if len(podGroups) > 0 {
		pgCycleState := framework.NewCycleState()
		state.SetPodGroupSchedulingCycle(pgCycleState)
	}
	tc.state = state

	return tc
}

// createReactor 让 fake client 正确处理 UID 与 ResourceVersion 字段。
// 用 client.PrependReactor 添加到 fake client。
// ResourceVersion 处理是更新时冲突检测的前提，部分用例依赖该行为。
func createReactor(tracker cgotesting.ObjectTracker, failPatch bool) func(action cgotesting.Action) (handled bool, ret apiruntime.Object, err error) {
	var nameCounter int
	var uidCounter int
	var resourceVersionCounter int
	inUseUIDs := sets.Set[types.UID]{}
	var mutex sync.Mutex

	podGroupGVR := schedulingapi.SchemeGroupVersion.WithResource("podgroups")

	return func(action cgotesting.Action) (handled bool, ret apiruntime.Object, err error) {
		if failPatch {
			if _, ok := action.(cgotesting.PatchAction); ok {
				return true, nil, errors.New("patch error")
			}
		}

		createAction, ok := action.(cgotesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		obj, ok := createAction.GetObject().(metav1.Object)
		if !ok {
			return false, nil, nil
		}

		mutex.Lock()
		defer mutex.Unlock()
		switch action.GetVerb() {
		case "create":
			// PodGroup 先于 Pod 创建，部分用例需要预设 UID 来匹配
			// ResourceClaim 的 status.reservedFor 列表。
			if obj.GetUID() != "" && action.GetResource() != podGroupGVR {
				return true, nil, errors.New("UID must not be set on create")
			}
			if obj.GetResourceVersion() != "" {
				return true, nil, errors.New("ResourceVersion must not be set on create")
			}
			if obj.GetUID() == "" {
				obj.SetUID(types.UID(fmt.Sprintf("UID-%d", uidCounter)))
				uidCounter++
			}
			if inUseUIDs.Has(obj.GetUID()) {
				return true, nil, fmt.Errorf("UID %s already in use", obj.GetUID())
			}
			inUseUIDs.Insert(obj.GetUID())
			obj.SetResourceVersion(fmt.Sprintf("%d", resourceVersionCounter))
			resourceVersionCounter++
			if obj.GetName() == "" {
				obj.SetName(obj.GetGenerateName() + fmt.Sprintf("%d", nameCounter))
				nameCounter++
			}
		case "update":
			uid := obj.GetUID()
			resourceVersion := obj.GetResourceVersion()
			if uid == "" {
				return true, nil, errors.New("UID must be set on update")
			}
			if resourceVersion == "" {
				return true, nil, errors.New("ResourceVersion must be set on update")
			}

			oldObj, err := tracker.Get(action.GetResource(), obj.GetNamespace(), obj.GetName())
			if err != nil {
				return true, nil, err
			}
			oldObjMeta, ok := oldObj.(metav1.Object)
			if !ok {
				return true, nil, errors.New("internal error: unexpected old object type")
			}
			if oldObjMeta.GetResourceVersion() != resourceVersion {
				return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), obj.GetName(), errors.New("ResourceVersion must match the object that gets updated"))
			}

			obj.SetResourceVersion(fmt.Sprintf("%d", resourceVersionCounter))
			resourceVersionCounter++
		}
		return false, nil, nil
	}
}

// TestIsSchedulableAfterClaimChange 验证 ResourceClaim 变更事件的队列提示计算。
func TestIsSchedulableAfterClaimChange(t *testing.T) {
	testIsSchedulableAfterClaimChange(ktesting.Init(t))
}

// testIsSchedulableAfterClaimChange 执行 claim 变更提示的用例表。
func testIsSchedulableAfterClaimChange(tCtx ktesting.TContext) {
	testcases := map[string]struct {
		pod            *v1.Pod
		claims         []*resourceapi.ResourceClaim
		oldObj, newObj interface{}
		wantHint       fwk.QueueingHint
		wantErr        bool
	}{
		"skip-deletes": {
			pod:      podWithClaimTemplate,
			oldObj:   pendingClaim,
			newObj:   nil,
			wantHint: fwk.QueueSkip,
		},
		"queue-on-delete-with-allocation": {
			pod:      podWithClaimTemplate,
			oldObj:   allocatedClaim,
			newObj:   nil,
			wantHint: fwk.Queue,
		},
		"queue-on-delete-final-state-unknown": {
			pod: podWithClaimTemplate,
			oldObj: cache.DeletedFinalStateUnknown{
				Key: allocatedClaim.Namespace + "/" + allocatedClaim.Name,
				Obj: allocatedClaim,
			},
			newObj:   nil,
			wantHint: fwk.Queue,
		},
		"queue-on-delete-final-state-unknown-with-object": {
			pod: podWithClaimTemplate,
			oldObj: cache.DeletedFinalStateUnknown{
				Key: allocatedClaim.Namespace + "/" + allocatedClaim.Name,
				Obj: allocatedClaim,
			},
			newObj:   nil,
			wantHint: fwk.Queue,
		},
		"queue-on-delete-final-state-unknown-without-object": {
			pod: podWithClaimTemplate,
			oldObj: cache.DeletedFinalStateUnknown{
				Key: allocatedClaim.Namespace + "/" + allocatedClaim.Name,
				Obj: nil,
			},
			newObj:   nil,
			wantHint: fwk.Queue,
		},
		"backoff-wrong-new-object": {
			pod:     podWithClaimTemplate,
			newObj:  "not-a-claim",
			wantErr: true,
		},
		"skip-wrong-claim": {
			pod: podWithClaimTemplate,
			newObj: func() *resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.OwnerReferences[0].UID += "123"
				return claim
			}(),
			wantHint: fwk.QueueSkip,
		},
		"skip-unrelated-claim": {
			pod:    podWithClaimTemplate,
			claims: []*resourceapi.ResourceClaim{allocatedClaim},
			newObj: func() *resourceapi.ResourceClaim {
				claim := allocatedClaim.DeepCopy()
				claim.Name += "-foo"
				claim.UID += "123"
				return claim
			}(),
			wantHint: fwk.QueueSkip,
		},
		"queue-on-add": {
			pod:      podWithClaimName,
			newObj:   pendingClaim,
			wantHint: fwk.Queue,
		},
		"backoff-wrong-old-object": {
			pod:     podWithClaimName,
			claims:  []*resourceapi.ResourceClaim{pendingClaim},
			oldObj:  "not-a-claim",
			newObj:  pendingClaim,
			wantErr: true,
		},
		"skip-adding-finalizer": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{pendingClaim},
			oldObj: pendingClaim,
			newObj: func() *resourceapi.ResourceClaim {
				claim := pendingClaim.DeepCopy()
				claim.Finalizers = append(claim.Finalizers, "foo")
				return claim
			}(),
			wantHint: fwk.QueueSkip,
		},
		"queue-on-status-change": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{pendingClaim},
			oldObj: pendingClaim,
			newObj: func() *resourceapi.ResourceClaim {
				claim := pendingClaim.DeepCopy()
				claim.Status.Allocation = &resourceapi.AllocationResult{}
				return claim
			}(),
			wantHint: fwk.Queue,
		},
		"claim-deallocate": {
			pod:    podWithClaimName,
			claims: []*resourceapi.ResourceClaim{pendingClaim, otherAllocatedClaim},
			oldObj: otherAllocatedClaim,
			newObj: func() *resourceapi.ResourceClaim {
				claim := otherAllocatedClaim.DeepCopy()
				claim.Status.Allocation = nil
				return claim
			}(),
			wantHint: fwk.Queue,
		},
	}

	for name, tc := range testcases {
		tCtx.SyncTest(name, func(tCtx ktesting.TContext) {
			features := feature.Features{
				EnableDRASchedulerFilterTimeout:    true,
				EnableDynamicResourceAllocation:    true,
				EnableDRADeviceBindingConditions:   true,
				EnableDRAResourceClaimDeviceStatus: true,
			}
			testCtx := setup(tCtx, nil, nil, tc.claims, nil, nil, nil, features, false, nil)
			oldObj := tc.oldObj
			newObj := tc.newObj
			if claim, ok := tc.newObj.(*resourceapi.ResourceClaim); ok {
				// 通过 client 新增或更新对象，并等待事件被处理完
				claimKey := claim.Namespace + "/" + claim.Name
				if tc.oldObj == nil {
					// 部分测试 claim 预先带了 UID，创建前需清空
					createClaim := claim.DeepCopy()
					createClaim.UID = ""
					storedClaim, err := testCtx.client.ResourceV1().ResourceClaims(createClaim.Namespace).Create(tCtx, createClaim, metav1.CreateOptions{})
					if err != nil {
						tCtx.Fatalf("create claim: expected no error, got: %v", err)
					}
					claim = storedClaim
				} else {
					cachedClaim, err := testCtx.draManager.resourceClaimTracker.cache.Get(claimKey)
					if err != nil {
						tCtx.Fatalf("retrieve old claim: expected no error, got: %v", err)
					}
					updateClaim := claim.DeepCopy()
					// 测试 claim 没有这些字段（由 client 动态生成），需从缓存中的对象复制
					updateClaim.UID = cachedClaim.(*resourceapi.ResourceClaim).UID
					updateClaim.ResourceVersion = cachedClaim.(*resourceapi.ResourceClaim).ResourceVersion

					storedClaim, err := testCtx.client.ResourceV1().ResourceClaims(updateClaim.Namespace).Update(tCtx, updateClaim, metav1.UpdateOptions{})
					if err != nil {
						tCtx.Fatalf("update claim: expected no error, got: %v", err)
					}
					claim = storedClaim
				}

				// 等待 assume cache 也同步到最新对象
				tCtx.Wait()
				cachedClaim, err := testCtx.draManager.resourceClaimTracker.cache.Get(claimKey)
				tCtx.ExpectNoError(err, "retrieve claim")
				if cachedClaim.(*resourceapi.ResourceClaim).ResourceVersion != claim.ResourceVersion {
					tCtx.Errorf("cached claim not updated yet")
				}

				// 使用带有真实 UID 与 ResourceVersion 的对象，
				// isSchedulableAfterClaimChange 依赖这些字段。
				newObj = claim
			}
			gotHint, err := testCtx.p.isSchedulableAfterClaimChange(tCtx.Logger(), tc.pod, oldObj, newObj)
			if tc.wantErr {
				if err == nil {
					tCtx.Fatal("want an error, got none")
				}
				return
			}

			if err != nil {
				tCtx.Fatalf("want no error, got: %v", err)
			}
			if tc.wantHint != gotHint {
				tCtx.Fatalf("want %#v, got %#v", tc.wantHint.String(), gotHint.String())
			}
		})
	}
}

// TestIsSchedulableAfterTargetPodUpdate 验证目标 Pod 更新事件的队列提示计算。
func TestIsSchedulableAfterTargetPodUpdate(t *testing.T) {
	testIsSchedulableAfterTargetPodUpdate(ktesting.Init(t))
}

// testIsSchedulableAfterTargetPodUpdate 执行目标 Pod 更新提示的用例表。
func testIsSchedulableAfterTargetPodUpdate(tCtx ktesting.TContext) {
	testcases := map[string]struct {
		objs     []apiruntime.Object
		pod      *v1.Pod
		claims   []*resourceapi.ResourceClaim
		obj      interface{}
		wantHint fwk.QueueingHint
		wantErr  bool
	}{
		"backoff-wrong-new-object": {
			pod:     podWithClaimTemplate,
			obj:     "not-a-claim",
			wantErr: true,
		},
		"complete": {
			objs:     []apiruntime.Object{pendingClaim},
			pod:      podWithClaimTemplate,
			obj:      podWithClaimTemplateInStatus,
			wantHint: fwk.Queue,
		},
		"missing-claim": {
			objs:     nil,
			pod:      podWithClaimTemplate,
			obj:      podWithClaimTemplateInStatus,
			wantHint: fwk.QueueSkip,
		},
		"incomplete": {
			objs: []apiruntime.Object{pendingClaim},
			pod:  podWithTwoClaimTemplates,
			obj: func() *v1.Pod {
				pod := podWithTwoClaimTemplates.DeepCopy()
				// 两个 claim 中只创建了一个 claim
				pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{{
					Name:              pod.Spec.ResourceClaims[0].Name,
					ResourceClaimName: &claimName,
				}}
				return pod
			}(),
			wantHint: fwk.QueueSkip,
		},
	}

	for name, tc := range testcases {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			features := feature.Features{
				EnableDRASchedulerFilterTimeout:    true,
				EnableDynamicResourceAllocation:    true,
				EnableDRADeviceBindingConditions:   true,
				EnableDRAResourceClaimDeviceStatus: true,
			}
			testCtx := setup(tCtx, nil, nil, tc.claims, nil, nil, tc.objs, features, false, nil)
			gotHint, err := testCtx.p.isSchedulableAfterTargetPodUpdate(tCtx.Logger(), tc.pod, nil, tc.obj)
			if tc.wantErr {
				if err == nil {
					tCtx.Fatal("want an error, got none")
				}
				return
			}

			if err != nil {
				tCtx.Fatalf("want no error, got: %v", err)
			}
			if tc.wantHint != gotHint {
				tCtx.Fatalf("want %#v, got %#v", tc.wantHint.String(), gotHint.String())
			}
		})
	}
}

// mockDeviceClassResolver 是 fwk.DeviceClassResolver 的简单 mock，
// 按资源名从预置映射中返回 DeviceClass。
type mockDeviceClassResolver struct {
	mapping map[v1.ResourceName]*resourceapi.DeviceClass
}

// GetDeviceClass 按资源名返回预置的 DeviceClass，未找到时返回 nil。
func (m *mockDeviceClassResolver) GetDeviceClass(resourceName v1.ResourceName) *resourceapi.DeviceClass {
	return m.mapping[resourceName]
}

// TestAllocatorSelection 验证如何根据实际 Kubernetes 特性门控选择 structured allocator 实现。
// 该测试放在这里而非 k8s.io/dynamic-resource-allocation/structured，
// 因为那边无法访问本仓库的特性门控定义。
func TestAllocatorSelection(t *testing.T) {
	// 确保 structured.Features 的每个字段都有专门的用例覆盖
	allFeatures := sets.New[string]()
	coveredFeatures := sets.New[string]()
	featureType := reflect.TypeFor[structured.Features]()
	for field := range featureType.Fields() {
		allFeatures.Insert(field.Name)
	}

	for name, tc := range map[string]struct {
		features             string
		usingFeatureGroup    bool
		expectImplementation string
	}{
		// 最保守的实现：仅在明确要求最稳定 Kubernetes（不含 alpha/beta 特性）时使用
		"only-GA": {
			features:             "AllAlpha=false,AllBeta=false",
			usingFeatureGroup:    true,
			expectImplementation: "stable",
		},

		// 默认情况下部分 beta 特性开启，使用 incubating 实现
		"default": {
			features:             "",
			usingFeatureGroup:    true,
			expectImplementation: "incubating",
		},

		// Alpha 特性可能需要 experimental 实现（如果它们影响分配）
		"alpha": {
			features:             "AllAlpha=true,AllBeta=true",
			usingFeatureGroup:    true,
			expectImplementation: "experimental",
		},

		// 再逐个检查影响选择决策的特性会选中哪个 allocator
		"AdminAccess": {
			features:             "AllAlpha=false,AllBeta=false,DRAAdminAccess=true",
			expectImplementation: "stable",
		},
		"CompatibilityGroups": {
			// DRAPartitionableDevices 必须与兼容组门控同时开启（特性依赖）
			features:             "AllAlpha=false,AllBeta=false,DRADeviceCompatibilityGroups=true,DRAPartitionableDevices=true",
			expectImplementation: "experimental",
		},
		"ConsumableCapacity": {
			features:             "AllAlpha=false,AllBeta=false,DRAConsumableCapacity=true",
			expectImplementation: "incubating",
		},
		"DeviceBindingAndStatus": {
			features:             "AllAlpha=false,AllBeta=false,DRAResourceClaimDeviceStatus=true,DRADeviceBindingConditions=true",
			expectImplementation: "incubating",
		},
		"DeviceTaints": {
			features:             "AllAlpha=false,AllBeta=false,DRADeviceTaints=true",
			expectImplementation: "stable",
		},
		"ListTypeAttributes": {
			features:             "AllAlpha=false,AllBeta=false,DRAListTypeAttributes=true",
			expectImplementation: "experimental",
		},
		"OptionalNodeOperations": {
			features:             "AllAlpha=false,AllBeta=false,DRAOptionalNodeOperations=true",
			expectImplementation: "experimental",
		},
		"PartitionableDevices": {
			features:             "AllAlpha=false,AllBeta=false,DRAPartitionableDevices=true",
			expectImplementation: "stable",
		},
		"FractionalCapacityRange": {
			features:             "AllAlpha=false,AllBeta=false,DRAConsumableCapacity=true,DRAFractionalCapacityRange=true",
			expectImplementation: "incubating",
		},
		"PrioritizedList": {
			features:             "AllAlpha=false,AllBeta=false,DRAPrioritizedList=true",
			expectImplementation: "stable",
		},
		"DerivedAttributes": {
			features:             "AllAlpha=false,AllBeta=false,DRADerivedAttributes=true",
			expectImplementation: "experimental",
		},
	} {
		t.Run(name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			featureGate := utilfeature.DefaultFeatureGate.DeepCopy()
			tCtx.ExpectNoError(featureGate.Set(tc.features), "set features")
			fts := feature.NewSchedulerFeaturesFromGates(featureGate)
			features := AllocatorFeatures(fts)

			// 略取巧：大部分参数并不合法，但当前构造函数还不应使用它们
			allocator, err := structured.NewAllocator(tCtx, features, structured.AllocatedState{}, nil, nil, nil)
			tCtx.ExpectNoError(err, "create allocator")
			allocatorType := fmt.Sprintf("%T", allocator)
			if !strings.Contains(allocatorType, tc.expectImplementation) {
				tCtx.Fatalf("Expected allocator implementation %q, got %s", tc.expectImplementation, allocatorType)
			}

			if !tc.usingFeatureGroup {
				for field := range featureType.Fields() {
					if !reflect.ValueOf(features).FieldByName(field.Name).IsZero() {
						coveredFeatures.Insert(field.Name)
					}
				}
			}
		})
	}

	notCovered := allFeatures.Difference(coveredFeatures)
	if len(notCovered) > 0 {
		t.Errorf("Some feature fields in %T were never set by any of the dedicated sub-tests: %s\nA test case for a new feature is missing and/or AllocatorFeatures was not updated to set the field.", structured.Features{}, strings.Join(sets.List(notCovered), ", "))
	}
}

// Test_allocationResultRequiresDRAOptionalNodeOperations 验证只要分配结果中
// 任一设备带 SkipNodeOperations，就判定需要节点支持 DRAOptionalNodeOperations。
func Test_allocationResultRequiresDRAOptionalNodeOperations(t *testing.T) {
	testcases := map[string]struct {
		alloc *resourceapi.AllocationResult
		want  bool
	}{
		"nil": {
			alloc: nil,
			want:  false,
		},
		"empty-results": {
			alloc: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: nil,
				},
			},
			want: false,
		},
		"no-skip-node-operations": {
			alloc: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver:  driver,
							Pool:    nodeName,
							Device:  "instance-1",
							Request: "req-1",
						},
					},
				},
			},
			want: false,
		},
		"with-skip-node-operations": {
			alloc: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver:             driver,
							Pool:               nodeName,
							Device:             "instance-1",
							Request:            "req-1",
							SkipNodeOperations: []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationAll},
						},
					},
				},
			},
			want: true,
		},
		"empty-skip-node-operations": {
			alloc: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver:             driver,
							Pool:               nodeName,
							Device:             "instance-1",
							Request:            "req-1",
							SkipNodeOperations: []resourceapi.SkipNodeOperation{},
						},
					},
				},
			},
			want: false,
		},
		"mixed-skip-node-operations": {
			alloc: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver:  driver,
							Pool:    nodeName,
							Device:  "instance-1",
							Request: "req-1",
						},
						{
							Driver:             driver,
							Pool:               nodeName,
							Device:             "instance-2",
							Request:            "req-2",
							SkipNodeOperations: []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationAll},
						},
					},
				},
			},
			want: true,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			got := allocationResultRequiresDRAOptionalNodeOperations(tc.alloc)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Test_computesScore 验证得分计算：按设备属性、分配状态等因素为节点打分。
func Test_computesScore(t *testing.T) {
	testcases := map[string]struct {
		claims        []*resourceapi.ResourceClaim
		allocations   nodeAllocation
		expectedScore int64
		expectErr     bool
	}{
		"more-claims-than-allocations": {
			claims: []*resourceapi.ResourceClaim{
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-1",
						st.SubRequest("subreq-1", className, 1),
					).
					Obj(),
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-2",
						st.SubRequest("subreq-1", className, 1),
					).
					Obj(),
			},
			allocations: nodeAllocation{},
			expectErr:   true,
		},
		"mix-of-allocated-and-unallocated-claims": {
			claims: []*resourceapi.ResourceClaim{
				allocatedClaim,
				pendingClaim2,
			},
			allocations: nodeAllocation{
				allocationResults: []resourceapi.AllocationResult{
					*allocationResult2,
				},
			},
			expectedScore: 0,
			expectErr:     false,
		},
		"single-request-only-subrequest-allocated": {
			claims: []*resourceapi.ResourceClaim{
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-1",
						st.SubRequest("subreq-1", className, 1),
					).
					Obj(),
			},
			allocations: nodeAllocation{
				allocationResults: []resourceapi.AllocationResult{
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1/subreq-1",
								},
							},
						},
					},
				},
			},
			expectedScore: 8,
		},
		"single-request-last-subrequest-allocated": {
			claims: []*resourceapi.ResourceClaim{
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-1",
						st.SubRequest("subreq-1", className, 1),
						st.SubRequest("subreq-2", className, 1),
						st.SubRequest("subreq-3", className, 1),
						st.SubRequest("subreq-4", className, 1),
						st.SubRequest("subreq-5", className, 1),
						st.SubRequest("subreq-6", className, 1),
						st.SubRequest("subreq-7", className, 1),
						st.SubRequest("subreq-8", className, 1),
					).
					Obj(),
			},
			allocations: nodeAllocation{
				allocationResults: []resourceapi.AllocationResult{
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1/subreq-8",
								},
							},
						},
					},
				},
			},
			expectedScore: 1,
		},
		"multiple-requests-with-middle-subrequests-allocated": {
			claims: []*resourceapi.ResourceClaim{
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-1",
						st.SubRequest("subreq-1", className, 1),
						st.SubRequest("subreq-2", className, 1),
						st.SubRequest("subreq-3", className, 1),
						st.SubRequest("subreq-4", className, 1),
					).
					NamedRequestWithPrioritizedList("req-2",
						st.SubRequest("subreq-1", className, 1),
						st.SubRequest("subreq-2", className, 1),
						st.SubRequest("subreq-3", className, 1),
						st.SubRequest("subreq-4", className, 1),
						st.SubRequest("subreq-5", className, 1),
					).
					Obj(),
			},
			allocations: nodeAllocation{
				allocationResults: []resourceapi.AllocationResult{
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1/subreq-4",
								},
								{
									Request: "req-2/subreq-5",
								},
							},
						},
					},
				},
			},
			expectedScore: 9,
		},
		"multiple-requests-with-top-subrequests-allocated": {
			claims: []*resourceapi.ResourceClaim{
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-1",
						st.SubRequest("subreq-1", className, 1),
						st.SubRequest("subreq-2", className, 1),
						st.SubRequest("subreq-3", className, 1),
						st.SubRequest("subreq-4", className, 1),
						st.SubRequest("subreq-5", className, 1),
						st.SubRequest("subreq-6", className, 1),
						st.SubRequest("subreq-7", className, 1),
						st.SubRequest("subreq-8", className, 1),
					).
					NamedRequestWithPrioritizedList("req-2",
						st.SubRequest("subreq-1", className, 1),
					).
					Obj(),
			},
			allocations: nodeAllocation{
				allocationResults: []resourceapi.AllocationResult{
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1/subreq-8",
								},
								{
									Request: "req-2/subreq-1",
								},
							},
						},
					},
				},
			},
			expectedScore: 9,
		},
		"multiple-claims-with-last-subrequests-allocated": {
			claims: []*resourceapi.ResourceClaim{
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-1",
						st.SubRequest("subreq-1", className, 1),
						st.SubRequest("subreq-2", className, 1),
						st.SubRequest("subreq-3", className, 1),
						st.SubRequest("subreq-4", className, 1),
						st.SubRequest("subreq-5", className, 1),
						st.SubRequest("subreq-6", className, 1),
						st.SubRequest("subreq-7", className, 1),
						st.SubRequest("subreq-8", className, 1),
					).
					Obj(),
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-2",
						st.SubRequest("subreq-1", className, 1),
						st.SubRequest("subreq-2", className, 1),
						st.SubRequest("subreq-3", className, 1),
						st.SubRequest("subreq-4", className, 1),
						st.SubRequest("subreq-5", className, 1),
						st.SubRequest("subreq-6", className, 1),
						st.SubRequest("subreq-7", className, 1),
						st.SubRequest("subreq-8", className, 1),
					).
					Obj(),
			},
			allocations: nodeAllocation{
				allocationResults: []resourceapi.AllocationResult{
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1/subreq-8",
								},
							},
						},
					},
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-2/subreq-8",
								},
							},
						},
					},
				},
			},
			expectedScore: 2,
		},
		"multiple-claims-with-top-subrequests-allocated": {
			claims: []*resourceapi.ResourceClaim{
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-1",
						st.SubRequest("subreq-1", className, 1),
					).
					Obj(),
				st.MakeResourceClaim().
					NamedRequestWithPrioritizedList("req-2",
						st.SubRequest("subreq-1", className, 1),
					).
					Obj(),
			},
			allocations: nodeAllocation{
				allocationResults: []resourceapi.AllocationResult{
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1/subreq-1",
								},
							},
						},
					},
					{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-2/subreq-1",
								},
							},
						},
					},
				},
			},
			expectedScore: 16,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			iterator := slices.All(tc.claims)
			score, err := computeScore(iterator, tc.allocations)
			if err != nil {
				if !tc.expectErr {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if tc.expectErr {
				t.Fatal("expected error, got none")
			}
			assert.Equal(t, tc.expectedScore, score)
		})
	}
}

// TestNormalizeScore 验证分数归一化逻辑。
func TestNormalizeScore(t *testing.T) {
	testcases := map[string]struct {
		scores         fwk.NodeScoreList
		expectedScores fwk.NodeScoreList
	}{
		"empty": {
			scores:         fwk.NodeScoreList{},
			expectedScores: fwk.NodeScoreList{},
		},
		"single-score": {
			scores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 42,
				},
			},
			expectedScores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 100,
				},
			},
		},
		"all-same": {
			scores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 8,
				},
				{
					Name:  "node-2",
					Score: 8,
				},
			},
			expectedScores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 100,
				},
				{
					Name:  "node-2",
					Score: 100,
				},
			},
		},
		"all-same-very-large": {
			scores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: math.MaxInt32,
				},
				{
					Name:  "node-2",
					Score: math.MaxInt32,
				},
			},
			expectedScores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 100,
				},
				{
					Name:  "node-2",
					Score: 100,
				},
			},
		},
		"max-and-min-values": {
			scores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: math.MaxInt32,
				},
				{
					Name:  "node-2",
					Score: 0,
				},
			},
			expectedScores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 100,
				},
				{
					Name:  "node-2",
					Score: 0,
				},
			},
		},
		"mid-value": {
			scores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 99,
				},
				{
					Name:  "node-2",
					Score: 98,
				},
				{
					Name:  "node-3",
					Score: 97,
				},
			},
			expectedScores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 100,
				},
				{
					Name:  "node-2",
					Score: 98,
				},
				{
					Name:  "node-3",
					Score: 97,
				},
			},
		},
		"large-spread-lost-precision": {
			scores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: math.MaxInt32,
				},
				{
					Name:  "node-2",
					Score: math.MaxInt32 - 1,
				},
				{
					Name:  "node-3",
					Score: 1,
				},
				{
					Name:  "node-4",
					Score: 0,
				},
			},
			expectedScores: fwk.NodeScoreList{
				{
					Name:  "node-1",
					Score: 100,
				},
				{
					Name:  "node-2",
					Score: 99,
				},
				{
					Name:  "node-3",
					Score: 0,
				},
				{
					Name:  "node-4",
					Score: 0,
				},
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			pl := &DynamicResources{
				enabled: true,
			}
			scores := tc.scores
			_ = pl.NormalizeScore(context.Background(), nil, nil, scores)
			assert.Equal(t, tc.expectedScores, scores)
		})
	}
}

// TestGatherAllocatedState 验证从 informer 缓存收集已分配状态的结果。
func TestGatherAllocatedState(t *testing.T) {
	tCtx := ktesting.Init(t)
	testGatherAllocatedState(tCtx)
}

// testGatherAllocatedState 执行已分配状态收集的用例表。
func testGatherAllocatedState(tCtx ktesting.TContext) {
	testcases := map[string]struct {
		allocatedResourceClaims   []*resourceapi.ResourceClaim
		inflightResourceClaims    map[types.UID]*resourceapi.ResourceClaim
		enabledConsumableCapacity bool
		expectErr                 bool
		expectedIDAllocated       int
		expectedSharedIDAllocated int
		expectedConsumedCapacity  string
	}{
		"no-claims": {
			expectedIDAllocated:       0,
			expectedSharedIDAllocated: 0,
		},
		"single-allocated-claim": {
			allocatedResourceClaims: []*resourceapi.ResourceClaim{
				allocatedClaim,
			},
			expectedIDAllocated:       1,
			expectedSharedIDAllocated: 0,
		},
		"single-allocated-claim-with-shared-device": {
			enabledConsumableCapacity: true,
			allocatedResourceClaims: []*resourceapi.ResourceClaim{
				allocatedClaimWithSharedDevice,
			},
			expectedIDAllocated:       0,
			expectedSharedIDAllocated: 1,
		},
		"single-allocated-claim-with-capacity": {
			enabledConsumableCapacity: true,
			allocatedResourceClaims: []*resourceapi.ResourceClaim{
				allocatedClaimWithConsumedCapacity,
			},
			expectedIDAllocated:       0,
			expectedSharedIDAllocated: 1,
			expectedConsumedCapacity:  "1",
		},
		"disabled-single-allocated-claim-with-capacity": {
			enabledConsumableCapacity: false,
			allocatedResourceClaims: []*resourceapi.ResourceClaim{
				allocatedClaimWithConsumedCapacity,
			},
			expectedIDAllocated:       1,
			expectedSharedIDAllocated: 0,
			expectedConsumedCapacity:  "",
		},
		"mixed-allocated-claim": {
			enabledConsumableCapacity: true,
			allocatedResourceClaims: []*resourceapi.ResourceClaim{
				allocatedClaim,
				allocatedClaimWithConsumedCapacity,
			},
			expectedIDAllocated:       1,
			expectedSharedIDAllocated: 1,
			expectedConsumedCapacity:  "1",
		},
		"add-inflight-allocated-claim-with-capacity": {
			enabledConsumableCapacity: true,
			allocatedResourceClaims: []*resourceapi.ResourceClaim{
				allocatedClaim,
				allocatedClaimWithConsumedCapacity,
			},
			inflightResourceClaims: map[types.UID]*resourceapi.ResourceClaim{
				"claim-2-uid": allocatedClaimWithConsumedCapacity2,
			},
			expectedIDAllocated:       1,
			expectedSharedIDAllocated: 2,
			expectedConsumedCapacity:  "2",
		},
		"disabled-inflight-allocated-claim-with-capacity": {
			enabledConsumableCapacity: false,
			allocatedResourceClaims: []*resourceapi.ResourceClaim{
				allocatedClaim,
				allocatedClaimWithConsumedCapacity,
			},
			inflightResourceClaims: map[types.UID]*resourceapi.ResourceClaim{
				"claim-2-uid": allocatedClaimWithConsumedCapacity2,
			},
			expectedIDAllocated:       2,
			expectedSharedIDAllocated: 0,
			expectedConsumedCapacity:  "",
		},
	}
	for name, tc := range testcases {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			featuregatetesting.SetFeatureGateDuringTest(tCtx, utilfeature.DefaultFeatureGate, features.DRAConsumableCapacity, tc.enabledConsumableCapacity)

			tCtx.Helper()
			logger := klog.FromContext(tCtx)
			assumeCacheInformer := &testInformer{}
			assumeCache := assumecache.NewAssumeCache(tCtx.Logger(), assumeCacheInformer, "", "", nil)
			draManager := &DefaultDRAManager{
				resourceClaimTracker: &claimTracker{
					cache:               assumeCache,
					inFlightAllocations: make(map[types.UID]inFlightAllocation),
					allocatedDevices:    newAllocatedDevices(logger),
				},
			}
			for _, obj := range tc.allocatedResourceClaims {
				draManager.resourceClaimTracker.allocatedDevices.handlers().OnAdd(obj, false)
			}
			if tc.inflightResourceClaims != nil {
				for claimUID, obj := range tc.inflightResourceClaims {
					assumeCacheInformer.add(obj)
					err := draManager.resourceClaimTracker.SignalClaimPendingAllocation(claimUID, obj)
					if err != nil {
						if !tc.expectErr {
							tCtx.Fatalf("unexpected error: %v", err)
							return
						}
					}
					if tc.expectErr {
						tCtx.Fatal("expected error, got none")
					}
				}
			}

			// 从这里开始正式执行收集与校验
			allocatedState, err := draManager.ResourceClaims().GatherAllocatedState()
			if err != nil {
				if !tc.expectErr {
					tCtx.Fatalf("unexpected error: %v", err)
					return
				}
			}
			if tc.expectErr {
				tCtx.Fatal("expected error, got none")
			}
			allocatedDeviceIDs := allocatedState.AllocatedDevices
			allocatedSharedDeviceIDs := allocatedState.AllocatedSharedDeviceIDs
			aggregatedCapacity := allocatedState.AggregatedCapacity

			// 校验各类计数与期望一致
			if allocatedDeviceIDs.Len() != tc.expectedIDAllocated {
				tCtx.Errorf("expected %d allocated device IDs, got %d", tc.expectedIDAllocated, allocatedDeviceIDs.Len())
			}
			if allocatedSharedDeviceIDs.Len() != tc.expectedSharedIDAllocated {
				tCtx.Errorf("expected %d allocated shared device IDs, got %d", tc.expectedSharedIDAllocated, allocatedSharedDeviceIDs.Len())
			}

			// 聚合容量必须初始化，否则后续读取会 panic
			if aggregatedCapacity == nil {
				tCtx.Error("aggregatedCapacity should not be nil")
			}
			if tc.expectedConsumedCapacity != "" {
				if len(aggregatedCapacity) == 0 {
					tCtx.Errorf("expected consumed capacity, got empty")
				}
				deviceID := schedulerapi.MakeDeviceID(driver, nodeName, sharedDeviceName)
				capacity := aggregatedCapacity[deviceID]
				if capacity == nil {
					tCtx.Errorf("expected aggregated capacity of %s, got nil", deviceID)
					return
				}
				value := capacity[capacityName]
				if value == nil {
					tCtx.Errorf("expected value of %s, got nil", capacityName)
					return
				}
				if value.Cmp(apiresource.MustParse(tc.expectedConsumedCapacity)) != 0 {
					tCtx.Errorf("expected value of %s to be %s, got %s", capacityName, tc.expectedConsumedCapacity, value)
				}
			} else if len(aggregatedCapacity) > 0 {
				tCtx.Errorf("got unexpected consumed capacity")
			}
		})
	}

}

// TestDynamicResources_DeferredResizeSkipped 验证延迟扩容的 Pod 跳过 PreFilter，
// 且 Filter 直接成功，不因 DRA 资源检查受阻。
func TestDynamicResources_DeferredResizeSkipped(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingSchedulerPreemption, true)
	ctx := context.Background()
	pod := st.MakePod().Name("p").UID("p").Condition(v1.PodResizePending, v1.ConditionTrue, v1.PodReasonDeferred).Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node1").Obj())

	pl := &DynamicResources{enabled: true, fts: feature.Features{EnableInPlacePodVerticalScalingSchedulerPreemption: true}}

	if preRes, preStatus := pl.PreFilter(ctx, nil, pod, nil); preStatus.Code() != fwk.Skip || preRes != nil {
		t.Errorf("PreFilter: got (res: %v, status: %v), want (nil, Skip)", preRes, preStatus.Code())
	}

	if filterStatus := pl.Filter(ctx, nil, pod, nodeInfo); filterStatus.Code() != fwk.Success {
		t.Errorf("Filter: got status %v, want Success (nil)", filterStatus.Code())
	}

	if _, postStatus := pl.PostFilter(ctx, nil, pod, nil); postStatus.Code() != fwk.Unschedulable || postStatus.Message() != "" {
		t.Errorf("PostFilter: got status (code: %v, msg: %q), want (Unschedulable, \"\")", postStatus.Code(), postStatus.Message())
	}
}

// TestPodGroupPostFilter 验证 PodGroup 级别的 PostFilter 行为。
func TestPodGroupPostFilter(t *testing.T) {
	tCtx := ktesting.Init(t)

	type testCase struct {
		pluginEnabled                         bool
		enableDRAWorkloadResourceClaims       bool
		enableDRAExtendedResource             bool
		enableTopologyAwareWorkloadScheduling bool
		podGroups                             []*schedulingapi.PodGroup
		claims                                []*resourceapi.ResourceClaim
		classes                               []*resourceapi.DeviceClass
		objs                                  []apiruntime.Object

		// PodGroupPostFilter 的输入
		unscheduledPods       []*v1.Pod
		unavailableClaimNames []string

		// 期望结果
		wantStatus   *fwk.Status
		verifyClaims func(tCtx ktesting.TContext, testCtx *testContext)
	}

	testcases := map[string]testCase{
		"disabled": {
			pluginEnabled:   false,
			podGroups:       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods: []*v1.Pod{groupedPodWithClaimName},
			wantStatus:      fwk.NewStatus(fwk.Unschedulable),
		},
		"empty": {
			pluginEnabled:   true,
			podGroups:       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods: []*v1.Pod{groupedPodWithClaimName},
			claims:          []*resourceapi.ResourceClaim{pendingClaim},
			wantStatus:      fwk.NewStatus(fwk.Unschedulable),
		},
		"deallocate-pod-level-claim": {
			pluginEnabled:                   true,
			enableDRAWorkloadResourceClaims: true,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods:                 []*v1.Pod{groupedPodWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{inUseClaim},
			objs:                            []apiruntime.Object{groupedPodWithClaimName},
			unavailableClaimNames:           []string{inUseClaim.Name},
			wantStatus:                      fwk.NewStatus(fwk.Unschedulable, "deallocation and deletion of ResourceClaims completed"),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				claim, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaim.Namespace).Get(tCtx, inUseClaim.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.Nil(tCtx, claim.Status.Allocation)
				assert.Nil(tCtx, claim.Status.ReservedFor)
			},
		},
		"deallocate-podgroup-level-claim": {
			pluginEnabled:                   true,
			enableDRAWorkloadResourceClaims: true,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods:                 []*v1.Pod{groupedPodWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{inUseClaimByPodGroup},
			objs:                            []apiruntime.Object{groupedPodWithClaimName},
			unavailableClaimNames:           []string{inUseClaimByPodGroup.Name},
			wantStatus:                      fwk.NewStatus(fwk.Unschedulable, "deallocation of PodGroup ResourceClaim completed"),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				claim, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaimByPodGroup.Namespace).Get(tCtx, inUseClaimByPodGroup.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.Nil(tCtx, claim.Status.Allocation)
				assert.Nil(tCtx, claim.Status.ReservedFor)
			},
		},
		"delete-pod-level-extended-claim": {
			pluginEnabled:             true,
			enableDRAExtendedResource: true,
			podGroups:                 []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods:           []*v1.Pod{groupedPodWithClaimName},
			claims: func() []*resourceapi.ResourceClaim {
				// 创建一个由 Pod 拥有的扩展资源 claim
				claim := inUseClaim.DeepCopy()
				claim.Name = "pod-extended-resources"
				if claim.Annotations == nil {
					claim.Annotations = make(map[string]string)
				}
				claim.Annotations[resourceapi.ExtendedResourceClaimAnnotation] = "true"
				claim.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "Pod",
						Name:       groupedPodWithClaimName.Name,
						UID:        groupedPodWithClaimName.UID,
						Controller: new(true),
					},
				}
				return []*resourceapi.ResourceClaim{claim, pendingClaim}
			}(),
			objs:                  []apiruntime.Object{groupedPodWithClaimName},
			unavailableClaimNames: []string{"pod-extended-resources"},
			wantStatus:            fwk.NewStatus(fwk.Unschedulable, "deallocation and deletion of ResourceClaims completed"),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				_, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaim.Namespace).Get(tCtx, "pod-extended-resources", metav1.GetOptions{})
				assert.Error(tCtx, err) // Should be deleted!
			},
		},
		"unreserve-podgroup-claim": {
			pluginEnabled:                   true,
			enableDRAWorkloadResourceClaims: true,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods:                 []*v1.Pod{groupedPodWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{inUseClaimByPodGroup},
			objs:                            []apiruntime.Object{groupedPodWithClaimName},
			wantStatus:                      fwk.NewStatus(fwk.Unschedulable, "ResourceClaim unreserved for PodGroup"),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				claim, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaimByPodGroup.Namespace).Get(tCtx, inUseClaimByPodGroup.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.Empty(tCtx, claim.Status.ReservedFor)
			},
		},
		"unreserve-podgroup-multiple-unscheduled-pods": {
			pluginEnabled:                   true,
			enableDRAWorkloadResourceClaims: true,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods:                 []*v1.Pod{groupedPodWithClaimName, groupedPodWithClaimName2},
			claims:                          []*resourceapi.ResourceClaim{inUseClaimByPodGroup},
			objs:                            []apiruntime.Object{groupedPodWithClaimName, groupedPodWithClaimName2},
			wantStatus:                      fwk.NewStatus(fwk.Unschedulable, "ResourceClaim unreserved for PodGroup"),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				claim, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaimByPodGroup.Namespace).Get(tCtx, inUseClaimByPodGroup.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.Empty(tCtx, claim.Status.ReservedFor)
			},
		},
		"mixed-pod-level-and-podgroup-claims": {
			pluginEnabled:                   true,
			enableDRAWorkloadResourceClaims: true,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods:                 []*v1.Pod{groupedPodWithClaimName, groupedPodWithPodLevelClaim},
			claims:                          []*resourceapi.ResourceClaim{inUseClaimByPodGroup, inUseClaimForPodLevel},
			objs:                            []apiruntime.Object{groupedPodWithClaimName, groupedPodWithPodLevelClaim},
			unavailableClaimNames:           []string{inUseClaimForPodLevel.Name},
			wantStatus:                      fwk.NewStatus(fwk.Unschedulable, "ResourceClaim unreserved for PodGroup"),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				claimByPG, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaimByPodGroup.Namespace).Get(tCtx, inUseClaimByPodGroup.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.NotNil(tCtx, claimByPG.Status.Allocation)
				assert.Empty(tCtx, claimByPG.Status.ReservedFor)

				claim2, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaimForPodLevel.Namespace).Get(tCtx, inUseClaimForPodLevel.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.Nil(tCtx, claim2.Status.Allocation)
				assert.Nil(tCtx, claim2.Status.ReservedFor)
			},
		},
		"skip-deallocate-feature-disabled": {
			pluginEnabled:                   true,
			enableDRAWorkloadResourceClaims: false,
			podGroups:                       []*schedulingapi.PodGroup{podGroupWithClaimName},
			unscheduledPods:                 []*v1.Pod{groupedPodWithClaimName},
			claims:                          []*resourceapi.ResourceClaim{inUseClaimByPodGroup},
			objs:                            []apiruntime.Object{groupedPodWithClaimName},
			wantStatus:                      fwk.NewStatus(fwk.Unschedulable),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				claim, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaimByPodGroup.Namespace).Get(tCtx, inUseClaimByPodGroup.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.NotEmpty(tCtx, claim.Status.ReservedFor)
			},
		},
		"skip-deallocate-topology-aware-podgroup": {
			pluginEnabled:                         true,
			enableDRAWorkloadResourceClaims:       true,
			enableTopologyAwareWorkloadScheduling: true,
			podGroups:                             []*schedulingapi.PodGroup{podGroupWithClaimNameAndConstraints},
			unscheduledPods:                       []*v1.Pod{groupedPodWithClaimName},
			claims:                                []*resourceapi.ResourceClaim{inUseClaimByPodGroup},
			objs:                                  []apiruntime.Object{groupedPodWithClaimName},
			wantStatus:                            fwk.NewStatus(fwk.Unschedulable),
			verifyClaims: func(tCtx ktesting.TContext, testCtx *testContext) {
				claim, err := testCtx.client.ResourceV1().ResourceClaims(inUseClaimByPodGroup.Namespace).Get(tCtx, inUseClaimByPodGroup.Name, metav1.GetOptions{})
				require.NoError(tCtx, err)
				assert.NotEmpty(tCtx, claim.Status.ReservedFor)
			},
		},
	}

	for name, tc := range testcases {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			featuregatetesting.SetFeatureGatesDuringTest(tCtx, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.DRAWorkloadResourceClaims:       tc.enableDRAWorkloadResourceClaims,
				features.TopologyAwareWorkloadScheduling: tc.enableTopologyAwareWorkloadScheduling,
				features.GenericWorkload:                 true,
			})

			feats := feature.Features{
				EnableDRAAdminAccess:                  true,
				EnableDRADeviceBindingConditions:      true,
				EnableDRAResourceClaimDeviceStatus:    true,
				EnableDRASchedulerFilterTimeout:       true,
				EnableDynamicResourceAllocation:       tc.pluginEnabled,
				EnableDRAWorkloadResourceClaims:       tc.enableDRAWorkloadResourceClaims,
				EnableDRAExtendedResource:             tc.enableDRAExtendedResource,
				EnableTopologyAwareWorkloadScheduling: tc.enableTopologyAwareWorkloadScheduling,
			}

			testCtx := setup(tCtx, nil, []*v1.Node{workerNode}, tc.claims, tc.classes, tc.podGroups, tc.objs, feats, false, nil)
			testCtx.p.enabled = tc.pluginEnabled

			podGroupCycleState := framework.NewCycleState()
			podGroupState := &podGroupStateData{
				pendingAllocations: make(map[types.UID]sets.Set[types.UID]),
				podsStateData:      make(map[types.NamespacedName]*stateData),
			}
			podGroupCycleState.Write(stateKey, podGroupState)

			if tc.pluginEnabled {
				claimsList, err := testCtx.client.ResourceV1().ResourceClaims("").List(tCtx, metav1.ListOptions{})
				require.NoError(tCtx, err)
				var testClaims []*resourceapi.ResourceClaim
				for i := range claimsList.Items {
					testClaims = append(testClaims, &claimsList.Items[i])
				}

				for _, pod := range tc.unscheduledPods {
					// 用 Pod 的 claim 及不可用 claim 初始化 stateData
					s := &stateData{}
					userClaims, err := testCtx.p.podResourceClaims(pod)
					require.NoError(tCtx, err)
					extendedResourceClaim := findExtendedResourceClaim(pod, testClaims)
					s.claims = newClaimStore(userClaims, extendedResourceClaim, nil)

					if len(tc.unavailableClaimNames) > 0 {
						s.unavailableClaims = sets.New[int]()
						for _, name := range tc.unavailableClaimNames {
							for index, claim := range s.claims.all() {
								if claim.Name == name {
									s.unavailableClaims.Insert(index)
								}
							}
						}
					}
					podGroupState.podsStateData[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}] = s
				}
			}

			pgInfo := &framework.PodGroupInfo{
				Name:            tc.podGroups[0].Name,
				Namespace:       tc.podGroups[0].Namespace,
				UnscheduledPods: tc.unscheduledPods,
				PodGroup:        tc.podGroups[0],
			}

			mockSchedulingFunc := func(ctx context.Context) (*fwk.PodGroupAssignments, *fwk.Status) {
				return nil, fwk.NewStatus(fwk.Unschedulable)
			}

			_, gotStatus := testCtx.p.PodGroupPostFilter(tCtx, podGroupCycleState, pgInfo, mockSchedulingFunc)

			if tc.wantStatus.AsError() != nil {
				require.ErrorContains(tCtx, gotStatus.AsError(), tc.wantStatus.AsError().Error())
			} else {
				assert.Equal(tCtx, tc.wantStatus, gotStatus)
			}

			if tc.verifyClaims != nil {
				tc.verifyClaims(tCtx, testCtx)
			}
		})
	}
}

// TestPreQueueingHint 验证 preQueueingHint 的默认行为。
func TestPreQueueingHint(t *testing.T) {
	tests := map[string]struct {
		oldObj, newObj interface{}
		wantKeys       sets.Set[string]
	}{
		"claim with no pod in indexer returns empty set": {
			newObj: &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-1",
					Namespace: "ns1",
				},
			},
			wantKeys: sets.New[string](), // no pod in indexer
		},
		"shared claim returns nil": {
			newObj: &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shared-claim",
					Namespace: "ns1",
				},
			},
			wantKeys: sets.New[string](), // no pod in indexer
		},
		"claim with no matching pods returns empty set": {
			newObj: &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-2",
					Namespace: "ns1",
				},
			},
			wantKeys: sets.New[string](), // no pod in indexer
		},
		"delete event uses oldObj": {
			oldObj: &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-3",
					Namespace: "ns2",
				},
			},
			newObj:   nil,
			wantKeys: sets.New[string](), // no pod in indexer
		},
		"delete of allocated claim returns AllPods": {
			oldObj: &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-allocated",
					Namespace: "ns1",
				},
				Status: resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{},
				},
			},
			newObj:   nil,
			wantKeys: nil, // AllPods - deletion frees resources
		},

		"deallocation returns nil to evaluate all pods": {
			oldObj: &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-4",
					Namespace: "ns1",
				},
				Status: resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{},
				},
			},
			newObj: &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "claim-4",
					Namespace: "ns1",
				},
			},
			wantKeys: nil,
		},
		"non-claim object returns nil": {
			newObj:   "not-a-claim",
			wantKeys: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			logger := klog.Background()
			pl := &DynamicResources{podIndexer: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{podResourceClaimIndexPrefix + "-test": podResourceClaimIndexFunc}), podResourceClaimIndex: podResourceClaimIndexPrefix + "-test"}
			got, err := pl.preQueueingHint(logger, tc.oldObj, tc.newObj)
			if err != nil {
				t.Fatalf("preQueueingHint returned unexpected error: %v", err)
			}
			if tc.wantKeys == nil {
				// wantKeys 为 nil 表示期望 AllPods=true
				if !got.AllPods {
					t.Errorf("expected AllPods=true, got %+v", got)
				}
			} else {
				if got.AllPods {
					t.Errorf("expected AllPods=false with specific pods, got AllPods=true")
				}
				gotPods := sets.New[string]()
				for _, nn := range got.Pods {
					gotPods.Insert(nn.Name + "_" + nn.Namespace)
				}
				wantPods := sets.New[string]()
				for k := range tc.wantKeys {
					wantPods.Insert(k)
				}
				if !gotPods.Equal(wantPods) {
					t.Errorf("expected pods %v, got %v", wantPods, gotPods)
				}
			}
		})
	}
}

// TestPreQueueingHint_WithPodInIndexer 验证当索引器中存在引用某 claim 的 Pod 时，
// preQueueingHint 返回该 Pod 的 key。
func TestPreQueueingHint_WithPodInIndexer(t *testing.T) {
	logger := klog.Background()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{podResourceClaimIndexPrefix + "-test": podResourceClaimIndexFunc})

	// 添加一个引用 ns1/claim-x 的 Pod
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pod", Namespace: "ns1"},
		Spec: v1.PodSpec{
			ResourceClaims: []v1.PodResourceClaim{
				{Name: "gpu", ResourceClaimName: new("claim-x")},
			},
		},
	}
	if err := indexer.Add(pod); err != nil {
		t.Fatal(err)
	}

	pl := &DynamicResources{podIndexer: indexer, podResourceClaimIndex: podResourceClaimIndexPrefix + "-test"}

	got, err := pl.preQueueingHint(logger, nil, &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-x", Namespace: "ns1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.AllPods {
		t.Fatal("expected AllPods=false, got true")
	}
	if len(got.Pods) != 1 || got.Pods[0].Name != "my-pod" || got.Pods[0].Namespace != "ns1" {
		t.Errorf("expected [{my-pod ns1}], got %v", got.Pods)
	}
}

// TestPreQueueingHint_SharedClaimMultiplePods 验证共享 claim 被多个 Pod 引用时，
// preQueueingHint 返回所有引用它的 Pod。
func TestPreQueueingHint_SharedClaimMultiplePods(t *testing.T) {
	logger := klog.Background()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{podResourceClaimIndexPrefix + "-test": podResourceClaimIndexFunc})

	// 添加多个引用同一共享 claim 的 Pod
	for _, name := range []string{"pod-a", "pod-b", "pod-c"} {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns1"},
			Spec: v1.PodSpec{
				ResourceClaims: []v1.PodResourceClaim{
					{Name: "shared", ResourceClaimName: new("shared-claim")},
				},
			},
		}
		if err := indexer.Add(pod); err != nil {
			t.Fatal(err)
		}
	}

	pl := &DynamicResources{podIndexer: indexer, podResourceClaimIndex: podResourceClaimIndexPrefix + "-test"}

	got, err := pl.preQueueingHint(logger, nil, &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-claim", Namespace: "ns1"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.AllPods {
		t.Fatal("expected AllPods=false, got true")
	}
	if len(got.Pods) != 3 {
		t.Errorf("expected 3 pods, got %d: %v", len(got.Pods), got.Pods)
	}
}
