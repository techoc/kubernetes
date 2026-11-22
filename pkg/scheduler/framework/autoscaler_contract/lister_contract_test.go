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

// 本文件用于检测 Listers 的接口是否发生变化。
// 这对于导入这些 Listers 的下游项目很重要，避免破坏它们的代码。

package contract

import (
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/structured/schedulerapi"
	fwk "k8s.io/kube-scheduler/framework"
)

var _ fwk.NodeInfoLister = &nodeInfoListerContract{}
var _ fwk.StorageInfoLister = &storageInfoListerContract{}
var _ fwk.SharedLister = &shareListerContract{}
var _ fwk.ResourceSliceLister = &resourceSliceListerContract{}
var _ fwk.PodGroupStateLister = &podGroupStateListerContract{}
var _ fwk.PodGroupLister = &podGroupListerContract{}
var _ fwk.PodGroupState = &podGroupStateContract{}
var _ fwk.CompositePodGroupStateLister = &compositePodGroupStateListerContract{}
var _ fwk.CompositePodGroupLister = &compositePodGroupListerContract{}
var _ fwk.CompositePodGroupState = &compositePodGroupStateContract{}
var _ fwk.DeviceClassLister = &deviceClassListerContract{}
var _ fwk.ResourceClaimTracker = &resourceClaimTrackerContract{}
var _ fwk.DeviceClassResolver = &deviceClassResolverContract{}
var _ fwk.SharedDRAManager = &sharedDRAManagerContract{}

// nodeInfoListerContract 用于锁定 fwk.NodeInfoLister 接口的方法签名。
type nodeInfoListerContract struct{}

func (c *nodeInfoListerContract) List() ([]fwk.NodeInfo, error) {
	return nil, nil
}

func (c *nodeInfoListerContract) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return nil, nil
}

func (c *nodeInfoListerContract) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nil, nil
}

func (c *nodeInfoListerContract) HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nil, nil
}

func (c *nodeInfoListerContract) Get(_ string) (fwk.NodeInfo, error) {
	return nil, nil
}

// storageInfoListerContract 用于锁定 fwk.StorageInfoLister 接口的方法签名。
type storageInfoListerContract struct{}

func (c *storageInfoListerContract) IsPVCUsedByPods(_ string) bool {
	return false
}

// shareListerContract 用于锁定 fwk.SharedLister 接口的方法签名。
type shareListerContract struct{}

func (c *shareListerContract) NodeInfos() fwk.NodeInfoLister {
	return nil
}

func (c *shareListerContract) StorageInfos() fwk.StorageInfoLister {
	return nil
}

func (c *shareListerContract) PodGroupStates() fwk.PodGroupStateLister {
	return nil
}

func (c *shareListerContract) PodGroups() fwk.PodGroupLister {
	return nil
}

func (c *shareListerContract) CompositePodGroupStates() fwk.CompositePodGroupStateLister {
	return nil
}

func (c *shareListerContract) CompositePodGroups() fwk.CompositePodGroupLister {
	return nil
}

// podGroupListerContract 用于锁定 fwk.PodGroupLister 接口的方法签名。
type podGroupListerContract struct{}

func (c *podGroupListerContract) Get(_ string, _ string) (*schedulingapi.PodGroup, error) {
	return nil, nil
}

// podGroupStateListerContract 用于锁定 fwk.PodGroupStateLister 接口的方法签名。
type podGroupStateListerContract struct{}

func (c *podGroupStateListerContract) Get(_ string, _ string) (fwk.PodGroupState, error) {
	return nil, nil
}

// podGroupStateContract 用于锁定 fwk.PodGroupState 接口的方法签名。
type podGroupStateContract struct{}

func (c *podGroupStateContract) AllPods() sets.Set[types.UID] {
	return nil
}

func (c *podGroupStateContract) UnscheduledPods() map[string]*v1.Pod {
	return nil
}

func (c *podGroupStateContract) AssumedPods() sets.Set[types.UID] {
	return nil
}

func (c *podGroupStateContract) AssignedPods() sets.Set[types.UID] {
	return nil
}

func (c *podGroupStateContract) AllPodsCount() int {
	return 0
}

func (c *podGroupStateContract) ScheduledPodsCount() int {
	return 0
}

func (c *podGroupStateContract) ScheduledPods() []*v1.Pod {
	return nil
}

// compositePodGroupListerContract 用于锁定 fwk.CompositePodGroupLister 接口的方法签名。
type compositePodGroupListerContract struct{}

func (c *compositePodGroupListerContract) Get(_ string, _ string) (*schedulingv1alpha3.CompositePodGroup, error) {
	return nil, nil
}

// compositePodGroupStateListerContract 用于锁定 fwk.CompositePodGroupStateLister 接口的方法签名。
type compositePodGroupStateListerContract struct{}

func (c *compositePodGroupStateListerContract) Get(_ string, _ string) (fwk.CompositePodGroupState, error) {
	return nil, nil
}

// compositePodGroupStateContract 用于锁定 fwk.CompositePodGroupState 接口的方法签名。
type compositePodGroupStateContract struct{}

func (c *compositePodGroupStateContract) GetChildren() []fwk.EntityKey {
	return nil
}

// resourceSliceListerContract 用于锁定 fwk.ResourceSliceLister 接口的方法签名。
type resourceSliceListerContract struct{}

func (c *resourceSliceListerContract) ListWithDeviceTaintRules() ([]*resourceapi.ResourceSlice, error) {
	return nil, nil
}

// deviceClassListerContract 用于锁定 fwk.DeviceClassLister 接口的方法签名。
type deviceClassListerContract struct{}

func (c *deviceClassListerContract) List() ([]*resourceapi.DeviceClass, error) {
	return nil, nil
}

func (c *deviceClassListerContract) Get(_ string) (*resourceapi.DeviceClass, error) {
	return nil, nil
}

// resourceClaimTrackerContract 用于锁定 fwk.ResourceClaimTracker 接口的方法签名。
type resourceClaimTrackerContract struct{}

func (r *resourceClaimTrackerContract) List() ([]*resourceapi.ResourceClaim, error) {
	return nil, nil
}

func (r *resourceClaimTrackerContract) Get(_, _ string) (*resourceapi.ResourceClaim, error) {
	return nil, nil
}

func (r *resourceClaimTrackerContract) ListAllAllocatedDevices() (sets.Set[schedulerapi.DeviceID], error) {
	return nil, nil
}

func (r *resourceClaimTrackerContract) GatherAllocatedState() (*schedulerapi.AllocatedState, error) {
	return nil, nil
}

func (r *resourceClaimTrackerContract) GetPendingAllocation(_ types.UID) *resourceapi.AllocationResult {
	return nil
}

func (r *resourceClaimTrackerContract) SignalClaimPendingAllocation(_ types.UID, _ *resourceapi.ResourceClaim) error {
	return nil
}

func (r *resourceClaimTrackerContract) MaybeRemoveClaimPendingAllocation(_ types.UID, _ bool) (deleted bool) {
	return false
}

func (r *resourceClaimTrackerContract) AssumeClaimAfterAPICall(_ *resourceapi.ResourceClaim) error {
	return nil
}

func (r *resourceClaimTrackerContract) AssumedClaimRestore(_, _ string) {
}

// sharedDRAManagerContract 用于锁定 fwk.SharedDRAManager 接口的方法签名。
type sharedDRAManagerContract struct{}

func (s *sharedDRAManagerContract) ResourceClaims() fwk.ResourceClaimTracker {
	return nil
}

func (s *sharedDRAManagerContract) ResourceSlices() fwk.ResourceSliceLister {
	return nil
}

func (s *sharedDRAManagerContract) DeviceClasses() fwk.DeviceClassLister {
	return nil
}

func (s *sharedDRAManagerContract) DeviceClassResolver() fwk.DeviceClassResolver {
	return nil
}

// deviceClassResolverContract 用于锁定 fwk.DeviceClassResolver 接口的方法签名。
type deviceClassResolverContract struct{}

func (d *deviceClassResolverContract) GetDeviceClass(_ v1.ResourceName) *resourceapi.DeviceClass {
	return nil
}
