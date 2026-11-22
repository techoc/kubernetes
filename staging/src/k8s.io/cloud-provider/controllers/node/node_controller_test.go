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

package node

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"
	cloudproviderapi "k8s.io/cloud-provider/api"
	fakecloud "k8s.io/cloud-provider/fake"
	_ "k8s.io/controller-manager/pkg/features/register"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

// Test_syncNode 是 syncNode 的主表驱动测试，覆盖节点注册/初始化的完整语义：
// 从云侧获取 ProviderID、地址、类型与拓扑标签，移除 external-cloud-provider taint，
// 并验证云实现返回错误或元数据缺失时的行为。前半部分用例走 Instances，
// 后半部分以 "[instanceV2]" 前缀的用例走 InstancesV2。
func Test_syncNode(t *testing.T) {
	// 每个用例给出 fakeCloud 的返回、初始 Node 与期望的最终 Node；比较整个对象，
	// 因而同时约束字段、标签与 taint 的最终状态。
	tests := []struct {
		name         string
		fakeCloud    *fakecloud.Cloud
		existingNode *v1.Node
		updatedNode  *v1.Node
		expectedErr  bool
	}{
		{
			// 经典路径：按节点名取到实例，写入 ProviderID 与三类地址。
			name: "node initialized with hostname",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ErrByProviderID: nil,
				Err:             nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "nodeoverriden",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
				},
			},
		},
		{
			name: "node initialized with hostname and cloud provider missing hostname",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ErrByProviderID: nil,
				Err:             nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "nodeoverriden",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
						{
							Type:    v1.NodeHostName,
							Address: "nodeoverriden",
						},
					},
				},
			},
		},
		{
			name: "node initialized with provider ID",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ErrByProviderID: nil,
				Err:             nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
				},
			},
		},
		{
			// 没有 external-cloud-provider taint 的节点不归 CCM 初始化，syncNode 应直接返回。
			name: "node ignored",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"):        "t1.micro",
					types.NodeName("fake://12345"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			// 同时写入新旧两套 topology 标签；旧标签用于兼容未升级的组件。
			name: "zone/region topology labels added",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Provider: "aws",
				Zone: cloudprovider.Zone{
					FailureDomain: "us-west-1a",
					Region:        "us-west",
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels:            map[string]string{},
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						"failure-domain.beta.kubernetes.io/region": "us-west",
						"failure-domain.beta.kubernetes.io/zone":   "us-west-1a",
						"topology.kubernetes.io/region":            "us-west",
						"topology.kubernetes.io/zone":              "us-west-1a",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
				},
				Spec: v1.NodeSpec{
					ProviderID: "aws://12345",
				},
			},
		},
		{
			name: "node addresses",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes:     map[types.NodeName]string{},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels:            map[string]string{},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
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
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
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
		{
			// InstancesV2 返回 (nil, nil) 表示元数据暂未就绪，节点应保持原样等待重试。
			name: "nil instanceMetadata provided by InstanceV2",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				Err:               nil,
				OverrideInstanceMetadata: func(ctx context.Context, node *v1.Node) (*cloudprovider.InstanceMetadata, error) {
					return nil, nil
				},
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
		},
		{
			// 用户通过注解指定节点 IP 时，地址应包含该 IP；后续三个用例覆盖
			// 地址非法、地址不在云返回列表中、以及地址缺失的错误分支。
			name: "provided node IP address",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
		},
		{
			name:        "provided node IP address is not valid",
			expectedErr: true,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "invalid-ip",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "invalid-ip",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
		},
		{
			name:        "provided node IP address is not present",
			expectedErr: true,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.2",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.2",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
		},
		{
			// 已有 providerID 时不应被云返回值覆盖，只补齐地址等缺失信息。
			name: "provider ID already set",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes:     map[types.NodeName]string{},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: false,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels:            map[string]string{},
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
				Spec: v1.NodeSpec{
					ProviderID: "test-provider-id",
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
				},
				Spec: v1.NodeSpec{
					ProviderID: "test-provider-id",
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
		{ // for backward compatibility the cloud providers that does not implement
			// providerID does not block the node initialization
			// 云实现不支持 providerID 时，不能伪造 ProviderID，应保留节点现有信息并成功返回。
			name:        "provider ID not implemented",
			expectedErr: false,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes:     map[types.NodeName]string{},
				Provider:          "test",
				ExtID:             map[types.NodeName]string{},
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): cloudprovider.NotImplemented,
				},
				Err: nil,
			},
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{},
				},
			},
		},
		{
			// 以下为 InstancesV2 用例：元数据由一次 InstanceMetadata 调用返回，
			// 期望结果与对应 Instances 用例保持一致，同时验证附加标签的合并逻辑。
			name: "[instanceV2] node initialized with provider ID",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ErrByProviderID: nil,
				Err:             nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "fake://12345",
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
				},
			},
		},
		{
			name: "[instanceV2] node ignored",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"):        "t1.micro",
					types.NodeName("fake://12345"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "[instanceV2] zone/region topology labels added",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				ProviderID: map[types.NodeName]string{
					types.NodeName("node0"): "fake://12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Provider: "aws",
				Zone: cloudprovider.Zone{
					FailureDomain: "us-west-1a",
					Region:        "us-west",
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels:            map[string]string{},
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels: map[string]string{
						"failure-domain.beta.kubernetes.io/region": "us-west",
						"failure-domain.beta.kubernetes.io/zone":   "us-west-1a",
						"topology.kubernetes.io/region":            "us-west",
						"topology.kubernetes.io/zone":              "us-west-1a",
					},
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
				},
			},
		},
		{
			name: "[instanceV2] node addresses",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes:     map[types.NodeName]string{},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				ProviderID: map[types.NodeName]string{
					types.NodeName("node0"): "fake://12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels:            map[string]string{},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
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
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "fake://12345",
				},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
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
		{
			name: "[instanceV2] provided node IP address",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.aws.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
		},
		{
			// 云厂商通过 AdditionalLabels 补充自定义标签，应全部写入节点。
			name: "[instanceV2] provided additional labels",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
				Zone: cloudprovider.Zone{
					FailureDomain: "us-west-1a",
					Region:        "us-west",
				},
				AdditionalLabels: map[string]string{
					"topology.k8s.cp/zone-id": "az1",
					"my.custom.label/foo":     "bar",
				},
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.cp.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
					Labels: map[string]string{
						"failure-domain.beta.kubernetes.io/region": "us-west",
						"failure-domain.beta.kubernetes.io/zone":   "us-west-1a",
						"topology.kubernetes.io/region":            "us-west",
						"topology.kubernetes.io/zone":              "us-west-1a",
						"topology.k8s.cp/zone-id":                  "az1",
						"my.custom.label/foo":                      "bar",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.cp.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
		},
		{
			// 节点上不存在（或值为空）的附加标签不应产生无意义的空值写入。
			name: "[instanceV2] provided additional labels with labels to discard",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: true,
				Err:                nil,
				Zone: cloudprovider.Zone{
					FailureDomain: "us-west-1a",
					Region:        "us-west",
				},
				AdditionalLabels: map[string]string{
					// Kubernetes reserves k8s.io and kubernetes.io namespaces
					// and should be discarded
					"topology.kubernetes.io/region": "us-other-west",
					"topology.k8s.io/region":        "us-other-west",
					"kubernetes.io/region":          "us-other-west",
					"k8s.io/region":                 "us-other-west",
					// Should discard labels that already exist
					"my.custom.label/foo": "bar",
					"my.custom.label/bar": "foo",
					// Should add labels that not match regex
					"app.k8snio/bar": "foo",
					"k8snio/bar":     "foo",
				},
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
					Labels: map[string]string{
						"my.custom.label/foo": "fizz",
						"my.custom.label/bar": "foo",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.cp.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
			updatedNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Annotations: map[string]string{
						cloudproviderapi.AnnotationAlphaProvidedIPAddr: "10.0.0.1",
					},
					Labels: map[string]string{
						"failure-domain.beta.kubernetes.io/region": "us-west",
						"failure-domain.beta.kubernetes.io/zone":   "us-west-1a",
						"topology.kubernetes.io/region":            "us-west",
						"topology.kubernetes.io/zone":              "us-west-1a",
						"my.custom.label/foo":                      "fizz",
						"my.custom.label/bar":                      "foo",
						"app.k8snio/bar":                           "foo",
						"k8snio/bar":                               "foo",
					},
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
					ProviderID: "node0.cp.12345",
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
					},
				},
			},
		},
		{
			name: "[instanceV2] provider ID already set",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes:     map[types.NodeName]string{},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ExistsByProviderID: false,
				Err:                nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
					Labels:            map[string]string{},
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
				Spec: v1.NodeSpec{
					ProviderID: "test-provider-id",
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
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
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeHostName,
							Address: "node0.cloud.internal",
						},
						{
							Type:    v1.NodeInternalIP,
							Address: "10.0.0.1",
						},
						{
							Type:    v1.NodeExternalIP,
							Address: "132.143.154.163",
						},
					},
				},
				Spec: v1.NodeSpec{
					ProviderID: "test-provider-id",
					Taints: []v1.Taint{
						{
							Key:    "ImproveCoverageTaint",
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
		{
			name:        "[instanceV2] provider ID not implemented",
			expectedErr: true,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes:     map[types.NodeName]string{},
				Provider:          "test",
				ExtID:             map[types.NodeName]string{},
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): cloudprovider.NotImplemented,
				},
				Err: nil,
			},
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
		{
			// InstanceMetadata 出错时应向上返回错误，且不产生部分更新。
			name:        "[instanceV2] error getting InstanceMetadata",
			expectedErr: true,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes:     map[types.NodeName]string{},
				Provider:          "test",
				ExtID:             map[types.NodeName]string{},
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): cloudprovider.NotImplemented,
				},
				MetadataErr: errors.New("metadata error"),
			},
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			updatedNode: &v1.Node{
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
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
	}

	// 每个用例独立创建 fake clientset 与 informer，避免用例间共享状态。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			clientset := fake.NewSimpleClientset(test.existingNode)
			factory := informers.NewSharedInformerFactory(clientset, 0)

			eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
			// 直接构造控制器（而非 startCloudNodeController），以便精确控制依赖并避免启动 goroutine。
			cloudNodeController := &CloudNodeController{
				kubeClient:                clientset,
				nodeInformer:              factory.Core().V1().Nodes(),
				nodesLister:               factory.Core().V1().Nodes().Lister(),
				cloud:                     test.fakeCloud,
				recorder:                  eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cloud-node-controller"}),
				nodeStatusUpdateFrequency: 1 * time.Second,
			}

			stopCh := make(chan struct{})
			defer close(stopCh)

			factory.Start(stopCh)
			factory.WaitForCacheSync(stopCh)

			w := eventBroadcaster.StartLogging(klog.Infof)
			defer w.Stop()

			err := cloudNodeController.syncNode(ctx, test.existingNode.Name)
			if (err != nil) != test.expectedErr {
				t.Fatalf("error got: %v expected: %v", err, test.expectedErr)
			}

			updatedNode, err := clientset.CoreV1().Nodes().Get(ctx, test.existingNode.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("error getting updated nodes: %v", err)
			}

			// 整体比较能同时捕捉多余/缺失的标签、taint 与地址。
			if !cmp.Equal(updatedNode, test.updatedNode) {
				t.Errorf("unexpected node %s", cmp.Diff(updatedNode, test.updatedNode))
			}
		})
	}
}

// Test_reconcileNodeLabels 验证旧 topology/instance-type 标签会被补齐或纠正为稳定标签，
// 包括“缺失”“已一致”“新旧标签值冲突”三种情况。
func Test_reconcileNodeLabels(t *testing.T) {
	testcases := []struct {
		name           string
		labels         map[string]string
		expectedLabels map[string]string
		expectedErr    error
	}{
		{
			// 仅有旧标签时，应补齐对应的新标签，旧标签保留。
			name: "requires reconcile",
			labels: map[string]string{
				v1.LabelFailureDomainBetaZone:   "foo",
				v1.LabelFailureDomainBetaRegion: "bar",
				v1.LabelInstanceType:            "the-best-type",
			},
			expectedLabels: map[string]string{
				v1.LabelFailureDomainBetaZone:   "foo",
				v1.LabelFailureDomainBetaRegion: "bar",
				v1.LabelTopologyZone:            "foo",
				v1.LabelTopologyRegion:          "bar",
				v1.LabelInstanceType:            "the-best-type",
				v1.LabelInstanceTypeStable:      "the-best-type",
			},
			expectedErr: nil,
		},
		{
			name: "doesn't require reconcile",
			labels: map[string]string{
				v1.LabelFailureDomainBetaZone:   "foo",
				v1.LabelFailureDomainBetaRegion: "bar",
				v1.LabelTopologyZone:            "foo",
				v1.LabelTopologyRegion:          "bar",
				v1.LabelInstanceType:            "the-best-type",
				v1.LabelInstanceTypeStable:      "the-best-type",
			},
			expectedLabels: map[string]string{
				v1.LabelFailureDomainBetaZone:   "foo",
				v1.LabelFailureDomainBetaRegion: "bar",
				v1.LabelTopologyZone:            "foo",
				v1.LabelTopologyRegion:          "bar",
				v1.LabelInstanceType:            "the-best-type",
				v1.LabelInstanceTypeStable:      "the-best-type",
			},
			expectedErr: nil,
		},
		{
			// 新旧标签冲突时以旧标签（源数据）为准覆盖新标签，保证标签最终一致。
			name: "require reconcile -- secondary labels are different from primary",
			labels: map[string]string{
				v1.LabelFailureDomainBetaZone:   "foo",
				v1.LabelFailureDomainBetaRegion: "bar",
				v1.LabelTopologyZone:            "wrongfoo",
				v1.LabelTopologyRegion:          "wrongbar",
				v1.LabelInstanceType:            "the-best-type",
				v1.LabelInstanceTypeStable:      "the-wrong-type",
			},
			expectedLabels: map[string]string{
				v1.LabelFailureDomainBetaZone:   "foo",
				v1.LabelFailureDomainBetaRegion: "bar",
				v1.LabelTopologyZone:            "foo",
				v1.LabelTopologyRegion:          "bar",
				v1.LabelInstanceType:            "the-best-type",
				v1.LabelInstanceTypeStable:      "the-best-type",
			},
			expectedErr: nil,
		},
	}

	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			stopCh := ctx.Done()

			testNode := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node01",
					Labels: test.labels,
				},
			}

			clientset := fake.NewSimpleClientset(testNode)
			factory := informers.NewSharedInformerFactory(clientset, 0)

			cnc := &CloudNodeController{
				kubeClient:   clientset,
				nodeInformer: factory.Core().V1().Nodes(),
			}

			// activate node informer
			factory.Core().V1().Nodes().Informer()
			factory.Start(stopCh)
			factory.WaitForCacheSync(stopCh)

			err := cnc.reconcileNodeLabels("node01")
			if err != test.expectedErr {
				t.Logf("actual err: %v", err)
				t.Logf("expected err: %v", test.expectedErr)
				t.Errorf("unexpected error")
			}

			actualNode, err := clientset.CoreV1().Nodes().Get(ctx, "node01", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("error getting updated node: %v", err)
			}

			if !reflect.DeepEqual(actualNode.Labels, test.expectedLabels) {
				t.Logf("actual node labels: %v", actualNode.Labels)
				t.Logf("expected node labels: %v", test.expectedLabels)
				t.Errorf("updated node did not match expected node")
			}
		})
	}

}

// Tests that node address changes are detected correctly
// 校验 nodeAddressesChangeDetected 的比较语义：集合语义（顺序无关）、
// 类型与值都参与比较，以及仅增删 Hostname 也算变化。
// 错误地判定“无变化”会导致节点地址长期过期。
func TestNodeAddressesChangeDetected(t *testing.T) {
	addressSet1 := []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.163",
		},
	}
	addressSet2 := []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.163",
		},
	}

	assert.False(t, nodeAddressesChangeDetected(addressSet1, addressSet2),
		"Node address changes are not detected correctly")

	addressSet1 = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.164",
		},
	}
	addressSet2 = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.163",
		},
	}

	assert.True(t, nodeAddressesChangeDetected(addressSet1, addressSet2),
		"Node address changes are not detected correctly")

	addressSet1 = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.164",
		},
		{
			Type:    v1.NodeHostName,
			Address: "hostname.zone.region.aws.test",
		},
	}
	addressSet2 = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.164",
		},
	}

	assert.True(t, nodeAddressesChangeDetected(addressSet1, addressSet2),
		"Node address changes are not detected correctly")

	addressSet1 = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.164",
		},
	}
	addressSet2 = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.164",
		},
		{
			Type:    v1.NodeHostName,
			Address: "hostname.zone.region.aws.test",
		},
	}

	assert.True(t, nodeAddressesChangeDetected(addressSet1, addressSet2),
		"Node address changes are not detected correctly")

	addressSet1 = []v1.NodeAddress{
		{
			Type:    v1.NodeExternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeInternalIP,
			Address: "132.143.154.163",
		},
	}
	addressSet2 = []v1.NodeAddress{
		{
			Type:    v1.NodeInternalIP,
			Address: "10.0.0.1",
		},
		{
			Type:    v1.NodeExternalIP,
			Address: "132.143.154.163",
		},
	}

	assert.True(t, nodeAddressesChangeDetected(addressSet1, addressSet2),
		"Node address changes are not detected correctly")
}

// Test updateNodeAddress with instanceV2, same test case with TestNodeAddressesNotUpdate.
// 云侧报告实例不存在（ExistsByProviderID=false）时，即使能返回元数据也不应更新地址，
// 否则会为已删除的实例保留陈旧网络信息。
func TestNodeAddressesNotUpdateV2(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	existingNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "node0",
			CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
			Labels:            map[string]string{},
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
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{
				{
					Key:    cloudproviderapi.TaintExternalCloudProvider,
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
		},
	}

	clientset := fake.NewSimpleClientset(existingNode)
	factory := informers.NewSharedInformerFactory(clientset, 0)

	fakeCloud := &fakecloud.Cloud{
		EnableInstancesV2: true,
		InstanceTypes:     map[types.NodeName]string{},
		Addresses: []v1.NodeAddress{
			{
				Type:    v1.NodeHostName,
				Address: "node0.cloud.internal",
			},
			{
				Type:    v1.NodeInternalIP,
				Address: "10.0.0.1",
			},
			{
				Type:    v1.NodeExternalIP,
				Address: "132.143.154.163",
			},
		},
		ExistsByProviderID: false,
		Err:                nil,
	}

	cloudNodeController := &CloudNodeController{
		kubeClient:   clientset,
		nodeInformer: factory.Core().V1().Nodes(),
		cloud:        fakeCloud,
	}

	instanceMeta, err := cloudNodeController.getInstanceNodeAddresses(ctx, existingNode)
	if err != nil {
		t.Errorf("get instance metadata with error %v", err)
	}
	cloudNodeController.updateNodeAddress(ctx, existingNode, instanceMeta)

	updatedNode, err := clientset.CoreV1().Nodes().Get(ctx, existingNode.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting updated nodes: %v", err)
	}

	if len(updatedNode.Status.Addresses) > 0 {
		t.Errorf("Node addresses should not be updated")
	}
}

// This test checks that a node with the external cloud provider taint is cloudprovider initialized and
// and node addresses will not be updated when node isn't present according to the cloudprovider
// 与 V2 用例对应，覆盖传统 Instances 路径；两条路径必须保持相同的保护逻辑。
func TestNodeAddressesNotUpdate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	existingNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "node0",
			CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
			Labels:            map[string]string{},
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
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{
				{
					Key:    cloudproviderapi.TaintExternalCloudProvider,
					Value:  "true",
					Effect: v1.TaintEffectNoSchedule,
				},
			},
		},
	}

	clientset := fake.NewSimpleClientset(existingNode)
	factory := informers.NewSharedInformerFactory(clientset, 0)

	fakeCloud := &fakecloud.Cloud{
		EnableInstancesV2: false,
		InstanceTypes:     map[types.NodeName]string{},
		Addresses: []v1.NodeAddress{
			{
				Type:    v1.NodeHostName,
				Address: "node0.cloud.internal",
			},
			{
				Type:    v1.NodeInternalIP,
				Address: "10.0.0.1",
			},
			{
				Type:    v1.NodeExternalIP,
				Address: "132.143.154.163",
			},
		},
		ExistsByProviderID: false,
		Err:                nil,
	}

	cloudNodeController := &CloudNodeController{
		kubeClient:   clientset,
		nodeInformer: factory.Core().V1().Nodes(),
		cloud:        fakeCloud,
	}

	instanceMeta, err := cloudNodeController.getInstanceNodeAddresses(ctx, existingNode)
	if err != nil {
		t.Errorf("get instance metadata with error %v", err)
	}
	cloudNodeController.updateNodeAddress(ctx, existingNode, instanceMeta)

	updatedNode, err := clientset.CoreV1().Nodes().Get(ctx, existingNode.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting updated nodes: %v", err)
	}

	if len(updatedNode.Status.Addresses) > 0 {
		t.Errorf("Node addresses should not be updated")
	}
}

// TestGetInstanceMetadata 验证 getInstanceMetadata 在 Instances 与 InstancesV2 之间
// 统一出相同形状的 InstanceMetadata：传统 Instances 需要拼装 ProviderID/地址/类型，
// InstancesV2 直接返回元数据；providerID 未实现时不得报错。
func TestGetInstanceMetadata(t *testing.T) {
	tests := []struct {
		name             string
		fakeCloud        *fakecloud.Cloud
		existingNode     *v1.Node
		expectedMetadata *cloudprovider.InstanceMetadata
		expectErr        bool
	}{
		{
			// 传统 Instances：由 InstanceID 与 ProviderName 拼出 fake://12345。
			name: "cloud implemented with Instances and provider ID",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				ProviderID: map[types.NodeName]string{
					types.NodeName("node0"): "fake://12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				ErrByProviderID: nil,
				Err:             nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedMetadata: &cloudprovider.InstanceMetadata{
				ProviderID: "fake://12345",
				NodeAddresses: []v1.NodeAddress{
					{Type: "Hostname", Address: "node0.cloud.internal"},
					{Type: "InternalIP", Address: "10.0.0.1"},
					{Type: "ExternalIP", Address: "132.143.154.163"},
				},
			},
		},
		{
			// InstanceID 返回 NotImplemented 时只跳过 ProviderID，地址仍需返回。
			name: "cloud implemented with Instances (providerID not implemented)",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"):        "t1.micro",
					types.NodeName("fake://12345"): "t1.micro",
				},
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): cloudprovider.NotImplemented,
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			expectedMetadata: &cloudprovider.InstanceMetadata{
				NodeAddresses: []v1.NodeAddress{
					{Type: "Hostname", Address: "node0.cloud.internal"},
					{Type: "InternalIP", Address: "10.0.0.1"},
					{Type: "ExternalIP", Address: "132.143.154.163"},
				},
			},
		},
		{
			// 云不支持 InstanceID 但节点已有 providerID 时，应沿用节点上的值。
			name: "cloud implemented with Instances (providerID not implemented) and node with providerID",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: false,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"):        "t1.micro",
					types.NodeName("fake://12345"): "t1.micro",
				},
				ExtIDErr: map[types.NodeName]error{
					types.NodeName("node0"): cloudprovider.NotImplemented,
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://asdasd",
				},
			},
			expectedMetadata: &cloudprovider.InstanceMetadata{
				ProviderID: "fake://asdasd",
				NodeAddresses: []v1.NodeAddress{
					{Type: "Hostname", Address: "node0.cloud.internal"},
					{Type: "InternalIP", Address: "10.0.0.1"},
					{Type: "ExternalIP", Address: "132.143.154.163"},
				},
			},
		},
		{
			// InstancesV2 且元数据带 providerID 时直接采用。
			name: "cloud implemented with InstancesV2 (with providerID)",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"):        "t1.micro",
					types.NodeName("fake://12345"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				ProviderID: map[types.NodeName]string{
					types.NodeName("node0"): "fake://12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedMetadata: &cloudprovider.InstanceMetadata{
				ProviderID: "fake://12345",
				NodeAddresses: []v1.NodeAddress{
					{Type: "Hostname", Address: "node0.cloud.internal"},
					{Type: "InternalIP", Address: "10.0.0.1"},
					{Type: "ExternalIP", Address: "132.143.154.163"},
				},
			},
		},
		{ // it will be requeueud later
			// InstancesV2 无法给出 providerID 时返回错误，让节点重新入队等待下一次元数据就绪。
			name:      "cloud implemented with InstancesV2 (without providerID)",
			expectErr: true,
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"):        "t1.micro",
					types.NodeName("fake://12345"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					Taints: []v1.Taint{
						{
							Key:    cloudproviderapi.TaintExternalCloudProvider,
							Value:  "true",
							Effect: v1.TaintEffectNoSchedule,
						},
					},
				},
			},
			expectedMetadata: &cloudprovider.InstanceMetadata{
				NodeAddresses: []v1.NodeAddress{
					{Type: "Hostname", Address: "node0.cloud.internal"},
					{Type: "InternalIP", Address: "10.0.0.1"},
					{Type: "ExternalIP", Address: "132.143.154.163"},
				},
			},
		},
		{
			// 节点自带 providerID 时，即使元数据未给出也能补齐类型等其余信息。
			name: "cloud implemented with InstancesV2 (without providerID) and node with providerID",
			fakeCloud: &fakecloud.Cloud{
				EnableInstancesV2: true,
				InstanceTypes: map[types.NodeName]string{
					types.NodeName("node0"):        "t1.micro",
					types.NodeName("fake://12345"): "t1.micro",
				},
				ExtID: map[types.NodeName]string{
					types.NodeName("node0"): "12345",
				},
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				Err: nil,
			},
			existingNode: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "node0",
					CreationTimestamp: metav1.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Spec: v1.NodeSpec{
					ProviderID: "fake://12345",
				},
			},
			expectedMetadata: &cloudprovider.InstanceMetadata{
				ProviderID:   "fake://12345",
				InstanceType: "t1.micro",
				NodeAddresses: []v1.NodeAddress{
					{Type: "Hostname", Address: "node0.cloud.internal"},
					{Type: "InternalIP", Address: "10.0.0.1"},
					{Type: "ExternalIP", Address: "132.143.154.163"},
				},
			},
		},
	}

	// 只依赖 cloud 字段，无需 informer/clientset，因此直接构造最小控制器。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)

			cloudNodeController := &CloudNodeController{
				cloud: test.fakeCloud,
			}

			metadata, err := cloudNodeController.getInstanceMetadata(ctx, test.existingNode)
			if (err != nil) != test.expectErr {
				t.Fatalf("error expected %v got: %v", test.expectErr, err)
			}

			if !cmp.Equal(metadata, test.expectedMetadata) {
				t.Errorf("unexpected metadata %s", cmp.Diff(metadata, test.expectedMetadata))
			}
		})
	}
}

// TestUpdateNodeStatus 验证节点状态回写的并发模型：不同 worker 数下都应处理完所有节点，
// 且对云提供的调用次数与节点数一致（每个节点只查一次地址）。
func TestUpdateNodeStatus(t *testing.T) {
	// emaulate the latency of the cloud API calls
	// 人为加入延迟以放大并发差异，使 worker 数对总耗时的影响可观察。
	const cloudLatency = 10 * time.Millisecond

	// generateNodes 批量生成最小 Node，只需名称即可驱动状态回写路径。
	generateNodes := func(n int) []runtime.Object {
		result := []runtime.Object{}
		for i := 0; i < n; i++ {
			result = append(result, &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("node0%d", i),
				},
			})
		}
		return result
	}

	tests := []struct {
		name    string
		workers int32
		nodes   int
	}{
		{
			name:    "single thread",
			workers: 1,
			nodes:   100,
		},
		{
			name:    "5 workers",
			workers: 5,
			nodes:   100,
		},
		{
			name:    "10 workers",
			workers: 10,
			nodes:   100,
		},
		{
			name:    "30 workers",
			workers: 30,
			nodes:   100,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			fakeCloud := &fakecloud.Cloud{
				EnableInstancesV2: false,
				Addresses: []v1.NodeAddress{
					{
						Type:    v1.NodeHostName,
						Address: "node0.cloud.internal",
					},
					{
						Type:    v1.NodeInternalIP,
						Address: "10.0.0.1",
					},
					{
						Type:    v1.NodeExternalIP,
						Address: "132.143.154.163",
					},
				},
				RequestDelay: cloudLatency,
				Err:          nil,
			}

			clientset := fake.NewSimpleClientset()
			// 拦截 patch 请求直接返回成功，避免 fake clientset 对不存在的更新对象做深拷贝校验。
			clientset.PrependReactor("patch", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
				return true, &v1.Node{}, nil
			})

			factory := informers.NewSharedInformerFactory(clientset, 0)
			eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
			nodeInformer := factory.Core().V1().Nodes()
			nodeIndexer := nodeInformer.Informer().GetIndexer()
			cloudNodeController := &CloudNodeController{
				kubeClient:                clientset,
				nodeInformer:              nodeInformer,
				nodesLister:               nodeInformer.Lister(),
				nodesSynced:               func() bool { return true },
				cloud:                     fakeCloud,
				recorder:                  eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cloud-node-controller"}),
				nodeStatusUpdateFrequency: 1 * time.Second,
				workerCount:               test.workers,
				statusUpdateWorkerCount:   test.workers,
			}

			for _, n := range generateNodes(test.nodes) {
				err := nodeIndexer.Add(n)
				if err != nil {
					t.Fatal(err)
				}
			}

			w := eventBroadcaster.StartStructuredLogging(0)
			defer w.Stop()

			start := time.Now()
			if err := cloudNodeController.UpdateNodeStatus(ctx); err != nil {
				t.Fatalf("error updating node status: %v", err)
			}
			t.Logf("%d workers: processed %d nodes int %v ", test.workers, test.nodes, time.Since(start))
			// 每个节点恰好一次云调用：多调用意味着重复同步，少调用意味着遗漏节点。
			if len(fakeCloud.Calls) != test.nodes {
				t.Errorf("expected %d cloud-provider calls, got %d", test.nodes, len(fakeCloud.Calls))
			}

		})
	}
}
