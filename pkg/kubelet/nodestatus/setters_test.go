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

package nodestatus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/uuid"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/version"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubecontainertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/util/sliceutils"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// testKubeletHostname 是测试中 kubelet 使用的固定主机名。
const (
	testKubeletHostname = "hostname"
)

// TestNodeAddress 验证 NodeAddress setter 在不同 nodeIP、云厂商、IPv4/IPv6
// 与单双栈组合下生成的地址列表，以及 provided-node-ip 注解的写入/清理。
//
// TODO(mtaufen): 以下代码移植自旧的 kubelet_node_status_test.go，未来可补充更多 NodeAddress setter 测试。
func TestNodeAddress(t *testing.T) {
	existingNodeAddress := v1.NodeAddress{Address: "10.1.1.2"}
	cases := []struct {
		name                           string
		nodeIP                         net.IP
		secondaryNodeIP                net.IP
		resolvedIP                     net.IP
		cloudProvider                  bool
		expectedAddresses              []v1.NodeAddress
		existingAnnotations            map[string]string
		expectedAnnotations            map[string]string
		shouldError                    bool
		shouldSetNodeAddressBeforeTest bool
	}{
		{
			name:          "using cloud provider and nodeIP specified",
			nodeIP:        netutils.ParseIPSloppy("10.0.0.1"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.0.0.1"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "no cloud provider and nodeIP IPv4 unspecified",
			nodeIP:        netutils.ParseIPSloppy("0.0.0.0"),
			resolvedIP:    netutils.ParseIPSloppy("10.0.0.2"),
			cloudProvider: false,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.0.0.2"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "no cloud provider and nodeIP IPv6 unspecified",
			nodeIP:        netutils.ParseIPSloppy("::"),
			resolvedIP:    netutils.ParseIPSloppy("2001:db2::2"),
			cloudProvider: false,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "2001:db2::2"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "using cloud provider and nodeIP IPv4 unspecified",
			nodeIP:        netutils.ParseIPSloppy("0.0.0.0"),
			resolvedIP:    netutils.ParseIPSloppy("10.0.0.2"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.0.0.2"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "using cloud provider and nodeIP IPv6 unspecified",
			nodeIP:        netutils.ParseIPSloppy("::"),
			resolvedIP:    netutils.ParseIPSloppy("2001:db2::2"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "2001:db2::2"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "no cloud provider and no nodeIP resolve IPv4",
			resolvedIP:    netutils.ParseIPSloppy("10.0.0.2"),
			cloudProvider: false,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.0.0.2"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "no cloud provider and no nodeIP resolve IPv6",
			resolvedIP:    netutils.ParseIPSloppy("2001:db2::2"),
			cloudProvider: false,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "2001:db2::2"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "using cloud provider and no nodeIP resolve IPv4",
			resolvedIP:    netutils.ParseIPSloppy("10.0.0.2"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "using cloud provider and no nodeIP resolve IPv6",
			resolvedIP:    netutils.ParseIPSloppy("2001:db2::2"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			shouldError: false,
		},
		{
			name:          "cloud provider gets nodeIP annotation",
			nodeIP:        netutils.ParseIPSloppy("10.1.1.1"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			expectedAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.1",
			},
			shouldError: false,
		},
		{
			name:                           "using cloud provider and node address is already set",
			nodeIP:                         netutils.ParseIPSloppy("10.1.1.1"),
			cloudProvider:                  true,
			expectedAddresses:              []v1.NodeAddress{existingNodeAddress},
			shouldError:                    true,
			shouldSetNodeAddressBeforeTest: true,
		},
		{
			name:          "No cloud provider does not get nodeIP annotation",
			nodeIP:        netutils.ParseIPSloppy("10.1.1.1"),
			cloudProvider: false,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			expectedAnnotations: map[string]string{},
			shouldError:         false,
		},
		{
			name:          "Stale nodeIP annotation is removed when not using cloud provider",
			nodeIP:        netutils.ParseIPSloppy("10.1.1.1"),
			cloudProvider: false,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			existingAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.3",
			},
			expectedAnnotations: map[string]string{},
			shouldError:         false,
		},
		{
			name:          "Incorrect nodeIP annotation is fixed",
			nodeIP:        netutils.ParseIPSloppy("10.1.1.1"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			existingAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.3",
			},
			expectedAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.1",
			},
			shouldError: false,
		},
		{
			// 无需测试“legacy 云厂商 + 双栈 IP”等组合：非法配置在更早阶段
			// 就会被拒绝，根本走不到这里。
			// 因此这些分支无需单独覆盖。
			name:            "Dual-stack cloud, with dual-stack nodeIPs",
			nodeIP:          netutils.ParseIPSloppy("2600:1f14:1d4:d101::ba3d"),
			secondaryNodeIP: netutils.ParseIPSloppy("10.1.1.2"),
			cloudProvider:   true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "2600:1f14:1d4:d101::ba3d"},
				{Type: v1.NodeInternalIP, Address: "10.1.1.2"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			expectedAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "2600:1f14:1d4:d101::ba3d,10.1.1.2",
			},
			shouldError: false,
		},
		{
			name:            "Upgrade to cloud dual-stack nodeIPs",
			nodeIP:          netutils.ParseIPSloppy("10.1.1.1"),
			secondaryNodeIP: netutils.ParseIPSloppy("2600:1f14:1d4:d101::ba3d"),
			cloudProvider:   true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeInternalIP, Address: "2600:1f14:1d4:d101::ba3d"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			existingAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.1",
			},
			expectedAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.1,2600:1f14:1d4:d101::ba3d",
			},
			shouldError: false,
		},
		{
			name:          "Downgrade from cloud dual-stack nodeIPs",
			nodeIP:        netutils.ParseIPSloppy("10.1.1.1"),
			cloudProvider: true,
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
			existingAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.1,2600:1f14:1d4:d101::ba3d",
			},
			expectedAnnotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip": "10.1.1.1",
			},
			shouldError: false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := ktesting.Init(t)
			// 构造测试用例的初始 node。
			existingNode := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        testKubeletHostname,
					Annotations: testCase.existingAnnotations,
				},
				Spec: v1.NodeSpec{},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{},
				},
			}

			if testCase.shouldSetNodeAddressBeforeTest {
				existingNode.Status.Addresses = append(existingNode.Status.Addresses, existingNodeAddress)
			}

			nodeIPValidator := func(nodeIP net.IP) error {
				return nil
			}
			hostname := testKubeletHostname

			// 强制 DNS 解析失败，确保 setter 使用注入的 resolveAddressFunc 而非真实网络解析。
			net.DefaultResolver = &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network string, address string) (net.Conn, error) {
					return nil, fmt.Errorf("error")
				},
			}
			defer func() {
				net.DefaultResolver = &net.Resolver{}
			}()

			resolveAddressFunc := func(net.IP) (net.IP, error) {
				return testCase.resolvedIP, nil
			}

			// 存在第二个 --node-ip 时构成双栈配置。
			nodeIPs := []net.IP{testCase.nodeIP}
			if testCase.secondaryNodeIP != nil {
				nodeIPs = append(nodeIPs, testCase.secondaryNodeIP)
			}

			// 构造待测 setter。
			setter := NodeAddress(nodeIPs,
				nodeIPValidator,
				hostname,
				testCase.cloudProvider,
				resolveAddressFunc,
			)

			// 对已有 node 调用 setter。
			err := setter(ctx, existingNode)
			if err != nil && !testCase.shouldError {
				t.Fatalf("unexpected error: %v", err)
			} else if err != nil && testCase.shouldError {
				// 预期会出错且确实出错时，直接提前返回。
				return
			}

			assert.True(t, apiequality.Semantic.DeepEqual(testCase.expectedAddresses, existingNode.Status.Addresses),
				"Diff: %s", cmp.Diff(testCase.expectedAddresses, existingNode.Status.Addresses))
			if testCase.expectedAnnotations != nil {
				assert.True(t, apiequality.Semantic.DeepEqual(testCase.expectedAnnotations, existingNode.Annotations),
					"Diff: %s", cmp.Diff(testCase.expectedAnnotations, existingNode.Annotations))
			}
		})
	}
}

// 失败与自动探测分支无法在此覆盖，因为相关代码不可 mock。
func TestNodeAddress_NoCloudProvider(t *testing.T) {
	cases := []struct {
		name              string
		nodeIPs           []net.IP
		expectedAddresses []v1.NodeAddress
		shouldError       bool
	}{
		{
			name:    "Single --node-ip",
			nodeIPs: []net.IP{netutils.ParseIPSloppy("10.1.1.1")},
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
		},
		{
			name:        "Invalid single --node-ip (using loopback)",
			nodeIPs:     []net.IP{netutils.ParseIPSloppy("127.0.0.1")},
			shouldError: true,
		},
		{
			name:    "Dual --node-ips",
			nodeIPs: []net.IP{netutils.ParseIPSloppy("10.1.1.1"), netutils.ParseIPSloppy("fd01::1234")},
			expectedAddresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: "10.1.1.1"},
				{Type: v1.NodeInternalIP, Address: "fd01::1234"},
				{Type: v1.NodeHostName, Address: testKubeletHostname},
			},
		},
		{
			name:        "Dual --node-ips but with invalid secondary IP (using multicast IP)",
			nodeIPs:     []net.IP{netutils.ParseIPSloppy("10.1.1.1"), netutils.ParseIPSloppy("224.0.0.0")},
			shouldError: true,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := ktesting.Init(t)
			// 构造测试用例的初始 node。
			existingNode := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname, Annotations: make(map[string]string)},
				Spec:       v1.NodeSpec{},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{},
				},
			}

			nodeIPValidator := func(nodeIP net.IP) error {
				if nodeIP.IsLoopback() {
					return fmt.Errorf("nodeIP can't be loopback address")
				} else if nodeIP.IsMulticast() {
					return fmt.Errorf("nodeIP can't be a multicast address")
				}
				return nil
			}
			resolvedAddressesFunc := func(net.IP) (net.IP, error) {
				return nil, fmt.Errorf("not reached")
			}

			// 构造待测 setter。
			setter := NodeAddress(testCase.nodeIPs,
				nodeIPValidator,
				testKubeletHostname,
				false, // externalCloudProvider
				resolvedAddressesFunc)

			// 对已有 node 调用 setter。
			err := setter(ctx, existingNode)
			if testCase.shouldError && err == nil {
				t.Fatal("expected error but no error returned")
			}
			if err != nil && !testCase.shouldError {
				t.Fatalf("unexpected error: %v", err)
			}

			assert.True(t, apiequality.Semantic.DeepEqual(testCase.expectedAddresses, existingNode.Status.Addresses),
				"Diff: %s", cmp.Diff(testCase.expectedAddresses, existingNode.Status.Addresses))
		})
	}
}

// TestMachineInfo 验证 MachineInfo setter 由 machine info 推导 capacity 与
// allocatable，覆盖 device plugin、hugepages、ephemeral storage、swap 与重启事件。
func TestMachineInfo(t *testing.T) {
	const nodeName = "test-node"

	type dprc struct {
		capacity    v1.ResourceList
		allocatable v1.ResourceList
		inactive    []string
	}

	cases := []struct {
		desc                                 string
		node                                 *v1.Node
		maxPods                              int
		podsPerCore                          int
		machineInfo                          *cadvisorapi.MachineInfo
		machineInfoError                     error
		capacity                             v1.ResourceList
		devicePluginResourceCapacity         dprc
		nodeAllocatableReservation           v1.ResourceList
		expectNode                           *v1.Node
		expectEvents                         []testEvent
		disableLocalStorageCapacityIsolation bool
		featureGateDependencies              []featuregate.Feature
	}{
		{
			desc:    "machine identifiers, basic capacity and allocatable",
			node:    &v1.Node{},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				MachineID:      "MachineID",
				SystemUUID:     "SystemUUID",
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						MachineID:  "MachineID",
						SystemUUID: "SystemUUID",
					},
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc:        "podsPerCore greater than zero, but less than maxPods/cores",
			node:        &v1.Node{},
			maxPods:     10,
			podsPerCore: 4,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(8, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(8, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc:        "podsPerCore greater than maxPods/cores",
			node:        &v1.Node{},
			maxPods:     10,
			podsPerCore: 6,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(10, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(10, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc:    "allocatable should equal capacity minus reservations",
			node:    &v1.Node{},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			nodeAllocatableReservation: v1.ResourceList{
				// 每个资源各预留 1 个单位，用于验证 allocatable = capacity - reservation
				v1.ResourceCPU:              *resource.NewMilliQuantity(1, resource.DecimalSI),
				v1.ResourceMemory:           *resource.NewQuantity(1, resource.BinarySI),
				v1.ResourcePods:             *resource.NewQuantity(1, resource.DecimalSI),
				v1.ResourceEphemeralStorage: *resource.NewQuantity(1, resource.BinarySI),
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(1999, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1023, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(109, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc: "allocatable memory does not double-count hugepages reservations",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						// 真实系统中不可能只预留 1 字节，
						// 这里只需要验证 setter 的算术逻辑。
						v1.ResourceHugePagesPrefix + "test": *resource.NewQuantity(1, resource.BinarySI),
					},
				},
			},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:                      *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:                   *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourceHugePagesPrefix + "test": *resource.NewQuantity(1, resource.BinarySI),
						v1.ResourcePods:                     *resource.NewQuantity(110, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU: *resource.NewMilliQuantity(2000, resource.DecimalSI),
						// 内存因 hugepages 预留少了 1 个单位
						v1.ResourceMemory:                   *resource.NewQuantity(1023, resource.BinarySI),
						v1.ResourceHugePagesPrefix + "test": *resource.NewQuantity(1, resource.BinarySI),
						v1.ResourcePods:                     *resource.NewQuantity(110, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc: "negative capacity resources should be set to 0 in allocatable",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						"negative-resource": *resource.NewQuantity(-1, resource.BinarySI),
					},
				},
			},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:      *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:   *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:     *resource.NewQuantity(110, resource.DecimalSI),
						"negative-resource": *resource.NewQuantity(-1, resource.BinarySI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:      *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:   *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:     *resource.NewQuantity(110, resource.DecimalSI),
						"negative-resource": *resource.NewQuantity(0, resource.BinarySI),
					},
				},
			},
		},
		{
			desc: "hugepages reservation greater than node memory capacity should result in memory capacity set to 0",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceHugePagesPrefix + "test": *resource.NewQuantity(1025, resource.BinarySI),
					},
				},
			},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:                      *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:                   *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:                     *resource.NewQuantity(110, resource.DecimalSI),
						v1.ResourceHugePagesPrefix + "test": *resource.NewQuantity(1025, resource.BinarySI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:                      *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:                   *resource.NewQuantity(0, resource.BinarySI),
						v1.ResourcePods:                     *resource.NewQuantity(110, resource.DecimalSI),
						v1.ResourceHugePagesPrefix + "test": *resource.NewQuantity(1025, resource.BinarySI),
					},
				},
			},
		},
		{
			desc:    "ephemeral storage is reflected in capacity and allocatable",
			node:    &v1.Node{},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			capacity: v1.ResourceList{
				v1.ResourceEphemeralStorage: *resource.NewQuantity(5000, resource.BinarySI),
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:              *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:           *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:             *resource.NewQuantity(110, resource.DecimalSI),
						v1.ResourceEphemeralStorage: *resource.NewQuantity(5000, resource.BinarySI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:              *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:           *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:             *resource.NewQuantity(110, resource.DecimalSI),
						v1.ResourceEphemeralStorage: *resource.NewQuantity(5000, resource.BinarySI),
					},
				},
			},
		},
		{
			desc:    "ephemeral storage is not reflected in capacity and allocatable because localStorageCapacityIsolation is disabled",
			node:    &v1.Node{},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			capacity: v1.ResourceList{
				v1.ResourceEphemeralStorage: *resource.NewQuantity(5000, resource.BinarySI),
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:              *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:           *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:             *resource.NewQuantity(110, resource.DecimalSI),
						v1.ResourceEphemeralStorage: *resource.NewQuantity(5000, resource.BinarySI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:              *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory:           *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:             *resource.NewQuantity(110, resource.DecimalSI),
						v1.ResourceEphemeralStorage: *resource.NewQuantity(5000, resource.BinarySI),
					},
				},
			},
			disableLocalStorageCapacityIsolation: true,
		},
		{
			desc:    "device plugin resources are reflected in capacity and allocatable",
			node:    &v1.Node{},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			devicePluginResourceCapacity: dprc{
				capacity: v1.ResourceList{
					"device-plugin": *resource.NewQuantity(1, resource.BinarySI),
				},
				allocatable: v1.ResourceList{
					"device-plugin": *resource.NewQuantity(1, resource.BinarySI),
				},
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
						"device-plugin":   *resource.NewQuantity(1, resource.BinarySI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
						"device-plugin":   *resource.NewQuantity(1, resource.BinarySI),
					},
				},
			},
		},
		{
			desc: "inactive device plugin resources should have their capacity set to 0",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						"inactive": *resource.NewQuantity(1, resource.BinarySI),
					},
				},
			},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			devicePluginResourceCapacity: dprc{
				inactive: []string{"inactive"},
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
						"inactive":        *resource.NewQuantity(0, resource.BinarySI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
						"inactive":        *resource.NewQuantity(0, resource.BinarySI),
					},
				},
			},
		},
		{
			desc: "extended resources not present in capacity are removed from allocatable",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						"example.com/extended": *resource.NewQuantity(1, resource.BinarySI),
					},
				},
			},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc:    "on failure to get machine info, allocatable and capacity for memory and cpu are set to 0, pods to maxPods",
			node:    &v1.Node{},
			maxPods: 110,
			// 获取 machine info 失败时不考虑 podsPerCore，pods 直接取 maxPods
			podsPerCore:      1,
			machineInfoError: fmt.Errorf("foo"),
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(0, resource.DecimalSI),
						v1.ResourceMemory: resource.MustParse("0Gi"),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(0, resource.DecimalSI),
						v1.ResourceMemory: resource.MustParse("0Gi"),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
				},
			},
		},
		{
			desc: "node reboot event is recorded",
			node: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						BootID: "foo",
					},
				},
			},
			maxPods: 110,
			machineInfo: &cadvisorapi.MachineInfo{
				BootID:         "bar",
				NumCores:       2,
				MemoryCapacity: 1024,
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						BootID: "bar",
					},
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(1024, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
					},
				},
			},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeWarning,
					event:     events.NodeRebooted,
					message:   fmt.Sprintf("Node %s has been rebooted, boot id: %s", nodeName, "bar"),
				},
			},
		},
		{
			desc: "with swap info",
			node: &v1.Node{},
			machineInfo: &cadvisorapi.MachineInfo{
				SwapCapacity: uint64(20 * 1024 * 1024 * 1024),
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						Swap: &v1.NodeSwapStatus{
							Capacity: ptr.To(int64(20 * 1024 * 1024 * 1024)),
						},
					},
					Capacity: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(0, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(0, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(0, resource.DecimalSI),
					},
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewMilliQuantity(0, resource.DecimalSI),
						v1.ResourceMemory: *resource.NewQuantity(0, resource.BinarySI),
						v1.ResourcePods:   *resource.NewQuantity(0, resource.DecimalSI),
					},
				},
			},
			featureGateDependencies: []featuregate.Feature{features.NodeSwap},
		},
	}

	for _, tc := range cases {
		featureGatesMissing := false
		for _, featureGateDependency := range tc.featureGateDependencies {
			if !utilfeature.DefaultFeatureGate.Enabled(featureGateDependency) {
				featureGatesMissing = true
				break
			}
		}

		// 依赖的 feature gate 未开启时跳过该用例，避免不同构建配置下误报。
		if featureGatesMissing {
			continue
		}

		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			machineInfoFunc := func() (*cadvisorapi.MachineInfo, error) {
				return tc.machineInfo, tc.machineInfoError
			}
			capacityFunc := func(_ klog.Logger, localStorageCapacityIsolation bool) v1.ResourceList {
				return tc.capacity
			}
			devicePluginResourceCapacityFunc := func(_ klog.Logger) (v1.ResourceList, v1.ResourceList, []string) {
				c := tc.devicePluginResourceCapacity
				return c.capacity, c.allocatable, c.inactive
			}
			nodeAllocatableReservationFunc := func() v1.ResourceList {
				return tc.nodeAllocatableReservation
			}

			events := []testEvent{}
			recordEventFunc := func(eventType, event, message string) {
				events = append(events, testEvent{
					eventType: eventType,
					event:     event,
					message:   message,
				})
			}
			runningInUserNSFunc := func() *bool {
				return nil
			}
			// 构造待测 setter。
			setter := MachineInfo(nodeName, tc.maxPods, tc.podsPerCore, machineInfoFunc, capacityFunc,
				devicePluginResourceCapacityFunc, nodeAllocatableReservationFunc, recordEventFunc, runningInUserNSFunc, tc.disableLocalStorageCapacityIsolation)
			// 对 node 调用 setter。
			if err := setter(ctx, tc.node); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验 node 是否符合预期。
			assert.True(t, apiequality.Semantic.DeepEqual(tc.expectNode, tc.node),
				"Diff: %s", cmp.Diff(tc.expectNode, tc.node))
			// 校验事件是否符合预期。
			require.Equal(t, len(tc.expectEvents), len(events))
			for i := range tc.expectEvents {
				assert.Equal(t, tc.expectEvents[i], events[i])
			}
		})
	}

}

// TestVersionInfo 验证 VersionInfo setter 写入内核、OS image 与容器运行时版本；
// 获取运行时版本失败时回退为 Unknown。
func TestVersionInfo(t *testing.T) {
	cases := []struct {
		desc                string
		node                *v1.Node
		versionInfo         *cadvisorapi.VersionInfo
		versionInfoError    error
		runtimeType         string
		runtimeVersion      kubecontainer.Version
		runtimeVersionError error
		expectNode          *v1.Node
		expectError         error
	}{
		{
			desc: "versions set in node info",
			node: &v1.Node{},
			versionInfo: &cadvisorapi.VersionInfo{
				KernelVersion:      "KernelVersion",
				ContainerOsVersion: "ContainerOSVersion",
			},
			runtimeType: "RuntimeType",
			runtimeVersion: &kubecontainertest.FakeVersion{
				Version: "RuntimeVersion",
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						KernelVersion:           "KernelVersion",
						OSImage:                 "ContainerOSVersion",
						ContainerRuntimeVersion: "RuntimeType://RuntimeVersion",
						KubeletVersion:          version.Get().String(),
						KubeProxyVersion:        "",
					},
				},
			},
		},
		{
			desc:             "error getting version info",
			node:             &v1.Node{},
			versionInfoError: fmt.Errorf("foo"),
			expectNode:       &v1.Node{},
			expectError:      fmt.Errorf("error getting version info: foo"),
		},
		{
			desc:                "error getting runtime version results in Unknown runtime",
			node:                &v1.Node{},
			versionInfo:         &cadvisorapi.VersionInfo{},
			runtimeType:         "RuntimeType",
			runtimeVersionError: fmt.Errorf("foo"),
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						ContainerRuntimeVersion: "RuntimeType://Unknown",
						KubeletVersion:          version.Get().String(),
						KubeProxyVersion:        "",
					},
				},
			},
		},
		{
			desc: "DisableNodeKubeProxyVersion FeatureGate enable, versions set in node info",
			node: &v1.Node{},
			versionInfo: &cadvisorapi.VersionInfo{
				KernelVersion:      "KernelVersion",
				ContainerOsVersion: "ContainerOSVersion",
			},
			runtimeType: "RuntimeType",
			runtimeVersion: &kubecontainertest.FakeVersion{
				Version: "RuntimeVersion",
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						KernelVersion:           "KernelVersion",
						OSImage:                 "ContainerOSVersion",
						ContainerRuntimeVersion: "RuntimeType://RuntimeVersion",
						KubeletVersion:          version.Get().String(),
						KubeProxyVersion:        "",
					},
				},
			},
		},
		{
			desc: "DisableNodeKubeProxyVersion FeatureGate enable, KubeProxyVersion will be cleared if it is set.",
			node: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						KernelVersion:           "KernelVersion",
						OSImage:                 "ContainerOSVersion",
						ContainerRuntimeVersion: "RuntimeType://RuntimeVersion",
						KubeletVersion:          version.Get().String(),
						KubeProxyVersion:        "",
					},
				},
			},
			versionInfo: &cadvisorapi.VersionInfo{
				KernelVersion:      "KernelVersion",
				ContainerOsVersion: "ContainerOSVersion",
			},
			runtimeType: "RuntimeType",
			runtimeVersion: &kubecontainertest.FakeVersion{
				Version: "RuntimeVersion",
			},
			expectNode: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						KernelVersion:           "KernelVersion",
						OSImage:                 "ContainerOSVersion",
						ContainerRuntimeVersion: "RuntimeType://RuntimeVersion",
						KubeletVersion:          version.Get().String(),
						KubeProxyVersion:        "",
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			versionInfoFunc := func() (*cadvisorapi.VersionInfo, error) {
				return tc.versionInfo, tc.versionInfoError
			}
			runtimeTypeFunc := func() string {
				return tc.runtimeType
			}
			runtimeVersionFunc := func(_ context.Context) (kubecontainer.Version, error) {
				return tc.runtimeVersion, tc.runtimeVersionError
			}
			// 构造待测 setter。
			setter := VersionInfo(versionInfoFunc, runtimeTypeFunc, runtimeVersionFunc)
			// 对 node 调用 setter。
			err := setter(ctx, tc.node)
			require.Equal(t, tc.expectError, err)
			// 校验 node 是否符合预期。
			assert.True(t, apiequality.Semantic.DeepEqual(tc.expectNode, tc.node),
				"Diff: %s", cmp.Diff(tc.expectNode, tc.node))
		})
	}
}

// TestImages 验证 Images setter 会限制镜像总数与每个镜像的 name 数、按大小降序排列，
// 并在获取镜像列表失败时清空 node 上的镜像列表。
func TestImages(t *testing.T) {
	const (
		minImageSize = 23 * 1024 * 1024
		maxImageSize = 1000 * 1024 * 1024
	)

	cases := []struct {
		desc           string
		maxImages      int32
		imageList      []kubecontainer.Image
		imageListError error
		expectError    error
	}{
		{
			desc:      "max images enforced",
			maxImages: 1,
			imageList: makeImageList(2, 1, minImageSize, maxImageSize),
		},
		{
			desc:      "no max images cap for -1",
			maxImages: -1,
			imageList: makeImageList(2, 1, minImageSize, maxImageSize),
		},
		{
			desc:      "max names per image enforced",
			maxImages: -1,
			imageList: makeImageList(1, MaxNamesPerImageInNodeStatus+1, minImageSize, maxImageSize),
		},
		{
			desc:      "images are sorted by size, descending",
			maxImages: -1,
			// 用例执行时 makeExpectedImageList 会为期望结果排序
			imageList: []kubecontainer.Image{{Size: 3}, {Size: 1}, {Size: 4}, {Size: 2}},
		},
		{
			desc:      "repo digests and tags both show up in image names",
			maxImages: -1,
			// makeExpectedImageList 会同时使用 digest 与 tag
			imageList: []kubecontainer.Image{
				{
					RepoDigests: []string{"foo", "bar"},
					RepoTags:    []string{"baz", "quux"},
				},
			},
		},
		{
			desc:           "error getting image list, image list on node is reset to empty",
			maxImages:      -1,
			imageListError: fmt.Errorf("foo"),
			expectError:    fmt.Errorf("error getting image list: foo"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			imageListFunc := func() ([]kubecontainer.Image, error) {
				// 目前约定 imageListFunc 返回已排序列表，
				// 但未来也可能改在 setter 内排序
				// （例如镜像缓存不再替我们排序时）。
				sort.Sort(sliceutils.ByImageSize(tc.imageList))
				return tc.imageList, tc.imageListError
			}
			// 构造待测 setter。
			setter := Images(tc.maxImages, imageListFunc)
			// 对 node 调用 setter。
			node := &v1.Node{}
			err := setter(ctx, node)
			require.Equal(t, tc.expectError, err)
			// 校验 node 是否符合预期；出错时镜像列表应被重置为空。
			expectNode := &v1.Node{}
			if err == nil {
				expectNode.Status.Images = makeExpectedImageList(tc.imageList, tc.maxImages, MaxNamesPerImageInNodeStatus)
			}
			assert.True(t, apiequality.Semantic.DeepEqual(expectNode, node),
				"Diff: %s", cmp.Diff(expectNode, node))
		})
	}

}

// TestReadyCondition 验证 ReadyCondition setter 由 runtime/network/storage/shutdown
// 错误推导 Ready condition，并只在状态翻转时更新时间戳与发送事件。
func TestReadyCondition(t *testing.T) {
	now := time.Now()
	before := now.Add(-time.Second)
	nowFunc := func() time.Time { return now }

	withCapacity := &v1.Node{
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:              *resource.NewMilliQuantity(2000, resource.DecimalSI),
				v1.ResourceMemory:           *resource.NewQuantity(10e9, resource.BinarySI),
				v1.ResourcePods:             *resource.NewQuantity(100, resource.DecimalSI),
				v1.ResourceEphemeralStorage: *resource.NewQuantity(5000, resource.BinarySI),
			},
		},
	}

	withoutStorageCapacity := &v1.Node{
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(2000, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(10e9, resource.BinarySI),
				v1.ResourcePods:   *resource.NewQuantity(100, resource.DecimalSI),
			},
		},
	}

	cases := []struct {
		desc                                 string
		node                                 *v1.Node
		runtimeErrors                        error
		networkErrors                        error
		storageErrors                        error
		cmStatus                             cm.Status
		nodeShutdownManagerErrors            error
		expectConditions                     []v1.NodeCondition
		expectEvents                         []testEvent
		disableLocalStorageCapacityIsolation bool
	}{
		{
			desc:             "new, ready",
			node:             withCapacity.DeepCopy(),
			expectConditions: []v1.NodeCondition{*makeReadyCondition(true, "kubelet is posting ready status", now, now)},
			// TODO(mtaufen): 当前不会为初始 NodeReady 条件发送事件，原因尚不明确。
			// 可以考虑实际发送事件，并调整这些测试用例以验证事件已发送。
		},
		{
			desc: "new, ready: soft requirement warning",
			node: withCapacity.DeepCopy(),
			cmStatus: cm.Status{
				SoftRequirements: fmt.Errorf("foo"),
			},
			expectConditions: []v1.NodeCondition{*makeReadyCondition(true, "kubelet is posting ready status. WARNING: foo", now, now)},
		},
		{
			desc:             "new, not ready: storage errors",
			node:             withCapacity.DeepCopy(),
			storageErrors:    errors.New("some storage error"),
			expectConditions: []v1.NodeCondition{*makeReadyCondition(false, "some storage error", now, now)},
		},
		{
			desc:                      "new, not ready: shutdown active",
			node:                      withCapacity.DeepCopy(),
			nodeShutdownManagerErrors: errors.New("node is shutting down"),
			expectConditions:          []v1.NodeCondition{*makeReadyCondition(false, "node is shutting down", now, now)},
		},
		{
			desc:             "new, not ready: runtime and network errors",
			node:             withCapacity.DeepCopy(),
			runtimeErrors:    errors.New("runtime"),
			networkErrors:    errors.New("network"),
			expectConditions: []v1.NodeCondition{*makeReadyCondition(false, "[runtime, network]", now, now)},
		},
		{
			desc:             "new, not ready: missing capacities",
			node:             &v1.Node{},
			expectConditions: []v1.NodeCondition{*makeReadyCondition(false, "missing node capacity for resources: cpu, memory, pods, ephemeral-storage", now, now)},
		},
		{
			desc:                                 "new, ready: localStorageCapacityIsolation is not supported",
			node:                                 withoutStorageCapacity.DeepCopy(),
			disableLocalStorageCapacityIsolation: true,
			expectConditions:                     []v1.NodeCondition{*makeReadyCondition(true, "kubelet is posting ready status", now, now)},
		},
		// 下面的状态翻转用例只关心时间戳是否正确，无需在此穷举整个 condition 组合
		{
			desc: "transition to ready",
			node: func() *v1.Node {
				node := withCapacity.DeepCopy()
				node.Status.Conditions = []v1.NodeCondition{*makeReadyCondition(false, "", before, before)}
				return node
			}(),
			expectConditions: []v1.NodeCondition{*makeReadyCondition(true, "kubelet is posting ready status", now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     events.NodeReady,
				},
			},
		},
		{
			desc: "transition to not ready",
			node: func() *v1.Node {
				node := withCapacity.DeepCopy()
				node.Status.Conditions = []v1.NodeCondition{*makeReadyCondition(true, "", before, before)}
				return node
			}(),
			runtimeErrors:    errors.New("foo"),
			expectConditions: []v1.NodeCondition{*makeReadyCondition(false, "foo", now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     events.NodeNotReady,
				},
			},
		},
		{
			desc: "ready, no transition",
			node: func() *v1.Node {
				node := withCapacity.DeepCopy()
				node.Status.Conditions = []v1.NodeCondition{*makeReadyCondition(true, "", before, before)}
				return node
			}(),
			expectConditions: []v1.NodeCondition{*makeReadyCondition(true, "kubelet is posting ready status", before, now)},
			expectEvents:     []testEvent{},
		},
		{
			desc: "not ready, no transition",
			node: func() *v1.Node {
				node := withCapacity.DeepCopy()
				node.Status.Conditions = []v1.NodeCondition{*makeReadyCondition(false, "", before, before)}
				return node
			}(),
			runtimeErrors:    errors.New("foo"),
			expectConditions: []v1.NodeCondition{*makeReadyCondition(false, "foo", before, now)},
			expectEvents:     []testEvent{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			runtimeErrorsFunc := func() error {
				return tc.runtimeErrors
			}
			networkErrorsFunc := func() error {
				return tc.networkErrors
			}
			storageErrorsFunc := func() error {
				return tc.storageErrors
			}
			cmStatusFunc := func() cm.Status {
				return tc.cmStatus
			}
			nodeShutdownErrorsFunc := func() error {
				return tc.nodeShutdownManagerErrors
			}
			events := []testEvent{}
			recordEventFunc := func(logger klog.Logger, eventType, event string) {
				events = append(events, testEvent{
					eventType: eventType,
					event:     event,
				})
			}
			// 构造待测 setter。
			setter := ReadyCondition(nowFunc, runtimeErrorsFunc, networkErrorsFunc, storageErrorsFunc, cmStatusFunc, nodeShutdownErrorsFunc, recordEventFunc, !tc.disableLocalStorageCapacityIsolation)
			// 对 node 调用 setter。
			if err := setter(ctx, tc.node); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验 condition 是否符合预期。
			assert.True(t, apiequality.Semantic.DeepEqual(tc.expectConditions, tc.node.Status.Conditions),
				"Diff: %s", cmp.Diff(tc.expectConditions, tc.node.Status.Conditions))
			// 校验事件是否符合预期。
			require.Equal(t, len(tc.expectEvents), len(events))
			for i := range tc.expectEvents {
				assert.Equal(t, tc.expectEvents[i], events[i])
			}
		})
	}
}

// TestMemoryPressureCondition 验证内存压力 condition 的状态推导与事件；
// 无状态翻转时只刷新 heartbeat，保留原 transition time。
func TestMemoryPressureCondition(t *testing.T) {
	now := time.Now()
	before := now.Add(-time.Second)
	nowFunc := func() time.Time { return now }

	// before 为上次记录时间：状态未翻转时 LastTransitionTime 保持 before，
	// 只有 LastHeartbeatTime 更新为 now。
	cases := []struct {
		desc             string
		node             *v1.Node
		pressure         bool
		expectConditions []v1.NodeCondition
		expectEvents     []testEvent
	}{
		{
			desc:             "new, no pressure",
			node:             &v1.Node{},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makeMemoryPressureCondition(false, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasSufficientMemory",
				},
			},
		},
		{
			desc:             "new, pressure",
			node:             &v1.Node{},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makeMemoryPressureCondition(true, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasInsufficientMemory",
				},
			},
		},
		{
			desc: "transition to pressure",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeMemoryPressureCondition(false, before, before)},
				},
			},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makeMemoryPressureCondition(true, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasInsufficientMemory",
				},
			},
		},
		{
			desc: "transition to no pressure",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeMemoryPressureCondition(true, before, before)},
				},
			},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makeMemoryPressureCondition(false, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasSufficientMemory",
				},
			},
		},
		{
			desc: "pressure, no transition",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeMemoryPressureCondition(true, before, before)},
				},
			},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makeMemoryPressureCondition(true, before, now)},
			expectEvents:     []testEvent{},
		},
		{
			desc: "no pressure, no transition",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeMemoryPressureCondition(false, before, before)},
				},
			},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makeMemoryPressureCondition(false, before, now)},
			expectEvents:     []testEvent{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			events := []testEvent{}
			recordEventFunc := func(logger klog.Logger, eventType, event string) {
				events = append(events, testEvent{
					eventType: eventType,
					event:     event,
				})
			}
			pressureFunc := func() bool {
				return tc.pressure
			}
			// 构造待测 setter。
			setter := MemoryPressureCondition(nowFunc, pressureFunc, recordEventFunc)
			// 对 node 调用 setter。
			if err := setter(ctx, tc.node); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验 condition 是否符合预期。
			assert.True(t, apiequality.Semantic.DeepEqual(tc.expectConditions, tc.node.Status.Conditions),
				"Diff: %s", cmp.Diff(tc.expectConditions, tc.node.Status.Conditions))
			// 校验事件是否符合预期。
			require.Equal(t, len(tc.expectEvents), len(events))
			for i := range tc.expectEvents {
				assert.Equal(t, tc.expectEvents[i], events[i])
			}
		})
	}
}

// TestPIDPressureCondition 验证 PID 压力 condition 的状态推导与事件；
// 无状态翻转时只刷新 heartbeat，保留原 transition time。
func TestPIDPressureCondition(t *testing.T) {
	now := time.Now()
	before := now.Add(-time.Second)
	nowFunc := func() time.Time { return now }

	// before 为上次记录时间：状态未翻转时 LastTransitionTime 保持 before，
	// 只有 LastHeartbeatTime 更新为 now。
	cases := []struct {
		desc             string
		node             *v1.Node
		pressure         bool
		expectConditions []v1.NodeCondition
		expectEvents     []testEvent
	}{
		{
			desc:             "new, no pressure",
			node:             &v1.Node{},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makePIDPressureCondition(false, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasSufficientPID",
				},
			},
		},
		{
			desc:             "new, pressure",
			node:             &v1.Node{},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makePIDPressureCondition(true, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasInsufficientPID",
				},
			},
		},
		{
			desc: "transition to pressure",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makePIDPressureCondition(false, before, before)},
				},
			},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makePIDPressureCondition(true, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasInsufficientPID",
				},
			},
		},
		{
			desc: "transition to no pressure",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makePIDPressureCondition(true, before, before)},
				},
			},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makePIDPressureCondition(false, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasSufficientPID",
				},
			},
		},
		{
			desc: "pressure, no transition",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makePIDPressureCondition(true, before, before)},
				},
			},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makePIDPressureCondition(true, before, now)},
			expectEvents:     []testEvent{},
		},
		{
			desc: "no pressure, no transition",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makePIDPressureCondition(false, before, before)},
				},
			},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makePIDPressureCondition(false, before, now)},
			expectEvents:     []testEvent{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			events := []testEvent{}
			recordEventFunc := func(logger klog.Logger, eventType, event string) {
				events = append(events, testEvent{
					eventType: eventType,
					event:     event,
				})
			}
			pressureFunc := func() bool {
				return tc.pressure
			}
			// 构造待测 setter。
			setter := PIDPressureCondition(nowFunc, pressureFunc, recordEventFunc)
			// 对 node 调用 setter。
			if err := setter(ctx, tc.node); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验 condition 是否符合预期。
			assert.True(t, apiequality.Semantic.DeepEqual(tc.expectConditions, tc.node.Status.Conditions),
				"Diff: %s", cmp.Diff(tc.expectConditions, tc.node.Status.Conditions))
			// 校验事件是否符合预期。
			require.Equal(t, len(tc.expectEvents), len(events))
			for i := range tc.expectEvents {
				assert.Equal(t, tc.expectEvents[i], events[i])
			}
		})
	}
}

// TestDiskPressureCondition 验证磁盘压力 condition 的状态推导与事件；
// 无状态翻转时只刷新 heartbeat，保留原 transition time。
func TestDiskPressureCondition(t *testing.T) {
	now := time.Now()
	before := now.Add(-time.Second)
	nowFunc := func() time.Time { return now }

	// before 为上次记录时间：状态未翻转时 LastTransitionTime 保持 before，
	// 只有 LastHeartbeatTime 更新为 now。
	cases := []struct {
		desc             string
		node             *v1.Node
		pressure         bool
		expectConditions []v1.NodeCondition
		expectEvents     []testEvent
	}{
		{
			desc:             "new, no pressure",
			node:             &v1.Node{},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makeDiskPressureCondition(false, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasNoDiskPressure",
				},
			},
		},
		{
			desc:             "new, pressure",
			node:             &v1.Node{},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makeDiskPressureCondition(true, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasDiskPressure",
				},
			},
		},
		{
			desc: "transition to pressure",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeDiskPressureCondition(false, before, before)},
				},
			},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makeDiskPressureCondition(true, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasDiskPressure",
				},
			},
		},
		{
			desc: "transition to no pressure",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeDiskPressureCondition(true, before, before)},
				},
			},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makeDiskPressureCondition(false, now, now)},
			expectEvents: []testEvent{
				{
					eventType: v1.EventTypeNormal,
					event:     "NodeHasNoDiskPressure",
				},
			},
		},
		{
			desc: "pressure, no transition",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeDiskPressureCondition(true, before, before)},
				},
			},
			pressure:         true,
			expectConditions: []v1.NodeCondition{*makeDiskPressureCondition(true, before, now)},
			expectEvents:     []testEvent{},
		},
		{
			desc: "no pressure, no transition",
			node: &v1.Node{
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{*makeDiskPressureCondition(false, before, before)},
				},
			},
			pressure:         false,
			expectConditions: []v1.NodeCondition{*makeDiskPressureCondition(false, before, now)},
			expectEvents:     []testEvent{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			events := []testEvent{}
			recordEventFunc := func(logger klog.Logger, eventType, event string) {
				events = append(events, testEvent{
					eventType: eventType,
					event:     event,
				})
			}
			pressureFunc := func() bool {
				return tc.pressure
			}
			// 构造待测 setter。
			setter := DiskPressureCondition(nowFunc, pressureFunc, recordEventFunc)
			// 对 node 调用 setter。
			if err := setter(ctx, tc.node); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验 condition 是否符合预期。
			assert.True(t, apiequality.Semantic.DeepEqual(tc.expectConditions, tc.node.Status.Conditions),
				"Diff: %s", cmp.Diff(tc.expectConditions, tc.node.Status.Conditions))
			// 校验事件是否符合预期。
			require.Equal(t, len(tc.expectEvents), len(events))
			for i := range tc.expectEvents {
				assert.Equal(t, tc.expectEvents[i], events[i])
			}
		})
	}
}

// TestVolumesInUse 验证仅在卷管理器完成 sync 后才用实际在用的卷覆盖 node 状态，
// 否则保留旧列表，避免上报不完整数据。
func TestVolumesInUse(t *testing.T) {
	withVolumesInUse := &v1.Node{
		Status: v1.NodeStatus{
			VolumesInUse: []v1.UniqueVolumeName{"foo"},
		},
	}

	cases := []struct {
		desc               string
		node               *v1.Node
		synced             bool
		volumesInUse       []v1.UniqueVolumeName
		expectVolumesInUse []v1.UniqueVolumeName
	}{
		{
			desc:               "synced",
			node:               withVolumesInUse.DeepCopy(),
			synced:             true,
			volumesInUse:       []v1.UniqueVolumeName{"bar"},
			expectVolumesInUse: []v1.UniqueVolumeName{"bar"},
		},
		{
			desc:               "not synced",
			node:               withVolumesInUse.DeepCopy(),
			synced:             false,
			volumesInUse:       []v1.UniqueVolumeName{"bar"},
			expectVolumesInUse: []v1.UniqueVolumeName{"foo"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := ktesting.Init(t)
			syncedFunc := func() bool {
				return tc.synced
			}
			volumesInUseFunc := func() []v1.UniqueVolumeName {
				return tc.volumesInUse
			}
			// 构造待测 setter。
			setter := VolumesInUse(syncedFunc, volumesInUseFunc)
			// 对 node 调用 setter。
			if err := setter(ctx, tc.node); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 校验 VolumesInUse 是否符合预期。
			assert.True(t, apiequality.Semantic.DeepEqual(tc.expectVolumesInUse, tc.node.Status.VolumesInUse),
				"Diff: %s", cmp.Diff(tc.expectVolumesInUse, tc.node.Status.VolumesInUse))
		})
	}
}

// TestDaemonEndpoints 验证 DaemonEndpoints setter 将 kubelet 端点端口写入 node status。
func TestDaemonEndpoints(t *testing.T) {
	for _, test := range []struct {
		name      string
		endpoints *v1.NodeDaemonEndpoints
		expected  *v1.NodeDaemonEndpoints
	}{
		{
			name:      "empty daemon endpoints",
			endpoints: &v1.NodeDaemonEndpoints{},
			expected:  &v1.NodeDaemonEndpoints{KubeletEndpoint: v1.DaemonEndpoint{Port: 0}},
		},
		{
			name:      "daemon endpoints with specific port",
			endpoints: &v1.NodeDaemonEndpoints{KubeletEndpoint: v1.DaemonEndpoint{Port: 5678}},
			expected:  &v1.NodeDaemonEndpoints{KubeletEndpoint: v1.DaemonEndpoint{Port: 5678}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := ktesting.Init(t)
			existingNode := &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: testKubeletHostname,
				},
				Spec: v1.NodeSpec{},
				Status: v1.NodeStatus{
					Addresses: []v1.NodeAddress{},
				},
			}

			setter := DaemonEndpoints(test.endpoints)
			if err := setter(ctx, existingNode); err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, *test.expected, existingNode.Status.DaemonEndpoints)
		})
	}
}

// 测试辅助函数：

// testEvent 记录 setter 在测试中发出的事件，供断言事件类型与内容。
type testEvent struct {
	eventType string
	event     string
	message   string
}

// makeImageList 随机生成指定数量的镜像，每个镜像带指定数量的 tag，大小落在给定区间内。
func makeImageList(numImages, numTags, minSize, maxSize int32) []kubecontainer.Image {
	images := make([]kubecontainer.Image, numImages)
	for i := range images {
		image := &images[i]
		image.ID = string(uuid.NewUUID())
		image.RepoTags = makeImageTags(numTags)
		image.Size = rand.Int63nRange(int64(minSize), int64(maxSize+1))
	}
	return images
}

// makeExpectedImageList 由输入镜像推导期望的 node status 镜像列表：
// 按大小降序排序、截断每个镜像的 name 数量，并按 maxImages 截断整体列表。
func makeExpectedImageList(imageList []kubecontainer.Image, maxImages, maxNames int32) []v1.ContainerImage {
	// 复制一份，避免原地修改而污染测试用例数据
	images := make([]kubecontainer.Image, len(imageList))
	copy(images, imageList)
	// 按镜像大小排序
	sort.Sort(sliceutils.ByImageSize(images))
	// 转换为 []v1.ContainerImage 并截断每个镜像的 name 列表
	expectedImages := make([]v1.ContainerImage, len(images))
	for i := range images {
		image := &images[i]
		expectedImage := &expectedImages[i]
		names := append(image.RepoDigests, image.RepoTags...)
		if len(names) > int(maxNames) {
			names = names[0:maxNames]
		}
		expectedImage.Names = names
		expectedImage.SizeBytes = image.Size
	}
	// maxImages 为 -1 表示不限制，否则按上限截断结果列表
	if maxImages > -1 &&
		int(maxImages) < len(expectedImages) {
		return expectedImages[0:maxImages]
	}
	return expectedImages
}

// makeImageTags 生成 num 个形如 registry.k8s.io:vN 的测试 tag。
func makeImageTags(num int32) []string {
	tags := make([]string, num)
	for i := range tags {
		tags[i] = "registry.k8s.io:v" + strconv.Itoa(i)
	}
	return tags
}

// makeReadyCondition 构造期望的 NodeReady condition，让测试表能简洁表达状态与时间戳。
func makeReadyCondition(ready bool, message string, transition, heartbeat time.Time) *v1.NodeCondition {
	if ready {
		return &v1.NodeCondition{
			Type:               v1.NodeReady,
			Status:             v1.ConditionTrue,
			Reason:             "KubeletReady",
			Message:            message,
			LastTransitionTime: metav1.NewTime(transition),
			LastHeartbeatTime:  metav1.NewTime(heartbeat),
		}
	}
	return &v1.NodeCondition{
		Type:               v1.NodeReady,
		Status:             v1.ConditionFalse,
		Reason:             "KubeletNotReady",
		Message:            message,
		LastTransitionTime: metav1.NewTime(transition),
		LastHeartbeatTime:  metav1.NewTime(heartbeat),
	}
}

// makeMemoryPressureCondition 构造期望的 NodeMemoryPressure condition。
func makeMemoryPressureCondition(pressure bool, transition, heartbeat time.Time) *v1.NodeCondition {
	if pressure {
		return &v1.NodeCondition{
			Type:               v1.NodeMemoryPressure,
			Status:             v1.ConditionTrue,
			Reason:             "KubeletHasInsufficientMemory",
			Message:            "kubelet has insufficient memory available",
			LastTransitionTime: metav1.NewTime(transition),
			LastHeartbeatTime:  metav1.NewTime(heartbeat),
		}
	}
	return &v1.NodeCondition{
		Type:               v1.NodeMemoryPressure,
		Status:             v1.ConditionFalse,
		Reason:             "KubeletHasSufficientMemory",
		Message:            "kubelet has sufficient memory available",
		LastTransitionTime: metav1.NewTime(transition),
		LastHeartbeatTime:  metav1.NewTime(heartbeat),
	}
}

// makePIDPressureCondition 构造期望的 NodePIDPressure condition。
func makePIDPressureCondition(pressure bool, transition, heartbeat time.Time) *v1.NodeCondition {
	if pressure {
		return &v1.NodeCondition{
			Type:               v1.NodePIDPressure,
			Status:             v1.ConditionTrue,
			Reason:             "KubeletHasInsufficientPID",
			Message:            "kubelet has insufficient PID available",
			LastTransitionTime: metav1.NewTime(transition),
			LastHeartbeatTime:  metav1.NewTime(heartbeat),
		}
	}
	return &v1.NodeCondition{
		Type:               v1.NodePIDPressure,
		Status:             v1.ConditionFalse,
		Reason:             "KubeletHasSufficientPID",
		Message:            "kubelet has sufficient PID available",
		LastTransitionTime: metav1.NewTime(transition),
		LastHeartbeatTime:  metav1.NewTime(heartbeat),
	}
}

// makeDiskPressureCondition 构造期望的 NodeDiskPressure condition。
func makeDiskPressureCondition(pressure bool, transition, heartbeat time.Time) *v1.NodeCondition {
	if pressure {
		return &v1.NodeCondition{
			Type:               v1.NodeDiskPressure,
			Status:             v1.ConditionTrue,
			Reason:             "KubeletHasDiskPressure",
			Message:            "kubelet has disk pressure",
			LastTransitionTime: metav1.NewTime(transition),
			LastHeartbeatTime:  metav1.NewTime(heartbeat),
		}
	}
	return &v1.NodeCondition{
		Type:               v1.NodeDiskPressure,
		Status:             v1.ConditionFalse,
		Reason:             "KubeletHasNoDiskPressure",
		Message:            "kubelet has no disk pressure",
		LastTransitionTime: metav1.NewTime(transition),
		LastHeartbeatTime:  metav1.NewTime(heartbeat),
	}
}
