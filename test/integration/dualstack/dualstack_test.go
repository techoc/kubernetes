/*
Copyright 2020 The Kubernetes Authors.

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

package dualstack

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"
)

// TestCreateServiceSingleStackIPv4 测试 IPv4 单栈集群中 Service 的双栈特性
func TestCreateServiceSingleStackIPv4(t *testing.T) {
	for _, enableMultiServiceCIDR := range []bool{false, true} {
		for _, disableAllocatorDualWrite := range []bool{false, true} {
			if !enableMultiServiceCIDR && disableAllocatorDualWrite {
				// 无效配置：DisableAllocatorDualWrite 依赖 MultiServiceCIDR
				continue
			}
			t.Run(fmt.Sprintf("MultiServiceCIDR=%v DisableAllocatorDualWrite=%v", enableMultiServiceCIDR, disableAllocatorDualWrite), func(t *testing.T) {
				// 创建 IPv4 单栈控制平面
				tCtx := ktesting.Init(t)
				etcdOptions := framework.SharedEtcd()
				apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
				flags := []string{
					"--service-cluster-ip-range=10.0.0.0/16",
					"--advertise-address=10.1.1.1",
					"--disable-admission-plugins=ServiceAccount",
					fmt.Sprintf("--feature-gates=%s=%v,%s=%v", features.MultiCIDRServiceAllocator, enableMultiServiceCIDR, features.DisableAllocatorDualWrite, disableAllocatorDualWrite),
				}
				if !enableMultiServiceCIDR || !disableAllocatorDualWrite {
					flags = append(flags, "--emulated-version=1.33")
				}
				s := kubeapiservertesting.StartTestServerOrDie(t,
					apiServerOptions,
					flags,
					etcdOptions)
				defer s.TearDownFn()

				client, err := kubernetes.NewForConfig(s.ClientConfig)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// 等待默认的 "kubernetes" Service 创建完成。
				if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
					_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						return false, err
					}
					return !apierrors.IsNotFound(err), nil
				}); err != nil {
					t.Fatalf("creating kubernetes service timed out")
				}

				var testcases = []struct {
					name               string
					serviceType        v1.ServiceType
					clusterIPs         []string
					ipFamilies         []v1.IPFamily
					ipFamilyPolicy     v1.IPFamilyPolicy
					expectedIPFamilies []v1.IPFamily
					expectError        bool
				}{
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         nil,
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Client Allocated IP - Default IP Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{"10.0.0.16"},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         nil,
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         nil,
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        true,
					},
				}

				for i, tc := range testcases {
					t.Run(tc.name, func(t *testing.T) {

						svc := &v1.Service{
							ObjectMeta: metav1.ObjectMeta{
								Name: fmt.Sprintf("svc-test-%d", i), // 每个测试使用不同的 Service
							},
							Spec: v1.ServiceSpec{
								Type:       tc.serviceType,
								ClusterIPs: tc.clusterIPs,
								IPFamilies: tc.ipFamilies,
								Ports: []v1.ServicePort{
									{
										Port:       443,
										TargetPort: intstr.FromInt32(443),
									},
								},
							},
						}

						if len(tc.ipFamilyPolicy) > 0 {
							svc.Spec.IPFamilyPolicy = &tc.ipFamilyPolicy
						}

						if len(tc.clusterIPs) > 0 {
							svc.Spec.ClusterIP = tc.clusterIPs[0]
						}

						// 创建 Service
						_, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
						if (err != nil) != tc.expectError {
							t.Errorf("Test failed expected result: %v received %v ", tc.expectError, err)
						}
						// 如果预期没有错误则校验 Service，否则返回
						if err != nil {
							return
						}
						// 如果预期不会失败，则校验 Service 是否创建正确
						svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
						if err != nil {
							t.Errorf("Unexpected error to get the service %s %v", svc.Name, err)
						}
						if err := validateServiceAndClusterIPFamily(svc, tc.expectedIPFamilies); err != nil {
							t.Errorf("Unexpected error validating the service %s\n%+v\n%v", svc.Name, svc, err)
						}
					})
				}
			})
		}
	}
}

// TestCreateServiceSingleStackIPv6 测试仅 IPv6 的 DoubleStack 集群中 Service 的双栈特性
func TestCreateServiceSingleStackIPv6(t *testing.T) {
	for _, enableMultiServiceCIDR := range []bool{false, true} {
		for _, disableAllocatorDualWrite := range []bool{false, true} {
			if !enableMultiServiceCIDR && disableAllocatorDualWrite {
				// 无效配置：DisableAllocatorDualWrite 依赖 MultiServiceCIDR
				continue
			}
			t.Run(fmt.Sprintf("MultiServiceCIDR=%v DisableAllocatorDualWrite=%v", enableMultiServiceCIDR, disableAllocatorDualWrite), func(t *testing.T) {
				// 创建仅 IPv6 的控制平面
				tCtx := ktesting.Init(t)
				etcdOptions := framework.SharedEtcd()
				apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
				flags := []string{
					"--service-cluster-ip-range=2001:db8:1::/108",
					"--advertise-address=2001:db8::10",
					"--disable-admission-plugins=ServiceAccount",
					fmt.Sprintf("--feature-gates=%s=%v,%s=%v", features.MultiCIDRServiceAllocator, enableMultiServiceCIDR, features.DisableAllocatorDualWrite, disableAllocatorDualWrite),
				}
				if !enableMultiServiceCIDR || !disableAllocatorDualWrite {
					flags = append(flags, "--emulated-version=1.33")
				}
				s := kubeapiservertesting.StartTestServerOrDie(t,
					apiServerOptions,
					flags,
					etcdOptions)
				defer s.TearDownFn()

				client, err := kubernetes.NewForConfig(s.ClientConfig)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// 等待默认的 "kubernetes" Service 创建完成。
				if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
					_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						return false, err
					}
					return !apierrors.IsNotFound(err), nil
				}); err != nil {
					t.Fatalf("creating kubernetes service timed out")
				}

				var testcases = []struct {
					name               string
					serviceType        v1.ServiceType
					clusterIPs         []string
					ipFamilies         []v1.IPFamily
					expectedIPFamilies []v1.IPFamily
					ipFamilyPolicy     v1.IPFamilyPolicy
					expectError        bool
				}{
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        true,
					},
				}

				for i, tc := range testcases {
					t.Run(tc.name, func(t *testing.T) {

						svc := &v1.Service{
							ObjectMeta: metav1.ObjectMeta{
								Name: fmt.Sprintf("svc-test-%d", i), // 每个测试使用不同的 Service
							},
							Spec: v1.ServiceSpec{
								Type:           tc.serviceType,
								ClusterIPs:     tc.clusterIPs,
								IPFamilies:     tc.ipFamilies,
								IPFamilyPolicy: &tc.ipFamilyPolicy,
								Ports: []v1.ServicePort{
									{
										Name:       fmt.Sprintf("port-test-%d", i),
										Port:       443,
										TargetPort: intstr.IntOrString{IntVal: 443},
										Protocol:   "TCP",
									},
								},
							},
						}

						// 创建 Service
						_, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
						if (err != nil) != tc.expectError {
							t.Errorf("Test failed expected result: %v received %v ", tc.expectError, err)
						}
						// 如果预期没有错误则校验 Service，否则返回
						if err != nil {
							return
						}
						// 如果预期不会失败，则校验 Service 是否创建正确
						svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
						if err != nil {
							t.Errorf("Unexpected error to get the service %s %v", svc.Name, err)
						}
						if err := validateServiceAndClusterIPFamily(svc, tc.expectedIPFamilies); err != nil {
							t.Errorf("Unexpected error validating the service %s %v", svc.Name, err)
						}
					})
				}
			})
		}
	}
}

// TestCreateServiceDualStackIPv4IPv6 测试 IPv4IPv6 双栈集群中 Service 的双栈特性
func TestCreateServiceDualStackIPv4IPv6(t *testing.T) {
	for _, enableMultiServiceCIDR := range []bool{false, true} {
		for _, disableAllocatorDualWrite := range []bool{false, true} {
			if !enableMultiServiceCIDR && disableAllocatorDualWrite {
				// 无效配置：DisableAllocatorDualWrite 依赖 MultiServiceCIDR
				continue
			}
			t.Run(fmt.Sprintf("MultiServiceCIDR=%v DisableAllocatorDualWrite=%v", enableMultiServiceCIDR, disableAllocatorDualWrite), func(t *testing.T) {
				// 创建 IPv4IPv6 双栈控制平面
				tCtx := ktesting.Init(t)
				etcdOptions := framework.SharedEtcd()
				apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
				flags := []string{
					"--service-cluster-ip-range=10.0.0.0/16,2001:db8:1::/108",
					"--advertise-address=10.0.0.1",
					"--disable-admission-plugins=ServiceAccount",
					fmt.Sprintf("--feature-gates=%s=%v,%s=%v", features.MultiCIDRServiceAllocator, enableMultiServiceCIDR, features.DisableAllocatorDualWrite, disableAllocatorDualWrite),
				}
				if !enableMultiServiceCIDR || !disableAllocatorDualWrite {
					flags = append(flags, "--emulated-version=1.33")
				}
				s := kubeapiservertesting.StartTestServerOrDie(t,
					apiServerOptions,
					flags,
					etcdOptions)
				defer s.TearDownFn()

				client, err := kubernetes.NewForConfig(s.ClientConfig)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// 等待默认的 "kubernetes" Service 创建完成。
				if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
					_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						return false, err
					}
					return !apierrors.IsNotFound(err), nil
				}); err != nil {
					t.Fatalf("creating kubernetes service timed out")
				}

				var testcases = []struct {
					name               string
					serviceType        v1.ServiceType
					clusterIPs         []string
					ipFamilies         []v1.IPFamily
					expectedIPFamilies []v1.IPFamily
					ipFamilyPolicy     v1.IPFamilyPolicy
					expectError        bool
				}{
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Client Allocated IP - IPv4 Family",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{"10.0.0.16"},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Client Allocated IP - IPv6 Family",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{"2001:db8:1::16"},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Client Allocated IP - IPv4 IPv6 Family ",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{"10.0.0.17", "2001:db8:1::17"},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Client Allocated IP - IPv4 IPv6 Family ",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{"10.0.0.17", "2001:db8:1::17"},
						ipFamilies:         nil,
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Client Allocated IP - IPv4 IPv6 Family ",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{"10.0.0.18", "2001:db8:1::18"},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},

					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
				}

				for i, tc := range testcases {
					t.Run(tc.name, func(t *testing.T) {

						svc := &v1.Service{
							ObjectMeta: metav1.ObjectMeta{
								Name: fmt.Sprintf("svc-test-%d", i), // 每个测试使用不同的 Service
							},
							Spec: v1.ServiceSpec{
								Type:       tc.serviceType,
								ClusterIPs: tc.clusterIPs,
								IPFamilies: tc.ipFamilies,
								Ports: []v1.ServicePort{
									{
										Port:       443,
										TargetPort: intstr.FromInt32(443),
									},
								},
							},
						}

						if len(tc.ipFamilyPolicy) > 0 {
							svc.Spec.IPFamilyPolicy = &tc.ipFamilyPolicy
						}

						if len(tc.clusterIPs) > 0 {
							svc.Spec.ClusterIP = tc.clusterIPs[0]
						}

						// 创建 Service
						_, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
						if (err != nil) != tc.expectError {
							t.Errorf("Test failed expected result: %v received %v ", tc.expectError, err)
						}
						// 如果预期没有错误则校验 Service，否则返回
						if err != nil {
							return
						}
						// 如果预期不会失败，则校验 Service 是否创建正确
						svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
						if err != nil {
							t.Errorf("Unexpected error to get the service %s %v", svc.Name, err)
						}

						if err := validateServiceAndClusterIPFamily(svc, tc.expectedIPFamilies); err != nil {
							t.Errorf("Unexpected error validating the service %s %v", svc.Name, err)
						}
					})
				}
			})
		}
	}
}

// TestCreateServiceDualStackIPv6IPv4 测试 IPv6IPv4 双栈集群中 Service 的双栈特性
func TestCreateServiceDualStackIPv6IPv4(t *testing.T) {
	for _, enableMultiServiceCIDR := range []bool{false, true} {
		for _, disableAllocatorDualWrite := range []bool{false, true} {
			if !enableMultiServiceCIDR && disableAllocatorDualWrite {
				// 无效配置：DisableAllocatorDualWrite 依赖 MultiServiceCIDR
				continue
			}
			t.Run(fmt.Sprintf("MultiServiceCIDR=%v DisableAllocatorDualWrite=%v", enableMultiServiceCIDR, disableAllocatorDualWrite), func(t *testing.T) {
				// 创建 IPv6IPv4 双栈控制平面
				tCtx := ktesting.Init(t)
				etcdOptions := framework.SharedEtcd()
				apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
				flags := []string{
					"--service-cluster-ip-range=2001:db8:1::/108,10.0.0.0/16",
					"--advertise-address=2001:db8::10",
					"--disable-admission-plugins=ServiceAccount",
					fmt.Sprintf("--feature-gates=%s=%v,%s=%v", features.MultiCIDRServiceAllocator, enableMultiServiceCIDR, features.DisableAllocatorDualWrite, disableAllocatorDualWrite),
				}
				if !enableMultiServiceCIDR || !disableAllocatorDualWrite {
					flags = append(flags, "--emulated-version=1.33")
				}
				s := kubeapiservertesting.StartTestServerOrDie(t,
					apiServerOptions,
					flags,
					etcdOptions)
				defer s.TearDownFn()

				client, err := kubernetes.NewForConfig(s.ClientConfig)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// 等待默认的 "kubernetes" Service 创建完成。
				if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
					_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						return false, err
					}
					return !apierrors.IsNotFound(err), nil
				}); err != nil {
					t.Fatalf("creating kubernetes service timed out")
				}

				// 验证客户端可用
				if err := wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
					_, err := client.CoreV1().Endpoints("default").Get(tCtx, "kubernetes", metav1.GetOptions{})
					if err != nil {
						t.Logf("error fetching endpoints: %v", err)
						return false, nil
					}
					return true, nil
				}); err != nil {
					t.Errorf("server without enabled endpoints failed to register: %v", err)
				}

				var testcases = []struct {
					name               string
					serviceType        v1.ServiceType
					clusterIPs         []string
					ipFamilies         []v1.IPFamily
					expectedIPFamilies []v1.IPFamily
					ipFamilyPolicy     v1.IPFamilyPolicy
					expectError        bool
				}{
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - Default IP Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         nil,
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},

					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv4 IPv6 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Single Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicySingleStack,
						expectError:        true,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Prefer Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyPreferDualStack,
						expectError:        false,
					},
					{
						name:               "Type ClusterIP - Server Allocated IP - IPv6 IPv4 Family - Policy Required Dual Stack",
						serviceType:        v1.ServiceTypeClusterIP,
						clusterIPs:         []string{},
						ipFamilies:         []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						expectedIPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
						ipFamilyPolicy:     v1.IPFamilyPolicyRequireDualStack,
						expectError:        false,
					},
				}

				for i, tc := range testcases {
					t.Run(tc.name, func(t *testing.T) {

						svc := &v1.Service{
							ObjectMeta: metav1.ObjectMeta{
								Name: fmt.Sprintf("svc-test-%d", i), // 每个测试使用不同的 Service
							},
							Spec: v1.ServiceSpec{
								Type:           tc.serviceType,
								ClusterIPs:     tc.clusterIPs,
								IPFamilies:     tc.ipFamilies,
								IPFamilyPolicy: &tc.ipFamilyPolicy,
								Ports: []v1.ServicePort{
									{
										Port:       443,
										TargetPort: intstr.FromInt32(443),
									},
								},
							},
						}

						// 创建 Service
						_, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
						if (err != nil) != tc.expectError {
							t.Errorf("Test failed expected result: %v received %v ", tc.expectError, err)
						}
						// 如果预期没有错误则校验 Service，否则返回
						if err != nil {
							return
						}
						// 如果预期不会失败，则校验 Service 是否创建正确
						svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
						if err != nil {
							t.Errorf("Unexpected error to get the service %s %v", svc.Name, err)
						}

						if err := validateServiceAndClusterIPFamily(svc, tc.expectedIPFamilies); err != nil {
							t.Errorf("Unexpected error validating the service %s %v", svc.Name, err)
						}
					})
				}
			})
		}
	}
}

// TestUpgradeDowngrade 测试 Service 升级到双栈以及从双栈降级
func TestUpgradeDowngrade(t *testing.T) {
	// 创建 IPv4IPv6 双栈控制平面
	tCtx := ktesting.Init(t)
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/16,2001:db8:1::/108",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}

	upgradeServiceName := "svc-upgrade"

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: upgradeServiceName,
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{
					Port:       443,
					TargetPort: intstr.FromInt32(443),
				},
			},
		},
	}

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("unexpected error while creating service:%v", err)
	}
	// 如果预期不会失败，则校验 Service 是否创建正确
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}

	if err := validateServiceAndClusterIPFamily(svc, []v1.IPFamily{v1.IPv4Protocol} /* 默认集群配置 */); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}

	// 升级 Service
	requireDualStack := v1.IPFamilyPolicyRequireDualStack
	svc.Spec.IPFamilyPolicy = &requireDualStack
	upgraded, err := client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error upgrading service to dual stack. %v", err)
	}
	if err := validateServiceAndClusterIPFamily(upgraded, []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol} /* +1 地址族 */); err != nil {
		t.Fatalf("Unexpected error validating the service(after upgrade) %s %v", svc.Name, err)
	}

	// 降级 Service
	singleStack := v1.IPFamilyPolicySingleStack
	upgraded.Spec.IPFamilyPolicy = &singleStack
	upgraded.Spec.ClusterIPs = upgraded.Spec.ClusterIPs[0:1]
	upgraded.Spec.IPFamilies = upgraded.Spec.IPFamilies[0:1]
	downgraded, err := client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, upgraded, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error downgrading service to single stack. %v", err)
	}
	if err := validateServiceAndClusterIPFamily(downgraded, []v1.IPFamily{v1.IPv4Protocol} /* -1 地址族 */); err != nil {
		t.Fatalf("unexpected error validating the service(after downgrade) %s %v", svc.Name, err)
	}

	// 再次运行测试，这次不移除次要 IPFamily 或 ClusterIP
	downgraded.Spec.IPFamilyPolicy = &requireDualStack
	upgradedAgain, err := client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, downgraded, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error upgrading service to dual stack. %v", err)
	}
	if err := validateServiceAndClusterIPFamily(upgradedAgain, []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol} /* +1 地址族 */); err != nil {
		t.Fatalf("Unexpected error validating the service(after upgrade) %s %v", svc.Name, err)
	}

	upgradedAgain.Spec.IPFamilyPolicy = &singleStack
	// 当 Service 被降级时，
	// api-server 会自动移除次要的 ClusterIP 和 IPFamily。
	downgradedAgain, err := client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, upgradedAgain, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error downgrading service to single stack. %v", err)
	}
	if err := validateServiceAndClusterIPFamily(downgradedAgain, []v1.IPFamily{v1.IPv4Protocol} /* -1 地址族 */); err != nil {
		t.Fatalf("unexpected error validating the service(after downgrade) %s %v", svc.Name, err)
	}
}

// TestConvertToFromExternalName 测试与可能不清空
// ClusterIPs 的旧客户端的兼容性
func TestConvertToFromExternalName(t *testing.T) {
	// 创建 IPv4IPv6 双栈控制平面
	tCtx := ktesting.Init(t)
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/16,2001:db8:1::/108",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}

	serviceName := "svc-ext-name"
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: serviceName,
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{
					Port:       443,
					TargetPort: intstr.FromInt32(443),
				},
			},
		},
	}

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("unexpected error while creating service:%v", err)
	}
	// 如果预期不会失败，则校验 Service 是否创建正确
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}

	if err := validateServiceAndClusterIPFamily(svc, []v1.IPFamily{v1.IPv4Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}

	// 转换为 ExternalName
	svc.Spec.Type = v1.ServiceTypeExternalName
	svc.Spec.ClusterIP = "" // 不清空 ClusterIPs
	svc.Spec.ExternalName = "something.somewhere"

	externalNameSvc, err := client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error converting service to external name. %v", err)
	}

	if len(externalNameSvc.Spec.ClusterIPs) > 0 || len(externalNameSvc.Spec.ClusterIP) > 0 || len(externalNameSvc.Spec.IPFamilies) > 0 {
		t.Fatalf("unpexpected externalname service with ClusterIPs %v or ClusterIP %v or IPFamilies %v", externalNameSvc.Spec.ClusterIPs, externalNameSvc.Spec.ClusterIP, externalNameSvc.Spec.IPFamilies)
	}

	// 转换回 ClusterIP Service
	externalNameSvc.Spec.Type = v1.ServiceTypeClusterIP
	externalNameSvc.Spec.ExternalName = ""
	clusterIPSvc, err := client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, externalNameSvc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error converting service to ClusterIP. %v", err)
	}
	if err := validateServiceAndClusterIPFamily(clusterIPSvc, []v1.IPFamily{v1.IPv4Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}
}

// TestPreferDualStack 测试创建和更新时的 preferDualstack
func TestPreferDualStack(t *testing.T) {
	// 创建 IPv4IPv6 双栈控制平面
	tCtx := ktesting.Init(t)
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/16,2001:db8:1::/108",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}

	preferDualStack := v1.IPFamilyPolicyPreferDualStack

	serviceName := "svc-upgrade"

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: serviceName,
		},
		Spec: v1.ServiceSpec{
			Type:           v1.ServiceTypeClusterIP,
			IPFamilyPolicy: &preferDualStack,
			Ports: []v1.ServicePort{
				{
					Port:       443,
					TargetPort: intstr.FromInt32(443),
				},
			},
		},
	}

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("unexpected error while creating service:%v", err)
	}
	// 如果预期不会失败，则校验 Service 是否创建正确
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}

	if err := validateServiceAndClusterIPFamily(svc, []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}

	// 更新 Service
	svc.Spec.Selector = map[string]string{"foo": "bar"}
	upgraded, err := client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error upgrading service to dual stack. %v", err)
	}
	if err := validateServiceAndClusterIPFamily(upgraded, []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service(after upgrade) %s %v", svc.Name, err)
	}
}

type labelsForMergePatch struct {
	Labels map[string]string `json:"lables,omitempty"`
}

// 测试 dualstack 开关关闭时更新 Service
func TestServiceUpdate(t *testing.T) {
	// 创建 IPv4 单栈控制平面
	tCtx := ktesting.Init(t)
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=10.0.0.0/16",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}

	serviceName := "test-service"
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: serviceName,
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{
					Port:       443,
					TargetPort: intstr.FromInt32(443),
				},
			},
		},
	}

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
	// 如果预期没有错误则校验 Service，否则返回
	if err != nil {
		t.Errorf("unexpected error creating service:%v", err)
		return
	}

	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Errorf("Unexpected error to get the service %s %v", svc.Name, err)
	}

	// 使用 PUT 更新
	svc.Labels = map[string]string{"x": "y"}
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Update(tCtx, svc, metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("Unexpected error updating the service %s %v", svc.Name, err)
	}

	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}

	// 使用 StrategicMergePatchType 更新
	labels := labelsForMergePatch{
		Labels: map[string]string{"foo": "bar"},
	}

	patchBytes, err := json.Marshal(&labels)
	if err != nil {
		t.Fatalf("failed to json.Marshal labels: %v", err)
	}

	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Patch(tCtx, svc.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("unexpected error patching service using strategic merge patch. %v", err)
	}

	current, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}

	// 使用 JSON Patch 更新
	toUpdate := current.DeepCopy()
	currentJSON, err := json.Marshal(current)
	if err != nil {
		t.Fatalf("unexpected error marshal current service. %v", err)
	}
	toUpdate.Labels = map[string]string{"alpha": "bravo"}
	toUpdateJSON, err := json.Marshal(toUpdate)
	if err != nil {
		t.Fatalf("unexpected error marshal toupdate service. %v", err)
	}

	patchBytes, err = jsonpatch.CreateMergePatch(currentJSON, toUpdateJSON)
	if err != nil {
		t.Fatalf("unexpected error creating json patch. %v", err)
	}

	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Patch(tCtx, svc.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("unexpected error patching service using merge patch. %v", err)
	}

	// 如果预期不会失败，则校验 Service 是否创建正确
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}
}

// validateServiceAndClusterIPFamily 检查 Service 是否具有预期的 IPFamilies
func validateServiceAndClusterIPFamily(svc *v1.Service, expectedIPFamilies []v1.IPFamily) error {
	// 创建一个切片用于存放错误
	var errstrings []string

	if svc.Spec.IPFamilies == nil {
		return fmt.Errorf("service ip family nil for service %s/%s", svc.Namespace, svc.Name)
	}
	if !reflect.DeepEqual(svc.Spec.IPFamilies, expectedIPFamilies) {
		return fmt.Errorf("ip families mismatch for service: %s/%s, expected: %s, actual: %s", svc.Namespace, svc.Name, expectedIPFamilies, svc.Spec.IPFamilies)
	}

	if len(svc.Spec.ClusterIPs) == 0 {
		return fmt.Errorf("svc %s is invalid it does not have ClusterIP", svc.Name)
	}

	// 非 headless Service
	if len(svc.Spec.ClusterIPs) > 0 && svc.Spec.ClusterIPs[0] != v1.ClusterIPNone {
		if len(svc.Spec.ClusterIPs) != len(svc.Spec.IPFamilies) {
			return fmt.Errorf("svc %v is invalid len(ClusterIPs:%v) != len(IPFamilies:%v)", svc.Name, svc.Spec.ClusterIPs, svc.Spec.IPFamilies)
		}
	}

	for j, ip := range svc.Spec.ClusterIPs {
		// 正常情况下不应执行到这里
		if ip == v1.ClusterIPNone && len(svc.Spec.ClusterIPs) > 1 {
			errstrings = append(errstrings, fmt.Sprintf("Error validating Service: %s, None is used with +1 clusterIPs (%v)", svc.Name, svc.Spec.ClusterIPs))
		}

		if ip == v1.ClusterIPNone {
			break // Service 是 headless，其余地址族检查没有意义
		}

		// 分配的 clusterIP 应与请求的 IPFamily 一致
		if netutils.IsIPv6String(ip) != (expectedIPFamilies[j] == v1.IPv6Protocol) {
			errstrings = append(errstrings, fmt.Sprintf("got unexpected service ip %s, should belong to %s ip family", ip, expectedIPFamilies[j]))
		}
	}

	if len(errstrings) > 0 {
		errstrings = append(errstrings, fmt.Sprintf("Error validating Service: %s, ClusterIPs: %v Expected IPFamilies %v", svc.Name, svc.Spec.ClusterIPs, expectedIPFamilies))
		return errors.New(strings.Join(errstrings, "\n"))
	}

	return nil
}

func TestUpgradeServicePreferToDualStack(t *testing.T) {
	sharedEtcd := framework.SharedEtcd()
	tCtx := ktesting.Init(t)

	// 创建仅 IPv4 的控制平面
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=192.168.0.0/24",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		sharedEtcd)

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}

	preferDualStack := v1.IPFamilyPolicyPreferDualStack
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc-prefer-dual",
		},
		Spec: v1.ServiceSpec{
			Type:           v1.ServiceTypeClusterIP,
			ClusterIPs:     nil,
			IPFamilies:     nil,
			IPFamilyPolicy: &preferDualStack,
			Ports: []v1.ServicePort{
				{
					Name:       "svc-port-1",
					Port:       443,
					TargetPort: intstr.IntOrString{IntVal: 443},
					Protocol:   "TCP",
				},
			},
		},
	}

	// 复制一份 Service，以便重新配置控制平面后测试再次创建
	svcDual := svc.DeepCopy()
	svcDual.Name = "svc-dual"

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// 如果预期不会失败，则校验 Service 是否创建正确
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}
	if err := validateServiceAndClusterIPFamily(svc, []v1.IPFamily{v1.IPv4Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}

	// 将 apiserver 重新配置为双栈
	s.TearDownFn()

	s = kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=192.168.0.0/24,2001:db8:1::/108",
			"--disable-admission-plugins=ServiceAccount",
		},
		sharedEtcd)
	defer s.TearDownFn()

	client, err = kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 等待默认的 "kubernetes" Service 创建完成。
	if err = wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}
	// 如果预期不会失败，则校验 Service 是否创建正确
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}
	// Service 应保持单栈
	if err = validateServiceAndClusterIPFamily(svc, []v1.IPFamily{v1.IPv4Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}
	// 验证以 prefer dual 方式新建的 Service 现在是双栈的

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svcDual, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// 校验 Service 是否创建正确
	svcDual, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svcDual.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svcDual.Name, err)
	}
	// Service 应为双栈
	if err = validateServiceAndClusterIPFamily(svcDual, []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svcDual.Name, err)
	}
}

func TestDowngradeServicePreferFromDualStack(t *testing.T) {
	tCtx := ktesting.Init(t)

	// 创建双栈控制平面
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=192.168.0.0/24,2001:db8:1::/108",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}
	preferDualStack := v1.IPFamilyPolicyPreferDualStack
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc-prefer-dual01",
		},
		Spec: v1.ServiceSpec{
			Type:           v1.ServiceTypeClusterIP,
			ClusterIPs:     nil,
			IPFamilies:     nil,
			IPFamilyPolicy: &preferDualStack,
			Ports: []v1.ServicePort{
				{
					Name:       "svc-port-1",
					Port:       443,
					TargetPort: intstr.IntOrString{IntVal: 443},
					Protocol:   "TCP",
				},
			},
		},
	}

	// 复制一份 Service，以便重新配置控制平面后测试再次创建
	svcSingle := svc.DeepCopy()
	svcSingle.Name = "svc-single"

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// 如果预期不会失败，则校验 Service 是否创建正确
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}
	if err := validateServiceAndClusterIPFamily(svc, []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}
	// 将 apiserver 重新配置为单栈
	s.TearDownFn()

	// 重置次要网段
	s = kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=192.168.0.0/24",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err = kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// 等待默认的 "kubernetes" Service 创建完成。
	if err = wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}
	// 校验 Service 仍然存在。
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svc.Name, err)
	}
	// Service 应保持双栈
	if err = validateServiceAndClusterIPFamily(svc, []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svc.Name, err)
	}

	// 验证以 prefer dual 方式新建的 Service 现在是单栈的

	// 创建 Service
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, svcSingle, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// 校验 Service 是否创建正确
	svcSingle, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svcSingle.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error to get the service %s %v", svcSingle.Name, err)
	}
	// Service 应为单栈
	if err = validateServiceAndClusterIPFamily(svcSingle, []v1.IPFamily{v1.IPv4Protocol}); err != nil {
		t.Fatalf("Unexpected error validating the service %s %v", svcSingle.Name, err)
	}
}

type serviceMergePatch struct {
	Spec specMergePatch `json:"spec,omitempty"`
}
type specMergePatch struct {
	Type         v1.ServiceType `json:"type,omitempty"`
	ExternalName string         `json:"externalName,omitempty"`
}

// 测试将 ClusterIP:Headless Service 转换为 ExternalName 能成功
func Test_ServiceChangeTypeHeadlessToExternalNameWithPatch(t *testing.T) {
	tCtx := ktesting.Init(t)
	etcdOptions := framework.SharedEtcd()
	apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
	s := kubeapiservertesting.StartTestServerOrDie(t,
		apiServerOptions,
		[]string{
			"--service-cluster-ip-range=192.168.0.0/24",
			"--advertise-address=10.0.0.1",
			"--disable-admission-plugins=ServiceAccount",
		},
		etcdOptions)
	defer s.TearDownFn()

	client, err := kubernetes.NewForConfig(s.ClientConfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ns := framework.CreateNamespaceOrDie(client, "test-service-allocate-node-ports", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-123",
		},
		Spec: v1.ServiceSpec{
			Type:      v1.ServiceTypeClusterIP,
			ClusterIP: "None",
			Selector:  map[string]string{"foo": "bar"},
		},
	}

	service, err = client.CoreV1().Services(ns.Name).Create(tCtx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}

	serviceMergePatch := serviceMergePatch{
		Spec: specMergePatch{
			Type:         v1.ServiceTypeExternalName,
			ExternalName: "foo.bar",
		},
	}
	patchBytes, err := json.Marshal(&serviceMergePatch)
	if err != nil {
		t.Fatalf("failed to json.Marshal ports: %v", err)
	}

	_, err = client.CoreV1().Services(ns.Name).Patch(tCtx, service.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		t.Fatalf("unexpected error patching service using strategic merge patch. %v", err)
	}
}
