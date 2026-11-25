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

package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	imageutils "k8s.io/kubernetes/test/utils/image"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// NodePortRange 必须与默认/配置的端口范围保持一致
var NodePortRange = utilnet.PortRange{Base: 30000, Size: 2768}

// 从 "k8s.io/kubernetes/pkg/registry/core/service/portallocator" 复制而来
var errAllocated = errors.New("provided port is already allocated")

// staticPortRange 实现了此处描述的端口分配模型
// https://github.com/kubernetes/enhancements/tree/master/keps/sig-network/3668-reserved-service-nodeport-range
type staticPortRange struct {
	sync.Mutex
	baseport      int32
	length        int32
	reservedPorts sets.Set[int32]
}

func calculateRange(size int32) int32 {
	var minPort int32 = 16
	var step int32 = 32
	var maxPort int32 = 128
	return min(max(minPort, size/step), maxPort)
}

var staticPortAllocator *staticPortRange

// 每个测试只初始化一次
func init() {
	staticPortAllocator = &staticPortRange{
		baseport:      int32(NodePortRange.Base),
		length:        calculateRange(int32(NodePortRange.Size)),
		reservedPorts: sets.New[int32](),
	}
}

// TestJig 是辅助 service 测试的夹具。
type TestJig struct {
	Client    clientset.Interface
	Namespace string
	Name      string
	ID        string
	Labels    map[string]string
	// Conformance 测试中 ExternalIPs 应为 false
	// Conformance 测试不检查外部地址上的 nodeport，e2e 测试才检查。
	ExternalIPs bool
}

// NewTestJig 分配并初始化一个新的 TestJig。
func NewTestJig(client clientset.Interface, namespace, name string) *TestJig {
	j := &TestJig{}
	j.Client = client
	j.Namespace = namespace
	j.Name = name
	j.ID = j.Name + "-" + string(uuid.NewUUID())
	j.Labels = map[string]string{"testid": j.ID}

	return j
}

// reservePort 预留传入的端口。
// 端口无效或已被预留时返回 false
func (s *staticPortRange) reservePort(port int32) bool {
	s.Lock()
	defer s.Unlock()
	if port < s.baseport || port > s.baseport+s.length || s.reservedPorts.Has(port) {
		return false
	}
	s.reservedPorts.Insert(port)
	return true
}

// getUnusedPort 从范围内返回一个空闲端口及其编号，错误值为 nil。
// 该端口尚未分配，调用方需显式调用 allocatePort() 完成分配。
// 若无可用端口，则返回 -1 和错误。
func (s *staticPortRange) getUnusedPort() (int32, error) {
	s.Lock()
	defer s.Unlock()
	// 从一个随机偏移开始查找
	start := rand.Int31n(s.length)
	for i := int32(0); i < s.length; i++ {
		port := s.baseport + (start+i)%(s.length)
		if !s.reservedPorts.Has(port) {
			return port, nil
		}
	}
	return -1, fmt.Errorf("no free ports were found")
}

// releasePort 释放作为实参传入的端口
func (s *staticPortRange) releasePort(port int32) {
	s.Lock()
	defer s.Unlock()
	s.reservedPorts.Delete(port)
}

// GetUnusedStaticNodePort 返回静态范围内的一个空闲端口，错误值为 nil。
// 若静态范围内无可用端口，则返回 -1 和一个错误值。
// 注意：不能保证返回的端口在 apiserver 上确实可用。
// 必须先分配端口，再尝试创建 Service，然后调用 ReserveStaticNodePort。
func GetUnusedStaticNodePort() (int32, error) {
	return staticPortAllocator.getUnusedPort()
}

// ReserveStaticNodePort 预留传入的端口。可以保证在你调用
// ReleaseStaticNodePort 之前，其他测试不会从 GetUnusedStaticNodePort 拿到该端口。
//
// port 必须先前由 GetUnusedStaticNodePort 分配，并在创建 Service 时
// 成功用作 NodePort 或 HealthCheckNodePort。预留未经 GetUnusedStaticNodePort
// 分配的端口，或在创建对应 Service 之前就预留，都可能导致其他 e2e 测试失败。
//
// 端口无效或已被预留时返回 false
func ReserveStaticNodePort(port int32) bool {
	return staticPortAllocator.reservePort(port)
}

// ReleaseStaticNodePort 释放指定端口。
// 对应的 Service 应已删除，以免端口分配器在 apiserver 认为该端口可用之前就复用它。
// 调用方应按下述方式处理：
//
//	ginkgo.DeferCleanup(func(ctx context.Context) {
//		err := cs.CoreV1().Services(ns).Delete(ctx, serviceName, metav1.DeleteOptions{})
//		if err != nil && !apierrors.IsNotFound(err) {
//			framework.ExpectNoError(err, "failed to delete service %s in namespace %s", serviceName, ns)
//		}
//		e2eservice.ReleaseStaticNodePort(nodePort)
//	})
func ReleaseStaticNodePort(port int32) {
	staticPortAllocator.releasePort(port)
}

// newServiceTemplate 返回此 j 的默认 v1.Service 模板，但并不实际创建 Service。
// 默认 Service 与 j 同名，并暴露指定端口。
func (j *TestJig) newServiceTemplate(proto v1.Protocol, port int32) *v1.Service {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: j.Namespace,
			Name:      j.Name,
			Labels:    j.Labels,
		},
		Spec: v1.ServiceSpec{
			Selector: j.Labels,
			Ports: []v1.ServicePort{
				{
					Protocol: proto,
					Port:     port,
				},
			},
		},
	}
	return service
}

// CreateTCPServiceWithPort 基于 j 的默认配置创建一个使用指定端口的新 TCP Service。
// 调用方可提供一个函数，在创建前调整 Service 对象。
func (j *TestJig) CreateTCPServiceWithPort(ctx context.Context, tweak func(svc *v1.Service), port int32) (*v1.Service, error) {
	svc := j.newServiceTemplate(v1.ProtocolTCP, port)
	if tweak != nil {
		tweak(svc)
	}
	result, err := j.Client.CoreV1().Services(j.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP Service %q: %w", svc.Name, err)
	}
	return j.sanityCheckService(result, svc.Spec.Type)
}

// CreateTCPService 基于 j 的默认配置创建一个新的 TCP Service。
// 调用方可提供一个函数，在创建前调整 Service 对象。
func (j *TestJig) CreateTCPService(ctx context.Context, tweak func(svc *v1.Service)) (*v1.Service, error) {
	return j.CreateTCPServiceWithPort(ctx, tweak, 80)
}

// CreateUDPService 基于 j 的默认配置创建一个新的 UDP Service。
// 调用方可提供一个函数，在创建前调整 Service 对象。
func (j *TestJig) CreateUDPService(ctx context.Context, tweak func(svc *v1.Service)) (*v1.Service, error) {
	svc := j.newServiceTemplate(v1.ProtocolUDP, 80)
	if tweak != nil {
		tweak(svc)
	}
	result, err := j.Client.CoreV1().Services(j.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP Service %q: %w", svc.Name, err)
	}
	return j.sanityCheckService(result, svc.Spec.Type)
}

// CreateExternalNameService 基于 j 的默认配置创建一个新的 ExternalName 类型 Service。
// 调用方可提供一个函数，在创建前调整 Service 对象。
func (j *TestJig) CreateExternalNameService(ctx context.Context, tweak func(svc *v1.Service)) (*v1.Service, error) {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: j.Namespace,
			Name:      j.Name,
			Labels:    j.Labels,
		},
		Spec: v1.ServiceSpec{
			Selector:     j.Labels,
			ExternalName: "foo.example.com",
			Type:         v1.ServiceTypeExternalName,
		},
	}
	if tweak != nil {
		tweak(svc)
	}
	result, err := j.Client.CoreV1().Services(j.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create ExternalName Service %q: %w", svc.Name, err)
	}
	return j.sanityCheckService(result, svc.Spec.Type)
}

// ChangeServiceType 将给定 Service 的 ServiceType 更新为 newType。
func (j *TestJig) ChangeServiceType(ctx context.Context, newType v1.ServiceType, timeout time.Duration) error {
	ingressIP := ""
	svc, err := j.UpdateService(ctx, func(s *v1.Service) {
		for _, ing := range s.Status.LoadBalancer.Ingress {
			if ing.IP != "" {
				ingressIP = ing.IP
			}
		}
		s.Spec.Type = newType
		s.Spec.Ports[0].NodePort = 0
	})
	if err != nil {
		return err
	}
	if ingressIP != "" {
		_, err = j.WaitForLoadBalancerDestroy(ctx, ingressIP, int(svc.Spec.Ports[0].Port), timeout)
	}
	return err
}

// CreateOnlyLocalNodePortService 创建一个 ExternalTrafficPolicy 为 Local 的
// NodePort Service，并对其 nodePort 做基本校验。
// createPod 为 true 时，还会创建一个使用本测试通用 netexec 容器、副本数为 1 的 RC。
func (j *TestJig) CreateOnlyLocalNodePortService(ctx context.Context, createPod bool) (*v1.Service, error) {
	ginkgo.By("creating a service " + j.Namespace + "/" + j.Name + " with type=NodePort and ExternalTrafficPolicy=Local")
	svc, err := j.CreateTCPService(ctx, func(svc *v1.Service) {
		svc.Spec.Type = v1.ServiceTypeNodePort
		svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
		svc.Spec.Ports = []v1.ServicePort{{Protocol: v1.ProtocolTCP, Port: 80}}
	})
	if err != nil {
		return nil, err
	}

	if createPod {
		ginkgo.By("creating a pod to be part of the service " + j.Name)
		_, err = j.Run(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	return svc, nil
}

// CreateOnlyLocalLoadBalancerService 创建一个 ExternalTrafficPolicy 为 Local 的
// LoadBalancer Service，并等待其获得 ingress IP。
// createPod 为 true 时，还会创建一个使用本测试通用 netexec 容器、副本数为 1 的 RC。
func (j *TestJig) CreateOnlyLocalLoadBalancerService(ctx context.Context, timeout time.Duration, createPod bool,
	tweak func(svc *v1.Service)) (*v1.Service, error) {
	_, err := j.CreateLoadBalancerService(ctx, timeout, func(svc *v1.Service) {
		ginkgo.By("setting ExternalTrafficPolicy=Local")
		svc.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
		if tweak != nil {
			tweak(svc)
		}
	})
	if err != nil {
		return nil, err
	}

	if createPod {
		ginkgo.By("creating a pod to be part of the service " + j.Name)
		_, err = j.Run(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	ginkgo.By("waiting for loadbalancer for service " + j.Namespace + "/" + j.Name)
	return j.WaitForLoadBalancer(ctx, timeout)
}

// CreateLoadBalancerService 创建一个 LoadBalancer Service，并等待其获得 ingress IP。
func (j *TestJig) CreateLoadBalancerService(ctx context.Context, timeout time.Duration, tweak func(svc *v1.Service)) (*v1.Service, error) {
	ginkgo.By("creating a service " + j.Namespace + "/" + j.Name + " with type=LoadBalancer")
	svc := j.newServiceTemplate(v1.ProtocolTCP, 80)
	svc.Spec.Type = v1.ServiceTypeLoadBalancer
	// LB 分发测试需要关闭会话亲和性
	svc.Spec.SessionAffinity = v1.ServiceAffinityNone
	if tweak != nil {
		tweak(svc)
	}
	_, err := j.Client.CoreV1().Services(j.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create LoadBalancer Service %q: %w", svc.Name, err)
	}

	ginkgo.By("waiting for loadbalancer for service " + j.Namespace + "/" + j.Name)
	return j.WaitForLoadBalancer(ctx, timeout)
}

// ListNodesWithEndpoint 返回运行给定 Service 的 endpoint 的节点列表。
func (j *TestJig) ListNodesWithEndpoint(ctx context.Context) ([]v1.Node, error) {
	nodeNames, err := j.GetEndpointNodeNames(ctx)
	if err != nil {
		return nil, err
	}
	allNodes, err := j.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	epNodes := make([]v1.Node, 0, nodeNames.Len())
	for _, node := range allNodes.Items {
		if nodeNames.Has(node.Name) {
			epNodes = append(epNodes, node)
		}
	}
	return epNodes, nil
}

// GetEndpointNodeNames 返回运行给定 Service 的 endpoint 的节点名称集合。
func (j *TestJig) GetEndpointNodeNames(ctx context.Context) (sets.String, error) {
	err := j.waitForAvailableEndpoint(ctx, ServiceEndpointsTimeout)
	if err != nil {
		return nil, err
	}
	slices, err := j.Client.DiscoveryV1().EndpointSlices(j.Namespace).List(ctx, metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + j.Name})
	if err != nil {
		return nil, fmt.Errorf("list endpointslices for service %s/%s failed (%w)", j.Namespace, j.Name, err)
	}
	epNodes := sets.NewString()
	for _, slice := range slices.Items {
		for _, ep := range slice.Endpoints {
			if ep.NodeName != nil {
				epNodes.Insert(*ep.NodeName)
			}
		}
	}
	if len(epNodes) == 0 {
		return nil, fmt.Errorf("EndpointSlice has no endpoints, cannot determine node addresses")
	}
	return epNodes, nil
}

// WaitForEndpointOnNode 等待给定节点上出现 service endpoint（该节点必须是 Service 唯一的 endpoint 所在节点）。
func (j *TestJig) WaitForEndpointOnNode(ctx context.Context, nodeName string) error {
	return wait.PollUntilContextTimeout(ctx, framework.Poll, KubeProxyLagTimeout, true, func(ctx context.Context) (bool, error) {
		slices, err := j.Client.DiscoveryV1().EndpointSlices(j.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "kubernetes.io/service-name=" + j.Name})
		if err != nil {
			framework.Logf("List endpointslices for service %s/%s failed (%s)", j.Namespace, j.Name, err)
			return false, nil
		}
		if len(slices.Items) == 0 {
			framework.Logf("Expected 1 EndpointSlice for service %s/%s, got 0", j.Namespace, j.Name)
			return false, nil
		}
		slice := slices.Items[0]
		if len(slice.Endpoints) == 0 {
			framework.Logf("Expected EndpointSlice with Endpoints, got none.")
			return false, nil
		}
		endpoint := slice.Endpoints[0]
		if len(endpoint.Addresses) == 0 || (endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready) {
			framework.Logf("Expected Ready endpoints - found none")
			return false, nil
		}
		epHostName := *endpoint.NodeName
		framework.Logf("Pod for service %s/%s is on node %s", j.Namespace, j.Name, epHostName)
		if epHostName != nodeName {
			framework.Logf("Found endpoint on wrong node, expected %v, got %v", nodeName, epHostName)
			return false, nil
		}
		return true, nil
	})
}

// waitForAvailableEndpoint 等待至少一个 endpoint 可用，直到超时
func (j *TestJig) waitForAvailableEndpoint(ctx context.Context, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, func(ctx context.Context) (bool, error) {
		slices, err := j.Client.DiscoveryV1().EndpointSlices(j.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "kubernetes.io/service-name=" + j.Name})
		if err != nil || len(slices.Items) == 0 {
			// 重试
			return false, nil
		}
		for _, es := range slices.Items {
			if len(es.Endpoints) > 0 && len(es.Endpoints[0].Addresses) > 0 {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("no subset of available IP address found for the endpoint %s within timeout %v", j.Name, timeout)
	}
	return nil
}

// sanityCheckService 对给定 Service 做基本校验，重点确认创建/更新 Service 时
// 按需分配了 IP、端口等。它不检查 ingress 分配，因为那是在 Service 创建后异步完成的。
func (j *TestJig) sanityCheckService(svc *v1.Service, svcType v1.ServiceType) (*v1.Service, error) {
	if svcType == "" {
		svcType = v1.ServiceTypeClusterIP
	}
	if svc.Spec.Type != svcType {
		return nil, fmt.Errorf("unexpected Spec.Type (%s) for service, expected %s", svc.Spec.Type, svcType)
	}

	if svcType != v1.ServiceTypeExternalName {
		if svc.Spec.ExternalName != "" {
			return nil, fmt.Errorf("unexpected Spec.ExternalName (%s) for service, expected empty", svc.Spec.ExternalName)
		}
		if svc.Spec.ClusterIP == "" {
			return nil, fmt.Errorf("didn't get ClusterIP for non-ExternalName service")
		}
	} else {
		if svc.Spec.ClusterIP != "" {
			return nil, fmt.Errorf("unexpected Spec.ClusterIP (%s) for ExternalName service, expected empty", svc.Spec.ClusterIP)
		}
	}

	expectNodePorts := needsNodePorts(svc)
	for i, port := range svc.Spec.Ports {
		hasNodePort := port.NodePort != 0
		if hasNodePort != expectNodePorts {
			return nil, fmt.Errorf("unexpected Spec.Ports[%d].NodePort (%d) for service", i, port.NodePort)
		}
		if hasNodePort {
			if !NodePortRange.Contains(int(port.NodePort)) {
				return nil, fmt.Errorf("out-of-range nodePort (%d) for service", port.NodePort)
			}
		}
	}

	// FIXME: 对于从 LoadBalancer 改为 ClusterIP 的测试，这里会失败。
	// if svcType != v1.ServiceTypeLoadBalancer {
	// 	if len(svc.Status.LoadBalancer.Ingress) != 0 {
	// 		return nil, fmt.Errorf("unexpected Status.LoadBalancer.Ingress on non-LoadBalancer service")
	// 	}
	// }

	return svc, nil
}

func needsNodePorts(svc *v1.Service) bool {
	if svc == nil {
		return false
	}
	// NodePort 类型
	if svc.Spec.Type == v1.ServiceTypeNodePort {
		return true
	}
	if svc.Spec.Type != v1.ServiceTypeLoadBalancer {
		return false
	}
	// LoadBalancer 类型
	if svc.Spec.AllocateLoadBalancerNodePorts == nil {
		return true // 向后兼容
	}
	return *svc.Spec.AllocateLoadBalancerNodePorts
}

// UpdateService 获取 Service，对其调用 update 函数，然后尝试提交更新后的 Service。
// 遇到超时或冲突时最多重试 3 次。
func (j *TestJig) UpdateService(ctx context.Context, update func(*v1.Service)) (*v1.Service, error) {
	for range 3 {
		service, err := j.Client.CoreV1().Services(j.Namespace).Get(ctx, j.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get Service %q: %w", j.Name, err)
		}
		update(service)
		result, err := j.Client.CoreV1().Services(j.Namespace).Update(ctx, service, metav1.UpdateOptions{})
		if err == nil {
			return j.sanityCheckService(result, service.Spec.Type)
		}
		if !apierrors.IsConflict(err) && !apierrors.IsServerTimeout(err) {
			return nil, fmt.Errorf("failed to update Service %q: %w", j.Name, err)
		}
	}
	return nil, fmt.Errorf("too many retries updating Service %q", j.Name)
}

// WaitForNewIngressIP 等待给定 Service 获得新的 ingress IP，超时则返回错误
func (j *TestJig) WaitForNewIngressIP(ctx context.Context, existingIP string, timeout time.Duration) (*v1.Service, error) {
	framework.Logf("Waiting up to %v for service %q to get a new ingress IP", timeout, j.Name)
	service, err := j.waitForCondition(ctx, timeout, "have a new ingress IP", func(svc *v1.Service) bool {
		if len(svc.Status.LoadBalancer.Ingress) == 0 {
			return false
		}
		ip := svc.Status.LoadBalancer.Ingress[0].IP
		if ip == "" || ip == existingIP {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return j.sanityCheckService(service, v1.ServiceTypeLoadBalancer)
}

// ChangeServiceNodePort 修改给定 Service 的 nodePort。
func (j *TestJig) ChangeServiceNodePort(ctx context.Context, initial int) (*v1.Service, error) {
	var err error
	var service *v1.Service
	for i := 1; i < NodePortRange.Size; i++ {
		offs1 := initial - NodePortRange.Base
		offs2 := (offs1 + i) % NodePortRange.Size
		newPort := NodePortRange.Base + offs2
		service, err = j.UpdateService(ctx, func(s *v1.Service) {
			s.Spec.Ports[0].NodePort = int32(newPort)
		})
		if err != nil && strings.Contains(err.Error(), errAllocated.Error()) {
			framework.Logf("tried nodePort %d, but it is in use, will try another", newPort)
			continue
		}
		// 否则 err 为 nil，或者是真正的错误
		break
	}
	return service, err
}

// WaitForLoadBalancer 等待给定 Service 拥有 LoadBalancer，超时则返回错误
func (j *TestJig) WaitForLoadBalancer(ctx context.Context, timeout time.Duration) (*v1.Service, error) {
	framework.Logf("Waiting up to %v for service %q to have a LoadBalancer", timeout, j.Name)
	service, err := j.waitForCondition(ctx, timeout, "have a load balancer", func(svc *v1.Service) bool {
		return len(svc.Status.LoadBalancer.Ingress) > 0
	})
	if err != nil {
		return nil, err
	}

	for i, ing := range service.Status.LoadBalancer.Ingress {
		if ing.IP == "" && ing.Hostname == "" {
			return nil, fmt.Errorf("unexpected Status.LoadBalancer.Ingress[%d] for service: %#v", i, ing)
		}
	}

	return j.sanityCheckService(service, v1.ServiceTypeLoadBalancer)
}

// WaitForLoadBalancerDestroy 等待给定 Service 销毁 LoadBalancer，超时则返回错误
func (j *TestJig) WaitForLoadBalancerDestroy(ctx context.Context, ip string, port int, timeout time.Duration) (*v1.Service, error) {
	// TODO: 工单 21807001 解决后，将此超时恢复为合理值
	defer func() {
		if err := framework.EnsureLoadBalancerResourcesDeleted(ctx, ip, strconv.Itoa(port)); err != nil {
			framework.Logf("Failed to delete cloud resources for service: %s %d (%v)", ip, port, err)
		}
	}()

	framework.Logf("Waiting up to %v for service %q to have no LoadBalancer", timeout, j.Name)
	service, err := j.waitForCondition(ctx, timeout, "have no load balancer", func(svc *v1.Service) bool {
		return len(svc.Status.LoadBalancer.Ingress) == 0
	})
	if err != nil {
		return nil, err
	}
	return j.sanityCheckService(service, service.Spec.Type)
}

func (j *TestJig) waitForCondition(ctx context.Context, timeout time.Duration, message string, conditionFn func(*v1.Service) bool) (*v1.Service, error) {
	var service *v1.Service
	pollFunc := func(ctx context.Context) (bool, error) {
		svc, err := j.Client.CoreV1().Services(j.Namespace).Get(ctx, j.Name, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Retrying .... error trying to get Service %s: %v", j.Name, err)
			return false, nil
		}
		if conditionFn(svc) {
			service = svc
			return true, nil
		}
		return false, nil
	}
	if err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, pollFunc); err != nil {
		return nil, fmt.Errorf("timed out waiting for service %q to %s: %w", j.Name, message, err)
	}
	return service, nil
}

// newDeploymentTemplate 返回此 j 的默认 appsv1.Deployment 对象，但并不实际创建 Deployment。
// 默认 Deployment 与 j 同名，并运行 "netexec" 容器。
func (j *TestJig) newDeploymentTemplate() *appsv1.Deployment {
	var replicas int32 = 1
	var grace int64 = 3 // 避免扩缩容时与 kube-proxy 产生竞态

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: j.Namespace,
			Name:      j.Name,
			Labels:    j.Labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: j.Labels,
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: j.Labels,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "netexec",
							Image: imageutils.GetE2EImage(imageutils.Agnhost),
							Args:  []string{"netexec", "--http-port=80", "--udp-port=80"},
							ReadinessProbe: &v1.Probe{
								PeriodSeconds: 3,
								ProbeHandler: v1.ProbeHandler{
									HTTPGet: &v1.HTTPGetAction{
										Port: intstr.FromInt32(80),
										Path: "/hostName",
									},
								},
							},
						},
					},
					TerminationGracePeriodSeconds: &grace,
				},
			},
		},
	}
	return deployment
}

// AddDeploymentAntiAffinity 为给定 Deployment 添加反亲和性配置。
func (j *TestJig) AddDeploymentAntiAffinity(deployment *appsv1.Deployment) {
	var replicas int32 = 2

	deployment.Spec.Replicas = &replicas
	if deployment.Spec.Template.Spec.Affinity == nil {
		deployment.Spec.Template.Spec.Affinity = &v1.Affinity{}
	}
	if deployment.Spec.Template.Spec.Affinity.PodAntiAffinity == nil {
		deployment.Spec.Template.Spec.Affinity.PodAntiAffinity = &v1.PodAntiAffinity{}
	}
	deployment.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		deployment.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
		v1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: j.Labels},
			Namespaces:    nil,
			TopologyKey:   "kubernetes.io/hostname",
		})
}

// CreatePDB 为给定 Deployment 创建 PodDisruptionBudget，若 PDB 未就绪则返回错误
func (j *TestJig) CreatePDB(ctx context.Context, deployment *appsv1.Deployment) (*policyv1.PodDisruptionBudget, error) {
	pdb := j.newPDBTemplate(deployment)
	newPdb, err := j.Client.PolicyV1().PodDisruptionBudgets(j.Namespace).Create(ctx, pdb, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create PDB %q %v", pdb.Name, err)
	}
	if err := j.waitForPdbReady(ctx); err != nil {
		return nil, fmt.Errorf("failed waiting for PDB to be ready: %w", err)
	}

	return newPdb, nil
}

// newPDBTemplate 返回此 j 的默认 policyv1.PodDisruptionBudget 对象，但并不实际创建 PDB。
// 默认 PDB 的 MinAvailable 为 N-1，并匹配该 Deployment 创建的 Pod。
func (j *TestJig) newPDBTemplate(rc *appsv1.Deployment) *policyv1.PodDisruptionBudget {
	minAvailable := intstr.FromInt32(*rc.Spec.Replicas - 1)

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: j.Namespace,
			Name:      j.Name,
			Labels:    j.Labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: j.Labels},
		},
	}

	return pdb
}

// Run 创建 Deployment 与 Pod，并等待 Pod 进入 Running 状态。
// 调用方可提供一个函数，在创建前调整 Deployment 对象。
func (j *TestJig) Run(ctx context.Context, tweak func(rc *appsv1.Deployment)) (*appsv1.Deployment, error) {
	deployment := j.newDeploymentTemplate()
	if tweak != nil {
		tweak(deployment)
	}

	result, err := j.Client.AppsV1().Deployments(j.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create Deployment %q: %w", deployment.Name, err)
	}

	err = e2edeployment.WaitForDeploymentComplete(j.Client, result)
	if err != nil {
		return nil, fmt.Errorf("failed waiting for Deployment %q: %w", deployment.Name, err)
	}

	return result, nil
}

// Scale 将 Pod 扩缩容到指定副本数
func (j *TestJig) Scale(replicas int) error {
	deployment, err := e2edeployment.UpdateDeploymentWithRetries(j.Client, j.Namespace, j.Name, func(deployment *appsv1.Deployment) {
		deployment.Spec.Replicas = ptr.To(int32(replicas))
	})
	if err != nil {
		return fmt.Errorf("failed to scale Deployment %q: %w", j.Name, err)
	}

	err = e2edeployment.WaitForDeploymentComplete(j.Client, deployment)

	if err != nil {
		return fmt.Errorf("failed waiting for Deployment %q: %w", j.Name, err)
	}

	return nil
}

func (j *TestJig) waitForPdbReady(ctx context.Context) error {
	timeout := 2 * time.Minute
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(2 * time.Second) {
		pdb, err := j.Client.PolicyV1().PodDisruptionBudgets(j.Namespace).Get(ctx, j.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pdb.Status.DisruptionsAllowed > 0 {
			return nil
		}
	}

	return fmt.Errorf("timeout waiting for PDB %q to be ready", j.Name)
}

func testReachabilityOverServiceName(ctx context.Context, serviceName string, sp v1.ServicePort, execPod *v1.Pod) error {
	return testEndpointReachability(ctx, serviceName, sp.Port, sp.Protocol, execPod)
}

func testReachabilityOverClusterIP(ctx context.Context, clusterIP string, sp v1.ServicePort, execPod *v1.Pod) error {
	// Service 的 .spec.clusterIP 为 "" 或 "None" 时不会创建 ClusterIP，因此无法通过 clusterIP:servicePort 测试可达性
	if netutils.ParseIPSloppy(clusterIP) == nil {
		return fmt.Errorf("unable to parse ClusterIP: %s", clusterIP)
	}
	return testEndpointReachability(ctx, clusterIP, sp.Port, sp.Protocol, execPod)
}

func testReachabilityOverExternalIP(ctx context.Context, externalIP string, sp v1.ServicePort, execPod *v1.Pod) error {
	return testEndpointReachability(ctx, externalIP, sp.Port, sp.Protocol, execPod)
}

func testReachabilityOverNodePorts(ctx context.Context, nodes *v1.NodeList, sp v1.ServicePort, pod *v1.Pod, clusterIP string, externalIPs bool) error {
	internalAddrs := e2enode.CollectAddresses(nodes, v1.NodeInternalIP)
	isClusterIPV4 := netutils.IsIPv4String(clusterIP)

	for _, internalAddr := range internalAddrs {
		// 节点内部地址指向 localhost 时，无法通过该地址测试 Service 可达性
		if isInvalidOrLocalhostAddress(internalAddr) {
			framework.Logf("skipping testEndpointReachability() for internal address %s", internalAddr)
			continue
		}
		// 在与 clusterIP 同地址族的节点 internalIP 上检查 Service 可达性
		if isClusterIPV4 != netutils.IsIPv4String(internalAddr) {
			framework.Logf("skipping testEndpointReachability() for internal address %s as it does not match clusterIP (%s) family", internalAddr, clusterIP)
			continue
		}

		err := testEndpointReachability(ctx, internalAddr, sp.NodePort, sp.Protocol, pod)
		if err != nil {
			return err
		}
	}
	if externalIPs {
		externalAddrs := e2enode.CollectAddresses(nodes, v1.NodeExternalIP)
		for _, externalAddr := range externalAddrs {
			if isClusterIPV4 != netutils.IsIPv4String(externalAddr) {
				framework.Logf("skipping testEndpointReachability() for external address %s as it does not match clusterIP (%s) family", externalAddr, clusterIP)
				continue
			}
			err := testEndpointReachability(ctx, externalAddr, sp.NodePort, sp.Protocol, pod)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// isInvalidOrLocalhostAddress 在传入的 `ip` 无法解析或为回环地址时返回 `true`，
// 否则返回 `false`。
func isInvalidOrLocalhostAddress(ip string) bool {
	parsedIP := netutils.ParseIPSloppy(ip)
	if parsedIP == nil || parsedIP.IsLoopback() {
		return true
	}
	return false
}

// testEndpointReachability 测试到 endpoint（即 IP、ServiceName）和端口的可达性，
// 测试请求从指定的 execPod 发起。目前支持基于 TCP 和 UDP 协议的 Service。
// TODO: 增加对基于 SCTP 协议的 Service 的测试支持。
func testEndpointReachability(ctx context.Context, endpoint string, port int32, protocol v1.Protocol, execPod *v1.Pod) error {
	ep := net.JoinHostPort(endpoint, strconv.Itoa(int(port)))
	cmd := ""
	switch protocol {
	case v1.ProtocolTCP:
		cmd = fmt.Sprintf("echo hostName | nc -v -t -w 2 %s %v", endpoint, port)
	case v1.ProtocolUDP:
		cmd = fmt.Sprintf("echo hostName | nc -v -u -w 2 %s %v", endpoint, port)
	default:
		return fmt.Errorf("service reachability check is not supported for %v", protocol)
	}

	err := wait.PollUntilContextTimeout(ctx, 1*time.Second, ServiceReachabilityShortPollTimeout, true, func(ctx context.Context) (bool, error) {
		stdout, err := e2epodoutput.RunHostCmd(execPod.Namespace, execPod.Name, cmd)
		if err != nil {
			framework.Logf("Service reachability failing with error: %v\nRetrying...", err)
			return false, nil
		}
		trimmed := strings.TrimSpace(stdout)
		if trimmed != "" {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("service is not reachable within %v timeout on endpoint %s over %s protocol", ServiceReachabilityShortPollTimeout, ep, protocol)
	}
	return nil
}

// checkClusterIPServiceReachability 确认 ClusterIP 类型的 Service 可通过
// ServiceName:ServicePort 和 ClusterIP:ServicePort 访问
func (j *TestJig) checkClusterIPServiceReachability(ctx context.Context, svc *v1.Service, pod *v1.Pod) error {
	clusterIP := svc.Spec.ClusterIP
	servicePorts := svc.Spec.Ports
	externalIPs := svc.Spec.ExternalIPs

	err := j.waitForAvailableEndpoint(ctx, ServiceEndpointsTimeout)
	if err != nil {
		return err
	}

	for _, servicePort := range servicePorts {
		err = testReachabilityOverServiceName(ctx, svc.Name, servicePort, pod)
		if err != nil {
			return err
		}
		err = testReachabilityOverClusterIP(ctx, clusterIP, servicePort, pod)
		if err != nil {
			return err
		}
		if len(externalIPs) > 0 {
			for _, externalIP := range externalIPs {
				err = testReachabilityOverExternalIP(ctx, externalIP, servicePort, pod)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// checkNodePortServiceReachability 确认 NodePort 类型的 Service 可达：
//   - 内部客户端可通过 ServiceName:ServicePort、ClusterIP:ServicePort 和
//     NodeInternalIPs:NodePort 访问 Service
//   - 外部客户端可通过 NodePublicIPs:NodePort 访问 Service
func (j *TestJig) checkNodePortServiceReachability(ctx context.Context, svc *v1.Service, pod *v1.Pod) error {
	clusterIP := svc.Spec.ClusterIP
	servicePorts := svc.Spec.Ports

	// 测试只考虑 2 个节点
	nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, j.Client, 2)
	if err != nil {
		return err
	}

	err = j.waitForAvailableEndpoint(ctx, ServiceEndpointsTimeout)
	if err != nil {
		return err
	}

	for _, servicePort := range servicePorts {
		err = testReachabilityOverServiceName(ctx, svc.Name, servicePort, pod)
		if err != nil {
			return err
		}
		err = testReachabilityOverClusterIP(ctx, clusterIP, servicePort, pod)
		if err != nil {
			return err
		}
		err = testReachabilityOverNodePorts(ctx, nodes, servicePort, pod, clusterIP, j.ExternalIPs)
		if err != nil {
			return err
		}
	}

	return nil
}

// checkExternalServiceReachability 确认 ExternalName 类型的 Service 能解析为 IP 地址，
// 且没有使用伪造的 externalName。这里使用 kubernetes 的 FQDN 作为 externalName（适配网络隔离平台）。
func (j *TestJig) checkExternalServiceReachability(ctx context.Context, svc *v1.Service, pod *v1.Pod) error {
	// NOTE(claudiub): Windows 不支持 PQDN。
	svcName := fmt.Sprintf("%s.%s.svc.%s", svc.Name, svc.Namespace, framework.TestContext.ClusterDNSDomain)
	// Service 必须解析为 IP
	cmd := fmt.Sprintf("nslookup %s", svcName)
	return wait.PollUntilContextTimeout(ctx, framework.Poll, ServiceReachabilityShortPollTimeout, true, func(ctx context.Context) (done bool, err error) {
		_, stderr, err := e2epodoutput.RunHostCmdWithFullOutput(pod.Namespace, pod.Name, cmd)
		// NOTE(claudiub): Windows 上即使未找到 DNS 名称，nslookup 也可能返回 0。
		// 此时可通过检查 stderr 来发现错误。
		if err != nil || (framework.NodeOSDistroIs("windows") && strings.Contains(stderr, fmt.Sprintf("can't find %s", svcName))) {
			framework.Logf("ExternalName service %q failed to resolve to IP", pod.Namespace+"/"+pod.Name)
			return false, nil
		}
		return true, nil
	})
}

// CheckServiceReachability 确认请求能被 Service 正常处理，仅支持 ClusterIP、NodePort 和 ExternalName 类型的 Service。
func (j *TestJig) CheckServiceReachability(ctx context.Context, svc *v1.Service, pod *v1.Pod) error {
	svcType := svc.Spec.Type

	_, err := j.sanityCheckService(svc, svcType)
	if err != nil {
		return err
	}

	switch svcType {
	case v1.ServiceTypeClusterIP:
		return j.checkClusterIPServiceReachability(ctx, svc, pod)
	case v1.ServiceTypeNodePort:
		return j.checkNodePortServiceReachability(ctx, svc, pod)
	case v1.ServiceTypeExternalName:
		return j.checkExternalServiceReachability(ctx, svc, pod)
	case v1.ServiceTypeLoadBalancer:
		return j.checkClusterIPServiceReachability(ctx, svc, pod)
	default:
		return fmt.Errorf("unsupported service type \"%s\" to verify service reachability for \"%s\" service. This may due to diverse implementation of the service type", svcType, svc.Name)
	}
}

// CreateServicePods 创建一个与 Service 使用相同标签的副本控制器，Service 监听 TCP 和 UDP。
func (j *TestJig) CreateServicePods(ctx context.Context, replica int) error {
	deploymentConfig := e2edeployment.NewDeployment(j.Name,
		int32(replica),
		j.Labels,
		j.Name,
		imageutils.GetE2EImage(imageutils.Agnhost),
		appsv1.RecreateDeploymentStrategyType)
	deploymentConfig.Spec.Template.Spec.Containers[0].Command = []string{"/agnhost", "serve-hostname", "--http=false", "--tcp", "--udp"}

	deployment, err := j.Client.AppsV1().Deployments(j.Namespace).Create(ctx, deploymentConfig, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return e2edeployment.WaitForDeploymentComplete(j.Client, deployment)
}

// CreateSCTPServiceWithPort 基于 j 的默认配置创建一个使用指定端口的新 SCTP Service。
// 调用方可提供一个函数，在创建前调整 Service 对象。
func (j *TestJig) CreateSCTPServiceWithPort(ctx context.Context, tweak func(svc *v1.Service), port int32) (*v1.Service, error) {
	svc := j.newServiceTemplate(v1.ProtocolSCTP, port)
	if tweak != nil {
		tweak(svc)
	}
	result, err := j.Client.CoreV1().Services(j.Namespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create SCTP Service %q: %w", svc.Name, err)
	}
	return j.sanityCheckService(result, svc.Spec.Type)
}

// CreateLoadBalancerServiceWaitForClusterIPOnly 创建一个 LoadBalancer Service，并等待其获得 ClusterIP
func (j *TestJig) CreateLoadBalancerServiceWaitForClusterIPOnly(tweak func(svc *v1.Service)) (*v1.Service, error) {
	ginkgo.By("creating a service " + j.Namespace + "/" + j.Name + " with type=LoadBalancer")
	svc := j.newServiceTemplate(v1.ProtocolTCP, 80)
	svc.Spec.Type = v1.ServiceTypeLoadBalancer
	// LB 分发测试需要关闭会话亲和性
	svc.Spec.SessionAffinity = v1.ServiceAffinityNone
	if tweak != nil {
		tweak(svc)
	}
	result, err := j.Client.CoreV1().Services(j.Namespace).Create(context.TODO(), svc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create LoadBalancer Service %q: %w", svc.Name, err)
	}

	return j.sanityCheckService(result, v1.ServiceTypeLoadBalancer)
}
