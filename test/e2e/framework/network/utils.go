/*
Copyright 2014 The Kubernetes Authors.

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

package network

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	coreclientset "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	// EndpointHTTPPort 是测试用的 endpoint HTTP 端口。
	EndpointHTTPPort = 8083
	// EndpointUDPPort 是测试用的 endpoint UDP 端口。
	EndpointUDPPort = 8081
	// EndpointSCTPPort 是测试用的 endpoint SCTP 端口。
	EndpointSCTPPort = 8082
	// testContainerHTTPPort 是测试容器的 http 端口。
	testContainerHTTPPort = 9080
	// ClusterHTTPPort 是测试用的集群 HTTP 端口。
	ClusterHTTPPort = 80
	// ClusterUDPPort 是测试用的集群 UDP 端口。
	ClusterUDPPort = 90
	// ClusterSCTPPort 是测试用的集群 SCTP 端口。
	ClusterSCTPPort            = 95
	testPodName                = "test-container-pod"
	hostTestPodName            = "host-test-container-pod"
	nodePortServiceName        = "node-port-service"
	sessionAffinityServiceName = "session-affinity-service"
	// 对 Service vip 和/或 nodePort 轮询之间的等待时间，
	// 与 testTries 共同决定总超时时间。
	hitEndpointRetryDelay = 2 * time.Second
	// 命中一组给定 endpoint 的重试次数。次数要多一些，
	// 因为要验证 iptables 的统计 rr 负载均衡。
	testTries = 30
	// 单个测试中 Pod 数的上限，保证测试在大规模集群中也能运行。
	maxNetProxyPodsCount = 10
	// SessionAffinityChecks 是启用会话保持时对一组给定 endpoint 的检查次数。
	SessionAffinityChecks = 10
	// RegexIPv4 用于匹配 IPv4 地址的正则表达式
	RegexIPv4 = "(?:\\d+)\\.(?:\\d+)\\.(?:\\d+)\\.(?:\\d+)"
	// RegexIPv6 用于匹配 IPv6 地址的正则表达式
	RegexIPv6                 = "(?:(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){6})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:::(?:(?:(?:[0-9a-fA-F]{1,4})):){5})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:(?:[0-9a-fA-F]{1,4})):){4})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,1}(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:(?:[0-9a-fA-F]{1,4})):){3})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,2}(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:(?:[0-9a-fA-F]{1,4})):){2})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,3}(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:[0-9a-fA-F]{1,4})):)(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,4}(?:(?:[0-9a-fA-F]{1,4})))?::)(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,5}(?:(?:[0-9a-fA-F]{1,4})))?::)(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,6}(?:(?:[0-9a-fA-F]{1,4})))?::))))"
	resizeNodeReadyTimeout    = 2 * time.Minute
	resizeNodeNotReadyTimeout = 2 * time.Minute
	// netexec dial 命令
	// 目标端会回显自己的 hostname。
	echoHostname = "hostname"
	// 目标端通过 /envvar 端点回显其 Kubernetes 节点名。
	// 用于 hostNetwork Pod：设置 --hostname-override 时 os.Hostname() 不可靠。
	echoNodename = "envvar?var=NODE_NAME"
	// echoNodenameUDP 是 echoNodename 的 UDP 版本。agnhost UDP handler
	// 期望以空格分隔的 "envvar NODE_NAME"；这里把空格做 percent 编码，
	// 让拨号中继处的 url.ParseQuery 能解码回该形式。
	echoNodenameUDP = "envvar%20NODE_NAME"
)

// Option 用于配置 NetworkingTest 对象
type Option func(*NetworkingTestConfig)

// EnableSCTP 在 endpoint 上监听 SCTP 端口
func EnableSCTP(config *NetworkingTestConfig) {
	config.SCTPEnabled = true
}

// EnableDualStack 创建 Dual Stack Service
func EnableDualStack(config *NetworkingTestConfig) {
	config.DualStackEnabled = true
}

// UseHostNetwork 以 HostNetwork=true 运行测试容器。
func UseHostNetwork(config *NetworkingTestConfig) {
	config.HostNetwork = true
}

// EndpointsUseHostNetwork 以 HostNetwork=true 运行 endpoint Pod。
func EndpointsUseHostNetwork(config *NetworkingTestConfig) {
	config.EndpointsHostNetwork = true
}

// PreferExternalAddresses 测试时优先使用节点的 External Address
func PreferExternalAddresses(config *NetworkingTestConfig) {
	config.PreferExternalAddresses = true
}

// NewNetworkingTestConfig 创建并配置一个新的测试配置 helper。
func NewNetworkingTestConfig(ctx context.Context, f *framework.Framework, setters ...Option) *NetworkingTestConfig {
	// 默认选项
	config := &NetworkingTestConfig{
		f:         f,
		Namespace: f.Namespace.Name,
	}
	for _, setter := range setters {
		setter(config)
	}
	ginkgo.By(fmt.Sprintf("Performing setup for networking test in namespace %v", config.Namespace))
	config.setup(ctx, getServiceSelector())
	return config
}

// NewCoreNetworkingTestConfig 为 Node E2E 创建并配置一个新的测试配置 helper。
func NewCoreNetworkingTestConfig(ctx context.Context, f *framework.Framework, hostNetwork bool) *NetworkingTestConfig {
	// 默认选项
	config := &NetworkingTestConfig{
		f:           f,
		Namespace:   f.Namespace.Name,
		HostNetwork: hostNetwork,
	}
	ginkgo.By(fmt.Sprintf("Performing setup for networking test in namespace %v", config.Namespace))
	config.setupCore(ctx, getServiceSelector())
	return config
}

func getServiceSelector() map[string]string {
	ginkgo.By("creating a selector")
	selectorName := "selector-" + string(uuid.NewUUID())
	serviceSelector := map[string]string{
		selectorName: "true",
	}
	return serviceSelector
}

// NetworkingTestConfig 封装了一些便利方法，
// 用于测试 kubeproxy/网络/Service/Endpoint。
type NetworkingTestConfig struct {
	// TestContainerPod 是运行 netexec 镜像的测试 Pod，能够
	// 可对 ip:port 执行 tcp/udp 请求。
	TestContainerPod *v1.Pod
	// HostTestContainerPod 是运行 hostexec 镜像的 Pod。
	HostTestContainerPod *v1.Pod
	// 前提是 HostTestContainerPod 以 HostNetwork=true 运行。
	HostNetwork bool
	// 前提是 endpoint Pod 以 HostNetwork=true 运行。
	EndpointsHostNetwork bool
	// 前提是测试 Pod 监听 sctp 端口。之所以需要此开关，是因为 sctp 测试
	// 会加载 sctp 模块，因此被标记为 disruptive。
	SCTPEnabled bool
	// DualStackEnabled 在 Service 上启用双栈
	DualStackEnabled bool
	// EndpointPods 属于本测试配置创建的 Service。
	// 每次调用 `setup` 都会创建一个 Service，
	// 并在每个节点上运行 1 个使用 netexecImage 的 Pod。
	EndpointPods []*v1.Pod
	f            *framework.Framework
	podClient    *e2epod.PodClient
	// NodePortService 是覆盖所有 endpointPods 的 Type=NodePort Service。
	//
	NodePortService *v1.Service
	// SessionAffinityService 是覆盖所有 endpointPods 的 SessionAffinity=ClientIP Service。
	//
	SessionAffinityService *v1.Service
	// Nodes 是集群中的节点列表。
	Nodes []v1.Node
	// MaxTries 是针对本配置创建的 endpoint 和 Service 测试时
	// 允许的重试次数。
	MaxTries int
	// 本测试配置创建的 Service 的 ClusterIP。
	ClusterIP string
	// 本测试配置创建的 Service 的 SecondaryClusterIP。
	SecondaryClusterIP string
	// 节点有 ExternalIP 则取 ExternalIP，否则取 InternalIP，
	// 用于 nodePort 测试。
	NodeIP string
	// 次 IP 族：节点有 ExternalIP 则取 ExternalIP，否则取 InternalIP，
	// 用于 nodePort 测试。
	SecondaryNodeIP string
	// Service 的 http/udp/sctp nodePort。
	NodeHTTPPort int
	NodeUDPPort  int
	NodeSCTPPort int
	// 本配置中所有资源创建所在的 Kubernetes 命名空间
	//
	Namespace string
	// 测试时是否优先使用节点 External Address
	PreferExternalAddresses bool
}

// NetexecDialResponse 表示 `agnhost` 的 `netexec` 子命令返回的响应。
type NetexecDialResponse struct {
	Responses []string `json:"responses"`
	Errors    []string `json:"errors"`
}

// DialFromEndpointContainer 通过 kubectl exec 在 endpoint 容器中执行 curl。返回错误由调用方处理。
func (config *NetworkingTestConfig) DialFromEndpointContainer(ctx context.Context, protocol, targetIP string, targetPort, maxTries, minTries int, expectedEps sets.String) error {
	dialCmd := echoHostname
	if config.EndpointsHostNetwork {
		if protocol == "udp" || protocol == "sctp" {
			dialCmd = echoNodenameUDP
		} else {
			dialCmd = echoNodename
		}
	}
	return config.DialFromContainer(ctx, protocol, dialCmd, config.EndpointPods[0].Status.PodIP, targetIP, EndpointHTTPPort, targetPort, maxTries, minTries, expectedEps)
}

// DialFromTestContainer 通过 kubectl exec 在测试容器中执行 curl。返回错误由调用方处理。
func (config *NetworkingTestConfig) DialFromTestContainer(ctx context.Context, protocol, targetIP string, targetPort, maxTries, minTries int, expectedEps sets.String) error {
	dialCmd := echoHostname
	if config.EndpointsHostNetwork {
		if protocol == "udp" || protocol == "sctp" {
			dialCmd = echoNodenameUDP
		} else {
			dialCmd = echoNodename
		}
	}
	return config.DialFromContainer(ctx, protocol, dialCmd, config.TestContainerPod.Status.PodIP, targetIP, testContainerHTTPPort, targetPort, maxTries, minTries, expectedEps)
}

// DialEchoFromTestContainer 通过 kubectl exec 在测试容器中执行 curl。响应预期与 echoMessage 匹配。返回错误由调用方处理。
func (config *NetworkingTestConfig) DialEchoFromTestContainer(ctx context.Context, protocol, targetIP string, targetPort, maxTries, minTries int, echoMessage string) error {
	expectedResponse := sets.NewString()
	expectedResponse.Insert(echoMessage)
	var dialCommand string

	// NOTE(claudiub): netexec /dialCommand 会按如下方式向 targetIP 和 targetPort 发请求：
	// HTTP：请求 http://targetIP:targetPort/dialCommand
	// UDP：把 targetCommand 作为消息发送。接收方收到数据后查找
	// 包括 echo 在内的若干起始字符串，并做相应处理。
	if protocol == "http" {
		dialCommand = fmt.Sprintf("echo?msg=%s", echoMessage)
	} else {
		dialCommand = fmt.Sprintf("echo%%20%s", echoMessage)
	}
	return config.DialFromContainer(ctx, protocol, dialCommand, config.TestContainerPod.Status.PodIP, targetIP, testContainerHTTPPort, targetPort, maxTries, minTries, expectedResponse)
}

// diagnoseMissingEndpoints 打印那些不在 foundEndpoints 列表中的 endpoint 的调试信息。
// 这些正是我们预期会响应、却没有响应的 endpoint。
func (config *NetworkingTestConfig) diagnoseMissingEndpoints(foundEndpoints sets.String) {
	for _, e := range config.EndpointPods {
		if foundEndpoints.Has(e.Name) {
			continue
		}
		framework.Logf("\nOutput of kubectl describe pod %v/%v:\n", e.Namespace, e.Name)
		desc, _ := e2ekubectl.RunKubectl(
			e.Namespace, "describe", "pod", e.Name, fmt.Sprintf("--namespace=%v", e.Namespace))
		framework.Logf("%s", desc)
	}
}

// EndpointHostnames 返回现有 endpoint 的 hostname 集合。
func (config *NetworkingTestConfig) EndpointHostnames() sets.String {
	expectedEps := sets.NewString()
	for _, p := range config.EndpointPods {

		if config.EndpointsHostNetwork {
			// 对 hostNetwork Pod，spec.nodeName 是唯一可靠的节点标识。
			// kubelet 设置 --hostname-override 时（例如 DigitalOcean、kops），
			// os.Hostname() 会与 k8s 节点名不一致。这里通过 Downward API
			// 支撑的 /envvar?var=NODE_NAME 直接读取 spec.nodeName。
			// xref: https://issues.k8s.io/121018
			expectedEps.Insert(p.Spec.NodeName)
		} else {
			expectedEps.Insert(p.Name)
		}
	}
	return expectedEps
}

func makeCURLDialCommand(ipPort, dialCmd, protocol, targetIP string, targetPort int) string {
	// CentOS 和 RHEL 发行版自带的 curl 版本会
	// 把 IPv6 周围的方括号误当作 glob 模式，因此用 -g
	// 参数关闭 glob 以正确处理 IPv6。
	return fmt.Sprintf("curl -g -q -s 'http://%s/dial?request=%s&protocol=%s&host=%s&port=%d&tries=1'",
		ipPort,
		dialCmd,
		protocol,
		targetIP,
		targetPort)
}

// DialFromContainer 通过 kubectl exec 在测试容器中执行 curl，
// 随后根据 url 中的 protocol 参数转换为 tcp 或 udp 请求。
//
//   - minTries 是判定成功前所需的最少 curl 次数。
//     设为 0 表示只要所有 endpoint 至少响应一次就立即返回。
//     至少响应一次。
//
//   - maxTries 是 curl 次数的上限。达到该次数仍未见所有预期 endpoint，测试失败。
//
//   - targetIP 是发起拨号的源 Pod IP，使用给定协议发送 dialCommand。
//
//   - dialCommand 是 targetIP 用给定协议发给目标的命令。
//     dialCommand 应按协议正确格式化（http：URL 路径+参数，
//     udp：command%20parameters，其中参数可选）。
//
//   - expectedResponses 是等待响应的无序集合，取决于 dialCommand；
//     例如 dialCommand 为 "hostname" 时，expectedResponses 应包含
//     Service 中各 Pod 通过 /hostName 上报的 hostname。
//
// maxTries == minTries 会确认在 maxTries 次内看到预期 endpoint 且不多不少。
// 适用于例如让 Pod 的 readiness 检查失败，并确认它没有出现在 endpoint 中的场景。
//
// 成功返回 nil，尝试 maxTries 次后仍失败则返回错误信息。
func (config *NetworkingTestConfig) DialFromContainer(ctx context.Context, protocol, dialCommand, containerIP, targetIP string, containerHTTPPort, targetPort, maxTries, minTries int, expectedResponses sets.String) error {
	ipPort := net.JoinHostPort(containerIP, strconv.Itoa(containerHTTPPort))

	cmd := makeCURLDialCommand(ipPort, dialCommand, protocol, targetIP, targetPort)

	responses := sets.NewString()

	for i := range maxTries {
		resp, err := config.GetResponseFromContainer(ctx, protocol, dialCommand, containerIP, targetIP, containerHTTPPort, targetPort)
		if err != nil {
			// kubectl exec 失败也算一次尝试，不算硬失败。
			// 另外注意，在验证不可达的测试中，我们会持续失败直到 maxTries。
			//
			framework.Logf("GetResponseFromContainer: %s", err)
			continue
		}
		for _, response := range resp.Responses {
			trimmed := strings.TrimSpace(response)
			if trimmed != "" {
				responses.Insert(trimmed)
			}
		}
		if responses.Difference(expectedResponses).Len() > 0 {
			returnMsg := fmt.Errorf("received unexpected responses... \nAttempt %d\nCommand %v\nretrieved %v\nexpected %v", i, cmd, responses, expectedResponses)
			framework.Logf("encountered error during dial (%v)", returnMsg)
			return returnMsg
		}

		framework.Logf("Waiting for responses: %v", expectedResponses.Difference(responses))

		// 与 i+1 比较，以便在 minTries == maxTries 时及时退出。
		if (responses.Equal(expectedResponses) || responses.Len() == 0 && expectedResponses.Len() == 0) && i+1 >= minTries {
			framework.Logf("reached %v after %v/%v tries", targetIP, i, maxTries)
			return nil
		}
		// TODO: 去掉这个延迟 #36281
		time.Sleep(hitEndpointRetryDelay)
	}
	if dialCommand == echoHostname || dialCommand == echoNodename || dialCommand == echoNodenameUDP {
		config.diagnoseMissingEndpoints(responses)
	}
	returnMsg := fmt.Errorf("did not find expected responses... \nTries %d\nCommand %v\nretrieved %v\nexpected %v", maxTries, cmd, responses, expectedResponses)
	framework.Logf("encountered error during dial (%v)", returnMsg)
	return returnMsg

}

// GetEndpointsFromTestContainer 通过 kubectl exec 在测试容器中执行 curl。
func (config *NetworkingTestConfig) GetEndpointsFromTestContainer(ctx context.Context, protocol, targetIP string, targetPort, tries int) (sets.String, error) {
	return config.GetEndpointsFromContainer(ctx, protocol, config.TestContainerPod.Status.PodIP, targetIP, testContainerHTTPPort, targetPort, tries)
}

// GetEndpointsFromContainer 通过 kubectl exec 在测试容器中执行 curl，
// 随后根据 url 中的 protocol 参数转换为 tcp 或 udp 请求。
// 返回多次重试中出现的所有不同 endpoint。
//   - tries 是 curl 尝试次数。达到该次数仍看不到任何 endpoint，测试失败。
func (config *NetworkingTestConfig) GetEndpointsFromContainer(ctx context.Context, protocol, containerIP, targetIP string, containerHTTPPort, targetPort, tries int) (sets.String, error) {
	ipPort := net.JoinHostPort(containerIP, strconv.Itoa(containerHTTPPort))
	cmd := makeCURLDialCommand(ipPort, "hostName", protocol, targetIP, targetPort)

	eps := sets.NewString()

	for i := range tries {
		stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, config.f, config.TestContainerPod.Name, cmd)
		if err != nil {
			// kubectl exec 失败也算一次尝试，不算硬失败。
			// 另外注意，在验证不可达的测试中，我们会持续失败直到 maxTries。
			//
			framework.Logf("Failed to execute %q: %v, stdout: %q, stderr: %q", cmd, err, stdout, stderr)
		} else {
			podInfo := fmt.Sprintf("name: %v, namespace: %v, hostIp: %v, podIp: %v, conditions: %v", config.TestContainerPod.Name, config.TestContainerPod.Namespace, config.TestContainerPod.Status.HostIP, config.TestContainerPod.Status.PodIP, config.TestContainerPod.Status.Conditions)
			framework.Logf("Tries: %d, in try: %d, stdout: %v, stderr: %v, command run in Pod { %#v }", tries, i, stdout, stderr, podInfo)

			var output NetexecDialResponse
			if err := json.Unmarshal([]byte(stdout), &output); err != nil {
				framework.Logf("WARNING: Failed to unmarshal curl response. Cmd %v run in %v, output: %s, err: %v",
					cmd, config.TestContainerPod.Name, stdout, err)
				continue
			}

			for _, hostName := range output.Responses {
				trimmed := strings.TrimSpace(hostName)
				if trimmed != "" {
					eps.Insert(trimmed)
				}
			}
			// TODO: 去掉这个延迟 #36281
			time.Sleep(hitEndpointRetryDelay)
		}
	}
	return eps, nil
}

// GetResponseFromContainer 通过 kubectl exec 在容器中执行 curl。
func (config *NetworkingTestConfig) GetResponseFromContainer(ctx context.Context, protocol, dialCommand, containerIP, targetIP string, containerHTTPPort, targetPort int) (NetexecDialResponse, error) {
	ipPort := net.JoinHostPort(containerIP, strconv.Itoa(containerHTTPPort))
	cmd := makeCURLDialCommand(ipPort, dialCommand, protocol, targetIP, targetPort)

	stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, config.f, config.TestContainerPod.Name, cmd)
	if err != nil {
		return NetexecDialResponse{}, fmt.Errorf("failed to execute %q: %v, stdout: %q, stderr: %q", cmd, err, stdout, stderr)
	}

	var output NetexecDialResponse
	if err := json.Unmarshal([]byte(stdout), &output); err != nil {
		return NetexecDialResponse{}, fmt.Errorf("failed to unmarshal curl response. Cmd %v run in %v, output: %s, err: %v",
			cmd, config.TestContainerPod.Name, stdout, err)
	}
	return output, nil
}

// GetResponseFromTestContainer 通过 kubectl exec 在测试容器中执行 curl。
func (config *NetworkingTestConfig) GetResponseFromTestContainer(ctx context.Context, protocol, dialCommand, targetIP string, targetPort int) (NetexecDialResponse, error) {
	return config.GetResponseFromContainer(ctx, protocol, dialCommand, config.TestContainerPod.Status.PodIP, targetIP, testContainerHTTPPort, targetPort)
}

// GetHTTPCodeFromTestContainer 通过 kubectl exec 在测试容器中执行 curl 并返回状态码。
func (config *NetworkingTestConfig) GetHTTPCodeFromTestContainer(ctx context.Context, path, targetIP string, targetPort int) (int, error) {
	cmd := fmt.Sprintf("curl -g -q -s -o /dev/null -w %%{http_code} http://%s%s",
		net.JoinHostPort(targetIP, strconv.Itoa(targetPort)),
		path)
	stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, config.f, config.TestContainerPod.Name, cmd)
	// 我们只关心 curl 上报的状态码，
	// 其他错误（例如无法在 Pod 中执行命令）都应返回。
	// 若 curl 连接主机失败，会以 code 7 退出，使 `ExecShellInPodWithFullOutput`
	// 返回非 nil 错误，并向 stdout 输出 "000"。
	if err != nil && len(stdout) == 0 {
		return 0, fmt.Errorf("failed to execute %q: %v, stderr: %q", cmd, err, stderr)
	}
	code, err := strconv.Atoi(stdout)
	if err != nil {
		return 0, fmt.Errorf("failed to parse status code returned by healthz endpoint: %w, code: %s", err, stdout)
	}
	return code, nil
}

// DialFromNode 在运行 host networking 的测试容器中，通过 kubectl exec
// 按协议执行 tcp/udp curl/nc 请求。
//   - minTries 是判定成功前所需的最少 curl/nc 次数。
//     设为 0 表示只要所有 endpoint 成功就立即返回。
//   - 如果失败发生在 endpoint 已经成功之后，测试结果不会变化，
//     因此过度增大 minTries 不会逆转已成功的结果，目前意义不大
//     （参见尚未实现的关于检查概率的 TODO）。
//   - maxTries 是返回错误前的最大 curl/echo 次数。
//     该值越小，判定成功的 '余量' 越少。
//   - 如果 maxTries < expectedEps，测试必然返回错误，因为无法命中所有 endpoint。
//   - maxTries == minTries 时，所有 endpoint 成功即返回（或达到 maxTries 仍
//     未全部成功则返回错误）。
//     一般应让 minTries 足够大，保证所有 Pod 都有公平机会收到流量。
func (config *NetworkingTestConfig) DialFromNode(ctx context.Context, protocol, targetIP string, targetPort, maxTries, minTries int, expectedEps sets.String) error {
	var cmd string
	if protocol == "udp" {
		if config.EndpointsHostNetwork {
			// UDP handler 期望以空格分隔的 "envvar NODE_NAME"。
			cmd = fmt.Sprintf("echo 'envvar NODE_NAME' | nc -w 1 -u %s %d", targetIP, targetPort)
		} else {
			cmd = fmt.Sprintf("echo hostName | nc -w 1 -u %s %d", targetIP, targetPort)
		}
	} else {
		ipPort := net.JoinHostPort(targetIP, strconv.Itoa(targetPort))
		// CentOS 和 RHEL 发行版自带的 curl 版本会
		// 把 IPv6 周围的方括号误当作 glob 模式，因此用 -g
		// 参数关闭 glob 以正确处理 IPv6。
		if config.EndpointsHostNetwork {
			cmd = fmt.Sprintf("curl -g -q -s --max-time 15 --connect-timeout 1 'http://%s/envvar?var=NODE_NAME'", ipPort)
		} else {
			cmd = fmt.Sprintf("curl -g -q -s --max-time 15 --connect-timeout 1 http://%s/hostName", ipPort)
		}
	}

	// TODO: 这里只能说明我们能到达 endpoint。还应检查
	// 命中某个特定 endpoint 的概率是否与其他 endpoint 大致相同。
	// 命中某个特定 endpoint 的概率与其他 endpoint 大致相同。
	eps := sets.NewString()

	filterCmd := fmt.Sprintf("%s | grep -v '^\\s*$'", cmd)
	framework.Logf("Going to poll %v on port %v at least %v times, with a maximum of %v tries before failing", targetIP, targetPort, minTries, maxTries)
	for i := range maxTries {
		stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, config.f, config.HostTestContainerPod.Name, filterCmd)
		if err != nil || len(stderr) > 0 {
			// exec 命令失败也算一次尝试，不算硬失败。
			// 另外注意，在验证不可达的测试中，我们会持续失败直到 maxTries。
			//
			framework.Logf("Failed to execute %q: %v, stdout: %q, stderr: %q", filterCmd, err, stdout, stderr)
		} else {
			trimmed := strings.TrimSpace(stdout)
			if trimmed != "" {
				eps.Insert(trimmed)
			}
		}

		// 与 i+1 比较，以便在 minTries == maxTries 时及时退出。
		if eps.Equal(expectedEps) && i+1 >= minTries {
			framework.Logf("Found all %d expected endpoints: %+v", eps.Len(), eps.List())
			return nil
		}

		framework.Logf("Waiting for %+v endpoints (expected=%+v, actual=%+v)", expectedEps.Difference(eps).List(), expectedEps.List(), eps.List())

		// TODO: 去掉这个延迟 #36281
		time.Sleep(hitEndpointRetryDelay)
	}

	config.diagnoseMissingEndpoints(eps)
	return fmt.Errorf("failed to find expected endpoints, \ntries %d\nCommand %v\nretrieved %v\nexpected %v", maxTries, cmd, eps, expectedEps)
}

// GetSelfURL 在运行 host networking 的测试容器中通过 kubectl exec 对给定路径执行 curl，
// 输出与预期字符串不符则失败。
func (config *NetworkingTestConfig) GetSelfURL(ctx context.Context, port int32, path string, expected string) {
	cmd := fmt.Sprintf("curl -i -q -s --connect-timeout 1 http://localhost:%d%s", port, path)
	ginkgo.By(fmt.Sprintf("Getting kube-proxy self URL %s", path))
	config.executeCurlCmd(ctx, cmd, expected)
}

// GetSelfURLStatusCode 在运行 host networking 的测试容器中通过 kubectl exec 对给定路径执行 curl，
// 返回的状态码与预期字符串不符则失败。
func (config *NetworkingTestConfig) GetSelfURLStatusCode(ctx context.Context, port int32, path string, expected string) {
	// 检查状态码
	cmd := fmt.Sprintf("curl -o /dev/null -i -q -s -w %%{http_code} --connect-timeout 1 http://localhost:%d%s", port, path)
	ginkgo.By(fmt.Sprintf("Checking status code against http://localhost:%d%s", port, path))
	config.executeCurlCmd(ctx, cmd, expected)
}

func (config *NetworkingTestConfig) executeCurlCmd(ctx context.Context, cmd string, expected string) {
	// 这些超时值是随意设定的。curl 命令首次尝试就应通过，
	// 除非远端服务器正被饿死/启动中/重启等。
	const retryInterval = 1 * time.Second
	const retryTimeout = 30 * time.Second
	podName := config.HostTestContainerPod.Name
	var msg string
	if pollErr := wait.PollUntilContextTimeout(ctx, retryInterval, retryTimeout, true, func(ctx context.Context) (bool, error) {
		stdout, err := e2epodoutput.RunHostCmd(config.Namespace, podName, cmd)
		if err != nil {
			msg = fmt.Sprintf("failed executing cmd %v in %v/%v: %v", cmd, config.Namespace, podName, err)
			framework.Logf("%s", msg)
			return false, nil
		}
		if !strings.Contains(stdout, expected) {
			msg = fmt.Sprintf("successfully executed %v in %v/%v, but output '%v' doesn't contain expected string '%v'", cmd, config.Namespace, podName, stdout, expected)
			framework.Logf("%s", msg)
			return false, nil
		}
		return true, nil
	}); pollErr != nil {
		framework.Logf("\nOutput of kubectl describe pod %v/%v:\n", config.Namespace, podName)
		desc, _ := e2ekubectl.RunKubectl(
			config.Namespace, "describe", "pod", podName, fmt.Sprintf("--namespace=%v", config.Namespace))
		framework.Logf("%s", desc)
		framework.Failf("Timed out in %v: %v", retryTimeout, msg)
	}
}

func (config *NetworkingTestConfig) createNetShellPodSpec(podName, hostname string) *v1.Pod {
	netexecArgs := []string{
		"netexec",
		fmt.Sprintf("--http-port=%d", EndpointHTTPPort),
		fmt.Sprintf("--udp-port=%d", EndpointUDPPort),
	}
	// hostNetwork endpoint 场景下，我们希望把 udp listener 绑定到特定 IP 地址。
	// 为同时覆盖旧集群和双栈集群，传入 host IP 和两个 Pod IP。Agnhost
	// 会去重，因此会同时监听这些地址（或仅监听现存的那一个）。
	if config.EndpointsHostNetwork {
		netexecArgs = append(netexecArgs, "--udp-listen-addresses=$(HOST_IP),$(POD_IPS)")
	}

	probe := &v1.Probe{
		InitialDelaySeconds: 10,
		TimeoutSeconds:      30,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		FailureThreshold:    3,
		ProbeHandler: v1.ProbeHandler{
			HTTPGet: &v1.HTTPGetAction{
				Path: "/healthz",
				Port: intstr.IntOrString{IntVal: EndpointHTTPPort},
			},
		},
	}
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: config.Namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "webserver",
					Image:           imageutils.GetE2EImage(imageutils.Agnhost),
					ImagePullPolicy: v1.PullIfNotPresent,
					Args:            netexecArgs,
					Ports: []v1.ContainerPort{
						{
							Name:          "http",
							ContainerPort: EndpointHTTPPort,
						},
						{
							Name:          "udp",
							ContainerPort: EndpointUDPPort,
							Protocol:      v1.ProtocolUDP,
						},
					},
					LivenessProbe:  probe,
					ReadinessProbe: probe,
				},
			},
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": hostname,
			},
		},
	}
	// 我们希望 sctp 是可选的，因为它会加载 sctp 内核模块
	if config.SCTPEnabled {
		pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, fmt.Sprintf("--sctp-port=%d", EndpointSCTPPort))
		pod.Spec.Containers[0].Ports = append(pod.Spec.Containers[0].Ports, v1.ContainerPort{
			Name:          "sctp",
			ContainerPort: EndpointSCTPPort,
			Protocol:      v1.ProtocolSCTP,
		})
	}

	if config.EndpointsHostNetwork {
		pod.Spec.Containers[0].Env = []v1.EnvVar{
			{
				Name: "HOST_IP",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "status.hostIP",
					},
				},
			},
			{
				Name: "POD_IPS",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "status.podIPs",
					},
				},
			},
			{
				Name: "NODE_NAME",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
		}
	}
	return pod
}

func (config *NetworkingTestConfig) createTestPodSpec() *v1.Pod {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPodName,
			Namespace: config.Namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "webserver",
					Image:           imageutils.GetE2EImage(imageutils.Agnhost),
					ImagePullPolicy: v1.PullIfNotPresent,
					Args: []string{
						"netexec",
						fmt.Sprintf("--http-port=%d", testContainerHTTPPort),
					},
					Ports: []v1.ContainerPort{
						{
							Name:          "http",
							ContainerPort: testContainerHTTPPort,
						},
					},
				},
			},
		},
	}
	return pod
}

func (config *NetworkingTestConfig) createNodePortServiceSpec(svcName string, selector map[string]string, enableSessionAffinity bool) *v1.Service {
	sessionAffinity := v1.ServiceAffinityNone
	if enableSessionAffinity {
		sessionAffinity = v1.ServiceAffinityClientIP
	}
	res := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: svcName,
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeNodePort,
			Ports: []v1.ServicePort{
				{Port: ClusterHTTPPort, Name: "http", Protocol: v1.ProtocolTCP, TargetPort: intstr.FromInt32(EndpointHTTPPort)},
				{Port: ClusterUDPPort, Name: "udp", Protocol: v1.ProtocolUDP, TargetPort: intstr.FromInt32(EndpointUDPPort)},
			},
			Selector:        selector,
			SessionAffinity: sessionAffinity,
		},
	}

	if config.SCTPEnabled {
		res.Spec.Ports = append(res.Spec.Ports, v1.ServicePort{Port: ClusterSCTPPort, Name: "sctp", Protocol: v1.ProtocolSCTP, TargetPort: intstr.FromInt32(EndpointSCTPPort)})
	}
	if config.DualStackEnabled {
		requireDual := v1.IPFamilyPolicyRequireDualStack
		res.Spec.IPFamilyPolicy = &requireDual
	}
	return res
}

func (config *NetworkingTestConfig) createNodePortService(ctx context.Context, selector map[string]string) {
	config.NodePortService = config.CreateService(ctx, config.createNodePortServiceSpec(nodePortServiceName, selector, false))
}

func (config *NetworkingTestConfig) createSessionAffinityService(ctx context.Context, selector map[string]string) {
	config.SessionAffinityService = config.CreateService(ctx, config.createNodePortServiceSpec(sessionAffinityServiceName, selector, true))
}

// DeleteNodePortService 删除 NodePort Service。
func (config *NetworkingTestConfig) DeleteNodePortService(ctx context.Context) {
	err := config.getServiceClient().Delete(ctx, config.NodePortService.Name, metav1.DeleteOptions{})
	framework.ExpectNoError(err, "error while deleting NodePortService. err:%v)", err)
	time.Sleep(15 * time.Second) // 等待 kube-proxy 跟上 Service 被删除的变化。
}

func (config *NetworkingTestConfig) createTestPods(ctx context.Context) {
	testContainerPod := config.createTestPodSpec()
	hostTestContainerPod := e2epod.NewExecPodSpec(config.Namespace, hostTestPodName, config.HostNetwork)

	config.createPod(ctx, testContainerPod)
	if config.HostNetwork {
		config.createPod(ctx, hostTestContainerPod)
	}

	framework.ExpectNoError(e2epod.WaitForPodNameRunningInNamespace(ctx, config.f.ClientSet, testContainerPod.Name, config.f.Namespace.Name))

	var err error
	config.TestContainerPod, err = config.getPodClient().Get(ctx, testContainerPod.Name, metav1.GetOptions{})
	if err != nil {
		framework.Failf("Failed to retrieve %s pod: %v", testContainerPod.Name, err)
	}

	if config.HostNetwork {
		framework.ExpectNoError(e2epod.WaitForPodNameRunningInNamespace(ctx, config.f.ClientSet, hostTestContainerPod.Name, config.f.Namespace.Name))
		config.HostTestContainerPod, err = config.getPodClient().Get(ctx, hostTestContainerPod.Name, metav1.GetOptions{})
		if err != nil {
			framework.Failf("Failed to retrieve %s pod: %v", hostTestContainerPod.Name, err)
		}
	}
}

// CreateService 在 config.Namespace 中创建给定的 Service 并返回创建结果。
func (config *NetworkingTestConfig) CreateService(ctx context.Context, serviceSpec *v1.Service) *v1.Service {
	_, err := config.getServiceClient().Create(ctx, serviceSpec, metav1.CreateOptions{})
	framework.ExpectNoError(err, fmt.Sprintf("Failed to create %s service: %v", serviceSpec.Name, err))

	err = WaitForService(ctx, config.f.ClientSet, config.Namespace, serviceSpec.Name, true, 5*time.Second, 45*time.Second)
	framework.ExpectNoError(err, fmt.Sprintf("error while waiting for service:%s err: %v", serviceSpec.Name, err))

	createdService, err := config.getServiceClient().Get(ctx, serviceSpec.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, fmt.Sprintf("Failed to create %s service: %v", serviceSpec.Name, err))

	return createdService
}

// setupCore 配置 Pod 和核心测试配置，
// 主要用于简化 node e2e 的设置。
func (config *NetworkingTestConfig) setupCore(ctx context.Context, selector map[string]string) {
	ginkgo.By("Creating the service pods in kubernetes")
	podName := "netserver"
	config.EndpointPods = config.createNetProxyPods(ctx, podName, selector)

	ginkgo.By("Creating test pods")
	config.createTestPods(ctx)

	epCount := len(config.EndpointPods)

	// 注意实践中这不是 O(n^2)，因为 epCount 应小于 10。epCount > 10 时规模会大得不可接受。
	// 详见 maxNetProxyPodsCount。
	config.MaxTries = epCount*epCount + testTries
	framework.Logf("Setting MaxTries for pod polling to %v for networking test based on endpoint count %v", config.MaxTries, epCount)
}

// setup 包含 setupCore，并额外配置 Service
func (config *NetworkingTestConfig) setup(ctx context.Context, selector map[string]string) {
	config.setupCore(ctx, selector)

	ginkgo.By("Getting node addresses")
	framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, config.f.ClientSet, 10*time.Minute))
	nodeList, err := e2enode.GetReadySchedulableNodes(ctx, config.f.ClientSet)
	framework.ExpectNoError(err)

	e2eskipper.SkipUnlessNodeCountIsAtLeast(2)
	config.Nodes = nodeList.Items

	ginkgo.By("Creating the service on top of the pods in kubernetes")
	config.createNodePortService(ctx, selector)
	config.createSessionAffinityService(ctx, selector)

	for _, p := range config.NodePortService.Spec.Ports {
		switch p.Protocol {
		case v1.ProtocolUDP:
			config.NodeUDPPort = int(p.NodePort)
		case v1.ProtocolTCP:
			config.NodeHTTPPort = int(p.NodePort)
		case v1.ProtocolSCTP:
			config.NodeSCTPPort = int(p.NodePort)
		default:
			continue
		}
	}

	// 获取 ClusterIP
	config.ClusterIP = config.NodePortService.Spec.ClusterIP
	if config.DualStackEnabled {
		config.SecondaryClusterIP = config.NodePortService.Spec.ClusterIPs[1]
	}

	var family, secondaryFamily v1.IPFamily
	family = config.NodePortService.Spec.IPFamilies[0]
	if len(config.NodePortService.Spec.IPFamilies) == 2 {
		secondaryFamily = config.NodePortService.Spec.IPFamilies[1]
	}
	if config.PreferExternalAddresses {
		// 从集群获取节点 IP，优先使用 ExternalIP
		config.NodeIP = e2enode.FirstAddressByTypeAndFamily(nodeList, v1.NodeExternalIP, family)
	}
	if config.NodeIP == "" {
		config.NodeIP = e2enode.FirstAddressByTypeAndFamily(nodeList, v1.NodeInternalIP, family)
	}
	if config.DualStackEnabled {
		if config.PreferExternalAddresses {
			config.SecondaryNodeIP = e2enode.FirstAddressByTypeAndFamily(nodeList, v1.NodeExternalIP, secondaryFamily)
		}
		if config.SecondaryNodeIP == "" {
			config.SecondaryNodeIP = e2enode.FirstAddressByTypeAndFamily(nodeList, v1.NodeInternalIP, secondaryFamily)
		}
	}

	ginkgo.By("Waiting for NodePort service to expose endpoint")
	numEndpoints := len(config.EndpointPods)
	if config.DualStackEnabled {
		numEndpoints = 2 * len(config.EndpointPods)
	}
	err = e2eendpointslice.WaitForEndpointCount(ctx, config.f.ClientSet, config.Namespace, nodePortServiceName, numEndpoints)
	framework.ExpectNoError(err, "failed to validate endpoints for service %s in namespace: %s", nodePortServiceName, config.Namespace)
	ginkgo.By("Waiting for Session Affinity service to expose endpoint")
	err = e2eendpointslice.WaitForEndpointCount(ctx, config.f.ClientSet, config.Namespace, sessionAffinityServiceName, numEndpoints)
	framework.ExpectNoError(err, "failed to validate endpoints for service %s in namespace: %s", sessionAffinityServiceName, config.Namespace)
}

func (config *NetworkingTestConfig) createNetProxyPods(ctx context.Context, podName string, selector map[string]string) []*v1.Pod {
	framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, config.f.ClientSet, 10*time.Minute))
	nodeList, err := e2enode.GetBoundedReadySchedulableNodes(ctx, config.f.ClientSet, maxNetProxyPodsCount)
	framework.ExpectNoError(err)
	nodes := nodeList.Items

	// 创建 Pod，每个节点一个
	createdPods := make([]*v1.Pod, 0, len(nodes))
	for i, n := range nodes {
		podName := fmt.Sprintf("%s-%d", podName, i)
		hostname, _ := n.Labels["kubernetes.io/hostname"]
		pod := config.createNetShellPodSpec(podName, hostname)
		pod.ObjectMeta.Labels = selector
		pod.Spec.HostNetwork = config.EndpointsHostNetwork

		// NOTE(claudiub): 在 Windows 上使用 HostNetwork 需要使用 Privileged Container。
		if pod.Spec.HostNetwork && framework.NodeOSDistroIs("windows") {
			e2epod.WithWindowsHostProcess(pod, "")
		}
		createdPod := config.createPod(ctx, pod)
		createdPods = append(createdPods, createdPod)
	}

	// 等待所有 Pod 启动
	runningPods := make([]*v1.Pod, 0, len(nodes))
	for _, p := range createdPods {
		framework.ExpectNoError(e2epod.WaitTimeoutForPodReadyInNamespace(ctx, config.f.ClientSet, p.Name, config.f.Namespace.Name, framework.PodStartTimeout))
		rp, err := config.getPodClient().Get(ctx, p.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		runningPods = append(runningPods, rp)
	}

	return runningPods
}

// DeleteNetProxyPod 删除第一个 endpoint Pod 并等待其被移除。
func (config *NetworkingTestConfig) DeleteNetProxyPod(ctx context.Context) {
	pod := config.EndpointPods[0]
	framework.ExpectNoError(config.getPodClient().Delete(ctx, pod.Name, metav1.DeleteOptions{}))
	config.EndpointPods = config.EndpointPods[1:]
	// 等待 Pod 被删除。
	err := e2epod.WaitForPodNotFoundInNamespace(ctx, config.f.ClientSet, pod.Name, config.Namespace, config.f.Timeouts.PodDelete)
	if err != nil {
		framework.Failf("Failed to delete %s pod: %v", pod.Name, err)
	}
	// 等待 endpoint 被移除。
	numEndpoints := len(config.EndpointPods)
	if config.DualStackEnabled {
		numEndpoints = 2 * len(config.EndpointPods)
	}
	err = e2eendpointslice.WaitForEndpointCount(ctx, config.f.ClientSet, config.Namespace, nodePortServiceName, numEndpoints)
	if err != nil {
		framework.Failf("Failed to remove endpoint from service: %s", nodePortServiceName)
	}
	// 等待 kube-proxy 跟上 Pod 被删除的变化。
	time.Sleep(5 * time.Second)
}

func (config *NetworkingTestConfig) createPod(ctx context.Context, pod *v1.Pod) *v1.Pod {
	return config.getPodClient().Create(ctx, pod)
}

func (config *NetworkingTestConfig) getPodClient() *e2epod.PodClient {
	if config.podClient == nil {
		config.podClient = e2epod.NewPodClient(config.f)
	}
	return config.podClient
}

func (config *NetworkingTestConfig) getServiceClient() coreclientset.ServiceInterface {
	return config.f.ClientSet.CoreV1().Services(config.Namespace)
}

// HTTPPokeParams 是 HTTP poke 参数的结构体。
type HTTPPokeParams struct {
	Timeout        time.Duration // 默认 = 10 秒
	ExpectCode     int           // 默认 = 200
	BodyContains   string
	RetriableCodes []int
	EnableHTTPS    bool
}

// HTTPPokeResult 是 HTTP poke 结果的结构体。
type HTTPPokeResult struct {
	Status HTTPPokeStatus
	Code   int    // HTTP 状态码：连接未建立时为 0
	Error  error  // 出现的任何错误
	Body   []byte // code != 0 时有效
}

// HTTPPokeStatus 是表示 HTTP poke 状态的字符串。
type HTTPPokeStatus string

const (
	// HTTPSuccess 表示 HTTP poke 成功的状态。
	HTTPSuccess HTTPPokeStatus = "Success"
	// HTTPError 表示 HTTP poke 出错的状态。
	HTTPError HTTPPokeStatus = "UnknownError"
	// HTTPTimeout 表示 HTTP poke 超时的状态。
	HTTPTimeout HTTPPokeStatus = "TimedOut"
	// HTTPRefused 表示 HTTP poke 连接被拒绝的状态。
	HTTPRefused HTTPPokeStatus = "ConnectionRefused"
	// HTTPRetryCode 表示 HTTP poke 收到重试状态码的状态。
	HTTPRetryCode HTTPPokeStatus = "RetryCode"
	// HTTPWrongCode 表示 HTTP poke 收到错误状态码的状态。
	HTTPWrongCode HTTPPokeStatus = "WrongCode"
	// HTTPBadResponse 表示 HTTP poke 响应错误的状态。
	HTTPBadResponse HTTPPokeStatus = "BadResponse"
	// 每次新增错误类型时，都应审查本结构体的所有调用方。
)

// PokeHTTP 尝试用给定 URL 路径连接某主机的某端口。调用方
// 可按需指定额外的成功参数。
//
// 根据已知调用方，结果状态会尽可能精确地描述情况。
//
// 任何连接失败时结果 code 为 0；HTTP 事务完成（即使其他测试参数使这次
// 判定为失败）则为非 0。
//
// 除 Success 外的任何状态都会填充结果 error。
//
// HTTP 事务完成时会填充结果 body，
// 即使其他测试参数使这次判定为失败。
func PokeHTTP(host string, port int, path string, params *HTTPPokeParams) HTTPPokeResult {
	// 设置默认参数。
	if params == nil {
		params = &HTTPPokeParams{}
	}

	hostPort := net.JoinHostPort(host, strconv.Itoa(port))
	var url string
	if params.EnableHTTPS {
		url = fmt.Sprintf("https://%s%s", hostPort, path)
	} else {
		url = fmt.Sprintf("http://%s%s", hostPort, path)
	}

	ret := HTTPPokeResult{}

	// 对输入做健全性检查，因为确实出过问题。只有这些情况
	// 会让测试硬失败——它们本质上是 ASSERT()。
	if host == "" {
		framework.Failf("Got empty host for HTTP poke (%s)", url)
		return ret
	}
	if port == 0 {
		framework.Failf("Got port==0 for HTTP poke (%s)", url)
		return ret
	}

	if params.ExpectCode == 0 {
		params.ExpectCode = http.StatusOK
	}

	if params.Timeout == 0 {
		params.Timeout = 10 * time.Second
	}

	framework.Logf("Poking %q", url)

	resp, err := httpGetNoConnectionPoolTimeout(url, params.Timeout)
	if err != nil {
		ret.Error = err
		neterr, ok := err.(net.Error)
		if ok && neterr.Timeout() {
			ret.Status = HTTPTimeout
		} else if strings.Contains(err.Error(), "connection refused") {
			ret.Status = HTTPRefused
		} else {
			ret.Status = HTTPError
		}
		framework.Logf("Poke(%q): %v", url, err)
		return ret
	}

	ret.Code = resp.StatusCode

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		ret.Status = HTTPError
		ret.Error = fmt.Errorf("error reading HTTP body: %w", err)
		framework.Logf("Poke(%q): %v", url, ret.Error)
		return ret
	}
	ret.Body = make([]byte, len(body))
	copy(ret.Body, body)

	if resp.StatusCode != params.ExpectCode {
		for _, code := range params.RetriableCodes {
			if resp.StatusCode == code {
				ret.Error = fmt.Errorf("retriable status code: %d", resp.StatusCode)
				ret.Status = HTTPRetryCode
				framework.Logf("Poke(%q): %v", url, ret.Error)
				return ret
			}
		}
		ret.Status = HTTPWrongCode
		ret.Error = fmt.Errorf("bad status code: %d", resp.StatusCode)
		framework.Logf("Poke(%q): %v", url, ret.Error)
		return ret
	}

	if params.BodyContains != "" && !strings.Contains(string(body), params.BodyContains) {
		ret.Status = HTTPBadResponse
		ret.Error = fmt.Errorf("response does not contain expected substring: %q", string(body))
		framework.Logf("Poke(%q): %v", url, ret.Error)
		return ret
	}

	ret.Status = HTTPSuccess
	framework.Logf("Poke(%q): success", url)
	return ret
}

// 执行 HTTP GET，但不复用 TCP 连接
// 这样能暴露 iptables 规则已变但我们却看不到变化的问题
func httpGetNoConnectionPoolTimeout(url string, timeout time.Duration) (*http.Response, error) {
	tr := utilnet.SetTransportDefaults(&http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	})
	client := &http.Client{
		Transport: tr,
		Timeout:   timeout,
	}

	return client.Get(url)
}

// WaitForService 等待 Service 出现（exist == true）或消失（exist == false）
func WaitForService(ctx context.Context, c clientset.Interface, namespace, name string, exist bool, interval, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := c.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
		switch {
		case err == nil:
			framework.Logf("Service %s in namespace %s found.", name, namespace)
			return exist, nil
		case apierrors.IsNotFound(err):
			framework.Logf("Service %s in namespace %s disappeared.", name, namespace)
			return !exist, nil
		case err != nil:
			framework.Logf("Non-retryable failure while getting service.")
			return false, err
		default:
			framework.Logf("Get service %s in namespace %s failed: %v", name, namespace, err)
			return false, nil
		}
	})
	if err != nil {
		stateMsg := map[bool]string{true: "to appear", false: "to disappear"}
		return fmt.Errorf("error waiting for service %s/%s %s: %w", namespace, name, stateMsg[exist], err)
	}
	return nil
}
