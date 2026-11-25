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
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enetwork "k8s.io/kubernetes/test/e2e/framework/network"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	"k8s.io/kubernetes/test/e2e/network/common"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

// checkConnectivityToHost 启动一个 Pod 测试到指定主机的连通性；若从 Pod 无法访问该主机则返回错误。
//
// nodeName 为空时由调度器决定 Pod 运行在哪个节点。
func checkConnectivityToHost(ctx context.Context, f *framework.Framework, nodeName, podName, host string, port, timeout int) error {
	command := []string{
		"nc",
		"-vz",
		"-w", strconv.Itoa(timeout),
		host,
		strconv.Itoa(port),
	}

	pod := e2epod.NewAgnhostPod(f.Namespace.Name, podName, nil, nil, nil)
	pod.Spec.Containers[0].Command = command
	pod.Spec.Containers[0].Args = nil // otherwise 'pause` is magically an argument to nc, which causes all hell to break loose
	nodeSelection := e2epod.NodeSelection{Name: nodeName}
	e2epod.SetNodeSelection(&pod.Spec, nodeSelection)
	pod.Spec.RestartPolicy = v1.RestartPolicyNever

	podClient := f.ClientSet.CoreV1().Pods(f.Namespace.Name)
	_, err := podClient.Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, podName, f.Namespace.Name)

	if err != nil {
		logs, logErr := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		if logErr != nil {
			framework.Logf("Warning: Failed to get logs from pod %q: %v", pod.Name, logErr)
		} else {
			framework.Logf("pod %s/%s logs:\n%s", f.Namespace.Name, pod.Name, logs)
		}
	}

	return err
}

var _ = common.SIGDescribe("Networking", func() {
	var svcname = "nettest"
	f := framework.NewDefaultFramework(svcname)
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	f.It("should provide Internet connection for containers", feature.NetworkingIPv4, func(ctx context.Context) {
		ginkgo.By("Running container which tries to connect to 8.8.8.8")
		framework.ExpectNoError(
			checkConnectivityToHost(ctx, f, "", "connectivity-test", "8.8.8.8", 53, 30))
	})

	f.It("should provide Internet connection for containers", feature.NetworkingIPv6, "[Experimental][LinuxOnly]", func(ctx context.Context) {
		// Windows 不支持 IPv6。
		e2eskipper.SkipIfNodeOSDistroIs("windows")
		ginkgo.By("Running container which tries to connect to 2001:4860:4860::8888")
		framework.ExpectNoError(
			checkConnectivityToHost(ctx, f, "", "connectivity-test", "2001:4860:4860::8888", 53, 30))
	})

	f.It("should provider Internet connection for containers using DNS", feature.NetworkingDNS, func(ctx context.Context) {
		ginkgo.By("Running container which tries to connect to google.com")
		framework.ExpectNoError(
			checkConnectivityToHost(ctx, f, "", "connectivity-test", "google.com", 80, 30))
	})

	// 先运行此测试，因为它不依赖后面创建的变量。
	ginkgo.It("should provide unchanging, static URL paths for kubernetes api services", func(ctx context.Context) {
		tests := []struct {
			path string
		}{
			{path: "/healthz"},
			{path: "/api"},
			{path: "/apis"},
			{path: "/metrics"},
			{path: "/openapi/v2"},
			{path: "/version"},
			// TODO: 在此测试 proxy links
		}
		for _, test := range tests {
			ginkgo.By(fmt.Sprintf("testing: %s", test.path))
			data, err := f.ClientSet.CoreV1().RESTClient().Get().
				AbsPath(test.path).
				DoRaw(ctx)
			if err != nil {
				framework.Failf("ginkgo.Failed: %v\nBody: %s", err, string(data))
			}
		}
	})

	ginkgo.It("should check kube-proxy urls", func(ctx context.Context) {
		// TODO: 这里有些过度设计；只需一个 hostNetwork Pod 访问 kube-proxy 的 URL 即可。
		config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.UseHostNetwork)

		ginkgo.By("checking kube-proxy URLs")
		config.GetSelfURL(ctx, ports.ProxyHealthzPort, "/healthz", "200 OK")
		// 验证 /healthz 返回正确内容。
		config.GetSelfURL(ctx, ports.ProxyHealthzPort, "/healthz", "lastUpdated")
		// 验证 /proxyMode 返回 HTTP 状态码 200。
		config.GetSelfURLStatusCode(ctx, ports.ProxyStatusPort, "/proxyMode", "200")
	})

	ginkgo.Describe("Granular Checks: Services", func() {

		ginkgo.It("should function for pod-Service: http", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err := config.DialFromTestContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (nodeIP)", config.TestContainerPod.Name, config.NodeIP, config.NodeHTTPPort))

			err = config.DialFromTestContainer(ctx, "http", config.NodeIP, config.NodeHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		ginkgo.It("should function for pod-Service: udp", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterUDPPort))
			err := config.DialFromTestContainer(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (nodeIP)", config.TestContainerPod.Name, config.NodeIP, config.NodeUDPPort))
			err = config.DialFromTestContainer(ctx, "udp", config.NodeIP, config.NodeUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		f.It("should function for pod-Service: sctp", feature.SCTPConnectivity, func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.EnableSCTP)
			ginkgo.By(fmt.Sprintf("dialing(sctp) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterSCTPPort))
			err := config.DialFromTestContainer(ctx, "sctp", config.ClusterIP, e2enetwork.ClusterSCTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(sctp) %v --> %v:%v (nodeIP)", config.TestContainerPod.Name, config.NodeIP, config.NodeSCTPPort))
			err = config.DialFromTestContainer(ctx, "sctp", config.NodeIP, config.NodeSCTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		ginkgo.It("should function for node-Service: http", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.UseHostNetwork)
			ginkgo.By(fmt.Sprintf("dialing(http) %v (node) --> %v:%v (config.clusterIP)", config.NodeIP, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err := config.DialFromNode(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(http) %v (node) --> %v:%v (nodeIP)", config.NodeIP, config.NodeIP, config.NodeHTTPPort))
			err = config.DialFromNode(ctx, "http", config.NodeIP, config.NodeHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		ginkgo.It("should function for node-Service: udp", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.UseHostNetwork)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v (node) --> %v:%v (config.clusterIP)", config.NodeIP, config.ClusterIP, e2enetwork.ClusterUDPPort))
			err := config.DialFromNode(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(udp) %v (node) --> %v:%v (nodeIP)", config.NodeIP, config.NodeIP, config.NodeUDPPort))
			err = config.DialFromNode(ctx, "udp", config.NodeIP, config.NodeUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		f.It("should function for node-Service: sctp", feature.SCTPConnectivity, func(ctx context.Context) {
			ginkgo.Skip("Skipping SCTP node to service test until DialFromNode supports SCTP #96482")
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.EnableSCTP)
			ginkgo.By(fmt.Sprintf("dialing(sctp) %v (node) --> %v:%v (config.clusterIP)", config.NodeIP, config.ClusterIP, e2enetwork.ClusterSCTPPort))
			err := config.DialFromNode(ctx, "sctp", config.ClusterIP, e2enetwork.ClusterSCTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(sctp) %v (node) --> %v:%v (nodeIP)", config.NodeIP, config.NodeIP, config.NodeSCTPPort))
			err = config.DialFromNode(ctx, "sctp", config.NodeIP, config.NodeSCTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		ginkgo.It("should function for endpoint-Service: http", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (config.clusterIP)", config.EndpointPods[0].Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err := config.DialFromEndpointContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (nodeIP)", config.EndpointPods[0].Name, config.NodeIP, config.NodeHTTPPort))
			err = config.DialFromEndpointContainer(ctx, "http", config.NodeIP, config.NodeHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		ginkgo.It("should function for endpoint-Service: udp", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v (endpoint) --> %v:%v (config.clusterIP)", config.EndpointPods[0].Name, config.ClusterIP, e2enetwork.ClusterUDPPort))
			err := config.DialFromEndpointContainer(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By(fmt.Sprintf("dialing(udp) %v (endpoint) --> %v:%v (nodeIP)", config.EndpointPods[0].Name, config.NodeIP, config.NodeUDPPort))
			err = config.DialFromEndpointContainer(ctx, "udp", config.NodeIP, config.NodeUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		f.It("should function for endpoint-Service: sctp", feature.SCTPConnectivity, func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.EnableSCTP)
			ginkgo.By(fmt.Sprintf("dialing(sctp) %v (endpoint) --> %v:%v (config.clusterIP)", config.EndpointPods[0].Name, config.ClusterIP, e2enetwork.ClusterSCTPPort))
			err := config.DialFromEndpointContainer(ctx, "sctp", config.ClusterIP, e2enetwork.ClusterSCTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By(fmt.Sprintf("dialing(sctp) %v (endpoint) --> %v:%v (nodeIP)", config.EndpointPods[0].Name, config.NodeIP, config.NodeSCTPPort))
			err = config.DialFromEndpointContainer(ctx, "sctp", config.NodeIP, config.NodeSCTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		// 该测试确保在多个 Service 使用相同 selector 的情况下，
		// 删除其中一个不会影响其余 Service 的连通性
		ginkgo.It("should function for multiple endpoint-Services with same selector", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By("creating a second service with same selector")
			svc2, httpPort := createSecondNodePortService(ctx, f, config)

			// 原 Service 应正常工作
			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (config.clusterIP)", config.EndpointPods[0].Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err := config.DialFromEndpointContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (nodeIP)", config.EndpointPods[0].Name, config.NodeIP, config.NodeHTTPPort))
			err = config.DialFromEndpointContainer(ctx, "http", config.NodeIP, config.NodeHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			// 连接第二个 Service
			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (svc2.clusterIP)", config.EndpointPods[0].Name, svc2.Spec.ClusterIP, e2enetwork.ClusterHTTPPort))
			err = config.DialFromEndpointContainer(ctx, "http", svc2.Spec.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (nodeIP)", config.EndpointPods[0].Name, config.NodeIP, httpPort))
			err = config.DialFromEndpointContainer(ctx, "http", config.NodeIP, httpPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By("deleting the original node port service")
			config.DeleteNodePortService(ctx)

			// 第二个 Service 应继续正常工作
			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (svc2.clusterIP)", config.EndpointPods[0].Name, svc2.Spec.ClusterIP, e2enetwork.ClusterHTTPPort))
			err = config.DialFromEndpointContainer(ctx, "http", svc2.Spec.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(http) %v (endpoint) --> %v:%v (nodeIP)", config.EndpointPods[0].Name, config.NodeIP, httpPort))
			err = config.DialFromEndpointContainer(ctx, "http", config.NodeIP, httpPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		ginkgo.It("should update endpoints: http", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err := config.DialFromTestContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint (initial), %v", err)
			}
			ginkgo.By("Deleting a pod which, will be replaced with a new endpoint")
			config.DeleteNetProxyPod(ctx)

			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (config.clusterIP) (endpoint recovery)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err = config.DialFromTestContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, config.MaxTries, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint (recovery), %v", err)
			}
		})

		ginkgo.It("should update endpoints: udp", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterUDPPort))
			err := config.DialFromTestContainer(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint (initial), %v", err)
			}
			ginkgo.By("Deleting a pod which, will be replaced with a new endpoint")
			config.DeleteNetProxyPod(ctx)

			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (config.clusterIP) (endpoint recovery)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterUDPPort))
			err = config.DialFromTestContainer(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, config.MaxTries, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint (recovery), %v", err)
			}
		})

		// 较慢，因为要确认 nodePort 不再承载流量，需要持续轮询。
		f.It("should update nodePort: http", f.WithSlow(), func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.UseHostNetwork)
			ginkgo.By(fmt.Sprintf("dialing(http) %v (node) --> %v:%v (ctx, nodeIP) and getting ALL host endpoints", config.NodeIP, config.NodeIP, config.NodeHTTPPort))
			err := config.DialFromNode(ctx, "http", config.NodeIP, config.NodeHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("Error dialing http from node: %v", err)
			}
			ginkgo.By("Deleting the node port access point")
			config.DeleteNodePortService(ctx)

			ginkgo.By(fmt.Sprintf("dialing(http) %v (node) --> %v:%v (nodeIP) and getting ZERO host endpoints", config.NodeIP, config.NodeIP, config.NodeHTTPPort))
			// #106770 大集群中 MaxTries 可能很大，存在其他测试新建 NodePort 并开始响应流量的风险。
			// 由于只需断言流量不再被转发，且重试超时为 2 秒，10 次尝试后 Service 无响应即可认为测试通过。
			err = config.DialFromNode(ctx, "http", config.NodeIP, config.NodeHTTPPort, 10, 10, sets.NewString())
			if err != nil {
				framework.Failf("Failure validating that node port service STOPPED removed properly: %v", err)
			}
		})

		// 快速校验 UDP；下一个测试会确认 endpoint 移除后 Service 也会更新，但耗时更长。
		ginkgo.It("should support basic nodePort: udp functionality", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.UseHostNetwork)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v (node) --> %v:%v (nodeIP) and getting ALL host endpoints", config.NodeIP, config.NodeIP, config.NodeUDPPort))
			err := config.DialFromNode(ctx, "udp", config.NodeIP, config.NodeUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("Failure validating that nodePort service WAS forwarding properly: %v", err)
			}
		})

		// 较慢，因为要确认 nodePort 不再承载流量，需要持续轮询。
		f.It("should update nodePort: udp", f.WithSlow(), func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.UseHostNetwork)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v (node) --> %v:%v (nodeIP) and getting ALL host endpoints", config.NodeIP, config.NodeIP, config.NodeUDPPort))
			err := config.DialFromNode(ctx, "udp", config.NodeIP, config.NodeUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("Failure validating that nodePort service WAS forwarding properly: %v", err)
			}

			ginkgo.By("Deleting the node port access point")
			config.DeleteNodePortService(ctx)

			ginkgo.By(fmt.Sprintf("dialing(udp) %v (node) --> %v:%v (nodeIP) and getting ZERO host endpoints", config.NodeIP, config.NodeIP, config.NodeUDPPort))
			// #106770 大集群中 MaxTries 可能很大，存在其他测试新建 NodePort 并开始响应流量的风险。
			// 由于只需断言流量不再被转发，且重试超时为 2 秒，10 次尝试后 Service 无响应即可认为测试通过。
			err = config.DialFromNode(ctx, "udp", config.NodeIP, config.NodeUDPPort, 10, 10, sets.NewString())
			if err != nil {
				framework.Failf("Failure validating that node port service STOPPED removed properly: %v", err)
			}
		})

		// [LinuxOnly]: Windows 不支持会话亲和性。
		ginkgo.It("should function for client IP based session affinity: http [LinuxOnly]", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v", config.TestContainerPod.Name, config.SessionAffinityService.Spec.ClusterIP, e2enetwork.ClusterHTTPPort))

			// 检查返回的 endpoint 数量是否恰好为 1。
			eps, err := config.GetEndpointsFromTestContainer(ctx, "http", config.SessionAffinityService.Spec.ClusterIP, e2enetwork.ClusterHTTPPort, e2enetwork.SessionAffinityChecks)
			if err != nil {
				framework.Failf("ginkgo.Failed to get endpoints from test container, error: %v", err)
			}
			if len(eps) == 0 {
				framework.Failf("Unexpected no endpoints return")
			}
			if len(eps) > 1 {
				framework.Failf("Unexpected endpoints return: %v, expect 1 endpoints", eps)
			}
		})

		// [LinuxOnly]: Windows 不支持会话亲和性。
		ginkgo.It("should function for client IP based session affinity: udp [LinuxOnly]", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v", config.TestContainerPod.Name, config.SessionAffinityService.Spec.ClusterIP, e2enetwork.ClusterUDPPort))

			// 检查返回的 endpoint 数量是否恰好为 1。
			eps, err := config.GetEndpointsFromTestContainer(ctx, "udp", config.SessionAffinityService.Spec.ClusterIP, e2enetwork.ClusterUDPPort, e2enetwork.SessionAffinityChecks)
			if err != nil {
				framework.Failf("ginkgo.Failed to get endpoints from test container, error: %v", err)
			}
			if len(eps) == 0 {
				framework.Failf("Unexpected no endpoints return")
			}
			if len(eps) > 1 {
				framework.Failf("Unexpected endpoints return: %v, expect 1 endpoints", eps)
			}
		})

		ginkgo.It("should be able to handle large requests: http", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			message := strings.Repeat("42", 1000)
			err := config.DialEchoFromTestContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, message)
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		ginkgo.It("should be able to handle large requests: udp", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f)
			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterUDPPort))
			message := "n" + strings.Repeat("o", 1999)
			err := config.DialEchoFromTestContainer(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, message)
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
		})

		// 若 endpoint Pod 使用 hostNetwork，多个测试无法并行运行，
		// 因为它们会尝试占用宿主机上的相同端口；因此串行运行以避免端口冲突。
		ginkgo.It("should function for service endpoints using hostNetwork", func(ctx context.Context) {
			config := e2enetwork.NewNetworkingTestConfig(ctx, f, e2enetwork.UseHostNetwork, e2enetwork.EndpointsUseHostNetwork)

			ginkgo.By("pod-Service(hostNetwork): http")

			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err := config.DialFromTestContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (nodeIP)", config.TestContainerPod.Name, config.NodeIP, config.NodeHTTPPort))
			err = config.DialFromTestContainer(ctx, "http", config.NodeIP, config.NodeHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By("pod-Service(hostNetwork): udp")

			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterUDPPort))
			err = config.DialFromTestContainer(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (nodeIP)", config.TestContainerPod.Name, config.NodeIP, config.NodeUDPPort))
			err = config.DialFromTestContainer(ctx, "udp", config.NodeIP, config.NodeUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By("node-Service(hostNetwork): http")

			ginkgo.By(fmt.Sprintf("dialing(http) %v (node) --> %v:%v (config.clusterIP)", config.NodeIP, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			err = config.DialFromNode(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(http) %v (node) --> %v:%v (nodeIP)", config.NodeIP, config.NodeIP, config.NodeHTTPPort))
			err = config.DialFromNode(ctx, "http", config.NodeIP, config.NodeHTTPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By("node-Service(hostNetwork): udp")

			ginkgo.By(fmt.Sprintf("dialing(udp) %v (node) --> %v:%v (config.clusterIP)", config.NodeIP, config.ClusterIP, e2enetwork.ClusterUDPPort))
			err = config.DialFromNode(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}
			ginkgo.By(fmt.Sprintf("dialing(udp) %v (node) --> %v:%v (nodeIP)", config.NodeIP, config.NodeIP, config.NodeUDPPort))
			err = config.DialFromNode(ctx, "udp", config.NodeIP, config.NodeUDPPort, config.MaxTries, 0, config.EndpointHostnames())
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By("handle large requests: http(hostNetwork)")

			ginkgo.By(fmt.Sprintf("dialing(http) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterHTTPPort))
			message := strings.Repeat("42", 1000)
			err = config.DialEchoFromTestContainer(ctx, "http", config.ClusterIP, e2enetwork.ClusterHTTPPort, config.MaxTries, 0, message)
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

			ginkgo.By("handle large requests: udp(hostNetwork)")

			ginkgo.By(fmt.Sprintf("dialing(udp) %v --> %v:%v (config.clusterIP)", config.TestContainerPod.Name, config.ClusterIP, e2enetwork.ClusterUDPPort))
			message = "n" + strings.Repeat("o", 1999)
			err = config.DialEchoFromTestContainer(ctx, "udp", config.ClusterIP, e2enetwork.ClusterUDPPort, config.MaxTries, 0, message)
			if err != nil {
				framework.Failf("failed dialing endpoint, %v", err)
			}

		})

	})

	f.It("should recreate its iptables rules if they are deleted", f.WithDisruptive(), f.WithProvider(framework.ProvidersWithSSH...), func(ctx context.Context) {
		e2eskipper.SkipUnlessSSHKeyPresent()

		hosts, err := e2essh.NodeSSHHosts(ctx, f.ClientSet)
		framework.ExpectNoError(err, "failed to find external/internal IPs for every node")
		if len(hosts) == 0 {
			framework.Failf("No ssh-able nodes")
		}
		host := hosts[0]

		ns := f.Namespace.Name
		numPods, servicePort := 3, defaultServeHostnameServicePort
		svc := "iptables-flush-test"

		ginkgo.DeferCleanup(StopServeHostnameService, f.ClientSet, ns, svc)
		podNames, svcIP, err := StartServeHostnameService(ctx, f.ClientSet, getServeHostnameService(svc), ns, numPods)
		framework.ExpectNoError(err, "failed to create replication controller with service: %s in the namespace: %s", svc, ns)

		// 理想情况下应重载系统防火墙，但不一定知道在本系统上该怎么做
		// （"firewall-cmd --reload"？"systemctl restart iptables"？），
		// 因此这里手动删除所有 "KUBE-" 链。

		ginkgo.By("dumping iptables rules on node " + host)
		result, err := e2essh.SSH(ctx, "sudo iptables-save", host, framework.TestContext.Provider)
		e2essh.LogResult(result)
		if err != nil || result.Code != 0 {
			framework.Failf("couldn't dump iptable rules: %v", err)
		}

		// 所有删除规则的命令必须放在删除链的命令之前，因为仍有规则引用时无法删除链。
		var deleteRuleCmds, deleteChainCmds []string
		table := ""
		for _, line := range strings.Split(result.Stdout, "\n") {
			if strings.HasPrefix(line, "*") {
				table = line[1:]
			} else if table == "" {
				continue
			}

			// 删除从非 KUBE 链跳转到 KUBE 链的规则
			if !strings.HasPrefix(line, "-A KUBE-") && strings.Contains(line, "-j KUBE-") {
				deleteRuleCmds = append(deleteRuleCmds, fmt.Sprintf("sudo iptables -t %s -D %s || true", table, line[3:]))
			}
			// 清空并删除所有 KUBE 链
			if strings.HasPrefix(line, ":KUBE-") {
				chain := strings.Split(line, " ")[0][1:]
				deleteRuleCmds = append(deleteRuleCmds, fmt.Sprintf("sudo iptables -t %s -F %s || true", table, chain))
				deleteChainCmds = append(deleteChainCmds, fmt.Sprintf("sudo iptables -t %s -X %s || true", table, chain))
			}
		}
		cmd := strings.Join(append(deleteRuleCmds, deleteChainCmds...), "\n")

		ginkgo.By("deleting all KUBE-* iptables chains")
		result, err = e2essh.SSH(ctx, cmd, host, framework.TestContext.Provider)
		if err != nil || result.Code != 0 {
			e2essh.LogResult(result)
			framework.Failf("couldn't delete iptable rules: %v", err)
		}

		ginkgo.By("verifying that kube-proxy rules are eventually recreated")
		framework.ExpectNoError(verifyServeHostnameServiceUp(ctx, f.ClientSet, ns, podNames, svcIP, servicePort))

		ginkgo.By("verifying that kubelet rules are eventually recreated")
		err = utilwait.PollImmediate(framework.Poll, framework.RestartNodeReadyAgainTimeout, func() (bool, error) {
			result, err = e2essh.SSH(ctx, "sudo iptables-save -t mangle", host, framework.TestContext.Provider)
			if err != nil || result.Code != 0 {
				e2essh.LogResult(result)
				return false, err
			}

			if strings.Contains(result.Stdout, "\n:KUBE-IPTABLES-HINT") {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			e2essh.LogResult(result)
		}
		framework.ExpectNoError(err, "kubelet did not recreate its iptables rules")
	})

	// 标记为 [Serial]，因为不能与 [Feature:SCTPConnectivity] 测试同时运行，
	// 后者可能导致 sctp.ko 被加载。
	f.It("should allow creating a Pod with an SCTP HostPort [LinuxOnly]", f.WithSerial(), func(ctx context.Context) {
		node, err := e2enode.GetRandomReadySchedulableNode(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		hostExec := utils.NewHostExec(f)
		ginkgo.DeferCleanup(hostExec.Cleanup)

		ginkgo.By("getting the state of the sctp module on the selected node")
		nodes := &v1.NodeList{}
		nodes.Items = append(nodes.Items, *node)
		sctpLoadedAtStart := CheckSCTPModuleLoadedOnNodes(ctx, f, nodes)

		ginkgo.By("creating a pod with hostport on the selected node")
		podName := "hostport"
		ports := []v1.ContainerPort{{Protocol: v1.ProtocolSCTP, ContainerPort: 5060, HostPort: 5060}}
		podSpec := e2epod.NewAgnhostPod(f.Namespace.Name, podName, nil, nil, ports)
		nodeSelection := e2epod.NodeSelection{Name: node.Name}
		e2epod.SetNodeSelection(&podSpec.Spec, nodeSelection)

		ginkgo.By(fmt.Sprintf("Launching the pod on node %v", node.Name))
		e2epod.NewPodClient(f).CreateSync(ctx, podSpec)
		ginkgo.DeferCleanup(func(ctx context.Context) {
			err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Delete(ctx, podName, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "failed to delete pod: %s in namespace: %s", podName, f.Namespace.Name)
		})
		ginkgo.By("validating sctp module is still not loaded")
		sctpLoadedAtEnd := CheckSCTPModuleLoadedOnNodes(ctx, f, nodes)
		if !sctpLoadedAtStart && sctpLoadedAtEnd {
			framework.Failf("The state of the sctp module has changed due to the test case")
		}
	})
})
