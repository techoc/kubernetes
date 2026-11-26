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

package network

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edaemonset "k8s.io/kubernetes/test/e2e/framework/daemonset"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eservice "k8s.io/kubernetes/test/e2e/framework/service"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/network/common"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/lithammer/dedent"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	serviceName = "svc-udp"
	podClient   = "pod-client"
	podBackend1 = "pod-server-1"
	podBackend2 = "pod-server-2"
	podBackend3 = "pod-server-3"
	srcPort     = 12345
)

// Linux NAT 用 conntrack 完成 NAT：每看到一条新 flow，conntrack 表中
// 就会创建一条连接，供 NAT 模块使用。
//
// conntrack 表中每条记录都带一个超时时间，到期后
// 连接会被移除。
// UDP 是无连接协议，因此 conntrack 模块的跟踪功能
// 并不复杂。
// 它使用很短的超时（默认 30 秒），有匹配该连接的新 flow 时续期，
// 否则记录过期。
// 在 Kubernetes 中这种行为可能引发问题：发送端不停发流量，conntrack 记录
// 永不超时，但 Pod 或 endpoint 已被删除，导致流量被黑洞。
//
// 为缓解这个问题，Kubernetes 会删除陈旧记录：
// - endpoint 被移除时
// - Service 从无 endpoint 变为有新 endpoint 时

// 参考：https://api.semanticscholar.org/CorpusID:198903401
// Boye, Magnus. "Netfilter Connection Tracking and NAT Implementation." (2012)。

var _ = common.SIGDescribe("Conntrack", func() {

	fr := framework.NewDefaultFramework("conntrack")
	fr.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	type nodeInfo struct {
		name   string
		nodeIP string
	}

	var (
		cs                             clientset.Interface
		ns                             string
		clientNodeInfo, serverNodeInfo nodeInfo
	)

	logContainsFn := func(text, podName string) wait.ConditionWithContextFunc {
		return func(ctx context.Context) (bool, error) {
			logs, err := e2epod.GetPodLogs(ctx, cs, ns, podName, podName)
			if err != nil {
				// 下次重试该错误。
				return false, nil
			}
			if !strings.Contains(string(logs), text) {
				return false, nil
			}
			return true, nil
		}
	}

	ginkgo.BeforeEach(func(ctx context.Context) {
		cs = fr.ClientSet
		ns = fr.Namespace.Name

		nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, cs, 2)
		framework.ExpectNoError(err)
		if len(nodes.Items) < 2 {
			e2eskipper.Skipf(
				"Test requires >= 2 Ready nodes, but there are only %v nodes",
				len(nodes.Items))
		}

		family := v1.IPv4Protocol
		if framework.TestContext.ClusterIsIPv6() {
			family = v1.IPv6Protocol
		}

		ips := e2enode.GetAddressesByTypeAndFamily(&nodes.Items[0], v1.NodeInternalIP, family)
		gomega.Expect(ips).ToNot(gomega.BeEmpty())

		clientNodeInfo = nodeInfo{
			name:   nodes.Items[0].Name,
			nodeIP: ips[0],
		}

		ips = e2enode.GetAddressesByTypeAndFamily(&nodes.Items[1], v1.NodeInternalIP, family)
		gomega.Expect(ips).ToNot(gomega.BeEmpty())

		serverNodeInfo = nodeInfo{
			name:   nodes.Items[1].Name,
			nodeIP: ips[0],
		}
	})

	// generateProbeCommand 生成一个 bash 脚本，持续向目标发送 UDP 包。
	// 需要持续的 UDP 流量防止 conntrack 记录过期。
	// UDP conntrack 记录默认 30 秒超时，周期性发包可保持 flow 活跃。
	// 参考：https://github.com/kubernetes/kubernetes/issues/139683
	generateProbeCommand := func(payload, targetIP string, targetPort int32, srcPort, timeout int) string {
		return fmt.Sprintf(dedent.Dedent(`
				TARGET_IP="%s"
				TARGET_PORT="%d"
				INTERVAL=1
				TIMEOUT=%d
				REQ_PAYLOAD="%s"

				echo "Starting UDP probe to ${TARGET_IP}:${TARGET_PORT} every ${INTERVAL}s..."

				PREV_RESPONSE=""
				COUNT=0

				for i in $(seq 1 3000); do
						TIMESTAMP=$(date '+%%H:%%M:%%S')

						# Send UDP packet and remove newlines from the response.
						# The agnhost image use netcat-openbsd (flags are not portable across different netcat versions):
						# -u: UDP mode.
						# -w <timeout>: timeout in seconds, in UDP mode it always waits for the entire timeout.
						# -W 1: Ensures that nc exits immediately after receiving 1 packet.
						# -p <srcPort>: UDP source port to always use the same flow.
						# - 2>/dev/null: Hides connection errors since we handle failures based on empty responses.
						RESPONSE=$(echo "$REQ_PAYLOAD" | nc -w "$TIMEOUT" -W 1 -u -p %d "$TARGET_IP" "$TARGET_PORT" 2>/dev/null | tr -d '\r\n')

						# If the response state changes (e.g., from SUCCESS to FAIL, or between different backends):
						if [ "$RESPONSE" != "$PREV_RESPONSE" ]; then
							# Print the accumulated total for the previous response state.
							if [ "$COUNT" -gt 0 ]; then
								if [ -n "$PREV_RESPONSE" ]; then
									echo "[$TIMESTAMP] SUCCESS: $PREV_RESPONSE ($COUNT times)"
								else
									echo "[$TIMESTAMP] FAIL ($COUNT times)"
								fi
							fi

							# Reset tracking variables for the new state.
							PREV_RESPONSE="$RESPONSE"
							COUNT=1

							# Immediately print the first occurrence of the new state.
							if [ -n "$RESPONSE" ]; then
								echo "[$TIMESTAMP] SUCCESS: $RESPONSE"
							else
								echo "[$TIMESTAMP] FAIL"
							fi
						else
							# If the response is the same as before, just increment the counter to avoid spamming the logs.
							COUNT=$((COUNT+1))
						fi

						sleep "$INTERVAL"
				done
				`), targetIP, targetPort, timeout, payload, srcPort)
	}

	ginkgo.It("should be able to preserve UDP traffic when server pod cycles for a NodePort service", func(ctx context.Context) {

		// 创建 NodePort Service
		udpJig := e2eservice.NewTestJig(cs, ns, serviceName)
		ginkgo.By("creating a UDP service " + serviceName + " with type=NodePort in " + ns)
		udpService, err := udpJig.CreateUDPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "udp", Protocol: v1.ProtocolUDP, TargetPort: intstr.FromInt32(80)},
			}
		})
		framework.ExpectNoError(err)

		// 在一个节点上创建 Pod，每 5 秒向 NodePort Service 发 UDP 流量
		ginkgo.By("creating a client pod for probing the service " + serviceName)
		clientPod := e2epod.NewAgnhostPod(ns, podClient, nil, nil, nil)
		nodeSelection := e2epod.NodeSelection{Name: clientNodeInfo.name}
		e2epod.SetNodeSelection(&clientPod.Spec, nodeSelection)
		cmd := generateProbeCommand("hostname", serverNodeInfo.nodeIP, udpService.Spec.Ports[0].NodePort, srcPort, 5)
		clientPod.Spec.Containers[0].Command = []string{"/bin/sh", "-c", cmd}
		clientPod.Spec.Containers[0].Name = podClient
		e2epod.NewPodClient(fr).CreateSync(ctx, clientPod)

		// 读取 client Pod 日志
		logs, err := e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
		framework.ExpectNoError(err)
		framework.Logf("Pod client logs: %s", logs)

		// 在另一个节点上给 Service 添加 backend Pod
		ginkgo.By("creating a backend pod " + podBackend1 + " for the service " + serviceName)
		serverPod1 := e2epod.NewAgnhostPod(ns, podBackend1, nil, nil, nil, "netexec", fmt.Sprintf("--udp-port=%d", 80))
		serverPod1.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod1.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod1)

		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, podBackend1)
		framework.ExpectNoError(err)

		// 注意 EndpointSlice 对象已存在，并不代表
		// iptables（或其他机制）已经配置好。
		// 还要考虑 UDP conntrack 记录默认
		// 30 秒超时。
		// 基于以上情况检查 Pod 是否收到流量。
		ginkgo.By("checking client pod connected to the backend 1 on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend1, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 1")
		}

		// 创建第二个 Pod
		ginkgo.By("creating a second backend pod " + podBackend2 + " for the service " + serviceName)
		serverPod2 := e2epod.NewAgnhostPod(ns, podBackend2, nil, nil, nil, "netexec", fmt.Sprintf("--udp-port=%d", 80))
		serverPod2.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod2.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod2)

		// 并删除第一个 Pod
		framework.Logf("Cleaning up %s pod", podBackend1)
		e2epod.NewPodClient(fr).DeleteSync(ctx, podBackend1, metav1.DeleteOptions{}, fr.Timeouts.PodDelete)

		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, podBackend2)
		framework.ExpectNoError(err)

		// 检查第二个 Pod 是否持续收到流量
		// UDP conntrack 记录默认 30 秒超时
		ginkgo.By("checking client pod connected to the backend 2 on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend2, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 2")
		}
	})

	ginkgo.It("should be able to preserve UDP traffic when server pod cycles for a ClusterIP service", func(ctx context.Context) {

		// 创建 ClusterIP Service
		udpJig := e2eservice.NewTestJig(cs, ns, serviceName)
		ginkgo.By("creating a UDP service " + serviceName + " with type=ClusterIP in " + ns)
		udpService, err := udpJig.CreateUDPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "udp", Protocol: v1.ProtocolUDP, TargetPort: intstr.FromInt32(80)},
			}
		})
		framework.ExpectNoError(err)

		// 在一个节点上创建 Pod，每 5 秒向 ClusterIP Service 发 UDP 流量
		ginkgo.By("creating a client pod for probing the service " + serviceName)
		clientPod := e2epod.NewAgnhostPod(ns, podClient, nil, nil, nil)
		nodeSelection := e2epod.NodeSelection{Name: clientNodeInfo.name}
		e2epod.SetNodeSelection(&clientPod.Spec, nodeSelection)
		cmd := generateProbeCommand("hostname", udpService.Spec.ClusterIP, udpService.Spec.Ports[0].Port, srcPort, 5)
		clientPod.Spec.Containers[0].Command = []string{"/bin/sh", "-c", cmd}
		clientPod.Spec.Containers[0].Name = podClient
		e2epod.NewPodClient(fr).CreateSync(ctx, clientPod)

		// 读取 client Pod 日志
		logs, err := e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
		framework.ExpectNoError(err)
		framework.Logf("Pod client logs: %s", logs)

		// 在另一个节点上给 Service 添加 backend Pod
		ginkgo.By("creating a backend pod " + podBackend1 + " for the service " + serviceName)
		serverPod1 := e2epod.NewAgnhostPod(ns, podBackend1, nil, nil, nil, "netexec", fmt.Sprintf("--udp-port=%d", 80))
		serverPod1.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod1.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod1)

		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, podBackend1)
		framework.ExpectNoError(err)

		// 注意 EndpointSlice 对象已存在，并不代表
		// iptables（或其他机制）已经配置好。
		// 还要考虑 UDP conntrack 记录默认
		// 30 秒超时。
		// 基于以上情况检查 Pod 是否收到流量。
		ginkgo.By("checking client pod connected to the backend 1 on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend1, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 1")
		}

		// 创建第二个 Pod
		ginkgo.By("creating a second backend pod " + podBackend2 + " for the service " + serviceName)
		serverPod2 := e2epod.NewAgnhostPod(ns, podBackend2, nil, nil, nil, "netexec", fmt.Sprintf("--udp-port=%d", 80))
		serverPod2.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod2.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod2)

		// 并删除第一个 Pod
		framework.Logf("Cleaning up %s pod", podBackend1)
		e2epod.NewPodClient(fr).DeleteSync(ctx, podBackend1, metav1.DeleteOptions{}, fr.Timeouts.PodDelete)

		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, podBackend2)
		framework.ExpectNoError(err)

		// 检查第二个 Pod 是否持续收到流量
		// UDP conntrack 记录默认 30 秒超时
		ginkgo.By("checking client pod connected to the backend 2 on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend2, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 2")
		}
	})

	// 针对 https://issues.k8s.io/126934 的回归测试
	ginkgo.It("should be able to preserve UDP traffic when server pod cycles for a ClusterIP service with InternalTrafficPolicy set to Local", func(ctx context.Context) {

		// 创建 InternalTrafficPolicy 为 Local 的 ClusterIP Service
		udpJig := e2eservice.NewTestJig(cs, ns, serviceName)
		ginkgo.By("creating a UDP service " + serviceName + " with type=ClusterIP in " + ns)
		udpService, err := udpJig.CreateUDPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "udp", Protocol: v1.ProtocolUDP, TargetPort: intstr.FromInt32(80)},
			}
			svc.Spec.InternalTrafficPolicy = ptr.To(v1.ServiceInternalTrafficPolicyLocal)
		})
		framework.ExpectNoError(err)

		// 在一个节点上创建 Pod，每秒向 ClusterIP Service 发 UDP 流量
		ginkgo.By("creating a client pod for probing the service " + serviceName)
		clientPod := e2epod.NewAgnhostPod(ns, podClient, nil, nil, nil)
		nodeSelection := e2epod.NodeSelection{Name: clientNodeInfo.name}
		e2epod.SetNodeSelection(&clientPod.Spec, nodeSelection)
		cmd := generateProbeCommand("hostname", udpService.Spec.ClusterIP, udpService.Spec.Ports[0].Port, srcPort, 1)
		clientPod.Spec.Containers[0].Command = []string{"/bin/sh", "-c", cmd}
		clientPod.Spec.Containers[0].Name = podClient
		e2epod.NewPodClient(fr).CreateSync(ctx, clientPod)

		// 读取 client Pod 日志
		logs, err := e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
		framework.ExpectNoError(err)
		framework.Logf("Pod client logs: %s", logs)

		// 为 InternalTrafficPolicy 为 Local 的 Service 创建 daemonset，使每个 Pod 中有一个 Server
		// 且节点上所有 Pod 都把流量发到本地 Server。
		ginkgo.By("creating a daemonset pod for the service " + serviceName)
		dsName := "ds-conntrack"
		ds := e2edaemonset.NewDaemonSet(dsName, imageutils.GetE2EImage(imageutils.Agnhost), udpJig.Labels, nil, nil, []v1.ContainerPort{{ContainerPort: 80}}, "netexec", "--udp-port=80", "--delay-shutdown=10")
		ginkgo.By("Creating a DaemonSet")
		ds, err = fr.ClientSet.AppsV1().DaemonSets(ns).Create(ctx, ds, metav1.CreateOptions{})
		if err != nil {
			framework.Failf("unable to create test DaemonSet %s: %v", dsName, err)
		}
		ginkgo.By("Waiting for DaemonSet pods to become ready")
		err = wait.PollUntilContextTimeout(ctx, framework.Poll, framework.PodStartTimeout, false, func(ctx context.Context) (bool, error) {
			return e2edaemonset.CheckRunningOnAllNodes(ctx, fr, ds)
		})
		framework.ExpectNoError(err)
		// 获取与 client 在同一节点上运行的 Pod 名
		labelSelector := labels.SelectorFromSet(udpJig.Labels).String()
		fieldSelector := fields.OneTermEqualSelector("spec.nodeName", clientNodeInfo.name).String()
		listOpts := metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: fieldSelector}
		pods, err := fr.ClientSet.CoreV1().Pods(ns).List(ctx, listOpts)
		framework.ExpectNoError(err)
		if len(pods.Items) != 1 {
			framework.Failf("expected 1 pod, got %d pods", len(pods.Items))
		}
		podBackend1 := pods.Items[0].Name
		// 注意 EndpointSlice 对象已存在，并不代表
		// iptables（或其他机制）已经配置好。
		// 还要考虑 UDP conntrack 记录默认
		// 30 秒超时。
		// 基于以上情况检查 Pod 是否收到流量。
		ginkgo.By("checking client pod connected to " + podBackend1 + " on Node " + clientNodeInfo.name)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend1, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 1")
		}

		// 现在重建第一个 backend Pod
		framework.Logf("Cleaning up %s pod", podBackend1)
		e2epod.NewPodClient(fr).DeleteSync(ctx, podBackend1, metav1.DeleteOptions{}, fr.Timeouts.PodDelete)

		ginkgo.By("Waiting for DaemonSet pods to become ready")
		err = wait.PollUntilContextTimeout(ctx, framework.Poll, framework.PodStartTimeout, false, func(ctx context.Context) (bool, error) {
			return e2edaemonset.CheckRunningOnAllNodes(ctx, fr, ds)
		})
		framework.ExpectNoError(err)

		pods, err = fr.ClientSet.CoreV1().Pods(ns).List(ctx, listOpts)
		framework.ExpectNoError(err)
		if len(pods.Items) != 1 {
			framework.Failf("expected 1 pod, got %d pods", len(pods.Items))
		}
		podBackend2 := pods.Items[0].Name
		// 检查重建后新 Pod 是否持续收到流量
		// UDP conntrack 记录默认 30 秒超时
		ginkgo.By("checking client pod connected to " + podBackend2 + " on Node " + clientNodeInfo.name)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend2, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 3")
		}
	})

	ginkgo.It("should be able to preserve UDP traffic when server pod cycles for a ClusterIP service and client is hostNetwork", func(ctx context.Context) {

		// 创建 ClusterIP Service
		udpJig := e2eservice.NewTestJig(cs, ns, serviceName)
		ginkgo.By("creating a UDP service " + serviceName + " with type=ClusterIP in " + ns)
		udpService, err := udpJig.CreateUDPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "udp", Protocol: v1.ProtocolUDP, TargetPort: intstr.FromInt32(80)},
			}
		})
		framework.ExpectNoError(err)

		// 在一个节点上创建 Pod，每 5 秒向 ClusterIP Service 发 UDP 流量
		ginkgo.By("creating a client pod for probing the service " + serviceName)
		clientPod := e2epod.NewAgnhostPod(ns, podClient, nil, nil, nil)
		nodeSelection := e2epod.NodeSelection{Name: clientNodeInfo.name}
		e2epod.SetNodeSelection(&clientPod.Spec, nodeSelection)
		cmd := generateProbeCommand("hostname", udpService.Spec.ClusterIP, udpService.Spec.Ports[0].Port, srcPort, 5)
		clientPod.Spec.Containers[0].Command = []string{"/bin/sh", "-c", cmd}
		clientPod.Spec.Containers[0].Name = podClient
		clientPod.Spec.HostNetwork = true
		e2epod.NewPodClient(fr).CreateSync(ctx, clientPod)

		// 读取 client Pod 日志
		logs, err := e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
		framework.ExpectNoError(err)
		framework.Logf("Pod client logs: %s", logs)

		// 在另一个节点上给 Service 添加 backend Pod
		ginkgo.By("creating a backend pod " + podBackend1 + " for the service " + serviceName)
		serverPod1 := e2epod.NewAgnhostPod(ns, podBackend1, nil, nil, nil, "netexec", fmt.Sprintf("--udp-port=%d", 80))
		serverPod1.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod1.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod1)

		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, podBackend1)
		framework.ExpectNoError(err)

		// 注意 EndpointSlice 对象已存在，并不代表
		// iptables（或其他机制）已经配置好。
		// 还要考虑 UDP conntrack 记录默认
		// 30 秒超时。
		// 基于以上情况检查 Pod 是否收到流量。
		ginkgo.By("checking client pod connected to the backend 1 on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend1, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 1")
		}

		// 创建第二个 Pod
		ginkgo.By("creating a second backend pod " + podBackend2 + " for the service " + serviceName)
		serverPod2 := e2epod.NewAgnhostPod(ns, podBackend2, nil, nil, nil, "netexec", fmt.Sprintf("--udp-port=%d", 80))
		serverPod2.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod2.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod2)

		// 并删除第一个 Pod
		framework.Logf("Cleaning up %s pod", podBackend1)
		e2epod.NewPodClient(fr).DeleteSync(ctx, podBackend1, metav1.DeleteOptions{}, fr.Timeouts.PodDelete)

		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, podBackend2)
		framework.ExpectNoError(err)

		// 检查第二个 Pod 是否持续收到流量
		// UDP conntrack 记录默认 30 秒超时
		ginkgo.By("checking client pod connected to the backend 2 on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend2, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 2")
		}
	})

	// 针对 #105657 的回归测试
	// 1. 创建 UDP Service
	// 2. client Pod 向 UDP Service 发流量
	// 3. 创建与该 Service 关联的 UDP server，其 init container 会 sleep 一段时间
	// init container 使 server Pod 尚未就绪，但 endpoint slice 已创建，只是
	// Endpoint 的 Ready 条件为 false。
	// 如果 kube-proxy 的 conntrack 逻辑不检查 readiness，它会在 endpoint slice
	// 创建时删除 UDP server 的 conntrack 记录，但 iptables 规则至少要等一个
	// endpoint 就绪才会安装。如果此时有流量到达（kube-proxy 清了记录，见 endpoint slice），
	// 到安装相应 iptables 规则（endpoint 就绪）之间，会生成一条 conntrack 记录，
	// 把后续流量黑洞掉。
	ginkgo.It("should be able to preserve UDP traffic when initial unready endpoints get ready", func(ctx context.Context) {

		// 创建 ClusterIP Service
		udpJig := e2eservice.NewTestJig(cs, ns, serviceName)
		ginkgo.By("creating a UDP service " + serviceName + " with type=ClusterIP in " + ns)
		udpService, err := udpJig.CreateUDPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeClusterIP
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "udp", Protocol: v1.ProtocolUDP, TargetPort: intstr.FromInt32(80)},
			}
		})
		framework.ExpectNoError(err)

		// 在一个节点上创建 Pod，每 5 秒向 ClusterIP Service 发 UDP 流量
		ginkgo.By("creating a client pod for probing the service " + serviceName)
		clientPod := e2epod.NewAgnhostPod(ns, podClient, nil, nil, nil)
		nodeSelection := e2epod.NodeSelection{Name: clientNodeInfo.name}
		e2epod.SetNodeSelection(&clientPod.Spec, nodeSelection)
		cmd := generateProbeCommand("hostname", udpService.Spec.ClusterIP, udpService.Spec.Ports[0].Port, srcPort, 5)
		clientPod.Spec.Containers[0].Command = []string{"/bin/sh", "-c", cmd}
		clientPod.Spec.Containers[0].Name = podClient
		e2epod.NewPodClient(fr).CreateSync(ctx, clientPod)

		// 读取 client Pod 日志
		logs, err := e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
		framework.ExpectNoError(err)
		framework.Logf("Pod client logs: %s", logs)

		// 在另一个节点上给 Service 添加 backend Pod
		ginkgo.By("creating a backend pod " + podBackend1 + " for the service " + serviceName)
		serverPod1 := e2epod.NewAgnhostPod(ns, podBackend1, nil, nil, nil, "netexec", fmt.Sprintf("--udp-port=%d", 80))
		serverPod1.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		// 添加 init container，让 Pod 延迟 15 秒就绪
		serverPod1.Spec.InitContainers = []v1.Container{
			{
				Name:    "init",
				Image:   imageutils.GetE2EImage(imageutils.BusyBox),
				Command: []string{"/bin/sh", "-c", "echo Pausing start. && sleep 15"},
			},
		}
		e2epod.SetNodeSelection(&serverPod1.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod1)

		// 等待 endpoint 就绪
		err = e2eendpointslice.WaitForEndpointPods(ctx, cs, ns, serviceName, podBackend1)
		framework.ExpectNoError(err)

		// 注意 EndpointSlice 对象已存在，并不代表
		// iptables（或其他机制）已经配置好。
		// 还要考虑 UDP conntrack 记录默认
		// 30 秒超时。
		// 基于以上情况检查 Pod 是否收到流量。
		ginkgo.By("checking client pod connected to the backend on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn(podBackend1, podClient)); err != nil {
			logs, err = e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend pod")
		}

	})

	// 针对 #74839 的回归测试
	// conntrack 判定为 INVALID 的包不会做 NAT，这会导致
	// 一个长连接 TCP 到 Service IP 时出现虚假重传，最终
	// 报 "Connection reset by peer"。
	// proxy 实现（依赖 conntrack）要么丢弃 conntrack 标记为 INVALID 的包，
	// 要么强制 `nf_conntrack_tcp_be_liberal` 来
	// 规避这个问题。
	// 参考：https://kubernetes.io/blog/2019/03/29/kube-proxy-subtleties-debugging-an-intermittent-connection-reset/
	ginkgo.It("proxy implementation should not be vulnerable to the invalid conntrack state bug [Privileged]", func(ctx context.Context) {
		serverLabel := map[string]string{
			"app": "boom-server",
		}

		serverPod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "boom-server",
				Labels: serverLabel,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "boom-server",
						Image: imageutils.GetE2EImage(imageutils.RegressionIssue74839),
						Ports: []v1.ContainerPort{
							{
								ContainerPort: 9000, // boom-server 暴露的默认端口
							},
						},
						Env: []v1.EnvVar{
							{
								Name: "POD_IP",
								ValueFrom: &v1.EnvVarSource{
									FieldRef: &v1.ObjectFieldSelector{
										APIVersion: "v1",
										FieldPath:  "status.podIP",
									},
								},
							},
							{
								Name: "POD_IPS",
								ValueFrom: &v1.EnvVarSource{
									FieldRef: &v1.ObjectFieldSelector{
										APIVersion: "v1",
										FieldPath:  "status.podIPs",
									},
								},
							},
						},
						SecurityContext: &v1.SecurityContext{
							Capabilities: &v1.Capabilities{
								Add: []v1.Capability{"NET_RAW"},
							},
						},
					},
				},
			},
		}
		nodeSelection := e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod)
		ginkgo.By("Server pod created on node " + serverNodeInfo.name)

		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "boom-server",
			},
			Spec: v1.ServiceSpec{
				Selector: serverLabel,
				Ports: []v1.ServicePort{
					{
						Protocol: v1.ProtocolTCP,
						Port:     9000,
					},
				},
			},
		}
		_, err := fr.ClientSet.CoreV1().Services(fr.Namespace.Name).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Server service created")

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "startup-script",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "startup-script",
						Image: imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{
							"sh", "-c", "while true; do sleep 2; nc boom-server 9000& done",
						},
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		}
		nodeSelection = e2epod.NodeSelection{Name: clientNodeInfo.name}
		e2epod.SetNodeSelection(&pod.Spec, nodeSelection)

		e2epod.NewPodClient(fr).CreateSync(ctx, pod)
		ginkgo.By("Client pod created")

		// client 向 server 建立连接。
		// server 注入序列号超出窗口的包；如果这些包没做 NAT，client 会收到
		// 非预期的 TCP 包并 RST 连接，此时 server 会记录 ERROR。
		//
		ginkgo.By("checking client pod does not RST the TCP connection because it receives an out-of-window packet")
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn("ERROR", "boom-server")); err == nil {
			logs, err := e2epod.GetPodLogs(ctx, cs, ns, "boom-server", "boom-server")
			framework.ExpectNoError(err)
			framework.Logf("boom-server pod logs: %s", logs)
			framework.Failf("boom-server pod received a RST from the client, enabling nf_conntrack_tcp_be_liberal or dropping packets marked invalid by conntrack might help here.")
		}

		logs, err := e2epod.GetPodLogs(ctx, cs, ns, "boom-server", "boom-server")
		framework.ExpectNoError(err)
		if !strings.Contains(logs, "connection established") {
			framework.Logf("boom-server pod logs: %s", logs)
			framework.Failf("boom-server pod did not send any bad packet to the client")
		}
		framework.Logf("boom-server pod logs: %s", logs)
		framework.Logf("boom-server OK: did not receive any RST packet")
	})

	// 本测试检查 UDP Service 的 target port 变化时，旧 target port 的 conntrack 记录会被清理。
	// 还覆盖打印 serverport 记录的情况，确保清理逻辑正确。
	// 验证 https://pr.k8s.io/130542 中的修复
	ginkgo.It("should be able to cleanup conntrack entries when UDP service target port changes for a NodePort service", func(ctx context.Context) {
		// 创建 NodePort Service
		udpJig := e2eservice.NewTestJig(cs, ns, serviceName)
		ginkgo.By("creating a UDP service " + serviceName + " with type=NodePort in " + ns)
		udpService, err := udpJig.CreateUDPService(ctx, func(svc *v1.Service) {
			svc.Spec.Type = v1.ServiceTypeNodePort
			svc.Spec.Ports = []v1.ServicePort{
				{Port: 80, Name: "udp", Protocol: v1.ProtocolUDP, TargetPort: intstr.FromInt32(8080)},
			}
		})
		framework.ExpectNoError(err)
		framework.Logf("Older Target Port: %v", udpService.Spec.Ports[0].TargetPort)

		// 在一个节点上创建 Pod，每 5 秒向 NodePort Service 发 UDP 流量
		ginkgo.By("creating a client pod for probing the service " + serviceName)
		clientPod := e2epod.NewAgnhostPod(ns, podClient, nil, nil, nil)
		nodeSelection := e2epod.NodeSelection{Name: clientNodeInfo.name}
		e2epod.SetNodeSelection(&clientPod.Spec, nodeSelection)
		cmd := generateProbeCommand("serverport", serverNodeInfo.nodeIP, udpService.Spec.Ports[0].NodePort, srcPort, 5)
		clientPod.Spec.Containers[0].Command = []string{"/bin/sh", "-c", cmd}
		clientPod.Spec.Containers[0].Name = podClient
		e2epod.NewPodClient(fr).CreateSync(ctx, clientPod)

		// 在另一个节点上给 Service 添加 backend Pod
		ginkgo.By("creating a backend pod " + podBackend1 + " for the service " + serviceName)

		// 添加带容器端口的 backend Pod
		port8080 := []v1.ContainerPort{
			{
				ContainerPort: 8080,
				Protocol:      v1.ProtocolUDP,
			},
		}
		port9090 := []v1.ContainerPort{
			{
				ContainerPort: 9090,
				Protocol:      v1.ProtocolUDP,
			},
		}
		serverPod := e2epod.NewAgnhostPodFromContainers(
			"", "conntrack-handle-target-ports", nil,
			e2epod.NewAgnhostContainer("container-handle-8080-request", nil, port8080, "netexec", "--http-port", "8080", "--udp-port", "8080"),
			e2epod.NewAgnhostContainer("container-handle-9090-request", nil, port9090, "netexec", "--http-port", "9090", "--udp-port", "9090"),
		)
		serverPod.Labels = udpJig.Labels
		nodeSelection = e2epod.NodeSelection{Name: serverNodeInfo.name}
		e2epod.SetNodeSelection(&serverPod.Spec, nodeSelection)
		e2epod.NewPodClient(fr).CreateSync(ctx, serverPod)

		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "udp", Protocol: v1.ProtocolUDP, Target: serverPod.Name, TargetPort: 8080},
			},
		)
		framework.ExpectNoError(err)

		// 检查 server Pod 的第一个容器是否持续收到流量
		// UDP conntrack 记录默认 30 秒超时
		ginkgo.By("checking client pod connected to the container 1 of backend pod on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn("8080", podClient)); err != nil {
			logs, err := e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 2")
		}

		// 把 NodePort Service 的 TargetPort 更新为 9090
		service, err := udpJig.UpdateService(ctx, func(svc *v1.Service) {
			svc.Spec.Ports[0].TargetPort = intstr.FromInt32(9090)
		})
		framework.ExpectNoError(err)
		framework.Logf("Updated Target Port: %v", service.Spec.Ports[0].TargetPort)

		err = e2eendpointslice.WaitForEndpointPorts(ctx, cs, ns, serviceName,
			[]e2eendpointslice.PortMapping{
				{Name: "udp", Protocol: v1.ProtocolUDP, Target: serverPod.Name, TargetPort: 9090},
			},
		)
		framework.ExpectNoError(err)

		// 检查清理 conntrack 记录后，server Pod 的第二个容器是否持续收到流量
		// UDP conntrack 记录默认 30 秒超时
		// 清理记录后，应校验 NodePort 上的 serverport 与容器端口一致
		ginkgo.By("checking client pod connected to the container 2 of backend pod on Node IP " + serverNodeInfo.nodeIP)
		if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, time.Minute, true, logContainsFn("9090", podClient)); err != nil {
			logs, err := e2epod.GetPodLogs(ctx, cs, ns, podClient, podClient)
			framework.ExpectNoError(err)
			framework.Logf("Pod client logs: %s", logs)
			framework.Failf("Failed to connect to backend 2")
		}
	})
})
