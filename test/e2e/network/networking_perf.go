/*
Copyright 2015 The Kubernetes Authors.

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

// 用 iperf2 或其他容器测试网络性能。
import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edaemonset "k8s.io/kubernetes/test/e2e/framework/daemonset"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	e2eendpointslice "k8s.io/kubernetes/test/e2e/framework/endpointslice"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/e2e/network/common"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	// 较大集群使用这个超时
	largeClusterTimeout = 400 * time.Second
	// iperf2BaselineBandwidthMegabytesPerSecond 为 iperf2 带宽设置基线 10 MBps = 80 Mbps
	// 选这个值是为了兼容只有 100 mbps 网卡的小设备。
	iperf2BaselineBandwidthMegabytesPerSecond = 10
	// iperf2Port 为 iperf2 的客户端和服务端选一个任意且唯一的端口
	iperf2Port = 6789
	// labelKey 用作 selector 的 key
	labelKey = "app"
	// clientLabelValue 用作 iperf2 客户端 selector 的值
	clientLabelValue = "iperf2-client"
	// serverLabelValue 用作 iperf2 服务端 selector 的值
	serverLabelValue = "iperf2-server"
	// serverServiceName 定义 iperf2 服务端使用的 service 名称
	serverServiceName = "iperf2-server"
)

func iperf2ServerDeployment(ctx context.Context, client clientset.Interface, namespace string, isIPV6 bool) (*appsv1.Deployment, error) {
	framework.Logf("deploying iperf2 server")
	one := int64(1)
	replicas := int32(1)
	labels := map[string]string{labelKey: serverLabelValue}
	args := []string{
		"-s",
		"-p",
		fmt.Sprintf("%d", iperf2Port),
	}
	if isIPV6 {
		args = append(args, "-V")
	}
	deploymentSpec := e2edeployment.NewDeployment(
		"iperf2-server-deployment", replicas, labels, "iperf2-server",
		imageutils.GetE2EImage(imageutils.Agnhost), appsv1.RollingUpdateDeploymentStrategyType)
	deploymentSpec.Spec.Template.Spec.TerminationGracePeriodSeconds = &one
	deploymentSpec.Spec.Template.Spec.Containers[0].Command = []string{"iperf"}
	deploymentSpec.Spec.Template.Spec.Containers[0].Args = args
	deploymentSpec.Spec.Template.Spec.Containers[0].Ports = []v1.ContainerPort{
		{
			ContainerPort: iperf2Port,
			Protocol:      v1.ProtocolTCP,
		},
	}

	deployment, err := client.AppsV1().Deployments(namespace).Create(ctx, deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("deployment %q Create API error: %w", deploymentSpec.Name, err)
	}
	framework.Logf("Waiting for deployment %q to complete", deploymentSpec.Name)
	err = e2edeployment.WaitForDeploymentComplete(client, deployment)
	if err != nil {
		return nil, fmt.Errorf("deployment %q failed to complete: %w", deploymentSpec.Name, err)
	}

	return deployment, nil
}

func iperf2ServerService(ctx context.Context, client clientset.Interface, namespace string) (*v1.Service, error) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: serverServiceName},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				labelKey: serverLabelValue,
			},
			Ports: []v1.ServicePort{
				{Protocol: v1.ProtocolTCP, Port: iperf2Port},
			},
		},
	}
	return client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
}

func iperf2ClientDaemonSet(ctx context.Context, client clientset.Interface, namespace string) (*appsv1.DaemonSet, error) {
	one := int64(1)
	labels := map[string]string{labelKey: clientLabelValue}
	spec := e2edaemonset.NewDaemonSet("iperf2-clients", imageutils.GetE2EImage(imageutils.Agnhost), labels, nil, nil, nil)
	spec.Spec.Template.Spec.TerminationGracePeriodSeconds = &one

	ds, err := client.AppsV1().DaemonSets(namespace).Create(ctx, spec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("daemonset %s Create API error: %w", spec.Name, err)
	}
	return ds, nil
}

// 测试摘要：
//
//	本测试用 iperf2 获取集群节点间的带宽数据，粗略衡量
//	集群网络的健康状况。测试运行两组 pod：
//	  1. 单个节点上的 iperf2 server
//	  2. 一组 iperf2 client 的 daemonset
//	测试逐个遍历 client，从每个 client 运行 iperf2，向 server 传输
//	数据十秒再传回，然后收集并解析结果。
//	因此，如果集群有 10 个节点，就执行 10 轮测试。
//	  注意：更完整的测试可以也用 daemonset 部署 server；但那样
//	  需要 n^2 次测试、n^2 的时间和 n^2 的网络资源，随着集群增大
//	  很快变得难以承受。
//	最后，收集完所有数据后，对结果进行分析和制表。
var _ = common.SIGDescribe("Networking IPerf2", feature.NetworkingPerformance, func() {
	// 本测试运行 iperf2：一个 pod 作为 server，一组 client 的 daemonset
	f := framework.NewDefaultFramework("network-perf")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	ginkgo.It("should run iperf2", func(ctx context.Context) {
		readySchedulableNodes, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		familyStr := ""
		if framework.TestContext.ClusterIsIPv6() {
			familyStr = "-V "
		}

		serverPodsListOptions := metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", labelKey, serverLabelValue),
		}

		// 第 1 步：搭建 iperf2 server —— 任意节点上的一个 pod
		_, err = iperf2ServerDeployment(ctx, f.ClientSet, f.Namespace.Name, framework.TestContext.ClusterIsIPv6())
		framework.ExpectNoError(err, "deploy iperf2 server deployment")

		_, err = iperf2ServerService(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err, "deploy iperf2 server service")

		// 第 2 步：搭建 iperf2 client daemonset
		// 起初 client 什么都不做，只是暂停等待被调用
		_, err = iperf2ClientDaemonSet(ctx, f.ClientSet, f.Namespace.Name)
		framework.ExpectNoError(err, "deploy iperf2 client daemonset")

		// 确保 server 准备好了。（我们用 WaitForEndpointSlices
		// 而不是更简单的 WaitForEndpointCount，因为想使用
		// largeClusterTimeout。）
		framework.Logf("waiting for iperf2 server endpoints")
		err = e2eendpointslice.WaitForEndpointSlices(ctx, f.ClientSet, f.Namespace.Name, serverServiceName, 2*time.Second, largeClusterTimeout, func(ctx context.Context, endpointSlices []discoveryv1.EndpointSlice) (bool, error) {
			if len(endpointSlices) == 0 {
				framework.Logf("EndpointSlice for Service %s/%s not found", f.Namespace.Name, serverServiceName)
				return false, nil
			}
			return true, nil
		})
		framework.ExpectNoError(err, "unable to wait for endpoints for the iperf service")
		framework.Logf("found iperf2 server endpoints")

		clientPodsListOptions := metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", labelKey, clientLabelValue),
		}

		framework.Logf("waiting for client pods to be running")
		var clientPodList *v1.PodList
		err = wait.Poll(2*time.Second, largeClusterTimeout, func() (done bool, err error) {
			clientPodList, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(ctx, clientPodsListOptions)
			if err != nil {
				return false, err
			}
			if len(clientPodList.Items) < len(readySchedulableNodes.Items) {
				return false, nil
			}
			for _, pod := range clientPodList.Items {
				if pod.Status.Phase != v1.PodRunning {
					return false, nil
				}
			}
			return true, nil
		})
		framework.ExpectNoError(err, "unable to wait for client pods to come up")
		framework.Logf("all client pods are ready: %d pods", len(clientPodList.Items))

		// 保存 server pod 的引用，后面要用
		serverPodList, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(ctx, serverPodsListOptions)
		framework.ExpectNoError(err)
		if len(serverPodList.Items) != 1 {
			framework.Failf("expected 1 server pod, found %d", len(serverPodList.Items))
		}
		serverPod := serverPodList.Items[0]
		framework.Logf("server pod phase %s", serverPod.Status.Phase)
		for i, condition := range serverPod.Status.Conditions {
			framework.Logf("server pod condition %d: %+v", i, condition)
		}
		for i, cont := range serverPod.Status.ContainerStatuses {
			framework.Logf("server pod container status %d: %+v", i, cont)
		}

		framework.Logf("found %d matching client pods", len(clientPodList.Items))

		nodeResults := &IPerf2NodeToNodeCSVResults{
			ServerNode: serverPod.Spec.NodeName,
			Results:    map[string]*IPerf2EnhancedCSVResults{},
		}

		// 第 3 步：逐个遍历 client pod，以 client 模式运行 iperf2，向 server
		// 传输数据再传回并测量带宽
		for _, pod := range clientPodList.Items {
			podName := pod.Name
			nodeName := pod.Spec.NodeName

			iperfVersion := e2epod.ExecShellInPod(ctx, f, podName, "iperf -v || true")
			framework.Logf("iperf version: %s", iperfVersion)

			for try := 0; ; try++ {
				/* iperf2 command parameters:
				 *  -e: use enhanced reporting giving more tcp/udp and traffic information
				 *  -p %d: server port to connect to
				 *  --reportstyle C: report as Comma-Separated Values
				 *  -i 1: seconds between periodic bandwidth reports
				 *  -c %s: run in client mode, connecting to <host>
				 */
				command := fmt.Sprintf(`iperf %s -e -p %d --reportstyle C -i 1 -c %s && sleep 5`, familyStr, iperf2Port, serverServiceName)
				framework.Logf("attempting to run command '%s' in client pod %s (node %s)", command, podName, nodeName)
				output := e2epod.ExecShellInPod(ctx, f, podName, command)
				framework.Logf("output from exec on client pod %s (node %s): \n%s\n", podName, nodeName, output)

				results, err := ParseIPerf2EnhancedResultsFromCSV(output)
				if err == nil {
					nodeResults.Results[nodeName] = results
					break
				} else if try == 2 {
					framework.ExpectNoError(err, "unable to parse iperf2 output from client pod %s (node %s)", pod.Name, nodeName)
				} else {
					framework.Logf("Retrying: IPerf run failed: %+v", err)
				}
			}
		}

		// 第 4 步：收集完所有 client<->server 数据后，整理并展示结果
		/*
						Example output:

			Dec 22 07:52:41.102: INFO:                                From                                 To    Bandwidth (MB/s)
			Dec 22 07:52:41.102: INFO:              three-node-ipv6-worker            three-node-ipv6-worker2                2381
			Dec 22 07:52:41.102: INFO:             three-node-ipv6-worker2            three-node-ipv6-worker2                2214
			Dec 22 07:52:41.102: INFO:             three-node-ipv6-worker3            three-node-ipv6-worker2                3123

		*/
		framework.Logf("%35s%35s%20s", "From", "To", "Bandwidth (MB/s)")
		for nodeFrom, results := range nodeResults.Results {
			framework.Logf("%35s%35s%20d", nodeFrom, nodeResults.ServerNode, results.Total.bandwidthMB())
		}
		for clientNode, results := range nodeResults.Results {
			megabytesPerSecond := results.Total.bandwidthMB()
			if megabytesPerSecond < iperf2BaselineBandwidthMegabytesPerSecond {
				framework.Failf("iperf2 MB/s received below baseline of %d for client %s to server %s: %d", iperf2BaselineBandwidthMegabytesPerSecond, clientNode, nodeResults.ServerNode, megabytesPerSecond)
			}
		}
	})
})
