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

package netpol

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	netutils "k8s.io/utils/net"
	"net"
	"strconv"
	"strings"
	"time"
)

// defaultPollIntervalSeconds [秒] 是 Prober 在下次尝试前等待的默认时长。
const defaultPollIntervalSeconds = 1

// defaultPollTimeoutSeconds [秒] 是探测轮询的默认超时时间。
const defaultPollTimeoutSeconds = 10

// TestPod 表示实际运行中的 pod。model 中定义的每个 Pod 都有一个对应的 TestPod。
// TestPod 包含 model 中没有的一些运行时信息（namespace 名称、service IP）。
type TestPod struct {
	Namespace string
	Name      string
	ServiceIP string
}

func (pod TestPod) PodString() PodString {
	return NewPodString(pod.Namespace, pod.Name)
}

// kubeManager 对轮询 NetworkPolicy 连通性用到的 kube 功能做了便捷封装。
// 职责包括：
//   - 创建资源（pod、deployment、namespace、service、network policy）
//   - 修改和清理资源
type kubeManager struct {
	framework      *framework.Framework
	clientSet      clientset.Interface
	namespaceNames []string
	allPods        []TestPod
	allPodStrings  []PodString
	dnsDomain      string
}

// newKubeManager 是创建 kubeManager 实例的便捷函数。
func newKubeManager(framework *framework.Framework, dnsDomain string) *kubeManager {
	return &kubeManager{
		framework: framework,
		clientSet: framework.ClientSet,
		dnsDomain: dnsDomain,
	}
}

// initializeCluster 初始化集群，按需创建 namespace、pod 和 service。
func (k *kubeManager) initializeClusterFromModel(ctx context.Context, model *Model) error {
	var createdPods []*v1.Pod
	for _, ns := range model.Namespaces {
		// 不需要额外 label，默认的 kubernetes.io/metadata.name 就够了
		namespace, err := k.framework.CreateNamespace(ctx, ns.Name, nil)
		if err != nil {
			return err
		}
		namespaceName := namespace.Name
		k.namespaceNames = append(k.namespaceNames, namespaceName)

		for _, pod := range ns.Pods {
			framework.Logf("creating pod %s/%s with matching service", namespaceName, pod.Name)

			// pod 的细节逻辑（如 node selector）交给 model 处理，
			// 因为它清楚 pod 是 Linux 还是 Windows
			kubePod, err := k.createPod(ctx, pod.KubePod(namespaceName))
			if err != nil {
				return err
			}

			createdPods = append(createdPods, kubePod)
			svc, err := k.createService(ctx, pod.Service(namespaceName))
			if err != nil {
				return err
			}
			if netutils.ParseIPSloppy(svc.Spec.ClusterIP) == nil {
				return fmt.Errorf("empty IP address found for service %s/%s", svc.Namespace, svc.Name)
			}

			k.allPods = append(k.allPods, TestPod{
				Namespace: kubePod.Namespace,
				Name:      kubePod.Name,
				ServiceIP: svc.Spec.ClusterIP,
			})
			k.allPodStrings = append(k.allPodStrings, NewPodString(kubePod.Namespace, kubePod.Name))
		}
	}

	for _, createdPod := range createdPods {
		err := e2epod.WaitForPodRunningInNamespace(ctx, k.clientSet, createdPod)
		if err != nil {
			return fmt.Errorf("unable to wait for pod %s/%s: %w", createdPod.Namespace, createdPod.Name, err)
		}
	}

	return nil
}

func (k *kubeManager) AllPods() []TestPod {
	return k.allPods
}

func (k *kubeManager) AllPodStrings() []PodString {
	return k.allPodStrings
}

func (k *kubeManager) DNSDomain() string {
	return k.dnsDomain
}

func (k *kubeManager) NamespaceNames() []string {
	return k.namespaceNames
}

// getPod 根据 namespace 和名称获取 pod。
func (k *kubeManager) getPod(ctx context.Context, ns string, name string) (*v1.Pod, error) {
	kubePod, err := k.clientSet.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to get pod %s/%s: %w", ns, name, err)
	}
	return kubePod, nil
}

// probeConnectivity 进入 pod 执行探测，检查它与另一个 pod 的连通性。
// 实现了 Prober 接口。
func (k *kubeManager) probeConnectivity(args *probeConnectivityArgs) (bool, string, error) {
	port := strconv.Itoa(args.toPort)
	if args.addrTo == "" {
		return false, "no IP provided", fmt.Errorf("empty addrTo field")
	}
	framework.Logf("Starting probe from pod %v to %v", args.podFrom, args.addrTo)
	var cmd []string
	timeout := fmt.Sprintf("--timeout=%vs", args.timeoutSeconds)

	switch args.protocol {
	case v1.ProtocolSCTP:
		cmd = []string{"/agnhost", "connect", net.JoinHostPort(args.addrTo, port), timeout, "--protocol=sctp"}
	case v1.ProtocolTCP:
		cmd = []string{"/agnhost", "connect", net.JoinHostPort(args.addrTo, port), timeout, "--protocol=tcp"}
	case v1.ProtocolUDP:
		cmd = []string{"/agnhost", "connect", net.JoinHostPort(args.addrTo, port), timeout, "--protocol=udp"}
		if framework.NodeOSDistroIs("windows") {
			framework.Logf("probing UDP for windows may result in cluster instability for certain windows nodes with low CPU/Memory, depending on CRI version")
		}
	default:
		framework.Failf("protocol %s not supported", args.protocol)
	}

	commandDebugString := fmt.Sprintf("kubectl exec %s -n %s -- %s", args.podFrom, args.nsFrom, strings.Join(cmd, " "))

	attempt := 0

	// NOTE: probeConnectivity 的返回值在探测成功时应为 true，否则为 false。

	// probeError 将作为本次 probeConnectivity 调用的返回值。
	var probeError error
	var stderr string

	// 传给 PollImmediate 的 conditionFunc 不会在连通性失败时重跑任务，
	// 而是在观测值与预期不符时重跑，所以我们不依赖 PollImmediate 的返回值，
	// 而是直接使用定义在 conditionFunc 作用域外的 probeError 作为 probeConnectivity 的结果。
	conditionFunc := func() (bool, error) {
		_, stderr, probeError = k.executeRemoteCommand(args.nsFrom, args.podFrom, cmd)
		// 只有在观测值与预期不符时才重试。
		if args.expectConnectivity {
			if probeError != nil {
				// 这里预期有连通性，所以让 PollImmediate 的条件失败以重试探测。
				// 这种情况可能发生在网络拥塞时：并没有策略拒绝 "podFrom" 到 "podTo" 的流量，
				// 但探测却失败了。
				framework.Logf("probe #%d :: connectivity expected :: %s/%s -> %s :: stderr - %s",
					attempt+1, args.nsFrom, args.podFrom, args.addrTo, stderr,
				)
				attempt++
				return false, nil
			} else {
				// 结果符合预期，立即退出。
				return true, nil
			}
		} else {
			if probeError != nil {
				// 结果符合预期，立即退出。
				return true, nil
			} else {
				// 这里预期没有连通性，所以让 PollImmediate 的条件失败以重试探测。
				// 这种情况可能发生在已有策略拒绝 "podFrom" 到 "podTo" 的流量，
				// 但 CNI 需要时间落实策略，在该时间窗口内探测仍然成功。
				framework.Logf(" probe #%d :: connectivity not expected :: %s/%s -> %s",
					attempt+1, args.nsFrom, args.podFrom, args.addrTo,
				)
				attempt++
				return false, nil
			}
		}
	}

	// 忽略 PollImmediate 的返回值，我们只关心 probeError。
	_ = wait.PollImmediate(
		time.Duration(args.pollIntervalSeconds)*time.Second,
		time.Duration(args.pollTimeoutSeconds)*time.Second,
		conditionFunc,
	)

	if probeError != nil {
		return false, commandDebugString, nil
	}
	return true, commandDebugString, nil
}

// executeRemoteCommand 在指定 pod 上执行远程 shell 命令。
func (k *kubeManager) executeRemoteCommand(namespace string, pod string, command []string) (string, string, error) {
	return e2epod.Exec(k.framework.TContext(context.Background()), e2epod.ExecOptions{
		Command:            command,
		Namespace:          namespace,
		PodName:            pod,
		Stdin:              nil,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: false,
	})
}

// createService 是创建 service 的便捷函数。
func (k *kubeManager) createService(ctx context.Context, service *v1.Service) (*v1.Service, error) {
	ns := service.Namespace
	name := service.Name

	createdService, err := k.clientSet.CoreV1().Services(ns).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to create service %s/%s: %w", ns, name, err)
	}
	return createdService, nil
}

// createPod 是创建 pod 的便捷函数。
func (k *kubeManager) createPod(ctx context.Context, pod *v1.Pod) (*v1.Pod, error) {
	ns := pod.Namespace
	framework.Logf("creating pod %s/%s", ns, pod.Name)

	createdPod, err := k.clientSet.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to create pod %s/%s: %w", ns, pod.Name, err)
	}
	return createdPod, nil
}

// cleanNetworkPolicies 是便捷函数，在新测试开始前删除所有 network policy。
func (k *kubeManager) cleanNetworkPolicies(ctx context.Context) error {
	for _, ns := range k.namespaceNames {
		framework.Logf("deleting policies in %s ..........", ns)
		l, err := k.clientSet.NetworkingV1().NetworkPolicies(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("unable to list network policies in ns %s: %w", ns, err)
		}
		for _, np := range l.Items {
			framework.Logf("deleting network policy %s/%s", ns, np.Name)
			err = k.clientSet.NetworkingV1().NetworkPolicies(ns).Delete(ctx, np.Name, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("unable to delete network policy %s/%s: %w", ns, np.Name, err)
			}
		}
	}
	return nil
}

// createNetworkPolicy 是创建 network policy 的便捷函数。
func (k *kubeManager) createNetworkPolicy(ctx context.Context, ns string, netpol *networkingv1.NetworkPolicy) (*networkingv1.NetworkPolicy, error) {
	framework.Logf("creating network policy %s/%s", ns, netpol.Name)
	netpol.ObjectMeta.Namespace = ns
	np, err := k.clientSet.NetworkingV1().NetworkPolicies(ns).Create(ctx, netpol, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to create network policy %s/%s: %w", ns, netpol.Name, err)
	}
	return np, nil
}

// updateNetworkPolicy 是更新 network policy 的便捷函数。
func (k *kubeManager) updateNetworkPolicy(ctx context.Context, ns string, netpol *networkingv1.NetworkPolicy) (*networkingv1.NetworkPolicy, error) {
	framework.Logf("updating network policy %s/%s", ns, netpol.Name)
	netpol.ObjectMeta.Namespace = ns
	np, err := k.clientSet.NetworkingV1().NetworkPolicies(ns).Update(ctx, netpol, metav1.UpdateOptions{})
	if err != nil {
		return np, fmt.Errorf("unable to update network policy %s/%s: %w", ns, netpol.Name, err)
	}
	return np, nil
}

// getNamespace 从 kubernetes 获取 namespace 对象。
func (k *kubeManager) getNamespace(ctx context.Context, ns string) (*v1.Namespace, error) {
	selectedNameSpace, err := k.clientSet.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to get namespace %s: %w", ns, err)
	}
	return selectedNameSpace, nil
}

// getProbeTimeoutSeconds 返回探测在判定失败前的超时时长。3 秒超时既保证了
// 探测在不同平台上的可靠性，也隐含断言了 dataplane 必须在可接受的延迟内
// 下发 network policy。对于网络策略功能来说这一点尤其关键，因为下发过慢
// 可能影响连通性和安全性。3 秒阈值是根据经验数据确定的、可接受的最大
// 策略生效延迟。
func getProbeTimeoutSeconds() int {
	return 3
}

// getWorkers 返回测试时建议运行的 worker 数量。
func getWorkers() int {
	return 3
}

// getPollInterval 返回 Prober 在下次尝试前等待的时长。
func getPollIntervalSeconds() int {
	return defaultPollIntervalSeconds
}

// getPollTimeout 返回探测轮询的超时时间，并针对 Windows 做了经验性调整，因为那里的请求有时会更慢。
func getPollTimeoutSeconds() int {
	if framework.NodeOSDistroIs("windows") {
		return defaultPollTimeoutSeconds * 2
	}
	return defaultPollTimeoutSeconds
}
