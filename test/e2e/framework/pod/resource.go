/*
Copyright 2019 The Kubernetes Authors.

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

package pod

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e/framework"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// LabelLogOnPodFailure 用于标记在测试失败时
// 哪些 Pod 会被记录日志。默认情况下，如果没有带此标签的 Pod，只记录前 5 个
// Pod 的日志。
const LabelLogOnPodFailure = "log-on-pod-failure"

// PodsCreatedByLabel 返回与指定标签匹配的已创建 Pod 列表。
func PodsCreatedByLabel(ctx context.Context, c clientset.Interface, ns, name string, replicas int32, label labels.Selector) (*v1.PodList, error) {
	timeout := 2 * time.Minute
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(5 * time.Second) {
		options := metav1.ListOptions{LabelSelector: label.String()}

		// 列出 Pod，确保观察到所有副本。
		pods, err := c.CoreV1().Pods(ns).List(ctx, options)
		if err != nil {
			return nil, err
		}

		created := []v1.Pod{}
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil {
				continue
			}
			created = append(created, pod)
		}
		framework.Logf("Pod name %s: Found %d pods out of %d", name, len(created), replicas)

		if int32(len(created)) == replicas {
			pods.Items = created
			return pods, nil
		}
	}
	return nil, fmt.Errorf("Pod name %s: Gave up waiting %v for %d pods to come up", name, timeout, replicas)
}

// VerifyPods 检查指定 Pod 是否有响应。
func VerifyPods(ctx context.Context, c clientset.Interface, ns, name string, selector labels.Selector, wantName bool, replicas int32) error {
	pods, err := PodsCreatedByLabel(ctx, c, ns, name, replicas, selector)
	if err != nil {
		return err
	}

	return podsRunningMaybeResponding(ctx, c, ns, name, selector, pods, wantName, true)
}

// VerifyPodsRunning 检查指定 Pod 是否正在运行。
func VerifyPodsRunning(ctx context.Context, c clientset.Interface, ns, name string, selector labels.Selector, wantName bool, replicas int32) error {
	pods, err := PodsCreatedByLabel(ctx, c, ns, name, replicas, selector)
	if err != nil {
		return err
	}

	return podsRunningMaybeResponding(ctx, c, ns, name, selector, pods, wantName, false)
}

func podsRunningMaybeResponding(ctx context.Context, c clientset.Interface, ns string, name string, selector labels.Selector, pods *v1.PodList, wantName bool, checkResponding bool) error {
	e := podsRunning(ctx, c, pods)
	if len(e) > 0 {
		return fmt.Errorf("failed to wait for pods running: %v", e)
	}
	if checkResponding {
		return WaitForPodsResponding(ctx, c, ns, name, selector, wantName, podRespondingTimeout, pods)
	}
	return nil
}

func podsRunning(ctx context.Context, c clientset.Interface, pods *v1.PodList) []error {
	// 等待 Pod 进入运行态。循环等待直到 Pod 运行，
	// 验证有服务在监听。
	ginkgo.By("ensuring each pod is running")
	e := []error{}
	errorChan := make(chan error)

	for _, pod := range pods.Items {
		go func(p v1.Pod) {
			errorChan <- WaitForPodRunningInNamespace(ctx, c, &p)
		}(pod)
	}

	for range pods.Items {
		err := <-errorChan
		if err != nil {
			e = append(e, err)
		}
	}

	return e
}

// LogPodStates 记录给定 Pod 的基本信息以便调试。
func LogPodStates(pods []v1.Pod) {
	// 找出 Pod、节点和 phase 字符串的最大宽度，用于列打印。
	maxPodW, maxNodeW, maxPhaseW, maxGraceW := len("POD"), len("NODE"), len("PHASE"), len("GRACE")
	for i := range pods {
		pod := &pods[i]
		if len(pod.ObjectMeta.Name) > maxPodW {
			maxPodW = len(pod.ObjectMeta.Name)
		}
		if len(pod.Spec.NodeName) > maxNodeW {
			maxNodeW = len(pod.Spec.NodeName)
		}
		if len(pod.Status.Phase) > maxPhaseW {
			maxPhaseW = len(pod.Status.Phase)
		}
	}
	// 宽度加一，用单个空格分隔。
	maxPodW++
	maxNodeW++
	maxPhaseW++
	maxGraceW++

	// 记录 Pod 信息。* 表示空格填充，- 表示左对齐。
	framework.Logf("%-[1]*[2]s %-[3]*[4]s %-[5]*[6]s %-[7]*[8]s %[9]s",
		maxPodW, "POD", maxNodeW, "NODE", maxPhaseW, "PHASE", maxGraceW, "GRACE", "CONDITIONS")
	for _, pod := range pods {
		grace := ""
		if pod.DeletionGracePeriodSeconds != nil {
			grace = fmt.Sprintf("%ds", *pod.DeletionGracePeriodSeconds)
		}
		framework.Logf("%-[1]*[2]s %-[3]*[4]s %-[5]*[6]s %-[7]*[8]s %[9]v",
			maxPodW, pod.ObjectMeta.Name, maxNodeW, pod.Spec.NodeName, maxPhaseW, pod.Status.Phase, maxGraceW, grace, pod.Status.Conditions)
	}
	framework.Logf("") // 最后一个空行有助于阅读。
}

// logPodTerminationMessages 记录失败 Pod 的终止消息。它很短（远小于完整日志），但常常能显示
// Pod 崩溃的原因，而且由于它位于 API 中，获取很快。
func logPodTerminationMessages(pods []v1.Pod) {
	for _, pod := range pods {
		for _, status := range pod.Status.InitContainerStatuses {
			if status.LastTerminationState.Terminated != nil && len(status.LastTerminationState.Terminated.Message) > 0 {
				framework.Logf("%s[%s].initContainer[%s]=%s", pod.Name, pod.Namespace, status.Name, status.LastTerminationState.Terminated.Message)
			}
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.LastTerminationState.Terminated != nil && len(status.LastTerminationState.Terminated.Message) > 0 {
				framework.Logf("%s[%s].container[%s]=%s", pod.Name, pod.Namespace, status.Name, status.LastTerminationState.Terminated.Message)
			}
		}
	}
}

// logPodLogs 记录指定命名空间中 Pod 的容器日志。有助于调试
// 那些不会导致容器失败的问题（例如网络连接问题）
// 我们将记录带有 LabelLogOnPodFailure 标签的 Pod。如果没有，则默认
// 只记录前 5 个 Pod。这需要设置 reportDir，日志写入：
// {report_dir}/pods/{namespace}/{pod}/{container_name}/logs.txt
func logPodLogs(ctx context.Context, c clientset.Interface, namespace string, pods []v1.Pod, reportDir string) {
	if reportDir == "" {
		return
	}

	var logPods []v1.Pod
	for _, pod := range pods {
		if _, ok := pod.Labels[LabelLogOnPodFailure]; ok {
			logPods = append(logPods, pod)
		}
	}
	maxPods := len(logPods)

	// 没有带 LabelLogOnPodFailure 标签的 Pod，默认记录前 5 个。
	if maxPods == 0 {
		logPods = pods
		maxPods = len(pods)
		if maxPods > 5 {
			maxPods = 5
		}
	}

	tailLen := 42
	for i := 0; i < maxPods; i++ {
		pod := logPods[i]
		for _, container := range pod.Spec.Containers {
			logs, err := getPodLogsInternal(ctx, c, namespace, pod.Name, container.Name, false, nil, &tailLen)
			if err != nil {
				framework.Logf("Unable to fetch %s/%s/%s logs: %v", pod.Namespace, pod.Name, container.Name, err)
				continue
			}

			logDir := filepath.Join(reportDir, namespace, pod.Name, container.Name)
			err = os.MkdirAll(logDir, 0755)
			if err != nil {
				framework.Logf("Unable to create path '%s'. Err: %v", logDir, err)
				continue
			}

			logPath := filepath.Join(logDir, "logs.txt")
			err = os.WriteFile(logPath, []byte(logs), 0644)
			if err != nil {
				framework.Logf("Could not write the container logs in: %s. Err: %v", logPath, err)
			}
		}
	}
}

// DumpAllPodInfoForNamespace 记录指定命名空间的所有 Pod 信息。
func DumpAllPodInfoForNamespace(ctx context.Context, c clientset.Interface, namespace, reportDir string) {
	pods, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		framework.Logf("unable to fetch pod debug info: %v", err)
	}
	LogPodStates(pods.Items)
	logPodTerminationMessages(pods.Items)
	logPodLogs(ctx, c, namespace, pods.Items, reportDir)
}

// FilterNonRestartablePods 过滤掉终止后删除时
// 永远不会被重建的 Pod。
func FilterNonRestartablePods(pods []*v1.Pod) []*v1.Pod {
	var results []*v1.Pod
	for _, p := range pods {
		if isNotRestartAlwaysMirrorPod(p) {
			// restart policy 为 Never 的 mirror Pod 在终止后被删除时
			// 不会被重建。
			// 目前先不考虑这类 Pod。
			// https://github.com/kubernetes/kubernetes/issues/34003
			continue
		}
		results = append(results, p)
	}
	return results
}

func isNotRestartAlwaysMirrorPod(p *v1.Pod) bool {
	// 检查 Pod 是否为 mirror pod
	if _, ok := p.Annotations[v1.MirrorPodAnnotationKey]; !ok {
		return false
	}
	return p.Spec.RestartPolicy != v1.RestartPolicyAlways
}

// NewAgnhostPod 返回一个使用 agnhost 镜像的 Pod。该镜像的二进制支持多种子命令，
// 无论底层 OS 如何，行为都一致。如果未提供参数，默认使用 pause 子命令。
// 有关 agnhost 子命令的详细信息，参见：https://github.com/kubernetes/kubernetes/tree/master/test/images/agnhost#agnhost
func NewAgnhostPod(ns, podName string, volumes []v1.Volume, mounts []v1.VolumeMount, ports []v1.ContainerPort, args ...string) *v1.Pod {
	immediate := int64(0)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				NewAgnhostContainer("agnhost-container", mounts, ports, args...),
			},
			Volumes:                       volumes,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &immediate,
		},
	}
	return pod
}

func NewAgnhostPodFromContainers(ns, podName string, volumes []v1.Volume, containers ...v1.Container) *v1.Pod {
	immediate := int64(0)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
		},
		Spec: v1.PodSpec{
			Containers:                    containers[:],
			Volumes:                       volumes,
			SecurityContext:               &v1.PodSecurityContext{},
			TerminationGracePeriodSeconds: &immediate,
		},
	}
	return pod
}

// NewAgnhostContainer 返回 agnhost 容器的容器 Spec。
func NewAgnhostContainer(containerName string, mounts []v1.VolumeMount, ports []v1.ContainerPort, args ...string) v1.Container {
	if len(args) == 0 {
		args = []string{"pause"}
	}
	return v1.Container{
		Name:            containerName,
		Image:           imageutils.GetE2EImage(imageutils.Agnhost),
		Args:            args,
		VolumeMounts:    mounts,
		Ports:           ports,
		SecurityContext: &v1.SecurityContext{},
		ImagePullPolicy: v1.PullIfNotPresent,
	}
}

// NewExecPodSpec 返回 hostexec Pod 的 Pod spec
func NewExecPodSpec(ns, name string, hostNetwork bool) *v1.Pod {
	pod := NewAgnhostPod(ns, name, nil, nil, nil)
	pod.Spec.HostNetwork = hostNetwork
	return pod
}

// newExecPodSpec 返回 exec Pod 的 Pod spec
func newExecPodSpec(ns, generateName string) *v1.Pod {
	// GenerateName 是可选前缀，由服务器使用，
	// 仅在未提供 Name 字段时生成唯一名称
	pod := NewAgnhostPod(ns, "", nil, nil, nil)
	pod.ObjectMeta.GenerateName = generateName
	return pod
}

// CreateExecPodOrFail 创建一个 agnhost pause Pod，用作 kubectl exec 命令的载体。
// Pod 名称是唯一生成的。
func CreateExecPodOrFail(ctx context.Context, client clientset.Interface, ns, generateName string, tweak func(*v1.Pod)) *v1.Pod {
	framework.Logf("Creating new exec pod")
	pod := newExecPodSpec(ns, generateName)
	if tweak != nil {
		tweak(pod)
	}
	execPod, err := client.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoErrorWithOffset(1, err, "failed to create new exec pod in namespace: %s", ns)
	err = WaitForPodNameRunningInNamespace(ctx, client, execPod.Name, execPod.Namespace)
	framework.ExpectNoErrorWithOffset(1, err, "failed to create new exec pod in namespace: %s", ns)
	return execPod
}

// WithWindowsHostProcess 将 Pod 的 Windows HostProcess 选项设为 true。设置该选项后，
// 可以启用 HostNetwork。
// 以 HostProcess 运行的容器需要设置特定用户名，否则 Pod
// 无法启动：NT AUTHORITY\SYSTEM、NT AUTHORITY\Local service、NT AUTHORITY\NetworkService。
// 如果给定的用户名为空，将使用 NT AUTHORITY\SYSTEM。
// See: https://kubernetes.io/docs/tasks/configure-pod-container/create-hostprocess-pod/
func WithWindowsHostProcess(pod *v1.Pod, username string) {
	if pod.Spec.SecurityContext == nil {
		pod.Spec.SecurityContext = &v1.PodSecurityContext{}
	}
	if pod.Spec.SecurityContext.WindowsOptions == nil {
		pod.Spec.SecurityContext.WindowsOptions = &v1.WindowsSecurityContextOptions{}
	}

	trueVar := true
	if username == "" {
		username = "NT AUTHORITY\\SYSTEM"
	}
	pod.Spec.SecurityContext.WindowsOptions.HostProcess = &trueVar
	pod.Spec.SecurityContext.WindowsOptions.RunAsUserName = &username
}

// CheckPodsRunningReady 返回命名空间 ns 中 podNames 列出的所有 Pod
// 是否都在运行且就绪，使用 c 并最多等待 timeout。
// timeout.
func CheckPodsRunningReady(ctx context.Context, c clientset.Interface, ns string, podNames []string, timeout time.Duration) bool {
	return checkPodsCondition(ctx, c, ns, podNames, timeout, testutils.PodRunningReady, "running and ready")
}

// CheckPodsRunningReadyOrSucceeded 返回命名空间 ns 中 podNames 列出的所有 Pod
// 是否都在运行且就绪或已成功；使用 c 并最多等待 timeout。
// 使用 c 并最多等待 timeout。
func CheckPodsRunningReadyOrSucceeded(ctx context.Context, c clientset.Interface, ns string, podNames []string, timeout time.Duration) bool {
	return checkPodsCondition(ctx, c, ns, podNames, timeout, testutils.PodRunningReadyOrSucceeded, "running and ready, or succeeded")
}

// checkPodsCondition 返回命名空间 ns 中 podNames 列出的所有 Pod
// 是否都处于指定 condition，使用 c 并最多等待 timeout。
func checkPodsCondition(ctx context.Context, c clientset.Interface, ns string, podNames []string, timeout time.Duration, condition podCondition, desc string) bool {
	np := len(podNames)
	framework.Logf("Waiting up to %v for %d pods to be %s: %s", timeout, np, desc, podNames)
	type waitPodResult struct {
		success bool
		podName string
	}
	result := make(chan waitPodResult, len(podNames))
	for _, podName := range podNames {
		// 启动 Pod 就绪检查器。
		go func(name string) {
			err := WaitForPodCondition(ctx, c, ns, name, desc, timeout, condition)
			result <- waitPodResult{err == nil, name}
		}(podName)
	}
	// 等待它们全部完成。
	success := true
	for range podNames {
		res := <-result
		if !res.success {
			framework.Logf("Pod %[1]s failed to be %[2]s.", res.podName, desc)
			success = false
		}
	}
	framework.Logf("Wanted all %d pods to be %s. Result: %t. Pods: %v", np, desc, success, podNames)
	return success
}

// GetPodLogs 返回指定容器的日志（namespace/pod/container）。
func GetPodLogs(ctx context.Context, c clientset.Interface, namespace, podName, containerName string) (string, error) {
	return getPodLogsInternal(ctx, c, namespace, podName, containerName, false, nil, nil)
}

// GetPodLogsSince 返回指定容器自某个时间戳以来的日志（namespace/pod/container）。
func GetPodLogsSince(ctx context.Context, c clientset.Interface, namespace, podName, containerName string, since time.Time) (string, error) {
	sinceTime := metav1.NewTime(since)
	return getPodLogsInternal(ctx, c, namespace, podName, containerName, false, &sinceTime, nil)
}

// GetPreviousPodLogs 返回指定容器上一个实例的日志
// 指定容器的上一个实例（namespace/pod/container）。
func GetPreviousPodLogs(ctx context.Context, c clientset.Interface, namespace, podName, containerName string) (string, error) {
	return getPodLogsInternal(ctx, c, namespace, podName, containerName, true, nil, nil)
}

// gomega Eventually 的辅助函数
func getPodLogsInternal(ctx context.Context, c clientset.Interface, namespace, podName, containerName string, previous bool, sinceTime *metav1.Time, tailLines *int) (string, error) {
	request := c.CoreV1().RESTClient().Get().
		Resource("pods").
		Namespace(namespace).
		Name(podName).SubResource("log").
		Param("container", containerName).
		Param("previous", strconv.FormatBool(previous))
	if sinceTime != nil {
		request.Param("sinceTime", sinceTime.Format(time.RFC3339))
	}
	if tailLines != nil {
		request.Param("tailLines", strconv.Itoa(*tailLines))
	}
	logs, err := request.Do(ctx).Raw()
	if err != nil {
		return "", err
	}
	if strings.Contains(string(logs), "Internal Error") {
		return "", fmt.Errorf("Fetched log contains \"Internal Error\": %q", string(logs))
	}
	return string(logs), err
}

// GetPodsInNamespace 返回指定命名空间中的 Pod。
func GetPodsInNamespace(ctx context.Context, c clientset.Interface, ns string, ignoreLabels map[string]string) ([]*v1.Pod, error) {
	pods, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return []*v1.Pod{}, err
	}
	ignoreSelector := labels.SelectorFromSet(ignoreLabels)
	var filtered []*v1.Pod
	for i := range pods.Items {
		p := pods.Items[i]
		if len(ignoreLabels) != 0 && ignoreSelector.Matches(labels.Set(p.Labels)) {
			continue
		}
		filtered = append(filtered, &p)
	}
	return filtered, nil
}

// GetPods 返回指定命名空间中匹配标签的 Pod
func GetPods(ctx context.Context, c clientset.Interface, ns string, matchLabels map[string]string) ([]v1.Pod, error) {
	label := labels.SelectorFromSet(matchLabels)
	listOpts := metav1.ListOptions{LabelSelector: label.String()}
	pods, err := c.CoreV1().Pods(ns).List(ctx, listOpts)
	if err != nil {
		return []v1.Pod{}, err
	}
	return pods.Items, nil
}

// GetPodSecretUpdateTimeout 返回更新 Pod secret 的超时时长。
func GetPodSecretUpdateTimeout(ctx context.Context, c clientset.Interface) time.Duration {
	// 使用 SecretManager（ConfigMapManager）时，Kubelet 将更新投射到 volume 并被容器
	// 感知之前，可能要等待完整的同步周期加上 secret（configmap）的 TTL。
	// volume 并被容器感知。
	// 因此该超时基于默认的 Kubelet 同步周期（1 分钟）加上
	// 基于集群规模的 secret（configmap）最大 TTL，再加一些余量。
	secretTTL, err := getNodeTTLAnnotationValue(ctx, c)
	if err != nil {
		framework.Logf("Couldn't get node TTL annotation (using default value of 0): %v", err)
	}
	podLogTimeout := 240*time.Second + secretTTL
	return podLogTimeout
}

// VerifyPodHasConditionWithType 按类型验证 Pod 是否具有预期的 condition
func VerifyPodHasConditionWithType(ctx context.Context, f *framework.Framework, pod *v1.Pod, cType v1.PodConditionType) {
	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "Failed to get the recent pod object for name: %q", pod.Name)
	if condition := FindPodConditionByType(&pod.Status, cType); condition == nil {
		framework.Failf("pod %q should have the condition: %q, pod status: %v", pod.Name, cType, pod.Status)
	}
}

func getNodeTTLAnnotationValue(ctx context.Context, c clientset.Interface) (time.Duration, error) {
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil || len(nodes.Items) == 0 {
		return time.Duration(0), fmt.Errorf("Couldn't list any nodes to get TTL annotation: %w", err)
	}
	// kubelet 使用的 TTL 存储在 node 对象中，为了计算超时，
	// 我们从第一个节点获取（所有节点应相同）。
	node := &nodes.Items[0]
	if node.Annotations == nil {
		return time.Duration(0), fmt.Errorf("No annotations found on the node")
	}
	value, ok := node.Annotations[v1.ObjectTTLAnnotationKey]
	if !ok {
		return time.Duration(0), fmt.Errorf("No TTL annotation found on the node")
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return time.Duration(0), fmt.Errorf("Cannot convert TTL annotation from %#v to int", *node)
	}
	return time.Duration(intValue) * time.Second, nil
}

// FilterActivePods 返回尚未终止的 Pod。
func FilterActivePods(pods []*v1.Pod) []*v1.Pod {
	var result []*v1.Pod
	for _, p := range pods {
		if IsPodActive(p) {
			result = append(result, p)
		} else {
			klog.V(4).Infof("Ignoring inactive pod %v/%v in state %v, deletion time %v",
				p.Namespace, p.Name, p.Status.Phase, p.DeletionTimestamp)
		}
	}
	return result
}

// IsPodActive 在 Pod 满足特定条件时返回 true。
func IsPodActive(p *v1.Pod) bool {
	return v1.PodSucceeded != p.Status.Phase &&
		v1.PodFailed != p.Status.Phase &&
		p.DeletionTimestamp == nil
}
