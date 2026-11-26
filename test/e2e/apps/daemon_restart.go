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

package apps

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/cluster/ports"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edebug "k8s.io/kubernetes/test/e2e/framework/debug"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2erc "k8s.io/kubernetes/test/e2e/framework/rc"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

// 本测试主要检查两点：
// 1. 守护进程能在合理时间（10 分钟）内自动重启。
// 2. 稳态下重启时不会采取异常动作。
// - controller manager 不应让副本数超调
// - kubelet 不应重启容器
// - scheduler 应继续为新 pod 分配节点

const (
	restartPollInterval = 5 * time.Second
	restartTimeout      = 10 * time.Minute
	numPods             = 10
	// ADD 表示 ADD 事件
	ADD = "ADD"
	// DEL 表示 DEL 事件
	DEL = "DEL"
	// UPDATE 表示 UPDATE 事件
	UPDATE = "UPDATE"
)

// RestartDaemonConfig 用于在节点上重启运行中的守护进程并等待
// 其恢复。它通过 ssh 发送 SIGTERM。
type RestartDaemonConfig struct {
	nodeName     string
	daemonName   string
	healthzPort  int
	pollInterval time.Duration
	pollTimeout  time.Duration
	enableHTTPS  bool
}

// NewRestartConfig 为给定节点和守护进程创建 RestartDaemonConfig。
func NewRestartConfig(nodeName, daemonName string, healthzPort int, pollInterval, pollTimeout time.Duration, enableHTTPS bool) *RestartDaemonConfig {
	if !framework.ProviderIs("gce") {
		framework.Logf("WARNING: SSH through the restart config might not work on %s", framework.TestContext.Provider)
	}
	return &RestartDaemonConfig{
		nodeName:     nodeName,
		daemonName:   daemonName,
		healthzPort:  healthzPort,
		pollInterval: pollInterval,
		pollTimeout:  pollTimeout,
		enableHTTPS:  enableHTTPS,
	}
}

func (r *RestartDaemonConfig) String() string {
	return fmt.Sprintf("Daemon %v on node %v", r.daemonName, r.nodeName)
}

// waitUp 轮询守护进程的 healthz，直到返回 "ok" 或达到 pollTimeout
func (r *RestartDaemonConfig) waitUp(ctx context.Context) {
	framework.Logf("Checking if %v is up by polling for a 200 on its /healthz endpoint", r)
	nullDev := "/dev/null"
	if framework.NodeOSDistroIs("windows") {
		nullDev = "NUL"
	}
	var healthzCheck string
	if r.enableHTTPS {
		healthzCheck = fmt.Sprintf(
			"curl -sk -o %v -I -w \"%%{http_code}\" https://localhost:%v/healthz", nullDev, r.healthzPort)
	} else {
		healthzCheck = fmt.Sprintf(
			"curl -s -o %v -I -w \"%%{http_code}\" http://localhost:%v/healthz", nullDev, r.healthzPort)

	}

	err := wait.PollUntilContextTimeout(ctx, r.pollInterval, r.pollTimeout, false, func(ctx context.Context) (bool, error) {

		result, err := e2essh.NodeExec(ctx, r.nodeName, healthzCheck, framework.TestContext.Provider)
		if err != nil {
			return false, err
		}
		e2essh.LogResult(result)
		if result.Code == 0 {
			httpCode, err := strconv.Atoi(result.Stdout)
			if err != nil {
				framework.Logf("Unable to parse healthz http return code: %v", err)
			} else if httpCode == 200 {
				return true, nil
			}
		}
		framework.Logf("node %v exec command, '%v' failed with exitcode %v: \n\tstdout: %v\n\tstderr: %v",
			r.nodeName, healthzCheck, result.Code, result.Stdout, result.Stderr)
		return false, nil
	})
	framework.ExpectNoError(err, "%v did not respond with a 200 via %v within %v", r, healthzCheck, r.pollTimeout)
}

// kill 向守护进程发送 SIGTERM
func (r *RestartDaemonConfig) kill(ctx context.Context) {
	killCmd := fmt.Sprintf("pgrep %v | xargs -I {} sudo kill {}", r.daemonName)
	if framework.NodeOSDistroIs("windows") {
		killCmd = fmt.Sprintf("taskkill /im %v.exe /f", r.daemonName)
	}
	framework.Logf("Killing %v", r)
	_, err := e2essh.NodeExec(ctx, r.nodeName, killCmd, framework.TestContext.Provider)
	framework.ExpectNoError(err)
}

// Restart 检查守护进程是否运行，杀掉它，再等待其恢复
func (r *RestartDaemonConfig) restart(ctx context.Context) {
	r.waitUp(ctx)
	r.kill(ctx)
	r.waitUp(ctx)
}

// podTracker 按时间顺序记录可能影响 pod 的事件。
type podTracker struct {
	cache.ThreadSafeStore
}

func (p *podTracker) remember(pod *v1.Pod, eventType string) {
	if eventType == UPDATE && pod.Status.Phase == v1.PodRunning {
		return
	}
	p.Add(fmt.Sprintf("[%v] %v: %v", time.Now(), eventType, pod.Name), pod)
}

func (p *podTracker) String() (msg string) {
	for _, k := range p.ListKeys() {
		obj, exists := p.Get(k)
		if !exists {
			continue
		}
		pod := obj.(*v1.Pod)
		msg += fmt.Sprintf("%v Phase %v Host %v\n", k, pod.Status.Phase, pod.Spec.NodeName)
	}
	return
}

func newPodTracker() *podTracker {
	return &podTracker{cache.NewThreadSafeStore(
		cache.Indexers{}, cache.Indices{})}
}

// replacePods 用给定 pod 替换 store 中的内容。
func replacePods(pods []*v1.Pod, store cache.Store) {
	found := make([]interface{}, 0, len(pods))
	for i := range pods {
		found = append(found, pods[i])
	}
	framework.ExpectNoError(store.Replace(found, "0"))
}

// getContainerRestarts 返回匹配给定 labelSelector 的所有 pod 中的容器重启次数，
// 以及发生重启的节点名列表。
func getContainerRestarts(ctx context.Context, c clientset.Interface, ns string, labelSelector labels.Selector) (int, []string) {
	options := metav1.ListOptions{LabelSelector: labelSelector.String()}
	pods, err := c.CoreV1().Pods(ns).List(ctx, options)
	framework.ExpectNoError(err)
	failedContainers := 0
	containerRestartNodes := sets.NewString()
	for _, p := range pods.Items {
		for _, v := range testutils.FailedContainers(&p) {
			failedContainers = failedContainers + v.Restarts
			containerRestartNodes.Insert(p.Spec.NodeName)
		}
	}
	return failedContainers, containerRestartNodes.List()
}

var _ = SIGDescribe("DaemonRestart", framework.WithDisruptive(), framework.WithProvider(framework.ProvidersWithSSH...) /* These tests require SSH */, func() {

	f := framework.NewDefaultFramework("daemonrestart")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	rcName := "daemonrestart" + strconv.Itoa(numPods) + "-" + string(uuid.NewUUID())
	labelSelector := labels.Set(map[string]string{"name": rcName}).AsSelector()
	existingPods := cache.NewStore(cache.MetaNamespaceKeyFunc)
	var ns string
	var config testutils.RCConfig
	var controller cache.Controller
	var newPods cache.Store
	var tracker *podTracker

	ginkgo.BeforeEach(func(ctx context.Context) {
		ns = f.Namespace.Name

		// 所有重启测试都需要一个 rc 以及对其 pod 的 watch。
		// 其中一些测试还可能在测试期间扩缩 rc。
		config = testutils.RCConfig{
			Client:      f.ClientSet,
			Name:        rcName,
			Namespace:   ns,
			Image:       imageutils.GetPauseImageName(),
			Replicas:    numPods,
			CreatedPods: &[]*v1.Pod{},
		}
		framework.ExpectNoError(e2erc.RunRC(ctx, config))
		replacePods(*config.CreatedPods, existingPods)

		// 以下代码在 BeforeEach 之后继续运行，因此
		// 不能使用 ctx。
		backgroundCtx, cancel := context.WithCancel(context.Background())
		ginkgo.DeferCleanup(cancel)
		tracker = newPodTracker()
		newPods, controller = cache.NewInformer(
			&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					options.LabelSelector = labelSelector.String()
					obj, err := f.ClientSet.CoreV1().Pods(ns).List(backgroundCtx, options)
					return runtime.Object(obj), err
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					options.LabelSelector = labelSelector.String()
					return f.ClientSet.CoreV1().Pods(ns).Watch(backgroundCtx, options)
				},
			},
			&v1.Pod{},
			0,
			cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					tracker.remember(obj.(*v1.Pod), ADD)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					tracker.remember(newObj.(*v1.Pod), UPDATE)
				},
				DeleteFunc: func(obj interface{}) {
					tracker.remember(obj.(*v1.Pod), DEL)
				},
			},
		)
		go controller.Run(backgroundCtx.Done())
	})

	f.It("Controller Manager should not create/delete replicas across restart", f.WithProvider("gce", "aws") /* Requires master ssh access. */, func(ctx context.Context) {

		nodes := framework.GetControlPlaneNodes(ctx, f.ClientSet)

		// 检查是否至少有一个 control-plane 节点
		gomega.Expect(nodes.Items).NotTo(gomega.BeEmpty(), "at least one node with label %s should exist.", framework.ControlPlaneLabel)

		for i := range nodes.Items {

			ips := framework.GetNodeExternalIPs(&nodes.Items[i])
			gomega.Expect(ips).NotTo(gomega.BeEmpty(), "at least one external ip should exist.")

			restarter := NewRestartConfig(
				ips[0], "kube-controller", ports.KubeControllerManagerPort, restartPollInterval, restartTimeout, true)
			restarter.restart(ctx)

			// 目的是确保 replication controller manager 在重启后至少观察并上报过一次
			// replication controller 的状态，这样如果它打算创建/删除 pod，
			// 我们就能判断它有过这样的机会。将 RC 扩缩到
			// 相同规模即可做到：扩缩操作会推进 RC 的序号，
			// 并等待该序号被观察并回写到 RC 状态中。
			framework.ExpectNoError(e2erc.ScaleRC(ctx, f.ClientSet, f.ScalesGetter, ns, rcName, numPods, true))

			// 只检查 key；如果 kubelet 更新过 pod，内容可能不同。
			// TODO: 真的会吗？
			existingKeys := sets.NewString()
			newKeys := sets.NewString()
			for _, k := range existingPods.ListKeys() {
				existingKeys.Insert(k)
			}
			for _, k := range newPods.ListKeys() {
				newKeys.Insert(k)
			}
			if len(newKeys.List()) != len(existingKeys.List()) ||
				!newKeys.IsSuperset(existingKeys) {
				framework.Failf("RcManager created/deleted pods after restart \n\n %+v", tracker)
			}
		}
	})

	f.It("Scheduler should continue assigning pods to nodes across restart", f.WithProvider("gce", "aws") /* Requires master ssh access. */, func(ctx context.Context) {
		nodes := framework.GetControlPlaneNodes(ctx, f.ClientSet)

		// 检查是否至少有一个 control-plane 节点
		gomega.Expect(nodes.Items).NotTo(gomega.BeEmpty(), "at least one node with label %s should exist.", framework.ControlPlaneLabel)

		for i := range nodes.Items {
			ips := framework.GetNodeExternalIPs(&nodes.Items[i])
			gomega.Expect(ips).NotTo(gomega.BeEmpty(), "at least one external ip should exist.")

			restarter := NewRestartConfig(
				ips[0], "kube-scheduler", kubeschedulerconfig.DefaultKubeSchedulerPort, restartPollInterval, restartTimeout, true)

			// 在 scheduler 停机期间创建 pod，再通过将 rc 扩缩到相同规模，
			// 确保 scheduler 恢复后会接管这些 pod。
			restarter.waitUp(ctx)
			restarter.kill(ctx)
			// 这是尽力而为地在 scheduler 停机期间创建 pod，
			// 因为我们无法确切知道它收到 kill 信号后何时重启。
			framework.ExpectNoError(e2erc.ScaleRC(ctx, f.ClientSet, f.ScalesGetter, ns, rcName, numPods+5, false))
			restarter.waitUp(ctx)
			framework.ExpectNoError(e2erc.ScaleRC(ctx, f.ClientSet, f.ScalesGetter, ns, rcName, numPods+5, true))
		}
	})

	ginkgo.It("Kubelet should not restart containers across restart", func(ctx context.Context) {
		nodeIPs, err := e2enode.GetPublicIps(ctx, f.ClientSet)
		if err != nil {
			framework.Logf("Unexpected error occurred: %v", err)
		}
		framework.ExpectNoErrorWithOffset(0, err)
		preRestarts, badNodes := getContainerRestarts(ctx, f.ClientSet, ns, labelSelector)
		if preRestarts != 0 {
			framework.Logf("WARNING: Non-zero container restart count: %d across nodes %v", preRestarts, badNodes)
		}
		for _, ip := range nodeIPs {
			restarter := NewRestartConfig(
				ip, "kubelet", ports.KubeletHealthzPort, restartPollInterval, restartTimeout, false)
			restarter.restart(ctx)
		}
		postRestarts, badNodes := getContainerRestarts(ctx, f.ClientSet, ns, labelSelector)
		if postRestarts != preRestarts {
			e2edebug.DumpNodeDebugInfo(ctx, f.ClientSet, badNodes, framework.Logf)
			framework.Failf("Net container restart count went from %v -> %v after kubelet restart on nodes %v \n\n %+v", preRestarts, postRestarts, badNodes, tracker)
		}
	})

	ginkgo.It("Kube-proxy should recover after being killed accidentally", func(ctx context.Context) {
		nodeIPs, err := e2enode.GetPublicIps(ctx, f.ClientSet)
		if err != nil {
			framework.Logf("Unexpected error occurred: %v", err)
		}
		for _, ip := range nodeIPs {
			restarter := NewRestartConfig(
				ip, "kube-proxy", ports.ProxyHealthzPort, restartPollInterval, restartTimeout, false)
			// restart 方法会杀掉 kube-proxy 进程并等待其恢复，
			// 若无法恢复则判定测试失败。
			restarter.restart(ctx)
		}
	})
})
