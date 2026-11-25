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

package e2enode

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2enodekubelet "k8s.io/kubernetes/test/e2e_node/kubeletconfig"
	"k8s.io/kubernetes/test/e2e_node/perf/workloads"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// makeNodePerfPod 根据 workload 提供的信息创建 pod
func makeNodePerfPod(w workloads.NodePerfWorkload) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-pod", w.Name()),
		},
		Spec: w.PodSpec(),
	}
}

func setKubeletConfig(ctx context.Context, f *framework.Framework, cfg *kubeletconfig.KubeletConfiguration) {
	if cfg != nil {
		// 更新 kubelet 配置
		ginkgo.By("Stopping the kubelet")
		restartKubelet := mustStopKubelet(ctx, f)

		framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(cfg))

		ginkgo.By("Restarting the kubelet")
		restartKubelet(ctx)
	}

	// 等待 kubelet 就绪，配置变更后节点需要重新上报 Ready
	gomega.Eventually(ctx, func(ctx context.Context) bool {
		nodes, err := e2enode.TotalReady(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		return nodes == 1
	}, time.Minute, time.Second).Should(gomega.BeTrueBecause("expected kubelet to be in ready state"))
}

// 串行执行，因为测试会修改 kubelet 配置。
// 设计上就标记为慢测试。
var _ = SIGDescribe("Node Performance Testing", framework.WithSerial(), framework.WithSlow(), func() {
	f := framework.NewDefaultFramework("node-performance-testing")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	var (
		wl     workloads.NodePerfWorkload
		oldCfg *kubeletconfig.KubeletConfiguration
		newCfg *kubeletconfig.KubeletConfiguration
		pod    *v1.Pod
	)
	ginkgo.JustBeforeEach(func(ctx context.Context) {
		err := wl.PreTestExec()
		framework.ExpectNoError(err)
		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)
		newCfg, err = wl.KubeletConfig(oldCfg)
		framework.ExpectNoError(err)
		setKubeletConfig(ctx, f, newCfg)
	})

	cleanup := func(ctx context.Context) {
		gp := int64(0)
		delOpts := metav1.DeleteOptions{
			GracePeriodSeconds: &gp,
		}
		e2epod.NewPodClient(f).DeleteSync(ctx, pod.Name, delOpts, f.Timeouts.PodDelete)

		// pod 删除后给 CPU manager 留出清理时间，否则 PostTestExec 可能在 CPU manager
		// 仍在工作时删除其 checkpoint 文件，随后又生成新的 checkpoint，导致 kubelet
		// 在下次设置配置时崩溃
		time.Sleep(15 * time.Second)
		ginkgo.By("running the post test exec from the workload")
		err := wl.PostTestExec()
		framework.ExpectNoError(err)
		setKubeletConfig(ctx, f, oldCfg)
	}

	runWorkload := func(ctx context.Context) {
		ginkgo.By("running the workload and waiting for success")
		// 为 workload 创建 pod
		pod = makeNodePerfPod(wl)
		// 创建 pod
		pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)
		// 等待 pod 成功；但不用 WaitForSuccess，失败时需要打印容器日志 #109295
		podErr := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, fmt.Sprintf("%s or %s", v1.PodSucceeded, v1.PodFailed), wl.Timeout(),
			func(pod *v1.Pod) (bool, error) {
				switch pod.Status.Phase {
				case v1.PodFailed:
					return true, fmt.Errorf("pod %q failed with reason: %q, message: %q", pod.Name, pod.Status.Reason, pod.Status.Message)
				case v1.PodSucceeded:
					return true, nil
				default:
					return false, nil
				}
			},
		)
		podLogs, err := e2epod.GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, pod.Name, pod.Spec.Containers[0].Name)
		framework.ExpectNoError(err)
		if podErr != nil {
			framework.Logf("dumping pod logs due to pod error detected: \n%s", podLogs)
			framework.Failf("pod error: %v", podErr)
		}
		perf, err := wl.ExtractPerformanceFromLogs(podLogs)
		framework.ExpectNoError(err)
		framework.Logf("Time to complete workload %s: %v", wl.Name(), perf)
		// 这里不用 framework.ExpectNoError 保持一致，因为那会改变输出格式
		gomega.Expect(podErr).To(gomega.Succeed(), "wait for pod %q to succeed", pod.Name)
	}

	ginkgo.BeforeEach(func(ctx context.Context) {
		ginkgo.By("ensure environment has enough CPU + Memory to run")
		minimumRequiredCPU := resource.MustParse("15")
		minimumRequiredMemory := resource.MustParse("48Gi")
		localNodeCap := getLocalNode(ctx, f).Status.Allocatable
		cpuCap := localNodeCap[v1.ResourceCPU]
		memCap := localNodeCap[v1.ResourceMemory]
		if cpuCap.Cmp(minimumRequiredCPU) == -1 {
			e2eskipper.Skipf("Skipping Node Performance Tests due to lack of CPU. Required %v is less than capacity %v.", minimumRequiredCPU, cpuCap)
		}
		if memCap.Cmp(minimumRequiredMemory) == -1 {
			e2eskipper.Skipf("Skipping Node Performance Tests due to lack of memory. Required %v is less than capacity %v.", minimumRequiredMemory, memCap)
		}
	})

	ginkgo.Context("Run node performance testing with pre-defined workloads", func() {
		ginkgo.BeforeEach(func() {
			wl = workloads.NodePerfWorkloads[0]
		})
		ginkgo.It("NAS parallel benchmark (NPB) suite - Integer Sort (IS) workload", func(ctx context.Context) {
			ginkgo.DeferCleanup(cleanup)
			runWorkload(ctx)
		})
	})
	ginkgo.Context("Run node performance testing with pre-defined workloads", func() {
		ginkgo.BeforeEach(func() {
			wl = workloads.NodePerfWorkloads[1]
		})
		ginkgo.It("NAS parallel benchmark (NPB) suite - Embarrassingly Parallel (EP) workload", func(ctx context.Context) {
			ginkgo.DeferCleanup(cleanup)
			runWorkload(ctx)
		})
	})
	ginkgo.Context("Run node performance testing with pre-defined workloads", func() {
		ginkgo.BeforeEach(func() {
			wl = workloads.NodePerfWorkloads[2]
		})
		ginkgo.It("PyTorch workload", func(ctx context.Context) {
			ginkgo.DeferCleanup(cleanup)
			runWorkload(ctx)
		})
	})
})
