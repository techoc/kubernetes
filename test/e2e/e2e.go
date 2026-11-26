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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/klog/v2"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/version"
	commontest "k8s.io/kubernetes/test/e2e/common"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/daemonset"
	e2edebug "k8s.io/kubernetes/test/e2e/framework/debug"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2ereporters "k8s.io/kubernetes/test/e2e/reporters"
	utilnet "k8s.io/utils/net"

	clientset "k8s.io/client-go/kubernetes"
	// 确保 auth 插件已加载
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	// 确保 logging 参数出现在命令行中。
	_ "k8s.io/component-base/logs/testinit"
)

const (
	// namespaceCleanupTimeout 是等待命名空间被删除的时长。
	// 如果需要清理孤立命名空间，说明测试运行在长期存活的集群上。
	// 此时宁可多等一会儿，也好过因上次测试遗留资源导致
	// 偶发的测试失败。
	namespaceCleanupTimeout = 15 * time.Minute
)

var progressReporter = &e2ereporters.ProgressReporter{}

var _ = ginkgo.SynchronizedBeforeSuite(func(ctx context.Context) []byte {
	// 引用公共测试使 import 有效。
	commontest.CurrentSuite = commontest.E2E
	progressReporter.SetStartMsg()
	setupSuite(ctx)
	return nil
}, func(ctx context.Context, data []byte) {
	// 在所有 Ginkgo node 上运行
	setupSuitePerGinkgoNode(ctx)
})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	progressReporter.SetEndMsg()
}, func(ctx context.Context) {
	AfterSuiteActions(ctx)
})

// RunE2ETests 检查（通过 flag 指定的）配置参数，然后用 Ginkgo runner 运行
// E2E 测试。
// 如果指定了 "report directory"，会在该目录生成一个或多个 JUnit 测试报告，
// 同时保存集群日志。
// 并行模式下，该函数会在每个 Ginkgo node 上调用。
func RunE2ETests(t *testing.T) {
	// InitLogs 禁用了 contextual logging，而 E2E 测试套件没有 feature gate，
	// 因此无法重新启用。它过去有个误导性的 --feature-gates 参数，
	// 但并做不到用户和开发者期望的事（定义集群支持哪些特性），
	// 因此已被移除。
	// 因此已被移除。
	//
	// contextual logging 有用且应被测试，因此这里无条件
	// 重新启用它。
	logs.InitLogs()
	defer logs.FlushLogs()
	klog.EnableContextualLogging(true)

	progressReporter = e2ereporters.NewProgressReporter(framework.TestContext.ProgressReportURL)
	gomega.RegisterFailHandler(framework.Fail)

	// 通过 Ginkgo runner 运行测试，输出同时到控制台和 JUnit（供 Jenkins 使用）
	suiteConfig, reporterConfig := framework.CreateGinkgoConfig()
	klog.Infof("Starting e2e run %q on Ginkgo node %d", framework.RunID, suiteConfig.ParallelProcess)
	ginkgo.RunSpecs(t, "Kubernetes e2e suite", suiteConfig, reporterConfig)
}

// getDefaultClusterIPFamily 通过 default 命名空间中 kubernetes service 的 Cluster IP
// 获取集群的默认 IP family。
// 由于 service 是单 family 的，这能明确判断默认 IP family。
// TODO: dual-stack 下每个 service 可能支持多个 family，
// 但可以通过 Pod 有两个地址（每个 family 一个）来检测集群是否为 dual-stack。
func getDefaultClusterIPFamily(ctx context.Context, c clientset.Interface) string {
	// 获取 default 命名空间中 kubernetes service 的 ClusterIP
	svc, err := c.CoreV1().Services(metav1.NamespaceDefault).Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		framework.Failf("Failed to get kubernetes service ClusterIP: %v", err)
	}

	if utilnet.IsIPv6String(svc.Spec.ClusterIP) {
		return "ipv6"
	}
	return "ipv4"
}

// waitForDaemonSets 等待给定命名空间中的所有 daemonset 就绪
// （定义为除 'allowedNotReadyNodes' 个节点外，其余节点上关联该
// daemonset 的 Pod 均就绪）。
//
// 如果 allowedNotReadyNodes 为 -1，该方法立即返回，不等待。
func waitForDaemonSets(ctx context.Context, c clientset.Interface, ns string, allowedNotReadyNodes int32, timeout time.Duration) error {
	if allowedNotReadyNodes == -1 {
		return nil
	}

	start := time.Now()
	framework.Logf("Waiting up to %v for all daemonsets in namespace '%s' to start",
		timeout, ns)

	return wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, func(ctx context.Context) (bool, error) {
		dsList, err := c.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Logf("Error getting daemonsets in namespace: '%s': %v", ns, err)
			return false, err
		}
		var notReadyDaemonSets []string
		for _, ds := range dsList.Items {
			framework.Logf("%d / %d pods ready in namespace '%s' in daemonset '%s' (%d seconds elapsed)", ds.Status.NumberReady, ds.Status.DesiredNumberScheduled, ns, ds.ObjectMeta.Name, int(time.Since(start).Seconds()))
			if ds.Status.DesiredNumberScheduled-ds.Status.NumberReady > allowedNotReadyNodes {
				notReadyDaemonSets = append(notReadyDaemonSets, ds.ObjectMeta.Name)
			}
		}

		if len(notReadyDaemonSets) > 0 {
			framework.Logf("there are not ready daemonsets: %v", notReadyDaemonSets)
			return false, nil
		}

		return true, nil
	})
}

// setupSuite 是可整套 ginkgo 测试套件复用的样板代码，在 SynchronizedBeforeSuite 阶段执行。
// 有些操作在整个测试调用中只需运行一次
// （例如删除旧命名空间，或确认所有系统 Pod 都在运行）。
// 由于 Ginkgo 并行运行测试，必须使用 SynchronizedBeforeSuite
// 确保这些操作只在第一个并行 Ginkgo node 上执行。
//
// 该函数接受两个参数：一个只在第一个 Ginkgo node 上运行的函数，
// 返回一个不透明的字节数组；另一个在所有 Ginkgo node 上运行的函数，
// 接受该字节数组。
func setupSuite(ctx context.Context) {
	// 只在 Ginkgo node 1 上运行

	switch framework.TestContext.Provider {
	case "gce":
		logClusterImageSources()
	}

	c, err := framework.LoadClientset()
	framework.ExpectNoError(err, "Error loading client")

	// 删除除系统创建的所有命名空间，确保上次测试运行不会留下
	// 残留资源。
	if framework.TestContext.CleanStart {
		deleted, err := framework.DeleteNamespaces(ctx, c, nil, /* deleteFilter */
			[]string{
				metav1.NamespaceSystem,
				metav1.NamespaceDefault,
				metav1.NamespacePublic,
				v1.NamespaceNodeLease,
			})
		if err != nil {
			framework.Failf("Error deleting orphaned namespaces: %v", err)
		}
		if err := framework.WaitForNamespacesDeleted(ctx, c, deleted, namespaceCleanupTimeout); err != nil {
			framework.Failf("Failed to delete orphaned namespaces %v: %v", deleted, err)
		}
	}

	timeouts := framework.NewTimeoutContext()

	// 在大规模集群中，执行到这里仍可能有一批节点尚未创建 Route。
	// 这会导致节点不可调度，因此需要等待所有节点都变为可调度。
	// 如果未指定 NumNodes，则自动检测可调度且未被 taint 的节点数量。
	framework.ExpectNoError(e2enode.WaitForAllNodesSchedulable(ctx, c, timeouts.NodeSchedulable))
	// 自动检测 NumNodes：
	if framework.TestContext.CloudConfig.NumNodes == framework.DefaultNumNodes {
		nodes, err := e2enode.GetReadySchedulableNodes(ctx, c)
		framework.ExpectNoError(err)
		// 测试开始前确保所有 Pod 都在运行且就绪（否则，
		// 正在拉取或启动的集群基础设施 Pod 会阻塞测试 Pod 运行，
		// 进而导致检查所有 Pod 运行且就绪的测试失败）。
		//
		// TODO: 在大规模集群中，经常因 #41007 看到 Pod 无法启动。
		// 为避免这些 Pod 阻塞整个测试运行（白白浪费整个 run），
		// 这里允许少量未就绪 Pod（数量等于允许未就绪的节点数）。
		framework.TestContext.CloudConfig.NumNodes = len(nodes.Items)
	}

	// TODO: 在大规模集群中，经常因 #41007 看到 Pod 无法启动。
	// 为避免这些 Pod 阻塞整个测试运行（白白浪费整个 run），
	// 这里允许少量未就绪 Pod（数量等于允许未就绪的节点数）。
	//
	//
	//
	//
	//
	//
	if err := e2epod.WaitForAlmostAllPodsReady(ctx, c, metav1.NamespaceSystem, framework.TestContext.MinStartupPods, framework.TestContext.AllowedNotReadyNodes, timeouts.SystemPodsStartup); err != nil {
		e2edebug.DumpAllNamespaceInfo(ctx, c, metav1.NamespaceSystem)
		e2ekubectl.LogFailedContainers(ctx, c, metav1.NamespaceSystem, framework.Logf)
		framework.Failf("Error waiting for all pods to be running and ready: %v", err)
	}

	if err := waitForDaemonSets(ctx, c, metav1.NamespaceSystem, int32(framework.TestContext.AllowedNotReadyNodes), timeouts.SystemDaemonsetStartup); err != nil {
		framework.Logf("WARNING: Waiting for all daemonsets to be ready failed: %v", err)
	}

	if framework.TestContext.PrepullImages {
		framework.Logf("Pre-pulling images so that they are cached for the tests.")
		prepullImages(ctx, c)
	}

	// 记录 server 和本客户端的版本。
	framework.Logf("e2e test version: %s", version.Get().GitVersion)

	dc := c.DiscoveryClient

	serverVersion, serverErr := dc.ServerVersion()
	if serverErr != nil {
		framework.Logf("Unexpected server error retrieving version: %v", serverErr)
	}
	if serverVersion != nil {
		framework.Logf("kube-apiserver version: %s", serverVersion.GitVersion)
	}
}

// logClusterImageSources 输出集群镜像来源。
func logClusterImageSources() {
	controlPlaneNodeImg, workerNodeImg, err := lookupClusterImageSources()
	if err != nil {
		framework.Logf("Cluster image sources lookup failed: %v\n", err)
		return
	}
	framework.Logf("cluster-control-plane-node-image: %s", controlPlaneNodeImg)
	framework.Logf("cluster-worker-node-image: %s", workerNodeImg)

	images := map[string]string{
		"control_plane_node_os_image": controlPlaneNodeImg,
		"worker_node_os_image":        workerNodeImg,
	}

	outputBytes, _ := json.MarshalIndent(images, "", "  ")
	filePath := filepath.Join(framework.TestContext.ReportDir, "images.json")
	if err := os.WriteFile(filePath, outputBytes, 0644); err != nil {
		framework.Logf("cluster images sources, could not write to %q: %v", filePath, err)
	}
}

// TODO: 这些代码真应该直接使用 GCE API client 库，或至少使用
// --format 参数输出更好解析的格式。

// 返回控制面节点和 worker 节点的镜像字符串，或错误
func lookupClusterImageSources() (string, string, error) {
	// 给定 gcloud compute 命令的参数，运行并返回结果值，
	// 无论其以换行、逗号还是分号分隔。
	gcloudf := func(argv ...string) ([]string, error) {
		args := []string{"compute"}
		args = append(args, argv...)
		args = append(args, "--project", framework.TestContext.CloudConfig.ProjectID)
		if framework.TestContext.CloudConfig.MultiMaster {
			args = append(args, "--region", framework.TestContext.CloudConfig.Region)
		} else {
			args = append(args, "--zone", framework.TestContext.CloudConfig.Zone)
		}
		outputBytes, err := exec.Command("gcloud", args...).CombinedOutput()
		str := strings.Replace(string(outputBytes), ",", "\n", -1)
		str = strings.Replace(str, ";", "\n", -1)
		lines := strings.Split(str, "\n")
		if err != nil {
			framework.Logf("lookupDiskImageSources: gcloud error with [%#v]; err:%v", argv, err)
			for _, l := range lines {
				framework.Logf(" > %s", l)
			}
		}
		return lines, err
	}

	// 遍历给定 GCE 实例的磁盘，找到带有 sourceImage 的那个
	host2image := func(instance string) (string, error) {
		// gcloud compute instances describe {INSTANCE} --format="get(disks[].source)"
		// gcloud compute disks describe {DISKURL} --format="get(sourceImage)"
		disks, err := gcloudf("instances", "describe", instance, "--format=get(disks[].source)")
		if err != nil {
			return "", err
		} else if len(disks) == 0 {
			return "", fmt.Errorf("instance %q had no findable disks", instance)
		}
		// 遍历磁盘，寻找启动盘
		for _, disk := range disks {
			lines, err := gcloudf("disks", "describe", disk, "--format=get(sourceImage)")
			if err != nil {
				return "", err
			} else if len(lines) > 0 && lines[0] != "" {
				return lines[0], nil // 结束，已完成
			}
		}
		return "", fmt.Errorf("instance %q had no disk with a sourceImage", instance)
	}

	// gcloud compute instance-groups list-instances {GROUPNAME} --format="get(instance)"
	workerNodeName := ""
	instGroupName := strings.Split(framework.TestContext.CloudConfig.NodeInstanceGroup, ",")[0]
	if lines, err := gcloudf("instance-groups", "list-instances", instGroupName, "--format=get(instance)"); err != nil {
		return "", "", err
	} else if len(lines) == 0 {
		return "", "", fmt.Errorf("no instances inside instance-group %q", instGroupName)
	} else {
		workerNodeName = lines[0]
	}

	workerNodeImg, err := host2image(workerNodeName)
	if err != nil {
		return "", "", err
	}
	frags := strings.Split(workerNodeImg, "/")
	workerNodeImg = frags[len(frags)-1]

	// 对 GKE 集群，controlPlaneNodeName 未定义；此时 controlPlaneNodeImg 留空。
	controlPlaneNodeImg := ""
	if controlPlaneNodeName := framework.TestContext.CloudConfig.MasterName; controlPlaneNodeName != "" {
		img, err := host2image(controlPlaneNodeName)
		if err != nil {
			return "", "", err
		}
		frags = strings.Split(img, "/")
		controlPlaneNodeImg = frags[len(frags)-1]
	}

	return controlPlaneNodeImg, workerNodeImg, nil
}

// setupSuitePerGinkgoNode 是可整套 ginkgo 测试套件复用的样板代码，在每个 Ginkgo node 的 SynchronizedBeforeSuite 阶段执行。
// 有些操作在整个测试调用中只需在每个 Ginkgo node 上运行一次，
// 例如让所有并行执行都能访问某些全局变量。
// 由于 Ginkgo 并行运行测试，必须使用 SynchronizedBeforeSuite。
// 参考：https://onsi.github.io/ginkgo/#parallel-specs
func setupSuitePerGinkgoNode(ctx context.Context) {
	// 获取集群默认 IP family
	// 有些 e2e 测试只面向 IPv4 设计，这个全局变量
	// 使这些测试能同时适配 IPv4 和 IPv6。
	// TODO: dual-stack
	// dual-stack 集群可能是 ipv4-ipv6 或 ipv6-ipv4，顺序很重要，
	// 且 service 默认使用主 IP family。
	c, err := framework.LoadClientset()
	framework.ExpectNoError(err, "Error loading client")
	framework.TestContext.IPFamily = getDefaultClusterIPFamily(ctx, c)
	framework.Logf("Cluster IP family: %s", framework.TestContext.IPFamily)
}

func prepullImages(ctx context.Context, c clientset.Interface) {
	namespace, err := framework.CreateTestingNS(ctx, "img-puller", c, map[string]string{
		"e2e-framework": "img-puller",
	})
	framework.ExpectNoError(err)
	ns := namespace.Name
	ginkgo.DeferCleanup(c.CoreV1().Namespaces().Delete, ns, metav1.DeleteOptions{})

	images := commontest.PrePulledImages
	if framework.NodeOSDistroIs("windows") {
		images = commontest.WindowsPrePulledImages
	}

	label := map[string]string{"app": "prepull-daemonset"}
	var imgPullers []*appsv1.DaemonSet
	for _, img := range images.List() {
		dsName := fmt.Sprintf("img-pull-%s", strings.ReplaceAll(strings.ReplaceAll(img, "/", "-"), ":", "-"))

		dsSpec := daemonset.NewDaemonSet(dsName, img, label, nil, nil, nil)
		ds, err := c.AppsV1().DaemonSets(ns).Create(ctx, dsSpec, metav1.CreateOptions{})
		framework.ExpectNoError(err)
		imgPullers = append(imgPullers, ds)
	}

	// 该值不应该是 5 的倍数，因为节点状态每 5 秒更新一次。
	// 参见 https://github.com/kubernetes/kubernetes/pull/14915。
	dsRetryPeriod := 9 * time.Second
	dsRetryTimeout := 5 * time.Minute

	for _, imgPuller := range imgPullers {
		checkDaemonset := func(ctx context.Context) (bool, error) {
			return daemonset.CheckPresentOnNodes(ctx, c, imgPuller, ns, framework.TestContext.CloudConfig.NumNodes)
		}
		framework.Logf("Waiting for %s", imgPuller.Name)
		err := wait.PollUntilContextTimeout(ctx, dsRetryPeriod, dsRetryTimeout, true, checkDaemonset)
		framework.ExpectNoError(err, "error waiting for image to be pulled")
	}
}
