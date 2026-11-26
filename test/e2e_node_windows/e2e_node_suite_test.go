//go:build windows

/*
Copyright The Kubernetes Authors.

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

// 运行这个 suite 中的测试
// 注意：这个测试套件需要免密 sudo 权限来运行 kubelet 和 kube-apiserver。
package e2enodewindows

import (
	"context"
	"flag"
	"fmt"

	"os"
	"os/exec"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/kubernetes/pkg/util/rlimit"
	commontest "k8s.io/kubernetes/test/e2e/common"
	"k8s.io/kubernetes/test/e2e/framework"
	e2econfig "k8s.io/kubernetes/test/e2e/framework/config"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	e2etestingmanifests "k8s.io/kubernetes/test/e2e/testing-manifests"
	"k8s.io/kubernetes/test/e2e_node_windows/criproxy"
	"k8s.io/kubernetes/test/e2e_node_windows/services"

	// 定义并冻结常量
	_ "k8s.io/kubernetes/test/e2e/feature"

	// 重新配置 framework
	_ "k8s.io/kubernetes/test/e2e/framework/debug/init"
	_ "k8s.io/kubernetes/test/e2e/framework/metrics/init"
	_ "k8s.io/kubernetes/test/e2e/framework/node/init"
	"k8s.io/kubernetes/test/utils/ktesting"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

var (
	e2eCriProxy *criproxy.RemoteRuntime
	e2es        *services.E2EServices
	// featureGates 是 feature 名称到 bool 的 map，用于启用或禁用 alpha/实验性特性。
	featureGates map[string]bool
	// serviceFeatureGates 是 feature 名称到 bool 的 map，用于为
	// API service 启用或禁用 alpha/实验性特性。
	serviceFeatureGates map[string]bool

	// TODO(random-liu): 把下面的模式改成子命令。
	runServicesMode    = flag.Bool("run-services-mode", false, "If true, only run services (etcd, apiserver) in current process, and not run test.")
	runKubeletMode     = flag.Bool("run-kubelet-mode", false, "If true, only start kubelet, and not run test.")
	systemValidateMode = flag.Bool("system-validate-mode", false, "If true, only run system validation in current process, and not run test.")
)

// registerNodeFlags 注册 node e2e 测试套件专用的 flag。
func registerNodeFlags(flags *flag.FlagSet) {
	// 当 node flag 是 api.Registry 时，把测试标记为 node e2e。
	framework.TestContext.NodeE2E = true
	flags.StringVar(&framework.TestContext.BearerToken, "bearer-token", "", "The bearer token to authenticate with. If not specified, it would be a random token. Currently this token is only used in node e2e tests.")
	flags.StringVar(&framework.TestContext.NodeName, "node-name", "", "Name of the node to run tests on.")
	flags.StringVar(&framework.TestContext.KubeletConfigDropinDir, "config-dir", "", "Path to a directory containing drop-in configurations for the kubelet.")
	// TODO(random-liu): 把 kubelet 启动逻辑移出测试。
	// TODO(random-liu): 把日志获取逻辑移出测试。
	// 启动 kubelet 有多种方式（systemd、initd、docker、手动启动等），
	// 管理日志也有多种方式（journald、upstart 等）。
	// 不同情况下需要向容器挂载不同内容、运行不同命令。
	// 在测试套件内部处理这种复杂性既困难也没必要。
	flags.BoolVar(&framework.TestContext.NodeConformance, "conformance", false, "If true, the test suite will not start kubelet, and fetch system log (kernel, docker, kubelet log etc.) to the report directory.")
	flags.BoolVar(&framework.TestContext.PrepullImages, "prepull-images", true, "If true, prepull images so image pull failures do not cause test failures.")
	flags.BoolVar(&framework.TestContext.RestartKubelet, "restart-kubelet", false, "If true, restart Kubelet unit when the process is killed.")
	flags.StringVar(&framework.TestContext.ImageDescription, "image-description", "", "The description of the image which the test will be running on.")
	flags.StringVar(&framework.TestContext.SystemSpecName, "system-spec-name", "", "The name of the system spec (e.g., gke) that's used in the node e2e test. The system specs are in test/e2e_node/system/specs/. This is used by the test framework to determine which tests to run for validating the system requirements.")
	flags.Var(cliflag.NewMapStringString(&framework.TestContext.ExtraEnvs), "extra-envs", "The extra environment variables needed for node e2e tests. Format: a list of key=value pairs, e.g., env1=val1,env2=val2")
	flag.StringVar(&framework.TestContext.ClusterDNSDomain, "dns-domain", "", "The DNS Domain of the cluster.")
	flag.Var(cliflag.NewMapStringString(&framework.TestContext.RuntimeConfig), "runtime-config", "The runtime configuration used on node e2e tests.")
	flags.BoolVar(&framework.TestContext.RequireDevices, "require-devices", false, "If true, require device plugins to be installed in the running environment.")
	flags.Var(cliflag.NewMapStringBool(&featureGates), "feature-gates", "A set of key=value pairs that describe feature gates for alpha/experimental features.")
	flags.Var(cliflag.NewMapStringBool(&serviceFeatureGates), "service-feature-gates", "A set of key=value pairs that describe feature gates for alpha/experimental features for API service.")
	flags.BoolVar(&framework.TestContext.StandaloneMode, "standalone-mode", false, "If true, starts kubelet in standalone mode.")
	flags.BoolVar(&framework.TestContext.CriProxyEnabled, "cri-proxy-enabled", false, "If true, enable CRI API proxy for failure injection.")
}

func init() {
	// 启用内嵌 FS 文件查找作为回退
	e2etestfiles.AddFileSource(e2etestingmanifests.GetE2ETestingManifestsFS())
}

func TestMain(m *testing.M) {
	// 在 TestMain 中复制 go flag，确保 go test flag 已注册（go1.13 起 init() 中不再可用）
	e2econfig.CopyFlags(e2econfig.Flags, flag.CommandLine)
	framework.RegisterCommonFlags(flag.CommandLine)
	registerNodeFlags(flag.CommandLine)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	// 把 run-services-mode flag 标记为隐藏，防止用户使用。
	pflag.CommandLine.MarkHidden("run-services-mode")
	// 奇怪的是，如果直接在 TestContext 中使用 pflag，它会报错。
	// 似乎有人在 init() 和 TestMain() 之后调用了 flag.Parse()。
	// TODO(random-liu): 找出谁在用 flag.Parse() 并导致错误，把下面的逻辑
	// 移到 TestContext 中。
	// TODO(pohly): 从 test_context.go 移除 RegisterNodeFlags，在这里启用 Viper 配置支持？

	pflag.Parse()
	if pflag.CommandLine.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "unknown additional command line arguments: %s", pflag.CommandLine.Args())
		os.Exit(1)
	}
	framework.AfterReadingAllFlags(&framework.TestContext)
	if err := e2eskipper.InitFeatureGates(utilfeature.DefaultFeatureGate, featureGates); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: initialize feature gates: %v", err)
		os.Exit(1)
	}

	if err := services.SetFeatureGatesForInProcessComponents(serviceFeatureGates); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: initialize process feature gates for API service: %v", err)
		os.Exit(1)
	}

	setExtraEnvs()
	os.Exit(m.Run())
}

func TestE2eNode(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 确保打开文件的数量不受 sshd 限制
	if err := rlimit.SetNumFiles(1000000); err != nil {
		klog.Infof("failed to set rlimit on max file handles: %v", err)
	}

	if *runServicesMode {
		// 如果指定了 run-services-mode，只在当前进程中运行服务。
		services.RunE2EServices(t)
		return
	}
	if *runKubeletMode {
		// 如果指定了 run-kubelet-mode，只启动 kubelet。
		services.RunKubelet(tCtx, featureGates)
		return
	}
	if *systemValidateMode {
		// 如果指定了 system-validate-mode，只在当前进程中运行系统验证。
		klog.Warningf("system spec validation is not supported on platform other than linux yet")
		return
	}

	// 我们没有运行在特殊模式下，那就运行测试。
	gomega.RegisterFailHandler(ginkgo.Fail)
	// 如果测试不是在 run-kubelet-mode 下运行，再次初始化 KubeletConfigDropinDir。
	if framework.TestContext.KubeletConfigDropinDir == "" {
		var err error
		framework.TestContext.KubeletConfigDropinDir, err = services.KubeletConfigDirCWDDir()
		if err != nil {
			klog.Errorf("failed to create kubelet config directory: %v", err)
		}
	}
	reportDir := framework.TestContext.ReportDir
	if reportDir != "" {
		// 如果目录不存在则创建
		// 注意：现在只需用新的 --junit-report flag 执行测试就能生成 junit 报告。
		if err := os.MkdirAll(reportDir, 0755); err != nil {
			klog.Errorf("Failed creating report directory: %v", err)
		}
	}

	// 给创建的 pod 加上源代码位置注解，方便找到
	// 清理不充分、留下残留 pod 污染节点状态的测试
	e2epod.GlobalOwnerTracking = true

	suiteConfig, reporterConfig := framework.CreateGinkgoConfig()
	ginkgo.RunSpecs(t, "E2eNode Suite", suiteConfig, reporterConfig)
}

// 在节点上设置 kubelet
var _ = ginkgo.SynchronizedBeforeSuite(func(ctx context.Context) []byte {
	// 运行系统验证测试。
	gomega.Expect(validateSystem()).To(gomega.Succeed(), "system validation")

	// 预先拉取测试依赖的镜像，这样镜像拉取出问题就能立即失败
	// 这有助于调试测试偶发失败，因为很难判断失败是否由镜像拉取导致。
	if framework.TestContext.PrepullImages {
		klog.Infof("Pre-pulling images so that they are cached for the tests.")
		updateImageAllowList(ctx)
		err := PrePullAllImages(ctx)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	if framework.TestContext.CriProxyEnabled {
		framework.Logf("Start cri proxy")
		rs, is, err := getCRIClient(ctx)
		framework.ExpectNoError(err)

		e2eCriProxy = criproxy.NewRemoteRuntimeProxy(rs, is)
		endpoint, err := criproxy.GenerateEndpoint()
		framework.ExpectNoError(err)

		err = e2eCriProxy.Start(endpoint)
		framework.ExpectNoError(err)

		framework.TestContext.ContainerRuntimeEndpoint = endpoint
		framework.TestContext.ImageServiceEndpoint = endpoint
	}

	if *startServices {
		// 如果服务应在测试后停止，它们应监控测试进程。
		// 如果服务应在测试后继续运行，则不应监控测试进程。
		e2es = services.NewE2EServices(*stopServices)
		gomega.Expect(e2es.Start(ctx, featureGates)).To(gomega.Succeed(), "should be able to start node services.")
	} else {
		klog.Infof("Running tests without starting services.")
	}

	if !framework.TestContext.StandaloneMode {
		klog.Infof("Wait for the node to be ready")
		waitForNodeReady(ctx)
	}

	// 引用公共测试，使 import 有效。
	commontest.CurrentSuite = commontest.NodeE2E

	// ginkgo 会派生多个进程来运行测试。
	// 由于 bearer token 在运行时随机生成，
	// 需要把它分发给其他进程，让它们使用同一个 token。
	return []byte(framework.TestContext.BearerToken)
}, func(ctx context.Context, token []byte) {
	framework.TestContext.BearerToken = string(token)
	// 用节点配置更新 test context。
	gomega.Expect(updateTestContext(ctx)).To(gomega.Succeed(), "update test context with node config.")

	// 把当前 Kubelet 配置存入包级变量
	// 这假设所有动态修改 kubelet 配置的测试
	// 必须：1) 串行运行；2) 测试后恢复 kubelet 配置。
	var err error
	kubeletCfg, err = getCurrentKubeletConfig(ctx)
	framework.ExpectNoError(err)
})

// 拆除节点上的 kubelet
var _ = ginkgo.SynchronizedAfterSuite(func() {}, func() {
	if e2es != nil {
		if *startServices && *stopServices {
			klog.Infof("Stopping node services...")
			e2es.Stop()
		}
	}

	if e2eCriProxy != nil {
		framework.Logf("Stopping cri proxy service...")
		e2eCriProxy.Stop()
	}

	klog.Infof("Tests Finished")
})

// validateSystem 在单独的进程中运行系统验证，验证失败则返回错误。
func validateSystem() error {
	testBin, err := os.Executable()
	if err != nil {
		return fmt.Errorf("can't get current binary: %w", err)
	}
	// 把所有 flag 传给子进程，使其看到相同的 flag 集合。
	output, err := exec.Command(testBin, append([]string{"--system-validate-mode"}, os.Args[1:]...)...).CombinedOutput()
	// 系统验证的输出应该已格式化好，直接打印。
	fmt.Print(string(output))
	if err != nil {
		return fmt.Errorf("system validation failed: %w", err)
	}
	return nil
}

func waitForNodeReady(ctx context.Context) {
	const (
		// nodeReadyTimeout 是等待节点变为 ready 的时间。
		nodeReadyTimeout = 2 * time.Minute
		// nodeReadyPollInterval 是检查节点 ready 的间隔。
		nodeReadyPollInterval = 1 * time.Second
	)
	client, err := getAPIServerClient()
	framework.ExpectNoError(err, "should be able to get apiserver client.")
	gomega.Eventually(ctx, func() error {
		node, err := getNode(client)
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}
		if !isNodeReady(node) {
			return fmt.Errorf("node is not ready: %+v", node)
		}
		return nil
	}, nodeReadyTimeout, nodeReadyPollInterval).Should(gomega.Succeed())
}

// updateTestContext 用节点名称更新 test context。
func updateTestContext(ctx context.Context) error {
	setExtraEnvs()
	updateImageAllowList(ctx)

	client, err := getAPIServerClient()
	if err != nil {
		return fmt.Errorf("failed to get apiserver client: %w", err)
	}

	if !framework.TestContext.StandaloneMode {
		// 用当前 node 对象更新 test context。
		node, err := getNode(client)
		if err != nil {
			return fmt.Errorf("failed to get node: %w", err)
		}
		framework.TestContext.NodeName = node.Name // Set node name from API server, it is already set to the computer name by default.
	}

	framework.Logf("Node name: %s", framework.TestContext.NodeName)

	return nil
}

// getNode 从 apiserver 获取 node 对象。
func getNode(c *clientset.Clientset) (*v1.Node, error) {
	nodes, err := c.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	framework.ExpectNoError(err, "should be able to list nodes.")
	if nodes == nil {
		return nil, fmt.Errorf("the node list is nil")
	}
	gomega.Expect(len(nodes.Items)).To(gomega.BeNumerically("<=", 1), "the number of nodes is more than 1.")
	if len(nodes.Items) == 0 {
		return nil, fmt.Errorf("empty node list: %+v", nodes)
	}
	return &nodes.Items[0], nil
}

// getAPIServerClient 获取 apiserver client。
func getAPIServerClient() (*clientset.Clientset, error) {
	config, err := framework.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	client, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return client, nil
}

func setExtraEnvs() {
	for name, value := range framework.TestContext.ExtraEnvs {
		os.Setenv(name, value)
	}
}
