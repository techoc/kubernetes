/*
Copyright 2025 The Kubernetes Authors.

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

// Package localupcluster 包含围绕调用 hack/local-up-cluster.sh 以及管理所创建集群的封装代码。
//
// 基本的工作方式如下：
//   - local-up-cluster.sh 包含如何配置和调用各组件的全部逻辑。
//   - local-up-cluster.sh 以 dry-run 模式运行，它会打印所有命令及其参数，
//     但并不实际执行这些命令，etcd 除外：etcd 的生命周期仍由调用方或
//     local-up-cluster.sh 像以前一样管理。
//   - 只要集群还在运行，local-up-cluster.sh 就会一直保持运行，以维持 etcd 进程并保留生成的配置文件。
//   - 本包负责执行这些命令，包括输出重定向。
//   - 它可以停止命令，并使用不同的二进制文件重新运行，以模拟升级或降级。
package localupcluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/gstruct"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/featuregate"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

type ClusterComponentName string

// 组件名称。
//
// 如果 local-up-cluster.sh 和 etc.sh 运行了这些组件，这里的名称与它们输出中的名称一致。
const (
	Etcd                  = ClusterComponentName("etcd")
	KubeAPIServer         = ClusterComponentName("kube-apiserver")
	KubeControllerManager = ClusterComponentName("kube-controller-manager")
	KubeScheduler         = ClusterComponentName("kube-scheduler")
	Kubelet               = ClusterComponentName("kubelet")
	KubeProxy             = ClusterComponentName("kube-proxy")
	Kubectl               = ClusterComponentName("kubectl")
)

// 集群中运行的 Kubernetes 组件，按启动和升级所需的顺序列出。
var KubeClusterComponents = []ClusterComponentName{KubeAPIServer, KubeControllerManager, KubeScheduler, Kubelet, KubeProxy}

// featureGatedComponents 列出命令行接受 --feature-gates 参数的组件。
var featureGatedComponents = []ClusterComponentName{KubeAPIServer, KubeControllerManager, KubeScheduler, Kubelet}

// 所有组件，包括 etcd。
var components = append([]ClusterComponentName{Etcd}, KubeClusterComponents...)

// local-up-cluster.sh 输出中的 RUN <name> <command line> 标记了我们需要执行的命令。
const localUpClusterRunPrefix = "RUN "

func repoRoot(tCtx ktesting.TContext) string {
	for i := 0; ; i++ {
		dir := path.Join(".", strings.Repeat("../", i))
		_, err := os.Stat(dir)
		tCtx.ExpectNoError(err, "examine parent directory while looking for hack/local-up-cluster.sh (not invoked inside the Kubernetes repository?)")
		_, err = os.Stat(path.Join(dir, "hack/local-up-cluster.sh"))
		if err == nil {
			dir, err = filepath.Abs(dir)
			tCtx.ExpectNoError(err, "turn into absolute path")
			return dir
		}
	}
}

func New() *Cluster {
	c := &Cluster{}
	return c
}

// Cluster 表示一个集群。
//
// hack/local-up-cluster.sh 必须能在当前环境中正常工作。如有必要，
// 需要设置 CONTAINER_RUNTIME_ENDPOINT 等环境变量，使脚本适配当前主机系统。
// 当前用户必须有权使用容器运行时。
// 所有 Kubernetes 组件都以该用户身份运行，文件存放在测试临时目录中。
//
// local-up-cluster.sh 不支持每台主机上运行多个集群，因此
// 使用本包的测试必须顺序执行。
type Cluster struct {
	running    map[ClusterComponentName]*Cmd
	dir        string
	kubeConfig string
	settings   map[string]string
}

// Start 重新启动集群。如果集群已经在运行，会先将其停止。
//
// 调用方必须在合适的上下文中调用 Stop 来停止集群。
// 如果设置了 ARTIFACTS 环境变量且测试失败，在停止集群之前，
// kind 集群的日志文件会被转储到
// $ARTIFACTS/<test name>/kind/<cluster name>。
//
// 二进制文件目录必须包含 kube-apiserver、kube-controller-manager、
// kube-scheduler、kube-proxy 和 kubelet。这些二进制文件可以来自旧的
// Kubernetes 版本。它们会按照*当前* local-up-cluster.sh 中定义的参数被调用。
// 只要默认配置下的 local-up-cluster.sh 不依赖最近才加入的功能，这种方式就能正常工作。
func (c *Cluster) Start(tCtx ktesting.TContext, state string, bindir string, localUpClusterEnv map[string]string, featureGates string) {
	tCtx.Helper()
	c.Stop(tCtx)

	if artifacts, ok := os.LookupEnv("ARTIFACTS"); ok {
		// 清理名称：
		// - 移除 E2E 的 [] 标签
		// - 将空白字符和一些特殊字符替换为连字符
		testName := tCtx.Name()
		testName = regexp.MustCompile(`\s*\[[^\]]*\]`).ReplaceAllString(testName, "")
		testName = regexp.MustCompile(`[[:space:]/:()\\]+`).ReplaceAllString(testName, "-")
		testName = strings.Trim(testName, "-")
		c.dir = path.Join(artifacts, testName)
		tCtx.ExpectNoError(os.MkdirAll(c.dir, 0766), "create artifacts directory for test")
	} else {
		c.dir = tCtx.TempDir()
	}
	c.running = make(map[ClusterComponentName]*Cmd)
	c.settings = make(map[string]string)

	// 在后台启动 local-up-cluster.sh，
	// 解析其输出以提取命令并按顺序执行，
	// 然后等待脚本结束。脚本完成后，
	// 我们就掌握了运行和修改集群所需的全部信息。
	lines := make(chan string, 100)
	cmd := &Cmd{
		Name: "local-up-cluster",
		CommandLine: []string{
			path.Join(repoRoot(tCtx), "hack/local-up-cluster.sh"),
			"-o", bindir,
			"-d", // dry run
		},
		ProcessOutput: func(output Output) {
			// 将处理过程转到主 goroutine 中执行。
			if output.EOF {
				close(lines)
				return
			}
			lines <- output.Line
		},
		AdditionalEnv: localUpClusterEnv,
	}

	kubeVerboseStr := cmd.AdditionalEnv["KUBE_VERBOSE"]
	kubeVerboseVal, err := strconv.Atoi(kubeVerboseStr)
	if kubeVerboseStr == "" {
		kubeVerboseVal = 0
	} else {
		tCtx.ExpectNoError(err, "KUBE_VERBOSE")
	}
	if kubeVerboseVal < 2 {
		cmd.AdditionalEnv["KUBE_VERBOSE"] = "2" // 为配置变量赋值启用 -x 输出。
	}
	cmd.Start(tCtx)
	defer cmd.Stop(tCtx, "stopping")

processLocalUpClusterOutput:
	for {
		select {
		case <-tCtx.Done():
			c.Stop(tCtx)
			tCtx.Fatalf("interrupted cluster startup: %v", context.Cause(tCtx))
		case line, ok := <-lines:
			if !ok {
				break processLocalUpClusterOutput
			}
			c.processLocalUpClusterOutput(tCtx, state, line, featureGates)
		}
	}
	// 通常此时它已经停止了，因为我们看到了输出流的结束，
	// 但仍需验证其退出码。
	cmd.Wait(tCtx)

	tCtx.Logf("cluster is running, use KUBECONFIG=%s to access it", c.kubeConfig)
}

// 匹配类似 "+ API_SECURE_PORT=6443" 的行。
var varAssignment = regexp.MustCompile(`^\+ ([A-Z0-9_]+)=(.*)$`)

func (c *Cluster) processLocalUpClusterOutput(tCtx ktesting.TContext, state string, line string, featureGates string) {
	tCtx.Logf("local-up-cluster: %s", line)

	if strings.HasPrefix(line, localUpClusterRunPrefix) {
		line := line[len(localUpClusterRunPrefix):]
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			tCtx.Fatalf("unexpected RUN line: %s", line)
		}
		name := parts[0]
		cmdLine := parts[1]

		// 集群组件和 etcd 会保持运行。
		if slices.Contains(components, ClusterComponentName(name)) {
			if featureGates != "" && slices.Contains(featureGatedComponents, ClusterComponentName(name)) {
				splitCmdLine := strings.Split(cmdLine, " ")
				merged, err := mergeFeatureGatesFlags(splitCmdLine, featureGates)
				tCtx.ExpectNoError(err, "merge feature gates %s into %s command line", featureGates, name)
				cmdLine = strings.Join(merged, " ")
			}
			c.runComponent(tCtx, state, ClusterComponentName(name), cmdLine)
			return
		}

		// 其他命令会被调用，并且必须结束后我们才能继续。
		c.runCmd(tCtx, name, cmdLine)
		return
	}
	if m := varAssignment.FindStringSubmatch(line); m != nil {
		c.settings[m[1]] = m[2]
		if m[1] == "CERT_DIR" {
			c.kubeConfig = path.Join(m[2], "admin.kubeconfig")
		}
	}
}

func (c *Cluster) runComponent(tCtx ktesting.TContext, state string, component ClusterComponentName, command string) {
	commandLine := fromLocalUpClusterOutput(command)

	cmd := &Cmd{
		Name:        string(component) + "-" + state,
		CommandLine: commandLine,
		LogFile:     path.Join(c.dir, fmt.Sprintf("%s-%s.log", component, state)),
		// 通过 Cluster.Stop 停止。
		KeepRunning: true,
	}

	c.runComponentWithRetry(tCtx, component, cmd)
}

func (c *Cluster) runCmd(tCtx ktesting.TContext, name, command string) {
	commandLine := fromLocalUpClusterOutput(command)
	cmd := &Cmd{
		Name:         name,
		CommandLine:  commandLine,
		GatherOutput: true,
	}
	cmd.Start(tCtx)
	cmd.Wait(tCtx)
}

func fromLocalUpClusterOutput(command string) []string {
	// 这里假设参数中不包含空格。
	// 我们不能把整个字符串交给 shell 去解析，因为有些参数包含
	// $ 之类的特殊字符，且没有对它们加引号。
	return strings.Split(command, " ")
}

// Stop 确保集群不再运行。
func (c *Cluster) Stop(tCtx ktesting.TContext) {
	tCtx.Helper()
	for _, component := range slices.Backward(components) {
		cmd := c.running[component]
		if cmd == nil {
			continue
		}
		tCtx.Logf("stopping %s", component)
		cmd.Stop(tCtx, "stopping cluster")
	}
}

// LoadConfig 返回正在运行的集群的 REST 配置。
func (c *Cluster) LoadConfig(tCtx ktesting.TContext) *restclient.Config {
	tCtx.Helper()
	cfg, err := clientcmd.LoadFromFile(c.kubeConfig)
	tCtx.ExpectNoError(err, "load KubeConfig")
	config, err := clientcmd.NewDefaultClientConfig(*cfg, nil).ClientConfig()
	tCtx.ExpectNoError(err, "construct REST config")
	return config

}

// GetSystemLogs 返回指定组件的输出；若该组件未启动，则返回空字符串和 false。
func (c *Cluster) GetSystemLogs(tCtx ktesting.TContext, component ClusterComponentName) (GString, bool) {
	cmd, ok := c.running[component]
	if !ok {
		return "", false
	}
	return GString(cmd.Output(tCtx)), true
}

// GString 适用于日志输出，因为与字符串的默认行为不同，它会从中间截断。
type GString string

var (
	_ format.GomegaStringer = GString("")
	_ fmt.Stringer          = GString("")
)

func (gs GString) GomegaString() string {
	s := string(gs)
	if len(s) < format.MaxLength {
		return s
	}
	elipsis := "\n...\n"
	keepLen := format.MaxLength - len(elipsis)
	if keepLen < 0 {
		// 无法从中间截断，否则结果会太长。交给 Gomega 处理。
		return s
	}
	return s[:keepLen/2] + elipsis + s[len(gs)-keepLen/2:]
}

func (gs GString) String() string {
	return string(gs)
}

type ModifyOptions struct {
	// BinDir 指定在哪里查找用于替换的 Kubernetes 组件。
	// 如果为空，则只修改 FileByComponent 中明确列出的组件。
	BinDir string

	// FileByComponent 为这里指定的组件覆盖 BinDir 的设置。
	FileByComponent map[ClusterComponentName]string

	// FeatureGatesByComponent 指定要应用到组件命令行上的 feature gate。
	FeatureGatesByComponent map[ClusterComponentName]string
}

func (m ModifyOptions) GetComponentFile(component ClusterComponentName) string {
	if file, ok := m.FileByComponent[component]; ok {
		return file
	}
	if m.BinDir != "" {
		return path.Join(m.BinDir, string(component))
	}
	return ""
}

// Modify 按照选项中的描述修改集群。
//
// 必须提供状态描述。它会被用作日志文件的文件名后缀，
// 也会出现在日志消息中。
// 在集群的整个生命周期内，该描述必须保持唯一。
//
// 返回的选项可以原样传给 Modify，以恢复原始状态。
func (c *Cluster) Modify(tCtx ktesting.TContext, state string, options ModifyOptions) ModifyOptions {
	tCtx.Helper()

	restore := ModifyOptions{
		FileByComponent: make(map[ClusterComponentName]string),
	}

	updated := make(map[ClusterComponentName]*Cmd)

	// 阶段 1：按逆序停止所有需要修改的组件，
	// 使依赖组件（KCM、scheduler）先于它们所依赖的 apiserver 停止。
	for _, component := range slices.Backward(KubeClusterComponents) {
		fileName := options.GetComponentFile(component)
		if fileName == "" && options.FeatureGatesByComponent[component] == "" {
			continue
		}
		tCtx := tCtx.WithStep(fmt.Sprintf("stop %s", component))
		cmd, ok := c.running[component]
		if !ok {
			tCtx.Fatal("not running")
		}
		tCtx.Logf("killing command %s before replacing it", cmd.Name)
		cmd.Stop(tCtx, "modifying the component")
		delete(c.running, component)
		tCtx.Logf("command %s with pid %d stopped: %s", cmd.Name, cmd.cmd.ProcessState.Pid(), cmd.cmd.ProcessState)
		dumpProcesses(tCtx)

		// 找到对应的命令（它可能包了一层 sudo！）。
		cmdLine := slices.Clone(cmd.CommandLine)

		// 如果需要，则替换二进制文件。
		if fileName != "" {
			found := false
			for i := range cmdLine {
				if path.Base(cmdLine[i]) == string(component) {
					found = true
					restore.FileByComponent[component] = cmdLine[i]
					cmdLine[i] = fileName
					break
				}
			}
			if !found {
				tCtx.Fatal("binary filename not found")
			}
		}
		cmd.Name = string(component) + "-" + state
		cmd.CommandLine = cmdLine
		cmd.LogFile = path.Join(c.dir, fmt.Sprintf("%s-%s.log", component, state))
		updated[component] = cmd
	}

	// 阶段 2：按照标准启动顺序（apiserver 优先）启动所有已停止的组件，
	// 使每个组件启动时面对的都是完全就绪的 apiserver。
	for _, component := range KubeClusterComponents {
		if cmd, ok := updated[component]; ok {
			// 应用 feature gate。
			if featureGates, ok := options.FeatureGatesByComponent[component]; ok {
				merged, err := mergeFeatureGatesFlags(cmd.CommandLine, featureGates)
				tCtx.ExpectNoError(err, "merge feature gates %s into %s command line", featureGates, component)
				cmd.CommandLine = merged
			}
			c.runComponentWithRetry(tCtx, component, cmd)
		}
	}

	return restore
}

func (c *Cluster) runComponentWithRetry(tCtx ktesting.TContext, component ClusterComponentName, cmd *Cmd) {
	// 有时组件会启动失败，必须重试。
	//
	// 例如，apiserver 的端口可能还没有释放（没有 SO_LINGER！）。
	// 又或者 kube-controller-manager：
	//  I0630 13:20:45.046709   61710 serving.go:380] Generated self-signed cert (/var/run/kubernetes/kube-controller-manager.crt, /var/run/kubernetes/kube-controller-manager.key)
	//  W0630 13:20:45.410578   61710 requestheader_controller.go:204] Unable to get configmap/extension-apiserver-authentication in kube-system.  Usually fixed by 'kubectl create rolebinding -n kube-system ROLEBINDING_NAME --role=extension-apiserver-authentication-reader --serviceaccount=YOUR_NS:YOUR_SA'
	//  E0630 13:20:45.410618   61710 run.go:72] "command failed" err="unable to load configmap based request-header-client-ca-file: configmaps \"extension-apiserver-authentication\" is forbidden: User \"system:kube-controller-manager\" cannot get resource \"configmaps\" in API group \"\" in the namespace \"kube-system\""
	// 随后 kube-controller-manager 就卡住了。也许它自己应该重试？
	for i := 0; ; i++ {
		tCtx.Logf("running %s with output redirected to %s", cmd.Name, cmd.LogFile)
		cmd.Start(tCtx)
		c.running[component] = cmd
		err := func() (finalErr error) {
			tCtx, finalize := tCtx.WithError(&finalErr)
			defer finalize()
			c.checkReadiness(tCtx, cmd)
			return nil
		}()
		if err == nil {
			break
		}
		if !cmd.Running() && i < 10 {
			// 重试。
			time.Sleep(time.Second)
			continue
		}
		// 重新抛出失败。
		tCtx.ExpectNoError(err)
	}
	tCtx.Logf("started %s with pid %d", cmd.Name, cmd.cmd.Process.Pid)
	dumpProcesses(tCtx)
}

func (c *Cluster) checkReadiness(tCtx ktesting.TContext, cmd *Cmd) {
	if strings.HasPrefix(cmd.Name, string(Etcd)) {
		c.checkEndpoint(tCtx, cmd, "http", c.settings["ETCD_HOST"], c.settings["ETCD_PORT"], "/health", nil)
		return
	}

	// 对于所有其他组件，我们期望 .kubeconfig 文件已经存在。
	restConfig := c.LoadConfig(tCtx)
	tCtx = tCtx.WithRESTConfig(restConfig)
	tCtx = tCtx.WithStep(fmt.Sprintf("wait for %s readiness", cmd.Name))

	// 对于 apiserver，我们使用带有集群 CA 的 admin 客户端证书。
	tlsConfig, err := restclient.TLSConfigFor(restConfig)
	if err != nil {
		tCtx.Fatalf("get TLS config for readiness check: %v", err)
	}

	// kubelet 的 /healthz 需要客户端认证。使用 admin 客户端证书，
	// 并设置 InsecureSkipVerify，因为 kubelet 使用的是自签名证书。
	tlsConfigWithClientCert := tlsConfig.Clone()
	tlsConfigWithClientCert.InsecureSkipVerify = true

	// 对于其他组件，可以跳过 TLS 验证，因为它们使用的是自签名证书。
	insecureTLSConfig := &tls.Config{InsecureSkipVerify: true}

	switch {
	case strings.HasPrefix(cmd.Name, string(KubeAPIServer)):
		c.checkReadyz(tCtx, cmd, "https", c.settings["API_HOST_IP"], c.settings["API_SECURE_PORT"], tlsConfig)
	case strings.HasPrefix(cmd.Name, string(KubeScheduler)):
		c.checkReadyz(tCtx, cmd, "https", c.settings["API_HOST_IP"], c.settings["SCHEDULER_SECURE_PORT"], insecureTLSConfig)
	case strings.HasPrefix(cmd.Name, string(KubeControllerManager)):
		// TODO：在 /readyz 实现并出现在所有测试的发行版中后切换过去。
		c.checkHealthz(tCtx, cmd, "https", c.settings["API_HOST_IP"], c.settings["KCM_SECURE_PORT"], insecureTLSConfig)
	case strings.HasPrefix(cmd.Name, string(KubeProxy)):
		// TODO：在 /readyz 实现并出现在所有测试的发行版中后切换过去。
		c.checkHealthz(tCtx, cmd, "http" /* not an error! */, c.settings["API_HOST_IP"], c.settings["PROXY_HEALTHZ_PORT"], insecureTLSConfig)
	case strings.HasPrefix(cmd.Name, string(Kubelet)):
		// TODO：在 /readyz 实现并出现在所有测试的发行版中后切换过去。
		c.checkHealthz(tCtx, cmd, "https", c.settings["KUBELET_HOST"], c.settings["KUBELET_PORT"], tlsConfigWithClientCert)

		// 同时等待节点就绪。
		tCtx.WithStep("wait for node ready").Eventually(func(tCtx ktesting.TContext) (*corev1.NodeList, error) {
			return tCtx.Client().CoreV1().Nodes().List(tCtx, metav1.ListOptions{})
		}).Should(gomega.HaveField("Items", gomega.ConsistOf(gomega.HaveField("Status.Conditions", gomega.ContainElement(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Type":   gomega.Equal(corev1.NodeReady),
			"Status": gomega.Equal(corev1.ConditionTrue),
		}))))))
	}
}

func (c *Cluster) checkHealthz(tCtx ktesting.TContext, cmd *Cmd, scheme, hostIP, port string, tlsConfig *tls.Config) {
	c.checkEndpoint(tCtx, cmd, scheme, hostIP, port, "/healthz", tlsConfig)
}

func (c *Cluster) checkReadyz(tCtx ktesting.TContext, cmd *Cmd, scheme, hostIP, port string, tlsConfig *tls.Config) {
	c.checkEndpoint(tCtx, cmd, scheme, hostIP, port, "/readyz", tlsConfig)
}

func (c *Cluster) checkEndpoint(tCtx ktesting.TContext, cmd *Cmd, scheme, hostIP, port, path string, tlsConfig *tls.Config) {
	url := fmt.Sprintf("%s://%s:%s%s", scheme, hostIP, port, path)
	tCtx.WithStep(fmt.Sprintf("check %s", url)).Eventually(func(tCtx ktesting.TContext) error {
		if !cmd.Running() {
			return gomega.StopTrying(fmt.Sprintf("%s stopped unexpectedly", cmd.Name))
		}
		req, err := http.NewRequestWithContext(tCtx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("get %s: %w", url, err)
		}
		if err := resp.Body.Close(); err != nil {
			return fmt.Errorf("close GET response: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%s returned %d, waiting for 200", url, resp.StatusCode)
		}
		return nil
	}).WithPolling(time.Second).Should(gomega.Succeed(), fmt.Sprintf("HTTP GET %s", url))
}

func dumpProcesses(tCtx ktesting.TContext) {
	// 取消注释以下代码可获得额外的调试输出。
	//
	// cmd := &Cmd{
	// 	Name:         "ps",
	// 	CommandLine:  []string{"ps", "-efww", "--forest"},
	// 	GatherOutput: true,
	// }
	// cmd.Start(tCtx)
	// processes := cmd.Wait(tCtx)
	// tCtx.Log(processes)
}

// ToggleFeatureGates 使用指定的 feature gate 重启受 feature gate 控制的组件。
// 返回的 ModifyOptions 可以传给 Modify，以恢复原始状态。
// 当前发行版中已锁定的 gate 会自动从 kubelet 的命令行中排除，
// 因为 kubelet 不支持 --emulated-version，会拒绝切换已锁定的 gate。
func (c *Cluster) ToggleFeatureGates(tCtx ktesting.TContext, state string, featureGates string) ModifyOptions {
	tCtx.Helper()

	opts := ModifyOptions{
		FileByComponent:         make(map[ClusterComponentName]string),
		FeatureGatesByComponent: make(map[ClusterComponentName]string),
	}

	allGates := utilfeature.DefaultMutableFeatureGate.GetAll()

	for _, component := range featureGatedComponents {
		gates := featureGates
		if component == Kubelet {
			gates = filterLockedFeatureGates(featureGates, allGates)
		}
		opts.FeatureGatesByComponent[component] = gates
	}

	return c.Modify(tCtx, state, opts)
}

// filterLockedFeatureGates 从 feature gate 字符串中移除那些已锁定为默认值的 gate。
// Kubelet 不支持 --emulated-version，会拒绝切换已锁定的 gate，
// 因此必须从它的命令行中排除已锁定的 gate。
func filterLockedFeatureGates(featureGates string, allGates map[featuregate.Feature]featuregate.FeatureSpec) string {
	filtered := slices.DeleteFunc(strings.Split(featureGates, ","), func(gate string) bool {
		gateName := strings.TrimSpace(strings.SplitN(gate, "=", 2)[0])
		spec, known := allGates[featuregate.Feature(gateName)]
		return known && spec.LockToDefault
	})
	return strings.Join(filtered, ",")
}

// mergeFeatureGatesFlags 将给定的 feature gate（采用 "fg1=true,fg2=false" 格式）
// 合并到命令行中。已有的 --feature-gates 标志会被合并在一起，
// 并被新的 gate 覆盖。结果中只保留一个 --feature-gates 标志，
// 位置与之前找到的第一个标志相同；如果之前没有，则追加到末尾。
// 如果最终没有任何 gate，则命令行原样返回。
//
// 如果 cmdLine 中的任何 --feature-gates 标志或 featureGates 参数本身格式错误，
// 则返回错误。
func mergeFeatureGatesFlags(cmdLine []string, featureGates string) ([]string, error) {
	gates := map[string]bool{}
	gatesValue := cliflag.NewMapStringBool(&gates)

	// 从命令行中去掉所有 --feature-gates 标记，把它们的值累积到
	// gatesValue 中。记住第一个标记的位置，以便合并后的标志放在同一位置。
	insertIdx := -1
	out := make([]string, 0, len(cmdLine))
	for _, tok := range cmdLine {
		value, ok := strings.CutPrefix(tok, "--feature-gates=")
		if !ok {
			out = append(out, tok)
			continue
		}
		if err := gatesValue.Set(value); err != nil {
			return nil, fmt.Errorf("invalid --feature-gates in command line %q: %w", tok, err)
		}
		if insertIdx == -1 {
			insertIdx = len(out)
		}
	}

	// 在此之上应用新的 gate（新值优先于已有值）。
	if featureGates != "" {
		if err := gatesValue.Set(featureGates); err != nil {
			return nil, fmt.Errorf("invalid feature gates %q: %w", featureGates, err)
		}
	}

	// 如果既没有找到可解析的已有标志，也没有产生任何 gate，则无需处理。
	if insertIdx == -1 && len(gates) == 0 {
		return out, nil
	}
	flag := "--feature-gates=" + gatesValue.String()
	if insertIdx != -1 {
		return slices.Insert(out, insertIdx, flag), nil
	}
	return append(out, flag), nil
}
