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

package framework

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	watchtools "k8s.io/client-go/tools/watch"
)

// 已弃用的常量。请改用 framework.Framework 中的超时设置。
const (
	// PodListTimeout 是等待 Pod 可被列出的时长。
	PodListTimeout = time.Minute

	// PodStartTimeout 是等待 Pod 启动的时长。
	PodStartTimeout = 5 * time.Minute

	// PodStartShortTimeout 与 `PodStartTimeout` 相同，但更短。
	// 在确信 Pod 启动不会延迟的情况下视情况使用。
	// 因 docker 拉取缓慢或其他原因而延迟几分钟。
	PodStartShortTimeout = 2 * time.Minute

	// PodDeleteTimeout 是等待 Pod 被删除的时长。
	PodDeleteTimeout = 5 * time.Minute

	// PodGetTimeout 是等待获取 Pod 的时长。
	PodGetTimeout = 2 * time.Minute

	// PodEventTimeout 是等待 Pod 事件发生的时长。
	PodEventTimeout = 2 * time.Minute

	// ServiceStartTimeout 是等待 service endpoint 可解析的时长。
	ServiceStartTimeout = 3 * time.Minute

	// Poll 是轮询 Pod、节点和 claim 的频率。
	Poll = 2 * time.Second

	// PollShortTimeout 是轮询中的较短超时值。
	PollShortTimeout = 1 * time.Minute

	// ServiceAccountProvisionTimeout 是等待 service account 被供给的时长。
	// service account 在命名空间创建后供给
	// 作为 admission control 的一部分，在命名空间中创建 Pod 需要有 service account
	ServiceAccountProvisionTimeout = 2 * time.Minute

	// SingleCallTimeout 是尝试单个 API 调用（如 'get' 或 'list'）的时长，用于防止
	// 瞬态失败导致测试失败。
	SingleCallTimeout = 5 * time.Minute

	// NodeReadyInitialTimeout 是测试开始时节点必须处于 "ready" 状态的时长。测试开始前它们就应
	// 已 "ready"，因此该值很小。
	NodeReadyInitialTimeout = 20 * time.Second

	// PodReadyBeforeTimeout 是测试开始时 Pod 必须处于 "ready" 状态的时长。
	PodReadyBeforeTimeout = 5 * time.Minute

	// ClaimProvisionShortTimeout 与 `ClaimProvisionTimeout` 相同，但更短。
	// 在确信该超时足够时视情况使用。
	ClaimProvisionShortTimeout = 1 * time.Minute

	// ClaimProvisionTimeout 是等待 claim 完成动态供给的时长。
	ClaimProvisionTimeout = 5 * time.Minute

	// RestartNodeReadyAgainTimeout 是节点重启后允许多长时间重新变为 "Ready"，
	// 超过则测试失败。
	RestartNodeReadyAgainTimeout = 5 * time.Minute

	// RestartPodReadyAgainTimeout 是节点重启后允许 Pod 多长时间重新变为 "running" 和 "ready"，
	// 超过则测试失败。
	RestartPodReadyAgainTimeout = 5 * time.Minute

	// SnapshotCreateTimeout 是等待 snapshot 创建 snapshotContent 的时长。
	SnapshotCreateTimeout = 5 * time.Minute

	// SnapshotDeleteTimeout 是等待 snapshot 删除 snapshotContent 的时长。
	SnapshotDeleteTimeout = 5 * time.Minute

	// ControlPlaneLabel 仅对基于 kubeadm 的集群（如 kops）是有效标签
	ControlPlaneLabel = "node-role.kubernetes.io/control-plane"
)

var (
	// ProvidersWithSSH 是指每个节点都可通过 SSH 访问的 provider
	ProvidersWithSSH = []string{"gce", "aws", "local", "azure"}
)

// RunID 是 e2e 运行的唯一标识符。
// 注意，e2e 运行中所有测试的该 ID 并不相同，因为每个 Ginkgo 节点都会单独创建它。
var RunID = uuid.NewUUID()

// CreateTestingNSFn 负责创建用于执行 e2e 测试的命名空间。
type CreateTestingNSFn func(ctx context.Context, baseName string, c clientset.Interface, labels map[string]string) (*v1.Namespace, error)

// APIAddress 返回实例的地址。
func APIAddress() string {
	instanceURL, err := url.Parse(TestContext.Host)
	ExpectNoError(err)
	return instanceURL.Hostname()
}

// 如果 provider 包含在 providers 中，ProviderIs 返回 true，否则返回 false。
func ProviderIs(providers ...string) bool {
	for _, provider := range providers {
		if strings.EqualFold(provider, TestContext.Provider) {
			return true
		}
	}
	return false
}

// 如果 master OS 发行版包含在 supportedMasterOsDistros 中，MasterOSDistroIs 返回 true，否则返回 false。
func MasterOSDistroIs(supportedMasterOsDistros ...string) bool {
	for _, distro := range supportedMasterOsDistros {
		if strings.EqualFold(distro, TestContext.MasterOSDistro) {
			return true
		}
	}
	return false
}

// 如果节点 OS 发行版包含在 supportedNodeOsDistros 中，NodeOSDistroIs 返回 true，否则返回 false。
func NodeOSDistroIs(supportedNodeOsDistros ...string) bool {
	for _, distro := range supportedNodeOsDistros {
		if strings.EqualFold(distro, TestContext.NodeOSDistro) {
			return true
		}
	}
	return false
}

// 如果节点 OS 架构包含在 supportedNodeOsArchs 中，NodeOSArchIs 返回 true，否则返回 false。
func NodeOSArchIs(supportedNodeOsArchs ...string) bool {
	for _, arch := range supportedNodeOsArchs {
		if strings.EqualFold(arch, TestContext.NodeOSArch) {
			return true
		}
	}
	return false
}

// DeleteNamespaces 删除所有匹配给定 delete 和 skip 过滤器的命名空间。
// 过滤器使用简单的 strings.Contains；先应用 skip 过滤器，再应用 delete 过滤器。
// 返回已删除命名空间的列表或错误。
func DeleteNamespaces(ctx context.Context, c clientset.Interface, deleteFilter, skipFilter []string) ([]string, error) {
	ginkgo.By("Deleting namespaces")
	nsList, err := c.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	ExpectNoError(err, "Failed to get namespace list")
	var deleted []string
	var wg sync.WaitGroup
OUTER:
	for _, item := range nsList.Items {
		for _, pattern := range skipFilter {
			if strings.Contains(item.Name, pattern) {
				continue OUTER
			}
		}
		if deleteFilter != nil {
			var shouldDelete bool
			for _, pattern := range deleteFilter {
				if strings.Contains(item.Name, pattern) {
					shouldDelete = true
					break
				}
			}
			if !shouldDelete {
				continue OUTER
			}
		}
		wg.Add(1)
		deleted = append(deleted, item.Name)
		go func(nsName string) {
			defer wg.Done()
			defer ginkgo.GinkgoRecover()
			gomega.Expect(c.CoreV1().Namespaces().Delete(ctx, nsName, metav1.DeleteOptions{})).To(gomega.Succeed())
			Logf("namespace : %v api call to delete is complete ", nsName)
		}(item.Name)
	}
	wg.Wait()
	return deleted, nil
}

// WaitForNamespacesDeleted 等待命名空间被删除。
func WaitForNamespacesDeleted(ctx context.Context, c clientset.Interface, namespaces []string, timeout time.Duration) error {
	ginkgo.By(fmt.Sprintf("Waiting for namespaces %+v to vanish", namespaces))
	nsMap := map[string]bool{}
	for _, ns := range namespaces {
		nsMap[ns] = true
	}
	// 现在轮询直到所有命名空间都被清除。
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, false,
		func(ctx context.Context) (bool, error) {
			nsList, err := c.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			for _, item := range nsList.Items {
				if _, ok := nsMap[item.Name]; ok {
					return false, nil
				}
			}
			return true, nil
		})
}

func waitForConfigMapInNamespace(ctx context.Context, c clientset.Interface, ns, name string, timeout time.Duration) error {
	fieldSelector := fields.OneTermEqualSelector("metadata.name", name).String()
	ctx, cancel := watchtools.ContextWithOptionalTimeout(ctx, timeout)
	defer cancel()
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (object runtime.Object, e error) {
			options.FieldSelector = fieldSelector
			return c.CoreV1().ConfigMaps(ns).List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (i watch.Interface, e error) {
			options.FieldSelector = fieldSelector
			return c.CoreV1().ConfigMaps(ns).Watch(ctx, options)
		},
	}
	_, err := watchtools.UntilWithSync(ctx, lw, &v1.ConfigMap{}, nil, func(event watch.Event) (bool, error) {
		switch event.Type {
		case watch.Deleted:
			return false, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, name)
		case watch.Added, watch.Modified:
			return true, nil
		}
		return false, nil
	})
	return err
}

func waitForServiceAccountInNamespace(ctx context.Context, c clientset.Interface, ns, serviceAccountName string, timeout time.Duration) error {
	fieldSelector := fields.OneTermEqualSelector("metadata.name", serviceAccountName).String()
	ctx, cancel := watchtools.ContextWithOptionalTimeout(ctx, timeout)
	defer cancel()
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (object runtime.Object, e error) {
			options.FieldSelector = fieldSelector
			return c.CoreV1().ServiceAccounts(ns).List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (i watch.Interface, e error) {
			options.FieldSelector = fieldSelector
			return c.CoreV1().ServiceAccounts(ns).Watch(ctx, options)
		},
	}
	_, err := watchtools.UntilWithSync(ctx, lw, &v1.ServiceAccount{}, nil, func(event watch.Event) (bool, error) {
		switch event.Type {
		case watch.Deleted:
			return false, apierrors.NewNotFound(schema.GroupResource{Resource: "serviceaccounts"}, serviceAccountName)
		case watch.Added, watch.Modified:
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("wait for service account %q in namespace %q: %w", serviceAccountName, ns, err)
	}
	return nil
}

// WaitForDefaultServiceAccountInNamespace 等待 default service account 被供给
// 当 Pod 未指定 service account 时，会关联 default service account
// 因此，在 service account 供给完成前，无法在命名空间中创建 Pod
func WaitForDefaultServiceAccountInNamespace(ctx context.Context, c clientset.Interface, namespace string) error {
	return waitForServiceAccountInNamespace(ctx, c, namespace, defaultServiceAccountName, ServiceAccountProvisionTimeout)
}

// WaitForKubeRootCAInNamespace 等待包含 service account CA trust bundle 的
// configmap kube-root-ca.crt 在指定命名空间中被供给，这样 Pod 就不必重试挂载
// 该 config map（会产生噪音，掩盖 Kubelet 中的其他问题）。
func WaitForKubeRootCAInNamespace(ctx context.Context, c clientset.Interface, namespace string) error {
	return waitForConfigMapInNamespace(ctx, c, namespace, "kube-root-ca.crt", ServiceAccountProvisionTimeout)
}

// 每个测试都应使用 CreateTestingNS。注意我们会在提供的测试名称前加上一个公共前缀。
// 请改用 NewFramework，而不是直接使用它。
func CreateTestingNS(ctx context.Context, baseName string, c clientset.Interface, labels map[string]string) (*v1.Namespace, error) {
	if labels == nil {
		labels = map[string]string{}
	}
	labels["e2e-run"] = string(RunID)

	// 我们不使用 ObjectMeta.GenerateName 特性，因为 API 调用
	// 失败时我们无法知道命名空间是否已创建及其名称是什么。
	// name.
	name := fmt.Sprintf("%v-%v", baseName, RandomSuffix())

	namespaceObj := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "",
			Labels:    labels,
		},
		Status: v1.NamespaceStatus{},
	}
	// 对创建命名空间的调用要健壮。
	var got *v1.Namespace
	if err := wait.PollUntilContextTimeout(ctx, Poll, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		var err error
		got, err = c.CoreV1().Namespaces().Create(ctx, namespaceObj, metav1.CreateOptions{})
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				// 冲突时重新生成
				Logf("Namespace name %q was already taken, generate a new name and retry", namespaceObj.Name)
				namespaceObj.Name = fmt.Sprintf("%v-%v", baseName, RandomSuffix())
			} else {
				Logf("Unexpected error while creating namespace: %v", err)
			}
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	if TestContext.VerifyServiceAccount {
		if err := WaitForDefaultServiceAccountInNamespace(ctx, c, got.Name); err != nil {
			// 即使在该命名空间中创建 serviceAccount 失败，
			// 我们也已成功创建了命名空间。
			// 因此返回已创建的命名空间。
			return got, err
		}
	}
	return got, nil
}

// CheckTestingNSDeletedExcept 检查所有现存的 e2e 命名空间是否处于 Terminating 状态，
// 并等待它们最终被删除。它会忽略要跳过的命名空间。
func CheckTestingNSDeletedExcept(ctx context.Context, c clientset.Interface, skip string) error {
	// TODO：由于 API 不支持批量删除资源，
	// 删除命名空间时我们逐个删除其中的所有对象
	//（一次删除等于一次 API 调用）。这基本上面临
	// 限流 —— 目前 controller-manager 的上限是 20 QPS。
	// 一旦 #10217 实现并用于 namespace-controller，删除给定命名空间中的所有
	// 对象应该会快得多，我们就能
	// 降低这个超时。
	// 然而，现在 Density 测试产生约 26000 个事件，Load capacity 测试
	// 产生约 35000 个事件。假设没有其他请求，完全删除命名空间
	// 大约需要 30 分钟。因此我将其设为 60
	// 分钟，以避免此处超时。
	timeout := 60 * time.Minute

	Logf("Waiting for terminating namespaces to be deleted...")
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(15 * time.Second) {
		namespaces, err := c.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err != nil {
			Logf("Listing namespaces failed: %v", err)
			continue
		}
		terminating := 0
		for _, ns := range namespaces.Items {
			if strings.HasPrefix(ns.ObjectMeta.Name, "e2e-tests-") && ns.ObjectMeta.Name != skip {
				if ns.Status.Phase == v1.NamespaceActive {
					return fmt.Errorf("Namespace %s is active", ns.ObjectMeta.Name)
				}
				terminating++
			}
		}
		if terminating == 0 {
			return nil
		}
	}
	return fmt.Errorf("Waiting for terminating namespaces to be deleted timed out")
}

// restclientConfig 返回一个配置，其中包含构建到 Kubernetes 集群连接所需的信息。
func restclientConfig(kubeContext string) (*clientcmdapi.Config, error) {
	Logf(">>> kubeConfig: %s", TestContext.KubeConfig)
	if TestContext.KubeConfig == "" {
		return nil, fmt.Errorf("KubeConfig must be specified to load client config")
	}
	c, err := clientcmd.LoadFromFile(TestContext.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("error loading KubeConfig: %v", err.Error())
	}
	if kubeContext != "" {
		Logf(">>> kubeContext: %s", kubeContext)
		c.CurrentContext = kubeContext
	}
	return c, nil
}

// ClientConfigGetter 是一个返回 getter 的函数，该 getter 返回配置。
type ClientConfigGetter func() (*restclient.Config, error)

// LoadConfig 返回一个 rest client 配置，其 UserAgent 包含当前测试名称。
func LoadConfig() (config *restclient.Config, err error) {
	defer func() {
		if err == nil && config != nil {
			testDesc := ginkgo.CurrentSpecReport()
			if len(testDesc.ContainerHierarchyTexts) > 0 {
				testName := strings.Join(testDesc.ContainerHierarchyTexts, " ")
				if len(testDesc.LeafNodeText) > 0 {
					testName = testName + " " + testDesc.LeafNodeText
				}
				config.UserAgent = fmt.Sprintf("%s -- %s", restclient.DefaultKubernetesUserAgent(), testName)
			}
		}
	}()

	if TestContext.NodeE2E {
		// 这是 node e2e 测试，应用 node e2e 配置
		return &restclient.Config{
			Host:        TestContext.Host,
			BearerToken: TestContext.BearerToken,
			TLSClientConfig: restclient.TLSClientConfig{
				Insecure: true,
			},
		}, nil
	}
	c, err := restclientConfig(TestContext.KubeContext)
	if err != nil {
		if TestContext.KubeConfig == "" {
			return restclient.InClusterConfig()
		}
		return nil, err
	}
	// 如果 TestContext 中未设置 Host，则将其设为
	// k8s API client 要连接的 CurrentContext Server。
	if TestContext.Host == "" && c.Clusters != nil {
		currentContext, ok := c.Clusters[c.CurrentContext]
		if ok {
			TestContext.Host = currentContext.Server
		}
	}

	return clientcmd.NewDefaultClientConfig(*c, &clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: TestContext.Host}}).ClientConfig()
}

// LoadClientset 返回用于连接 Kubernetes 集群的 clientset。
func LoadClientset() (*clientset.Clientset, error) {
	config, err := LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err.Error())
	}
	return clientset.NewForConfig(config)
}

// RandomSuffix 提供一个随机序列，用于附加到 pod、service、rc 上。
func RandomSuffix() string {
	return strconv.Itoa(rand.Intn(10000))
}

// StartCmdAndStreamOutput 启动给定 cmd 后返回 stdout 和 stderr。
func StartCmdAndStreamOutput(cmd *exec.Cmd) (stdout, stderr io.ReadCloser, err error) {
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		return
	}
	stderr, err = cmd.StderrPipe()
	if err != nil {
		return
	}
	// cmd.Args 的第 0 个参数是命令本身，因此只需
	// 打印第 1 个及之后的参数即可。
	Logf("Asynchronously running '%s %s'", cmd.Path, strings.Join(cmd.Args[1:], " "))
	err = cmd.Start()
	return
}

// TryKill 大致相当于 ctrl+c，用于清理进程。应在 defer 中运行。
func TryKill(cmd *exec.Cmd) {
	if err := cmd.Process.Kill(); err != nil {
		Logf("ERROR failed to kill command %v! The process may leak", cmd)
	}
}

// EnsureLoadBalancerResourcesDeleted 确保创建的云负载均衡器资源
// 确实被清理。目前仅针对 GCE/GKE 实现。
func EnsureLoadBalancerResourcesDeleted(ctx context.Context, ip, portRange string) error {
	return TestContext.CloudConfig.Provider.EnsureLoadBalancerResourcesDeleted(ctx, ip, portRange)
}

// CoreDump 通过 SSH 登录 master 和所有节点，将日志 dump 到 dir。
// 它调用 cluster/log-dump/log-dump.sh 来完成。
func CoreDump(dir string) {
	if TestContext.DisableLogDump {
		Logf("Skipping dumping logs from cluster")
		return
	}
	var cmd *exec.Cmd
	if TestContext.LogexporterGCSPath != "" {
		Logf("Dumping logs from nodes to GCS directly at path: %s", TestContext.LogexporterGCSPath)
		cmd = exec.Command(path.Join(TestContext.RepoRoot, "cluster", "log-dump", "log-dump.sh"), dir, TestContext.LogexporterGCSPath)
	} else {
		Logf("Dumping logs locally to: %s", dir)
		cmd = exec.Command(path.Join(TestContext.RepoRoot, "cluster", "log-dump", "log-dump.sh"), dir)
	}
	env := os.Environ()
	env = append(env, fmt.Sprintf("LOG_DUMP_SYSTEMD_SERVICES=%s", parseSystemdServices(TestContext.SystemdServices)))
	env = append(env, fmt.Sprintf("LOG_DUMP_SYSTEMD_JOURNAL=%v", TestContext.DumpSystemdJournal))
	cmd.Env = env

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		Logf("Error running cluster/log-dump/log-dump.sh: %v", err)
	}
}

// parseSystemdServices 将 service 分隔符从逗号转换为空格。
func parseSystemdServices(services string) string {
	return strings.TrimSpace(strings.Replace(services, ",", " ", -1))
}

// RunCmd 用 args 运行 cmd 并返回其 stdout 和 stderr，同时
// 将 cmd 的 stdout 和 stderr 输出到相应的 OS 流。
func RunCmd(command string, args ...string) (string, string, error) {
	return RunCmdEnv(nil, command, args...)
}

// RunCmdEnv 用给定的环境和参数运行 cmd，并
// 返回其 stdout 和 stderr，同时也会输出 cmd 的 stdout 和
// stderr 到相应的 OS 流。
func RunCmdEnv(env []string, command string, args ...string) (string, string, error) {
	Logf("Running %s %v", command, args)
	var bout, berr bytes.Buffer
	cmd := exec.Command(command, args...)
	// 我们还输出到 OS stdout/stderr，以便在 cmd
	// 挂起且测试被杀之前一直不返回时辅助调试。
	//
	// 这会产生一些难看的输出，因为 gcloud 并不总是提供
	// newlines.
	cmd.Stdout = io.MultiWriter(os.Stdout, &bout)
	cmd.Stderr = io.MultiWriter(os.Stderr, &berr)
	cmd.Env = env
	err := cmd.Run()
	stdout, stderr := bout.String(), berr.String()
	if err != nil {
		return "", "", fmt.Errorf("error running %s %v; got error %v, stdout %q, stderr %q",
			command, args, err, stdout, stderr)
	}
	return stdout, stderr, nil
}

// 如果节点有外部 IP，GetNodeExternalIPs 返回其外部 IP 地址列表
func GetNodeExternalIPs(node *v1.Node) (ips []string) {
	for j := range node.Status.Addresses {
		nodeAddress := &node.Status.Addresses[j]
		if nodeAddress.Type == v1.NodeExternalIP && nodeAddress.Address != "" {
			ips = append(ips, nodeAddress.Address)
		}
	}
	return
}

// GetControlPlaneNodes 返回 control plane 节点列表
func GetControlPlaneNodes(ctx context.Context, c clientset.Interface) *v1.NodeList {
	allNodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	ExpectNoError(err, "error reading all nodes")

	var cpNodes v1.NodeList

	for _, node := range allNodes.Items {
		// 检查 control plane 标签
		if _, hasLabel := node.Labels[ControlPlaneLabel]; hasLabel {
			cpNodes.Items = append(cpNodes.Items, node)
			continue
		}

		// 检查特定的 taint
		for _, taint := range node.Spec.Taints {
			// 注意，taint key 与 control plane 标签相同
			if taint.Key == ControlPlaneLabel && taint.Effect == v1.TaintEffectNoSchedule {
				cpNodes.Items = append(cpNodes.Items, node)
				continue
			}
		}
	}

	return &cpNodes
}

// PrettyPrintJSON 将 metrics 转换为 JSON 格式。
func PrettyPrintJSON(metrics interface{}) string {
	output := &bytes.Buffer{}
	if err := json.NewEncoder(output).Encode(metrics); err != nil {
		Logf("Error building encoder: %v", err)
		return ""
	}
	formatted := &bytes.Buffer{}
	if err := json.Indent(formatted, output.Bytes(), "", "  "); err != nil {
		Logf("Error indenting: %v", err)
		return ""
	}
	return formatted.String()
}

// WatchEventSequenceVerifier ...
// 管理给定资源的 watch，确保事件按给定顺序发生，失败时重试测试
//
//	ctx                 跨 API 边界的取消信号，例如来自 Ginkgo 的 context
//	dc                  设置到 API 的 client
//	resourceType       指定资源类型
//	namespace          选择一个命名空间
//	resourceName       给定资源的名称
//	listOptions        查找资源时使用的选项，建议使用 listOptions.labelSelector
//	expectedWatchEvents 预期发生的事件数组
//	scenario           测试本身
//	retryCleanup       测试失败时运行的函数，用于确保没有残留资源
//
// 此工具依赖测试在事件发生时返回事件
// 必须运行整个场景，以确保所需的 watch 事件按顺序到达（允许 watch 事件交织）
//
//	scenario           测试本身
//
// 场景尝试三次，允许顺序有几次失败
func WatchEventSequenceVerifier(ctx context.Context, dc dynamic.Interface, resourceType schema.GroupVersionResource, namespace string, resourceName string, listOptions metav1.ListOptions, expectedWatchEvents []watch.Event, scenario func(*watchtools.RetryWatcher) []watch.Event, retryCleanup func() error) {
	listWatcher := &cache.ListWatch{
		WatchFunc: func(listOptions metav1.ListOptions) (watch.Interface, error) {
			return dc.Resource(resourceType).Namespace(namespace).Watch(ctx, listOptions)
		},
	}

	retries := 3
retriesLoop:
	for try := 1; try <= retries; try++ {
		initResource, err := dc.Resource(resourceType).Namespace(namespace).List(ctx, listOptions)
		ExpectNoError(err, "Failed to fetch initial resource")

		resourceWatch, err := watchtools.NewRetryWatcher(initResource.GetResourceVersion(), listWatcher)
		ExpectNoError(err, "Failed to create a resource watch of %v in namespace %v", resourceType.Resource, namespace)

		// 注意，测试可能需要访问事件以了解情况，例如 status 的变化
		actualWatchEvents := scenario(resourceWatch)
		errs := sets.NewString()
		gomega.Expect(len(expectedWatchEvents)).To(gomega.BeNumerically("<=", len(actualWatchEvents)), "Did not get enough watch events")

		totalValidWatchEvents := 0
		foundEventIndexes := map[int]*int{}

		for watchEventIndex, expectedWatchEvent := range expectedWatchEvents {
			foundExpectedWatchEvent := false
		actualWatchEventsLoop:
			for actualWatchEventIndex, actualWatchEvent := range actualWatchEvents {
				if foundEventIndexes[actualWatchEventIndex] != nil {
					continue actualWatchEventsLoop
				}
				if actualWatchEvent.Type == expectedWatchEvent.Type {
					foundExpectedWatchEvent = true
					foundEventIndexes[actualWatchEventIndex] = &watchEventIndex
					break actualWatchEventsLoop
				}
			}
			if !foundExpectedWatchEvent {
				errs.Insert(fmt.Sprintf("Watch event %v not found", expectedWatchEvent.Type))
			}
			totalValidWatchEvents++
		}
		err = retryCleanup()
		ExpectNoError(err, "Error occurred when cleaning up resources")
		if errs.Len() > 0 && try < retries {
			fmt.Println("invariants violated:\n", strings.Join(errs.List(), "\n - "))
			continue retriesLoop
		}
		if errs.Len() > 0 {
			Failf("Unexpected error(s): %v", strings.Join(errs.List(), "\n - "))
		}
		gomega.Expect(expectedWatchEvents).To(gomega.HaveLen(totalValidWatchEvents), "Error: there must be an equal amount of total valid watch events (%d) and expected watch events (%d)", totalValidWatchEvents, len(expectedWatchEvents))
		break retriesLoop
	}
}
