/*
Copyright 2016 The Kubernetes Authors.

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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/ginkgo/v2/types"
	"github.com/onsi/gomega"
	gomegaformat "github.com/onsi/gomega/format"

	"k8s.io/apimachinery/pkg/util/sets"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e/framework/internal/junit"
	"k8s.io/kubernetes/test/utils/image"
	"k8s.io/kubernetes/test/utils/kubeconfig"
)

const (
	defaultHost = "https://127.0.0.1:6443"

	// DefaultNumNodes 是节点数量。未指定时会自动探测节点数
	DefaultNumNodes = -1
)

var (
	// Output 用于非测试运行时的输出，例如 -list-tests。
	// 测试输出应写入 ginkgo.GinkgoWriter。
	Output io.Writer = os.Stdout

	// framework 检测到致命错误时调用 Exit，
	// 执行完 -list-tests 等操作后也会调用。
	Exit = os.Exit

	// CheckForBugs 决定测试初始化发现 bug 时 framework 是否中止。
	//
	CheckForBugs = true
)

// TestContextType 包含测试设置和全局状态。由于历史原因，它是
// 测试框架自身、云 provider 和各个测试各自管理的项目的混合体。
//
// 目标是把框架不需要的所有内容
// 移到使用这些设置的代码中。
//
// 对这些设置的建议是：
//   - 存放在各自的 context 结构体或局部变量
//     中。
//   - 使用标准 `flag` 包注册。
//     flag 名应遵循 <part1>.<part2>....<partn> 模式，
//     前缀不太可能与其他测试或
//     标准包冲突，每部分用 lower camel case。例
//     如，test/e2e/storage/csi/context.go 可以定义
//     storage.csi.numIterations。
//   - 可用 framework/config 通过一次函数调用简化
//     多个选项的注册：
//     var storageCSI {
//     NumIterations `default:"1" usage:"number of iterations"`
//     }
//     _ config.AddOptions(&storageCSI, "storage.csi")
//   - 测试中直接使用 Viper 是可行的，但不推荐，因为
//     它只在使用 Viper 的测试套件中有效（并非必需），且支持的选项无法被
//     测试套件使用者发现。
//
// 测试套件作者可用 framework/viper 让所有命令行参数
// 也可以通过配置文件配置。
type TestContextType struct {
	KubeConfig             string
	KubeContext            string
	KubeAPIContentType     string
	KubeletRootDir         string
	KubeletConfigDropinDir string
	CertDir                string
	Host                   string
	BearerToken            string `datapolicy:"token"`
	// TODO: 逐步弃用它……改用 gobindata_util.go，见 #23987。
	RepoRoot string
	// ListImages 列出所有使用到的镜像然后退出
	ListImages bool

	listTests, listLabels bool

	// ListConformanceTests 列出所有可用的 conformance 测试然后退出
	ListConformanceTests bool

	// Provider 标识基础设施 provider（gce、gke、aws）
	Provider string

	// Tooling 是使用的工具链（例如 kops、gke）。Provider 是云 provider，可能无法唯一确定工具链。
	Tooling string

	// timeouts 包含各种操作的用户可配置超时时间。
	// 各个 Framework 实例也有自己的超时时间，可能
	// 与这里的不同。为避免混淆，该字段不
	// 导出。其值可通过
	// NewTimeoutContext 访问。
	timeouts TimeoutContext

	CloudConfig                 CloudConfig
	KubectlPath                 string
	OutputDir                   string
	ReportDir                   string
	ReportPrefix                string
	ReportCompleteGinkgo        bool
	ReportCompleteJUnit         bool
	Prefix                      string
	MinStartupPods              int
	EtcdUpgradeStorage          string
	EtcdUpgradeVersion          string
	GCEUpgradeScript            string
	ContainerRuntimeEndpoint    string
	ContainerRuntimeProcessName string
	ContainerRuntimePidFile     string
	// SystemdServices 是测试框架要 dump 日志的 systemd 服务列表，
	// 以逗号分隔。
	SystemdServices string
	// DumpSystemdJournal 控制是否 dump 完整的 systemd journal。
	DumpSystemdJournal       bool
	ImageServiceEndpoint     string
	MasterOSDistro           string
	NodeOSDistro             string
	NodeOSArch               string
	VerifyServiceAccount     bool
	DeleteNamespace          bool
	DeleteNamespaceOnFailure bool
	AllowedNotReadyNodes     int
	CleanStart               bool
	// 设为 'true' 或 'all' 时，framework 会启动 goroutine 监控系统 add-on 的资源使用。
	// 每 30 秒从所有 Node 读取数据，并在 afterEach 打印摘要。设为 'master' 时
	// 只监控 master Node。
	GatherKubeSystemResourceUsageData string
	GatherLogsSizes                   bool
	GatherMetricsAfterTest            string
	GatherSuiteMetricsAfterTest       bool
	MaxNodesToGather                  int
	// 设为 'true' 时，framework 在收集其他组件的指标时也会收集 ClusterAutoscaler 指标。
	IncludeClusterAutoscalerMetrics bool
	// 当前支持的值为 'hr'（可读格式）和 'json'。以逗号分隔的列表。
	OutputPrintType string
	// CreateTestingNS 负责创建执行 e2e 测试使用的命名空间。
	// 接受命名空间基础名，会加上 e2e 前缀，并传入 kube client
	// 以及要打到命名空间上的 label。
	CreateTestingNS CreateTestingNSFn
	// 设为 true 时测试会 dump 运行所在命名空间的数据。
	DumpLogsOnFailure bool
	// 禁止在所有测试结束后从 master 和节点 dump 集群日志。
	DisableLogDump bool
	// 用于从节点 dump 日志的 GCS artifacts 目录路径。非空时启用 Logexporter。
	LogexporterGCSPath string
	// node e2e 特有的测试上下文
	NodeTestContextType

	// 集群的 DNS 域。
	ClusterDNSDomain string

	// 集群的默认 IP family（"ipv4" 或 "ipv6"）
	IPFamily string

	// NonblockingTaints 是用户提供的逗号分隔字符串，指定不应阻止测试框架运行测试的 taint。
	NonblockingTaints string

	// ProgressReportURL 是测试完成时发送进度更新的 URL。为空则不发送。
	ProgressReportURL string

	// SriovdpConfigMapFile 是在本机配置 SRIOV 设备插件的 ConfigMap 路径。
	SriovdpConfigMapFile string

	// SpecSummaryOutput 是测试完成时写入 ginkgo.SpecSummary 对象的文件。便于调试和测试内省。
	SpecSummaryOutput string

	// DockerConfigFile 是包含凭据的文件，测试需要它从某些私有 registry 拉取镜像。
	DockerConfigFile string

	// E2EDockerConfigFile 是 docker 凭据配置文件，包含用户提供的、可从某些私有 registry 拉取镜像的授权 token。
	// 详情参见 https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/#log-in-to-docker-hub
	E2EDockerConfigFile string

	// KubeTestRepoConfigFile 是用于覆盖测试镜像 registry 的 yaml 文件。
	KubeTestRepoList string

	// SnapshotControllerPodName 是识别 snapshot controller Pod 使用的名称。
	SnapshotControllerPodName string

	// SnapshotControllerHTTPPort 是与 snapshot controller HTTP 端点通信使用的端口。
	SnapshotControllerHTTPPort int

	// RequireDevices 要求运行测试的环境暴露至少 1 个通过设备插件上报的设备。
	// 启用后，需要设备才能运行的 e2e 测试可以假定：若设备未上报，测试可以失败
	RequireDevices bool

	// 启用默认禁用的卷驱动。详见 test/e2e/storage/in_tree_volumes.go。
	EnabledVolumeDrivers []string
}

// NodeTestContextType 是 TestContextType 的一部分，由所有 node e2e 测试共享。
type NodeTestContextType struct {
	// NodeE2E 表示当前是否运行 node e2e。
	NodeE2E bool
	// 运行测试的节点名。
	NodeName string
	// NodeConformance 表示测试是否以 node conformance 模式运行。
	NodeConformance bool
	// PrepullImages 表示 node e2e framework 是否应预拉取镜像。
	PrepullImages bool
	// ImageDescription 是运行测试所用镜像的描述。
	ImageDescription string
	// RuntimeConfig 是 API server runtime 配置值的 map。
	RuntimeConfig map[string]string
	// SystemSpecName 是 node e2e 测试中使用的 system spec 名称（例如 gke）。
	// 为空时使用默认值（system.DefaultSpec）。system spec 位于
	// test/e2e_node/system/specs/。
	SystemSpecName string
	// RestartKubelet 在进程被杀时重启 Kubelet unit。
	RestartKubelet bool
	// ExtraEnvs 是环境变量名到值的 map。
	ExtraEnvs map[string]string
	// StandaloneMode 表示测试是否以 standalone 模式运行 kubelet。
	StandaloneMode bool
	// CriProxyEnabled 表示为故障注入启用 CRI API proxy。
	CriProxyEnabled bool
}

// CloudConfig 保存 e2e 测试套件的云配置。
type CloudConfig struct {
	APIEndpoint       string
	ProjectID         string
	Zone              string   // 用于 multizone 测试，任选一个 zone
	Zones             []string // 用于 multizone 测试，使用这组 zone 而不查询云 provider。必须包含 Zone。
	Region            string
	MultiZone         bool
	MultiMaster       bool
	Cluster           string
	MasterName        string
	NodeInstanceGroup string // 以逗号分隔的 group 名称列表
	NumNodes          int
	ClusterIPRange    string
	ClusterTag        string
	Network           string
	ConfigFile        string // 用于 azure
	NodeTag           string
	MasterTag         string

	Provider ProviderInterface
}

// 所有测试都应使用 TestContext 访问公共上下文数据。
var TestContext = TestContextType{
	timeouts: defaultTimeouts,
}

// StringArrayValue 与 flag.Var 配合使用，把逗号分隔的字符串列表放入字符串数组。
type stringArrayValue struct {
	stringArray *[]string
}

func (v stringArrayValue) String() string {
	if v.stringArray != nil {
		return strings.Join(*v.stringArray, ",")
	}
	return ""
}

func (v stringArrayValue) Set(s string) error {
	if len(s) == 0 {
		*v.stringArray = []string{}
	} else {
		*v.stringArray = strings.Split(s, ",")
	}
	return nil
}

// 集群为 IPv6 时 ClusterIsIPv6 返回 true
func (tc TestContextType) ClusterIsIPv6() bool {
	return tc.IPFamily == "ipv6"
}

// RegisterCommonFlags 注册所有 e2e 测试套件共用的 flag。
// flag set 可以是 flag.CommandLine（若需要），也可以是自定义
// flag set，随后传给 viperconfig.ViperizeFlags。
//
// 下面其他 Register*Flags 方法可用于添加更多
// 测试特有 flag。但无论测试是否真的在测试套件中，这些设置都会被加入。
// 这些 flag 会被无条件加入，无论当前测试是否真的在测试套件中。
//
// 对于已经改为自行注册选项的测试，可参照 HandleFlags，
// 从 test/e2e/framework/config 复制 flag。
func RegisterCommonFlags(flags *flag.FlagSet) {
	// 对 pod 这类对象，默认值太小，即便用 YAML 也一样。这里把默认值翻倍。
	flags.IntVar(&gomegaformat.MaxLength, "gomega-max-length", 8000, "Sets the maximum size for the gomega formatter (= gomega.MaxLength). Use 0 to disable truncation.")

	flags.StringVar(&TestContext.GatherKubeSystemResourceUsageData, "gather-resource-usage", "false", "If set to 'true' or 'all' framework will be monitoring resource usage of system all add-ons in (some) e2e tests, if set to 'master' framework will be monitoring master node only, if set to 'none' of 'false' monitoring will be turned off.")
	flags.BoolVar(&TestContext.GatherLogsSizes, "gather-logs-sizes", false, "If set to true framework will be monitoring logs sizes on all machines running e2e tests.")
	flags.IntVar(&TestContext.MaxNodesToGather, "max-nodes-to-gather-from", 20, "The maximum number of nodes to gather extended info from on test failure.")
	flags.StringVar(&TestContext.GatherMetricsAfterTest, "gather-metrics-at-teardown", "false", "If set to 'true' framework will gather metrics from all components after each test. If set to 'master' only master component metrics would be gathered.")
	flags.BoolVar(&TestContext.GatherSuiteMetricsAfterTest, "gather-suite-metrics-at-teardown", false, "If set to true framework will gather metrics from all components after the whole test suite completes.")
	flags.BoolVar(&TestContext.IncludeClusterAutoscalerMetrics, "include-cluster-autoscaler", false, "If set to true, framework will include Cluster Autoscaler when gathering metrics.")
	flags.StringVar(&TestContext.OutputPrintType, "output-print-type", "json", "Format in which summaries should be printed: 'hr' for human readable, 'json' for JSON ones.")
	flags.BoolVar(&TestContext.DumpLogsOnFailure, "dump-logs-on-failure", true, "If set to true test will dump data about the namespace in which test was running.")
	flags.BoolVar(&TestContext.DisableLogDump, "disable-log-dump", false, "If set to true, logs from master and nodes won't be gathered after test run.")
	flags.StringVar(&TestContext.LogexporterGCSPath, "logexporter-gcs-path", "", "Path to the GCS artifacts directory to dump logs from nodes. Logexporter gets enabled if this is non-empty.")
	flags.BoolVar(&TestContext.DeleteNamespace, "delete-namespace", true, "If true tests will delete namespace after completion. It is only designed to make debugging easier, DO NOT turn it off by default.")
	flags.BoolVar(&TestContext.DeleteNamespaceOnFailure, "delete-namespace-on-failure", true, "If true, framework will delete test namespace on failure. Used only during test debugging.")
	flags.IntVar(&TestContext.AllowedNotReadyNodes, "allowed-not-ready-nodes", 0, "If greater than zero, framework will allow for that many non-ready nodes when checking for all ready nodes. If -1, no waiting will be performed for ready nodes or daemonset pods.")

	flags.StringVar(&TestContext.Host, "host", "", fmt.Sprintf("The host, or apiserver, to connect to. Will default to %s if this argument and --kubeconfig are not set.", defaultHost))
	flags.StringVar(&TestContext.ReportPrefix, "report-prefix", "", "Optional prefix for JUnit XML reports. Default is empty, which doesn't prepend anything to the default name.")
	flags.StringVar(&TestContext.ReportDir, "report-dir", "", "Path to the directory where the simplified JUnit XML reports and other tests results should be saved. Default is empty, which doesn't generate these reports.  If ginkgo's -junit-report parameter is used, that parameter instead of -report-dir determines the location of a single JUnit report.")
	flags.BoolVar(&TestContext.ReportCompleteGinkgo, "report-complete-ginkgo", false, "Enables writing a complete test report as Ginkgo JSON to <report dir>/ginkgo/report.json. Ignored if --report-dir is not set.")
	flags.BoolVar(&TestContext.ReportCompleteJUnit, "report-complete-junit", false, "Enables writing a complete test report as JUnit XML to <report dir>/ginkgo/report.json. Ignored if --report-dir is not set.")
	flags.StringVar(&TestContext.ContainerRuntimeEndpoint, "container-runtime-endpoint", "unix:///run/containerd/containerd.sock", "The container runtime endpoint of cluster VM instances.")
	flags.StringVar(&TestContext.ContainerRuntimeProcessName, "container-runtime-process-name", "containerd", "The name of the container runtime process.")
	flags.StringVar(&TestContext.ContainerRuntimePidFile, "container-runtime-pid-file", "/run/containerd/containerd.pid", "The pid file of the container runtime.")
	flags.StringVar(&TestContext.SystemdServices, "systemd-services", "containerd*", "The comma separated list of systemd services the framework will dump logs for.")
	flags.BoolVar(&TestContext.DumpSystemdJournal, "dump-systemd-journal", false, "Whether to dump the full systemd journal.")
	flags.StringVar(&TestContext.ImageServiceEndpoint, "image-service-endpoint", "", "The image service endpoint of cluster VM instances.")
	flags.StringVar(&TestContext.NonblockingTaints, "non-blocking-taints", `node-role.kubernetes.io/control-plane`, "Nodes with taints in this comma-delimited list will not block the test framework from starting tests.")

	flags.BoolVar(&TestContext.ListImages, "list-images", false, "If true, will show list of images used for running tests.")
	flags.BoolVar(&TestContext.listLabels, "list-labels", false, "If true, will show the list of labels that can be used to select tests via -ginkgo.label-filter.")
	flags.BoolVar(&TestContext.listTests, "list-tests", false, "If true, will show the full names of all tests (aka specs) that can be used to select test via -ginkgo.focus/skip.")
	flags.StringVar(&TestContext.KubectlPath, "kubectl-path", "kubectl", "The kubectl binary to use. For development, you might use 'cluster/kubectl.sh' here.")

	flags.StringVar(&TestContext.ProgressReportURL, "progress-report-url", "", "The URL to POST progress updates to as the suite runs to assist in aiding integrations. If empty, no messages sent.")
	flags.StringVar(&TestContext.SpecSummaryOutput, "spec-dump", "", "The file to dump all ginkgo.SpecSummary to after tests run. If empty, no objects are saved/printed.")
	flags.StringVar(&TestContext.DockerConfigFile, "docker-config-file", "", "A docker credential file which contains authorization token that is used to perform image pull tests from an authenticated registry. For more details regarding the content of the file refer https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/#log-in-to-docker-hub")

	flags.StringVar(&TestContext.E2EDockerConfigFile, "e2e-docker-config-file", "", "A docker credentials configuration file used which contains authorization token that can be used to pull images from certain private registries provided by the users. For more details refer https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/#log-in-to-docker-hub")
	flags.StringVar(&TestContext.KubeTestRepoList, "kube-test-repo-list", "", "A yaml file used for overriding registries for test images. Alternatively, the KUBE_TEST_REPO_LIST env variable can be set.")

	flags.StringVar(&TestContext.SnapshotControllerPodName, "snapshot-controller-pod-name", "", "The pod name to use for identifying the snapshot controller in the kube-system namespace.")
	flags.IntVar(&TestContext.SnapshotControllerHTTPPort, "snapshot-controller-http-port", 0, "The port to use for snapshot controller HTTP communication.")

	flags.Var(&stringArrayValue{&TestContext.EnabledVolumeDrivers}, "enabled-volume-drivers", "Comma-separated list of in-tree volume drivers to enable for testing. This is only needed for in-tree drivers disabled by default. An example is gcepd; see test/e2e/storage/in_tree_volumes.go for full details.")
}

func CreateGinkgoConfig() (types.SuiteConfig, types.ReporterConfig) {
	// 获取当前的 config
	suiteConfig, reporterConfig := ginkgo.GinkgoConfiguration()
	// 同时随机化 spec 和 suite
	suiteConfig.RandomizeAllSpecs = true
	// 禁止运行被跳过的测试，除非显式指定。
	if len(suiteConfig.FocusStrings) == 0 && len(suiteConfig.SkipStrings) == 0 && suiteConfig.LabelFilter == "" {
		suiteConfig.SkipStrings = []string{`\[Flaky\]|\[Feature:.+\]`}
	}
	return suiteConfig, reporterConfig
}

// RegisterClusterFlags 注册集群 e2e 测试套件特有的 flag。
func RegisterClusterFlags(flags *flag.FlagSet) {
	flags.BoolVar(&TestContext.VerifyServiceAccount, "e2e-verify-service-account", true, "If true tests will verify the service account before running.")
	flags.StringVar(&TestContext.KubeConfig, clientcmd.RecommendedConfigPathFlag, os.Getenv(clientcmd.RecommendedConfigPathEnvVar), "Path to kubeconfig containing embedded authinfo.")
	flags.StringVar(&TestContext.KubeContext, clientcmd.FlagContext, "", "kubeconfig context to use/override. If unset, will use value from 'current-context'")
	flags.StringVar(&TestContext.KubeAPIContentType, "kube-api-content-type", "application/vnd.kubernetes.protobuf", "ContentType used to communicate with apiserver")

	flags.StringVar(&TestContext.KubeletRootDir, "kubelet-root-dir", "/var/lib/kubelet", "The data directory of kubelet. Some tests (for example, CSI storage tests) deploy DaemonSets which need to know this value and cannot query it. Such tests only work in clusters where the data directory is the same on all nodes.")
	flags.StringVar(&TestContext.KubeletRootDir, "volume-dir", "/var/lib/kubelet", "An alias for --kubelet-root-dir, kept for backwards compatibility.")
	flags.StringVar(&TestContext.CertDir, "cert-dir", "", "Path to the directory containing the certs. Default is empty, which doesn't use certs.")
	flags.StringVar(&TestContext.RepoRoot, "repo-root", "../../", "Root directory of kubernetes repository, for finding test files.")
	// 注意：Node E2E 测试也定义了这个 flag，但默认为 true。
	// 如果这里也变成 true，应把它们重构到 RegisterCommonFlags 中。
	flags.BoolVar(&TestContext.PrepullImages, "prepull-images", false, "If true, prepull images so image pull failures do not cause test failures.")
	flags.StringVar(&TestContext.Provider, "provider", "", "The name of the Kubernetes provider (gce, gke, local, skeleton (the fallback if not set), etc.)")
	flags.StringVar(&TestContext.Tooling, "tooling", "", "The tooling in use (kops, gke, etc.)")
	flags.StringVar(&TestContext.OutputDir, "e2e-output-dir", "/tmp", "Output directory for interesting/useful test data, like performance data, benchmarks, and other metrics.")
	flags.StringVar(&TestContext.Prefix, "prefix", "e2e", "A prefix to be added to cloud resources created during testing.")
	flags.StringVar(&TestContext.MasterOSDistro, "master-os-distro", "debian", "The OS distribution of cluster master (debian, ubuntu, gci, coreos, or custom).")
	flags.StringVar(&TestContext.NodeOSDistro, "node-os-distro", "debian", "The OS distribution of cluster VM instances (debian, ubuntu, gci, coreos, windows, or custom), which determines how specific tests are implemented.")
	flags.StringVar(&TestContext.NodeOSArch, "node-os-arch", "amd64", "The OS architecture of cluster VM instances (amd64, arm64, or custom).")
	flags.StringVar(&TestContext.ClusterDNSDomain, "dns-domain", "cluster.local", "The DNS Domain of the cluster.")

	// TODO: 按 provider 设置 flag？重命名 gce-project/gce-zone？
	cloudConfig := &TestContext.CloudConfig
	flags.StringVar(&cloudConfig.MasterName, "kube-master", "", "Name of the kubernetes master. Only required if provider is gce or gke")
	flags.StringVar(&cloudConfig.APIEndpoint, "gce-api-endpoint", "", "The GCE APIEndpoint being used, if applicable")
	flags.StringVar(&cloudConfig.ProjectID, "gce-project", "", "The GCE project being used, if applicable")
	flags.StringVar(&cloudConfig.Zone, "gce-zone", "", "GCE zone being used, if applicable")
	flags.Var(cliflag.NewStringSlice(&cloudConfig.Zones), "gce-zones", "The set of zones to use in a multi-zone test instead of querying the cloud provider.")
	flags.StringVar(&cloudConfig.Region, "gce-region", "", "GCE region being used, if applicable")
	flags.BoolVar(&cloudConfig.MultiZone, "gce-multizone", false, "If true, start GCE cloud provider with multizone support.")
	flags.BoolVar(&cloudConfig.MultiMaster, "gce-multimaster", false, "If true, the underlying GCE/GKE cluster is assumed to be multi-master.")
	flags.StringVar(&cloudConfig.Cluster, "gke-cluster", "", "GKE name of cluster being used, if applicable")
	flags.StringVar(&cloudConfig.NodeInstanceGroup, "node-instance-group", "", "Name of the managed instance group for nodes. Valid only for gce, gke or aws. If there is more than one group: comma separated list of groups.")
	flags.StringVar(&cloudConfig.Network, "network", "e2e", "The cloud provider network for this e2e cluster.")
	flags.IntVar(&cloudConfig.NumNodes, "num-nodes", DefaultNumNodes, fmt.Sprintf("Number of nodes in the cluster. If the default value of '%d' is used the number of schedulable nodes is auto-detected.", DefaultNumNodes))
	flags.StringVar(&cloudConfig.ClusterIPRange, "cluster-ip-range", "10.64.0.0/14", "A CIDR notation IP range from which to assign IPs in the cluster.")
	flags.StringVar(&cloudConfig.NodeTag, "node-tag", "", "Network tags used on node instances. Valid only for gce, gke")
	flags.StringVar(&cloudConfig.MasterTag, "master-tag", "", "Network tags used on master instances. Valid only for gce, gke")

	flags.StringVar(&cloudConfig.ClusterTag, "cluster-tag", "", "Tag used to identify resources.  Only required if provider is aws.")
	flags.StringVar(&cloudConfig.ConfigFile, "cloud-config-file", "", "Cloud config file.  Only required if provider is azure or vsphere.")
	flags.IntVar(&TestContext.MinStartupPods, "minStartupPods", 0, "The number of pods which we need to see in 'Running' state with a 'Ready' condition of true, before we try running tests. This is useful in any cluster which needs some base pod-based services running before it can be used. If set to -1, no pods are checked and tests run straight away.")
	flags.DurationVar(&TestContext.timeouts.SystemPodsStartup, "system-pods-startup-timeout", TestContext.timeouts.SystemPodsStartup, "Timeout for waiting for all system pods to be running before starting tests.")
	flags.DurationVar(&TestContext.timeouts.NodeSchedulable, "node-schedulable-timeout", TestContext.timeouts.NodeSchedulable, "Timeout for waiting for all nodes to be schedulable.")
	flags.DurationVar(&TestContext.timeouts.SystemDaemonsetStartup, "system-daemonsets-startup-timeout", TestContext.timeouts.SystemDaemonsetStartup, "Timeout for waiting for all system daemonsets to be ready.")
	flags.StringVar(&TestContext.EtcdUpgradeStorage, "etcd-upgrade-storage", "", "The storage version to upgrade to (either 'etcdv2' or 'etcdv3') if doing an etcd upgrade test.")
	flags.StringVar(&TestContext.EtcdUpgradeVersion, "etcd-upgrade-version", "", "The etcd binary version to upgrade to (e.g., '3.0.14', '2.3.7') if doing an etcd upgrade test.")
	flags.StringVar(&TestContext.GCEUpgradeScript, "gce-upgrade-script", "", "Script to use to upgrade a GCE cluster.")
	flags.BoolVar(&TestContext.CleanStart, "clean-start", false, "If true, purge all namespaces except default and system before running tests. This serves to Cleanup test namespaces from failed/interrupted e2e runs in a long-lived cluster.")
}

// generateSecureToken 返回长度为 tokenLen 的字符串，由
// 随机字节经 base64 编码组成，用作与 APIServer 通信时的 Bearer Token。
func generateSecureToken(tokenLen int) (string, error) {
	// base64 编码后字节数要等于 tokenLen。
	tokenSize := math.Ceil(float64(tokenLen) * 6 / 8)
	rawToken := make([]byte, int(tokenSize))
	if _, err := rand.Read(rawToken); err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(rawToken)
	token := encoded[:tokenLen]
	return token, nil
}

// 读完所有 flag 后，AfterReadingAllFlags 修改上下文，
// 并让进程做好运行测试的准备。
func AfterReadingAllFlags(t *TestContextType) {
	// 重新配置 klog，让输出写到 GinkgoWriter 而不是
	// stderr。好处是能与写到 GinkgoWriter 的输出（By、Logf）
	// 正确交错。

	// 这些 flag 没有通过普通命令行 flag set 暴露，
	// 因此这里必须使用自己的私有 flag set。
	if t.KubeTestRepoList != "" {
		image.Init(t.KubeTestRepoList)
	}

	if t.ListImages {
		for _, v := range image.GetImageConfigs() {
			fmt.Println(v.GetE2EImage())
		}
		Exit(0)
	}

	// 重新配置 gomega 默认值。poll 间隔应适合
	// 大多数测试。超时更主观，测试可能想覆盖，但这些默认值
	// 对 E2E 来说仍优于 Gomega 的默认值（1 秒超时、10 毫秒间隔）。
	//
	gomega.SetDefaultEventuallyPollingInterval(t.timeouts.Poll)
	gomega.SetDefaultConsistentlyPollingInterval(t.timeouts.Poll)
	gomega.SetDefaultEventuallyTimeout(t.timeouts.PodStart)
	gomega.SetDefaultConsistentlyDuration(t.timeouts.PodStartShort)
	gomega.EnforceDefaultTimeoutsWhenUsingContexts()

	// ginkgo.PreviewSpecs 会展开所有节点，可能发现新的 bug。
	report := ginkgo.PreviewSpecs("Kubernetes e2e test statistics")
	validateSpecs(report.SpecReports)
	if err := FormatBugs(); CheckForBugs && err != nil {
		// E2E 套件有 bug 时拒绝执行任何操作。
		fmt.Fprint(Output, "ERROR: E2E suite initialization was faulty, these errors must be fixed:")
		fmt.Fprint(Output, "\n"+err.Error())
		Exit(1)
	}
	if t.listLabels || t.listTests {
		listTestInformation(report)
		Exit(0)
	}

	// 除非 kubeconfig 会提供 host，否则才设置默认 host
	if len(t.Host) == 0 && len(t.KubeConfig) == 0 {
		// 检查能否使用 in-cluster config
		if clusterConfig, err := restclient.InClusterConfig(); err == nil {
			if tempFile, err := os.CreateTemp(os.TempDir(), "kubeconfig-"); err == nil {
				kubeConfig := kubeconfig.CreateKubeConfig(clusterConfig)
				clientcmd.WriteToFile(*kubeConfig, tempFile.Name())
				t.KubeConfig = tempFile.Name()
				klog.V(4).Infof("Using a temporary kubeconfig file from in-cluster config : %s", tempFile.Name())
			}
		}
		if len(t.KubeConfig) == 0 {
			klog.Warningf("Unable to find in-cluster config, using default host : %s", defaultHost)
			t.Host = defaultHost
		}
	}
	if len(t.BearerToken) == 0 {
		var err error
		t.BearerToken, err = generateSecureToken(16)
		ExpectNoError(err, "Failed to generate bearer token")
	}

	// 允许 1% 的节点未就绪（统计意义上）——与大规模集群相关。
	if t.AllowedNotReadyNodes == 0 {
		t.AllowedNotReadyNodes = t.CloudConfig.NumNodes / 100
	}

	klog.V(4).Infof("Tolerating taints %q when considering if nodes are ready", TestContext.NonblockingTaints)

	// 确保所有测试运行都有有效的 TestContext.CloudConfig.Provider。
	// 关于这段代码是否还需要、需要多久，正在讨论，
	// 见 https://github.com/kubernetes/kubernetes/issues/70194。
	if TestContext.Provider == "" {
		// e2e.test 二进制的一些使用者会传 --provider=。
		// 必须支持这种用法，改动会破坏他们。
		Logf("The --provider flag is not set. Continuing as if --provider=skeleton had been used.")
		TestContext.Provider = "skeleton"
	}

	var err error
	TestContext.CloudConfig.Provider, err = SetupProviderConfig(TestContext.Provider)
	if err != nil {
		if os.IsNotExist(errors.Unwrap(err)) {
			// provider 未知时给出更有帮助的错误消息。
			var providers []string
			for _, name := range GetProviders() {
				// 空字符串可接受，但下面的输出中不加引号会显得奇怪。
				if name == "" {
					name = `""`
				}
				providers = append(providers, name)
			}
			sort.Strings(providers)
			klog.Errorf("Unknown provider %q. The following providers are known: %v", TestContext.Provider, strings.Join(providers, " "))
		} else {
			klog.Errorf("Failed to setup provider config for %q: %v", TestContext.Provider, err)
		}
		Exit(1)
	}

	if TestContext.ReportDir != "" {
		// 在运行套件前创建目录。如果
		// --report-dir 不可用，应尽快
		// 报告。各 worker 会并行执行，
		// 因此多数会得到 "exists" 错误。
		if err := os.MkdirAll(TestContext.ReportDir, 0777); err != nil && !os.IsExist(err) {
			klog.Errorf("Create report dir: %v", err)
			Exit(1)
		}
		ginkgoDir := path.Join(TestContext.ReportDir, "ginkgo")
		if TestContext.ReportCompleteGinkgo || TestContext.ReportCompleteJUnit {
			if err := os.MkdirAll(ginkgoDir, 0777); err != nil && !os.IsExist(err) {
				klog.Errorf("Create <report-dir>/ginkgo: %v", err)
				Exit(1)
			}
		}

		if TestContext.ReportCompleteGinkgo {
			ginkgo.ReportAfterSuite("Ginkgo JSON report", func(report ginkgo.Report) {
				ExpectNoError(reporters.GenerateJSONReport(report, path.Join(ginkgoDir, "report.json")))
			})
			ginkgo.ReportAfterSuite("JUnit XML report", func(report ginkgo.Report) {
				ExpectNoError(reporters.GenerateJUnitReport(report, path.Join(ginkgoDir, "report.xml")))
			})
		}

		ginkgo.ReportAfterSuite("Kubernetes e2e JUnit report", func(report ginkgo.Report) {
			// Ginkgo v1 时代，我们为每个并行节点写一个文件。
			// 现在 Ginkgo v2 会自动把所有结果合并成
			// 一份报告。保留 01 后缀是为了兼容
			// 期望文件名为
			// "junit_<prefix><number>.xml" 的用户。
			junitReport := path.Join(TestContext.ReportDir, "junit_"+TestContext.ReportPrefix+"01.xml")

			// writeJUnitReport 在 e2e 报告目录中生成一个 JUnit 文件，
			// 比 `ginkgo --junit-report` 通常生成的要短。
			// 这是必要的，因为完整报告可能太大，
			// Spyglass 等工具无法处理
			//（https://github.com/kubernetes/kubernetes/issues/111510）。
			//
			ExpectNoError(junit.WriteJUnitReport(report, junitReport))
		})
	}
}

func listTestInformation(report ginkgo.Report) {
	indent := strings.Repeat(" ", 4)

	if TestContext.listLabels {
		labels := sets.New[string]()
		for _, spec := range report.SpecReports {
			if spec.LeafNodeType == types.NodeTypeIt {
				labels.Insert(spec.Labels()...)
			}
		}
		fmt.Fprintf(Output, "The following labels can be used with 'ginkgo run --label-filter':\n%s%s\n\n", indent, strings.Join(sets.List(labels), "\n"+indent))
	}
	if TestContext.listTests {
		leafs := make([][]string, 0, len(report.SpecReports))
		wd, _ := os.Getwd()
		for _, spec := range report.SpecReports {
			if spec.LeafNodeType == types.NodeTypeIt {
				leafs = append(leafs, []string{fmt.Sprintf("%s:%d: ", relativePath(wd, spec.LeafNodeLocation.FileName), spec.LeafNodeLocation.LineNumber), spec.FullText()})
			}
		}
		// 按测试名排序，而非源码位置，因为测试
		// 名在代码重构后更稳定。
		sort.Slice(leafs, func(i, j int) bool {
			return leafs[i][1] < leafs[j][1]
		})
		fmt.Fprint(Output, "The following spec names can be used with 'ginkgo run --focus/skip':\n")
		for _, leaf := range leafs {
			fmt.Fprintf(Output, "%s%s%s\n", indent, leaf[0], leaf[1])
		}
		fmt.Fprint(Output, "\n")
	}
}

func relativePath(wd, path string) string {
	if wd == "" {
		return path
	}
	relpath, err := filepath.Rel(wd, path)
	if err != nil {
		return path
	}
	return relpath
}
