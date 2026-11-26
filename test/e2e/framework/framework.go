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

// Package framework 包含与 provider 无关的辅助代码，用于
// 用 Ginkgo 构建和运行 E2E 测试。实际的 Ginkgo 测试
// suite 由本框架、可选的
// provider 支持代码以及通过单独 .go 文件引入的具体测试
// （如 Kubernetes 的 test/e2e.go）组合而成。
package framework

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	v1svc "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/discovery"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	scaleclient "k8s.io/client-go/scale"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/go-logr/logr"
	"github.com/onsi/ginkgo/v2"
)

const (
	// DefaultNamespaceDeletionTimeout 是等待 namespace 删除的超时时间。
	DefaultNamespaceDeletionTimeout = 5 * time.Minute
	defaultServiceAccountName       = "default"
)

var (
	// NewFrameworkExtensions 列出一些函数，它们在
	// NewFramework 构建完新 framework 后、调用该 framework 的
	// ginkgo.BeforeEach 之后被调用。
	//
	// 核心框架的扩展可用它来修改 framework 实例中的设置，
	// 或通过 ginkgo.BeforeEach/AfterEach/DeferCleanup
	// 添加额外的回调。
	//
	// 测试运行时，函数按以下顺序调用：
	// - 测试在 f.NewDefaultFramework 之前定义的 BeforeEach，
	//   按定义顺序（先进先出）
	// - f.BeforeEach
	// - 测试在 f.NewDefaultFramework 之后定义的 BeforeEach
	// - It 回调
	// - 所有 AfterEach，按定义顺序
	// - 所有 DeferCleanup，按相反顺序（先进后出）
	// - f.AfterEach
	//
	// 由于测试可能在 f.BeforeEach 之前运行的 BeforeEach 中被跳过，
	// 依赖 framework 实例的 AfterEach 回调必须检查它是否
	// 已初始化。可通过检查 f.ClientSet 是否为 nil 来判断。DeferCleanup
	// 回调不需要这样做，因为它们是在测试
	// 运行时定义的。
	//
	NewFrameworkExtensions []func(f *Framework)
)

// Framework 支持 e2e 测试常用的操作；它会为你维护一个 client 和一个 namespace。
// 最终目标是将它与 integration 测试框架合并。
//
// 你可以通过设置 `NamespacePodSecurityLevel` 来配置测试的 pod 安全级别，
// 它会为 namespace 设置 pod security admission 的 enforce、warn 和 audit 三个标签。
// 默认的 pod security profile 是 "restricted"。
// 每个标签都可以通过本
// 结构体中更具体的 NamespacePodSecurity* 属性覆盖。
//
// Framework 实例实现了 ktesting.TB，可作为
// 第一个参数传给 [github.com/stretchr/testify/assert]。
// 但有两个注意点：
//   - Error 和 Errorf 会中止当前正在运行的测试。
//   - 这只在测试运行期间有效，定义测试阶段无效。
//     错误的测试可能 panic。在测试之外使用 ginkgo.Fail
//     也是如此。
type Framework struct {
	ktesting.TB

	BaseName string

	// 在创建 ClientSet 和 namespace 时一并设置。
	// 即使并行多次运行同一测试，也保证在集群中唯一。
	//
	UniqueName string

	clientConfig                     *rest.Config
	restMapper                       *restmapper.DeferredDiscoveryRESTMapper
	ClientSet                        clientset.Interface
	KubemarkExternalClusterClientSet clientset.Interface

	DynamicClient dynamic.Interface

	ScalesGetter scaleclient.ScalesGetter

	SkipNamespaceCreation            bool            // Whether to skip creating a namespace
	SkipSecretCreation               bool            // Whether to skip creating secret for a test
	Namespace                        *v1.Namespace   // Every test has at least one namespace unless creation is skipped
	namespacesToDelete               []*v1.Namespace // Some tests have more than one.
	NamespaceDeletionTimeout         time.Duration
	NamespacePodSecurityEnforceLevel admissionapi.Level // The pod security enforcement level for namespaces to be applied.
	NamespacePodSecurityWarnLevel    admissionapi.Level // The pod security warn (client logging) level for namespaces to be applied.
	NamespacePodSecurityAuditLevel   admissionapi.Level // The pod security audit (server logging) level for namespaces to be applied.
	NamespacePodSecurityLevel        admissionapi.Level // The pod security level to be used for all of enforcement, warn and audit. Can be rewritten by more specific configuration attributes.

	// e2e 测试中的偶发操作失败可通过它捕获。
	flakeReport *FlakeReport

	// framework 客户端的配置
	Options Options

	// 测试运行期间存放各种附加数据的地方，测试结束后打印到 ReportDir，
	// 若未设置 ReportDir 则打印到 stdout。
	TestSummaries []TestDataSummary

	// Timeouts 包含测试执行期间使用的自定义超时。
	Timeouts *TimeoutContext

	// framework 调用 DumpAllNamespaceInfo 在测试失败后记录
	// namespace 的信息。
	DumpAllNamespaceInfo DumpAllNamespaceInfoAction
}

// CleanupCtx 实现了 [ktesting.ContextTB.CleanupCtx]，与
// ginkgo.DeferCleanup 相同。
func (f *Framework) CleanupCtx(cb func(context.Context)) {
	ginkgo.GinkgoHelper()
	ginkgo.DeferCleanup(cb)
}

// Log 实现 TB.Log。它覆盖 Ginkgo 的实现以确保输出一致。
func (f *Framework) Log(args ...any) {
	log(1, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
}

// Logf 实现 TB.Logf。它覆盖 Ginkgo 的实现以确保输出一致。
func (f *Framework) Logf(format string, args ...any) {
	log(1, fmt.Sprintf(format, args...))
}

var _ ktesting.ContextTB = &Framework{}

// TContext 把 framework 和 context 组合进一个 [ktesting.TContext]。
//
// 接受独立 [context.Context] 和 [clientset.Interface] 参数的
// 遗留代码，可以把这两个参数传给 [ContextTODO]，
// 创建一个同样持有所有 client 的 [ktesting.TContext]，但这是
// 过渡方案。更干净的做法是改写那些代码，让它接受
// 一个 [ktesting.TContext]。
func (f *Framework) TContext(ctx context.Context) ktesting.TContext {
	if f.TB == nil {
		// 测试设置阶段通常没有 context，所以不太会走到这里。
		// 但也可能有开发者铁了心传入 context.Background()，
		// 所以还是显式处理。
		panic("TContext may only be used while a test runs.")
	}
	tCtx := ktesting.InitCtx(ctx, f /* intentionally using f here and not f.TB because f overrides some methods */)
	tCtx = tCtx.WithClients(f.clientConfig, f.restMapper, f.ClientSet, f.DynamicClient, apiextensions.NewForConfigOrDie(f.clientConfig))
	if f.Namespace != nil {
		tCtx = tCtx.WithNamespace(f.Namespace.Name)
	}
	tCtx = ensureLogger(tCtx)
	return tCtx
}

// ContextTODO 可作为过渡方案，用于当前接受
// [context.Context] 和 [clientset.Interface] 的函数，为已使用
// [ktesting.TContext] 而非不同参数的其他代码创建
// 一个 [ktesting.TContext]。如果 client 来自 [Framework.ClientSet]，
// 结果与 [Framework.TContext] 的完全相同。
// 特别地，dynamic client 和 rest config 都可用。
//
// 长期来看，依赖 ContextTODO 的代码应被重写。
func ContextTODO(ctx context.Context, client clientset.Interface) ktesting.TContext {
	// 如果 client 来自某个 framework 实例，
	// 可以在这里把它转换类型，准备一个完整的 context。
	if fc, ok := client.(fClient); ok {
		return fc.f.TContext(ctx)
	}
	f := NewDefaultFramework("tcontext")
	tCtx := ktesting.InitCtx(ctx, f)
	tCtx = tCtx.WithClients(nil, nil, client, nil, nil)
	tCtx = ensureLogger(tCtx)
	return tCtx
}

// ensureLogger 在 context 中安装 Ginkgo logger（如果还没有的话）。
//
// 对于使用 klog.FromContext 的代码，这一步并非必需，因为 klog
// logger 本身就是默认的 Ginkgo logger。但也可能有
// 直接使用 go-logr 的代码：这时通过 context 提供 logger
// 很有用。
func ensureLogger(tCtx ktesting.TContext) ktesting.TContext {
	_, ok := logr.FromContext(tCtx)
	if ok == nil {
		return tCtx
	}

	return tCtx.WithLogger(ginkgoLogger)
}

// fClient 存储在 framework.ClientSet 中，由 ContextTODO 使用。
//
// TODO: 当不再需要时删除它和 ContextTODO，即
// 所有代码都接受 TContext 时。
type fClient struct {
	clientset.Interface
	f *Framework
}

// 每个失败测试为测试创建的 namespace 调用 DumpAllNamespaceInfoAction。
type DumpAllNamespaceInfoAction func(ctx context.Context, f *Framework, namespace string)

// TestDataSummary 是管理测试数据的接口。
type TestDataSummary interface {
	SummaryKind() string
	PrintHumanReadable() string
	PrintJSON() string
}

// Options 是管理测试框架选项的结构体。
type Options struct {
	ClientQPS    float32
	ClientBurst  int
	GroupVersion *schema.GroupVersion
}

// NewFrameworkWithCustomTimeouts 创建一个带自定义超时的 framework。
// 超时值为零时继续使用正常默认值。
func NewFrameworkWithCustomTimeouts(baseName string, timeouts *TimeoutContext) *Framework {
	f := NewDefaultFramework(baseName)
	in := reflect.ValueOf(timeouts).Elem()
	out := reflect.ValueOf(f.Timeouts).Elem()
	for i := 0; i < in.NumField(); i++ {
		value := in.Field(i)
		if !value.IsZero() {
			out.Field(i).Set(value)
		}
	}
	return f
}

// NewDefaultFramework 创建新 framework 并设置 BeforeEach 来
// 初始化 framework 实例。它用 DeferCleanup 清理，
// 清理最后运行，因此测试中的 AfterEach 仍能拿到有效的 framework
// 实例。
func NewDefaultFramework(baseName string) *Framework {
	options := Options{
		ClientQPS:   20,
		ClientBurst: 50,
	}
	return NewFramework(baseName, options, nil)
}

// NewFramework 创建一个测试框架。
func NewFramework(baseName string, options Options, client clientset.Interface) *Framework {
	f := &Framework{
		// TB 起初尚未设置，只能在测试运行期间使用。
		BaseName:  baseName,
		Options:   options,
		ClientSet: client,
		Timeouts:  NewTimeoutContext(),
	}

	// 这里的顺序很重要：如果扩展自己调用 ginkgo.BeforeEach，
	// 就能确保它的回调被调用时 f.BeforeEach 已运行完毕。
	//
	ginkgo.BeforeEach(f.BeforeEach, AnnotatedLocation("set up framework"))
	for _, extension := range NewFrameworkExtensions {
		extension(f)
	}

	return f
}

// BeforeEach 获取 client 并创建 namespace。
func (f *Framework) BeforeEach(ctx context.Context) {
	f.TB = ginkgo.GinkgoT()

	// 与 AfterEach 不同，DeferCleanup 按
	// 先进后出的顺序触发，这保证 framework 实例
	// 尽可能长时间保持有效。
	//
	// 此外，如果测试根本没走到这里，AfterEach 也不会被调用。
	ginkgo.DeferCleanup(f.AfterEach, AnnotatedLocation("tear down framework"))

	// 注册得更晚，因此在删除 namespace 之前运行。
	ginkgo.DeferCleanup(f.dumpNamespaceInfo, AnnotatedLocation("dump namespaces"))

	ginkgo.By("Creating a kubernetes client")
	config, err := LoadConfig()
	ExpectNoError(err)

	config.QPS = f.Options.ClientQPS
	config.Burst = f.Options.ClientBurst
	if f.Options.GroupVersion != nil {
		config.GroupVersion = f.Options.GroupVersion
	}
	if TestContext.KubeAPIContentType != "" {
		config.ContentType = TestContext.KubeAPIContentType
	}
	f.clientConfig = rest.CopyConfig(config)
	clientSet, err := clientset.NewForConfig(config)
	ExpectNoError(err)
	f.ClientSet = fClient{
		Interface: clientSet,
		f:         f,
	}
	f.DynamicClient, err = dynamic.NewForConfig(config)
	ExpectNoError(err)

	// 创建 scales getter，把 GroupVersion 和 NegotiatedSerializer 设为默认值，
	// 因为创建 REST client 时需要它们。
	if config.GroupVersion == nil {
		config.GroupVersion = &schema.GroupVersion{}
	}
	if config.NegotiatedSerializer == nil {
		config.NegotiatedSerializer = scheme.Codecs
	}
	restClient, err := rest.RESTClientFor(config)
	ExpectNoError(err)
	discoClient, err := discovery.NewDiscoveryClientForConfig(config)
	ExpectNoError(err)
	cachedDiscoClient := cacheddiscovery.NewMemCacheClient(discoClient)
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(cachedDiscoClient)
	restMapper.Reset()
	f.restMapper = restMapper
	resolver := scaleclient.NewDiscoveryScaleKindResolver(cachedDiscoClient)
	f.ScalesGetter = scaleclient.New(restClient, restMapper, dynamic.LegacyAPIPathResolverFunc, resolver)

	TestContext.CloudConfig.Provider.FrameworkBeforeEach(f)

	if !f.SkipNamespaceCreation {
		ginkgo.By(fmt.Sprintf("Building a namespace api object, basename %s", f.BaseName))
		namespace, err := f.CreateNamespace(ctx, f.BaseName, map[string]string{
			"e2e-framework": f.BaseName,
		})
		ExpectNoError(err)

		f.Namespace = namespace

		if TestContext.VerifyServiceAccount {
			ginkgo.By("Waiting for a default service account to be provisioned in namespace")
			err = WaitForDefaultServiceAccountInNamespace(ctx, f.ClientSet, namespace.Name)
			ExpectNoError(err)
			ginkgo.By("Waiting for kube-root-ca.crt to be provisioned in namespace")
			err = WaitForKubeRootCAInNamespace(ctx, f.ClientSet, namespace.Name)
			ExpectNoError(err)
		} else {
			Logf("Skipping waiting for service account")
		}

		f.UniqueName = f.Namespace.GetName()
	} else {
		// 不保证唯一，但大概率唯一
		f.UniqueName = fmt.Sprintf("%s-%08x", f.BaseName, rand.Int31())
	}

	f.flakeReport = NewFlakeReport()
}

func (f *Framework) dumpNamespaceInfo(ctx context.Context) {
	if !ginkgo.CurrentSpecReport().Failed() {
		return
	}
	if !TestContext.DumpLogsOnFailure {
		return
	}
	if f.DumpAllNamespaceInfo == nil {
		return
	}
	ginkgo.By("dump namespace information after failure", func() {
		if !f.SkipNamespaceCreation {
			for _, ns := range f.namespacesToDelete {
				f.DumpAllNamespaceInfo(ctx, f, ns.Name)
			}
		}
	})
}

// printSummaries 打印测试摘要。
func printSummaries(summaries []TestDataSummary, testBaseName string) {
	now := time.Now()
	for i := range summaries {
		Logf("Printing summary: %v", summaries[i].SummaryKind())
		switch TestContext.OutputPrintType {
		case "hr":
			if TestContext.ReportDir == "" {
				Logf("%s", summaries[i].PrintHumanReadable())
			} else {
				// TODO: 学会提取测试名称并追加到 kind 上，而不是用时间戳。
				filePath := path.Join(TestContext.ReportDir, summaries[i].SummaryKind()+"_"+testBaseName+"_"+now.Format(time.RFC3339)+".txt")
				if err := os.WriteFile(filePath, []byte(summaries[i].PrintHumanReadable()), 0644); err != nil {
					Logf("Failed to write file %v with test performance data: %v", filePath, err)
				}
			}
		case "json":
			fallthrough
		default:
			if TestContext.OutputPrintType != "json" {
				Logf("Unknown output type: %v. Printing JSON", TestContext.OutputPrintType)
			}
			if TestContext.ReportDir == "" {
				Logf("%v JSON\n%v", summaries[i].SummaryKind(), summaries[i].PrintJSON())
				Logf("Finished")
			} else {
				// TODO: 学会提取测试名称并追加到 kind 上，而不是用时间戳。
				filePath := path.Join(TestContext.ReportDir, summaries[i].SummaryKind()+"_"+testBaseName+"_"+now.Format(time.RFC3339)+".json")
				Logf("Writing to %s", filePath)
				if err := os.WriteFile(filePath, []byte(summaries[i].PrintJSON()), 0644); err != nil {
					Logf("Failed to write file %v with test performance data: %v", filePath, err)
				}
			}
		}
	}
}

// AfterEach 在读取 namespace 事件后删除它。
func (f *Framework) AfterEach(ctx context.Context) {
	// 这不应该发生。ClientSet 是公开字段，说明测试改动过它！
	// 在清理阶段的任何 API 调用之前尽早报错。
	if f.ClientSet == nil {
		Failf("The framework ClientSet must not be nil at this point")
	}

	// 把 DeleteNamespace 放在最后的 defer 中，避免
	// 断言失败阻止 namespace 删除。
	defer func() {
		nsDeletionErrors := map[string]error{}
		// 是否删除 namespace 由三个因素决定：delete-namespace flag、delete-namespace-on-failure flag 和测试结果
		// 如果 delete-namespace 设为 false，namespace 总会保留。
		// 如果 delete-namespace 为 true 且 delete-namespace-on-failure 为 false，测试失败时保留 namespace。
		if TestContext.DeleteNamespace && (TestContext.DeleteNamespaceOnFailure || !ginkgo.CurrentSpecReport().Failed()) {
			for _, ns := range f.namespacesToDelete {
				ginkgo.By(fmt.Sprintf("Destroying namespace %q for this suite.", ns.Name))
				if err := f.ClientSet.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{}); err != nil {
					if !apierrors.IsNotFound(err) {
						nsDeletionErrors[ns.Name] = err

						// 如果无法删除 namespace 且尚未转储，则转储它。
						if !ginkgo.CurrentSpecReport().Failed() && TestContext.DumpLogsOnFailure && f.DumpAllNamespaceInfo != nil {
							f.DumpAllNamespaceInfo(ctx, f, ns.Name)
						}
					} else {
						Logf("Namespace %v was already deleted", ns.Name)
					}
				}
			}
		} else {
			if !TestContext.DeleteNamespace {
				Logf("Found DeleteNamespace=false, skipping namespace deletion!")
			} else {
				Logf("Found DeleteNamespaceOnFailure=false and current test failed, skipping namespace deletion!")
			}
		}

		// 取消设置这一点对后续使用同一实例的测试很重要，
		// 因为如果其他 BeforeEach 先跳过了测试，它可能到不了 f.BeforeEach。
		//
		f.Namespace = nil
		f.clientConfig = nil
		f.ClientSet = nil
		f.namespacesToDelete = nil
		f.TB = nil

		// 如果删除时出错，现在报告。
		if len(nsDeletionErrors) != 0 {
			messages := []string{}
			for namespaceKey, namespaceErr := range nsDeletionErrors {
				messages = append(messages, fmt.Sprintf("Couldn't delete ns: %q: %s (%#v)", namespaceKey, namespaceErr, namespaceErr))
			}
			Fail(strings.Join(messages, ","))
		}
	}()

	TestContext.CloudConfig.Provider.FrameworkAfterEach(f)

	// 报告 e2e 测试中观察到的偶发问题并重置。
	if f.flakeReport != nil && f.flakeReport.GetFlakeCount() > 0 {
		f.TestSummaries = append(f.TestSummaries, f.flakeReport)
		f.flakeReport = nil
	}

	printSummaries(f.TestSummaries, f.BaseName)
}

// DeleteNamespace 用于删除 namespace。也可用于
// 转储 namespace 信息，从而在测试结束时替代 framework
// 删除 namespace 的行为。
func (f *Framework) DeleteNamespace(ctx context.Context, name string) {
	defer func() {
		err := f.ClientSet.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			Logf("error deleting namespace %s: %v", name, err)
			return
		}
		err = WaitForNamespacesDeleted(ctx, f.ClientSet, []string{name}, DefaultNamespaceDeletionTimeout)
		if err != nil {
			Logf("error deleting namespace %s: %v", name, err)
			return
		}
		// 从 namespacesToDelete map 中移除已删除的 namespace
		for i, ns := range f.namespacesToDelete {
			if ns == nil {
				continue
			}
			if ns.Name == name {
				f.namespacesToDelete = append(f.namespacesToDelete[:i], f.namespacesToDelete[i+1:]...)
			}
		}
	}()
	// 如果当前测试失败，应转储 namespace 信息
	if !f.SkipNamespaceCreation && ginkgo.CurrentSpecReport().Failed() && TestContext.DumpLogsOnFailure && f.DumpAllNamespaceInfo != nil {
		f.DumpAllNamespaceInfo(ctx, f, name)
	}

}

// CreateNamespace 为 e2e 测试创建 namespace。
func (f *Framework) CreateNamespace(ctx context.Context, baseName string, labels map[string]string) (*v1.Namespace, error) {
	createTestingNS := TestContext.CreateTestingNS
	if createTestingNS == nil {
		createTestingNS = CreateTestingNS
	}

	if labels == nil {
		labels = make(map[string]string)
	} else {
		labelsCopy := make(map[string]string)
		for k, v := range labels {
			labelsCopy[k] = v
		}
		labels = labelsCopy
	}

	labels[admissionapi.EnforceLevelLabel] = firstNonEmptyPSaLevelOrRestricted(f.NamespacePodSecurityEnforceLevel, f.NamespacePodSecurityLevel)
	labels[admissionapi.WarnLevelLabel] = firstNonEmptyPSaLevelOrRestricted(f.NamespacePodSecurityWarnLevel, f.NamespacePodSecurityLevel)
	labels[admissionapi.AuditLevelLabel] = firstNonEmptyPSaLevelOrRestricted(f.NamespacePodSecurityAuditLevel, f.NamespacePodSecurityLevel)

	ns, err := createTestingNS(ctx, baseName, f.ClientSet, labels)
	// 检查 ns 而不是 err 是否为 nil，因为可能
	// 在其中创建 serviceAccount 失败。
	f.AddNamespacesToDelete(ns)

	if TestContext.E2EDockerConfigFile != "" && !f.SkipSecretCreation {
		// Secret 创建后，默认 service account（在新 namespace 中）
		// 会被 patch 上该 secret，从而可供 E2E 进程创建的所有 pod 引用，仓库认证应当成功。
		secret, err := f.createSecretFromDockerConfig(ctx, ns.Name)
		if err != nil {
			return ns, fmt.Errorf("failed to create secret from docker config file: %v", err)
		}

		serviceAccountClient := f.ClientSet.CoreV1().ServiceAccounts(ns.Name)
		serviceAccountConfig := v1svc.ServiceAccount(defaultServiceAccountName, ns.Name)
		serviceAccountConfig.ImagePullSecrets = append(serviceAccountConfig.ImagePullSecrets, v1svc.LocalObjectReferenceApplyConfiguration{Name: &secret.Name})

		svc, err := serviceAccountClient.Apply(ctx, serviceAccountConfig, metav1.ApplyOptions{FieldManager: "e2e-framework"})
		if err != nil {
			return ns, fmt.Errorf("failed to patch imagePullSecret [%s] to service account [%s]: %v", secret.Name, svc.Name, err)
		}

	}

	return ns, err
}

func firstNonEmptyPSaLevelOrRestricted(levelConfig ...admissionapi.Level) string {
	for _, l := range levelConfig {
		if len(l) > 0 {
			return string(l)
		}
	}
	return string(admissionapi.LevelRestricted)
}

// createSecretFromDockerConfig 使用私有镜像仓库凭据创建 secret。
// 凭据由 --e2e-docker-config-file flag 提供。
func (f *Framework) createSecretFromDockerConfig(ctx context.Context, namespace string) (*v1.Secret, error) {
	contents, err := os.ReadFile(TestContext.E2EDockerConfigFile)
	if err != nil {
		return nil, fmt.Errorf("error reading docker config file: %v", err)
	}

	secretObject := &v1.Secret{
		Data: map[string][]byte{v1.DockerConfigJsonKey: contents},
		Type: v1.SecretTypeDockerConfigJson,
	}
	secretObject.GenerateName = "registry-cred"
	Logf("create image pull secret %s", secretObject.Name)

	secret, err := f.ClientSet.CoreV1().Secrets(namespace).Create(ctx, secretObject, metav1.CreateOptions{})

	return secret, err
}

// RecordFlakeIfError 在发生错误时记录偶发问题信息。
// NOTE: This function is not used at any places yet, but we are in progress for https://github.com/kubernetes/kubernetes/issues/66239 which requires this. Please don't remove this.
func (f *Framework) RecordFlakeIfError(err error, optionalDescription ...interface{}) {
	f.flakeReport.RecordFlakeIfError(err, optionalDescription...)
}

// AddNamespacesToDelete 添加一个或多个 namespace，在测试
// 结束时删除。
func (f *Framework) AddNamespacesToDelete(namespaces ...*v1.Namespace) {
	for _, ns := range namespaces {
		if ns == nil {
			continue
		}
		f.namespacesToDelete = append(f.namespacesToDelete, ns)

	}
}

// ClientConfig 是读取 kube client 配置的外部可访问方法。
func (f *Framework) ClientConfig() *rest.Config {
	ret := rest.CopyConfig(f.clientConfig)
	// json 是最小公分母
	ret.ContentType = runtime.ContentTypeJSON
	ret.AcceptContentTypes = runtime.ContentTypeJSON
	return ret
}

// SetClientConfig 允许设置或更新 Framework 实例中的 REST config。
func (f *Framework) SetClientConfig(config *rest.Config) {
	f.clientConfig = rest.CopyConfig(config)
}

// KubeUser 是管理 kubernetes 用户信息的结构体。
type KubeUser struct {
	Name string `yaml:"name"`
	User struct {
		Username string `yaml:"username"`
		Password string `yaml:"password" datapolicy:"password"`
		Token    string `yaml:"token" datapolicy:"token"`
	} `yaml:"user"`
}

// KubeCluster 是管理 kubernetes 集群信息的结构体。
type KubeCluster struct {
	Name    string `yaml:"name"`
	Cluster struct {
		CertificateAuthorityData string `yaml:"certificate-authority-data"`
		Server                   string `yaml:"server"`
	} `yaml:"cluster"`
}

// KubeConfig 是管理 kubernetes 配置的结构体。
type KubeConfig struct {
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster string `yaml:"cluster"`
			User    string
		} `yaml:"context"`
	} `yaml:"contexts"`

	Clusters []KubeCluster `yaml:"clusters"`

	Users []KubeUser `yaml:"users"`
}

// FindUser 返回指定用户名对应的用户信息。
func (kc *KubeConfig) FindUser(name string) *KubeUser {
	for _, user := range kc.Users {
		if user.Name == name {
			return &user
		}
	}
	return nil
}

// FindCluster 返回指定集群名对应的集群信息。
func (kc *KubeConfig) FindCluster(name string) *KubeCluster {
	for _, cluster := range kc.Clusters {
		if cluster.Name == name {
			return &cluster
		}
	}
	return nil
}

// PodStateVerification 表示对 pod 状态的验证。
// 任何时候你有一组要操作或查询的 pod，
// 都可以用这个结构体声明式地识别它们。
type PodStateVerification struct {
	// 可选：只有带 k=v 标签的 pod 才能通过此过滤器。
	Selectors map[string]string

	// 必填：对 pod 有效的阶段。
	ValidPhases []v1.PodPhase

	// 可选：只有通过该函数的 pod 才能通过过滤器
	// 验证 pod。
	// 作为优化，除布尔过滤器外，
	// 该函数还允许返回 error。
	// error 表示应停止对 pod 集合的轮询。
	Verify func(v1.Pod) (bool, error)

	// 可选：只有该名称的 pod 才能通过过滤器。
	PodName string
}

// ClusterVerification 是用于验证集群状态的结构体。
type ClusterVerification struct {
	client    clientset.Interface
	namespace *v1.Namespace // pointer rather than string, since ns isn't created until before each.
	podState  PodStateVerification
}

// NewClusterVerification 创建一个新的集群验证。
func (f *Framework) NewClusterVerification(namespace *v1.Namespace, filter PodStateVerification) *ClusterVerification {
	return &ClusterVerification{
		f.ClientSet,
		namespace,
		filter,
	}
}

func passesPodNameFilter(pod v1.Pod, name string) bool {
	return name == "" || strings.Contains(pod.Name, name)
}

func passesVerifyFilter(pod v1.Pod, verify func(p v1.Pod) (bool, error)) (bool, error) {
	if verify == nil {
		return true, nil
	}

	verified, err := verify(pod)
	// 如果返回错误，按定义 pod 验证失败
	if err != nil {
		return false, err
	}
	return verified, nil
}

func passesPhasesFilter(pod v1.Pod, validPhases []v1.PodPhase) bool {
	passesPhaseFilter := false
	for _, phase := range validPhases {
		if pod.Status.Phase == phase {
			passesPhaseFilter = true
		}
	}
	return passesPhaseFilter
}

// filterLabels 返回带标签的 pod 列表。
func filterLabels(ctx context.Context, selectors map[string]string, cli clientset.Interface, ns string) (*v1.PodList, error) {
	var err error
	var selector labels.Selector
	var pl *v1.PodList
	// 基于 selector 列举 pod。相比手动过滤
	// 所有内容，这可能只是个微小的优化。
	if len(selectors) > 0 {
		selector = labels.SelectorFromSet(labels.Set(selectors))
		options := metav1.ListOptions{LabelSelector: selector.String()}
		pl, err = cli.CoreV1().Pods(ns).List(ctx, options)
	} else {
		pl, err = cli.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	}
	return pl, err
}

// filter 过滤通过过滤条件的 pod。它可用于组合
// 更有用的抽象，如 ForEach、WaitFor 等，
// 测试可以直接使用它们。
func (p *PodStateVerification) filter(ctx context.Context, c clientset.Interface, namespace *v1.Namespace) ([]v1.Pod, error) {
	if len(p.ValidPhases) == 0 || namespace == nil {
		panic(fmt.Errorf("Need to specify a valid pod phases (%v) and namespace (%v). ", p.ValidPhases, namespace))
	}

	ns := namespace.Name
	pl, err := filterLabels(ctx, p.Selectors, c, ns) // Build an v1.PodList to operate against.
	Logf("Selector matched %v pods for %v", len(pl.Items), p.Selectors)
	if len(pl.Items) == 0 || err != nil {
		return pl.Items, err
	}

	unfilteredPods := pl.Items
	filteredPods := []v1.Pod{}
ReturnPodsSoFar:
	// 下一步：pod 必须匹配用户指定的至少一个状态
	for _, pod := range unfilteredPods {
		if !(passesPhasesFilter(pod, p.ValidPhases) && passesPodNameFilter(pod, p.PodName)) {
			continue
		}
		passesVerify, err := passesVerifyFilter(pod, p.Verify)
		if err != nil {
			Logf("Error detected on %v : %v !", pod.Name, err)
			break ReturnPodsSoFar
		}
		if passesVerify {
			filteredPods = append(filteredPods, pod)
		}
	}
	return filteredPods, err
}

// WaitFor 按 PodStateVerification 定义等待
// 至少指定数量的 pod 验证通过。
func (cl *ClusterVerification) WaitFor(ctx context.Context, atLeast int, timeout time.Duration) ([]v1.Pod, error) {
	pods := []v1.Pod{}
	var returnedErr error

	err := wait.PollUntilContextTimeout(ctx, 1*time.Second, timeout, false, func(ctx context.Context) (bool, error) {
		pods, returnedErr = cl.podState.filter(ctx, cl.client, cl.namespace)

		// 失败
		if returnedErr != nil {
			Logf("Cutting polling short: We got an error from the pod filtering layer.")
			// pod 过滤返回错误时停止轮询，这不该发生，
			// 它表示例如 client 坏了或与 pod 无关的问题。
			return false, returnedErr
		}
		Logf("Found %v / %v", len(pods), atLeast)

		// 成功
		if len(pods) >= atLeast {
			return true, nil
		}
		// 继续尝试...
		return false, nil
	})
	Logf("WaitFor completed with timeout %v.  Pods found = %v out of %v", timeout, len(pods), atLeast)
	return pods, err
}

// WaitForOrFail 提供 WaitFor 的简写，出错即失败。
func (cl *ClusterVerification) WaitForOrFail(ctx context.Context, atLeast int, timeout time.Duration) {
	pods, err := cl.WaitFor(ctx, atLeast, timeout)
	if err != nil || len(pods) < atLeast {
		Failf("Verified %v of %v pods , error : %v", len(pods), atLeast, err)
	}
}

// ForEach 对每个可验证的 pod 运行函数。注意它不等待 "n" 个 pod 验证通过，
// 所以如果你对 pod 状态要求严格，它可能很快返回。
//
// 例如，如果你要求至少 5 个 pod 运行测试才算通过，
// 最好先调用 "clusterVerification.WaitFor(5)"，再调用 clusterVerification.ForEach。
func (cl *ClusterVerification) ForEach(ctx context.Context, podFunc func(v1.Pod)) error {
	pods, err := cl.podState.filter(ctx, cl.client, cl.namespace)
	if err == nil {
		if len(pods) == 0 {
			Failf("No pods matched the filter.")
		}
		Logf("ForEach: Found %v pods from the filter.  Now looping through them.", len(pods))
		for _, p := range pods {
			podFunc(p)
		}
	} else {
		Logf("ForEach: Something went wrong when filtering pods to execute against: %v", err)
	}

	return err
}
