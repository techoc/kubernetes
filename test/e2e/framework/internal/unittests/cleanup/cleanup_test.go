/*
Copyright 2022 The Kubernetes Authors.

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

// 本测试使用的 etcd 在 Linux 上仅对 AMD64 和 ARM64 提供完整支持。
// 我们也在 tier 2 的 PPC64LE 和 s390x 上测试。
//
// 如果在 windows / macOS 上测试，请注意这些平台没有上游 CI。
//
// https://etcd.io/docs/v3.6/op-guide/supported-platform/#support-tiers

package cleanup

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/internal/output"
	testapiserver "k8s.io/kubernetes/test/utils/apiserver"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// 下面代码的行号会在 TestFailureOutput 中校验。
// 移动这段代码或修改上面的 import 语句时请小心。
// 这里有意留了一些空行，可在未来新增 import 时删掉以抵消行号变化。
//
//
//
// 这里必须是第 50 行。

func init() {
	framework.NewFrameworkExtensions = append(framework.NewFrameworkExtensions,
		// 该回调在 NewDefaultFramework 完成后立即运行。
		func(f *framework.Framework) {
			ginkgo.BeforeEach(func() { framework.Logf("extension before") })
			ginkgo.AfterEach(func() { framework.Logf("extension after") })
		},
	)
}

var _ = ginkgo.Describe("e2e", func() {
	ginkgo.BeforeEach(func() {
		logBeforeHelper()
	})

	f := framework.NewDefaultFramework("test-namespace")

	// BeforeEach/AfterEach 按先进先出顺序运行。

	ginkgo.BeforeEach(func() {
		framework.Logf("before #1")
	})

	ginkgo.BeforeEach(func() {
		framework.Logf("before #2")
	})

	ginkgo.AfterEach(func() {
		framework.Logf("after #1")
		if f.ClientSet == nil {
			framework.Fail("Wrong order of cleanup operations: framework.AfterEach already ran and cleared f.ClientSet.")
		}
	})

	ginkgo.AfterEach(func() {
		framework.Logf("after #2")
	})

	ginkgo.It("works", func(ctx context.Context) {
		// DeferCleanup 按先进后出顺序调用
		ginkgo.DeferCleanup(func() {
			framework.Logf("cleanup last")
		})
		ginkgo.DeferCleanup(func() {
			framework.Logf("cleanup first")
		})

		ginkgo.DeferCleanup(framework.IgnoreNotFound(f.ClientSet.CoreV1().PersistentVolumes().Delete), "simple", metav1.DeleteOptions{})
		fail := func(ctx context.Context, name string) error {
			return fmt.Errorf("fake error for %q", name)
		}
		ginkgo.DeferCleanup(framework.IgnoreNotFound(fail), "failure") // 没有失败时输出不会出现在 JUnit 中。

		tCtx := f.TContext(ctx)
		tCtx.Log("log", "hello", "world")
		tCtx.Logger().Info("info hello world")
		var discardLogger klog.Logger
		tCtx = tCtx.WithLogger(discardLogger)
		oldCtx := tCtx
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			if tCtx.Logger() != discardLogger {
				tCtx.Errorf("expected discard logger in context, got %+v", tCtx.Logger())
			}
			_, ok := tCtx.Value("GINKGO_SPEC_CONTEXT").(ginkgo.SpecContext)
			if !ok {
				tCtx.Errorf("expected Ginkgo context, got %+v", tCtx.Context)
			}
			if oldCtx.Err() == nil {
				tCtx.Error("Ginkgo.It context should be canceled but isn't")
			}
			if tCtx.Err() != nil {
				tCtx.Errorf("Ginkgo.DeferCleanup context should not be canceled but is: %v", tCtx.Err())
			}
		})

		// 这里可以添加更多测试用例，不会影响现有测试的
		// 行号。
	})
})

// 日志实现做栈回溯时必须跳过 logBeforeHelper。
// 日志实现做栈回溯时必须跳过 logBeforeHelper。
func logBeforeHelper() {
	ginkgo.GinkgoHelper()
	framework.Logf("before")
}

const (
	ginkgoOutput = `> Enter [BeforeEach] e2e - cleanup_test.go:63 <time>
<klog> cleanup_test.go:64] before
< Exit [BeforeEach] e2e - cleanup_test.go:63 <time>
> Enter [BeforeEach] e2e - set up framework | framework.go:xxx <time>
STEP: Creating a kubernetes client - framework.go:xxx <time>
<klog> util.go:xxx] >>> kubeConfig: yyy/kube.config
STEP: Building a namespace api object, basename test-namespace - framework.go:xxx <time>
<klog> framework.go:xxx] Skipping waiting for service account
< Exit [BeforeEach] e2e - set up framework | framework.go:xxx <time>
> Enter [BeforeEach] e2e - cleanup_test.go:56 <time>
<klog> cleanup_test.go:56] extension before
< Exit [BeforeEach] e2e - cleanup_test.go:56 <time>
> Enter [BeforeEach] e2e - cleanup_test.go:71 <time>
<klog> cleanup_test.go:72] before #1
< Exit [BeforeEach] e2e - cleanup_test.go:71 <time>
> Enter [BeforeEach] e2e - cleanup_test.go:75 <time>
<klog> cleanup_test.go:76] before #2
< Exit [BeforeEach] e2e - cleanup_test.go:75 <time>
> Enter [It] works - cleanup_test.go:90 <time>
<klog> cleanup_test.go:106] log hello world
<klog> cleanup_test.go:107] "info hello world"
< Exit [It] works - cleanup_test.go:90 <time>
> Enter [AfterEach] e2e - cleanup_test.go:57 <time>
<klog> cleanup_test.go:57] extension after
< Exit [AfterEach] e2e - cleanup_test.go:57 <time>
> Enter [AfterEach] e2e - cleanup_test.go:79 <time>
<klog> cleanup_test.go:80] after #1
< Exit [AfterEach] e2e - cleanup_test.go:79 <time>
> Enter [AfterEach] e2e - cleanup_test.go:86 <time>
<klog> cleanup_test.go:87] after #2
< Exit [AfterEach] e2e - cleanup_test.go:86 <time>
> Enter [DeferCleanup (Each)] e2e - cleanup_test.go:111 <time>
< Exit [DeferCleanup (Each)] e2e - cleanup_test.go:111 <time>
> Enter [DeferCleanup (Each)] e2e - cleanup_test.go:103 <time>
[FAILED] DeferCleanup callback returned error: fake error for "failure"
In [DeferCleanup (Each)] at: cleanup_test.go:103 <time>
< Exit [DeferCleanup (Each)] e2e - cleanup_test.go:103 <time>
> Enter [DeferCleanup (Each)] e2e - cleanup_test.go:99 <time>
< Exit [DeferCleanup (Each)] e2e - cleanup_test.go:99 <time>
> Enter [DeferCleanup (Each)] e2e - cleanup_test.go:95 <time>
<klog> cleanup_test.go:96] cleanup first
< Exit [DeferCleanup (Each)] e2e - cleanup_test.go:95 <time>
> Enter [DeferCleanup (Each)] e2e - cleanup_test.go:92 <time>
<klog> cleanup_test.go:93] cleanup last
< Exit [DeferCleanup (Each)] e2e - cleanup_test.go:92 <time>
> Enter [DeferCleanup (Each)] e2e - dump namespaces | framework.go:xxx <time>
< Exit [DeferCleanup (Each)] e2e - dump namespaces | framework.go:xxx <time>
> Enter [DeferCleanup (Each)] e2e - tear down framework | framework.go:xxx <time>
STEP: Destroying namespace "test-namespace-zzz" for this suite. - framework.go:xxx <time>
< Exit [DeferCleanup (Each)] e2e - tear down framework | framework.go:xxx <time>
`
)

func TestCleanup(t *testing.T) {
	// 控制面很吵，会随机通过 klog 输出日志，例如：
	// E0912 07:08:46.100164   75466 controller.go:254] unable to sync kubernetes service: Endpoints "kubernetes" is invalid: subsets[0].addresses[0].ip: Invalid value: "127.0.0.1": may not be in the loopback range (127.0.0.0/8, ::1/128)
	//
	// 通过创建 ktesting logger 并注册为全局
	// 默认 logger，可在失败时把控制面输出带入
	// "go test" 输出（便于调试！），
	// 同时又让它不进入下面测试要比较的
	// Ginkgo 捕获输出。
	//
	// 这样做有几点小代价：
	// - 控制面日志的源码位置显示为 klog.go，因为 klog 没有正确
	//   跳过自己的 helper 函数。这没关系，通常
	//   也不该这样把 ktesting 装成日志后端。
	// - klog.Infof 消息会多打印一个换行。
	//
	logger, _ := ktesting.NewTestContext(t)
	klog.SetLogger(logger)

	apiServer := testapiserver.StartAPITestServer(t)

	// 这模拟了 test/e2e 使用框架的方式以及用户
	// 调用 test/e2e 的方式。
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.RegisterClusterFlags(flag.CommandLine)
	for flagname, value := range map[string]string{
		"kubeconfig": apiServer.KubeConfigFile,
		// fake 集群不支持某些特性。
		"e2e-verify-service-account": "false",
		"allowed-not-ready-nodes":    "-1",
		// 这简化了文本比较。
		"ginkgo.no-color": "true",
	} {
		if err := flag.Set(flagname, value); err != nil {
			t.Fatalf("set %s: %v", flagname, err)
		}
	}
	framework.AfterReadingAllFlags(&framework.TestContext)
	suiteConfig, reporterConfig := framework.CreateGinkgoConfig()

	expected := output.TestResult{
		NormalizeOutput: normalizeOutput,
		Suite: reporters.JUnitTestSuite{
			Tests:    1,
			Failures: 1,
			Errors:   0,
			Disabled: 0,
			Skipped:  0,

			TestCases: []reporters.JUnitTestCase{
				{
					Name:   "[It] e2e works",
					Status: "failed",
					Failure: &reporters.JUnitFailure{
						Type: "failed",
						Description: `[FAILED] DeferCleanup callback returned error: fake error for "failure"
In [DeferCleanup (Each)] at: cleanup_test.go:103 <time>
`,
					},
					SystemErr: ginkgoOutput,
				},
			},
		},
	}

	output.TestGinkgoOutput(t, expected, suiteConfig, reporterConfig)
}

func normalizeOutput(output string) string {
	for exp, replacement := range map[string]string{
		// 忽略 framework 源码中的行号（容易变化）。
		`(framework|util)\.go:\d+`: `$1.go:xxx`,
		// 配置文件名每次运行都不同。
		`kubeConfig: .*/kube.config`: `kubeConfig: yyy/kube.config`,
		// 命名空间的随机后缀。
		`test-namespace-\d+`: `test-namespace-zzz`,
	} {
		output = regexp.MustCompile(exp).ReplaceAllString(output, replacement)
	}
	return output
}
