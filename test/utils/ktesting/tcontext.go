/*
Copyright 2023 The Kubernetes Authors.

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

package ktesting

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/onsi/gomega"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"
	"k8s.io/kubernetes/test/utils/ktesting/internal"
)

// Underlier 是 [TContext.Logger] 背后的每个测试对应的 LogSink
// 所实现的额外接口。配合 [initoption.BufferLogs]，它可用于
// 在内存中捕获日志输出，以便在测试中检查。
type Underlier = ktesting.Underlier

// DefaultCleanupGracePeriod 是 [TContext] 在其底层测试套件的截止时间之前
// （通常通过 "go test -timeout" 确定）被取消的时间量。这给正在运行的测试
// 留出时间，使其能带着有用的超时错误失败。之后，所有清理回调
// 在测试二进制文件被终止前，还有剩余时间可以完成。
//
// 要使其生效，测试中的每个阻塞调用都必须响应 [TContext] 的取消。
//
// 当使用 Ginkgo 管理测试套件并运行测试时，清理宽限期会被忽略，
// 因为 Ginkgo 自己管理超时。
//
// 该值可通过 [initoption.WithCleanupGracePeriod] 按 [TContext] 覆盖。
const DefaultCleanupGracePeriod = 5 * time.Second

// TB 是 [testing.T]、[testing.B]、[testing.F] 和
// [github.com/onsi/ginkgo/v2] 共有的接口，ktesting 自身依赖它，
// 或将其透传（如 Chdir、TempDir）。
//
// 与 [testing.TB] 不同，它也可以在 testing 包之外实现。
//
// ktesting 使用 Fatal/Error/Skip，而不是 Log + FailNow/Failed/SkipNow，
// 因为当由 Ginkgo 实现时，把失败或跳过的理由直接传给为此目的准备的
// 方法更好。
type TB interface {
	Attr(key, value string)
	Chdir(dir string)
	Cleanup(func())
	Error(args ...any)
	Errorf(format string, args ...any)
	Fail()
	FailNow()
	Failed() bool
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Helper()
	Log(args ...any)
	Logf(format string, args ...any)
	Name() string
	Setenv(key, value string)
	Skip(args ...any)
	Skipf(format string, args ...any)
	SkipNow()
	Skipped() bool
	TempDir() string
}

// ContextTB 增加了对带显式 context 参数的清理回调的支持。
// 在与 Ginkgo 集成时会使用到它：此时 CleanupCtx
// 通过 ginkgo.DeferCleanup 实现。
type ContextTB interface {
	TB
	CleanupCtx(func(ctx context.Context))
}

// Init 可在单元测试或集成测试中调用，用于创建一个
// 测试上下文，它具备以下特点：
// - 拥有每个测试对应的 logger，详细程度源自 -v 命令行 flag
// - 在测试结束时（通过 [TB.Cleanup]）被取消
//
// 注意，该测试上下文支持 [TB] 和 [context.Context] 的接口，
// 因此在需要的地方可以当作其中任一种来用。它还提供了
// 额外的方法，用于获取 logger 以及提前取消上下文，
// 这在希望等待 goroutine 在取消后终止的测试中
// 很有用。
//
// 如果 [TB] 的实现也实现了 [ContextTB]，那么
// [TContext.CleanupCtx] 会使用 [ContextTB.CleanupCtx]，并采用
// 传入该回调的 context。这可用于让 Ginkgo
// 为清理代码创建一个新的 context。
//
// 每个测试可以多次调用，以获得具有独立取消机制的
// 不同上下文。上述默认行为可以通过 [initoption] 中定义的
// 可选的函数式选项修改。
//
// 可以在 synctest bubble 内部调用。此时不会初始化信号处理
// （SIGINT 时清理、SIGUSR1 时报告进度），因为在 bubble 中运行的代码
// 不应依赖外部输入。如果某个父测试已经初始化了进度报告，
// 那么进度报告仍然可用。
// 因此，推荐的模式是先初始化 ktesting，再创建
// synctest bubble：
//
//	func TestSomething(t *testing.T) { ktesting.Init(t).SyncTest("", testSomething) }
//	func testSomething(tCtx ktesting.TContext) { ... }
//
// 这种模式的另一个好处是，测试代码不会意外地
// 直接使用 testing.T 实例。普通测试也可以这样做：
//
//	func TestSomething(t *testing.T) { testSomething(ktesting.Init(t)) }
//	func testSomething(tCtx ktesting.TContext) { ... }
func Init(tb TB, opts ...InitOption) TContext {
	tb.Helper()

	c := internal.InitConfig{
		PerTestOutput: true,
	}
	for _, opt := range opts {
		opt(&c)
	}

	// 不需要 Deadline 实现，testing.B 没有该方法。
	// 但如果有的话，我们就用它来确定截止时间，并
	// 在其之前不久设置一个超时。
	//
	// 这也让我们能够检测 synctest bubble。
	isSyncTest := false
	var deadline *time.Time
	if deadlineTB, deadlineOK := tb.(interface {
		Deadline() (time.Time, bool)
	}); deadlineOK {
		func() {
			defer func() {
				// 在 synctest bubble 内调用 testing.T.Deadline 会 panic。
				// 没有 API 可以提前检测到这一点，因此这里通过
				// 捕获 panic 来应对。
				if r := recover(); r != nil {
					isSyncTest = true
				}
			}()
			if d, ok := deadlineTB.Deadline(); ok {
				deadline = &d
			}
		}()
	}

	ctx := defaultProgressReporter.init(tb, isSyncTest)
	var header func() string
	if c.PerTestOutput {
		logger := newLogger(tb, c.BufferLogs)
		ctx = klog.NewContext(ctx, logger)
		header = klogHeader
	}

	// 确定有效的清理宽限期：如果调用方提供的值为正数，
	// 则使用该值，否则回退到 DefaultCleanupGracePeriod。
	gracePeriod := c.CleanupGracePeriod
	if gracePeriod <= 0 {
		gracePeriod = DefaultCleanupGracePeriod
	}

	var cancelTimeout func(cause string)
	if deadline != nil {
		timeLeft := time.Until(*deadline)
		timeLeft -= gracePeriod
		ctx, cancelTimeout = withTimeout(ctx, tb, timeLeft, fmt.Sprintf("test suite deadline (%s) is close, need to clean up before the %s cleanup grace period", deadline.Truncate(time.Second), gracePeriod))
	}

	// 使用上面确定的 context 和设置构造新的 TContext。
	tCtx := InitCtx(ctx, tb)
	tCtx.cleanupGracePeriod = gracePeriod
	tCtx.isSyncTest = isSyncTest
	if cancelTimeout != nil {
		tCtx.cancel = cancelTimeout
	} else {
		tCtx = tCtx.WithCancel()
		tCtx.Cleanup(func() {
			tCtx.Cancel(cleanupErr(tCtx.Name()).Error())
		})
	}
	tCtx.perTestHeader = header

	return tCtx
}

func newLogger(tb TB, bufferLogs bool) klog.Logger {
	config := ktesting.NewConfig(
		ktesting.AnyToString(func(v interface{}) string {
			// 对于字符串表示形式
			// "显而易见"的基本类型，我们使用 fmt.Sprintf，
			// 因为 format.Object 总会添加
			// 一个 <"type"> 前缀，这对简单值
			// 来说太长了。
			switch v := v.(type) {
			case int, int32, int64, uint, uint32, uint64, float32, float64, bool:
				return fmt.Sprintf("%v", v)
			case string:
				return v
			default:
				return strings.TrimSpace(format.Object(v, 1))
			}
		}),
		ktesting.VerbosityFlagName("v"),
		ktesting.VModuleFlagName("vmodule"),
		ktesting.BufferLogs(bufferLogs),
	)

	// 复制 klog 的设置，而不是让 ktesting logger
	// 直接可配置。
	var fs flag.FlagSet
	config.AddFlags(&fs)
	for _, name := range []string{"v", "vmodule"} {
		from := flag.CommandLine.Lookup(name)
		to := fs.Lookup(name)
		if err := to.Value.Set(from.Value.String()); err != nil {
			panic(err)
		}
	}

	// 确保日志一致：这个 klog.Logger 写入 tb 时会加上
	// 日期/时间头，而我们自己的包装器也为 Log/Logf/...
	// 模拟了这种行为。
	logger := ktesting.NewLogger(tb, config)
	return logger
}

type InitOption = initoption.InitOption

// InitCtx 是 [Init] 的一个变体，它使用已有的 context，
// 以及其中保存的 logger 和超时设置。
func InitCtx(ctx context.Context, tb TB, opts ...InitOption) TContext {
	c := internal.InitConfig{}
	for _, opt := range opts {
		opt(&c)
	}
	gracePeriod := c.CleanupGracePeriod
	if gracePeriod <= 0 {
		gracePeriod = DefaultCleanupGracePeriod
	}
	return TContext{
		Context:            ctx,
		testingTB:          testingTB{TB: tb},
		cleanupGracePeriod: gracePeriod,
	}
}

// withTB 使用另一个 TB 实例构造新的 TContext。
//
// 它在内部用于设置 context 的某些部分（尤其是
// client），先在根测试中完成，然后再运行子测试：
//
//	func TestSomething(t *testing.T) {
//	   tCtx := ktesting.Init(t)
//	   ...
//	   tCtx = ktesting.WithRESTConfig(tCtx, config)
//
//	   t.Run("sub", func (t *testing.T) {
//	       tCtx := ktesting.WithTB(tCtx, t)
//	       ...
//	   })
//
// withTB 会为子测试设置取消机制，并使用每个测试独立的输出。
func (tCtx TContext) withTB(tb TB) TContext {
	tCtx.testingTB.TB = tb
	if tCtx.perTestHeader != nil {
		logger := newLogger(tb, false /* don't buffer logs in sub-test */)
		tCtx.Context = klog.NewContext(tCtx.Context, logger)
	}
	return tCtx.WithCancel()
}

// run 实现了不同的 Run 和 SyncTest 方法。它不是导出方法，
// 因为 tCtx.Run 更容易被发现（用法与普通 Go
// 一致）。
func run(tCtx TContext, name string, syncTest bool, cb func(tCtx TContext)) bool {
	tCtx.Helper()
	switch tb := tCtx.TB().(type) {
	case *testing.T:
		if syncTest {
			f := func(t *testing.T) {
				// 不能把父级的取消 channel
				// 传播到 bubble 中，否则会导致
				// "panic: receive on synctest channel from outside bubble"。
				//
				// 同步测试应该不需要整个测试套件的超时，
				// 所以这样似乎没问题。
				tCtx.isSyncTest = true
				tCtx = tCtx.WithoutCancel().withTB(t)
				cb(tCtx)
			}
			if name != "" {
				return tb.Run(name, func(t *testing.T) { synctest.Test(t, f) })
			}
			synctest.Test(tb, f)
			return true
		}
		return tb.Run(name, func(t *testing.T) {
			cb(tCtx.withTB(t))
		})
	case *testing.B:
		if !syncTest {
			return tb.Run(name, func(b *testing.B) {
				cb(tCtx.withTB(b))
			})
		}
	}

	what := "Run"
	if syncTest {
		what = "SyncTest"
	}
	tCtx.Fatalf("%s not implemented, underlying %T does not support it", what, tCtx.TB())

	return false
}

// WithContext 使用另一个 Context 实例构造新的 TContext。
// 它可用于接收 Context 的回调中，例如
// 来自 Gomega 的回调：
//
//	gomega.Eventually(tCtx, func(ctx context.Context) {
//	   tCtx := ktesting.WithContext(tCtx, ctx)
//	   ...
//
// 取消和截止时间由新的 context 决定。
// 值会先在新的 context 中查找，然后在旧的 context 中查找。
// 换句话说，之前通过 WithValue 设置的值仍然
// 可用。
func (tCtx TContext) WithContext(ctx context.Context) TContext {
	tCtx.Context = &chainContext{Context: ctx, previousCtx: tCtx.Context}
	return tCtx
}

// WithValue 包装了 [context.WithValue]，使结果仍然是 TContext。
func (tCtx TContext) WithValue(key, val any) TContext {
	ctx := context.WithValue(tCtx, key, val)
	return tCtx.WithContext(ctx)
}

type chainContext struct {
	context.Context
	previousCtx context.Context
}

func (ctx *chainContext) Value(key any) any {
	if val := ctx.Context.Value(key); val != nil {
		return val
	}
	return ctx.previousCtx.Value(key)
}

// TContext 实现了 [context.Context]、[testing.TB] 以及一些额外方法。
// [TContext] 是用于引用 TC 的公开指针类型。
// 变量通常命名为 tCtx。为确保测试代码不会无意中
// 直接使用 `t`，推荐使用两个函数：
//
//	func TestSomething(t *testing.T) { testSomething(ktesting.Init(t)) }
//	func testSomething(tCtx ktesting.TContext) { ... }
//
// 日志输出与当前测试关联，并带有类似 klog 的头部，
// 这样后处理就能区分日志输出（以头部开头）和
// 失败消息（头部在后面）。错误
// （[Error]、[Errorf]）以 "ERROR" 为前缀记录，致命错误
// （[Fatal]、[Fatalf]）以 "FATAL ERROR" 为前缀。使用缩进是为了确保
// 属于同一日志条目的后续行能被后正确处理，
// 同时让头部更醒目。
//
// 即使使用普通的 Go [testing]，tCtx 也提供了 Ginkgo 才有的功能：
//   - context 包含一个截止时间，它在整体超时之前
//     足够早地到期，使清理代码仍能运行。
//   - 通过 [CleanupCtx] 注册的清理回调
//     可以获得各自独立的 context。
//   - CTRL-C 会中止、打印进度报告，然后先清理
//     再终止。
//   - SIGUSR1 打印进度报告，但不会中止。
//
// 在使用 [gomega.Eventually] 和 [gomega.Consistently] 进行轮询时，
// 进度报告会更有信息量。否则，它
// 只能报告哪些测试正在运行。
type TContext struct {
	// Context 使底层 context 的方法可用。
	// 不得修改它。
	context.Context

	// testingTB 使底层测试实现的方法可用。
	// 其内嵌的 TB 不得修改。
	testingTB

	// perTestHeader 是可选函数，在未使用全局 logger 时
	// 生成类似 klog 的每个测试独立头部。
	perTestHeader func() string

	// 用于 Cancel
	cancel func(cause string)

	// steps 是传给 WithStep 的各步骤的拼接（"step1: step2: step3: "）。
	// 没有步骤时为空。
	steps string

	// 用于 IsSyncTest
	isSyncTest bool

	// 用于 WithNamespace
	namespace string

	// capture 若非 nil，会改变 Error/Errorf/Fatal/Fatalf/Fail/FailNow，
	// 使其拦截问题并转换为错误。日志消息
	// 则原样透传。
	//
	// 由 WithError 使用。
	capture *capture

	// cleanupGracePeriod 是该上下文有效的清理宽限期。
	// 它始终非零：Init 和 InitCtx 都会从提供的选项中
	// 解析它，或回退到 DefaultCleanupGracePeriod。
	cleanupGracePeriod time.Duration
}

type capture struct {
	mutex  sync.Mutex
	errors []error
	failed bool
}

// 需要 testingTB 来避免 tContext 中
// 字段与方法之间的命名冲突。
type testingTB struct {
	// TB 使底层测试实现的方法可用。
	// 特别是 Helper 必须直接调用，不能通过包装器。
	// 不得修改它。
	TB
}

// Parallel 表示该测试将与（且仅与）其他并行测试
// 一起运行。换句话说，每个打算并行运行的测试
// 都需要调用它。
//
// 仅支持 Go 单元测试，在其他地方调用会导致测试失败。
//
// 当单元测试因使用 -test.count 或 -test.cpu 而多次运行时，
// 同一测试的多个实例之间永远不会并行运行。
func (tCtx TContext) Parallel() {
	if tb, ok := tCtx.TB().(interface{ Parallel() }); ok {
		tb.Parallel()
	} else {
		tCtx.Fatalf("Parallel not implemented, underlying %T does not support it", tCtx.TB())
	}
}

// 可以调用 Cancel，在测试结束前取消 context。
// 使用 context 控制 goroutine 并等待这些 goroutine
// 终止的测试必须调用 Cancel，以避免死锁。
//
// cause 若非空，会被转换为一个等价于 context.Canceled 的错误。
// context.Cause 将针对该 context 返回
// 这个错误。
func (tCtx TContext) Cancel(cause string) {
	if tCtx.cancel != nil {
		tCtx.cancel(cause)
	}
}

// CleanupCtx 注册一个回调，在测试结束后调用。
// 回调按后进先出（LIFO）顺序调用。
//
// 应优先使用 CleanupCtx，因为它会自动取消 context。
// 下面这段有问题的（！）清理代码会使用已取消的 context，
// 这是不可取的：
//
//	// tCtx gets canceled when the test ends and before the callback runs.
//	tCtx.Cleanup(func() { /* do something with the test's tCtx */ })
//
// 更安全的清理代码写法是：
//
//	tCtx.CleanupCtx(func (tCtx ktesting.TContext) { /* do something with the cleanup's tCtx */ })
//
// logger 和 client 与调用 CleanupCtx 时所在的 TContext
// 中的相同。
func (tCtx TContext) CleanupCtx(cb func(TContext)) {
	tCtx.Helper()

	if tb, ok := tCtx.TB().(ContextTB); ok {
		// 使用基础 TB（很可能是 Ginkgo）提供的 context。
		tb.CleanupCtx(func(ctx context.Context) {
			tCtx := tCtx.WithContext(ctx)
			cb(tCtx)
		})
		return
	}

	tCtx.Cleanup(func() {
		// 使用新的 context。这是 "go test" 的代码路径，
		// 此时 context *没有* 截止时间。在上面 Ginkgo 的代码路径中，
		// Ginkgo 更完善，也会对接受 context 的清理调用
		// 应用超时。
		childCtx := tCtx.WithContext(context.WithoutCancel(tCtx))
		cb(childCtx)
	})
}

// Run 将 cb 作为名为 name 的子测试运行。它会阻塞，直到 cb 返回，
// 或调用 t.Parallel 成为并行测试。
//
// 仅支持 Go 单元测试或基准测试。在其他地方调用会使当前
// 测试失败。
func (tCtx TContext) Run(name string, cb func(tCtx TContext)) bool {
	return run(tCtx, name, false, cb)
}

// SyncTest 使用 [synctest.Test] 在 bubble 内执行回调。
// 如果 name 非空，则创建一个新的子测试，否则直接
// 在当前测试上下文中创建 bubble。
//
// 仅适用于 Go 单元测试。
//
// 不支持在 SIGINT 时清理，因为在 bubble 内运行的代码
// 不应依赖外部输入。
func (tCtx TContext) SyncTest(name string, cb func(tCtx TContext)) bool {
	return run(tCtx, name, true, cb)
}

// 如果 context 由 SyncTest 创建，IsSyncTest 返回 true。
//
// 在这样的 context 中可以使用 Wait。这可用于
// 既在 synctest bubble 内、又在 bubble 外运行的代码：
//   - 在 bubble 内，Wait 可用于阻塞等待后台活动
//     平息（即 "durably blocked"）。Eventually 和 Consistently
//     都会先调用 Wait，然后检查
//     条件。
//   - 在 bubble 外，必须使用轮询或某种同步机制。
func (tCtx TContext) IsSyncTest() bool {
	return tCtx.isSyncTest
}

// Wait 调用 [synctest.Wait]，从而确保所有后台
// 活动已平息（即 "durably blocked"）。
//
// 仅在由 SyncTest 启动的 bubble 内有效（可用
// IsSyncTest 检查），在其他地方调用会 panic。
func (tCtx TContext) Wait() {
	synctest.Wait()
}

// TB 返回底层的 TB。这可用于"打破封装"，
// 再转回 testing.T 或 TB。必须调用 TB，
// 因为 TContext 包装了底层 TB。
//
// 例如，在基准测试中需要转回 testing.B，
// 因为并非所有基准测试专用的方法都有提供。
// 这个例子再次使用单独的函数，以确保
// 不会无意中使用 b：
//
//	func BenchmarkSomething(b *testing.B) { benchmarkSomething(ktesting.Init(b) }
//	func benchmarkSomething(tCtx ktesting.TContext) {
//	    ... set up with tCtx ...
//	    for tCtx.TB().(*testing.B).Loop() {
//	        ...
//	    }
//	}
func (tCtx TContext) TB() TB { return tCtx.testingTB.TB }

// Logger 返回当前测试对应的 logger。这是调用
// klog.FromContext 的快捷方式。
//
// 通过此 logger 和 TB 接口（如 Logf）输出的内容
// 格式一致。TB 接口生成单条消息字符串，而 Logger
// 支持结构化日志，并可传递给需要 logger 的代码。
//
// 要在堆栈回溯时跳过中间的辅助函数，
// 可在这些函数中调用 TB.Helper。
func (tCtx TContext) Logger() klog.Logger {
	return klog.FromContext(tCtx.Context)
}

// Expect 包装了 [gomega.Expect]，失败时会通过
// [TContext.Fatal] 报告。与 [gomega.Expect] 一样，
// 可以传入额外的值。断言要通过，这些值都必须为 nil。
// 这可用于返回某个值和 error 的函数，
// error 会被自动检查。
//
//	myAmazingThing := func(int, error) { ...}
//	tCtx.Expect(myAmazingThing()).Should(gomega.Equal(1))
func (tCtx TContext) Expect(actual interface{}, extra ...interface{}) gomega.Assertion {
	return gomegaAssertion(tCtx, true, actual, extra...)
}

// Require 是 Expect 的别名。
func (tCtx TContext) Require(actual interface{}, extra ...interface{}) gomega.Assertion {
	return gomegaAssertion(tCtx, true, actual, extra...)
}

// Assert 同样包装了 [gomega.Expect]，但与 Expect = Require 不同，
// 它通过 [TContext.Error] 报告失败。这样可以
// 测试多个不同的断言。
func (tCtx TContext) Assert(actual interface{}, extra ...interface{}) gomega.Assertion {
	return gomegaAssertion(tCtx, false, actual, extra...)
}

// WithNamespace 创建一个新的 context，其中带有可通过 [Namespace] 获取的 Kubernetes 命名空间名称。
func (tCtx TContext) WithNamespace(namespace string) TContext {
	tCtx.namespace = namespace
	return tCtx
}

// Namespace 返回此前通过 WithNamespace 设置的 Kubernetes 命名空间名称，
// 如果没有设置则返回空字符串。
//
// 需要创建命名空间级对象的测试应使用这个命名空间。
// 该名称在该测试上下文中保证唯一，因此并行运行的测试
// 需要进行设置，使每个测试都有自己的命名空间。
func (tCtx TContext) Namespace() string {
	return tCtx.namespace
}
