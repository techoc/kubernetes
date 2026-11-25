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

// Package logcheck 通过可选的系统日志文件监控
// 扩展 Ginkgo 测试套件。目前它可以检测 DATA RACE 问题。
//
// 未来可能扩展，例如还检查
// 未处理和潜在意外错误（https://github.com/kubernetes/kubernetes/issues/122005）
// 或日志刷屏（https://github.com/kubernetes/kubernetes/issues/109297）。
//
// 由于它在日志产生时抓取输出，因此可以将*全部*数据 dump 到
// $ARTIFACTS/system-logs，而其他方法
// 可能只捕获输出的尾部（日志轮转）。
//
// 额外的检查必须选择启用，因为它们不可移植。
// 该包为此注册了额外的命令行 flag。
// 需要有专门的 job 来启用它们。
//
// 对于 DATA RACE，集群中的二进制文件必须使用 race 编译
// 检测（https://github.com/kubernetes/kubernetes/pull/133834/files）。
// 还需要指定要检查哪些容器。其他输出
// （例如 kind 集群中的 kubelet）目前无法检查。
//
// 在 ginkgo.ReportBeforeSuite 中，Ginkgo 主 worker 进程开始
// 监控日志输出：
//
//   - podlogs 包监听感兴趣的命名空间中的 Pod（默认是
//     名称中包含 "system" 的命名空间）。这些 Pod 中的每个容器都会
//     创建一个 logOutputChecker 实例，并通过 Write 向其传入日志输出行。
//     podlogs 会流式输出，类似 "kubectl logs -f"。
//
//   - 通过 node log query 特性定期获取 kubelet 日志输出。
//     每个分段的起止时间经过选择，确保
//     没有间隙。输出会去掉 journald 额外的行
//     头，然后传给每个节点对应的一个 logOutputChecker。
//
//   - logOutputChecker 根据前缀和后缀行检测 DATA RACE 报告。
//
//   - 监控期间产生的日志条目会被缓冲。
//
// 在 ginkgo.ReportAfterSuite 中，日志输出会作为检查的 stdout 输出，
// 并附上未发现问题的检查摘要。
// DATA RACE 和意外的日志错误会转为测试失败。
package logcheck

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/storage/podlogs"
)

const (
	// defaultNamespacesRE 是默认情况下需要检查哪些命名空间的正则表达式，
	// 即启用检查时：任何包含 "system" 字样的命名空间。
	//
	// 如果有识别 "system" 命名空间的 API，我们就会用它，但目前
	// 还没有（https://github.com/kubernetes/enhancements/issues/5708）。
	defaultNamespacesRE = `system`

	// defaultNodesRE 是默认正则表达式，用于确定哪些节点
	// 需要借助 log query 特性检查其 kubelet 日志输出
	//（https://kubernetes.io/docs/concepts/cluster-administration/system-logs/#log-query）。
	defaultNodesRE = `.*`

	dataRaceStart     = "WARNING: DATA RACE\n"
	dataRaceEnd       = "==================\n"
	maxBacktraceLines = 20
)

var (
	enabledLogChecks = logCheck{
		namespaces: regexpValue{
			re: regexp.MustCompile(defaultNamespacesRE),
		},
		nodes: regexpValue{
			re: regexp.MustCompile(defaultNodesRE),
		},
	}

	lc       *logChecker
	lcLogger klog.Logger
)

// logCheck 决定检查日志输出中的哪些内容。
type logCheck struct {
	// namespaces 包含一个正则表达式，用于确定需要检查哪些命名空间。
	// 这些命名空间中的所有容器都会被检查。
	// 如果正则表达式为 nil，则使用默认值。
	namespaces regexpValue

	// namespaces 包含一个正则表达式，用于确定哪些节点需要被检查
	// 通过 log query 特性获取 kubelet 日志进行检查（https://kubernetes.io/docs/concepts/cluster-administration/system-logs/#log-query）。
	nodes regexpValue

	// dataRaces 启用对 "DATA RACE" 报告的检查。
	dataRaces bool
	// 未来可能会添加更多检查。
}

// 如果启用了任何日志输出检查，any 返回 true。
func (c logCheck) any() bool {
	// 添加新检查时需要扩展此函数。
	return c.dataRaces
}

// regexpValue 为正则表达式实现 flag.Value。
type regexpValue struct {
	re *regexp.Regexp
}

var _ flag.Value = &regexpValue{}

func (r *regexpValue) String() string { return r.re.String() }
func (r *regexpValue) Set(expr string) error {
	re, err := regexp.Compile(expr)
	if err != nil {
		// 它已以 "error parsing regexp" 开头，
		// 调用方会追加表达式字符串，
		// 因此这里不需要包装错误。
		return err
	}
	r.re = re
	return nil
}

// RegisterFlags 向给定 flag set 添加用于配置本包的命令行 flag。
// 它们以 "logcheck" 为前缀。
func RegisterFlags(fs *flag.FlagSet) {
	fs.BoolVar(&enabledLogChecks.dataRaces, "logcheck-data-races", false, "enables checking logs for DATA RACE warnings")
	fs.Var(&enabledLogChecks.namespaces, "logcheck-namespaces-regexp", "all namespaces matching this regular expressions get checked")
	fs.Var(&enabledLogChecks.nodes, "logcheck-nodes-regexp", "all kubelets on nodes matching this regular expressions get checked")
}

var _ = ginkgo.ReportBeforeSuite(func(ctx ginkgo.SpecContext, report ginkgo.Report) {
	if report.SuiteConfig.DryRun {
		return
	}

	// 这只在主进程中执行。
	initialize()
})

// SIG Testing 将其作为服务运行，服务于拥有这些代码的 SIG。
var _ = ginkgo.ReportAfterSuite("[sig-testing] Log Check", func(ctx ginkgo.SpecContext, report ginkgo.Report) {
	if report.SuiteConfig.DryRun {
		return
	}

	// 这只在主进程中测试运行完成后执行。
	finalize()
})

// initialize 在测试开始运行前调用一次。
// 如果请求，它会设置对系统组件日志输出的监控。
func initialize() {
	if !enabledLogChecks.any() {
		return
	}

	config, err := framework.LoadConfig()
	framework.ExpectNoError(err, "loading client config")
	client, err := kubernetes.NewForConfig(config)
	framework.ExpectNoError(err, "creating client")

	ctx, l, err := newLogChecker(context.Background(), client, enabledLogChecks, framework.TestContext.ReportDir)
	framework.ExpectNoError(err, "set up log checker")

	lc = l
	lcLogger = klog.FromContext(ctx)
	lc.start(ctx, podlogs.CopyAllLogs, kubeletLogQuery)
}

// finalize 在测试运行后调用一次，且在与 initialize 相同的进程中调用。
func finalize() {
	if lc == nil {
		return
	}

	failure, stdout := lc.stop(lcLogger)
	_, _ = ginkgo.GinkgoWriter.Write([]byte(stdout))
	if failure != "" {
		// 作为套件后失败报告。
		ginkgo.Fail(failure)
	}
}

func newLogChecker(ctx context.Context, client kubernetes.Interface, check logCheck, logDir string) (context.Context, *logChecker, error) {
	var logFile string

	if logDir != "" {
		// 重定向日志输出。同时检查停止时的错误，这些错误在 CI job 中不能被忽略。
		logFile = path.Join(logDir, "system-logs.log")
		output, err := os.Create(logFile)
		if err != nil {
			return ctx, nil, fmt.Errorf("create log file: %w", err)
		}
		// 允许通过命令行提高详细程度，但写入文件时始终至少使用
		// 4 —— 我们承受得起。
		vflag := flag.CommandLine.Lookup("v")
		v, _ := strconv.Atoi(vflag.Value.String())
		if v < 5 {
			v = 5
		}

		logger := textlogger.NewLogger(textlogger.NewConfig(textlogger.Output(output), textlogger.Verbosity((int(v)))))
		ctx = klog.NewContext(ctx, logger)
	}
	if check.namespaces.re == nil {
		check.namespaces.re = regexp.MustCompile(defaultNamespacesRE)
	}
	if check.nodes.re == nil {
		check.nodes.re = regexp.MustCompile(defaultNodesRE)
	}

	ctx, cancel := context.WithCancelCause(ctx)
	return ctx, &logChecker{
		wg:        newWaitGroup(),
		client:    client,
		cancel:    cancel,
		check:     check,
		logDir:    logDir,
		logFile:   logFile,
		dataRaces: make(map[string][][]string),
	}, nil
}

type logChecker struct {
	client  kubernetes.Interface
	cancel  func(err error)
	check   logCheck
	logDir  string
	logFile string

	// wg 跟踪后台活动。
	wg *waitGroup

	// mutex 保护以下所有字段。
	mutex sync.Mutex

	// 到目前为止检测到的所有数据竞争，以 "<namespace>/<pod name>/<container name>"
	// 或 "kubelet/<node name>" 为索引。
	//
	// 最后一个条目在等待下一个数据竞争时初始为空。
	//
	// 只有至少收到部分输出的实体才会加入这里，
	// 因此报告中的 "<entity>: okay" 部分也只针对这些实体出现。
	dataRaces map[string][][]string
}

// stop 取消 Pod 监控，等待其关闭，然后生成失败消息文本（理想情况下为空）和 stdout。
func (l *logChecker) stop(logger klog.Logger) (failure, stdout string) {
	logger.V(4).Info("Asking log monitors to stop")
	l.cancel(errors.New("asked to stop"))

	// 等待完成。
	l.wg.wait()

	// 现在可以继续生成报告。
	l.mutex.Lock()
	defer l.mutex.Unlock()

	var failureBuffer strings.Builder
	var stdoutBuffer strings.Builder
	if l.logFile != "" {
		logData, err := os.ReadFile(l.logFile)
		if err != nil {
			stdoutBuffer.WriteString(fmt.Sprintf("Reading %s failed: %v", l.logFile, err))
		} else {
			stdoutBuffer.Write(logData)
		}

		// 找出所有错误日志行。有点粗糙（不处理多行输出），但足够用，
		// 因为有完整日志可用。
		errorLogs := regexp.MustCompile(`(?m)^E.*\n`).FindAllString(string(logData), -1)
		if len(errorLogs) > 0 {
			failureBuffer.WriteString("Unexpected errors during log data collection (see stdout for full log):\n\n")
			for _, errorLog := range errorLogs {
				// 缩进以使其成为 Markdown 中的逐字块。
				failureBuffer.WriteString("    ")
				failureBuffer.WriteString(errorLog)
			}
		}
	}

	keys := slices.AppendSeq([]string(nil), maps.Keys(l.dataRaces))
	slices.Sort(keys)
	for _, k := range keys {
		races := l.dataRaces[k]
		buffer := &failureBuffer
		if len(races) == 0 {
			buffer = &stdoutBuffer
		}
		if buffer.Len() > 0 {
			buffer.WriteString("\n")
		}
		buffer.WriteString("#### " + k + "\n")
		if len(races) == 0 {
			buffer.WriteString("\nOkay.\n")
			continue
		}
		for _, race := range races {
			indent := "    "
			buffer.WriteString("\n")
			if len(races) > 1 {
				// 格式化为项目符号列表。
				buffer.WriteString("- DATA RACE:\n  \n")
				// 这也会将文本块向右移动。
				indent += "  "
			} else {
				// 单行介绍文本，然后是文本块。
				buffer.WriteString("DATA RACE:\n\n")
			}
			// 缩进的行是栈回溯，可能很长，
			// 因此遍历行时收集它们，并在中间截断。
			var backtrace []string
			dumpBacktrace := func() {
				if len(backtrace) == 0 {
					return
				}

				if len(backtrace) > maxBacktraceLines {
					head := backtrace[0 : maxBacktraceLines/2]
					tail := backtrace[len(backtrace)-maxBacktraceLines/2:]

					backtrace = slices.Clone(head)
					backtrace = append(backtrace, "  ...\n")
					backtrace = append(backtrace, tail...)
				}

				for _, line := range backtrace {
					buffer.WriteString(indent)
					buffer.WriteString(line)
				}

				backtrace = nil
			}
			for _, line := range race {
				if !strings.HasPrefix(line, " ") {
					// 非回溯行 => 清空缓冲并写入该行。
					dumpBacktrace()
					buffer.WriteString(indent)
					buffer.WriteString(line)
					continue
				}
				backtrace = append(backtrace, line)
			}
			dumpBacktrace()
		}
	}

	return failureBuffer.String(), stdoutBuffer.String()
}

func (l *logChecker) start(ctx context.Context, startCopyAllLogs func(ctx context.Context, cs kubernetes.Interface, ns string, to podlogs.LogOutput) error, startNodeLog func(ctx context.Context, cs kubernetes.Interface, wg *waitGroup, nodeName string) io.Reader) {
	if !l.check.any() {
		return
	}

	logger := klog.FromContext(ctx)

	// 确定要监控哪些命名空间。
	namespaces, err := l.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		logger.Error(err, "Failed to list namespaces")
		return
	}

	// 节点同理。
	nodes, err := l.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		logger.Error(err, "Failed to list nodes")
		return
	}

	for _, namespace := range namespaces.Items {
		if !l.check.namespaces.re.MatchString(namespace.Name) {
			continue
		}
		to := podlogs.LogOutput{
			StatusWriter:  &statusWriter{logger: logger, namespace: &namespace},
			LogPathPrefix: path.Join(l.logDir, namespace.Name) + "/",
			LogOpen: func(podName, containerName string) io.Writer {
				return l.logOpen(logger, namespace.Name, podName, containerName)
			},
		}

		logger.Info("Watching", "namespace", klog.KObj(&namespace))
		if err := startCopyAllLogs(ctx, l.client, namespace.Name, to); err != nil {
			logger.Error(err, "Log output collection failed", "namespace", klog.KObj(&namespace))
		}
	}

	for _, node := range nodes.Items {
		if !l.check.nodes.re.MatchString(node.Name) {
			continue
		}

		logger.Info("Watching", "node", klog.KObj(&node))
		kubeletLog := startNodeLog(ctx, l.client, l.wg, node.Name)
		l.wg.goIfNotShuttingDown(nil, func() {
			scanner := bufio.NewScanner(kubeletLog)
			writer := logOutputChecker{
				logger: logger,
				k:      "kubelet/" + node.Name,
				l:      l,
			}
			for scanner.Scan() {
				// 需要去掉查询日志特性时日志存储可能添加的任何
				// 头信息。我们并不确切知道会是什么，
				// 因此使用匹配所有已知行头的正则。
				//
				// 未知行会原样传递，可以接受（它们会被当作
				// 来自 kubelet 的未知原始输出）。
				line := scanner.Text()
				line = logQueryLineHeaderRE.ReplaceAllString(line, "")
				_, _ = writer.Write([]byte(line + "\n"))
			}
			if err := scanner.Err(); err != nil {
				logger.Error(err, "Reading kubelet log failed", "node", klog.KObj(&node))
			}
		})
	}
}

// journaldHeader 匹配包含 kubelet 输出的 journald 行：
//
//	Jan 06 15:20:26.641748 kind-worker2 kubelet[311]: I0106 15:20:26.641743     311 labels.go:289] ...
//
// 还会收到 systemd 自身的消息。这些不会被匹配：
//
//	Jan 07 08:35:52.139136 kind-worker3 systemd[1]: Started kubelet.service - kubelet: The Kubernetes Node Agent.
const journaldHeader = `... .. ..:..:......... \S+ kubelet\[\d+\]: `

// logQueryLineHeaderRE 合并所有支持的 log query 数据格式。
// 目前仅支持 journald。
var logQueryLineHeaderRE = regexp.MustCompile(`^(?:` + journaldHeader + `)`)

func (l *logChecker) logOpen(logger klog.Logger, names ...string) io.Writer {
	if !l.check.dataRaces {
		return nil
	}

	if !l.wg.add(1) {
		return io.Discard
	}

	k := strings.Join(names, "/")
	logger.Info("Starting to check for data races", "container", k)
	return &logOutputChecker{logger: logger, k: k, l: l}
}

type statusWriter struct {
	logger    klog.Logger
	namespace *v1.Namespace
}

// Write 接收描述监控 Pod 及其输出时遇到的问题的文本。
func (s *statusWriter) Write(data []byte) (int, error) {
	msg := string(data)
	msg = strings.TrimRight(msg, "\n")
	if strings.HasPrefix(msg, "WARNING:") {
		// 警告仅供参考，podlogs 能从问题中恢复（例如高负载下的 "connection refused"，
		// 或读取日志输出时 Pod 终止导致的 "pod not found"）。
		// 之后查看日志时无需将它们视为 logcheck 失败。
		s.logger.Info("PodLogs status", "namespace", klog.KObj(s.namespace), "msg", msg)
	} else {
		s.logger.Error(nil, "PodLogs status", "namespace", klog.KObj(s.namespace), "msg", msg)
	}
	return len(msg), nil
}

// logOutputChecker 接收某个组件的日志输出，并检测其中嵌入的 DATA RACE 报告。
type logOutputChecker struct {
	logger     klog.Logger
	k          string
	l          *logChecker
	inDataRace bool
}

var (
	_ io.Writer = &logOutputChecker{}
	_ io.Closer = &logOutputChecker{}
)

// 每收到容器或 kubelet 实例的一行输出，都会调用 Write。
// 该行以换行符结尾。
func (p *logOutputChecker) Write(l []byte) (int, error) {
	line := string(l)

	p.l.mutex.Lock()
	defer p.l.mutex.Unlock()

	races := p.l.dataRaces[p.k]
	switch {
	case p.inDataRace && line != dataRaceEnd:
		races[len(races)-1] = append(races[len(races)-1], line)
	case p.inDataRace && line == dataRaceEnd:
		// 停止收集数据竞争行。
		p.inDataRace = false
		p.logger.Info("Completed data race", "container", p.k, "count", len(races), "dataRace", strings.Join(races[len(races)-1], ""))
	case !p.inDataRace && line == dataRaceStart:
		// 开始收集数据竞争行。
		p.inDataRace = true
		races = append(races, nil)
		p.logger.Info("Started new data race", "container", p.k, "count", len(races))
	default:
		// 其他日志输出。
	}
	p.l.dataRaces[p.k] = races

	return len(l), nil
}

// 所有输出处理完毕后调用 Close。
func (p *logOutputChecker) Close() error {
	p.logger.Info("Done checking for data races", "container", p.k)
	p.l.wg.done()
	return nil
}

// kubeletLogQuery 设置通过 log query 特性反复查询给定节点上
// 通过 log query 特性查询。
//
// 当 context 取消时，所有后台 goroutine 停止，并在 kubeletLogQuery 返回前
// 加入给定的 WaitGroup。届时 reader 将
// 停止提供数据。
//
// 写入 reader 的数据中的行可能包含 journald 头或其他
// 平台特定的头（log query 特性未对此作出规定）。
func kubeletLogQuery(ctx context.Context, cs kubernetes.Interface, wg *waitGroup, nodeName string) io.Reader {
	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithName(logger, "KubeletLogQuery")
	logger = klog.LoggerWithValues(logger, "node", klog.KRef("", nodeName))
	reader, writer := io.Pipe()

	wg.goIfNotShuttingDown(func() {
		_ = writer.Close()
	}, func() {
		logger.V(4).Info("Started")
		defer func() {
			logger.V(4).Info("Stopped", "reason", context.Cause(ctx))
		}()

		// 查询间隔是"太长"（数据过多）与
		// "太短"（频繁查询导致开销过大）之间的折中。目前尚不清楚
		// 最佳点在哪里。使用当前默认值时，在一次约 1 小时的
		// pull-kubernetes-e2e-kind-alpha-beta-features 运行中，一个 worker 节点有 12 次调用，
		// 平均结果大小约 16MB。
		//
		// 所有日志检查活动共享同一个 client 实例。它专门为
		// 此目的创建，因此客户端限流不会影响其他测试。
		ticker := time.NewTicker(300 * time.Second)
		defer ticker.Stop()

		var since time.Time
		for {
			select {
			case <-ctx.Done():
				logger.V(4).Info("Asked to stop, will query log one last time", "reason", context.Cause(ctx))
			case <-ticker.C:
				logger.V(6).Info("Starting periodic log query")
			}

			// 收到取消停止的请求时也查询一次，以获取输出的尾部。
			// 这里使用超时防止永久阻塞。
			err := func() error {
				ctx := context.WithoutCancel(ctx)
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				now := time.Now()
				untilTime := now.Format(time.RFC3339)
				sinceTime := ""
				if !since.IsZero() {
					sinceTime = since.Format(time.RFC3339)
				}
				// 下一次查询将使用当前的结束时间作为开始时间。开始和结束
				// 都是闭区间，因此很遗憾有风险重复获取同一时间戳的
				// 日志输出。没有好的办法避免这一点。
				// 希望这很少发生且无关紧要。遗漏
				// 输出更糟，因为那样可能错过 DATA RACE 报告的
				// 结束标记。
				since = now
				req := cs.CoreV1().RESTClient().Get().
					Resource("nodes").
					Name(nodeName).
					SubResource("proxy").
					Suffix("logs").
					Param("query", "kubelet").
					Param("untilTime", untilTime)
				if sinceTime != "" {
					req = req.Param("sinceTime", sinceTime)
				}
				data, err := req.DoRaw(ctx)
				if loggerV := logger.V(4); loggerV.Enabled() {
					head := string(data)
					if len(head) > 30 {
						head = head[:30]
						head += "..."
					}
					loggerV.Info("Queried log", "sinceTime", sinceTime, "endTime", now, "len", len(data), "data", head, "err", err)
				}

				// 先处理现有的数据。查询结果的确切格式有点
				// 未明确定义。这是基于 Kubernetes 1.35 中观察到的行为。

				// 特性禁用时 kubelet 似乎返回 HTML？！
				// 根据 Slack 讨论，这可能是也可能不是有意为之，
				// 将在 https://github.com/kubernetes/kubernetes/issues/136275 中澄清。
				if strings.HasPrefix(string(data), "<!doctype html>") {
					return fmt.Errorf("unexpected result of log query (feature disabled?): %q", string(data))
				}

				// 跳过用于表示"无可用数据"的特殊字符串。
				if string(data) != "-- No entries --\n" {
					_, _ = writer.Write(data)
				}
				return err
			}()

			if err != nil {
				logger.Error(err, "Log query failed")
				return
			}

			if ctx.Err() != nil {
				return
			}
		}
	})
	return reader
}
