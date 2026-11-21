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

package kubelet

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"regexp/syntax"
	"runtime"
	"strconv"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	// dateLayout 是 Windows 事件日志查询使用的时间格式。
	dateLayout = "2006-1-2 15:4:5"
	// maxTailLines 限制单次查询返回的最大尾部行数，防止响应过大。
	maxTailLines = 100000
	// maxServiceLength 是服务名的最大长度（各受支持操作系统中的上限）。
	maxServiceLength = 256
	// maxServices 限制单次查询的服务数量，避免一次请求聚合过多日志源。
	maxServices = 4
	// nodeLogDir 是节点日志文件所在的根目录。
	nodeLogDir = "/var/log/"
)

var (
	journal = journalServer{}
	// 允许传给 journalctl / Get-WinEvent 参数的安全字符集合。
	// 只有确认无法用于构造非法序列的字符才能加入该列表；
	// 这是对可能引发逃逸的畸形输入的一层广泛防御。
	reServiceNameUnsafeCharacters = regexp.MustCompile(`[^a-zA-Z\-_.:0-9@]+`)
)

// journalServer 返回操作系统特定服务日志的文本输出，供客户端查看。
// 它以调用进程（kubelet）的权限运行，因此只应允许 root 用户调用。
type journalServer struct{}

// ServeHTTP 将 HTTP 查询参数转换为当前系统上 journalctl 的参数并执行，
// 支持 gzip 内容编码以减小响应体积。
func (journalServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodPost {
		// 日志查询仅支持 GET 与 POST 方法。
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var out io.Writer = w

	// 先解析并校验查询参数，非法请求直接返回 400。
	nlq, errs := newNodeLogQuery(req.URL.Query())
	if len(errs) > 0 {
		http.Error(w, errs.ToAggregate().Error(), http.StatusBadRequest)
		return
	}

	// TODO: 还应设置一个响应头说明查询最终被解析到哪种日志源，
	// 例如 "kube-log-source: journal://foobar?arg1=value" 或
	// "kube-log-source: file:///var/log/foobar.log"。
	w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
	if req.Header.Get("Accept-Encoding") == "gzip" {
		w.Header().Set("Content-Encoding", "gzip")

		// 日志通常是大段文本，使用最高压缩速度在体积与 CPU 开销之间取得平衡。
		gz, err := gzip.NewWriterLevel(out, gzip.BestSpeed)
		if err != nil {
			fmt.Fprintf(w, "\nfailed to get gzip writer: %v\n", err)
			return
		}
		defer gz.Close()
		out = gz
	}
	nlq.Copy(req.Context(), out)
}

// nodeLogQuery 封装一次节点日志查询请求。
type nodeLogQuery struct {
	// Services 是待查询的服务列表。
	Services []string
	// Files 是待查询的文件列表。
	Files []string
	options
}

// options 封装服务日志查询的可选参数。
type options struct {
	// SinceTime 是只显示该 RFC3339 时间点之后日志的起点。
	SinceTime *time.Time
	// UntilTime 是只显示该 RFC3339 时间点之前日志的终点。
	UntilTime *time.Time
	// TailLines 用于从日志末尾取指定行数（不超过 10 万行）。
	// 是否支持取决于具体实现，目前仅对服务日志生效。
	TailLines *int
	// Boot 用于显示特定启动轮次的日志，允许的取值小于 1。
	// 传入非法的启动偏移量会导致日志获取失败并返回错误。是否支持取决于具体实现。
	Boot *int
	// Pattern 使用正则表达式过滤日志条目。在 Linux 节点上按 PCRE2 正则解析，
	// 在 Windows 节点上按 PowerShell 正则解析。是否支持取决于具体实现。
	Pattern string
}

// newNodeLogQuery 解析查询参数，把已知的选项转换为 nodeLogQuery。
func newNodeLogQuery(query url.Values) (*nodeLogQuery, field.ErrorList) {
	allErrs := field.ErrorList{}
	var nlq nodeLogQuery
	var err error

	queries, ok := query["query"]
	if len(queries) > 0 {
		for _, q := range queries {
			// 查询值中出现 / 或 \ 时视为日志文件路径；即便像 foo.log 这样不带斜杠，
			// 后续启发式逻辑仍会尝试按文件解析并返回文件内容。
			if strings.ContainsAny(q, `/\`) {
				nlq.Files = append(nlq.Files, q)
			} else if strings.TrimSpace(q) != "" { // 过滤仅含空白的查询，避免无意义的查询。
				nlq.Services = append(nlq.Services, q)
			}
		}
	}

	// 禁止空查询或仅含空白的查询。
	// 例如：kubectl get --raw /api/v1/nodes/$node/proxy/logs?query="   "
	if ok && (len(nlq.Files) == 0 && len(nlq.Services) == 0) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("query"), queries, "may not be empty"))
	}

	var sinceTime time.Time
	sinceTimeValue := query.Get("sinceTime")
	if len(sinceTimeValue) > 0 {
		sinceTime, err = time.Parse(time.RFC3339, sinceTimeValue)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("sinceTime"), sinceTimeValue, "invalid time format"))
		} else {
			nlq.SinceTime = &sinceTime
		}
	}

	var untilTime time.Time
	untilTimeValue := query.Get("untilTime")
	if len(untilTimeValue) > 0 {
		untilTime, err = time.Parse(time.RFC3339, untilTimeValue)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("untilTime"), untilTimeValue, "invalid time format"))
		} else {
			nlq.UntilTime = &untilTime
		}
	}

	var boot int
	bootValue := query.Get("boot")
	if len(bootValue) > 0 {
		boot, err = strconv.Atoi(bootValue)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("boot"), bootValue, err.Error()))
		} else {
			nlq.Boot = &boot
		}
	}

	var tailLines int
	tailLinesValue := query.Get("tailLines")
	if len(tailLinesValue) > 0 {
		tailLines, err = strconv.Atoi(tailLinesValue)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("tailLines"), tailLinesValue, err.Error()))
		} else {
			nlq.TailLines = &tailLines
		}
	}

	pattern := query.Get("pattern")
	if len(pattern) > 0 {
		nlq.Pattern = pattern
	}

	if len(allErrs) > 0 {
		return nil, allErrs
	}

	// 没有任何有效查询条件时返回空查询，交由上层决定如何处理。
	if reflect.DeepEqual(nlq, nodeLogQuery{}) {
		return nil, allErrs
	}

	return &nlq, allErrs
}

// validateServices 校验服务名是否安全以及服务数量是否超限。
func validateServices(services []string) field.ErrorList {
	allErrs := field.ErrorList{}

	for _, s := range services {
		if err := safeServiceName(s); err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("query"), s, err.Error()))
		}
	}

	if len(services) > maxServices {
		allErrs = append(allErrs, field.TooMany(field.NewPath("query"), len(services), maxServices))
	}
	return allErrs
}

// validate 对查询做完整校验：文件与服务互斥、时间区间有效、启动轮次与行数范围、
// 以及正则表达式语法合法。
func (n *nodeLogQuery) validate() field.ErrorList {
	allErrs := validateServices(n.Services)
	switch {
	case len(n.Files) == 0 && len(n.Services) == 0:
		allErrs = append(allErrs, field.Required(field.NewPath("query"), "cannot be empty with options"))
	case len(n.Files) > 0 && len(n.Services) > 0:
		allErrs = append(allErrs, field.Invalid(field.NewPath("query"), fmt.Sprintf("%v, %v", n.Files, n.Services),
			"cannot specify a file and service"))
	case len(n.Files) > 1:
		allErrs = append(allErrs, field.Invalid(field.NewPath("query"), n.Files, "cannot specify more than one file"))
	case len(n.Files) == 1 && n.options != (options{}):
		allErrs = append(allErrs, field.Invalid(field.NewPath("query"), n.Files, "cannot specify file with options"))
	case len(n.Files) == 1:
		// 通过 os.OpenRoot 将文件访问限制在 /var/log 下，防止路径逃逸。
		if root, err := os.OpenRoot(nodeLogDir); err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("query"), n.Files, err.Error()))
		} else {
			// root.Close() 不会返回错误。
			defer func() { _ = root.Close() }()
			if _, err := root.Stat(n.Files[0]); err != nil {
				allErrs = append(allErrs, field.Invalid(field.NewPath("query"), n.Files, err.Error()))
			}
		}
	}

	if n.SinceTime != nil && n.UntilTime != nil && (n.SinceTime.After(*n.UntilTime)) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("untilTime"), n.UntilTime, "must be after `sinceTime`"))
	}

	// boot 偏移量仅对 journald 有意义，Windows 事件日志不支持。
	if n.Boot != nil && runtime.GOOS == "windows" {
		allErrs = append(allErrs, field.Invalid(field.NewPath("boot"), *n.Boot, "not supported on Windows"))
	}

	if n.Boot != nil && *n.Boot > 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("boot"), *n.Boot, "must be less than 1"))
	}

	if n.TailLines != nil {
		if err := utilvalidation.IsInRange((int)(*n.TailLines), 0, maxTailLines); err != nil {
			allErrs = append(allErrs, field.Invalid(field.NewPath("tailLines"), *n.TailLines, err[0]))
		}
	}

	// 在服务端预先解析正则，尽早拒绝非法表达式，避免把错误留到命令执行阶段。
	if _, err := syntax.Parse(n.Pattern, syntax.Perl); err != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("pattern"), n.Pattern, err.Error()))
	}

	return allErrs
}

// Copy 执行操作系统特定的日志命令，并把输出流式写入 w；
// 出错时会在输出中写入一行错误信息。
func (n *nodeLogQuery) Copy(ctx context.Context, w io.Writer) {
	// 设置 30 秒截止时间，避免日志查询长时间占用请求处理资源。
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(30*time.Second))
	defer cancel()
	boot := 0
	if n.Boot != nil {
		boot = *n.Boot
	}
	n.copyForBoot(ctx, w, boot)
}

// copyForBoot 使用给定参数调用操作系统特定的日志命令。
func (n *nodeLogQuery) copyForBoot(ctx context.Context, w io.Writer, previousBoot int) {
	if ctx.Err() != nil {
		return
	}
	nativeLoggers, fileLoggers := n.splitNativeVsFileLoggers(ctx)
	if len(nativeLoggers) > 0 {
		n.copyServiceLogs(ctx, w, nativeLoggers, previousBoot)
	}

	// 文件日志不支持任何查询选项，明确提示用户去掉选项重试。
	if len(fileLoggers) > 0 && n.options != (options{}) {
		fmt.Fprintf(w, "\noptions present and query resolved to log files for %v\ntry without specifying options\n",
			fileLoggers)
		return
	}

	if len(fileLoggers) > 0 {
		copyFileLogs(ctx, w, fileLoggers)
	}
}

// splitNativeVsFileLoggers 判断每个服务是写入操作系统原生日志还是文件，
// 并分别返回原生日志服务列表与可能写文件的服务列表。
func (n *nodeLogQuery) splitNativeVsFileLoggers(ctx context.Context) ([]string, []string) {
	var nativeLoggers []string
	var fileLoggers []string

	for _, service := range n.Services {
		// 通过检查 journalctl 输出判断服务是否使用 journald。
		// Get-WinEvent 场景无需此判断：服务未写入 Application provider 时命令会直接报错。
		if checkForNativeLogger(ctx, service) {
			nativeLoggers = append(nativeLoggers, service)
		} else {
			fileLoggers = append(fileLoggers, service)
		}
	}
	return nativeLoggers, fileLoggers
}

// copyServiceLogs 使用给定参数调用 journalctl 或 Get-WinEvent。
// 注意这里显式传入服务列表，以配合前面的启发式判定。
func (n *nodeLogQuery) copyServiceLogs(ctx context.Context, w io.Writer, services []string, previousBoot int) {
	cmdStr, args, cmdEnv, err := getLoggingCmd(n, services)
	if err != nil {
		fmt.Fprintf(w, "\nfailed to get logging cmd: %v\n", err)
		return
	}
	// 标准输出与标准错误都写入同一个 writer，保证错误信息也能返回给客户端。
	cmd := exec.CommandContext(ctx, cmdStr, args...)
	cmd.Stdout = w
	cmd.Stderr = w
	cmd.Env = append(os.Environ(), cmdEnv...)

	if err := cmd.Run(); err != nil {
		// 命令正常退出码非零时输出已写入 w，无需额外提示。
		if _, ok := err.(*exec.ExitError); ok {
			return
		}
		// 仅在查询当前启动轮次时提示 journal 输出不可用，避免历史轮次查询刷屏。
		if previousBoot == 0 {
			fmt.Fprintf(w, "\nerror: journal output not available\n")
		}
	}
}

// copyFileLogs 遍历所有服务，尝试收集每个服务的文件日志。
func copyFileLogs(ctx context.Context, w io.Writer, services []string) {
	if ctx.Err() != nil {
		fmt.Fprintf(w, "\ncontext error: %v\n", ctx.Err())
		return
	}

	for _, service := range services {
		heuristicsCopyFileLogs(ctx, w, nodeLogDir, service)
	}
}

// heuristicsCopyFileLogs 依次尝试以下路径收集日志，命中即停止：
// /var/log/service
// /var/log/service.log
// /var/log/service/service.log
func heuristicsCopyFileLogs(ctx context.Context, w io.Writer, logDir, service string) {
	logFileNames := [3]string{
		service,
		fmt.Sprintf("%s.log", service),
		fmt.Sprintf("%s/%s.log", service, service),
	}

	var err error
	for _, logFileName := range logFileNames {
		err = heuristicsCopyFileLog(ctx, w, logDir, logFileName)
		if err == nil {
			break
		} else if errors.Is(err, os.ErrNotExist) {
			// 文件不存在时继续尝试下一个候选路径。
			continue
		} else {
			// 其他错误（如权限问题）重试也无济于事，直接停止。
			break
		}
	}

	if err != nil {
		// 若最后一个错误是文件不存在，说明所有候选路径都没有该服务的日志。
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintf(w, "\nlog not found for %s\n", service)
			return
		}
		fmt.Fprintf(w, "\nerror getting log for %s: %v\n", service, err)
	}
}

// readerCtx 将 io.Reader 与上下文绑定，使读取操作可以响应取消。
type readerCtx struct {
	ctx context.Context
	io.Reader
}

// Read 在每次读取前检查上下文是否已取消，已取消则返回对应错误。
func (r *readerCtx) Read(p []byte) (n int, err error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

// newReaderCtx 返回一个能感知上下文取消的 io.Reader。
func newReaderCtx(ctx context.Context, r io.Reader) io.Reader {
	return &readerCtx{
		ctx:    ctx,
		Reader: r,
	}
}

// heuristicsCopyFileLog 将指定日志文件的内容复制到 w。
func heuristicsCopyFileLog(ctx context.Context, w io.Writer, logDir, logFileName string) error {
	f, err := os.OpenInRoot(logDir, logFileName)
	if err != nil {
		return err
	}
	// 只读打开的文件关闭出错不会造成数据丢失，可以安全忽略。
	defer func() { _ = f.Close() }()
	fInfo, err := f.Stat()
	if err != nil {
		return err
	}
	// 目录不是日志文件：服务 foo 的日志可能位于 /var/log/foo/ 目录下，
	// 这种情况交由下一候选路径处理。
	if fInfo.IsDir() {
		return os.ErrNotExist
	}

	if _, err := io.Copy(w, newReaderCtx(ctx, f)); err != nil {
		return err
	}
	return nil
}

// safeServiceName 校验服务名长度与字符集，防止把恶意输入传给外部命令。
func safeServiceName(s string) error {
	// 各受支持操作系统中服务名的最大长度为 256。
	if len(s) > maxServiceLength {
		return fmt.Errorf("length must be less than %d", maxServiceLength)
	}

	if reServiceNameUnsafeCharacters.MatchString(s) {
		return fmt.Errorf("input contains unsupported characters")
	}
	return nil
}
