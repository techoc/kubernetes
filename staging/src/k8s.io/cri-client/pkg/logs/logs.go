/*
Copyright 2017 The Kubernetes Authors.

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

package logs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"

	remote "k8s.io/cri-client/pkg"
)

// Notice that the current CRI logs implementation doesn't handle
// log rotation.
// * It will not retrieve logs in rotated log file.
// * If log rotation happens when following the log:
//   * If the rotation is using create mode, we'll still follow the old file.
//   * If the rotation is using copytruncate, we'll be reading at the original position and get nothing.
// 注意：当前实现不处理日志轮转——不会读取已轮转出去的文件；follow 期间发生轮转时，
// create 模式会继续跟随旧文件，copytruncate 模式则从原偏移读取，因而读不到任何内容。

const (
	// RFC3339NanoFixed is the fixed width version of time.RFC3339Nano.
	// RFC3339NanoFixed 是 time.RFC3339Nano 的定宽版本：纳秒固定 9 位，适合生成长度一致的时间戳。
	RFC3339NanoFixed = "2006-01-02T15:04:05.000000000Z07:00"
	// RFC3339NanoLenient is the variable width RFC3339 time format for lenient parsing of strings into timestamps.
	// RFC3339NanoLenient 是纳秒位数可变的宽松 RFC3339 格式，用于兼容不同运行时产出的时间戳。
	RFC3339NanoLenient = "2006-01-02T15:04:05.999999999Z07:00"

	// timeFormatOut is the format for writing timestamps to output.
	// timeFormatOut 是输出时间戳时使用的格式（定宽）。
	timeFormatOut = RFC3339NanoFixed
	// timeFormatIn is the format for parsing timestamps from other logs.
	// timeFormatIn 是解析日志时间戳时使用的格式（宽松）。
	timeFormatIn = RFC3339NanoLenient

	// logForceCheckPeriod is the period to check for a new read
	// logForceCheckPeriod 是 follow 模式下未收到文件系统事件时强制重读的兜底周期。
	logForceCheckPeriod = 1 * time.Second
)

var (
	// eol is the end-of-line sign in the log.
	// eol 是日志的行尾分隔符（换行符）。
	eol = []byte{'\n'}
	// delimiter is the delimiter for timestamp and stream type in log line.
	// delimiter 是日志行中时间戳、流类型与日志标签之间的分隔符（空格）。
	delimiter = []byte{' '}
	// tagDelimiter is the delimiter for log tags.
	// tagDelimiter 是多个日志标签之间的分隔符。
	tagDelimiter = []byte(runtimeapi.LogTagDelimiter)
)

// logMessage is the CRI internal log type.
// logMessage 是日志解析后的内部统一表示，屏蔽 CRI 与 Docker JSON 两种格式的差异。
type logMessage struct {
	// timestamp 为该条日志的时间戳；stream 为来源流（stdout/stderr）；log 为日志正文（可能含结尾换行符）。
	timestamp time.Time
	stream    runtimeapi.LogStreamType
	log       []byte
}

// reset resets the log to nil.
// reset 将对象恢复为零值，便于复用同一实例解析下一行，避免重复分配。
func (l *logMessage) reset() {
	l.timestamp = time.Time{}
	l.stream = ""
	l.log = nil
}

// LogOptions is the CRI internal type of all log options.
// LogOptions 汇总读取容器日志的全部选项，各字段与 v1.PodLogOptions 中的选项一一对应。
type LogOptions struct {
	// TailLines is the number of lines to show from the end of the logs.
	// If it is nil, it means all log lines will be shown.
	// If it is 0, no log lines will be shown.
	// This corresponds to the TailLines field in v1.PodLogOptions.
	// TailLines 表示只显示末尾 N 行；nil 显示全部，0 不显示任何行。
	TailLines *int64
	// LimitBytes is the maximum number of bytes to read from the logs.
	// If it is nil, it means all bytes will be read.
	// If it is 0, no bytes will be read.
	// This corresponds to the LimitBytes field in v1.PodLogOptions.
	// LimitBytes 限制最多读取的字节数；nil 表示不限，0 表示不读取任何字节。
	LimitBytes *int64
	// Since is the absolute time from which to show logs.
	// Only logs with a timestamp after this time will be returned.
	// This can be derived from either SinceSeconds or SinceTime in v1.PodLogOptions.
	// Since 是绝对起始时间，只返回时间戳晚于该时刻的日志（可由 SinceSeconds/SinceTime 换算得到）。
	Since time.Time
	// Follow indicates whether the logs should be followed.
	// If true, the read will continue until the container is stopped or the context is cancelled.
	// This corresponds to the Follow field in v1.PodLogOptions.
	// Follow 表示是否持续跟随日志，直到容器停止或 context 被取消。
	Follow bool
	// Timestamp indicates whether to include timestamps in the log output.
	// If true, each log line will be prefixed with its RFC3339 timestamp.
	// This corresponds to the Timestamps field in v1.PodLogOptions.
	// Timestamp 表示是否在每行日志前加上 RFC3339 时间戳前缀。
	Timestamp bool
}

// parseFunc is a function parsing one log line to the internal log type.
// Notice that the caller must make sure logMessage is not nil.
// parseFunc 将一行原始日志解析到 logMessage 中；调用方必须保证传入的 msg 非 nil。
type parseFunc func([]byte, *logMessage) error

// parseFuncs 是支持的日志格式解析器列表，按探测优先级排列。
var parseFuncs = []parseFunc{
	parseCRILog,        // CRI log format parse function
	parseDockerJSONLog, // Docker JSON log format parse function
}

// parseCRILog parses logs in CRI log format. CRI Log format example:
//
//	2016-10-06T00:17:09.669794202Z stdout P log content 1
//	2016-10-06T00:17:09.669794203Z stderr F log content 2
//
// parseCRILog 按 CRI 日志格式解析单行日志。
// CRI 行由空格分隔的四个字段组成：时间戳、流类型、日志标签、正文。
func parseCRILog(log []byte, msg *logMessage) error {
	var err error
	// Parse timestamp
	// 查找第一个空格，其前面的部分即为时间戳。
	idx := bytes.Index(log, delimiter)
	if idx < 0 {
		return fmt.Errorf("timestamp is not found")
	}
	// 用宽松格式解析，以兼容纳秒位数不固定的时间戳。
	msg.timestamp, err = time.Parse(timeFormatIn, string(log[:idx]))
	if err != nil {
		return fmt.Errorf("unexpected timestamp format %q: %v", timeFormatIn, err)
	}

	// Parse stream type
	// 跳过时间戳，继续查找下一个空格，中间内容是流类型。
	log = log[idx+1:]
	idx = bytes.Index(log, delimiter)
	if idx < 0 {
		return fmt.Errorf("stream type is not found")
	}
	msg.stream = runtimeapi.LogStreamType(log[:idx])
	// 只接受 stdout/stderr，其他值视为格式错误。
	if msg.stream != runtimeapi.Stdout && msg.stream != runtimeapi.Stderr {
		return fmt.Errorf("unexpected stream type %q", msg.stream)
	}

	// Parse log tag
	// 跳过流类型，继续查找下一个空格，中间内容是日志标签。
	log = log[idx+1:]
	idx = bytes.Index(log, delimiter)
	if idx < 0 {
		return fmt.Errorf("log tag is not found")
	}
	// Keep this forward compatible.
	// 标签可有多个，用分隔符拆开后只关心第一个（P/F），其余忽略以保持前向兼容。
	tags := bytes.Split(log[:idx], tagDelimiter)
	partial := runtimeapi.LogTag(tags[0]) == runtimeapi.LogTagPartial
	// Trim the tailing new line if this is a partial line.
	// P 表示这是被拆开的部分日志：去掉结尾换行符，使后续片段能接到同一行上。
	if partial && len(log) > 0 && log[len(log)-1] == '\n' {
		log = log[:len(log)-1]
	}

	// Get log content
	// 最后一个空格之后的内容是日志正文。
	msg.log = log[idx+1:]

	return nil
}

// jsonLog is a log message, typically a single entry from a given log stream.
// since the data structure is originally from docker, we should be careful to
// with any changes to jsonLog
// jsonLog 对应 Docker JSON 日志格式中的一条记录，结构源自 Docker，改动需谨慎以免破坏兼容性。
type jsonLog struct {
	// Log is the log message
	// Log 是日志正文。
	Log string `json:"log,omitempty"`
	// Stream is the log source
	// Stream 是日志来源流名称（stdout/stderr）。
	Stream string `json:"stream,omitempty"`
	// Created is the created timestamp of log
	// Created 是该条日志的时间戳。
	Created time.Time `json:"time"`
}

// parseDockerJSONLog parses logs in Docker JSON log format. Docker JSON log format
// example:
//
//	{"log":"content 1","stream":"stdout","time":"2016-10-20T18:39:20.57606443Z"}
//	{"log":"content 2","stream":"stderr","time":"2016-10-20T18:39:20.57606444Z"}
//
// parseDockerJSONLog 按 Docker JSON 日志格式解析单行日志，并映射到内部统一结构。
func parseDockerJSONLog(log []byte, msg *logMessage) error {
	var l = &jsonLog{}

	// TODO: JSON decoding is fairly expensive, we should evaluate this.
	// 先整行反序列化；JSON 解码开销较大，后续需要评估其性能影响。
	if err := json.Unmarshal(log, l); err != nil {
		return fmt.Errorf("failed with %v to unmarshal log %q", err, l)
	}
	// 把 Docker 的三个字段转换为 logMessage 的统一表示。
	msg.timestamp = l.Created
	msg.stream = runtimeapi.LogStreamType(l.Stream)
	msg.log = []byte(l.Log)
	return nil
}

// getParseFunc returns proper parse function based on the sample log line passed in.
// getParseFunc 依次尝试已注册的解析器，返回第一个能成功解析样例行的函数。
func getParseFunc(log []byte) (parseFunc, error) {
	for _, p := range parseFuncs {
		// 用空的 logMessage 试解析，完全成功才认定格式匹配。
		if err := p(log, &logMessage{}); err == nil {
			return p, nil
		}
	}
	return nil, fmt.Errorf("unsupported log format: %q", log)
}

// logWriter controls the writing into the stream based on the log options.
// logWriter 按选项把解析后的日志写入对应输出流，负责时间戳前缀与字节数上限。
type logWriter struct {
	// stdout/stderr 是两路输出目标（v1.32 起未请求的流可为 nil）；opts 是读取选项；remain 是剩余可写字节数。
	stdout io.Writer
	stderr io.Writer
	opts   *LogOptions
	remain int64
}

// errMaximumWrite is returned when all bytes have been written.
// errMaximumWrite 表示字节配额已用尽，调用方应停止输出。
var errMaximumWrite = errors.New("maximum write")

// errShortWrite is returned when the message is not fully written.
// errShortWrite 表示底层 Writer 没有写完整个内容。
var errShortWrite = errors.New("short write")

// newLogWriter 创建 logWriter；未设置 LimitBytes 时 remain 初始化为 math.MaxInt64，相当于不限制。
func newLogWriter(stdout io.Writer, stderr io.Writer, opts *LogOptions) *logWriter {
	w := &logWriter{
		stdout: stdout,
		stderr: stderr,
		opts:   opts,
		remain: math.MaxInt64, // initialize it as infinity
	}
	// 显式指定上限时以该值作为初始配额（0 表示输出 0 字节）。
	if opts.LimitBytes != nil {
		w.remain = *opts.LimitBytes
	}
	return w
}

// writeLogs writes logs into stdout, stderr.
// write 把一条日志写到对应流；addPrefix 表示是否添加时间戳前缀，多行日志只有首行为 true。
func (w *logWriter) write(msg *logMessage, addPrefix bool) error {
	// 早于 Since 的日志直接跳过，也不占用字节配额。
	if msg.timestamp.Before(w.opts.Since) {
		// Skip the line because it's older than since
		return nil
	}
	line := msg.log
	// 开启时间戳时，为多行日志的首行拼上定宽时间戳加一个空格。
	if w.opts.Timestamp && addPrefix {
		prefix := append([]byte(msg.timestamp.Format(timeFormatOut)), delimiter[0])
		line = append(prefix, line...)
	}
	// If the line is longer than the remaining bytes, cut it.
	// 超出剩余配额的部分直接截断，只输出还允许写入的字节数。
	if int64(len(line)) > w.remain {
		line = line[:w.remain]
	}
	// Get the proper stream to write to.
	// 根据流类型选择目标 writer，未知类型视为错误。
	var stream io.Writer
	switch msg.stream {
	case runtimeapi.Stdout:
		stream = w.stdout
	case runtimeapi.Stderr:
		stream = w.stderr
	default:
		return fmt.Errorf("unexpected stream type %q", msg.stream)
	}
	// Since v1.32, either w.stdout or w.stderr may be nil if the stream is not requested.
	// In such case, we should neither count the bytes nor write to the stream.
	// 未被请求的流为 nil 时既不写入也不扣减配额，避免 stderr 日志影响 stdout 的字节限制。
	if stream == nil {
		return nil
	}
	// 写入并按实际写入字节数扣减剩余配额。
	n, err := stream.Write(line)
	w.remain -= int64(n)
	if err != nil {
		return err
	}
	// If the line has not been fully written, return errShortWrite
	// 写入字节数不足时报告短写。
	if n < len(line) {
		return errShortWrite
	}
	// If there are no more bytes left, return errMaximumWrite
	// 配额耗尽时通知调用方已达到上限。
	if w.remain <= 0 {
		return errMaximumWrite
	}
	return nil
}

// ReadLogs read the container log and redirect into stdout and stderr.
// Note that containerID is only needed when following the log, or else
// just pass in empty string "".
// ReadLogs 读取容器日志文件，解析后按流类型分别写入 stdout/stderr。
// containerID 仅在 Follow 为 true（需判断容器是否运行）时用到，否则传空字符串即可。
func ReadLogs(ctx context.Context, path, containerID string, opts *LogOptions, runtimeService internalapi.RuntimeService, stdout, stderr io.Writer) error {
	// fsnotify has different behavior for symlinks in different platform,
	// for example it follows symlink on Linux, but not on Windows,
	// so we explicitly resolve symlinks before reading the logs.
	// There shouldn't be security issue because the container log
	// path is owned by kubelet and the container runtime.
	// 不同平台对符号链接的 fsnotify 行为不同（Linux 跟随、Windows 不跟随），故先显式解析。
	// 日志路径由 kubelet 与运行时所有，不存在安全风险。
	evaluated, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("failed to try resolving symlinks in path %q: %v", path, err)
	}
	path = evaluated
	// 打开文件；Windows 上使用允许删除/重命名的共享模式，以支持日志轮转。
	f, err := openFileShareDelete(path)
	if err != nil {
		return fmt.Errorf("failed to open log file %q: %v", path, err)
	}
	defer f.Close()

	// Search start point based on tail line.
	// 默认 -1 表示从文件开头读取；指定 TailLines 时计算倒数第 N 行的起点。
	var tail int64 = -1
	if opts.TailLines != nil {
		tail = *opts.TailLines
	}
	start, err := findTailLineStartIndex(f, tail)
	if err != nil {
		return fmt.Errorf("failed to tail %d lines of log file %q: %w", tail, path, err)
	}
	// 将文件指针移动到计算出的起始偏移。
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek %d in log file %q: %w", start, path, err)
	}

	// limitedMode 表示"非 follow 且指定 TailLines"：读完有限行即可退出。
	limitedMode := (opts.TailLines != nil) && (!opts.Follow)
	limitedNum := tail
	// Start parsing the logs.
	// 使用带缓冲的 reader 按行读取。
	r := bufio.NewReader(f)
	// Do not create watcher here because it is not needed if `Follow` is false.
	// fsnotify watcher 只在 follow 模式下按需创建，这里仅声明。
	var watcher *fsnotify.Watcher
	var parse parseFunc
	var stop bool
	isNewLine := true
	found := true
	writer := newLogWriter(stdout, stderr, opts)
	msg := &logMessage{}
	baseName := filepath.Base(path)
	dir := filepath.Dir(path)
	logger := klog.FromContext(ctx)

	for {
		// 读完有限行、容器停止或文件结束标志置位时正常收尾。
		if stop || (limitedMode && limitedNum == 0) {
			logger.V(2).Info("Finished parsing log file", "path", path)
			return nil
		}
		// 按换行符读取一行；最后一行可能没有换行符，此时 reader 返回 io.EOF。
		l, err := r.ReadBytes(eol[0])
		if err != nil {
			if err != io.EOF { // This is an real error
				return fmt.Errorf("failed to read log file %q: %v", path, err)
			}
			if opts.Follow {
				// The container is not running, we got to the end of the log.
				// found 为 false 表示容器已退出且无新内容，结束跟随。
				if !found {
					return nil
				}
				// Reset seek so that if this is an incomplete line,
				// it will be read again.
				// 回退指针，使不完整的行在下次被重新读取，避免丢半行日志。
				if _, err := f.Seek(-int64(len(l)), io.SeekCurrent); err != nil {
					return fmt.Errorf("failed to reset seek in log file %q: %v", path, err)
				}
				if watcher == nil {
					// Initialize the watcher if it has not been initialized yet.
					// 首次到达 EOF 时才创建 watcher，并监听日志所在目录（便于发现日志文件被替换）。
					if watcher, err = fsnotify.NewWatcher(); err != nil {
						return fmt.Errorf("failed to create fsnotify watcher: %v", err)
					}
					defer watcher.Close()
					if err := watcher.Add(dir); err != nil {
						return fmt.Errorf("failed to watch directory %q: %w", dir, err)
					}
					// If we just created the watcher, try again to read as we might have missed
					// the event.
					// 创建 watcher 与日志写入之间存在竞争，先重试读取一次以免漏掉事件。
					continue
				}
				var recreated bool
				// Wait until the next log change.
				// 等待日志变更：返回是否发现新内容、日志文件是否被重建以及错误。
				found, recreated, err = waitLogs(ctx, containerID, baseName, watcher, runtimeService)
				if err != nil {
					return err
				}
				if recreated {
					// 日志轮转后旧文件失效，重新打开同名文件并替换 reader。
					newF, err := openFileShareDelete(path)
					if err != nil {
						if os.IsNotExist(err) {
							continue
						}
						return fmt.Errorf("failed to open log file %q: %v", path, err)
					}
					defer newF.Close()
					f.Close()
					f = newF
					r = bufio.NewReader(f)
				}
				// If the container exited consume data until the next EOF
				// 容器可能刚好退出，继续读取直到下一个 EOF 再判断是否结束。
				continue
			}
			// Should stop after writing the remaining content.
			// 非 follow 模式下 EOF 表示已到文件末尾，写完最后一段内容后退出。
			stop = true
			if len(l) == 0 {
				continue
			}
			logger.Info("Incomplete line in log file", "path", path, "line", l)
		}
		if parse == nil {
			// Initialize the log parsing function.
			// 根据首行内容自动探测日志格式（CRI 或 Docker JSON），后续行复用该解析器。
			parse, err = getParseFunc(l)
			if err != nil {
				return fmt.Errorf("failed to get parse function: %v", err)
			}
		}
		// Parse the log line.
		// 复用同一个 msg 解析当前行；解析失败只记录日志并跳过该行。
		msg.reset()
		if err := parse(l, msg); err != nil {
			logger.Error(err, "Failed when parsing line in log file", "path", path, "line", l)
			continue
		}
		// Write the log line into the stream.
		// 写入对应流；达到字节上限时正常结束，其他写入错误直接向上返回。
		if err := writer.write(msg, isNewLine); err != nil {
			if err == errMaximumWrite {
				var bytes int64 = -1
				if opts.LimitBytes != nil {
					bytes = *opts.LimitBytes
				}
				logger.V(2).Info("Finished parsing log file, hit bytes limit", "path", path, "limit", bytes)
				return nil
			}
			logger.Error(err, "Failed when writing line to log file", "path", path, "line", msg)
			return err
		}
		// 有限行数模式下每成功输出一行就递减剩余行数。
		if limitedMode {
			limitedNum--
		}
		// 当前行是否以换行符结尾，决定下一行是否还需要时间戳前缀：
		// P（部分）日志的续行不重复时间戳。
		if len(msg.log) > 0 {
			isNewLine = msg.log[len(msg.log)-1] == eol[0]
		} else {
			isNewLine = true
		}
	}
}

// isContainerRunning 查询容器状态，判断 follow 期间是否应继续跟随日志。
func isContainerRunning(ctx context.Context, id string, r internalapi.RuntimeService) (bool, error) {
	resp, err := r.ContainerStatus(ctx, id, false)
	if err != nil {
		// Assume that the container is still running when the runtime is
		// unavailable. Most runtimes support that containers can be in running
		// state even if their CRI server is not available right now.
		// 运行时暂时不可用（Unavailable）时保守地认为容器仍在运行，避免误停跟随。
		if status.Code(err) == codes.Unavailable {
			return true, nil
		}

		return false, err
	}
	status := resp.GetStatus()
	if status == nil {
		// 响应中缺少状态信息属于异常情况，返回明确错误。
		return false, remote.ErrContainerStatusNil
	}
	logger := klog.FromContext(ctx)
	// Only keep following container log when it is running.
	// 只有 RUNNING 状态才继续跟随；等待期间容器停止是正常现象，不视为错误。
	if status.State != runtimeapi.ContainerState_CONTAINER_RUNNING {
		logger.V(5).Info("Container is not running", "containerId", id, "state", status.State)
		// Do not return error because it's normal that the container stops
		// during waiting.
		return false, nil
	}
	return true, nil
}

// waitLogs wait for the next log write. It returns two booleans and an error. The first boolean
// indicates whether a new log is found; the second boolean if the log file was recreated;
// the error is error happens during waiting new logs.
// waitLogs 等待下一次日志写入。返回值依次表示：是否发现新日志、日志文件是否被重建、等待期间发生的错误。
func waitLogs(ctx context.Context, id string, logName string, w *fsnotify.Watcher, runtimeService internalapi.RuntimeService) (bool, bool, error) {
	logger := klog.FromContext(ctx)
	// no need to wait if the pod is not running
	// 容器已停止时无需等待，直接返回。
	if running, err := isContainerRunning(ctx, id, runtimeService); !running {
		return false, false, err
	}
	// watcher 报错时允许的重试次数。
	errRetry := 5
	for {
		select {
		case <-ctx.Done():
			// context 被取消（如组件关闭）时终止等待。
			return false, false, fmt.Errorf("context cancelled")
		case e := <-w.Events:
			// 根据文件系统事件类型决定如何返回。
			switch e.Op {
			case fsnotify.Write, fsnotify.Rename, fsnotify.Remove, fsnotify.Chmod:
				// 写入、重命名、删除、权限变更都认为可能有新日志，且文件未被重建。
				return true, false, nil
			case fsnotify.Create:
				// 创建事件：若创建的是目标日志文件，说明日志被轮转后重新创建。
				return true, filepath.Base(e.Name) == logName, nil
			default:
				logger.Error(nil, "Received unexpected fsnotify event, retrying", "event", e)
			}
		case err := <-w.Errors:
			// watcher 出错时记录并有限次重试，重试耗尽后把错误返回。
			logger.Error(err, "Received fsnotify watch error, retrying unless no more retries left", "retries", errRetry)
			if errRetry == 0 {
				return false, false, err
			}
			errRetry--
		case <-time.After(logForceCheckPeriod):
			// 定时兜底：即使漏掉事件，也周期性触发一次重新读取。
			return true, false, nil
		}
	}
}
