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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	apitesting "k8s.io/cri-api/pkg/apis/testing"
	"k8s.io/utils/ptr"
)

// TestReadLogs 验证不同 LogOptions 组合下 ReadLogs 的输出。
// 输入：一个包含 3 行 Docker JSON 格式日志的临时文件（line1/line2/line3，时间依次递增 1 秒）。
// 断言：默认选项输出全部行；TailLines、LimitBytes、Since、Follow 及其组合按预期裁剪输出。
func TestReadLogs(t *testing.T) {
	// 创建临时日志文件，并在测试结束时关闭、删除。
	file, err := os.CreateTemp("", "TestFollowLogs")
	if err != nil {
		t.Fatalf("unable to create temp file")
	}
	// 注册清理函数，确保临时文件不残留。
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Logf("failed to close file: %v", err)
		}
		if err := os.Remove(file.Name()); err != nil {
			t.Logf("failed to remove file: %v", err)
		}
	})
	// 写入 3 行 stdout 日志，时间戳分别为 11:18:01、11:18:02、11:18:03。
	file.WriteString(`{"log":"line1\n","stream":"stdout","time":"2020-09-27T11:18:01.00000000Z"}` + "\n")
	file.WriteString(`{"log":"line2\n","stream":"stdout","time":"2020-09-27T11:18:02.00000000Z"}` + "\n")
	file.WriteString(`{"log":"line3\n","stream":"stdout","time":"2020-09-27T11:18:03.00000000Z"}` + "\n")

	// 表驱动用例：每行指定日志选项和期望的 stdout 输出。
	testCases := []struct {
		// name 为用例名，opts 为日志选项，expected 为期望的 stdout 内容。
		name     string
		opts     LogOptions
		expected string
	}{
		{
			name:     "default pod log options should output all lines",
			opts:     LogOptions{},
			expected: "line1\nline2\nline3\n",
		},
		{
			name: "using TailLines 2 should output last 2 lines",
			opts: LogOptions{
				TailLines: ptr.To[int64](2),
			},
			expected: "line2\nline3\n",
		},
		{
			name: "using TailLines 4 should output all lines when the log has less than 4 lines",
			opts: LogOptions{
				TailLines: ptr.To[int64](4),
			},
			expected: "line1\nline2\nline3\n",
		},
		{
			name: "using TailLines 0 should output nothing",
			opts: LogOptions{
				TailLines: ptr.To[int64](0),
			},
			expected: "",
		},
		{
			name: "using LimitBytes 9 should output first 9 bytes",
			opts: LogOptions{
				LimitBytes: ptr.To[int64](9),
			},
			expected: "line1\nlin",
		},
		{
			name: "using LimitBytes 100 should output all bytes when the log has less than 100 bytes",
			opts: LogOptions{
				LimitBytes: ptr.To[int64](100),
			},
			expected: "line1\nline2\nline3\n",
		},
		{
			name: "using LimitBytes 0 should output nothing",
			opts: LogOptions{
				LimitBytes: ptr.To[int64](0),
			},
			expected: "",
		},
		{
			name: "using SinceTime should output lines with a time on or after the specified time",
			opts: LogOptions{
				Since: time.Date(2020, time.Month(9), 27, 11, 18, 02, 0, time.UTC),
			},
			expected: "line2\nline3\n",
		},
		{
			name: "using SinceTime now should output nothing",
			opts: LogOptions{
				Since: time.Now(),
			},
			expected: "",
		},
		{
			name: "using follow should output all log lines",
			opts: LogOptions{
				Follow: true,
			},
			expected: "line1\nline2\nline3\n",
		},
		{
			name: "using follow combined with TailLines 2 should output the last 2 lines",
			opts: LogOptions{
				Follow:    true,
				TailLines: ptr.To[int64](2),
			},
			expected: "line2\nline3\n",
		},
		{
			name: "using follow combined with SinceTime should output lines with a time on or after the specified time",
			opts: LogOptions{
				Follow: true,
				Since:  time.Date(2020, time.Month(9), 27, 11, 18, 02, 0, time.UTC),
			},
			expected: "line2\nline3\n",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			containerID := "fake-container-id"
			// 构造 fake runtime，容器初始状态为 RUNNING。
			fakeRuntimeService := &apitesting.FakeRuntimeService{
				Containers: map[string]*apitesting.FakeContainer{
					containerID: {
						ContainerStatus: runtimeapi.ContainerStatus{
							State: runtimeapi.ContainerState_CONTAINER_RUNNING,
						},
					},
				},
			}
			// follow 用例必须把容器标记为 EXITED，否则 ReadLogs 会一直等待新日志而不退出。
			if tc.opts.Follow {
				fakeRuntimeService.Containers[containerID].State = runtimeapi.ContainerState_CONTAINER_EXITED
			}

			// 用内存缓冲区分别接收 stdout 和 stderr。
			stdoutBuf := bytes.NewBuffer(nil)
			stderrBuf := bytes.NewBuffer(nil)
			// 调用 ReadLogs 并断言：无错误、stderr 为空、stdout 与期望完全一致。
			err = ReadLogs(context.TODO(), file.Name(), containerID, &tc.opts, fakeRuntimeService, stdoutBuf, stderrBuf)

			if err != nil {
				t.Fatal(err.Error())
			}
			if stderrBuf.Len() > 0 {
				t.Fatalf("Stderr: %v", stderrBuf.String())
			}
			if actual := stdoutBuf.String(); tc.expected != actual {
				t.Fatalf("Actual output does not match expected.\nActual:  %v\nExpected: %v\n", actual, tc.expected)
			}
		})
	}
}

// TestReadRotatedLog 验证 follow 模式下日志文件被轮转（重命名旧文件 + 新建同名文件）后仍能继续读取。
// 输入：向临时文件交替写入 10 行日志（偶数为 stdout、奇数为 stderr），第 5 行后模拟一次轮转。
// 断言：轮转前后的日志都被完整收集，并按流类型正确分流到两个缓冲区。
func TestReadRotatedLog(t *testing.T) {
	// 在临时目录中创建日志文件，便于后续重命名模拟轮转。
	tmpDir := t.TempDir()
	file, err := os.CreateTemp(tmpDir, "logfile")
	if err != nil {
		assert.NoErrorf(t, err, "unable to create temp file")
	}
	// 两个缓冲区分别接收 stdout/stderr 日志。
	stdoutBuf := &bytes.Buffer{}
	stderrBuf := &bytes.Buffer{}
	containerID := "fake-container-id"
	// 构造 fake runtime，容器初始为 RUNNING，使 follow 模式正常工作。
	fakeRuntimeService := &apitesting.FakeRuntimeService{
		Containers: map[string]*apitesting.FakeContainer{
			containerID: {
				ContainerStatus: runtimeapi.ContainerStatus{
					State: runtimeapi.ContainerState_CONTAINER_RUNNING,
				},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 在独立 goroutine 中启动 follow 读取，主 goroutine 负责写文件与触发轮转。
	fileName := file.Name()
	go func(ctx context.Context) {
		opts := LogOptions{
			Follow: true,
		}
		_ = ReadLogs(ctx, fileName, containerID, &opts, fakeRuntimeService, stdoutBuf, stderrBuf)
	}(ctx)

	// 期望偶数行进 stdout、奇数行进 stderr。
	expectedStdout := "line0\nline2\nline4\nline6\nline8\n"
	expectedStderr := "line1\nline3\nline5\nline7\nline9\n"

	dir := filepath.Dir(file.Name())
	baseName := filepath.Base(file.Name())

	// 等待 50ms，让 ReadLogs 启动并进入等待日志变更的状态。
	time.Sleep(50 * time.Millisecond)

	for line := 0; line < 10; line++ {
		// 每行使用当前时间作时间戳，按奇偶写入不同流，行间稍作间隔。
		now := time.Now().Format(RFC3339NanoLenient)
		if line%2 == 0 {
			file.WriteString(fmt.Sprintf(
				`{"log":"line%d\n","stream":"stdout","time":"%s"}`+"\n", line, now))
		} else {
			file.WriteString(fmt.Sprintf(
				`{"log":"line%d\n","stream":"stderr","time":"%s"}`+"\n", line, now))
		}
		time.Sleep(1 * time.Millisecond)

		if line == 5 {
			// 关闭当前文件，模拟日志轮转。
			file.Close()
			// 先把旧文件重命名，再创建一个同名新文件继续写。
			rotatedName := fmt.Sprintf("%s.%s", baseName, time.Now().Format("220060102-150405"))
			rotatedName = filepath.Join(dir, rotatedName)
			if err := os.Rename(filepath.Join(dir, baseName), rotatedName); err != nil {
				assert.NoErrorf(t, err, "failed to rotate log %q to %q", file.Name(), rotatedName)
				return
			}

			newF := filepath.Join(dir, baseName)
			if file, err = os.Create(newF); err != nil {
				assert.NoError(t, err, "unable to create new log file")
				return
			}
			// 留时间让 watcher 感知新文件被创建。
			time.Sleep(20 * time.Millisecond)
		}
	}

	// 写完所有行后关闭文件，便于清理。
	err = file.Close()
	assert.NoErrorf(t, err, "could not close file.")

	time.Sleep(20 * time.Millisecond)
	// 将容器标记为 EXITED，使 follow 循环在读到 EOF 后退出。
	fakeRuntimeService.Lock()
	fakeRuntimeService.Containers[containerID].State = runtimeapi.ContainerState_CONTAINER_EXITED
	fakeRuntimeService.Unlock()

	// 断言收集到的 stdout/stderr 内容与预期一致。
	assert.Equal(t, expectedStdout, stdoutBuf.String())
	assert.Equal(t, expectedStderr, stderrBuf.String())
}

// TestParseLog 验证 CRI 与 Docker JSON 两种日志格式的自动探测与解析。
// 输入：覆盖 stdout/stderr、完整行/部分行（P）、多标签、不支持格式等单行日志样例。
// 断言：getParseFunc 选出正确解析器，解析出的时间戳/流类型/正文与期望一致；不支持的格式返回错误。
func TestParseLog(t *testing.T) {
	// 所有样例行共用的期望时间戳。
	timestamp, err := time.Parse(timeFormatIn, "2016-10-20T18:39:20.57606443Z")
	assert.NoError(t, err)
	// 复用同一个 msg，验证解析器会先重置再填充。
	msg := &logMessage{}
	// 表驱动用例：line 为输入行，msg 为期望解析结果，err 表示是否预期解析失败。
	for c, test := range []struct {
		// line 是原始日志行，msg 是期望的解析结果，err 表示预期是否报错。
		line string
		msg  *logMessage
		err  bool
	}{
		// Docker JSON 格式的 stdout 日志。
		{ // Docker 日志格式 stdout
			line: `{"log":"docker stdout test log","stream":"stdout","time":"2016-10-20T18:39:20.57606443Z"}` + "\n",
			msg: &logMessage{
				timestamp: timestamp,
				stream:    runtimeapi.Stdout,
				log:       []byte("docker stdout test log"),
			},
		},
		// Docker JSON 格式的 stderr 日志。
		{ // Docker 日志格式 stderr
			line: `{"log":"docker stderr test log","stream":"stderr","time":"2016-10-20T18:39:20.57606443Z"}` + "\n",
			msg: &logMessage{
				timestamp: timestamp,
				stream:    runtimeapi.Stderr,
				log:       []byte("docker stderr test log"),
			},
		},
		// CRI 格式的 stdout 完整行（F），正文保留结尾换行符。
		{ // CRI 日志格式 stdout
			line: "2016-10-20T18:39:20.57606443Z stdout F cri stdout test log\n",
			msg: &logMessage{
				timestamp: timestamp,
				stream:    runtimeapi.Stdout,
				log:       []byte("cri stdout test log\n"),
			},
		},
		// CRI 格式的 stderr 完整行（F）。
		{ // CRI 日志格式 stderr
			line: "2016-10-20T18:39:20.57606443Z stderr F cri stderr test log\n",
			msg: &logMessage{
				timestamp: timestamp,
				stream:    runtimeapi.Stderr,
				log:       []byte("cri stderr test log\n"),
			},
		},
		// 无法识别的日志格式，期望 getParseFunc 返回错误。
		{ // 不支持的日志格式
			line: "unsupported log format test log\n",
			msg:  &logMessage{},
			err:  true,
		},
		// CRI 部分日志行（P）：期望去掉正文结尾的换行符。
		{ // 部分 CRI 日志行
			line: "2016-10-20T18:39:20.57606443Z stdout P cri stdout partial test log\n",
			msg: &logMessage{
				timestamp: timestamp,
				stream:    runtimeapi.Stdout,
				log:       []byte("cri stdout partial test log"),
			},
		},
		// 带多个标签的 CRI 部分日志行（P）：只识别首个标签，正文同样去掉结尾换行符。
		{ // 带多个日志标签的部分 CRI 日志行。
			line: "2016-10-20T18:39:20.57606443Z stdout P:TAG1:TAG2 cri stdout partial test log\n",
			msg: &logMessage{
				timestamp: timestamp,
				stream:    runtimeapi.Stdout,
				log:       []byte("cri stdout partial test log"),
			},
		},
	} {
		t.Logf("TestCase #%d: %+v", c, test)
		// 先用样例行自动探测应选用的解析函数。
		parse, err := getParseFunc([]byte(test.line))
		if test.err {
			// 不支持的格式：断言探测阶段就报错。
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		// 解析该行并断言三个字段与期望的 logMessage 完全一致。
		err = parse([]byte(test.line), msg)
		assert.NoError(t, err)
		assert.Equal(t, test.msg, msg)
	}
}

// TestWriteLogs 验证 logWriter.write 的基本写入行为。
// 输入：固定时间戳的日志，分别测试 stderr/stdout 分流、Since 过滤、Timestamp 前缀。
// 断言：只有目标流收到内容；Since 晚于日志时间时无输出；开启 Timestamp 时输出带时间戳前缀。
func TestWriteLogs(t *testing.T) {
	// 固定的日志时间戳与正文内容。
	timestamp := time.Unix(1234, 43210)
	log := "abcdefg\n"

	// 表驱动用例：stream 决定目标流，since/timestamp 对应选项，后两个字段是期望输出。
	for c, test := range []struct {
		// stream 为日志流类型；since/timestamp 为写入选项；后两项为期望输出。
		stream       runtimeapi.LogStreamType
		since        time.Time
		timestamp    bool
		expectStdout string
		expectStderr string
	}{
		// stderr 日志只写入 stderr 流。
		{ // stderr 日志
			stream:       runtimeapi.Stderr,
			expectStderr: log,
		},
		// stdout 日志只写入 stdout 流。
		{ // stdout 日志
			stream:       runtimeapi.Stdout,
			expectStdout: log,
		},
		// Since 晚于日志时间：该行被过滤，两个流都无输出。
		{ // since 晚于时间戳
			stream: runtimeapi.Stdout,
			since:  timestamp.Add(1 * time.Second),
		},
		// 开启时间戳：正文前应有格式化时间戳加空格。
		{ // 已启用时间戳
			stream:       runtimeapi.Stderr,
			timestamp:    true,
			expectStderr: timestamp.Format(timeFormatOut) + " " + log,
		},
	} {
		t.Logf("TestCase #%d: %+v", c, test)
		// 构造一条日志，使用内存缓冲区接收两路输出。
		msg := &logMessage{
			timestamp: timestamp,
			stream:    test.stream,
			log:       []byte(log),
		}
		stdoutBuf := bytes.NewBuffer(nil)
		stderrBuf := bytes.NewBuffer(nil)
		// 按用例选项写入，并断言无错误且两个缓冲区内容符合预期。
		w := newLogWriter(stdoutBuf, stderrBuf, &LogOptions{Since: test.since, Timestamp: test.timestamp})
		err := w.write(msg, true)
		assert.NoError(t, err)
		assert.Equal(t, test.expectStdout, stdoutBuf.String())
		assert.Equal(t, test.expectStderr, stderrBuf.String())
	}
}

// TestWriteLogsWithBytesLimit 验证 LimitBytes 字节上限的截断行为。
// 输入：多行 8 字节日志，分别设置小于一行、跨行、大于总量、仅 stderr、两流混合及带时间戳等额度。
// 断言：输出被截断到指定字节数，配额耗尽时 write 返回 errMaximumWrite。
func TestWriteLogsWithBytesLimit(t *testing.T) {
	// 时间戳与正文用于计算含时间戳时的字节数。
	timestamp := time.Unix(1234, 4321)
	timestampStr := timestamp.Format(timeFormatOut)
	log := "abcdefg\n"

	// 表驱动用例：指定写入行数、字节上限和期望的两路输出。
	for c, test := range []struct {
		// stdoutLines/stderrLines 为向两路各写入的行数；bytes 为字节上限；timestamp 控制是否加前缀。
		stdoutLines  int
		stderrLines  int
		bytes        int
		timestamp    bool
		expectStdout string
		expectStderr string
	}{
		// 额度不足一行：只输出前 3 个字节。
		{ // 字节限制小于一行
			stdoutLines:  3,
			bytes:        3,
			expectStdout: "abc",
		},
		// 额度跨行：第一行完整输出，第二行只输出部分。
		{ // 字节限制跨多行
			stdoutLines:  3,
			bytes:        len(log) + 3,
			expectStdout: "abcdefg\nabc",
		},
		// 额度大于总字节数：全部输出。
		{ // 字节限制大于所有行
			stdoutLines:  3,
			bytes:        3 * len(log),
			expectStdout: "abcdefg\nabcdefg\nabcdefg\n",
		},
		// 仅向 stderr 写入多行时，额度同样生效。
		{ // stderr 的字节限制
			stderrLines:  3,
			bytes:        len(log) + 3,
			expectStderr: "abcdefg\nabc",
		},
		// 两路都写入：stdout 先消耗额度，剩余额度留给 stderr。
		{ // stdout 与 stderr 的字节限制，stdout 优先。
			stdoutLines:  1,
			stderrLines:  2,
			bytes:        len(log) + 3,
			expectStdout: "abcdefg\n",
			expectStderr: "abc",
		},
		// 开启时间戳：时间戳前缀也计入字节额度。
		{ // 带时间戳的字节限制
			stdoutLines:  3,
			timestamp:    true,
			bytes:        len(timestampStr) + 1 + len(log) + 2,
			expectStdout: timestampStr + " " + log + timestampStr[:2],
		},
	} {
		t.Logf("TestCase #%d: %+v", c, test)
		// 所有行共用一条日志（只改变 stream），观察额度如何被逐步消耗。
		msg := &logMessage{
			timestamp: timestamp,
			log:       []byte(log),
		}
		stdoutBuf := bytes.NewBuffer(nil)
		stderrBuf := bytes.NewBuffer(nil)
		// 字节上限由用例指定；超过上限时预期返回 errMaximumWrite。
		w := newLogWriter(stdoutBuf, stderrBuf, &LogOptions{Timestamp: test.timestamp, LimitBytes: ptr.To(int64(test.bytes))})
		for i := 0; i < test.stdoutLines; i++ {
			msg.stream = runtimeapi.Stdout
			if err := w.write(msg, true); err != nil {
				assert.EqualError(t, err, errMaximumWrite.Error())
			}
		}
		for i := 0; i < test.stderrLines; i++ {
			msg.stream = runtimeapi.Stderr
			if err := w.write(msg, true); err != nil {
				assert.EqualError(t, err, errMaximumWrite.Error())
			}
		}
		// 断言两路最终输出都被正确截断。
		assert.Equal(t, test.expectStdout, stdoutBuf.String())
		assert.Equal(t, test.expectStderr, stderrBuf.String())
	}
}

// TestReadLogsLimitsWithTimestamps 验证带时间戳的多行（P）日志在跨缓冲区读取时能正确拼接。
// 输入：临时文件中写入两段 10000 行 P 日志，每段以一行 F 空行结束。
// 断言：最终只输出 2 行，每行都以 9999 结尾（内容完整）且带有可解析的时间戳。
func TestReadLogsLimitsWithTimestamps(t *testing.T) {
	// P 表示部分日志，F 表示完整（结束）日志；用带时区偏移的时间戳测试解析。
	logLineFmt := "2022-10-29T16:10:22.592603036-05:00 stdout P %v\n"
	logLineNewLine := "2022-10-29T16:10:22.592603036-05:00 stdout F \n"

	// 创建临时日志文件用于构造跨缓冲区的长日志。
	tmpfile, err := os.CreateTemp("", "log.*.txt")
	assert.NoError(t, err)

	// 每段写 10000 行 P 日志，足以跨越 bufio 的默认缓冲区。
	count := 10000

	for i := 0; i < count; i++ {
		tmpfile.WriteString(fmt.Sprintf(logLineFmt, i))
	}
	// 第一段以 F 行结束。
	tmpfile.WriteString(logLineNewLine)

	for i := 0; i < count; i++ {
		tmpfile.WriteString(fmt.Sprintf(logLineFmt, i))
	}
	// 第二段同样以 F 行结束。
	tmpfile.WriteString(logLineNewLine)

	// 预期最终缓冲区中只有两行完整日志。

	defer os.Remove(tmpfile.Name()) // 清理

	assert.NoError(t, err)
	tmpfile.Close()

	// stdout/stderr 指向同一个缓冲区，开启 Timestamp 选项读取。
	var buf bytes.Buffer
	w := io.MultiWriter(&buf)

	// Follow 为 false，containerID 与 runtimeService 均可传空。
	err = ReadLogs(context.Background(), tmpfile.Name(), "", &LogOptions{Timestamp: true}, nil, w, w)
	assert.NoError(t, err)

	// 逐行校验输出：时间戳可解析，正文是完整的最后一段内容。
	lineCount := 0
	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for scanner.Scan() {
		lineCount++

		// 以第一个空格为界拆出时间戳和日志正文。
		ts, logline, _ := bytes.Cut(scanner.Bytes(), []byte(" "))

		// 断言时间戳格式合法，且正文以 9999 结尾（未被截断/拼接错误）。
		_, err = time.Parse(time.RFC3339, string(ts))
		assert.NoError(t, err, "timestamp not found")
		assert.True(t, bytes.HasSuffix(logline, []byte("9999")), "is the complete log found")
	}

	// 期望恰好输出两行。
	assert.Equal(t, 2, lineCount, "should have two lines")
}

// TestOnlyStdoutStream 验证 stderr writer 为 nil（未请求 stderr 流）时的写入行为。
// 输入：交替的 stdout/stderr/stdout 三条日志，stderr 目标传 nil，分别测试不限额和限额 7 字节。
// 断言：stderr 日志被忽略且不消耗字节配额，stdout 输出只包含 stdout 行并按上限截断。
func TestOnlyStdoutStream(t *testing.T) {
	// 三条交替的日志，时间相同，仅流类型与正文不同。
	timestamp := time.Unix(1234, 43210)
	msgs := []*logMessage{
		{
			timestamp: timestamp,
			stream:    runtimeapi.Stdout,
			log:       []byte("out1\n"),
		},
		{
			timestamp: timestamp,
			stream:    runtimeapi.Stderr,
			log:       []byte("err1\n"),
		},
		{
			timestamp: timestamp,
			stream:    runtimeapi.Stdout,
			log:       []byte("out2\n"),
		},
	}

	// 表驱动用例：limitBytes 为 -1 表示不限额，否则转换为 LimitBytes 选项。
	testCases := map[string]struct {
		// limitBytes 为字节上限；expectedStdout 为期望的 stdout 输出。
		limitBytes int64

		expectedStdout string
	}{
		// 不限额：stdout 两行全部输出，stderr 行被忽略。
		"all stdout logs": {
			limitBytes: -1,

			expectedStdout: "out1\nout2\n",
		},
		// 限额 7 字节：stdout 内容被截断，stderr 行不占用额度。
		"the first 7 bytes from stdout": {
			limitBytes: 7,

			expectedStdout: "out1\nou",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// stderr 传 nil，模拟只请求 stdout 流的场景。
			stdoutBuf := bytes.NewBuffer(nil)
			var bytesPtr *int64
			if tc.limitBytes >= 0 {
				bytesPtr = ptr.To(tc.limitBytes)
			}
			w := newLogWriter(stdoutBuf, nil, &LogOptions{
				LimitBytes: bytesPtr,
			})
			// 依次写入三条日志；达到字节上限后忽略后续写入。
			for _, msg := range msgs {
				err := w.write(msg, false)
				if errors.Is(err, errMaximumWrite) {
					continue
				}
				require.NoError(t, err)
			}
			// 断言 stdout 内容与期望一致（无 stderr 输出需要检查）。
			assert.EqualValues(t, tc.expectedStdout, stdoutBuf.String())
		})
	}
}
