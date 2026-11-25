/*
Copyright 2024 The Kubernetes Authors.

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

// Package logparse 提供 klog 文本格式的解析器：
//
//	 I1007 13:16:55.727802 1146763 example.go:57] "Key/value encoding" logger="example" foo="bar" duration="1s" int=42 float=3.14 string="hello world" quotedString="hello \"world\"" multiLinString=<
//		hello
//		world
//	  >
//	  E1007 15:20:04.343375 1157755 example.go:41] Log using Errorf, err: fail
//
// 同时也支持 klog/ktesting 的单元测试日志输出：
//
//	  === RUN   TestKlogr
//		    example_test.go:45: I1007 13:28:21.908998] hello world
//		    example_test.go:46: E1007 13:28:21.909034] failed err="failed: some error"
//		    example_test.go:47: I1007 13:28:21.909042] verbosity 1
//		    example_test.go:48: I1007 13:28:21.909047] main/helper: with prefix
//		    example_test.go:50: I1007 13:28:21.909076] key/value pairs int=1 float=2 pair="(1, 2)" raw={"Name":"joe","NS":"kube-system"} slice=[1,2,3,"str"] map={"a":1,"b":2} kobj="kube-system/joe" string="hello world" quotedString="hello \"world\"" multiLineString=<
//		        	hello
//		        	world
//		         >
//		    example_test.go:63: I1007 13:28:21.909085] info message level 4
//		    example_test.go:64: I1007 13:28:21.909089] info message level 5
//	  --- PASS: TestKlogr (0.00s)
//	  PASS
//	  ok  	k8s.io/klog/v2/ktesting/example	(cached)
//
// 支持用空格或 tab 进行任意缩进，但同一条 klog 日志的所有行
// 必须使用相同的缩进。
package logparse

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"iter"
	"regexp"
	"strings"
)

// Parse 将 reader 提供的日志输出拆分为一条条日志条目。
// 拼接这些条目即可逐字还原原始日志。如果 reader 读取失败，
// 产生的错误会记录在最后一条日志条目中。
func Parse(in io.Reader) []Entry {
	var log []Entry
	for entry := range All(in) {
		log = append(log, entry)
	}
	return log
}

// All 与 Parse 类似，区别是可用于 for/range 循环：
//
//	for entry := range logparse.All(reader) {
//	    // entry is the next log entry.
//	}
func All(in io.Reader) iter.Seq[Entry] {
	return func(yield func(Entry) bool) {
		parse(in, yield)
	}
}

// Entry 是日志条目的基础类型。
//
// 使用类型断言获取更多信息。该接口背后的所有实例都是指针。
type Entry interface {
	// LogData 返回原始日志片段的逐字副本（包含一个或多个换行符）。
	// 拼接所有条目的这些片段即可还原解析前的日志。
	LogData() string
}

// ErrorEntry 记录读取日志输入时遇到的错误。
type ErrorEntry struct {
	Err error
}

var _ Entry = &ErrorEntry{}
var _ fmt.Stringer = &ErrorEntry{}

func (e *ErrorEntry) LogData() string { return "" }
func (e *ErrorEntry) String() string  { return e.Err.Error() }

// OtherEntry 记录不属于 klog 日志条目的普通日志行。
type OtherEntry struct {
	Data string
}

var _ Entry = &OtherEntry{}
var _ fmt.Stringer = &OtherEntry{}

func (e *OtherEntry) LogData() string { return e.Data }
func (e *OtherEntry) String() string  { return e.Data }

// LogEntry 记录被识别为来自 klog 的日志条目。
type KlogEntry struct {
	Data     string
	Severity Severity
}

type Severity byte

const (
	SeverityInfo    = Severity('I')
	SeverityWarning = Severity('W')
	SeverityError   = Severity('E')
	SeverityFatal   = Severity('F')
)

var _ Entry = &KlogEntry{}
var _ fmt.Stringer = &KlogEntry{}

func (e *KlogEntry) LogData() string { return e.Data }
func (e *KlogEntry) String() string  { return e.Data }

func parse(in io.Reader, yield func(Entry) bool) {
	// 按任意长度读取行，这是 bufio.Scanner 做不到的。
	reader := bufio.NewReader(in)
	line, err := reader.ReadString('\n')
	for {
		// 先处理当前行；这可能需要向前预读，因此会返回下一行。
		var nextLine string
		var nextErr error
		if len(line) > 0 {
			var cont bool
			nextLine, cont, nextErr = parseLine(reader, line, yield)
			if !cont {
				return
			}
		} else {
			nextLine, nextErr = reader.ReadString('\n')
		}

		// 解析是否该结束？
		switch {
		case err == nil:
			// 一切正常。
		case errors.Is(err, io.EOF):
			return
		default:
			yield(&ErrorEntry{Err: err})
			return
		}

		line = nextLine
		err = nextErr
	}
}

const (
	pid      = `(?<pid>[[:digit:]]+)`
	source   = `(?<source>[^:]+:[[:digit:]]+)`
	severity = `(?<severity>[IWEF])`
	datetime = `(?<month>[[:digit:]]{2})(?<day>[[:digit:]]{2}) (?<hour>[[:digit:]]{2}):(?<minutes>[[:digit:]]{2}):(?<seconds>[[:digit:]]{2})\.(?<microseconds>[[:digit:]]{6})`
)

var (
	klogPrefix = regexp.MustCompile(`^(?<indention>[[:blank:]]*)` +
		`(?:` + source + `: )?` + // `go test` 的源代码位置
		severity +
		datetime +
		`(?: +` + pid + ` ` + source + `)?` + // klog 的 pid + 源代码位置
		`\] `)

	indentionIndex = lookupSubmatch("indention")
	severityIndex  = lookupSubmatch("severity")
)

func lookupSubmatch(name string) int {
	names := klogPrefix.SubexpNames()
	for i, n := range names {
		if n == name {
			return i
		}
	}
	panic(fmt.Errorf("named submatch %q not found in %q", name, klogPrefix.String()))
}

// parseLine 处理一个非空行，返回 yield 的结果，以及可能的下一行和/或读取错误。
// 如果没有新数据需要处理，则返回空字符串和 nil 错误。
func parseLine(reader *bufio.Reader, line string, yield func(Entry) bool) (string, bool, error) {
	match := klogPrefix.FindStringSubmatchIndex(line)
	if match == nil {
		cont := yield(&OtherEntry{Data: line})
		return "", cont, nil
	}

	e := &KlogEntry{
		Data:     line,
		Severity: Severity(line[match[2*severityIndex]]),
		// TODO (?): 保存更多已识别出的字段
	}
	// 必要时处理多行字符串值可能存在的续行。
	if !strings.HasSuffix(line, "=<\n") {
		return "", yield(e), nil
	}
	indention := line[match[2*indentionIndex]:match[2*indentionIndex+1]]
	for {
		var err error
		line, err = reader.ReadString('\n')
		if !strings.HasPrefix(line, indention) ||
			!strings.HasPrefix(line[len(indention):], "\t") && !strings.HasPrefix(line[len(indention):], " >") {
			// 这是其他行（缩进不对或续行格式不对）。
			// 先交出目前已有的内容，再回去处理这个新行。
			cont := yield(e)
			return line, cont, err
		}
		e.Data += line
		if err != nil {
			return "", yield(e), err
		}
	}
}
