//go:build linux

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

package iptables

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
	utiltrace "k8s.io/utils/trace"
)

// RulePosition 表示规则的插入位置，对应 iptables 的 -I/-A 标志。
type RulePosition string

const (
	// Prepend 表示将规则插入链首（-I）。
	Prepend RulePosition = "-I"
	// Append 表示将规则追加到链尾（-A）。
	Append RulePosition = "-A"
)

// Interface 是可注入的 iptables 命令执行接口。实现必须保证并发安全。
type Interface interface {
	// EnsureChain 检查指定链是否存在，不存在则创建；链原本已存在时返回 true。
	EnsureChain(table Table, chain Chain) (bool, error)
	// FlushChain 清空指定链；链不存在时返回错误。
	FlushChain(table Table, chain Chain) error
	// DeleteChain 删除指定链；链不存在时返回错误。
	DeleteChain(table Table, chain Chain) error
	// ChainExists 判断指定链是否存在；链不存在或无法检查时返回错误。
	ChainExists(table Table, chain Chain) (bool, error)
	// EnsureRule 检查指定规则是否存在，不存在则创建；规则原本已存在时返回 true。
	EnsureRule(position RulePosition, table Table, chain Chain, args ...string) (bool, error)
	// DeleteRule 检查指定规则是否存在，存在则删除。
	DeleteRule(table Table, chain Chain, args ...string) error
	// IsIPv6 判断是否管理 IPv6 表。
	IsIPv6() bool
	// Protocol 返回本实例所管理的 IP 协议族。
	Protocol() Protocol
	// SaveInto 对指定表执行 `iptables-save`，并将结果写入给定缓冲区。
	SaveInto(table Table, buffer *bytes.Buffer) error
	// Restore 通过 `iptables-restore` 恢复规则。
	// table 为待恢复的表；data 的格式须与 SaveInto() 的输出一致。
	// flush 控制 "--noflush" 标志，见 FlushFlag。
	// counters 控制 "--counters" 标志，见 RestoreCountersFlag。
	Restore(table Table, data []byte, flush FlushFlag, counters RestoreCountersFlag) error
	// RestoreAll 与 Restore 相同，但不指定表。
	RestoreAll(data []byte, flush FlushFlag, counters RestoreCountersFlag) error
	// Monitor 通过创建金丝雀链并轮询其是否被删除，检测外部工具（如防火墙重载）
	// 是否清空了给定的 iptables 表。具体做法：每隔 interval 轮询 tables[0]，
	// 直到其中的金丝雀链被删除，再稍等片刻确认其余表中的金丝雀链也被删除。
	// 把相对空闲的表放在 tables[0] 可以降低轮询开销。检测到清空后调用 reloadFunc，
	// 让调用方重新加载自己的规则。若无法创建金丝雀链（初次或重载后），记录错误并停止监控。
	// 该方法应在 goroutine 中调用。
	Monitor(canary Chain, tables []Table, reloadFunc func(), interval time.Duration, stopCh <-chan struct{})

	// Present 检查当前内核是否支持 iptables 接口。
	Present() error
}

// Protocol 表示 IP 协议族，取值为 IPv4 或 IPv6。
type Protocol string

const (
	// ProtocolIPv4 表示 iptables 中的 IPv4 协议。
	ProtocolIPv4 Protocol = "IPv4"
	// ProtocolIPv6 表示 iptables 中的 IPv6 协议。
	ProtocolIPv6 Protocol = "IPv6"
)

// Table 表示不同的 iptables 表，如 filter、nat、mangle。
type Table string

const (
	// TableNAT 表示内置的 nat 表。
	TableNAT Table = "nat"
	// TableFilter 表示内置的 filter 表。
	TableFilter Table = "filter"
	// TableMangle 表示内置的 mangle 表。
	TableMangle Table = "mangle"
)

// Chain 表示 iptables 的规则链。
type Chain string

const (
	// ChainPostrouting 用于 nat 表的源地址转换（SNAT）。
	ChainPostrouting Chain = "POSTROUTING"
	// ChainPrerouting 用于 nat 表的目的地址转换（DNAT）。
	ChainPrerouting Chain = "PREROUTING"
	// ChainOutput 用于处理本机发出的包。
	ChainOutput Chain = "OUTPUT"
	// ChainInput 用于处理进入本机的包。
	ChainInput Chain = "INPUT"
	// ChainForward 用于处理需要转发到其他网卡的包。
	ChainForward Chain = "FORWARD"
)

const (
	cmdIPTablesSave     string = "iptables-save"
	cmdIPTablesRestore  string = "iptables-restore"
	cmdIPTables         string = "iptables"
	cmdIP6TablesRestore string = "ip6tables-restore"
	cmdIP6TablesSave    string = "ip6tables-save"
	cmdIP6Tables        string = "ip6tables"
)

// RestoreCountersFlag 是 Restore 的选项标志，控制是否恢复计数器。
type RestoreCountersFlag bool

// RestoreCounters 表示恢复计数器（RestoreCountersFlag 的 true 值）。
const RestoreCounters RestoreCountersFlag = true

// NoRestoreCounters 表示不恢复计数器（RestoreCountersFlag 的 false 值）。
const NoRestoreCounters RestoreCountersFlag = false

// FlushFlag 是 Flush 的选项标志，控制是否清空表。
type FlushFlag bool

// FlushTables 表示清空表（FlushFlag 的 true 值）。
const FlushTables FlushFlag = true

// NoFlushTables 表示不清空表（FlushFlag 的 false 值）。
const NoFlushTables FlushFlag = false

// WaitString 是指定等待标志的常量。
const WaitString = "-w"

// WaitSecondsValue 是默认等待秒数的常量。
const WaitSecondsValue = "5"

// runner 通过 exec("iptables") 实现 Interface。
type runner struct {
	exec     utilexec.Interface
	protocol Protocol
}

// newInternal 返回一个通过 exec 执行 iptables 的 Interface。
func newInternal(exec utilexec.Interface, protocol Protocol) Interface {
	runner := &runner{
		exec:     exec,
		protocol: protocol,
	}

	return runner
}

// New 返回一个通过 exec 执行 iptables 的 Interface。
// 注意：即使只支持单个协议族，该函数也只返回一个 iptables Interface，不会返回错误。
func New(protocol Protocol) Interface {
	return newInternal(utilexec.New(), protocol)
}

// newDualStackInternal 返回受支持协议族到对应 Interface 的映射：
// 仅当对应协议族的 iptables 可用时才将其纳入。
func newDualStackInternal(exec utilexec.Interface) map[v1.IPFamily]Interface {
	interfaces := map[v1.IPFamily]Interface{}
	iptv4 := newInternal(exec, ProtocolIPv4)
	if presentErr := iptv4.Present(); presentErr == nil {
		interfaces[v1.IPv4Protocol] = iptv4
	}
	iptv6 := newInternal(exec, ProtocolIPv6)
	if presentErr := iptv6.Present(); presentErr == nil {
		interfaces[v1.IPv6Protocol] = iptv6
	}
	return interfaces
}

// NewBestEffort 返回一个映射：若支持 IPv4 iptables 则包含 IPv4 Interface，
// 若支持 IPv6 iptables 则包含 IPv6 Interface；若都不支持则返回空映射。
// 该函数服务于"尽力而为"的场景——部分支持或完全不支持都不算错误。
func NewBestEffort() map[v1.IPFamily]Interface {
	return newDualStackInternal(utilexec.New())
}

// EnsureChain 实现 Interface 的 EnsureChain 方法。
// 链已存在属于正常情况，因此把退出码 1 视为"已创建"而不是错误。
func (runner *runner) EnsureChain(table Table, chain Chain) (bool, error) {
	fullArgs := makeFullArgs(table, chain)

	out, err := runner.run(opCreateChain, fullArgs)
	if err != nil {
		if ee, ok := err.(utilexec.ExitError); ok {
			if ee.Exited() && ee.ExitStatus() == 1 {
				return true, nil
			}
		}
		return false, fmt.Errorf("error creating chain %q: %v: %s", chain, err, out)
	}
	return false, nil
}

// FlushChain 实现 Interface 的 FlushChain 方法。
func (runner *runner) FlushChain(table Table, chain Chain) error {
	fullArgs := makeFullArgs(table, chain)

	out, err := runner.run(opFlushChain, fullArgs)
	if err != nil {
		return fmt.Errorf("error flushing chain %q: %v: %s", chain, err, out)
	}
	return nil
}

// DeleteChain 实现 Interface 的 DeleteChain 方法。
func (runner *runner) DeleteChain(table Table, chain Chain) error {
	fullArgs := makeFullArgs(table, chain)

	out, err := runner.run(opDeleteChain, fullArgs)
	if err != nil {
		return fmt.Errorf("error deleting chain %q: %v: %s", chain, err, out)
	}
	return nil
}

// EnsureRule 实现 Interface 的 EnsureRule 方法。
// 先检查再添加，保证规则幂等。
func (runner *runner) EnsureRule(position RulePosition, table Table, chain Chain, args ...string) (bool, error) {
	fullArgs := makeFullArgs(table, chain, args...)

	exists, err := runner.checkRule(fullArgs)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	out, err := runner.run(operation(position), fullArgs)
	if err != nil {
		return false, fmt.Errorf("error appending rule: %v: %s", err, out)
	}
	return false, nil
}

// DeleteRule 实现 Interface 的 DeleteRule 方法。
// 规则不存在时直接返回成功，使删除操作幂等。
func (runner *runner) DeleteRule(table Table, chain Chain, args ...string) error {
	fullArgs := makeFullArgs(table, chain, args...)

	exists, err := runner.checkRule(fullArgs)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	out, err := runner.run(opDeleteRule, fullArgs)
	if err != nil {
		return fmt.Errorf("error deleting rule: %v: %s", err, out)
	}
	return nil
}

// IsIPv6 实现 Interface 的 IsIPv6 方法。
func (runner *runner) IsIPv6() bool {
	return runner.protocol == ProtocolIPv6
}

// Protocol 实现 Interface 的 Protocol 方法。
func (runner *runner) Protocol() Protocol {
	return runner.protocol
}

// SaveInto 实现 Interface 的 SaveInto 方法。
func (runner *runner) SaveInto(table Table, buffer *bytes.Buffer) error {
	trace := utiltrace.New("iptables save")
	defer trace.LogIfLong(2 * time.Second)

	// 执行命令并返回结果。
	iptablesSaveCmd := iptablesSaveCommand(runner.protocol)
	args := []string{"-t", string(table)}
	klog.V(4).InfoS("Running", "command", iptablesSaveCmd, "arguments", args)
	cmd := runner.exec.Command(iptablesSaveCmd, args...)
	cmd.SetStdout(buffer)
	stderrBuffer := bytes.NewBuffer(nil)
	cmd.SetStderr(stderrBuffer)

	err := cmd.Run()
	if err != nil {
		stderrBuffer.WriteTo(buffer) // 忽略写入错误，优先返回原始执行错误。
	}
	return err
}

// Restore 实现 Interface 的 Restore 方法。
func (runner *runner) Restore(table Table, data []byte, flush FlushFlag, counters RestoreCountersFlag) error {
	// 准备指定表的参数。
	args := []string{"-T", string(table)}
	return runner.restoreInternal(args, data, flush, counters)
}

// RestoreAll 实现 Interface 的 RestoreAll 方法。
func (runner *runner) RestoreAll(data []byte, flush FlushFlag, counters RestoreCountersFlag) error {
	// 不指定表，参数留空。
	args := make([]string, 0)
	return runner.restoreInternal(args, data, flush, counters)
}

// restoreInternal 是 Restore/RestoreAll 的公共实现。
func (runner *runner) restoreInternal(args []string, data []byte, flush FlushFlag, counters RestoreCountersFlag) error {
	trace := utiltrace.New("iptables restore")
	defer trace.LogIfLong(2 * time.Second)

	if !flush {
		args = append(args, "--noflush")
	}
	if counters {
		args = append(args, "--counters")
	}

	// 执行命令并返回输出；出错时把输出一并带上，便于定位问题。
	fullArgs := append([]string{WaitString, WaitSecondsValue}, args...)
	iptablesRestoreCmd := iptablesRestoreCommand(runner.protocol)
	klog.V(4).InfoS("Running", "command", iptablesRestoreCmd, "arguments", fullArgs)
	cmd := runner.exec.Command(iptablesRestoreCmd, fullArgs...)
	cmd.SetStdin(bytes.NewBuffer(data))
	b, err := cmd.CombinedOutput()
	if err != nil {
		pErr, ok := parseRestoreError(string(b))
		if ok {
			return pErr
		}
		return fmt.Errorf("%w: %s", err, b)
	}
	return nil
}

// iptablesSaveCommand 按协议族返回对应的 save 命令名。
func iptablesSaveCommand(protocol Protocol) string {
	if protocol == ProtocolIPv6 {
		return cmdIP6TablesSave
	}
	return cmdIPTablesSave
}

// iptablesRestoreCommand 按协议族返回对应的 restore 命令名。
func iptablesRestoreCommand(protocol Protocol) string {
	if protocol == ProtocolIPv6 {
		return cmdIP6TablesRestore
	}
	return cmdIPTablesRestore
}

// iptablesCommand 按协议族返回对应的 iptables 命令名。
func iptablesCommand(protocol Protocol) string {
	if protocol == ProtocolIPv6 {
		return cmdIP6Tables
	}
	return cmdIPTables
}

// run 使用 context.TODO() 执行单条 iptables 操作。
func (runner *runner) run(op operation, args []string) ([]byte, error) {
	return runner.runContext(context.TODO(), op, args)
}

// runContext 在给定 context 下执行单条 iptables 操作。
func (runner *runner) runContext(ctx context.Context, op operation, args []string) ([]byte, error) {
	iptablesCmd := iptablesCommand(runner.protocol)
	fullArgs := append([]string{WaitString, WaitSecondsValue, string(op)}, args...)
	klog.V(5).InfoS("Running", "command", iptablesCmd, "arguments", fullArgs)
	if ctx == nil {
		return runner.exec.Command(iptablesCmd, fullArgs...).CombinedOutput()
	}
	return runner.exec.CommandContext(ctx, iptablesCmd, fullArgs...).CombinedOutput()
	// 此处不记录错误：调用方可能并不把它当作错误。
}

// checkRule 检查规则是否存在。成功检查返回 (bool, nil)；
// 检查过程本身失败则返回 (<未定义>, error)。
func (runner *runner) checkRule(args []string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	out, err := runner.runContext(ctx, opCheckRule, args)
	if ctx.Err() == context.DeadlineExceeded {
		return false, fmt.Errorf("timed out while checking rules")
	}
	if err == nil {
		return true, nil
	}
	if ee, ok := err.(utilexec.ExitError); ok {
		// iptables 用退出码 1 表示规则不存在（操作失败），
		// 与命令行格式错误等其他失败区分开。
		if ee.Exited() && ee.ExitStatus() == 1 {
			return false, nil
		}
	}
	return false, fmt.Errorf("error checking rule: %v: %s", err, out)
}

const (
	// 发现清空开始后，等待 iptables 清空完成的最长时间。
	iptablesFlushTimeout = 5 * time.Second
	// 等待清空完成时轮询的间隔。
	iptablesFlushPollTime = 100 * time.Millisecond
)

// Monitor 实现 Interface 的 Monitor 方法。
// 外层循环负责在每次重载后重新建立金丝雀链，应对反复发生的清空。
func (runner *runner) Monitor(canary Chain, tables []Table, reloadFunc func(), interval time.Duration, stopCh <-chan struct{}) {
	for {
		_ = utilwait.PollImmediateUntil(interval, func() (bool, error) {
			for _, table := range tables {
				if _, err := runner.EnsureChain(table, canary); err != nil {
					klog.ErrorS(err, "Could not set up iptables canary", "table", table, "chain", canary)
					return false, nil
				}
			}
			return true, nil
		}, stopCh)

		// 轮询直到 stopCh 关闭或 iptables 被清空。
		err := utilwait.PollUntil(interval, func() (bool, error) {
			if exists, err := runner.ChainExists(tables[0], canary); exists {
				return false, nil
			} else if isResourceError(err) {
				// 资源错误（如拿不到锁）不代表清空，继续等待。
				klog.ErrorS(err, "Could not check for iptables canary", "table", tables[0], "chain", canary)
				return false, nil
			}
			klog.V(2).InfoS("IPTables canary deleted", "table", tables[0], "chain", canary)
			// 等其余金丝雀链也被删除再返回，避免过早开始重载。
			err := utilwait.PollImmediate(iptablesFlushPollTime, iptablesFlushTimeout, func() (bool, error) {
				for i := 1; i < len(tables); i++ {
					if exists, err := runner.ChainExists(tables[i], canary); exists || isResourceError(err) {
						return false, nil
					}
				}
				return true, nil
			})
			if err != nil {
				klog.InfoS("Inconsistent iptables state detected")
			}
			return true, nil
		}, stopCh)
		if err != nil {
			// stopCh 已关闭，清理金丝雀链后退出。
			for _, table := range tables {
				_ = runner.DeleteChain(table, canary)
			}
			return
		}

		klog.V(2).InfoS("Reloading after iptables flush")
		reloadFunc()
	}
}

// ChainExists 实现 Interface 的 ChainExists 方法。
func (runner *runner) ChainExists(table Table, chain Chain) (bool, error) {
	fullArgs := makeFullArgs(table, chain)

	trace := utiltrace.New("iptables ChainExists")
	defer trace.LogIfLong(2 * time.Second)

	out, err := runner.run(opListChain, fullArgs)
	if err != nil {
		return false, fmt.Errorf("error listing chain %q in table %q: %w: %s", chain, table, err, out)
	}
	return true, nil
}

// operation 表示传给 iptables 的操作标志。
type operation string

const (
	opCreateChain operation = "-N"
	opFlushChain  operation = "-F"
	opDeleteChain operation = "-X"
	opListChain   operation = "-S"
	opCheckRule   operation = "-C"
	opDeleteRule  operation = "-D"
)

// makeFullArgs 将表名和链名拼接到操作参数之前，生成完整的 iptables 参数列表。
func makeFullArgs(table Table, chain Chain, args ...string) []string {
	return append([]string{string(chain), "-t", string(table)}, args...)
}

// Present 通过检查默认表和链是否存在，判断当前内核是否支持 iptables。
func (runner *runner) Present() error {
	if _, err := runner.ChainExists(TableNAT, ChainPostrouting); err != nil {
		return err
	}
	return nil
}

// iptablesNotFoundStrings 汇总了不同 iptables 版本/后端表示"未找到"的错误文本。
var iptablesNotFoundStrings = []string{
	// iptables-legacy [-A|-I] BAD-CHAIN [...]
	// iptables-legacy [-C|-D] GOOD-CHAIN [...规则不匹配...]
	// iptables-legacy [-X|-F|-Z] BAD-CHAIN
	// iptables-nft -X BAD-CHAIN
	// 注意：iptables-nft [-F|-Z] BAD-CHAIN 不会报错。
	"No chain/target/match by that name",

	// iptables-legacy [...] -j BAD-CHAIN
	// iptables-nft-1.8.0 [-A|-I] BAD-CHAIN [...]
	// iptables-nft-1.8.0 [-A|-I] GOOD-CHAIN -j BAD-CHAIN
	// 注意：也会匹配其他情况，比如 "-m BAD-MODULE"。
	"No such file or directory",

	// iptables-legacy [-C|-D] BAD-CHAIN [...]
	// iptables-nft [-C|-D] GOOD-CHAIN [...规则不匹配...]
	"does a matching rule exist",

	// iptables-nft-1.8.2 [-A|-C|-D|-I] BAD-CHAIN [...]
	// iptables-nft-1.8.2 [...] -j BAD-CHAIN
	"does not exist",
}

// IsNotFoundError 判断错误是否表示"未找到"。它通过匹配错误文本中的已知值实现，
// 并不精确；请勿用于"记录日志还是忽略错误"之外的场景。
func IsNotFoundError(err error) bool {
	es := err.Error()
	for _, str := range iptablesNotFoundStrings {
		if strings.Contains(es, str) {
			return true
		}
	}
	return false
}

// iptablesStatusResourceProblem 是 iptables 表示"资源问题"的退出码。
const iptablesStatusResourceProblem = 4

// isResourceError 判断错误是否表示 iptables 遇到"资源问题"、
// 无法执行请求，典型情况是获取 iptables 锁超时。
func isResourceError(err error) bool {
	if ee, isExitError := err.(utilexec.ExitError); isExitError {
		return ee.ExitStatus() == iptablesStatusResourceProblem
	}
	return false
}

// ParseError 记录 iptables 报告输入解析错误时的信息。
type ParseError interface {
	// Line 返回报告解析错误的行号。
	// 注意：首行行号为 1。
	Line() int
	// Error 返回包含行号的解析错误消息。
	Error() string
}

// parseError 是 ParseError 的具体实现。
type parseError struct {
	cmd  string
	line int
}

// Line 实现 ParseError 的 Line 方法。
func (e parseError) Line() int {
	return e.line
}

// Error 实现 ParseError 的 Error 方法。
func (e parseError) Error() string {
	return fmt.Sprintf("%s: input error on line %d: ", e.cmd, e.line)
}

// LineData 表示带行号的一行数据。
type LineData struct {
	// Line 为行号（首行为 1）。
	Line int
	// Data 为该行的内容。
	Data string
}

// regexpParseError 用于从 iptables 的错误文本中提取失败行号。
var regexpParseError = regexp.MustCompile("line ([1-9][0-9]*) failed$")

// parseRestoreError 从错误中提取行号；匹配成功时返回 parseError。示例：
// 输入：iptables-restore: line 51 failed
// 输出：parseError，其中 cmd = iptables-restore，line = 51
// 注意：该函数依赖 iptables 的错误格式，格式变化时需同步更新。
func parseRestoreError(str string) (ParseError, bool) {
	errors := strings.Split(str, ":")
	if len(errors) != 2 {
		return nil, false
	}
	cmd := errors[0]
	matches := regexpParseError.FindStringSubmatch(errors[1])
	if len(matches) != 2 {
		return nil, false
	}
	line, errMsg := strconv.Atoi(matches[1])
	if errMsg != nil {
		return nil, false
	}
	return parseError{cmd: cmd, line: line}, true
}

// ExtractLines 提取 lines 中第 line 行前后 count 行的数据（共 2*count+1 行，边界除外）。
// 注意：行号从 1 开始。
func ExtractLines(lines []byte, line, count int) []LineData {
	// 首行行号为 1，因此 line 不能小于 1。
	if line < 1 {
		return nil
	}
	start := line - count
	if start <= 0 {
		start = 1
	}
	end := line + count + 1

	offset := 1
	scanner := bufio.NewScanner(bytes.NewBuffer(lines))
	extractLines := make([]LineData, 0, count*2)
	for scanner.Scan() {
		if offset >= start && offset < end {
			extractLines = append(extractLines, LineData{
				Line: offset,
				Data: scanner.Text(),
			})
		}
		if offset == end {
			break
		}
		offset++
	}
	return extractLines
}
