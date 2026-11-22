//go:build linux

/*
Copyright 2019 The Kubernetes Authors.

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
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	utilwait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/exec"
)

// monitorFakeExec 是为 Monitor 测试定制的 Exec 假实现。
// 普通 FakeExec 不适用：Monitor 线程的检查次数以及与主线程的调用交错都不确定。
// 这里只实现 iptables 所需的最小接口；iptables.runner 改用 Exec 时需同步更新。
type monitorFakeExec struct {
	sync.Mutex

	tables map[string]sets.Set[string]

	block      bool
	wasBlocked bool
}

// newMonitorFakeExec 创建一个包含 mangle/filter/nat 三张空表的假 Exec。
func newMonitorFakeExec() *monitorFakeExec {
	tables := make(map[string]sets.Set[string])
	tables["mangle"] = sets.New[string]()
	tables["filter"] = sets.New[string]()
	tables["nat"] = sets.New[string]()
	return &monitorFakeExec{tables: tables}
}

// blockIPTables 模拟 xtables.lock 被占用：阻塞创建和列举链的操作。
func (mfe *monitorFakeExec) blockIPTables(block bool) {
	mfe.Lock()
	defer mfe.Unlock()

	mfe.block = block
}

// getWasBlocked 返回并清除“是否发生过阻塞”的标记，避免多次读取相互干扰。
func (mfe *monitorFakeExec) getWasBlocked() bool {
	mfe.Lock()
	defer mfe.Unlock()

	wasBlocked := mfe.wasBlocked
	mfe.wasBlocked = false
	return wasBlocked
}

// Command 返回一个监控测试专用的假命令对象。
func (mfe *monitorFakeExec) Command(cmd string, args ...string) exec.Cmd {
	return &monitorFakeCmd{mfe: mfe, cmd: cmd, args: args}
}

// CommandContext 忽略 ctx，直接委托给 Command。
func (mfe *monitorFakeExec) CommandContext(ctx context.Context, cmd string, args ...string) exec.Cmd {
	return mfe.Command(cmd, args...)
}

// LookPath 直接返回原路径，假装可执行文件总能找到。
func (mfe *monitorFakeExec) LookPath(file string) (string, error) {
	return file, nil
}

// monitorFakeCmd 是 monitorFakeExec 返回的假命令，
// 只实现 Monitor 实际调用的 CombinedOutput 等方法。
type monitorFakeCmd struct {
	mfe  *monitorFakeExec
	cmd  string
	args []string
}

// CombinedOutput 模拟 iptables 命令执行：支持版本查询及链的创建/列举/删除。
func (mfc *monitorFakeCmd) CombinedOutput() ([]byte, error) {
	if mfc.cmd == cmdIPTablesRestore {
		// 仅用于 "iptables-restore --version"，返回值不影响测试
		return []byte{}, nil
	} else if mfc.cmd != cmdIPTables {
		panic("bad command " + mfc.cmd)
	}

	if len(mfc.args) == 1 && mfc.args[0] == "--version" {
		return []byte("iptables v1.6.2"), nil
	}

	if len(mfc.args) != 6 || mfc.args[0] != WaitString || mfc.args[1] != WaitSecondsValue || mfc.args[4] != "-t" {
		panic(fmt.Sprintf("bad args %#v", mfc.args))
	}
	op := operation(mfc.args[2])
	chainName := mfc.args[3]
	tableName := mfc.args[5]

	mfc.mfe.Lock()
	defer mfc.mfe.Unlock()

	table := mfc.mfe.tables[tableName]
	if table == nil {
		return []byte{}, fmt.Errorf("no such table %q", tableName)
	}

	// 为便于测试，blockIPTables 只阻塞创建和列举，不阻塞删除，
	// 这样测试可以主动删除金丝雀链来触发 reload。
	if mfc.mfe.block && op != opDeleteChain {
		mfc.mfe.wasBlocked = true
		return []byte{}, exec.CodeExitError{Code: 4, Err: fmt.Errorf("could not get xtables.lock, etc")}
	}

	switch op {
	case opCreateChain:
		if !table.Has(chainName) {
			table.Insert(chainName)
		}
		return []byte{}, nil
	case opListChain:
		if table.Has(chainName) {
			return []byte{}, nil
		}
		return []byte{}, fmt.Errorf("no such chain %q", chainName)
	case opDeleteChain:
		table.Delete(chainName)
		return []byte{}, nil
	default:
		panic("should not be reached")
	}
}

// SetStdin 模拟设置标准输入，版本查询用不到，直接忽略。
func (mfc *monitorFakeCmd) SetStdin(in io.Reader) {
	// 该方法只被 getIPTablesRestoreVersionString() 调用，不影响本测试
}

// Run 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) Run() error {
	panic("should not be reached")
}

// Output 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) Output() ([]byte, error) {
	panic("should not be reached")
}

// SetDir 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) SetDir(dir string) {
	panic("should not be reached")
}

// SetStdout 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) SetStdout(out io.Writer) {
	panic("should not be reached")
}

// SetStderr 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) SetStderr(out io.Writer) {
	panic("should not be reached")
}

// SetEnv 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) SetEnv(env []string) {
	panic("should not be reached")
}

// StdoutPipe 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) StdoutPipe() (io.ReadCloser, error) {
	panic("should not be reached")
}

// StderrPipe 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) StderrPipe() (io.ReadCloser, error) {
	panic("should not be reached")
}

// Start 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) Start() error {
	panic("should not be reached")
}

func (mfc *monitorFakeCmd) Wait() error {
	panic("should not be reached")
}

// Stop 未实现：Monitor 不会调用该方法的路径。
func (mfc *monitorFakeCmd) Stop() {
	panic("should not be reached")
}

// TestIPTablesMonitor 验证 Monitor 能在链被删除时触发 reload，在锁占用时重试，并在停止时清理金丝雀链。
func TestIPTablesMonitor(t *testing.T) {
	mfe := newMonitorFakeExec()
	ipt := newInternal(mfe, ProtocolIPv4)

	var reloads uint32
	stopCh := make(chan struct{})

	canary := Chain("MONITOR-TEST-CANARY")
	tables := []Table{TableMangle, TableFilter, TableNAT}
	go ipt.Monitor(canary, tables, func() {
		if !ensureNoChains(mfe) {
			t.Errorf("reload called while canaries still exist")
		}
		atomic.AddUint32(&reloads, 1)
	}, 100*time.Millisecond, stopCh)

	// Monitor 应迅速创建金丝雀链
	if err := waitForChains(mfe, canary, tables); err != nil {
		t.Errorf("failed to create iptables canaries: %v", err)
	}

	if err := waitForReloads(&reloads, 0); err != nil {
		t.Errorf("got unexpected reloads: %v", err)
	}

	// 删除所有金丝雀链后应触发 reload
	ipt.DeleteChain(TableMangle, canary)
	ipt.DeleteChain(TableFilter, canary)
	ipt.DeleteChain(TableNAT, canary)

	if err := waitForReloads(&reloads, 1); err != nil {
		t.Errorf("got unexpected number of reloads after flush: %v", err)
	}
	if err := waitForChains(mfe, canary, tables); err != nil {
		t.Errorf("failed to create iptables canaries: %v", err)
	}

	// 只删除两条链时，还不能触发 reload
	ipt.DeleteChain(TableMangle, canary)
	ipt.DeleteChain(TableFilter, canary)

	if err := waitForNoReload(&reloads, 1); err != nil {
		t.Errorf("got unexpected number of reloads after partial flush: %v", err)
	}

	// 让 "iptables -L" 遇到 xtables.lock 错误，然后删除最后一条链。
	// 此时 Monitor 无法确认链是否真的被删除，因此不应 reload。
	mfe.blockIPTables(true)
	ipt.DeleteChain(TableNAT, canary)
	if err := waitForBlocked(mfe); err != nil {
		t.Errorf("failed waiting for monitor to be blocked from monitoring: %v", err)
	}

	// 解除阻塞后，Monitor 应能 reload
	mfe.blockIPTables(false)

	if err := waitForReloads(&reloads, 2); err != nil {
		t.Errorf("got unexpected number of reloads after slow flush: %v", err)
	}
	if err := waitForChains(mfe, canary, tables); err != nil {
		t.Errorf("failed to create iptables canaries: %v", err)
	}

	// 关闭 stop 通道后，Monitor 应停止运行并清理金丝雀链
	close(stopCh)

	if err := waitForNoReload(&reloads, 2); err != nil {
		t.Errorf("got unexpected number of reloads after stop: %v", err)
	}
	if !ensureNoChains(mfe) {
		t.Errorf("canaries still exist after stopping monitor")
	}

	// 在 iptables 锁被占用时启动新 Monitor，它会不断重试创建金丝雀链直到成功

	stopCh = make(chan struct{})
	_ = mfe.getWasBlocked()
	mfe.blockIPTables(true)
	go ipt.Monitor(canary, tables, func() {
		if !ensureNoChains(mfe) {
			t.Errorf("reload called while canaries still exist")
		}
		atomic.AddUint32(&reloads, 1)
	}, 100*time.Millisecond, stopCh)

	// 锁仍被占用时，Monitor 不应创建出金丝雀链
	if !ensureNoChains(mfe) {
		t.Errorf("canary created while iptables blocked")
	}

	if err := waitForBlocked(mfe); err != nil {
		t.Errorf("failed waiting for monitor to fail creating canaries: %v", err)
	}

	mfe.blockIPTables(false)
	if err := waitForChains(mfe, canary, tables); err != nil {
		t.Errorf("failed to create iptables canaries: %v", err)
	}

	close(stopCh)
}

// waitForChains 轮询等待所有表中都存在指定的金丝雀链。
func waitForChains(mfe *monitorFakeExec, canary Chain, tables []Table) error {
	return utilwait.PollImmediate(100*time.Millisecond, time.Second, func() (bool, error) {
		mfe.Lock()
		defer mfe.Unlock()

		for _, table := range tables {
			if !mfe.tables[string(table)].Has(string(canary)) {
				return false, nil
			}
		}
		return true, nil
	})
}

// ensureNoChains 检查三张表中都没有任何链。
func ensureNoChains(mfe *monitorFakeExec) bool {
	mfe.Lock()
	defer mfe.Unlock()
	return mfe.tables["mangle"].Len() == 0 &&
		mfe.tables["filter"].Len() == 0 &&
		mfe.tables["nat"].Len() == 0
}

// waitForReloads 轮询等待 reload 次数达到 expected，并断言次数精确一致。
func waitForReloads(reloads *uint32, expected uint32) error {
	if atomic.LoadUint32(reloads) < expected {
		utilwait.PollImmediate(100*time.Millisecond, time.Second, func() (bool, error) {
			return atomic.LoadUint32(reloads) >= expected, nil
		})
	}
	got := atomic.LoadUint32(reloads)
	if got != expected {
		return fmt.Errorf("expected %d, got %d", expected, got)
	}
	return nil
}

// waitForNoReload 在短时间内观察 reload 次数不应超过 expected。
func waitForNoReload(reloads *uint32, expected uint32) error {
	utilwait.PollImmediate(50*time.Millisecond, 250*time.Millisecond, func() (bool, error) {
		return atomic.LoadUint32(reloads) > expected, nil
	})

	got := atomic.LoadUint32(reloads)
	if got != expected {
		return fmt.Errorf("expected %d, got %d", expected, got)
	}
	return nil
}

// waitForBlocked 轮询等待 Monitor 至少被阻塞过一次。
func waitForBlocked(mfe *monitorFakeExec) error {
	return utilwait.PollImmediate(100*time.Millisecond, time.Second, func() (bool, error) {
		blocked := mfe.getWasBlocked()
		return blocked, nil
	})
}
