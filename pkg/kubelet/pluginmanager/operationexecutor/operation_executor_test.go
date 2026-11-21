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

package operationexecutor

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	numPluginsToRegister   = 2
	numPluginsToUnregister = 2
	// ch 通道的缓冲区大小；必须不小于任一测试启动的 goroutine 数，
	// 避免超时前发送阻塞导致 goroutine 泄漏。
	maxConcurrentOperations = max(numPluginsToRegister, numPluginsToUnregister)
)

// 编译期断言：fakeOperationGenerator 实现 OperationGenerator 接口。
var _ OperationGenerator = &fakeOperationGenerator{}
var socketDir string

// init 创建测试用临时 socket 目录。
func init() {
	d, err := os.MkdirTemp("", "operation_executor_test")
	if err != nil {
		panic(fmt.Sprintf("Could not create a temp directory: %s", d))
	}
	socketDir = d
}

// TestOperationExecutor_RegisterPlugin_ConcurrentRegisterPlugin 验证不同 socket
// 的注册操作可并发执行。
func TestOperationExecutor_RegisterPlugin_ConcurrentRegisterPlugin(t *testing.T) {
	tCtx, ch, quit, oe := setup(t)
	for i := 0; i < numPluginsToRegister; i++ {
		socketPath := fmt.Sprintf("%s/plugin-%d.sock", socketDir, i)
		err := oe.RegisterPlugin(tCtx, socketPath, uuid.NewUUID(), nil /* plugin handlers */, nil /* actual state of the world updator */)
		assert.NoError(t, err)
	}
	if !isOperationRunConcurrently(ch, quit, numPluginsToRegister) {
		t.Fatalf("Unable to start register operations in Concurrent for plugins")
	}
}

// TestOperationExecutor_RegisterPlugin_SerialRegisterPlugin 验证同一 socket
// 的注册操作串行执行，重复注册会失败。
func TestOperationExecutor_RegisterPlugin_SerialRegisterPlugin(t *testing.T) {
	tCtx, ch, quit, oe := setup(t)
	socketPath := fmt.Sprintf("%s/plugin-serial.sock", socketDir)

	// 第一次注册应成功。
	err := oe.RegisterPlugin(tCtx, socketPath, uuid.NewUUID(), nil /* plugin handlers */, nil /* actual state of the world updator */)
	assert.NoError(t, err)

	for i := 1; i < numPluginsToRegister; i++ {
		err := oe.RegisterPlugin(tCtx, socketPath, uuid.NewUUID(), nil /* plugin handlers */, nil /* actual state of the world updator */)
		if err == nil {
			t.Fatalf("RegisterPlugin did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name is already executing.> Actual: <no error>", socketPath)
		}

	}
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Unable to start register operations serially for plugins")
	}
}

// TestOperationExecutor_UnregisterPlugin_ConcurrentUnregisterPlugin 验证不同 socket
// 的注销操作可并发执行。
func TestOperationExecutor_UnregisterPlugin_ConcurrentUnregisterPlugin(t *testing.T) {
	tCtx, ch, quit, oe := setup(t)
	for i := 0; i < numPluginsToUnregister; i++ {
		socketPath := "socket-path" + strconv.Itoa(i)
		pluginInfo := cache.PluginInfo{SocketPath: socketPath}
		_ = oe.UnregisterPlugin(tCtx, pluginInfo, nil /* actual state of the world updator */)
	}
	if !isOperationRunConcurrently(ch, quit, numPluginsToUnregister) {
		t.Fatalf("Unable to start unregister operations in Concurrent for plugins")
	}
}

// TestOperationExecutor_UnregisterPlugin_SerialUnregisterPlugin 验证同一 socket
// 的注销操作串行执行。
func TestOperationExecutor_UnregisterPlugin_SerialUnregisterPlugin(t *testing.T) {
	tCtx, ch, quit, oe := setup(t)
	socketPath := fmt.Sprintf("%s/plugin-serial.sock", socketDir)
	for i := 0; i < numPluginsToUnregister; i++ {
		pluginInfo := cache.PluginInfo{SocketPath: socketPath}
		_ = oe.UnregisterPlugin(tCtx, pluginInfo, nil /* actual state of the world updator */)
	}
	if !isOperationRunSerially(ch, quit) {
		t.Fatalf("Unable to start unregister operations serially for plugins")
	}
}

// fakeOperationGenerator 通过 ch/quit 通道模拟耗时操作。
type fakeOperationGenerator struct {
	ch   chan interface{}
	quit chan interface{}
}

// newFakeOperationGenerator 创建 fake 操作生成器。
func newFakeOperationGenerator(ch chan interface{}, quit chan interface{}) OperationGenerator {
	return &fakeOperationGenerator{
		ch:   ch,
		quit: quit,
	}
}

// GenerateRegisterPluginFunc 返回一个启动后阻塞的注册操作函数。
func (fopg *fakeOperationGenerator) GenerateRegisterPluginFunc(
	ctx context.Context,
	socketPath string,
	pluginUUID types.UID,
	pluginHandlers map[string]cache.PluginHandler,
	actualStateOfWorldUpdater ActualStateOfWorldUpdater) func() error {

	opFunc := func() error {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return nil
	}
	return opFunc
}

// GenerateUnregisterPluginFunc 返回一个启动后阻塞的注销操作函数。
func (fopg *fakeOperationGenerator) GenerateUnregisterPluginFunc(
	ctx context.Context,
	pluginInfo cache.PluginInfo,
	actualStateOfWorldUpdater ActualStateOfWorldUpdater) func() error {
	opFunc := func() error {
		startOperationAndBlock(fopg.ch, fopg.quit)
		return nil
	}
	return opFunc
}

// isOperationRunSerially 统计并发启动的操作数，超过 1 即判定失败。
func isOperationRunSerially(ch <-chan interface{}, quit chan<- interface{}) bool {
	defer close(quit)
	numOperationsStarted := 0
loop:
	for {
		select {
		case <-ch:
			numOperationsStarted++
			if numOperationsStarted > 1 {
				return false
			}
		case <-time.After(5 * time.Second):
			break loop
		}
	}
	return true
}

// isOperationRunConcurrently 等待指定数量的操作同时启动。
func isOperationRunConcurrently(ch <-chan interface{}, quit chan<- interface{}, numOperationsToRun int) bool {
	defer close(quit)
	numOperationsStarted := 0
loop:
	for {
		select {
		case <-ch:
			numOperationsStarted++
			if numOperationsStarted == numOperationsToRun {
				return true
			}
		case <-time.After(5 * time.Second):
			break loop
		}
	}
	return false
}

// setup 构造测试上下文、通道与操作执行器。
func setup(t *testing.T) (context.Context, chan interface{}, chan interface{}, OperationExecutor) {
	tCtx := ktesting.Init(t)
	ch, quit := make(chan interface{}, maxConcurrentOperations), make(chan interface{})
	return tCtx, ch, quit, NewOperationExecutor(newFakeOperationGenerator(ch, quit))
}

// startOperationAndBlock 先向 ch 写入表示操作已启动，然后阻塞直到 quit 关闭。
func startOperationAndBlock(ch chan<- interface{}, quit <-chan interface{}) {
	select {
	case ch <- nil:
		<-quit
	case <-quit:
	}
}
