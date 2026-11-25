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

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/server/healthz"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// PostStartHookFunc is a function that is called after the server has started.
// It must properly handle cases like:
//  1. asynchronous start in multiple API server processes
//  2. conflicts between the different processes all trying to perform the same action
//  3. partially complete work (API server crashes while running your hook)
//  4. API server access **BEFORE** your hook has completed
//
// Think of it like a mini-controller that is super privileged and gets to run in-process
// If you use this feature, tag @deads2k on github who has promised to review code for anyone's PostStartHook
// until it becomes easier to use.
//
// hook 在独立 goroutine 中运行，失败会直接终止进程：它代表启动的必要前置条件，
// 不能带着不完整状态继续对外服务。
type PostStartHookFunc func(context PostStartHookContext) error

// PreShutdownHookFunc 是可以加入关闭逻辑的函数；
// 与 PostStartHook 不同，其 panic 会被捕获，错误也会被聚合返回而非终止进程。
type PreShutdownHookFunc func() error

// PostStartHookContext provides information about this API server to a PostStartHookFunc
type PostStartHookContext struct {
	// LoopbackClientConfig is a config for a privileged loopback connection to the API server
	LoopbackClientConfig *restclient.Config
	// Context gets cancelled when the server stops.
	context.Context
}

// PostStartHookProvider is an interface in addition to provide a post start hook for the api server
type PostStartHookProvider interface {
	PostStartHook() (string, PostStartHookFunc, error)
}

// postStartHookEntry 记录已注册的 hook 及其注册调用栈（用于重复注册时给出可诊断的错误）。
type postStartHookEntry struct {
	hook PostStartHookFunc
	// originatingStack holds the stack that registered postStartHooks. This allows us to show a more helpful message
	// for duplicate registration.
	originatingStack string

	// done will be closed when the postHook is finished
	done chan struct{}
}

// PostStartHookConfigEntry 是 PostStartHook 的只读配置视图，用于在注册后查询。
type PostStartHookConfigEntry struct {
	hook PostStartHookFunc
	// originatingStack holds the stack that registered postStartHooks. This allows us to show a more helpful message
	// for duplicate registration.
	originatingStack string
}

// preShutdownHookEntry 包装一个 PreShutdownHook。
type preShutdownHookEntry struct {
	hook PreShutdownHookFunc
}

// AddPostStartHook allows you to add a PostStartHook.
func (s *GenericAPIServer) AddPostStartHook(name string, hook PostStartHookFunc) error {
	if len(name) == 0 {
		return fmt.Errorf("missing name")
	}
	if hook == nil {
		return fmt.Errorf("hook func may not be nil: %q", name)
	}
	// 被显式禁用的 hook 静默跳过：这是特性门控/运维开关的正常路径，不应报错。
	if s.disabledPostStartHooks.Has(name) {
		klog.V(1).Infof("skipping %q because it was explicitly disabled", name)
		return nil
	}

	s.postStartHookLock.Lock()
	defer s.postStartHookLock.Unlock()

	// 一旦开始运行就不允许再注册，否则新 hook 将永远不会被执行。
	if s.postStartHooksCalled {
		return fmt.Errorf("unable to add %q because PostStartHooks have already been called", name)
	}
	if postStartHook, exists := s.postStartHooks[name]; exists {
		// this is programmer error, but it can be hard to debug
		return fmt.Errorf("unable to add %q because it was already registered by: %s", name, postStartHook.originatingStack)
	}

	// done is closed when the poststarthook is finished.  This is used by the health check to be able to indicate
	// that the poststarthook is finished
	done := make(chan struct{})
	// 注册健康检查后，/healthz 在 hook 完成前会一直失败，从而阻止流量过早进入。
	if err := s.AddBootSequenceHealthChecks(postStartHookHealthz{name: "poststarthook/" + name, done: done}); err != nil {
		return err
	}
	s.postStartHooks[name] = postStartHookEntry{hook: hook, originatingStack: string(debug.Stack()), done: done}

	return nil
}

// AddPostStartHookOrDie allows you to add a PostStartHook, but dies on failure
func (s *GenericAPIServer) AddPostStartHookOrDie(name string, hook PostStartHookFunc) {
	if err := s.AddPostStartHook(name, hook); err != nil {
		klog.Fatalf("Error registering PostStartHook %q: %v", name, err)
	}
}

// AddPreShutdownHook allows you to add a PreShutdownHook.
func (s *GenericAPIServer) AddPreShutdownHook(name string, hook PreShutdownHookFunc) error {
	if len(name) == 0 {
		return fmt.Errorf("missing name")
	}
	// nil hook 视为无操作，直接成功；与 PostStartHook 的严格校验不同，
	// 因为 pre-shutdown 钩子常由可选组件注册。
	if hook == nil {
		return nil
	}

	s.preShutdownHookLock.Lock()
	defer s.preShutdownHookLock.Unlock()

	if s.preShutdownHooksCalled {
		return fmt.Errorf("unable to add %q because PreShutdownHooks have already been called", name)
	}
	if _, exists := s.preShutdownHooks[name]; exists {
		return fmt.Errorf("unable to add %q because it is already registered", name)
	}

	s.preShutdownHooks[name] = preShutdownHookEntry{hook: hook}

	return nil
}

// AddPreShutdownHookOrDie allows you to add a PostStartHook, but dies on failure
func (s *GenericAPIServer) AddPreShutdownHookOrDie(name string, hook PreShutdownHookFunc) {
	if err := s.AddPreShutdownHook(name, hook); err != nil {
		klog.Fatalf("Error registering PreShutdownHook %q: %v", name, err)
	}
}

// RunPostStartHooks 运行该 server 的 PostStartHooks；
// 每个 hook 并发执行；持有锁直到所有 goroutine 启动完毕，避免与后续注册竞争。
func (s *GenericAPIServer) RunPostStartHooks(ctx context.Context) {
	s.postStartHookLock.Lock()
	defer s.postStartHookLock.Unlock()
	s.postStartHooksCalled = true

	context := PostStartHookContext{
		LoopbackClientConfig: s.LoopbackClientConfig,
		Context:              ctx,
	}

	for hookName, hookEntry := range s.postStartHooks {
		go runPostStartHook(hookName, hookEntry, context)
	}
}

// RunPreShutdownHooks 运行该 server 的 PreShutdownHooks；
// 串行执行并聚合所有错误，确保一个 hook 失败不会阻止其他清理逻辑运行。
func (s *GenericAPIServer) RunPreShutdownHooks() error {
	var errorList []error

	s.preShutdownHookLock.Lock()
	defer s.preShutdownHookLock.Unlock()
	s.preShutdownHooksCalled = true

	for hookName, hookEntry := range s.preShutdownHooks {
		if err := runPreShutdownHook(hookName, hookEntry); err != nil {
			errorList = append(errorList, err)
		}
	}
	return utilerrors.NewAggregate(errorList)
}

// isPostStartHookRegistered checks whether a given PostStartHook is registered
func (s *GenericAPIServer) isPostStartHookRegistered(name string) bool {
	s.postStartHookLock.Lock()
	defer s.postStartHookLock.Unlock()
	_, exists := s.postStartHooks[name]
	return exists
}

// runPostStartHook 执行单个 hook：panic 被转为错误，随后统一 Fatal，
// 使 panic 与普通失败具有相同的“启动失败”语义。
func runPostStartHook(name string, entry postStartHookEntry, context PostStartHookContext) {
	var err error
	func() {
		defer utilruntime.HandleCrashWithContext(context)
		err = entry.hook(context)
	}()
	// if the hook intentionally wants to kill server, let it.
	if err != nil {
		klog.Fatalf("PostStartHook %q failed: %v", name, err)
	}
	close(entry.done)
}

// runPreShutdownHook 执行单个 pre-shutdown hook，panic 被捕获并转为错误返回。
func runPreShutdownHook(name string, entry preShutdownHookEntry) error {
	var err error
	func() {
		// don't let the hook *accidentally* panic and kill the server
		defer utilruntime.HandleCrash()
		err = entry.hook()
	}()
	if err != nil {
		return fmt.Errorf("PreShutdownHook %q failed: %v", name, err)
	}
	return nil
}

// postStartHookHealthz implements a healthz check for poststarthooks.  It will return a "hookNotFinished"
// error until the poststarthook is finished.
type postStartHookHealthz struct {
	name string

	// done will be closed when the postStartHook is finished
	done chan struct{}
}

var _ healthz.HealthChecker = postStartHookHealthz{}

// Name 返回健康检查名称，形如 poststarthook/<hook>。
func (h postStartHookHealthz) Name() string {
	return h.name
}

// errHookNotFinished 表示 hook 尚未完成；作为固定错误值便于调用方识别。
var errHookNotFinished = errors.New("not finished")

// Check 非阻塞地判断 done 是否已关闭。
func (h postStartHookHealthz) Check(req *http.Request) error {
	select {
	case <-h.done:
		return nil
	default:
		return errHookNotFinished
	}
}
