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

package healthz

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/server/httplog"
	"k8s.io/component-base/metrics/prometheus/slis"
	"k8s.io/klog/v2"
)

const DefaultHealthzPath = "/healthz"

// HealthChecker 是带名称的 healthz 检查项。
//
// Name 会出现在 URL（/healthz/<name>）与排除列表（?exclude=<name>）中，
// 因此必须在同一 handler 内唯一且稳定。
type HealthChecker interface {
	Name() string
	Check(req *http.Request) error
}

// GroupedHealthChecker 支持按组排除检查项，便于批量禁用相关检查。
type GroupedHealthChecker interface {
	HealthChecker
	GroupName() string
}

// PingHealthz 在被检查时自动返回 true。
var PingHealthz HealthChecker = ping{}

// ping 实现最简单的 healthz 检查。
type ping struct{}

// Name 返回固定名称 "ping"。
func (ping) Name() string {
	return "ping"
}

// PingHealthz 是始终返回 true 的健康检查。
func (ping) Check(_ *http.Request) error {
	return nil
}

// 日志未被阻塞时 LogHealthz 返回 true。
var LogHealthz HealthChecker = &log{}

// log 通过周期性 Flush 检测日志管道是否卡死。
type log struct {
	startOnce    sync.Once
	lastVerified atomic.Value
}

// Name 返回固定名称 "log"。
func (l *log) Name() string {
	return "log"
}

// Check 在首次调用时启动后台 Flush 循环，并判断最近一次成功 Flush 的时间。
// 超过 2 分钟未成功说明日志后端阻塞，健康检查应失败以触发外部告警。
func (l *log) Check(_ *http.Request) error {
	l.startOnce.Do(func() {
		l.lastVerified.Store(time.Now())
		go wait.Forever(func() {
			klog.Flush()
			l.lastVerified.Store(time.Now())
		}, time.Minute)
	})

	lastVerified := l.lastVerified.Load().(time.Time)
	if time.Since(lastVerified) < (2 * time.Minute) {
		return nil
	}
	return fmt.Errorf("logging blocked")
}

// cacheSyncWaiter 是 informer 缓存同步的最小接口，便于测试注入。
type cacheSyncWaiter interface {
	WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool
}

// informerSync 检查所有 informer 是否完成首次同步。
type informerSync struct {
	cacheSyncWaiter cacheSyncWaiter
}

var _ HealthChecker = &informerSync{}

// NewInformerSyncHealthz 返回一个 HealthChecker，仅当给定 cacheSyncWaiter 中所有 informer 都同步时才通过。
func NewInformerSyncHealthz(cacheSyncWaiter cacheSyncWaiter) HealthChecker {
	return &informerSync{
		cacheSyncWaiter: cacheSyncWaiter,
	}
}

// Name 返回固定名称 "informer-sync"。
func (i *informerSync) Name() string {
	return "informer-sync"
}

// shutdown 在 stopCh 关闭后报告不健康，用于优雅退出期间快速摘流。
type shutdown struct {
	stopCh <-chan struct{}
}

// NewShutdownHealthz 返回一个 HealthChecker，内嵌 channel 关闭时检查失败。
// 用于在优雅退出过程中让流量尽快摘除。
func NewShutdownHealthz(stopCh <-chan struct{}) HealthChecker {
	return &shutdown{stopCh}
}

// Name 返回固定名称 "shutdown"。
func (s *shutdown) Name() string {
	return "shutdown"
}

// Check 在 stopCh 已关闭时返回错误，提示负载均衡器停止转发流量。
func (s *shutdown) Check(req *http.Request) error {
	select {
	case <-s.stopCh:
		return fmt.Errorf("process is shutting down")
	default:
	}
	return nil
}

// Check 立即返回当前同步状态而不阻塞等待。
// 已关闭的 stopCh 让 WaitForCacheSync 立刻给出快照，因此健康检查本身不会挂起。
func (i *informerSync) Check(_ *http.Request) error {
	stopCh := make(chan struct{})
	// 关闭 stopCh 以立即检查 informer 是否已同步。
	close(stopCh)

	informersByStarted := make(map[bool][]string)
	for informerType, started := range i.cacheSyncWaiter.WaitForCacheSync(stopCh) {
		informersByStarted[started] = append(informersByStarted[started], informerType.String())
	}

	if notStarted := informersByStarted[false]; len(notStarted) > 0 {
		return fmt.Errorf("%d informers not started yet: %v", len(notStarted), notStarted)
	}
	return nil
}

// NamedCheck 返回给定名称与函数对应的 healthz 检查项。
func NamedCheck(name string, check func(r *http.Request) error) HealthChecker {
	return &healthzCheck{name, check}
}

// NamedGroupedCheck 返回给定名称与函数对应的 healthz 检查项。
//
// 分组检查既能按 name 单独排除，也能按 groupName 整组排除。
func NamedGroupedCheck(name string, groupName string, check func(r *http.Request) error) HealthChecker {
	return &groupedHealthzCheck{
		groupName:     groupName,
		HealthChecker: &healthzCheck{name, check},
	}
}

// InstallHandler 在 mux 的 "/healthz" 路径注册健康检查处理器。
// 同一 mux 的所有处理器必须在一次 InstallHandler 调用中全部指定。
// 对同一 mux 多次调用 InstallHandler 会 panic。
// 对同一 mux 多次调用会 panic。
func InstallHandler(mux mux, checks ...HealthChecker) {
	InstallPathHandler(mux, DefaultHealthzPath, checks...)
}

// InstallReadyzHandler 在 mux 的 "/readyz" 路径注册健康检查处理器。
// 同一 mux 的所有处理器必须在一次 InstallHandler 调用中全部指定。
// 对同一 mux 多次调用 InstallHandler 会 panic。
// 对同一 mux 多次调用会 panic。
func InstallReadyzHandler(mux mux, checks ...HealthChecker) {
	InstallPathHandler(mux, "/readyz", checks...)
}

// InstallLivezHandler 在 mux 的 "/livez" 路径注册存活检查处理器。
// 同一 mux 的所有处理器必须在一次 InstallHandler 调用中全部指定。
// 对同一 mux 多次调用 InstallHandler 会 panic。
// 对同一 mux 多次调用会 panic。
func InstallLivezHandler(mux mux, checks ...HealthChecker) {
	InstallPathHandler(mux, "/livez", checks...)
}

// InstallPathHandler 在 mux 的指定路径注册健康检查处理器。
// 同一路径的所有处理器必须在一次 InstallPathHandler 调用中全部指定。
// 对同一路径与 mux 多次调用 InstallPathHandler 会 panic。
// 对同一路径与 mux 多次调用 InstallPathHandler
// 会 panic。
func InstallPathHandler(mux mux, path string, checks ...HealthChecker) {
	InstallPathHandlerWithHealthyFunc(mux, path, nil, checks...)
}

// InstallPathHandlerWithHealthyFunc 与 InstallPathHandler 类似，但会在处理器首次成功时
// 恰好调用一次 firstTimeHealthy。
//
// 根路径的响应只聚合全部检查，不泄漏细节；每个检查另有独立的
// <path>/<name> 端点，但注意这些端点会直接暴露检查错误信息，
// 因此通常只在需要时由调用方单独保护。
func InstallPathHandlerWithHealthyFunc(mux mux, path string, firstTimeHealthy func(), checks ...HealthChecker) {
	if len(checks) == 0 {
		klog.V(5).Info("No default health checks specified. Installing the ping handler.")
		checks = []HealthChecker{PingHealthz}
	}

	klog.V(5).Infof("Installing health checkers for (%v): %v", path, formatQuoted(checkerNames(checks...)...))

	// 从路径首段推导指标中的 name（"healthz"/"readyz"/"livez"）。
	name := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
	mux.Handle(path,
		metrics.InstrumentHandlerFunc("GET",
			/* group = */ "",
			/* version = */ "",
			/* resource = */ "",
			/* subresource = */ path,
			/* scope = */ "",
			/* component = */ "",
			/* deprecated */ false,
			/* removedRelease */ "",
			handleRootHealth(name, firstTimeHealthy, checks...)))
	for _, check := range checks {
		mux.Handle(fmt.Sprintf("%s/%v", path, check.Name()), adaptCheckToHandler(check.Check))
	}
}

// mux 描述 InstallHandler 所需方法的接口。
type mux interface {
	Handle(pattern string, handler http.Handler)
}

// healthzCheck 用任意名称与检查函数实现 HealthChecker。
type healthzCheck struct {
	name  string
	check func(r *http.Request) error
}

var _ HealthChecker = &healthzCheck{}

// Name 返回构造时指定的名称。
func (c *healthzCheck) Name() string {
	return c.name
}

// Check 委托给用户提供的检查函数。
func (c *healthzCheck) Check(r *http.Request) error {
	return c.check(r)
}

// groupedHealthzCheck 在普通检查之上附加组名。
type groupedHealthzCheck struct {
	groupName string
	HealthChecker
}

var _ GroupedHealthChecker = &groupedHealthzCheck{}

// GroupName 返回所属组的名称。
func (c *groupedHealthzCheck) GroupName() string {
	return c.groupName
}

// getExcludedChecks 从查询参数中提取要排除的健康检查名称。
func getExcludedChecks(r *http.Request) sets.String {
	checks, found := r.URL.Query()["exclude"]
	if found {
		return sets.NewString(checks...)
	}
	return sets.NewString()
}

// handleRootHealth 返回提供这些检查的 http.HandlerFunc。
//
// 安全边界：失败原因只写入日志，响应体中只给出 "reason withheld"。
// 该端点通常无需认证，详细错误可能泄漏内部状态。
func handleRootHealth(name string, firstTimeHealthy func(), checks ...HealthChecker) http.HandlerFunc {
	var notifyOnce sync.Once
	return func(w http.ResponseWriter, r *http.Request) {
		excluded := getExcludedChecks(r)
		unknownExcluded := excluded.Clone()
		// failedVerboseLogOutput 用于写日志，表示失败的详细信息。
		var failedVerboseLogOutput bytes.Buffer
		var failedChecks []string
		var individualCheckOutput bytes.Buffer
		for _, check := range checks {
			// 若指定排除该检查，则不做检查。
			if excluded.Has(check.Name()) {
				unknownExcluded.Delete(check.Name())
				fmt.Fprintf(&individualCheckOutput, "[+]%s excluded: ok\n", check.Name())
				continue
			}
			// 若是分组检查且要排除该组，则不做检查。
			if check, ok := check.(GroupedHealthChecker); ok && excluded.Has(check.GroupName()) {
				unknownExcluded.Delete(check.GroupName())
				fmt.Fprintf(&individualCheckOutput, "[+]%s excluded with %s: ok\n", check.Name(), check.GroupName())
				continue
			}
			if err := check.Check(r); err != nil {
				slis.ObserveHealthcheck(context.Background(), check.Name(), name, slis.Error)
				// 响应中不带错误，因为该端点是公开的。若需要更多细节，
				// 访问者应对详细检查拥有明确权限。
				fmt.Fprintf(&individualCheckOutput, "[-]%s failed: reason withheld\n", check.Name())
				// 但日志中需要详细信息。
				fmt.Fprintf(&failedVerboseLogOutput, "[-]%s failed: %v\n", check.Name(), err)
				failedChecks = append(failedChecks, check.Name())
			} else {
				slis.ObserveHealthcheck(context.Background(), check.Name(), name, slis.Success)
				fmt.Fprintf(&individualCheckOutput, "[+]%s ok\n", check.Name())
			}
		}
		// 排除不存在的检查名不视为失败，只给出警告，避免拼写错误导致误判为不健康。
		if unknownExcluded.Len() > 0 {
			fmt.Fprintf(&individualCheckOutput, "warn: some health checks cannot be excluded: no matches for %s\n", formatQuoted(unknownExcluded.List()...))
			klog.V(6).Infof("cannot exclude some health checks, no health checks are installed matching %s",
				formatQuoted(unknownExcluded.List()...))
		}
		// 失败时始终输出详细信息。
		if len(failedChecks) > 0 {
			klog.V(2).Infof("%s check failed: %s\n%v", strings.Join(failedChecks, ","), name, failedVerboseLogOutput.String())
			// 健康检查失败是预期内的状态报告，不应打印堆栈。
			httplog.SetStacktracePredicate(r.Context(), func(int) bool { return false })
			http.Error(w, fmt.Sprintf("%s%s check failed", individualCheckOutput.String(), name), http.StatusInternalServerError)
			return
		}

		// 标记首次变为健康。
		if firstTimeHealthy != nil {
			notifyOnce.Do(firstTimeHealthy)
		}

		// 显式声明纯文本并加 nosniff，防止浏览器把响应当作可执行内容解析。
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		// 默认只回 "ok"：kubelet 等探针不关心细节，缩短响应也减少信息暴露。
		if _, found := r.URL.Query()["verbose"]; !found {
			fmt.Fprint(w, "ok")
			return
		}

		individualCheckOutput.WriteTo(w)
		fmt.Fprintf(w, "%s check passed\n", name)
	}
}

// adaptCheckToHandler 返回提供这些检查的 http.HandlerFunc。
//
// 注意与根路径不同：单检查端点会回显错误详情，因此不应无条件暴露给匿名用户。
func adaptCheckToHandler(c func(r *http.Request) error) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := c(r)
		if err != nil {
			http.Error(w, fmt.Sprintf("internal server error: %v", err), http.StatusInternalServerError)
		} else {
			fmt.Fprint(w, "ok")
		}
	})
}

// checkerNames 按传入顺序返回检查项的名称。
func checkerNames(checks ...HealthChecker) []string {
	// 收集检查项名称以便输出。
	checkerNames := make([]string, 0, len(checks))
	for _, check := range checks {
		checkerNames = append(checkerNames, check.Name())
	}
	return checkerNames
}

// formatQuoted 返回格式化后的健康检查名称字符串，
// 保留传入顺序。
//
// 保留传入顺序是为了让日志与错误输出稳定、可 diff。
func formatQuoted(names ...string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}
	return strings.Join(quoted, ",")
}
