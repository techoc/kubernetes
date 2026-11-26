/*
Copyright 2016 The Kubernetes Authors.

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

package mux

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/klog/v2"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
)

// PathRecorderMux 包装 mux 并记录已注册的 exposedPaths。
//
// PathRecorderMux 在标准 http 路由之上记录已注册路径，用于 / 索引页与
// 启动完成检测。注册通过锁保护并整表重建后原子发布，保证并发请求始终
// 看到一致的 handler 集合。
type PathRecorderMux struct {
	// name 用于日志，便于追踪请求经过的 mux
	name string

	lock            sync.Mutex
	notFoundHandler http.Handler
	pathToHandler   map[string]http.Handler
	prefixToHandler map[string]http.Handler

	// mux 保存 pathHandler，负责实际的服务分发。
	// 我们希望接受结尾斜杠，但不希望因此匹配其下所有路径。
	//
	// 这里只做精确匹配，除非显式要求其他行为
	//
	// mux 保存当前生效的只读路由快照，用 atomic.Value 实现无锁读取；
	// 注册/注销时整体替换而不是原地修改。
	mux atomic.Value

	// exposedPaths 是应在 / 展示的路径列表
	exposedPaths []string

	// pathStacks 保存所有注册路径的调用栈，以便在
	// "http: multiple registrations for %s" panic 前给出更有用的信息。
	pathStacks map[string]string
}

// pathHandler 先按精确匹配、再按前缀匹配、最后交给 notFoundHandler
//
// pathHandler 是一次性构建、之后只读的路由表：先精确匹配，再按优先级
// 尝试前缀，最后落到 notFoundHandler。
type pathHandler struct {
	// muxName 用于日志，便于追踪请求经过的 mux
	muxName string

	// pathToHandler 是精确路径到 handler 的映射
	pathToHandler map[string]http.Handler

	// 必须先按斜杠数、再按长度排序
	prefixHandlers []prefixHandler

	// notFoundHandler 处理没有其他匹配的请求
	notFoundHandler http.Handler
}

// prefixHandler 保存要匹配的前缀与对应 handler
type prefixHandler struct {
	// prefix 是用于匹配请求的前缀
	prefix string
	// handler 处理匹配的请求
	handler http.Handler
}

// NewPathRecorderMux 创建新的 PathRecorderMux
//
// NewPathRecorderMux 初始化为只返回 404 的路由表；name 会出现在
// 调试日志中，便于定位是哪个 mux 处理了请求。
func NewPathRecorderMux(name string) *PathRecorderMux {
	ret := &PathRecorderMux{
		name:            name,
		pathToHandler:   map[string]http.Handler{},
		prefixToHandler: map[string]http.Handler{},
		mux:             atomic.Value{},
		exposedPaths:    []string{},
		pathStacks:      map[string]string{},
	}

	ret.mux.Store(&pathHandler{notFoundHandler: http.NotFoundHandler()})
	return ret
}

// ListedPaths 返回已注册 handler 的 exposedPaths。
//
// ListedPaths 返回排序后的已暴露路径副本；复制后解锁，避免调用方在
// 排序时持有锁或看到并发修改。
func (m *PathRecorderMux) ListedPaths() []string {
	m.lock.Lock()
	handledPaths := append([]string{}, m.exposedPaths...)
	m.lock.Unlock()

	sort.Strings(handledPaths)
	return handledPaths
}

// trackCallers 记录注册路径时的调用栈。重复注册在标准库中只会触发
// 难以定位的 panic，这里先记录两份栈，帮助找到冲突的注册方。
func (m *PathRecorderMux) trackCallers(path string) {
	stack := string(debug.Stack())
	if existingStack, ok := m.pathStacks[path]; ok {
		utilruntime.HandleError(fmt.Errorf("duplicate path registration of %q: original registration from %v\n\nnew registration from %v", path, existingStack, stack))
	}
	m.pathStacks[path] = stack
}

// refreshMuxLocked 创建新 mux，必须在持锁时调用，否则 handler 视图
// 可能不一致
//
// refreshMuxLocked 根据当前注册表构建新的只读快照并原子发布。前缀按
// byPrefixPriority 逆序排列，使"更具体"的前缀先被匹配。
func (m *PathRecorderMux) refreshMuxLocked() {
	newMux := &pathHandler{
		muxName:         m.name,
		pathToHandler:   map[string]http.Handler{},
		prefixHandlers:  []prefixHandler{},
		notFoundHandler: http.NotFoundHandler(),
	}
	if m.notFoundHandler != nil {
		newMux.notFoundHandler = m.notFoundHandler
	}
	for path, handler := range m.pathToHandler {
		newMux.pathToHandler[path] = handler
	}

	keys := sets.StringKeySet(m.prefixToHandler).List()
	sort.Sort(sort.Reverse(byPrefixPriority(keys)))
	for _, prefix := range keys {
		newMux.prefixHandlers = append(newMux.prefixHandlers, prefixHandler{
			prefix:  prefix,
			handler: m.prefixToHandler[prefix],
		})
	}

	m.mux.Store(newMux)
}

// NotFoundHandler 设置路径无匹配时使用的 handler
//
// NotFoundHandler 替换兜底 handler；启动完成检测依赖自定义的 not found
// handler 把 404 转换为 503。
func (m *PathRecorderMux) NotFoundHandler(notFoundHandler http.Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.notFoundHandler = notFoundHandler

	m.refreshMuxLocked()
}

// Unregister 从 mux 移除路径。
//
// Unregister 从精确与前缀注册表中删除路径，并同步移除索引页条目，
// 使注销后的路由不再对外可见。
func (m *PathRecorderMux) Unregister(path string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.pathToHandler, path)
	delete(m.prefixToHandler, path)
	delete(m.pathStacks, path)
	for i := range m.exposedPaths {
		if m.exposedPaths[i] == path {
			m.exposedPaths = append(m.exposedPaths[:i], m.exposedPaths[i+1:]...)
			break
		}
	}

	m.refreshMuxLocked()
}

// Handle 为给定 pattern 注册 handler。
// pattern 已有 handler 时 Handle 会 panic。
//
// Handle 注册精确匹配路由并列入索引页。
func (m *PathRecorderMux) Handle(path string, handler http.Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.trackCallers(path)

	m.exposedPaths = append(m.exposedPaths, path)
	m.pathToHandler[path] = handler
	m.refreshMuxLocked()
}

// HandleFunc 为给定 pattern 注册 handler 函数。
// pattern 已有 handler 时 Handle 会 panic。
func (m *PathRecorderMux) HandleFunc(path string, handler func(http.ResponseWriter, *http.Request)) {
	m.Handle(path, http.HandlerFunc(handler))
}

// UnlistedHandle 注册 handler 但不列入列表。
// pattern 已有 handler 时 Handle 会 panic。
//
// UnlistedHandle 注册路由但不列入索引页，用于内部/敏感路径（如
func (m *PathRecorderMux) UnlistedHandle(path string, handler http.Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.trackCallers(path)

	m.pathToHandler[path] = handler
	m.refreshMuxLocked()
}

// UnlistedHandleFunc 为给定 pattern 注册 handler 函数但不列入列表。
// pattern 已有 handler 时 Handle 会 panic。
func (m *PathRecorderMux) UnlistedHandleFunc(path string, handler func(http.ResponseWriter, *http.Request)) {
	m.UnlistedHandle(path, http.HandlerFunc(handler))
}

// HandlePrefix 类似 Handle，但匹配路径下的所有内容，类似 Go 标准库结尾斜杠语义。
//
// HandlePrefix 要求路径以 / 结尾：前缀匹配语义明确，且避免 /foo 误匹配
// /foobar 之类的路径。
func (m *PathRecorderMux) HandlePrefix(path string, handler http.Handler) {
	if !strings.HasSuffix(path, "/") {
		panic(fmt.Sprintf("%q must end in a trailing slash", path))
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	m.trackCallers(path)

	m.exposedPaths = append(m.exposedPaths, path)
	m.prefixToHandler[path] = handler
	m.refreshMuxLocked()
}

// UnlistedHandlePrefix 类似 UnlistedHandle，但匹配路径下的所有内容，类似 Go 标准库结尾斜杠语义。
func (m *PathRecorderMux) UnlistedHandlePrefix(path string, handler http.Handler) {
	if !strings.HasSuffix(path, "/") {
		panic(fmt.Sprintf("%q must end in a trailing slash", path))
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	m.trackCallers(path)

	m.prefixToHandler[path] = handler
	m.refreshMuxLocked()
}

// ServeHTTP 使其实现 http.Handler
//
// ServeHTTP 读取原子快照，因此请求路径上无需加锁。
func (m *PathRecorderMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mux.Load().(*pathHandler).ServeHTTP(w, r)
}

// ServeHTTP 使其实现 http.Handler
//
// ServeHTTP 按精确匹配、前缀匹配、兜底 handler 的顺序处理请求；
// 精确匹配优先于任何前缀，保证 /healthz 不会被 / 前缀截获。
func (h *pathHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if exactHandler, ok := h.pathToHandler[r.URL.Path]; ok {
		klog.V(5).Infof("%v: %q satisfied by exact match", h.muxName, r.URL.Path)
		exactHandler.ServeHTTP(w, r)
		return
	}

	for _, prefixHandler := range h.prefixHandlers {
		if strings.HasPrefix(r.URL.Path, prefixHandler.prefix) {
			klog.V(5).Infof("%v: %q satisfied by prefix %v", h.muxName, r.URL.Path, prefixHandler.prefix)
			prefixHandler.handler.ServeHTTP(w, r)
			return
		}
	}

	klog.V(5).Infof("%v: %q satisfied by NotFoundHandler", h.muxName, r.URL.Path)
	h.notFoundHandler.ServeHTTP(w, r)
}

// byPrefixPriority 按 mux 应测试的顺序排序 URL 前缀
// 必须按斜杠数再按长度排序，这样顺序遍历时
// 先匹配到"最佳"前缀。
//
// byPrefixPriority 的排序规则：斜杠更多者优先（层级更深），其次字符串
// 更长者优先，最后按字典序。配合 ServeHTTP 的线性扫描即可让最具体的
// 前缀先匹配。
type byPrefixPriority []string

func (s byPrefixPriority) Len() int      { return len(s) }
func (s byPrefixPriority) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byPrefixPriority) Less(i, j int) bool {
	lhsNumParts := strings.Count(s[i], "/")
	rhsNumParts := strings.Count(s[j], "/")
	if lhsNumParts != rhsNumParts {
		return lhsNumParts < rhsNumParts
	}

	lhsLen := len(s[i])
	rhsLen := len(s[j])
	if lhsLen != rhsLen {
		return lhsLen < rhsLen
	}

	return strings.Compare(s[i], s[j]) < 0
}
