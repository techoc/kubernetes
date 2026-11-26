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

package testing

import (
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
)

// Fake 实现 client.Interface，供嵌入结构体以获得默认实现，这样只需覆写待测方法即可。
type Fake struct {
	// 内嵌读写锁：reactor 链和 action 记录都可能被并发访问。
	sync.RWMutex
	actions []Action // these may be castable to other types, but "Action" is the minimum

	// ReactionChain 是按顺序尝试的 reactor 列表。
	ReactionChain []Reactor
	// WatchReactionChain 是按顺序尝试的 watch reactor 列表。
	WatchReactionChain []WatchReactor
	// ProxyReactionChain 是按顺序尝试的 proxy reactor 列表。
	ProxyReactionChain []ProxyReactor

	// Resources 供 fake discovery 返回的 API 资源列表；为 nil 时按需构造。
	Resources []*metav1.APIResourceList
}

// Reactor 用于组合响应函数。
type Reactor interface {
	// Handles 判断该 Reactor 是否处理给定 action。
	Handles(action Action) bool
	// React 处理 action 并返回结果，可通过 handled=false 交给下一个 reactor。
	React(action Action) (handled bool, ret runtime.Object, err error)
}

// WatchReactor 用于组合 watch 响应函数。
type WatchReactor interface {
	// Handles 判断该 Reactor 是否处理给定 action。
	Handles(action Action) bool
	// React 处理 watch action 并返回结果，可通过 handled=false 交给下一个 reactor。
	React(action Action) (handled bool, ret watch.Interface, err error)
}

// ProxyReactor 用于组合 proxy GET 响应函数。
type ProxyReactor interface {
	// Handles 判断该 Reactor 是否处理给定 action。
	Handles(action Action) bool
	// React 处理 watch action 并返回结果，可通过 handled=false 交给下一个 reactor。
	React(action Action) (handled bool, ret restclient.ResponseWrapper, err error)
}

// ReactionFunc 按 Action 返回对象或错误。handled 为 false 时测试客户端忽略结果，继续尝试下一个函数。
// 可通过 action 的 GetSubresource() 为子资源定义不同响应。
type ReactionFunc func(action Action) (handled bool, ret runtime.Object, err error)

// WatchReactionFunc 返回 watch 接口；handled 为 false 时继续尝试下一个函数。
type WatchReactionFunc func(action Action) (handled bool, ret watch.Interface, err error)

// ProxyReactionFunc 按 Action 返回 ResponseWrapper；handled 为 false 时继续尝试下一个函数。
type ProxyReactionFunc func(action Action) (handled bool, ret restclient.ResponseWrapper, err error)

// AddReactor 把 reactor 追加到链尾。
// 后添加的 reactor 优先级更低：链前面的 reactor 先处理。
func (c *Fake) AddReactor(verb, resource string, reaction ReactionFunc) {
	c.ReactionChain = append(c.ReactionChain, &SimpleReactor{verb, resource, reaction})
}

// PrependReactor 把 reactor 插入链首。
func (c *Fake) PrependReactor(verb, resource string, reaction ReactionFunc) {
	c.ReactionChain = append([]Reactor{&SimpleReactor{verb, resource, reaction}}, c.ReactionChain...)
}

// AddWatchReactor 把 watch reactor 追加到链尾。
func (c *Fake) AddWatchReactor(resource string, reaction WatchReactionFunc) {
	c.Lock()
	defer c.Unlock()
	c.WatchReactionChain = append(c.WatchReactionChain, &SimpleWatchReactor{resource, reaction})
}

// PrependWatchReactor 把 watch reactor 插入链首。
func (c *Fake) PrependWatchReactor(resource string, reaction WatchReactionFunc) {
	c.Lock()
	defer c.Unlock()
	c.WatchReactionChain = append([]WatchReactor{&SimpleWatchReactor{resource, reaction}}, c.WatchReactionChain...)
}

// AddProxyReactor 把 proxy reactor 追加到链尾。
func (c *Fake) AddProxyReactor(resource string, reaction ProxyReactionFunc) {
	c.ProxyReactionChain = append(c.ProxyReactionChain, &SimpleProxyReactor{resource, reaction})
}

// PrependProxyReactor 把 proxy reactor 插入链首。
func (c *Fake) PrependProxyReactor(resource string, reaction ProxyReactionFunc) {
	c.ProxyReactionChain = append([]ProxyReactor{&SimpleProxyReactor{resource, reaction}}, c.ProxyReactionChain...)
}

// Invokes 记录 Action 并调用可处理它的 ReactionFunc；defaultReturnObj 应与正常调用的返回类型一致。
func (c *Fake) Invokes(action Action, defaultReturnObj runtime.Object) (runtime.Object, error) {
	c.Lock()
	defer c.Unlock()

	// 记录与 reactor 都使用深拷贝：防止 reactor 或调用方事后修改同一对象导致
	// 断言结果不稳定。
	actionCopy := action.DeepCopy()
	c.actions = append(c.actions, action.DeepCopy())
	for _, reactor := range c.ReactionChain {
		if !reactor.Handles(actionCopy) {
			continue
		}

		handled, ret, err := reactor.React(actionCopy)
		if !handled {
			continue
		}

		return ret, err
	}

	// 没有 reactor 处理时返回调用方给的默认对象，模拟“成功但无特定行为”。
	return defaultReturnObj, nil
}

// InvokesWatch 记录 Action 并调用可处理它的 ReactionFunc。
func (c *Fake) InvokesWatch(action Action) (watch.Interface, error) {
	c.Lock()
	defer c.Unlock()

	actionCopy := action.DeepCopy()
	c.actions = append(c.actions, action.DeepCopy())
	for _, reactor := range c.WatchReactionChain {
		if !reactor.Handles(actionCopy) {
			continue
		}

		handled, ret, err := reactor.React(actionCopy)
		if !handled {
			continue
		}

		return ret, err
	}

	return nil, fmt.Errorf("unhandled watch: %#v", action)
}

// InvokesProxy 记录 Action 并调用可处理它的 ReactionFunc。
func (c *Fake) InvokesProxy(action Action) restclient.ResponseWrapper {
	c.Lock()
	defer c.Unlock()

	actionCopy := action.DeepCopy()
	c.actions = append(c.actions, action.DeepCopy())
	for _, reactor := range c.ProxyReactionChain {
		if !reactor.Handles(actionCopy) {
			continue
		}

		// 代理 reactor 出错时静默跳过，由调用方以 nil 响应判定未处理。
		handled, ret, err := reactor.React(actionCopy)
		if !handled || err != nil {
			continue
		}

		return ret
	}

	return nil
}

// ClearActions 清空 fake 客户端记录的 action 历史。
func (c *Fake) ClearActions() {
	c.Lock()
	defer c.Unlock()

	c.actions = make([]Action, 0)
}

// Actions 按时间顺序返回 fake 客户端收到的 action。
func (c *Fake) Actions() []Action {
	c.RLock()
	defer c.RUnlock()
	// 返回副本，避免调用方在锁外修改内部切片。
	fa := make([]Action, len(c.actions))
	copy(fa, c.actions)
	return fa
}
