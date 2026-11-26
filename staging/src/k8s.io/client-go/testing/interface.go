/*
Copyright 2021 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
)

// FakeClient 是各类 fake 客户端（clientset、metadata、dynamic 等）的公共行为集合。
// reactor 链按顺序匹配，Prepend 的方法优先于 Add 的方法执行。
type FakeClient interface {
	// Tracker 暴露 fake 客户端内部的 ObjectTracker。
	Tracker() ObjectTracker

	// AddReactor 把 reactor 追加到链尾。
	AddReactor(verb, resource string, reaction ReactionFunc)

	// PrependReactor 把 reactor 插入链首。
	PrependReactor(verb, resource string, reaction ReactionFunc)

	// AddWatchReactor 把 watch reactor 追加到链尾。
	AddWatchReactor(resource string, reaction WatchReactionFunc)

	// PrependWatchReactor 把 watch reactor 插入链首。
	PrependWatchReactor(resource string, reaction WatchReactionFunc)

	// AddProxyReactor 把 proxy reactor 追加到链尾。
	AddProxyReactor(resource string, reaction ProxyReactionFunc)

	// PrependProxyReactor 把 proxy reactor 插入链首。
	PrependProxyReactor(resource string, reaction ProxyReactionFunc)

	// Invokes 记录 Action 并调用可处理它的 ReactionFunc；defaultReturnObj 应与正常调用的返回类型一致。
	Invokes(action Action, defaultReturnObj runtime.Object) (runtime.Object, error)

	// InvokesWatch 记录 Action 并调用可处理它的 ReactionFunc。
	InvokesWatch(action Action) (watch.Interface, error)

	// InvokesProxy 记录 Action 并调用可处理它的 ReactionFunc。
	InvokesProxy(action Action) restclient.ResponseWrapper

	// ClearActions 清空 fake 客户端记录的 action 历史。
	// 只清空记录，不影响 tracker 中的对象或 reactor。
	ClearActions()

	// Actions 按时间顺序返回 fake 客户端收到的 action。
	// 返回副本语义的实现应保证调用方修改切片不会影响内部记录。
	Actions() []Action
}
