/*
Copyright 2018 The Kubernetes Authors.

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

package polymorphichelpers

import (
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
)

// AllPodLogsForObjectFunc 定义获取 runtime.Object 全部 Pod 日志的函数类型。
type AllPodLogsForObjectFunc func(restClientGetter genericclioptions.RESTClientGetter, object, options runtime.Object, timeout time.Duration, allContainers bool) (map[v1.ObjectReference]rest.ResponseWrapper, error)

// AllPodLogsForObjectFn 便于在单元测试中替换该函数。
var AllPodLogsForObjectFn AllPodLogsForObjectFunc = allPodLogsForObject

// LogsForObjectFunc 定义获取 runtime.Object 日志的函数类型。
type LogsForObjectFunc func(restClientGetter genericclioptions.RESTClientGetter, object, options runtime.Object, timeout time.Duration, allContainers bool) (map[v1.ObjectReference]rest.ResponseWrapper, error)

// LogsForObjectFn 便于在单元测试中替换该函数。
var LogsForObjectFn LogsForObjectFunc = logsForObject

// AttachablePodForObjectFunc 定义获取对象可 attach 的 Pod 的函数类型。
type AttachablePodForObjectFunc func(restClientGetter genericclioptions.RESTClientGetter, object runtime.Object, timeout time.Duration) (*v1.Pod, error)

// AttachablePodForObjectFn 便于在单元测试中替换该函数。
var AttachablePodForObjectFn AttachablePodForObjectFunc = attachablePodForObject

// HistoryViewerFunc 定义查看变更历史的函数类型。
type HistoryViewerFunc func(restClientGetter genericclioptions.RESTClientGetter, mapping *meta.RESTMapping) (HistoryViewer, error)

// HistoryViewerFn 便于在单元测试中替换该函数。
var HistoryViewerFn HistoryViewerFunc = historyViewer

// StatusViewerFunc 定义输出 rollout 状态的函数类型。
type StatusViewerFunc func(mapping *meta.RESTMapping) (StatusViewer, error)

// StatusViewerFn 便于在单元测试中替换该函数。
var StatusViewerFn StatusViewerFunc = statusViewer

// 在对象支持的 PodSpec 上调用给定函数；
// 不支持 PodSpec 时返回 false，出错时返回错误。
type UpdatePodSpecForObjectFunc func(obj runtime.Object, fn func(*v1.PodSpec) error) (bool, error)

// UpdatePodSpecForObjectFn 便于在单元测试中替换该函数。
var UpdatePodSpecForObjectFn UpdatePodSpecForObjectFunc = updatePodSpecForObject

// 调用给定函数获取对象的 map 形式选择器；
// 不支持该对象时返回 ""，出错时返回错误。
type MapBasedSelectorForObjectFunc func(object runtime.Object) (string, error)

// MapBasedSelectorForObjectFn 便于在单元测试中替换该函数。
var MapBasedSelectorForObjectFn MapBasedSelectorForObjectFunc = mapBasedSelectorForObject

// 调用给定函数获取对象的协议；
// 对象没有协议时返回 nil map，出错时返回错误。
// Deprecated: use PortsProtocolsForObjectFunc instead.
// 同一端口使用不同协议时会丢失信息。
type ProtocolsForObjectFunc func(object runtime.Object) (map[string]string, error)

// ProtocolsForObjectFn 便于在单元测试中替换该函数。
// Deprecated: use MultiProtocolsForObjectFn instead.
var ProtocolsForObjectFn ProtocolsForObjectFunc = protocolsForObject

// 调用给定函数获取对象的多协议信息；
// 对象没有协议时返回 nil map，出错时返回错误。
type MultiProtocolsWithForObjectFunc func(object runtime.Object) (map[string][]string, error)

// MultiProtocolsForObjectFn 便于在单元测试中替换该函数。
var MultiProtocolsForObjectFn MultiProtocolsWithForObjectFunc = multiProtocolsForObject

// 返回给定对象关联的端口。
type PortsForObjectFunc func(object runtime.Object) ([]string, error)

// PortsForObjectFn 便于在单元测试中替换该函数。
var PortsForObjectFn PortsForObjectFunc = portsForObject

// CanBeExposedFunc 定义判断 GroupKind 是否支持 expose 的函数类型。
type CanBeExposedFunc func(kind schema.GroupKind) error

// CanBeExposedFn 便于在单元测试中替换该函数。
var CanBeExposedFn CanBeExposedFunc = canBeExposed

// ObjectPauserFunc 定义把 Info 中的对象标记为暂停的函数类型。
type ObjectPauserFunc func(runtime.Object) ([]byte, error)

// ObjectPauserFn 便于在单元测试中替换该函数。
// 返回 patch 后的对象字节，以及编码或对象已暂停时产生的错误。
var ObjectPauserFn ObjectPauserFunc = defaultObjectPauser

// ObjectResumerFunc 定义把 Info 中的对象标记为恢复的函数类型。
type ObjectResumerFunc func(runtime.Object) ([]byte, error)

// ObjectResumerFn 便于在单元测试中替换该函数。
// 返回 patch 后的对象字节，以及编码或对象已暂停时产生的错误。
var ObjectResumerFn ObjectResumerFunc = defaultObjectResumer

// RollbackerFunc 用于切换指定 RESTMapping 类型的回滚版本。
type RollbackerFunc func(restClientGetter genericclioptions.RESTClientGetter, mapping *meta.RESTMapping) (Rollbacker, error)

// RollbackerFn 便于在单元测试中替换该函数。
var RollbackerFn RollbackerFunc = rollbacker

// ObjectRestarterFunc 定义通过更新 Deployment 注解触发重启的函数类型。
type ObjectRestarterFunc func(runtime.Object) ([]byte, error)

// ObjectRestarterFn 便于在单元测试中替换该函数。
// 返回 patch 后的对象字节及编码错误。
var ObjectRestarterFn ObjectRestarterFunc = defaultObjectRestarter
