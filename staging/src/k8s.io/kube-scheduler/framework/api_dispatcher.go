/*
Copyright 2025 The Kubernetes Authors.

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

package framework

import (
	"context"
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
)

var (
	// ErrCallSkipped is returned by APIDispatcher.Add or sent to the OnFinish channel when the call is skipped and will not be executed.
	// 调用还没入队就被判定为没必要执行（例如合并后已是空操作）。
	// 它不是失败，而是正常节流的结果，所以单独成一类错误。
	ErrCallSkipped = errors.New("call skipped")
	// ErrCallOverwritten is sent to the OnFinish channel when an enqueued call is overwritten by a newer or more relevant call.
	// 已排队的调用被更新、更相关的调用取代时使用。
	// 保留通知是为了让调用方能区分“被取代”和“真的执行失败”，而不是把它当异常。
	ErrCallOverwritten = errors.New("call overwritten")
)

// IsUnexpectedError returns true if the given error is not nil and is not one of the expected
// dispatcher lifecycle errors (ErrCallSkipped, ErrCallOverwritten). This can be used to
// filter for errors that may require logging or special handling.
//
// 把生命周期噪音和实际故障分开：调用方通常只想为真正的问题打日志，
// 而不想为每次合并去重都记一条错误。集中在一个函数里，避免多处重复 errors.Is 判断。
func IsUnexpectedError(err error) bool {
	return err != nil && !errors.Is(err, ErrCallSkipped) && !errors.Is(err, ErrCallOverwritten)
}

// APICallType defines a call type name that governs how the dispatcher handles multiple pending calls for the same object.
//
// The type determines if two calls for the same object are mergeable
// or if one should overwrite the other based on relevance:
//   - Calls with the same type should be merged.
//   - When calls have different types, their scores in APICallRelevances are used to determine precedence.
//
// Each APICall implementation should have a unique type within a given dispatcher.
//
// 用字符串类型而非枚举，让插件可以自定义 API 调用种类而不必改框架代码。
// 同一类型代表“可以合并”，这是后面去重逻辑的基础。
type APICallType string

// APICallRelevances maps all possible APICallTypes to a relevance value.
// A more relevant API call should overwrite a less relevant one for the same object.
// Types of the same relevance should only be defined for different object types.
//
// 把“相关性”抽成可配置的分值，是为了让框架不必知道每种 API 调用的业务含义。
// 同一个对象上来了冲突调用时，只比大小决定谁留下，从而保持调度器状态收敛。
type APICallRelevances map[APICallType]int

// APICall defines the interface for an API call that can be processed by an APIDispatcher.
//
// 把“要做什么”（Execute）和“能不能合并/是否还有意义”（CallType、Merge、IsNoOp）拆开，
// 让调度器能在真正执行前反复整理队列。
// 只有这样，短时间内针对同一对象的大量事件才能被压缩成少量实际 API 请求。
type APICall interface {
	// CallType returns the type of the API call.
	// See the APICallType and APICallRelevances comments on how to define the APICallType.
	CallType() APICallType
	// UID returns the UID of the object this call refers to.
	// This is used to identify and potentially merge or skip calls for the same object.
	// 以对象 UID 而非名字作为键，避免对象删除重建后新对象误合并到旧调用上。
	UID() types.UID
	// Execute performs the actual API call.
	Execute(ctx context.Context, client clientset.Interface) error
	// Merge merges the state of an older call for the same object into the current (receiver) call.
	// The receiver should incorporate all necessary information from oldCall, as oldCall will be discarded.
	// After this method is called, IsNoOp() should be checked to see if the call can be skipped.
	// 合并是把旧调用的信息吸进新调用，旧调用随后被丢弃。
	// 让调用自己实现合并，是因为“变化是否被抵消”只有了解业务语义的实现才知道。
	Merge(oldCall APICall) error
	// Sync synchronizes the state of this call with the given object.
	// It may apply changes to the object or store information from the object needed for later execution.
	// The implementation should return a copy of the object if it is modified.
	// After this method is called, IsNoOp() should be checked to see if the call can be skipped.
	// 同步解决的是“调用已排队但对象又变了”的问题：调用方看到的对象要带上待生效的变更，
	// 调用本身也能据最新对象判断请求是否已完成，从而避免多余写入。
	Sync(obj metav1.Object) (metav1.Object, error)
	// IsNoOp returns true if the call represents a no-operation and should be skipped by the dispatcher.
	// A call may be a no-op from its creation or become one after a Merge or Update.
	// 把“是否还要执行”作为显式查询，让调度器在队列整理的每一步都能尽早丢掉无效调用，
	// 而不必等到执行时才发现没意义。
	IsNoOp() bool
}

// APICallOptions defines options for an API call.
//
// 把回调通道放在“每次调用”的选项里而不是接口字段里，
// 是因为同一个 APICall 实现可能被不同调用方提交，各自关心的完成时机不同。
type APICallOptions struct {
	// OnFinish is an optional channel to receive the final result of a call's lifecycle.
	//
	// The result is sent in a non-blocking way. If this channel is unbuffered and has no
	// ready receiver, the result will be dropped.
	// 非阻塞是刻意的：通知只是锦上添花，不能让调度器的异步执行卡在无人接收的回调上。
	//
	// Note that receiving an error does not guarantee the API call itself was executed.
	// For instance, an ErrCallOverwritten or ErrCallSkipped error may be sent.
	// 通道里收到错误不代表调用真的失败，合并去重也会走这里，所以接收方要按错误类型区分。
	//
	// To opt out of receiving a result, leave this channel nil.
	OnFinish chan<- error
}

// APIDispatcher defines the interface for a dispatcher that queues and asynchronously executes API calls.
//
// 异步化是为了把高频事件与慢速 API 调用解耦：调度器先更新本地视图，真正写 API 的事延后批量做。
// 代价是本地视图和 API 状态之间短暂不一致，SyncObject 接口就是为了把这段窗口控制在可接受范围。
type APIDispatcher interface {
	// Add adds an API call to the dispatcher's queue. It returns an error if the call is not enqueued
	// (e.g., if it's skipped). The caller should handle ErrCallSkipped if returned.
	// 把“是否入队”的结果同步返回，让调用方能立刻知道有没有必要继续后续流程，
	// 而完成通知则走异步通道，两者职责不同。
	Add(incomingAPICall APICall, opts APICallOptions) error
	// SyncObject performs a two-way synchronization between the given object
	// and a pending API call held within the dispatcher.
	// This can be called by the scheduler's event handlers on object updates
	// to enrich the cached state and the call.
	//
	// If a call for the object exists there, this method:
	// 1. Applies the call's pending changes to the object, providing an optimistic preview of its state.
	// 2. Allows the call to update its own internal state from the object,
	//    ensuring it has the most recent data before its eventual execution.
	//
	// It returns the modified object. If no call is pending for the object,
	// the original object is returned unmodified.
	// 双向同步是“乐观预览”：调用方先看到即将生效的状态，调用再据此修正自己的计划。
	// 没有待处理调用时原样返回，让事件处理路径无需额外分支判断。
	SyncObject(obj metav1.Object) (metav1.Object, error)
}
