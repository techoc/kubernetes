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
	// ErrCallSkipped：调用被跳过、不会被执行时，由 APIDispatcher.Add 返回或发送到 OnFinish 通道。
	// 调用还没入队就被判定为没必要执行（例如合并后已是空操作）。
	// 它不是失败，而是正常节流的结果，所以单独成一类错误。
	ErrCallSkipped = errors.New("call skipped")
	// ErrCallOverwritten：已入队的调用被更新的或相关性更高的调用覆盖时，发送到 OnFinish 通道。
	// 已排队的调用被更新、更相关的调用取代时使用。
	// 保留通知是为了让调用方能区分“被取代”和“真的执行失败”，而不是把它当异常。
	ErrCallOverwritten = errors.New("call overwritten")
)

// IsUnexpectedError 在给定错误非 nil 且不属于预期的调度器生命周期错误（ErrCallSkipped、
// ErrCallOverwritten）时返回 true，可用于筛选出可能需要打日志或特殊处理的错误。
//
// 把生命周期噪音和实际故障分开：调用方通常只想为真正的问题打日志，
// 而不想为每次合并去重都记一条错误。集中在一个函数里，避免多处重复 errors.Is 判断。
func IsUnexpectedError(err error) bool {
	return err != nil && !errors.Is(err, ErrCallSkipped) && !errors.Is(err, ErrCallOverwritten)
}

// APICallType 定义调用类型名称，决定调度器如何处理同一对象上的多个待执行调用。
//
// 类型决定同一对象的两个调用是可合并，还是按相关性互相覆盖：
//   - 同一类型的调用应当合并。
//   - 类型不同的调用，按它们在 APICallRelevances 中的分值决定谁优先。
//
// 每个 APICall 实现应在同一个调度器内拥有唯一的类型。
//
// 用字符串类型而非枚举，让插件可以自定义 API 调用种类而不必改框架代码。
// 同一类型代表“可以合并”，这是后面去重逻辑的基础。
type APICallType string

// APICallRelevances 把所有可能的 APICallType 映射到一个相关性分值。
// 对同一个对象，相关性更高的 API 调用应覆盖更低的。
// 相同相关性的类型只应定义在不同的对象类型上。
//
// 把“相关性”抽成可配置的分值，是为了让框架不必知道每种 API 调用的业务含义。
// 同一个对象上来了冲突调用时，只比大小决定谁留下，从而保持调度器状态收敛。
type APICallRelevances map[APICallType]int

// APICall 定义可由 APIDispatcher 处理的 API 调用接口。
//
// 把“要做什么”（Execute）和“能不能合并/是否还有意义”（CallType、Merge、IsNoOp）拆开，
// 让调度器能在真正执行前反复整理队列。
// 只有这样，短时间内针对同一对象的大量事件才能被压缩成少量实际 API 请求。
type APICall interface {
	// CallType returns the type of the API call.
	// See the APICallType and APICallRelevances comments on how to define the APICallType.
	CallType() APICallType
	// UID 返回本调用所引用对象的 UID，用于识别同一对象的调用并进行合并或跳过。
	// 以对象 UID 而非名字作为键，避免对象删除重建后新对象误合并到旧调用上。
	UID() types.UID
	// Execute performs the actual API call.
	Execute(ctx context.Context, client clientset.Interface) error
	// Merge 把同一对象上较旧调用的状态合并进当前（接收方）调用。
	// 接收方应吸收 oldCall 中所有必要信息，因为 oldCall 随后会被丢弃。
	// 本方法调用后应检查 IsNoOp()，判断调用是否可以跳过。
	// 合并是把旧调用的信息吸进新调用，旧调用随后被丢弃。
	// 让调用自己实现合并，是因为“变化是否被抵消”只有了解业务语义的实现才知道。
	Merge(oldCall APICall) error
	// Sync 让本调用的状态与给定对象同步：可能把变更应用到对象上，
	// 或从对象存取后续执行所需的信息。若实现修改了对象，应返回对象的副本。
	// 本方法调用后应检查 IsNoOp()，判断调用是否可以跳过。
	// 同步解决的是“调用已排队但对象又变了”的问题：调用方看到的对象要带上待生效的变更，
	// 调用本身也能据最新对象判断请求是否已完成，从而避免多余写入。
	Sync(obj metav1.Object) (metav1.Object, error)
	// IsNoOp 在调用属于空操作、应被调度器跳过时返回 true。
	// 调用可能在创建时就是空操作，也可能在 Merge 或 Update 之后变成空操作。
	// 把“是否还要执行”作为显式查询，让调度器在队列整理的每一步都能尽早丢掉无效调用，
	// 而不必等到执行时才发现没意义。
	IsNoOp() bool
}

// APICallOptions 定义单次 API 调用的选项。
// 把回调通道放在“每次调用”的选项里而不是接口字段里，
// 是因为同一个 APICall 实现可能被不同调用方提交，各自关心的完成时机不同。
type APICallOptions struct {
	// OnFinish 是可选通道，用于接收一次调用生命周期的最终结果。
	//
	// 结果以非阻塞方式发送。如果该通道无缓冲且没有就绪的接收方，结果会被丢弃。
	// 非阻塞是刻意的：通知只是锦上添花，不能让调度器的异步执行卡在无人接收的回调上。
	//
	// 注意，收到错误并不能保证 API 调用本身被执行过。
	// 例如，发送的可能是 ErrCallOverwritten 或 ErrCallSkipped 错误。
	// 通道里收到错误不代表调用真的失败，合并去重也会走这里，所以接收方要按错误类型区分。
	//
	// 若不想接收结果，把该通道留为 nil。
	OnFinish chan<- error
}

// APIDispatcher 定义用于排队并异步执行 API 调用的调度器接口。
//
// 异步化是为了把高频事件与慢速 API 调用解耦：调度器先更新本地视图，真正写 API 的事延后批量做。
// 代价是本地视图和 API 状态之间短暂不一致，SyncObject 接口就是为了把这段窗口控制在可接受范围。
type APIDispatcher interface {
	// Add 把一个 API 调用加入调度器队列。若调用未入队（例如被跳过）则返回错误，
	// 调用方应处理可能返回的 ErrCallSkipped。
	// 把“是否入队”的结果同步返回，让调用方能立刻知道有没有必要继续后续流程，
	// 而完成通知则走异步通道，两者职责不同。
	Add(incomingAPICall APICall, opts APICallOptions) error
	// SyncObject 在给定的对象与调度器内部持有的待执行 API 调用之间做双向同步。
	// 调度器的事件处理器可以在对象更新时调用它，以同时丰富缓存状态和调用内容。
	//
	// 如果该对象存在待执行调用，本方法会：
	// 1. 把调用中待生效的变更应用到对象上，提供其状态的乐观预览。
	// 2. 允许调用从对象更新自己的内部状态，
	//    确保它在真正执行前拿到最新数据。
	//
	// 返回被修改后的对象。如果该对象没有待执行调用，原对象不做修改直接返回。
	// 双向同步是“乐观预览”：调用方先看到即将生效的状态，调用再据此修正自己的计划。
	// 没有待处理调用时原样返回，让事件处理路径无需额外分支判断。
	SyncObject(obj metav1.Object) (metav1.Object, error)
}
