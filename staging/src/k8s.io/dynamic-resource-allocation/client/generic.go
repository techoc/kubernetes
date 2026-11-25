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

package client

import (
	"context"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	types "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	drav1beta1 "k8s.io/dynamic-resource-allocation/api/v1beta1"
	drav1beta2 "k8s.io/dynamic-resource-allocation/api/v1beta2"
	"k8s.io/klog/v2"
)

// scheme 注册了需要参与类型转换的各 DRA API 版本，供 scheme.Convert 使用。
var scheme = runtime.NewScheme()

// init 向 scheme 注册 v1beta1 与 v1beta2 类型，注册失败则直接终止程序。
func init() {
	// 注册 v1beta1 类型，用于与最新版本双向转换。
	utilruntime.Must(drav1beta1.AddToScheme(scheme))
	// 注册 v1beta2 类型，用于与最新版本双向转换。
	utilruntime.Must(drav1beta2.AddToScheme(scheme))
}

// 上述泛型中：N 是最新版本的非指针类型，O/O2 是需要来回转换的旧版类型，
// P 系列为对应指针类型，L 为列表类型，AC 为 apply 配置类型。
// 随着 API 达到 GA，后续还会加入更多旧版类型参数。

// newConvertingClient 构造跨版本转换客户端，保存最新版与各旧版 API 的接口集合，
// 具体使用哪一组接口由调用时的当前 API 版本决定。
func newConvertingClient[NP objectPtr[N], N, NL, NAC any, OP objectPtr[O], O, OL, OAC any, O2P objectPtr[O2], O2, O2L, O2AC any](c *Client, native funcs[N, NL, NAC], v1beta1 funcs[O, OL, OAC], v1beta2 funcs[O2, O2L, O2AC]) *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC] {
	// 此处只做字段填充，不发起任何请求。
	return &convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]{
		c:       c,
		native:  native,
		v1beta1: v1beta1,
		v1beta2: v1beta2,
	}
}

// convertingClient 是资源接口的通用包装：对外暴露最新版 API 类型，
// 内部按当前 API 版本选择原生接口或旧版接口，并在必要时执行类型转换。
// 字段说明：c 提供并保存当前 API 版本；native 是最新版（v1）的接口方法集合；
// v1beta1 与 v1beta2 分别是两个旧版 API 的接口方法集合。
type convertingClient[NP objectPtr[N], N, NL, NAC any, OP objectPtr[O], O, OL, OAC any, O2P objectPtr[O2], O2, O2L, O2AC any] struct {
	c       *Client
	native  funcs[N, NL, NAC]
	v1beta1 funcs[O, OL, OAC]
	v1beta2 funcs[O2, O2L, O2AC]
}

// funcs 抽象某个 API 版本下资源客户端必须具备的创建、更新、删除、查询与监听方法，
// 使泛型代码可以用统一方式调用不同版本的 client-go 接口。
type funcs[T, TL, TAC any] interface {
	// Create 创建新对象。
	Create(context.Context, *T, metav1.CreateOptions) (*T, error)
	// Update 更新整个对象。
	Update(context.Context, *T, metav1.UpdateOptions) (*T, error)
	// Delete 按名称删除对象。
	Delete(context.Context, string, metav1.DeleteOptions) error
	// DeleteCollection 按条件批量删除对象。
	DeleteCollection(context.Context, metav1.DeleteOptions, metav1.ListOptions) error
	// Get 按名称获取对象。
	Get(context.Context, string, metav1.GetOptions) (*T, error)
	// List 列出对象。
	List(context.Context, metav1.ListOptions) (*TL, error)
	// Watch 监听对象变化。
	Watch(context.Context, metav1.ListOptions) (watch.Interface, error)
	// Apply 以 server-side apply 方式应用配置。
	Apply(context.Context, *TAC, metav1.ApplyOptions) (*T, error)
}

// funcsWithStatus 在 funcs 之上增加 status 子资源的更新能力。
type funcsWithStatus[T, TL, TAC any] interface {
	funcs[T, TL, TAC]
	// UpdateStatus 更新对象的 status 子资源。
	UpdateStatus(context.Context, *T, metav1.UpdateOptions) (*T, error)
	// ApplyStatus 以 server-side apply 方式更新 status 子资源。
	ApplyStatus(context.Context, *TAC, metav1.ApplyOptions) (*T, error)
}

// Create 创建资源：使用旧版 API 时先把最新版对象转换为旧版类型再发起请求，
// 并把响应转回最新版；使用最新 API 时直接调用原生接口。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) Create(ctx context.Context, obj *N, opts metav1.CreateOptions) (*N, error) {
	// 构造调用序列，遇到 NotFound 时会自动尝试下一个 API 版本。
	apis := newCall(t.c, func(currentAPI int32) (*N, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1：双向转换请求体与响应。
			return putWithConversion(obj, func(obj *O) (*O, error) {
				return t.v1beta1.Create(ctx, obj, opts)
			})
		case useV1beta2API:
			// 回退到 v1beta2：双向转换请求体与响应。
			return putWithConversion(obj, func(obj *O2) (*O2, error) {
				return t.v1beta2.Create(ctx, obj, opts)
			})
		default:
			// 当前为最新版 API，直接透传对象与选项。
			return t.native.Create(ctx, obj, opts)
		}
	})
	return apis.run()
}

// Update 更新资源：逻辑与 Create 相同，旧版 API 下双向转换，最新 API 下直接调用。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) Update(ctx context.Context, obj *N, opts metav1.UpdateOptions) (*N, error) {
	apis := newCall(t.c, func(currentAPI int32) (*N, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1：双向转换请求体与响应。
			return putWithConversion(obj, func(obj *O) (*O, error) {
				return t.v1beta1.Update(ctx, obj, opts)
			})
		case useV1beta2API:
			// 回退到 v1beta2：双向转换请求体与响应。
			return putWithConversion(obj, func(obj *O2) (*O2, error) {
				return t.v1beta2.Update(ctx, obj, opts)
			})
		default:
			// 最新版 API 直接透传。
			return t.native.Update(ctx, obj, opts)
		}
	})
	return apis.run()
}

// UpdateStatus 更新资源的 status 子资源：旧版 API 下需把接口断言为 funcsWithStatus，
// 并在调用前后执行类型双向转换。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) UpdateStatus(ctx context.Context, obj *N, opts metav1.UpdateOptions) (*N, error) {
	apis := newCall(t.c, func(currentAPI int32) (*N, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1：断言状态接口并双向转换。
			return putWithConversion(obj, func(obj *O) (*O, error) {
				return t.v1beta1.(funcsWithStatus[O, OL, OAC]).UpdateStatus(ctx, obj, opts)
			})
		case useV1beta2API:
			// 回退到 v1beta2：对 v1beta1 字段按 O2 类型断言状态接口并双向转换。
			return putWithConversion(obj, func(obj *O2) (*O2, error) {
				return t.v1beta1.(funcsWithStatus[O2, O2L, O2AC]).UpdateStatus(ctx, obj, opts)
			})
		default:
			// 最新版 API 直接调用其状态接口。
			return t.native.(funcsWithStatus[N, NL, NAC]).UpdateStatus(ctx, obj, opts)
		}
	})
	return apis.run()
}

// Delete 按名称删除资源：删除操作无需对象类型转换，直接按当前 API 版本调用对应接口。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	apis := newCall(t.c, func(currentAPI int32) (*N, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1。
			return nil, t.v1beta1.Delete(ctx, name, opts)
		case useV1beta2API:
			// 回退到 v1beta2。
			return nil, t.v1beta2.Delete(ctx, name, opts)
		default:
			// 最新版 API。
			return nil, t.native.Delete(ctx, name, opts)
		}
	})
	// 删除只关心错误，回调返回的对象恒为 nil。
	_, err := apis.run()
	return err
}

// DeleteCollection 按条件批量删除资源：同样无需类型转换，按当前 API 版本透传选项。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	apis := newCall(t.c, func(currentAPI int32) (*N, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1。
			return nil, t.v1beta1.DeleteCollection(ctx, opts, listOpts)
		case useV1beta2API:
			// 回退到 v1beta2。
			return nil, t.v1beta2.DeleteCollection(ctx, opts, listOpts)
		default:
			// 最新版 API。
			return nil, t.native.DeleteCollection(ctx, opts, listOpts)
		}
	})
	// 批量删除只关心错误。
	_, err := apis.run()
	return err
}

// Get 按名称获取资源：旧版 API 下调用旧版接口后，把响应对象转换为最新版类型。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) Get(ctx context.Context, name string, opts metav1.GetOptions) (*N, error) {
	apis := newCall(t.c, func(currentAPI int32) (*N, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1：只需把响应从旧版转换为最新版。
			return getWithConversion[N, O](func() (*O, error) {
				return t.v1beta1.Get(ctx, name, opts)
			})
		case useV1beta2API:
			// 回退到 v1beta2：只需把响应从旧版转换为最新版。
			return getWithConversion[N, O2](func() (*O2, error) {
				return t.v1beta2.Get(ctx, name, opts)
			})
		default:
			// 最新版 API 直接返回最新类型对象。
			return t.native.Get(ctx, name, opts)
		}
	})
	return apis.run()
}

// List 获取资源列表：旧版 API 下把返回的旧版列表对象转换为最新版本列表类型。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) List(ctx context.Context, opts metav1.ListOptions) (*NL, error) {
	apis := newCall(t.c, func(currentAPI int32) (*NL, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1：转换旧版列表对象。
			return getWithConversion[NL, OL](func() (*OL, error) {
				return t.v1beta1.List(ctx, opts)
			})
		case useV1beta2API:
			// 回退到 v1beta2：转换旧版列表对象。
			return getWithConversion[NL, O2L](func() (*O2L, error) {
				return t.v1beta2.List(ctx, opts)
			})
		default:
			// 最新版 API 直接返回最新列表类型。
			return t.native.List(ctx, opts)
		}
	})
	return apis.run()
}

// Watch 监听资源变化：旧版 API 下由 watchWithConversion 把事件中的旧版对象
// 转换成最新类型后再投递给消费者。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	apis := newCall(t.c, func(currentAPI int32) (watch.Interface, error) {
		switch currentAPI {
		case useV1beta1API:
			// 回退到 v1beta1：包装上游监听并转换事件对象。
			return watchWithConversion[NP, N, OP](func() (watch.Interface, error) {
				return t.v1beta1.Watch(ctx, opts)
			})
		case useV1beta2API:
			// 回退到 v1beta2：包装上游监听并转换事件对象。
			return watchWithConversion[NP, N, O2P](func() (watch.Interface, error) {
				return t.v1beta2.Watch(ctx, opts)
			})
		default:
			// 最新版 API 直接返回其原生监听。
			return t.native.Watch(ctx, opts)
		}
	})
	return apis.run()
}

// Patch 不受支持：补丁内容难以跨 API 版本转换，始终返回 ErrNotImplemented。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *N, err error) {
	return nil, ErrNotImplemented
}

// Apply 不受支持：需要额外代码转换 apply 配置，始终返回 ErrNotImplemented。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) Apply(ctx context.Context, obj *NAC, opts metav1.ApplyOptions) (result *N, err error) {
	return nil, ErrNotImplemented
}

// ApplyStatus 不受支持：同样需要转换 apply 配置，始终返回 ErrNotImplemented。
func (t *convertingClient[NP, N, NL, NAC, OP, O, OL, OAC, O2P, O2, O2L, O2AC]) ApplyStatus(ctx context.Context, obj *NAC, opts metav1.ApplyOptions) (result *N, err error) {
	return nil, ErrNotImplemented
}

// newCall 把一次 API 调用封装为 callSequence，记录客户端与按当前 API 编号执行的回调。
func newCall[N any](c *Client, call func(currentAPI int32) (N, error)) callSequence[N] {
	// 此阶段不发起请求，真正的版本轮换在 run 中完成。
	seq := callSequence[N]{
		c:    c,
		call: call,
	}
	return seq
}

// callSequence 描述一次会对各 API 版本依次尝试的调用。
// 字段说明：c 提供并保存当前 API 版本；call 是实际发起请求的函数，参数为要尝试的 API 编号。
type callSequence[N any] struct {
	c    *Client
	call func(currentAPI int32) (N, error)
}

// run 从客户端记录的当前版本开始尝试调用；遇到 NotFound 就轮换到下一个版本，
// 遍历完所有版本后放弃，并把最终使用的版本写回客户端。
func (s *callSequence[N]) run() (N, error) {
	// 从上次成功的版本开始，避免每次都从最新版重新探测。
	currentAPI := s.c.useAPI.Load()
	var value N
	var err error
	// 最多尝试 numAPIs 次，轮换一圈后回到起点。
	for i := int32(0); i < numAPIs; i++ {
		value, err = s.call(currentAPI)
		// NotFound 可能意味着该资源在此版本不存在，因此依次尝试其余所有 API 版本，直到回到起点。
		// 即使对象真的不存在，额外尝试的转换与请求开销也可接受。
		if apierrors.IsNotFound(err) {
			// 取模保证轮换到末尾后回到第一个版本。
			currentAPI = (currentAPI + 1) % numAPIs
			continue
		}
		// 成功或遇到其他错误时停止尝试。
		break
	}
	// 记录本次最终使用的版本，供后续调用复用。
	s.c.useAPI.Store(currentAPI)
	return value, err
}

// putWithConversion 处理请求体也需要转换的调用：先把最新版对象转成旧版类型发起请求，
// 再把旧版响应转回最新版类型。
func putWithConversion[N, O any](obj *N, call func(*O) (*O, error)) (*N, error) {
	// 创建旧版对象作为转换目标。
	out := new(O)
	if err := scheme.Convert(obj, out, nil); err != nil {
		// 最新版转换为旧版失败，直接返回错误。
		return nil, err
	}
	in, err := call(out)
	if err != nil {
		// 旧版接口调用失败，无需再转换响应。
		return nil, err
	}
	// 创建最新版对象承接响应。
	value := new(N)
	if err := scheme.Convert(in, value, nil); err != nil {
		// 旧版响应转换为最新版失败。
		return nil, err
	}
	return value, nil
}

// getWithConversion 处理只有响应需要转换的调用（Get/List）：
// 调用旧版接口拿到结果后，再把它转换为最新版本类型。
func getWithConversion[N, O any](call func() (*O, error)) (*N, error) {
	// 先调用旧版接口获取对象。
	in, err := call()
	if err != nil {
		return nil, err
	}
	// 创建最新版对象作为转换目标。
	value := new(N)
	if err := scheme.Convert(in, value, nil); err != nil {
		// 转换失败时返回错误，不返回半成品对象。
		return nil, err
	}
	return value, nil
}

// watchWithConversion 包装旧版 API 的监听：先建立上游 watch，
// 再启动后台 goroutine 把每个事件中的旧版对象转换为最新类型后转发。
func watchWithConversion[NP objectPtr[N], N any, OP runtime.Object](call func() (watch.Interface, error)) (watch.Interface, error) {
	// 建立上游监听，失败则直接返回错误。
	in, err := call()
	if err != nil {
		return nil, err
	}
	// 创建包装 watch：自带结果通道与停止通道。
	out := &watchSomething[NP, N, OP]{
		upstream:   in,
		resultChan: make(chan watch.Event),
		stopChan:   make(chan struct{}),
	}
	// 后台持续消费上游事件并转换后转发。
	go out.run() // TODO：接入 ctx
	return out, nil
}

// objectPtr 约束 T 为指针类型且实现 runtime.Object，
// 从而保证转换后的对象可以作为 watch 事件对外返回。
type objectPtr[T any] interface {
	*T
	runtime.Object
}

// watchSomething 实现 watch.Interface：从上游读取事件，将其中可识别的旧版对象
// 转换为最新类型后投递给下游消费者。
// 字段说明：upstream 是旧版 API 返回的原始监听；resultChan 向消费者投递转换后的事件；
// stopChan 在 Stop 时关闭以解除发送阻塞；stopOnce 保证停止通道只被关闭一次。
type watchSomething[NP objectPtr[N], N any, OP runtime.Object] struct {
	upstream   watch.Interface
	resultChan chan watch.Event
	stopChan   chan struct{}
	stopOnce   sync.Once
}

// Stop 停止上游监听，并幂等地关闭停止通道以唤醒转发 goroutine。
func (w *watchSomething[NP, N, OP]) Stop() {
	// 先停止上游，避免继续产生新事件。
	w.upstream.Stop()
	// 即使 Stop 被调用多次，也只关闭一次停止通道。
	w.stopOnce.Do(func() {
		close(w.stopChan)
	})
}

// ResultChan 返回投递转换后事件的输出通道（只读）。
func (w *watchSomething[NP, N, OP]) ResultChan() <-chan watch.Event {
	return w.resultChan
}

// run 在后台循环读取上游事件：对象可识别为旧版类型时执行转换，
// 随后把事件转发给下游；上游关闭或 Stop 被调用时结束转发。
func (w *watchSomething[NP, N, OP]) run() {
	// 防止 goroutine 中的 panic 导致进程崩溃。
	defer utilruntime.HandleCrash()
	// 退出时关闭结果通道，通知消费者监听已结束。
	defer close(w.resultChan)
	// 获取上游事件通道。
	resultChan := w.upstream.ResultChan()
	for {
		// 阻塞读取下一个事件；通道关闭表示上游先停止。
		e, ok := <-resultChan
		if !ok {
			// 上游已关闭，结束转发。
			break
		}
		// 只转换能识别为旧版类型的事件对象，其他对象原样转发。
		if in, ok := e.Object.(OP); ok {
			// 创建最新版对象作为转换目标。
			out := new(N)
			if err := scheme.Convert(in, out, nil); err != nil {
				//nolint:logcheck // 不应发生。
				klog.Error(err, "convert ResourceSlice")
			}
			// 保留事件类型，把对象替换为最新版指针类型。
			e = watch.Event{
				Type:   e.Type,
				Object: NP(out),
			}
		}
		// 发送时同时监听停止通道，避免消费者不再读取时 goroutine 永久阻塞。
		select {
		case w.resultChan <- e:
			// 正常投递转换后的事件。
		case <-w.stopChan:
			// 收到停止信号，放弃本次投递。
			break
		}
	}
}
