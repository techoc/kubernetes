/*
Copyright 2017 The Kubernetes Authors.

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

// Package pager 把一次大 list 拆成多个分页请求，降低对 apiserver 与 etcd 的单次压力。
// 它支持并发预取若干页，并在分页游标过期时回退为一次性的完整 list。
package pager

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// 500 是服务端常见的分页上限；缓冲 10 页在吞吐与内存之间取折中。
const defaultPageSize = 500
const defaultPageBufferSize = 10

// ListPageFunc 根据给定列表选项返回一个列表对象。
type ListPageFunc func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error)

// SimplePageFunc 将不接受 context 的列表函数适配为接受 context 的列表函数。
// 保留该适配器是为了兼容只接受 ListOptions 的旧式 lister/list 实现。
func SimplePageFunc(fn func(opts metav1.ListOptions) (runtime.Object, error)) ListPageFunc {
	return func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		return fn(opts)
	}
}

// ListPager 帮助客户端把大型列表查询拆成不超过 PageSize 的多个小块。
// PageFn 应接受支持分页的 metav1.ListOptions 并返回列表。
// 分页器不会修改初始选项中的字段选择器和标签选择器。
type ListPager struct {
	PageSize int64
	PageFn   ListPageFunc

	// FullListIfExpired 决定分页游标过期时是否回退为完整 list；
	// 关闭它会让过期错误直接透传给调用方。
	FullListIfExpired bool

	// PageBufferSize 是后台预取的页数；0 表示完全串行，负值会在 EachListItem 中报错。
	PageBufferSize int32
}

// New 根据给定分页函数创建使用默认选项的分页器。
// 若遇到过期错误，分页器会作为最后手段回退为完整列表。
func New(fn ListPageFunc) *ListPager {
	return &ListPager{
		PageSize:          defaultPageSize,
		PageFn:            fn,
		FullListIfExpired: true,
		PageBufferSize:    defaultPageBufferSize,
	}
}

// TODO：引入其他类型的分页函数，例如按命名空间列表获取数据的函数。

// List 返回一个列表对象，但会尝试从服务端分块获取，以减小对服务端的影响。
// 若分块获取失败，则改为加载完整列表。options 的 Limit 未设置时，默认使用页大小。
//
// 若返回列表中的各项保留时间不同，且不希望任一项仍被引用时一直保留 p.PageFn
// 返回的整个切片，请使用 ListWithAlloc。
//
// 返回值中的 bool 表示结果是否来自分页读取（而非一次完整 list），调用方可用它判断
// 是否可能存在快照不一致的风险。
func (p *ListPager) List(ctx context.Context, options metav1.ListOptions) (runtime.Object, bool, error) {
	return p.list(ctx, options, false)
}

// ListWithAlloc 的用法与 List 相同，但不会保留对 p.PageFn 返回的条目切片的引用。
// 实现方式是对该切片中的非指针条目做浅拷贝。
//
// 若返回列表中的条目不被保留，或保留时间相同，为节省内存请改用 List。
func (p *ListPager) ListWithAlloc(ctx context.Context, options metav1.ListOptions) (runtime.Object, bool, error) {
	return p.list(ctx, options, true)
}

// allocNew 为 true 时逐项做浅拷贝，用于切断结果对整页切片的引用，避免长时间持有大对象。
func (p *ListPager) list(ctx context.Context, options metav1.ListOptions, allocNew bool) (runtime.Object, bool, error) {
	if options.Limit == 0 {
		options.Limit = p.PageSize
	}
	// 记录调用方原始的 RV 语义：回退完整 list 时必须恢复，否则会改变快照语义。
	requestedResourceVersion := options.ResourceVersion
	requestedResourceVersionMatch := options.ResourceVersionMatch
	var list *metainternalversion.List
	paginatedResult := false

	for {
		// 每轮都先检查取消，避免在长分页过程中忽略调用方的超时。
		select {
		case <-ctx.Done():
			return nil, paginatedResult, ctx.Err()
		default:
		}

		obj, err := p.PageFn(ctx, options)
		if err != nil {
			// 仅当错误为 Expired、FullListIfExpired 为真且错误发生在第二页或更晚时才回退为完整列表。
			// 第一页请求确定资源版本后，后续分页请求期间它可能因压缩而失效；
			// 回退旨在避免这种情况下整个分页读取失败。
			if !errors.IsResourceExpired(err) || !p.FullListIfExpired || options.Continue == "" {
				return nil, paginatedResult, err
			}
			// 处理过程中列表已过期，改为按调用方请求的 ResourceVersion 获取完整列表。
			// 注意只从第二页起回退：第一页就过期说明调用方给的 RV 本身已不可用，
			// 此时回退也救不回来，交给调用方处理更合适。
			options.Limit = 0
			options.Continue = ""
			options.ResourceVersion = requestedResourceVersion
			options.ResourceVersionMatch = requestedResourceVersionMatch
			result, err := p.PageFn(ctx, options)
			return result, paginatedResult, err
		}
		m, err := meta.ListAccessor(obj)
		if err != nil {
			return nil, paginatedResult, fmt.Errorf("returned object must be a list: %v", err)
		}

		// 尚未处理其他页且服务端未返回 continue 游标时，直接返回该对象，无需合并。
		if len(m.GetContinue()) == 0 && list == nil {
			return obj, paginatedResult, nil
		}

		// 初始化合并结果，并用首页的 RV/SelfLink 标识整个列表，后续页只贡献 Items。
		if list == nil {
			list = &metainternalversion.List{Items: make([]runtime.Object, 0, options.Limit+1)}
			list.ResourceVersion = m.GetResourceVersion()
			list.SelfLink = m.GetSelfLink()
		}
		eachListItemFunc := meta.EachListItem
		if allocNew {
			eachListItemFunc = meta.EachListItemWithAlloc
		}
		if err := eachListItemFunc(obj, func(obj runtime.Object) error {
			list.Items = append(list.Items, obj)
			return nil
		}); err != nil {
			return nil, paginatedResult, err
		}

		// 没有后续条目时返回合并后的列表。
		if len(m.GetContinue()) == 0 {
			return list, paginatedResult, nil
		}

		// 为下一轮分页设置游标。
		options.Continue = m.GetContinue()
		// 后续 List 调用需清空 ResourceVersion 和 ResourceVersionMatch，避免服务端报
		// `specifying resource version is not allowed when using continue` 错误。
		// 参见 https://github.com/kubernetes/kubernetes/issues/85221#issuecomment-553748143。
		// 继续分页时快照已由 continue 令牌固定，再带 RV 会被服务端拒绝。
		options.ResourceVersion = ""
		options.ResourceVersionMatch = ""
		// 此时已确定结果需要分页读取。
		paginatedResult = true
	}
}

// EachListItem 使用此 ListPager 获取 runtime.Object 条目，并逐项调用 fn。
// 若 fn 返回错误，则停止处理并返回该错误；否则返回从服务端获取列表时遇到的错误。
// 若 context 被取消或超时，则返回 context 错误。由于列表按页获取，当分页请求超出
// 所调用 apiserver 的过期限制时，可能返回 Expired 错误（metav1.StatusReasonExpired）。
//
// 条目从服务端分块获取以减轻服务端压力，后台最多并发缓冲 ListPager.PageBufferSize 块。
//
// 若传给 fn 的条目保留时间不同，且不希望任一项仍被引用时一直保留 p.PageFn 返回的
// 整个切片，请使用 EachListItemWithAlloc。EachListItem 逐项回调，内存中只保留少量页，
// 与一次性返回整表的 List 不同，适合流式消费大量对象。
func (p *ListPager) EachListItem(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	return p.eachListChunkBuffered(ctx, options, func(obj runtime.Object) error {
		return meta.EachListItem(obj, fn)
	})
}

// EachListItemWithAlloc 的用法与 EachListItem 相同，但不会保留对 p.PageFn 返回的
// 条目切片的引用。实现方式是对该切片中的非指针条目做浅拷贝。
//
// 若传给 fn 的条目不被保留，或保留时间相同，为节省内存请改用 EachListItem。
func (p *ListPager) EachListItemWithAlloc(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	return p.eachListChunkBuffered(ctx, options, func(obj runtime.Object) error {
		return meta.EachListItemWithAlloc(obj, fn)
	})
}

// eachListChunkBuffered 使用此 ListPager 获取 runtime.Object 列表块，并逐块调用 fn。
// 若 fn 返回错误，则停止处理并返回该错误；否则返回从服务端获取列表时遇到的错误。
// 若 context 被取消或超时，则返回 context 错误。由于列表按页获取，当分页请求超出
// 所调用 apiserver 的过期限制时，可能返回 Expired 错误（metav1.StatusReasonExpired）。
//
// 后台最多并发缓冲 ListPager.PageBufferSize 个列表块，且只预取、不执行回调。
// 因此 fn 始终在前台 goroutine 中串行执行，调用方无需考虑并发。
func (p *ListPager) eachListChunkBuffered(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	if p.PageBufferSize < 0 {
		return fmt.Errorf("ListPager.PageBufferSize must be >= 0, got %d", p.PageBufferSize)
	}

	// 若此调用在所有条目处理完毕前退出，确保后台 goroutine 停止。
	// 延迟 cancel 产生的取消错误不会返回给调用方：列表结果要么已发送到 bgResultC，
	// 要么已返回 fn 的错误并丢弃取消错误。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 后台生产、前台消费：chunkC 的容量即 PageBufferSize，达到容量后后台 goroutine 会阻塞，
	// 因此最多只会领先消费者 PageBufferSize 页。
	chunkC := make(chan runtime.Object, p.PageBufferSize)
	bgResultC := make(chan error, 1)
	go func() {
		defer utilruntime.HandleCrashWithContext(ctx)

		var err error
		defer func() {
			close(chunkC)
			bgResultC <- err
		}()
		err = p.eachListChunk(ctx, options, func(chunk runtime.Object) error {
			select {
			case chunkC <- chunk: // 缓冲列表块，发送可能阻塞
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}()

	for o := range chunkC {
		// 消费者也要响应取消，否则后台阻塞在发送时前台可能永远等不到下一项。
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := fn(o)
		if err != nil {
			return err // fn 的错误应立即返回
		}
	}
	// channel 关闭后读取后台 goroutine 的结果：此时错误已经确定，且 goroutine 一定已退出。
	return <-bgResultC
}

// eachListChunk 使用此 ListPager 获取 runtime.Object 列表块，并逐块调用 fn。
// 若 fn 返回错误，则停止处理并返回该错误；否则返回从服务端获取列表时遇到的错误。
// 若 context 被取消或超时，则返回 context 错误。由于列表按页获取，当分页请求超出
// 所调用 apiserver 的过期限制时，可能返回 Expired 错误（metav1.StatusReasonExpired）。
//
// 本函数串行分页并逐页回调，不像 list 那样合并结果，也不做过期回退，
// 因此调用方需要自行处理 Expired 错误。
func (p *ListPager) eachListChunk(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	if options.Limit == 0 {
		options.Limit = p.PageSize
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		obj, err := p.PageFn(ctx, options)
		if err != nil {
			return err
		}
		m, err := meta.ListAccessor(obj)
		if err != nil {
			return fmt.Errorf("returned object must be a list: %v", err)
		}
		if err := fn(obj); err != nil {
			return err
		}
		// 没有后续条目时结束分页。
		if len(m.GetContinue()) == 0 {
			return nil
		}
		// 为下一轮分页设置游标。
		options.Continue = m.GetContinue()
	}
}
