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

// ListPageFunc returns a list object for the given list options.
type ListPageFunc func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error)

// SimplePageFunc adapts a context-less list function into one that accepts a context.
//
// 保留该适配器是为了兼容只接受 ListOptions 的旧式 lister/list 实现。
func SimplePageFunc(fn func(opts metav1.ListOptions) (runtime.Object, error)) ListPageFunc {
	return func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		return fn(opts)
	}
}

// ListPager assists client code in breaking large list queries into multiple
// smaller chunks of PageSize or smaller. PageFn is expected to accept a
// metav1.ListOptions that supports paging and return a list. The pager does
// not alter the field or label selectors on the initial options list.
type ListPager struct {
	PageSize int64
	PageFn   ListPageFunc

	// FullListIfExpired 决定分页游标过期时是否回退为完整 list；
	// 关闭它会让过期错误直接透传给调用方。
	FullListIfExpired bool

	// Number of pages to buffer
	// 后台预取的页数；0 表示完全串行，负值会在 EachListItem 中报错。
	PageBufferSize int32
}

// New creates a new pager from the provided pager function using the default
// options. It will fall back to a full list if an expiration error is encountered
// as a last resort.
func New(fn ListPageFunc) *ListPager {
	return &ListPager{
		PageSize:          defaultPageSize,
		PageFn:            fn,
		FullListIfExpired: true,
		PageBufferSize:    defaultPageBufferSize,
	}
}

// TODO: introduce other types of paging functions - such as those that retrieve from a list
// of namespaces.

// List returns a single list object, but attempts to retrieve smaller chunks from the
// server to reduce the impact on the server. If the chunk attempt fails, it will load
// the full list instead. The Limit field on options, if unset, will default to the page size.
//
// If items in the returned list are retained for different durations, and you want to avoid
// retaining the whole slice returned by p.PageFn as long as any item is referenced,
// use ListWithAlloc instead.
// 返回值中的 bool 表示结果是否来自分页读取（而非一次完整 list），调用方可用它判断
// 是否可能存在快照不一致的风险。
func (p *ListPager) List(ctx context.Context, options metav1.ListOptions) (runtime.Object, bool, error) {
	return p.list(ctx, options, false)
}

// ListWithAlloc works like List, but avoids retaining references to the items slice returned by p.PageFn.
// It does this by making a shallow copy of non-pointer items in the slice returned by p.PageFn.
//
// If the items in the returned list are not retained, or are retained for the same duration, use List instead for memory efficiency.
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
			// Only fallback to full list if an "Expired" errors is returned, FullListIfExpired is true, and
			// the "Expired" error occurred in page 2 or later (since full list is intended to prevent a pager.List from
			// failing when the resource versions is established by the first page request falls out of the compaction
			// during the subsequent list requests).
			if !errors.IsResourceExpired(err) || !p.FullListIfExpired || options.Continue == "" {
				return nil, paginatedResult, err
			}
			// the list expired while we were processing, fall back to a full list at
			// the requested ResourceVersion.
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

		// exit early and return the object we got if we haven't processed any pages
		// 服务端没有返回 continue 游标，说明一次请求就取完了，无需合并。
		if len(m.GetContinue()) == 0 && list == nil {
			return obj, paginatedResult, nil
		}

		// initialize the list and fill its contents
		// 用首頁的 RV/SelfLink 作为合并结果的身份，后续页只贡献 Items。
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

		// if we have no more items, return the list
		if len(m.GetContinue()) == 0 {
			return list, paginatedResult, nil
		}

		// set the next loop up
		options.Continue = m.GetContinue()
		// Clear the ResourceVersion(Match) on the subsequent List calls to avoid the
		// `specifying resource version is not allowed when using continue` error.
		// See https://github.com/kubernetes/kubernetes/issues/85221#issuecomment-553748143.
		// 继续分页时快照已由 continue 令牌固定，再带 RV 会被服务端拒绝。
		options.ResourceVersion = ""
		options.ResourceVersionMatch = ""
		// At this point, result is already paginated.
		paginatedResult = true
	}
}

// EachListItem fetches runtime.Object items using this ListPager and invokes fn on each item. If
// fn returns an error, processing stops and that error is returned. If fn does not return an error,
// any error encountered while retrieving the list from the server is returned. If the context
// cancels or times out, the context error is returned. Since the list is retrieved in paginated
// chunks, an "Expired" error (metav1.StatusReasonExpired) may be returned if the pagination list
// requests exceed the expiration limit of the apiserver being called.
//
// Items are retrieved in chunks from the server to reduce the impact on the server with up to
// ListPager.PageBufferSize chunks buffered concurrently in the background.
//
// If items passed to fn are retained for different durations, and you want to avoid
// retaining the whole slice returned by p.PageFn as long as any item is referenced,
// use EachListItemWithAlloc instead.
// EachListItem 逐项回调，内存中同时只保留少量页；这与 List 一次性返回整表形成对比，
// 适合处理大量对象且只做流式消费的场景。
func (p *ListPager) EachListItem(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	return p.eachListChunkBuffered(ctx, options, func(obj runtime.Object) error {
		return meta.EachListItem(obj, fn)
	})
}

// EachListItemWithAlloc works like EachListItem, but avoids retaining references to the items slice returned by p.PageFn.
// It does this by making a shallow copy of non-pointer items in the slice returned by p.PageFn.
//
// If the items passed to fn are not retained, or are retained for the same duration, use EachListItem instead for memory efficiency.
func (p *ListPager) EachListItemWithAlloc(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	return p.eachListChunkBuffered(ctx, options, func(obj runtime.Object) error {
		return meta.EachListItemWithAlloc(obj, fn)
	})
}

// eachListChunkBuffered fetches runtimeObject list chunks using this ListPager and invokes fn on
// each list chunk.  If fn returns an error, processing stops and that error is returned. If fn does
// not return an error, any error encountered while retrieving the list from the server is
// returned. If the context cancels or times out, the context error is returned. Since the list is
// retrieved in paginated chunks, an "Expired" error (metav1.StatusReasonExpired) may be returned if
// the pagination list requests exceed the expiration limit of the apiserver being called.
//
// Up to ListPager.PageBufferSize chunks are buffered concurrently in the background.
//
// 后台只预取、不回调，因此 fn 始终在前台 goroutine 中串行执行，调用方无需考虑并发。
func (p *ListPager) eachListChunkBuffered(ctx context.Context, options metav1.ListOptions, fn func(obj runtime.Object) error) error {
	if p.PageBufferSize < 0 {
		return fmt.Errorf("ListPager.PageBufferSize must be >= 0, got %d", p.PageBufferSize)
	}

	// Ensure background goroutine is stopped if this call exits before all list items are
	// processed. Cancelation error from this deferred cancel call is never returned to caller;
	// either the list result has already been sent to bgResultC or the fn error is returned and
	// the cancelation error is discarded.
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
			case chunkC <- chunk: // buffer the chunk, this can block
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
			return err // any fn error should be returned immediately
		}
	}
	// promote the results of our background goroutine to the foreground
	// channel 关闭后读取后台结果：此时错误已经确定，且 goroutine 一定已退出。
	return <-bgResultC
}

// eachListChunk fetches runtimeObject list chunks using this ListPager and invokes fn on each list
// chunk. If fn returns an error, processing stops and that error is returned. If fn does not return
// an error, any error encountered while retrieving the list from the server is returned. If the
// context cancels or times out, the context error is returned. Since the list is retrieved in
// paginated chunks, an "Expired" error (metav1.StatusReasonExpired) may be returned if the
// pagination list requests exceed the expiration limit of the apiserver being called.
// eachListChunk 串行分页并逐页回调；它不像 list 那样合并结果，也不做过期回退，
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
		// if we have no more items, return.
		if len(m.GetContinue()) == 0 {
			return nil
		}
		// set the next loop up
		options.Continue = m.GetContinue()
	}
}
