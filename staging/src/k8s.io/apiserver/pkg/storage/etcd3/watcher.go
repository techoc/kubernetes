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

package etcd3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	"k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// 设置缓冲是为了减少上下文切换次数。
	// 两个 100 容量的缓冲把"读 etcd"与"解码/分发"解耦：短时抖动由缓冲吸收，
	// 持续慢消费则由 blockLogger 记录告警。
	incomingBufSize         = 100
	outgoingBufSize         = 100
	processEventConcurrency = 10

	watchChanLogWarningInterval  = 5 * time.Second
	watchChanLogWarningThreshold = 100 * time.Millisecond
)

// defaultWatcherMaxLimit 用于方便构造测试
var defaultWatcherMaxLimit int64 = maxLimit

// fatalOnDecodeError 在测试期间使用：watcher 遇到解码错误时让进程 panic
var fatalOnDecodeError atomic.Bool

func init() {
	// 检查是否运行在测试环境中
	b, _ := strconv.ParseBool(os.Getenv("KUBE_PANIC_WATCH_DECODE_ERROR"))
	fatalOnDecodeError.Store(b)
}

// TestingTB 是 TestOnlySetFatalOnDecodeError 所需要的 testing.TB 子集，
// 以避免导入 testing 包。
type TestingTB interface {
	Helper()
	Cleanup(func())
}

// TestOnlySetFatalOnDecodeError 只应当用于解码错误符合预期、需要被测试的场景，
// 例如 conversion webhook。
//
// 临时切换全局开关并在测试结束时恢复，避免影响并行运行的其他测试。
func TestOnlySetFatalOnDecodeError(tb TestingTB, b bool) {
	tb.Helper()
	old := fatalOnDecodeError.Swap(b)
	tb.Cleanup(func() {
		fatalOnDecodeError.Store(old)
	})
}

// watcher 为单个资源（GroupResource）构造 watch 通道。
// 它只持有不可变依赖，可被多个 watch 并发复用。
type watcher struct {
	client                   *clientv3.Client
	codec                    runtime.Codec
	newFunc                  func() runtime.Object
	objectType               string
	groupResource            schema.GroupResource
	versioner                storage.Versioner
	transformer              value.Transformer
	getCurrentStorageRV      func(context.Context) (uint64, error)
	getResourceSizeEstimator func() *resourceSizeEstimator
}

// watchChan 实现 watch.Interface。
//
// 内部有两条 goroutine 链：startWatching 从 etcd 拉事件写入 incomingEventChan，
// processEvents 解码、过滤后写入 resultChan。run 负责在任一方退出时统一回收。
type watchChan struct {
	watcher                  *watcher
	key                      string
	initialRev               int64
	recursive                bool
	progressNotify           bool
	recordTimestamps         bool
	internalPred             storage.SelectionPredicate
	ctx                      context.Context
	cancel                   context.CancelFunc
	incomingEventChan        chan *event
	resultChan               chan watch.Event
	getResourceSizeEstimator func() *resourceSizeEstimator

	incomingEventLogger *blockLogger
	resultLogger        *blockLogger
}

// Watch 对某个 key 进行 watch，并返回传输相关通知的 watch.Interface。
// 如果 rev 为零，会先返回已存在的对象，然后从返回对象的最大 revision+1 处开始 watch。
// 如果 rev 非零，则 watch 给定 revision 之后发生的事件。
// 如果 opts.Recursive 为 false，只 watch 给定的 key。
// 如果 opts.Recursive 为 true，watch 该 key 下的所有子节点和目录，但不包含根 key 本身。
// pred 必须非 nil。只有 opts.Predicate 匹配该变更时才会返回。
//
// 返回后立即调用 WatchInitialized：etcd 路径无法可靠判断"已追上最新 RV"，
// 与其让流控误判，不如按已初始化处理（watch cache 默认已承担该职责）。
func (w *watcher) Watch(ctx context.Context, key string, rev int64, opts storage.ListOptions) (watch.Interface, error) {
	if opts.Recursive && !strings.HasSuffix(key, "/") {
		return nil, fmt.Errorf(`recursive key needs to end with "/"`)
	}
	// progress notify 需要构造一个占位对象来携带 RV，缺少 newFunc 则无法实现。
	if opts.ProgressNotify && w.newFunc == nil {
		return nil, apierrors.NewInternalError(errors.New("progressNotify for watch is unsupported by the etcd storage because no newFunc was provided"))
	}
	startWatchRV, err := w.getStartWatchResourceVersion(ctx, rev, opts)
	if err != nil {
		return nil, err
	}
	wc := w.createWatchChan(ctx, key, startWatchRV, opts.Recursive, opts.ProgressNotify, opts.RecordTimestamps, opts.Predicate)
	go wc.run(isInitialEventsEndBookmarkRequired(opts), areInitialEventsRequired(rev, opts))

	// 对 etcd watch 而言，我们没有简单的方法判断 watch 是否已经追上最新进度。
	// 所以在初版实现中（考虑到 watchcache 默认对除 Events 外的所有资源启用），
	// 我们立即发出初始化信号。后续会探索改进方案。
	utilflowcontrol.WatchInitialized(ctx)

	return wc, nil
}

func (w *watcher) createWatchChan(ctx context.Context, key string, rev int64, recursive, progressNotify, recordTimestamps bool, pred storage.SelectionPredicate) *watchChan {
	wc := &watchChan{
		watcher:                  w,
		key:                      key,
		initialRev:               rev,
		recursive:                recursive,
		progressNotify:           progressNotify,
		recordTimestamps:         recordTimestamps,
		internalPred:             pred,
		incomingEventChan:        make(chan *event, incomingBufSize),
		resultChan:               make(chan watch.Event, outgoingBufSize),
		getResourceSizeEstimator: w.getResourceSizeEstimator,
	}
	if pred.Empty() {
		// 该 filter 不会过滤掉任何对象。
		// 用 Everything 而非 Empty，避免后续 filter 对每个对象都做无谓的判断。
		wc.internalPred = storage.Everything
	}
	wc.ctx, wc.cancel = context.WithCancel(ctx)

	wc.incomingEventLogger = newBlockLogger(watchChanLogWarningInterval, watchChanLogWarningThreshold, "Fast watcher, slow processing. Probably caused by slow decoding, user not receiving fast, or other processing logic", wc.watcher.objectType, wc.watcher.groupResource, clock.RealClock{})
	wc.resultLogger = newBlockLogger(watchChanLogWarningInterval, watchChanLogWarningThreshold, "Fast watcher, slow processing. Probably caused by slow dispatching events to watchers", wc.watcher.objectType, wc.watcher.groupResource, clock.RealClock{})

	return wc
}

// getStartWatchResourceVersion 返回 watch 将要启动的 ResourceVersion。
// 根据输入参数，返回的 ResourceVersion 语义为：
//   - 从 Exact 位置开始（返回 resourceVersion）
//   - 从 Most Recent 位置开始（返回来自 etcd 的 RV）
//
// WatchList 关闭时返回 0，沿用旧语义：先列出现有对象再从最新 RV 开始 watch。
func (w *watcher) getStartWatchResourceVersion(ctx context.Context, resourceVersion int64, opts storage.ListOptions) (int64, error) {
	if resourceVersion > 0 {
		return resourceVersion, nil
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
		return 0, nil
	}
	if opts.SendInitialEvents == nil || *opts.SendInitialEvents {
		// 注意：当 opts.SendInitialEvents=true 时，我们会先对 etcd 发起一次
		// 一致性 LIST 请求，随后跟上特殊的 bookmark 事件
		return 0, nil
	}
	// 走到这里说明客户端只想要从 MostRecent 时间点（RV）开始的事件流
	currentStorageRV, err := w.getCurrentStorageRV(ctx)
	if err != nil {
		return 0, err
	}
	// currentStorageRV 取自 resp.Header.Revision（int64）再转换为 uint64，
	// 因此在这里反向转换是安全的。
	// 将来应当统一这个接口，但那需要修改 Versioner.UpdateList
	return int64(currentStorageRV), nil
}

// isInitialEventsEndBookmarkRequired：由于无法从 API 直接设置 opts.ProgressNotify，
// 且 etcd3 实现不支持对外部客户端的通知，我们返回 initialEventsEndBookmarkRequired，
// 以便只在初始列表调用之后发送 bookmark 事件。
//
// see: https://github.com/kubernetes/kubernetes/issues/120348
//
// 只有"发送初始事件 + 允许 bookmark"同时成立时才需要结束书签，
// 否则客户端无法据此判断初始列表已经结束。
func isInitialEventsEndBookmarkRequired(opts storage.ListOptions) bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
		return false
	}
	return opts.SendInitialEvents != nil && *opts.SendInitialEvents && opts.Predicate.AllowWatchBookmarks
}

// areInitialEventsRequired 在需要返回 etcd 的全部事件时返回 true。
//
// SendInitialEvents 未显式设置且 RV 为 0 属于旧的 list-then-watch 语义，同样需要初始事件。
func areInitialEventsRequired(resourceVersion int64, opts storage.ListOptions) bool {
	if opts.SendInitialEvents == nil && resourceVersion == 0 {
		return true // 旧的 list-then-watch 场景
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
		return false
	}
	return opts.SendInitialEvents != nil && *opts.SendInitialEvents
}

// etcdError 与 grpcError 覆盖 etcd 客户端在不同版本/路径上返回的错误类型。
type etcdError interface {
	Code() grpccodes.Code
	Error() string
}

type grpcError interface {
	GRPCStatus() *grpcstatus.Status
}

// isCancelError 判断错误是否只是"调用方取消"，这类错误无需作为失败上报。
func isCancelError(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled {
		return true
	}
	if etcdErr, ok := err.(etcdError); ok && etcdErr.Code() == grpccodes.Canceled {
		return true
	}
	if grpcErr, ok := err.(grpcError); ok && grpcErr.GRPCStatus().Code() == grpccodes.Canceled {
		return true
	}
	return false
}

// run 启动拉取与处理两条 goroutine，并在任一方结束后统一取消与回收。
// 无论正常关闭、用户取消还是错误，都通过 cancel 触发所有 goroutine 退出，
// 最后关闭 resultChan 作为"流结束"信号。
func (wc *watchChan) run(initialEventsEndBookmarkRequired, forceInitialEvents bool) {
	watchClosedCh := make(chan struct{})
	var resultChanWG sync.WaitGroup

	resultChanWG.Add(1)
	go func() {
		defer resultChanWG.Done()
		wc.startWatching(watchClosedCh, initialEventsEndBookmarkRequired, forceInitialEvents)
	}()
	wc.processEvents(&resultChanWG)

	select {
	case <-watchClosedCh:
	case <-wc.ctx.Done(): // 用户取消
	}

	// 我们使用 wc.ctx 来回收所有 goroutine。无论在哪种情况下，都应当把它们全部停止。
	// 重复 cancel 也没关系。
	wc.cancel()

	// 需要等到 resultChan 不再被使用之后才能关闭
	resultChanWG.Wait()
	close(wc.resultChan)
}

// Stop 实现 watch.Interface，取消上下文以终止后台 goroutine。
func (wc *watchChan) Stop() {
	wc.cancel()
}

// ResultChan 实现 watch.Interface，返回只读事件通道。
func (wc *watchChan) ResultChan() <-chan watch.Event {
	return wc.resultChan
}

// RequestWatchProgress 向 etcd 请求一次进度通知，使客户端能推进已知 RV。
func (wc *watchChan) RequestWatchProgress() error {
	return wc.watcher.client.RequestProgress(wc.ctx)
}

// sync 尝试获取已存在的数据并发送给处理流程。
// watch 的 revision 将被设置为响应中的 revision。
// 所有发送的事件都会带有 isCreated=true
//
// 优先使用 RangeStream（流式、内存占用低）；若 etcd 不支持则降级为分页 list，
// 并记录该能力不支持，避免对每个 watch 反复尝试。
func (wc *watchChan) sync() error {
	if wc.recursive && utilfeature.DefaultFeatureGate.Enabled(features.EtcdRangeStream) &&
		etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RangeStream) {
		err := wc.syncStreamRecursive()
		if err == nil {
			return nil
		}
		// 只有 Unimplemented 才是"服务端不支持"，其它错误应直接上抛。
		if grpcstatus.Code(err) != grpccodes.Unimplemented {
			return err
		}
		etcdfeature.DefaultFeatureSupportChecker.MarkUnsupported(storage.RangeStream)
		klog.V(4).Infof("etcd server does not support RangeStream for %v; falling back to paginated list", wc.watcher.groupResource)
	}
	return wc.syncPaginated()
}

// syncPaginated 用带 limit 的 Get 分页拉取现有对象，直到 etcd 报告没有更多。
// 所有分页固定在同一 RV 上，保证初始快照一致。
func (wc *watchChan) syncPaginated() error {
	opts := []clientv3.OpOption{}
	if wc.recursive {
		opts = append(opts, clientv3.WithLimit(defaultWatcherMaxLimit))
		rangeEnd := clientv3.GetPrefixRangeEnd(wc.key)
		opts = append(opts, clientv3.WithRange(rangeEnd))
	}

	var err error
	var lastKey []byte
	var withRev int64
	var getResp *clientv3.GetResponse

	metricsOp := "get"
	if wc.recursive {
		metricsOp = "list"
	}

	preparedKey := wc.key

	for {
		startTime := time.Now()
		getResp, err = wc.watcher.client.KV.Get(wc.ctx, preparedKey, opts...)
		metrics.RecordEtcdRequest(metricsOp, wc.watcher.groupResource, err, startTime)
		if err != nil {
			return interpretListError(err, true, preparedKey, wc.key)
		}

		if len(getResp.Kvs) == 0 && getResp.More {
			return fmt.Errorf("no results were found, but etcd indicated there were more values remaining")
		}

		// 持续发送响应中的条目，直到没有更多结果
		for i, kv := range getResp.Kvs {
			lastKey = kv.Key
			wc.queueEvent(parseKV(kv))
			// 尽早释放 kv。长列表的解码可能需要 O(秒) 级别的时间。
			getResp.Kvs[i] = nil
		}

		if withRev == 0 {
			wc.initialRev = getResp.Header.Revision
		}

		// 已没有更多结果
		if !getResp.More {
			return nil
		}

		// 下一页从 lastKey + "\x00" 开始，跳过已处理键且不遗漏以 lastKey 为前缀的键。
		preparedKey = string(lastKey) + "\x00"
		if withRev == 0 {
			withRev = getResp.Header.Revision
			opts = append(opts, clientv3.WithRev(withRev))
		}
	}
}

// syncStreamRecursive 通过 etcd 的 RangeStream 流式读取整个前缀，
// 避免一次性把大列表全部装入内存。仅在 recursive watch 下使用。
func (wc *watchChan) syncStreamRecursive() error {
	if !wc.recursive {
		return fmt.Errorf("syncStreamRecursive called on a non-recursive watch")
	}
	opts := []clientv3.OpOption{
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(wc.key)),
	}

	startTime := time.Now()
	streamResp, err := wc.watcher.client.KV.GetStream(wc.ctx, wc.key, opts...)
	if err != nil {
		return err
	}
	defer func() {
		// 只有真正使用了流式读取时才记录 listStream 指标。
		if grpcstatus.Code(err) == grpccodes.Unimplemented {
			return
		}
		metrics.RecordEtcdRequest("listStream", wc.watcher.groupResource, err, startTime)
	}()

	var initialRev int64
	for r := range streamResp {
		if err = r.Err(); err != nil {
			// paging=false：流中途发生压缩时无法用 continue token 恢复，
			// 因此按 ResourceExpired 上报，让客户端重新 list。
			return interpretListError(err, false, wc.key, wc.key)
		}
		rangeResp := r.RangeResponse
		for i, kv := range rangeResp.Kvs {
			wc.queueEvent(parseKV(kv))
			// 尽早释放 kv。长列表的解码可能需要 O(秒) 级别的时间。
			rangeResp.Kvs[i] = nil
		}
		if initialRev == 0 && rangeResp.Header != nil {
			initialRev = rangeResp.Header.Revision
		}
	}
	if initialRev == 0 {
		return fmt.Errorf("rangeStream for %q completed without a revision", wc.key)
	}

	wc.initialRev = initialRev
	return nil
}

// logWatchChannelErr 按错误性质分级记录：压缩属于可恢复的预期情况，取消无需记录。
func logWatchChannelErr(err error) {
	switch {
	case strings.Contains(err.Error(), "mvcc: required revision has been compacted"):
		// mvcc revision 被压缩，这被视为警告，不是错误
		klog.Warningf("watch chan error: %v", err)
	case isCancelError(err):
		// watch 关闭时属于预期情况，无需记录
	default:
		klog.Errorf("watch chan error: %v", err)
	}
}

// startWatching 做两件事：
// - 若 initialRev=0，获取当前对象，并把 initialRev 设置为当前 revision
// - 对给定 key 进行 watch，并把事件发送给处理流程。
//
// initialEventsEndBookmarkSent 帮助我们跟踪
// 是否已经发送过带标注的 bookmark 事件。
//
// 需要注意，我们不需要跟踪实际的 RV，
// 因为我们只在初始列表调用之后
// 发送 bookmark 事件。
//
// 当这个变量被设置为 false 时，
// 表示我们对 bookmark 事件的投递没有特殊要求。
//
// 若请求的初始 RV 超前于 etcd 当前 RV，返回 TooLargeResourceVersion 让客户端
// 等待后重试，而不是在 etcd 上等待一个永远到不了的 revision。
func (wc *watchChan) startWatching(watchClosedCh chan struct{}, initialEventsEndBookmarkRequired, forceInitialEvents bool) {
	if wc.initialRev > 0 && forceInitialEvents {
		currentStorageRV, err := wc.watcher.getCurrentStorageRV(wc.ctx)
		if err != nil {
			wc.sendError(err)
			return
		}
		if uint64(wc.initialRev) > currentStorageRV {
			wc.sendError(storage.NewTooLargeResourceVersionError(uint64(wc.initialRev), currentStorageRV, int(wait.Jitter(1*time.Second, 3).Seconds())))
			return
		}
	}
	if forceInitialEvents {
		if err := wc.sync(); err != nil {
			klog.Errorf("failed to sync with latest state: %v", err)
			wc.sendError(err)
			return
		}
	}
	// 结束书签必须在初始列表之后入队，客户端据此得知"历史事件已发完"。
	if initialEventsEndBookmarkRequired {
		wc.queueEvent(func() *event {
			e := progressNotifyEvent(wc.initialRev)
			e.isInitialEventsEndBookmark = true
			return e
		}())
	}
	opts := []clientv3.OpOption{clientv3.WithRev(wc.initialRev + 1), clientv3.WithPrevKV()}
	if wc.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}
	if wc.progressNotify {
		opts = append(opts, clientv3.WithProgressNotify())
	}
	wch := wc.watcher.client.Watch(wc.ctx, wc.key, opts...)
	// estimator 可为 nil（未启用），此时跳过对象大小统计。
	estimator := wc.getResourceSizeEstimator()
	for wres := range wch {
		if wres.Err() != nil {
			err := wres.Err()
			// 服务端出错时（例如压缩），通道会在关闭前先返回该错误。
			logWatchChannelErr(err)
			// sendError 无法保证不再向 resultChan 放入条目。但在这里从
			// startWatching 返回，可以保证不会入队比该错误 resourceVersion 更高的事件，
			// 因此也不会再处理并发送给用户。
			// TODO(wojtek-t): 弄清楚是否可以先同步阻止更多事件。
			wc.sendError(err)
			return
		}
		if wres.IsProgressNotify() {
			wc.queueEvent(progressNotifyEvent(wres.Header.GetRevision()))
			metrics.RecordEtcdBookmark(wc.watcher.groupResource)
			continue
		}

		for _, e := range wres.Events {
			if estimator != nil {
				switch e.Type {
				case clientv3.EventTypePut:
					estimator.UpdateKey(e.Kv)
				case clientv3.EventTypeDelete:
					estimator.DeleteKey(e.Kv)
				}
			}
			metrics.RecordEtcdEvent(wc.watcher.groupResource)
			parsedEvent, err := parseEvent(e)
			if err != nil {
				logWatchChannelErr(err)
				// sendError 无法保证不再向 resultChan 放入条目。但在这里从
				// startWatching 返回，可以保证不会入队比该错误 resourceVersion 更高的事件，
				// 因此也不会再处理并发送给用户。
				// TODO(wojtek-t): 弄清楚是否可以先同步阻止更多事件。
				wc.sendError(err)
				return
			}
			wc.queueEvent(parsedEvent)
		}
	}
	// 走到这里时，只可能是客户端一侧结束了 watch，
	// 例如取消 context、关闭 client。
	// 如果这个 watch chan 断了而 context 没被取消，其他 goroutine 会一直挂住。
	// 我们应当通知主线程这个 goroutine 已退出。
	close(watchClosedCh)
}

// processEvents 从 etcd watcher 处理事件并把结果发送到 resultChan。
//
// 并发解码可提升吞吐，但必须保序；关闭特性时退化为单 goroutine 串行处理。
func (wc *watchChan) processEvents(wg *sync.WaitGroup) {
	if utilfeature.DefaultFeatureGate.Enabled(features.ConcurrentWatchObjectDecode) {
		wc.concurrentProcessEvents(wg)
	} else {
		wg.Add(1)
		go wc.serialProcessEvents(wg)
	}
}

// serialProcessEvents 串行解码并分发事件。
func (wc *watchChan) serialProcessEvents(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case e := <-wc.incomingEventChan:
			res, err := wc.transform(e)
			if err != nil {
				wc.sendError(err)
				return
			}

			if res == nil {
				continue
			}
			if !wc.sendEvent(res) {
				return
			}
		case <-wc.ctx.Done():
			return
		}
	}
}

// concurrentProcessEvents 启动调度与收集两条 goroutine，
// 由 concurrentOrderedEventProcessing 保证"并发解码、按序输出"。
func (wc *watchChan) concurrentProcessEvents(wg *sync.WaitGroup) {
	p := concurrentOrderedEventProcessing{
		wc:              wc,
		processingQueue: make(chan chan *processingResult, processEventConcurrency-1),

		objectType:    wc.watcher.objectType,
		groupResource: wc.watcher.groupResource,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.scheduleEventProcessing(wc.ctx, wg)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.collectEventProcessing(wc.ctx)
	}()
}

// processingResult 是并发解码的单条结果，err 非空时整条流终止。
type processingResult struct {
	event *watch.Event
	err   error
}

// concurrentOrderedEventProcessing 用"每事件一个结果通道"的方式维持顺序：
// 调度器按到达顺序把结果通道放入 processingQueue，收集器再按同样顺序读取，
// 因此解码可以并行，输出顺序仍与 etcd 事件顺序一致。
type concurrentOrderedEventProcessing struct {
	wc *watchChan

	processingQueue chan chan *processingResult
	// 用于日志的元数据
	objectType    string
	groupResource schema.GroupResource
}

// scheduleEventProcessing 读取 incomingEventChan，为每条事件分配结果通道，
// 入队后启动解码 goroutine。队列容量为并发数-1，天然限制在途任务数。
func (p *concurrentOrderedEventProcessing) scheduleEventProcessing(ctx context.Context, wg *sync.WaitGroup) {
	var e *event
	for {
		select {
		case <-ctx.Done():
			return
		case e = <-p.wc.incomingEventChan:
		}
		processingResponse := make(chan *processingResult, 1)
		select {
		case <-ctx.Done():
			return
		case p.processingQueue <- processingResponse:
		}
		wg.Add(1)
		go func(e *event, response chan<- *processingResult) {
			defer wg.Done()
			responseEvent, err := p.wc.transform(e)
			select {
			case <-ctx.Done():
			case response <- &processingResult{event: responseEvent, err: err}:
			}
		}(e, processingResponse)
	}
}

// collectEventProcessing 严格按调度顺序取结果并发送，任一错误即终止流。
func (p *concurrentOrderedEventProcessing) collectEventProcessing(ctx context.Context) {
	var processingResponse chan *processingResult
	var r *processingResult
	for {
		select {
		case <-ctx.Done():
			return
		case processingResponse = <-p.processingQueue:
		}
		select {
		case <-ctx.Done():
			return
		case r = <-processingResponse:
		}
		if r.err != nil {
			p.wc.sendError(r.err)
			return
		}
		if r.event == nil {
			continue
		}
		if !p.wc.sendEvent(r.event) {
			return
		}
	}
}

// filter 判断对象是否满足选择器；匹配出错时按不匹配处理（fail closed）。
func (wc *watchChan) filter(obj runtime.Object) bool {
	if wc.internalPred.Empty() {
		return true
	}
	matched, err := wc.internalPred.Matches(obj)
	return err == nil && matched
}

// acceptAll 表示没有过滤条件，可用于跳过对 oldObj 的解码。
func (wc *watchChan) acceptAll() bool {
	return wc.internalPred.Empty()
}

// transform 在未命中过滤条件时把事件转换为用户侧结果。
//
// 关键语义：过滤条件变化会导致同一对象"进入/离开"结果集，因此修改事件
// 需要比较新旧对象是否通过过滤，再映射为 Added/Modified/Deleted。
func (wc *watchChan) transform(e *event) (res *watch.Event, err error) {
	curObj, oldObj, err := wc.prepareObjs(e)
	if err != nil {
		klog.Errorf("failed to prepare current and previous objects: %v", err)
		return nil, err
	}

	switch {
	case e.isProgressNotify:
		object := wc.watcher.newFunc()
		if err := wc.watcher.versioner.UpdateObject(object, uint64(e.rev)); err != nil {
			klog.Errorf("failed to propagate object version: %v", err)
			return nil, fmt.Errorf("failed to propagate object resource version: %w", err)
		}
		if e.isInitialEventsEndBookmark {
			if err := storage.AnnotateInitialEventsEndBookmark(object); err != nil {
				return nil, fmt.Errorf("error while accessing object's metadata gr: %v, type: %v, obj: %#v, err: %w", wc.watcher.groupResource, wc.watcher.objectType, object, err)
			}
		}
		res = &watch.Event{
			Type:   watch.Bookmark,
			Object: object,
		}
	case e.isDeleted:
		if !wc.filter(oldObj) {
			return nil, nil
		}
		res = &watch.Event{
			Type:   watch.Deleted,
			Object: oldObj,
		}
	case e.isCreated:
		if !wc.filter(curObj) {
			return nil, nil
		}
		res = &watch.Event{
			Type:   watch.Added,
			Object: curObj,
		}
	default:
		if wc.acceptAll() {
			res = &watch.Event{
				Type:   watch.Modified,
				Object: curObj,
			}
			break
		}
		curObjPasses := wc.filter(curObj)
		oldObjPasses := wc.filter(oldObj)
		switch {
		case curObjPasses && oldObjPasses:
			res = &watch.Event{
				Type:   watch.Modified,
				Object: curObj,
			}
		case curObjPasses && !oldObjPasses:
			res = &watch.Event{
				Type:   watch.Added,
				Object: curObj,
			}
		case !curObjPasses && oldObjPasses:
			res = &watch.Event{
				Type:   watch.Deleted,
				Object: oldObj,
			}
		}
	}
	if res != nil && wc.recordTimestamps && !e.isInitialEvent {
		res.Object = &timedWatchEvent{Object: res.Object, recordTime: e.recordTime}
	}
	return res, nil
}

// timedWatchEvent 将解码后的 watch 对象与 storage 层完成解码的时间戳包装在一起，
// 使 watch cache 能够测量端到端的分发延迟。它只存在于 watch cache 的摄入路径上
// （以 ListOptions.RecordTimestamps 打开），并且在对象被存储或序列化之前，
// 必须由 watch cache 解开这层包装。
//
// 只用于测量延迟，不能泄漏到对外事件中：一旦被序列化就会暴露内部包装类型。
type timedWatchEvent struct {
	runtime.Object
	recordTime time.Time
}

var _ storage.WatchEventWithRecordTime = &timedWatchEvent{}

// GetObjectMeta 代理到底层对象，使包装后仍满足 metav1.Object 访问需求。
func (t *timedWatchEvent) GetObjectMeta() metav1.Object {
	m, _ := meta.Accessor(t.Object)
	return m
}

// DeepCopyObject 深拷贝底层对象并保留时间戳。
func (t *timedWatchEvent) DeepCopyObject() runtime.Object {
	return &timedWatchEvent{Object: t.Object.DeepCopyObject(), recordTime: t.recordTime}
}

// RecordTime 返回解码完成时间。
func (t *timedWatchEvent) RecordTime() time.Time {
	return t.recordTime
}

// Unwrap 返回被包装的原始对象。
func (t *timedWatchEvent) Unwrap() runtime.Object {
	return t.Object
}

// transformErrorToEvent 把任意错误转换为 watch.Error 事件。
// 非 APIStatus 错误统一包装为 InternalError，避免把内部错误细节原样透出。
func transformErrorToEvent(err error) *watch.Event {
	err = interpretWatchError(err)
	if _, ok := err.(apierrors.APIStatus); !ok {
		err = apierrors.NewInternalError(err)
	}
	status := err.(apierrors.APIStatus).Status()
	return &watch.Event{
		Type:   watch.Error,
		Object: &status,
	}
}

// sendError 同步把一个错误事件放入 resultChan，并触发取消所有 goroutine。
//
// 错误事件必须尽力送达（阻塞直到写入或被用户放弃），否则客户端只看到
// 通道关闭而不知道失败原因。
func (wc *watchChan) sendError(err error) {
	// 我们使用 wc.ctx 来回收所有 goroutine。无论在哪种情况下，都应当把它们全部停止。
	// 重复 cancel 也没关系。
	defer wc.cancel()

	if isCancelError(err) {
		return
	}
	errResult := transformErrorToEvent(err)
	if errResult != nil {
		// 保证用户在 ResultChan 关闭之前能收到这个错误结果。
		select {
		case wc.resultChan <- *errResult:
		case <-wc.ctx.Done(): // 用户已经放弃接收所有结果
		}
	}
}

// sendEvent 同步把一个事件放入 resultChan。
// 成功写入时返回 true。
//
// 反压设计：消费者慢时这里会阻塞，从而阻塞整条处理链，而不是无限缓存事件。
func (wc *watchChan) sendEvent(event *watch.Event) bool {
	defer func(start time.Time) { wc.resultLogger.recordWait(time.Since(start)) }(time.Now())

	// 如果用户接收结果不够快，我们也会阻塞 watcher 发来的事件。
	// 因为在本地缓存事件会造成更多内存占用。
	// 最坏的情况就是关闭这个快速的 watcher。
	select {
	case wc.resultChan <- *event:
		return true
	case <-wc.ctx.Done():
		return false
	}
}

// queueEvent 把事件放入处理队列，同时记录入队等待时间以发现处理瓶颈。
func (wc *watchChan) queueEvent(e *event) {
	defer func(start time.Time) { wc.incomingEventLogger.recordWait(time.Since(start)) }(time.Now())

	select {
	case wc.incomingEventChan <- e:
	case <-wc.ctx.Done():
	}
}

// prepareObjs 解密并解码事件中的当前对象与前一对象。
// 前一对象只在删除事件或存在过滤条件时才需要：无过滤时它必然通过，
// 解码它纯属浪费。删除事件的对象使用删除时的 etcd revision。
func (wc *watchChan) prepareObjs(e *event) (curObj runtime.Object, oldObj runtime.Object, err error) {
	if e.isProgressNotify {
		// progressNotify 事件既不包含当前对象版本也不包含先前对象版本，
		return nil, nil, nil
	}

	if !e.isDeleted {
		data, _, err := wc.watcher.transformer.TransformFromStorage(wc.ctx, e.value, authenticatedDataString(e.key))
		if err != nil {
			return nil, nil, err
		}
		curObj, err = decodeObj(wc.watcher.codec, wc.watcher.versioner, data, e.rev)
		if err != nil {
			return nil, nil, err
		}
	}
	// 只有在删除事件、或底层 filter 并非接受所有对象时才需要解码 prevValue
	// （否则我们知道对先前对象的 filter 必然返回 true，而这个对象只用于计算
	// 它此前是否被过滤掉）。
	if len(e.prevValue) > 0 && (e.isDeleted || !wc.acceptAll()) {
		data, _, err := wc.watcher.transformer.TransformFromStorage(wc.ctx, e.prevValue, authenticatedDataString(e.key))
		if err != nil {
			return nil, nil, wc.watcher.transformIfCorruptObjectError(e, err)
		}
		// 注意：这里发送的是*旧*对象，附带它被删除时刻的 etcd revision。
		oldObj, err = decodeObj(wc.watcher.codec, wc.watcher.versioner, data, e.rev)
		if err != nil {
			return nil, nil, wc.watcher.transformIfCorruptObjectError(e, err)
		}
	}
	return curObj, oldObj, nil
}

// corruptObjectDeletedError 表示删除事件携带的对象数据已损坏（解密或解码失败）。
// 单独成类型是为了让上层区分"数据损坏"与一般错误，从而决定是否继续。
type corruptObjectDeletedError struct {
	err error
}

// Error 描述"删除事件但对象损坏"这一特殊状态。
func (e *corruptObjectDeletedError) Error() string {
	return fmt.Sprintf("saw a DELETED event, but object data is corrupt - %v", e.err)
}

// Unwrap 暴露底层错误，支持 errors.Is/As 检查。
func (e *corruptObjectDeletedError) Unwrap() error { return e.err }

// transformIfCorruptObjectError 把"删除事件 + 对象损坏"包装成专门错误，
// 其它情况原样返回，避免改变普通错误的上报路径。
func (w *watcher) transformIfCorruptObjectError(e *event, err error) error {
	var corruptObjErr *corruptObjectError
	if !e.isDeleted || !errors.As(err, &corruptObjErr) {
		return err
	}

	// 走到这里说明我们收到了一个 DELETED 事件，但其关联对象数据已损坏，
	// 因为我们无法对该对象的数据执行 transform 或解码。
	// 包装原始错误，以便发送一个合适的 watch Error 事件。
	return &corruptObjectDeletedError{err: corruptObjErr}
}

// decodeObj 解码对象并把 etcd revision 写入其 resourceVersion。
// 测试环境（KUBE_PANIC_WATCH_DECODE_ERROR）下解码失败直接 panic，
// 以便在 CI 中暴露编解码器错误，而不是静默丢事件。
func decodeObj(codec runtime.Codec, versioner storage.Versioner, data []byte, rev int64) (_ runtime.Object, err error) {
	obj, err := runtime.Decode(codec, []byte(data))
	if err != nil {
		if fatalOnDecodeError.Load() {
			// 我们运行在测试环境中，如果 defer 没有捕获到这里，
			// 说明这里的错误是编解码器的失误
			panic(err)
		}
		return nil, err
	}
	// 确保从 etcd 读出的对象设置了 resource version
	if err := versioner.UpdateObject(obj, uint64(rev)); err != nil {
		return nil, fmt.Errorf("failure to version api object (%d) %#v: %v", rev, obj, err)
	}
	return obj, nil
}
