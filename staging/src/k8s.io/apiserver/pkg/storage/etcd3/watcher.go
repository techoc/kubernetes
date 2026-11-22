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
	// We have set a buffer in order to reduce times of context switches.
	//
	// 两个 100 容量的缓冲把"读 etcd"与"解码/分发"解耦：短时抖动由缓冲吸收，
	// 持续慢消费则由 blockLogger 记录告警。
	incomingBufSize         = 100
	outgoingBufSize         = 100
	processEventConcurrency = 10

	watchChanLogWarningInterval  = 5 * time.Second
	watchChanLogWarningThreshold = 100 * time.Millisecond
)

// defaultWatcherMaxLimit is used to facilitate construction tests
var defaultWatcherMaxLimit int64 = maxLimit

// fatalOnDecodeError is used during testing to panic the server if watcher encounters a decoding error
var fatalOnDecodeError atomic.Bool

func init() {
	// check to see if we are running in a test environment
	b, _ := strconv.ParseBool(os.Getenv("KUBE_PANIC_WATCH_DECODE_ERROR"))
	fatalOnDecodeError.Store(b)
}

// TestingTB is the subset of testing.TB required by TestOnlySetFatalOnDecodeError. Avoids importing
// the testing package.
type TestingTB interface {
	Helper()
	Cleanup(func())
}

// TestOnlySetFatalOnDecodeError should only be used for cases where decode errors are expected and need to be tested. e.g. conversion webhooks.
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

// watchChan implements watch.Interface.
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

// Watch watches on a key and returns a watch.Interface that transfers relevant notifications.
// If rev is zero, it will return the existing object(s) and then start watching from
// the maximum revision+1 from returned objects.
// If rev is non-zero, it will watch events happened after given revision.
// If opts.Recursive is false, it watches on given key.
// If opts.Recursive is true, it watches any children and directories under the key, excluding the root key itself.
// pred must be non-nil. Only if opts.Predicate matches the change, it will be returned.
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

	// For etcd watch we don't have an easy way to answer whether the watch
	// has already caught up. So in the initial version (given that watchcache
	// is by default enabled for all resources but Events), we just deliver
	// the initialization signal immediately. Improving this will be explored
	// in the future.
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
		// The filter doesn't filter out any object.
		// 用 Everything 而非 Empty，避免后续 filter 对每个对象都做无谓的判断。
		wc.internalPred = storage.Everything
	}
	wc.ctx, wc.cancel = context.WithCancel(ctx)

	wc.incomingEventLogger = newBlockLogger(watchChanLogWarningInterval, watchChanLogWarningThreshold, "Fast watcher, slow processing. Probably caused by slow decoding, user not receiving fast, or other processing logic", wc.watcher.objectType, wc.watcher.groupResource, clock.RealClock{})
	wc.resultLogger = newBlockLogger(watchChanLogWarningInterval, watchChanLogWarningThreshold, "Fast watcher, slow processing. Probably caused by slow dispatching events to watchers", wc.watcher.objectType, wc.watcher.groupResource, clock.RealClock{})

	return wc
}

// getStartWatchResourceVersion returns a ResourceVersion
// the watch will be started from.
// Depending on the input parameters the semantics of the returned ResourceVersion are:
//   - start at Exact (return resourceVersion)
//   - start at Most Recent (return an RV from etcd)
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
		// note that when opts.SendInitialEvents=true
		// we will be issuing a consistent LIST request
		// against etcd followed by the special bookmark event
		return 0, nil
	}
	// at this point the clients is interested
	// only in getting a stream of events
	// starting at the MostRecent point in time (RV)
	currentStorageRV, err := w.getCurrentStorageRV(ctx)
	if err != nil {
		return 0, err
	}
	// currentStorageRV is taken from resp.Header.Revision (int64)
	// and cast to uint64, so it is safe to do reverse
	// at some point we should unify the interface but that
	// would require changing  Versioner.UpdateList
	return int64(currentStorageRV), nil
}

// isInitialEventsEndBookmarkRequired since there is no way to directly set
// opts.ProgressNotify from the API and the etcd3 impl doesn't support
// notification for external clients we simply return initialEventsEndBookmarkRequired
// to only send the bookmark event after the initial list call.
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

// areInitialEventsRequired returns true if all events from the etcd should be returned.
//
// SendInitialEvents 未显式设置且 RV 为 0 属于旧的 list-then-watch 语义，同样需要初始事件。
func areInitialEventsRequired(resourceVersion int64, opts storage.ListOptions) bool {
	if opts.SendInitialEvents == nil && resourceVersion == 0 {
		return true // legacy case
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
	case <-wc.ctx.Done(): // user cancel
	}

	// We use wc.ctx to reap all goroutines. Under whatever condition, we should stop them all.
	// It's fine to double cancel.
	wc.cancel()

	// we need to wait until resultChan wouldn't be used anymore
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

// sync tries to retrieve existing data and send them to process.
// The revision to watch will be set to the revision in response.
// All events sent will have isCreated=true
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

		// send items from the response until no more results
		for i, kv := range getResp.Kvs {
			lastKey = kv.Key
			wc.queueEvent(parseKV(kv))
			// free kv early. Long lists can take O(seconds) to decode.
			getResp.Kvs[i] = nil
		}

		if withRev == 0 {
			wc.initialRev = getResp.Header.Revision
		}

		// no more results remain
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
		// Only record the listStream metric if streaming was actually exercised.
		if grpcstatus.Code(err) == grpccodes.Unimplemented {
			return
		}
		metrics.RecordEtcdRequest("listStream", wc.watcher.groupResource, err, startTime)
	}()

	var initialRev int64
	for r := range streamResp {
		if err = r.Err(); err != nil {
			// paging=false: a compaction mid-stream can't be resumed with a
			// continue token, so surface it as ResourceExpired for a relist.
			return interpretListError(err, false, wc.key, wc.key)
		}
		rangeResp := r.RangeResponse
		for i, kv := range rangeResp.Kvs {
			wc.queueEvent(parseKV(kv))
			// free kv early. Long lists can take O(seconds) to decode.
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
		// mvcc revision compaction which is regarded as warning, not error
		klog.Warningf("watch chan error: %v", err)
	case isCancelError(err):
		// expected when watches close, no need to log
	default:
		klog.Errorf("watch chan error: %v", err)
	}
}

// startWatching does:
// - get current objects if initialRev=0; set initialRev to current rev
// - watch on given key and send events to process.
//
// initialEventsEndBookmarkSent helps us keep track
// of whether we have sent an annotated bookmark event.
//
// it's important to note that we don't
// need to track the actual RV because
// we only send the bookmark event
// after the initial list call.
//
// when this variable is set to false,
// it means we don't have any specific
// preferences for delivering bookmark events.
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
			// If there is an error on server (e.g. compaction), the channel will return it before closed.
			logWatchChannelErr(err)
			// sendError doesn't guarantee that no more items will be put into resultChan.
			// However, by returning from startWatching here, we guarantee, that events
			// with higher resourceVersion than the error will not be queue and thus also
			// processed and send to the user.
			// TODO(wojtek-t): Figure out if we can synchronously prevent more events.
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
				// sendError doesn't guarantee that no more items will be put into resultChan.
				// However, by returning from startWatching here, we guarantee, that events
				// with higher resourceVersion than the error will not be queue and thus also
				// processed and send to the user.
				// TODO(wojtek-t): Figure out if we can synchronously prevent more events.
				wc.sendError(err)
				return
			}
			wc.queueEvent(parsedEvent)
		}
	}
	// When we come to this point, it's only possible that client side ends the watch.
	// e.g. cancel the context, close the client.
	// If this watch chan is broken and context isn't cancelled, other goroutines will still hang.
	// We should notify the main thread that this goroutine has exited.
	close(watchClosedCh)
}

// processEvents processes events from etcd watcher and sends results to resultChan.
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
	// Metadata for logging
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

// transform transforms an event into a result for user if not filtered.
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

// timedWatchEvent wraps a decoded watch object with the timestamp at which the
// storage layer decoded it, so the watch cache can measure end-to-end dispatch
// latency. It exists only on the watch cache's ingest path (opened with
// ListOptions.RecordTimestamps) and MUST be unwrapped by the watch cache before
// the object is stored or serialized.
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

// sendError synchronously puts an error event into resultChan and
// trigger cancelling all goroutines.
//
// 错误事件必须尽力送达（阻塞直到写入或被用户放弃），否则客户端只看到
// 通道关闭而不知道失败原因。
func (wc *watchChan) sendError(err error) {
	// We use wc.ctx to reap all goroutines. Under whatever condition, we should stop them all.
	// It's fine to double cancel.
	defer wc.cancel()

	if isCancelError(err) {
		return
	}
	errResult := transformErrorToEvent(err)
	if errResult != nil {
		// error result is guaranteed to be received by user before closing ResultChan.
		select {
		case wc.resultChan <- *errResult:
		case <-wc.ctx.Done(): // user has given up all results
		}
	}
}

// sendEvent synchronously puts an event into resultChan.
// Returns true if it was successful.
//
// 反压设计：消费者慢时这里会阻塞，从而阻塞整条处理链，而不是无限缓存事件。
func (wc *watchChan) sendEvent(event *watch.Event) bool {
	defer func(start time.Time) { wc.resultLogger.recordWait(time.Since(start)) }(time.Now())

	// If user couldn't receive results fast enough, we also block incoming events from watcher.
	// Because storing events in local will cause more memory usage.
	// The worst case would be closing the fast watcher.
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
		// progressNotify events doesn't contain neither current nor previous object version,
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
	// We need to decode prevValue, only if this is deletion event or
	// the underlying filter doesn't accept all objects (otherwise we
	// know that the filter for previous object will return true and
	// we need the object only to compute whether it was filtered out
	// before).
	if len(e.prevValue) > 0 && (e.isDeleted || !wc.acceptAll()) {
		data, _, err := wc.watcher.transformer.TransformFromStorage(wc.ctx, e.prevValue, authenticatedDataString(e.key))
		if err != nil {
			return nil, nil, wc.watcher.transformIfCorruptObjectError(e, err)
		}
		// Note that this sends the *old* object with the etcd revision for the time at
		// which it gets deleted.
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

	// if we are here it means we received a DELETED event but the object
	// associated with it is corrupt because we failed to transform or
	// decode the data associated with the object.
	// wrap the original error so we can send a proper watch Error event.
	return &corruptObjectDeletedError{err: corruptObjErr}
}

// decodeObj 解码对象并把 etcd revision 写入其 resourceVersion。
// 测试环境（KUBE_PANIC_WATCH_DECODE_ERROR）下解码失败直接 panic，
// 以便在 CI 中暴露编解码器错误，而不是静默丢事件。
func decodeObj(codec runtime.Codec, versioner storage.Versioner, data []byte, rev int64) (_ runtime.Object, err error) {
	obj, err := runtime.Decode(codec, []byte(data))
	if err != nil {
		if fatalOnDecodeError.Load() {
			// we are running in a test environment and thus an
			// error here is due to a coder mistake if the defer
			// does not catch it
			panic(err)
		}
		return nil, err
	}
	// ensure resource version is set on the object we load from etcd
	if err := versioner.UpdateObject(obj, uint64(rev)); err != nil {
		return nil, fmt.Errorf("failure to version api object (%d) %#v: %v", rev, obj, err)
	}
	return obj, nil
}
