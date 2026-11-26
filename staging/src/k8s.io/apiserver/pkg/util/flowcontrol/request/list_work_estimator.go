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

package request

import (
	"math"
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/delegator"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
)

const (
	// bytesPerSeat 是开启 SizeBasedListCostEstimate 后每个 seat 折算的内存量。
	// 该常数直接决定 list 请求在公平性队列中的权重，调整需配合压测。
	bytesPerSeat = 100_000
	// cacheWithStreamingMaxMemoryUsage 是流式（chunked）list 从缓存读取时计入的成本上限：
	// 流式发送意味着对象不会同时驻留内存，实际峰值远小于总量。
	cacheWithStreamingMaxMemoryUsage = 1_000_000
	// 1.5MB 是客户端请求大小的推荐值（字节），
	// etcd server 应当接受该大小。参见
	// https://github.com/etcd-io/etcd/blob/release-3.4/embed/config.go#L56.
	maxObjectSize = 1_500_000
	// infiniteObjectCount 用于“计数过期/未知”时保守估计：相当于把 list 视为无界。
	infiniteObjectCount = 1_000_000_000
)

// newListWorkEstimator 构造 list 请求的成本估算器；countFn 提供各资源的对象计数，
// maxSeatsFn 按优先级返回该层允许的最大 seats。
func newListWorkEstimator(countFn statsGetterFunc, config *WorkEstimatorConfig, maxSeatsFn maxSeatsFunc) *listWorkEstimator {
	estimator := &listWorkEstimator{
		config:        config,
		statsGetterFn: countFn,
		maxSeatsFn:    maxSeatsFn,
	}
	return estimator
}

// listWorkEstimator 依据对象计数或平均大小估算 list/watch 请求占用的 seats。
type listWorkEstimator struct {
	config        *WorkEstimatorConfig
	statsGetterFn statsGetterFunc
	maxSeatsFn    maxSeatsFunc
}

// estimate 返回该请求在 API 优先级与公平性队列中应占的初始 seats。
//
// 核心原则是宁可高估不可低估：低估会让单个大 list 请求挤占其他请求的带宽，
// 破坏优先级与公平性隔离；高估只影响自身的吞吐。因此所有无法判定的分支都返回上限。
func (e *listWorkEstimator) estimate(r *http.Request, flowSchemaName, priorityLevelName string) WorkEstimate {
	minSeats := e.config.MinimumSeats
	maxSeats := e.maxSeatsFn(priorityLevelName)
	// 优先级层配置的上限不能超过全局上限；为 0 表示未设置，回落到全局值。
	if maxSeats == 0 || maxSeats > e.config.MaximumListSeatsLimit {
		maxSeats = e.config.MaximumListSeatsLimit
	}

	requestInfo, ok := apirequest.RequestInfoFrom(r.Context())
	if !ok {
		// 不应出现没有 RequestInfo 的情况，但为了安全起见
		// 返回 maximumSeats。
		return WorkEstimate{InitialSeats: maxSeats}
	}

	// 指定了 metadata.name 的请求通常在存储层按 get 执行，宽度应为 1。
	//
	// 这类 list 请求示例：
	// /apis/certificates.k8s.io/v1/certificatesigningrequests?fieldSelector=metadata.name%3Dcsr-xxs4m
	// /api/v1/namespaces/test/configmaps?fieldSelector=metadata.name%3Dbig-deployment-1&limit=500&resourceVersion=0
	//
	// 按 name 精确查询在存储层等价于一次 get，成本与对象总数无关，因此单独走最便宜的分支。
	matchesSingle := requestInfo.Name != ""

	query := r.URL.Query()
	listOptions := metav1.ListOptions{}
	if err := metav1.Convert_url_Values_To_v1_ListOptions(&query, &listOptions, nil); err != nil {
		klog.ErrorS(err, "Failed to convert options while estimating work for the list request")

		// 该请求注定会在校验层失败，
		// 为保持一致，返回 maximumSeats。
		return WorkEstimate{InitialSeats: maxSeats}
	}

	// 对 watch 请求，若不请求初始事件，我们希望成本较低：
	// 无论是显式 SendInitialEvents 参数，还是 RV=0/未设置的旧式 watch。
	//
	// 普通 watch 建立后只推送增量，单次成本低；而带 SendInitialEvents 或 RV=0 的 watch
	// 会一次性回放全量，必须按 list 计费。
	if requestInfo.Verb == "watch" {
		sendInitEvents := listOptions.SendInitialEvents != nil && *listOptions.SendInitialEvents
		legacyWatch := listOptions.ResourceVersion == "" || listOptions.ResourceVersion == "0"
		if !sendInitEvents && !legacyWatch {
			return WorkEstimate{InitialSeats: e.config.MinimumSeats}
		}
	}
	// TODO: 检查是否启用了 watchcache。
	var listFromStorage bool
	result, err := delegator.ShouldDelegateListMeta(&listOptions, delegator.CacheWithoutSnapshots{})
	if err != nil {
		// 按最坏情况处理：需要访问 etcd。
		listFromStorage = true
	} else {
		listFromStorage = result.ShouldDelegate
	}
	// 从缓存读取的 list/watch 不会给 etcd 造成压力，但仍有内存与 CPU 成本，后续估算分开处理。
	isListFromCache := requestInfo.Verb == "watch" || !listFromStorage

	stats, err := e.statsGetterFn(key(requestInfo))
	switch {
	case err == ObjectCountStaleErr:
		// 对象计数过期说明系统已退化，这里应保守处理，
		// 返回最大的对象数量与大小。
		// 注意：CRD 被删除时，其计数会先过期，
		// 然后 pruner 最终把该 CRD 从缓存中移除。
		//
		// 计数过期本身是异常信号，按最坏情况计费可以避免在系统退化时进一步过载。
		stats = storage.Stats{ObjectCount: infiniteObjectCount, EstimatedAverageObjectSizeBytes: maxObjectSize}
	case err == ObjectCountNotFoundErr:
		// 出现该错误可能有多种情况：
		//  a. 类型确实未知，调用方写错了；
		//  b. 计数过期太久，pruner 已把该类型从缓存中移除；
		//     已从缓存中移除该类型。
		//  c. 类型是由其它 apiserver 提供的聚合资源（因此其对象计数不会更新）。
		//     不同的 apiserver（因此其对象计数不会更新）。
		// 我们无法区分这些情况。
		// 但在情况 c 下，请求会被委托给其它 apiserver，
		// 对本服务的开销极小。为避免高估聚合 API 调用，
		// 这里分配尽可能小的 seats（见 #109106：更保守的做法曾引发问题）。
		//
		// 引发过问题）。
		//
		// 与过期场景相反，这里取最小值：聚合资源在本服务上的真实开销接近于零，
		// 若按最大值计费会误伤使用聚合 API 的客户端。
		return WorkEstimate{InitialSeats: minSeats}
	case err != nil:
		// 按理不会走到这里，因为 Get 只返回 ObjectCountStaleErr 或
		// ObjectCountNotFoundErr；此处返回最大对象数量与大小。
		klog.ErrorS(err, "Unexpected error from object count tracker")
		stats = storage.Stats{ObjectCount: infiniteObjectCount, EstimatedAverageObjectSizeBytes: maxObjectSize}
	}

	var seats uint64
	// 两种估算策略由 feature gate 切换：按对象数（旧）与按对象大小（新），
	// 后者更能反映大对象 list 的真实内存压力。
	if utilfeature.DefaultFeatureGate.Enabled(features.SizeBasedListCostEstimate) {
		seats = e.seatsBasedOnObjectSize(stats, listOptions, isListFromCache, matchesSingle)
	} else {
		seats = e.seatsBasedOnObjectCount(stats, listOptions, isListFromCache, matchesSingle)
	}

	// 确保永远不会返回 0 个 seat。
	if seats < minSeats {
		seats = minSeats
	}
	if seats > maxSeats {
		seats = maxSeats
	}
	return WorkEstimate{InitialSeats: seats}
}

// seatsBasedOnObjectCount 用“待处理对象数 / ObjectsPerSeat”估算 seats（旧策略）。
func (e *listWorkEstimator) seatsBasedOnObjectCount(stats storage.Stats, listOptions metav1.ListOptions, isListFromCache bool, matchesSingle bool) uint64 {
	numStored := stats.ObjectCount
	limit := numStored
	if listOptions.Limit > 0 && listOptions.Limit < numStored {
		limit = listOptions.Limit
	}

	var estimatedObjectsToBeProcessed int64

	switch {
	case matchesSingle:
		estimatedObjectsToBeProcessed = 1
	case isListFromCache:
		// TODO: 对于在 watchcache 层实现索引的资源，
		//  需相应调整成本估算。
		estimatedObjectsToBeProcessed = numStored
	case listOptions.FieldSelector != "" || listOptions.LabelSelector != "":
		// 带选择器时存储层要先全量扫描再过滤，成本约为“总量 + 返回量”。
		estimatedObjectsToBeProcessed = numStored + limit
	default:
		// 无选择器：写入响应缓冲与序列化各算一遍，故取 2*limit。
		estimatedObjectsToBeProcessed = 2 * limit
	}

	// 目前粗略估算：list 请求每处理 100 个对象分配一个 seat。
	//
	// 未来迭代会为转换函数采用不同公式，并/或微调此数值。
	//
	return uint64(math.Ceil(float64(estimatedObjectsToBeProcessed) / e.config.ObjectsPerSeat))
}

// seatsBasedOnObjectSize 用“同时驻留内存的字节数 / bytesPerSeat”估算 seats（新策略）。
func (e *listWorkEstimator) seatsBasedOnObjectSize(stats storage.Stats, listOptions metav1.ListOptions, isListFromCache bool, matchesSingle bool) uint64 {
	// 平均大小缺失但对象数非零时，按 etcd 推荐的最大请求体大小兜底。
	if stats.EstimatedAverageObjectSizeBytes <= 0 && stats.ObjectCount != 0 {
		stats.EstimatedAverageObjectSizeBytes = maxObjectSize
	}
	limited := stats.ObjectCount
	if listOptions.Limit > 0 && listOptions.Limit < limited {
		limited = listOptions.Limit
	}
	var objectsLoadedInMemory int64
	switch {
	case matchesSingle:
		objectsLoadedInMemory = 1
	case isListFromCache:
		objectsLoadedInMemory = limited
	case listOptions.FieldSelector != "" || listOptions.LabelSelector != "":
		// 带选择器时无法预知命中率，按“返回量”与“总量一半”中较大者估计内存。
		objectsLoadedInMemory = max(limited, stats.ObjectCount/2)
	default:
		objectsLoadedInMemory = limited
	}

	memoryUsedAtOnce := objectsLoadedInMemory * stats.EstimatedAverageObjectSizeBytes
	if isListFromCache {
		// TODO: 识别资源是否为流式传输。
		// 从缓存读取可能是流式发送，峰值内存不会随对象总量线性增长，故设上限。
		memoryUsedAtOnce = min(memoryUsedAtOnce, cacheWithStreamingMaxMemoryUsage)
	}
	return uint64(math.Ceil(float64(memoryUsedAtOnce) / bytesPerSeat))
}

// key 生成统计缓存的键：group/resource，与 API 路径上的资源标识一致。
func key(requestInfo *apirequest.RequestInfo) string {
	groupResource := &schema.GroupResource{
		Group:    requestInfo.APIGroup,
		Resource: requestInfo.Resource,
	}
	return groupResource.String()
}
