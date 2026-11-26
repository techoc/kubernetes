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
	"time"

	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
)

// newMutatingWorkEstimator 构造写请求（mutation）的工作量估算器。
// 写请求会向所有 watcher 广播事件，因此成本与 watcher 数量相关。
func newMutatingWorkEstimator(countFn watchCountGetterFunc, config *WorkEstimatorConfig, maxSeatsFn maxSeatsFunc) WorkEstimatorFunc {
	estimator := &mutatingWorkEstimator{
		config:     config,
		countFn:    countFn,
		maxSeatsFn: maxSeatsFn,
	}
	return estimator.estimate
}

// mutatingWorkEstimator 保存估算所需配置与外部查询函数。
type mutatingWorkEstimator struct {
	config     *WorkEstimatorConfig
	countFn    watchCountGetterFunc
	maxSeatsFn maxSeatsFunc
}

// estimate 返回写请求的初始/最终席位与额外延迟。
// 思路：watcher 越多，事件扇出成本越高；用 finalSeats 表达总工作量，
// 用 additionalLatency 把“宽而短”的工作重塑为“窄而长”的矩形，以适配 P&F 的调度模型。
func (e *mutatingWorkEstimator) estimate(r *http.Request, flowSchemaName, priorityLevelName string) WorkEstimate {
	minSeats := e.config.MinimumSeats
	maxSeats := e.maxSeatsFn(priorityLevelName)
	if maxSeats == 0 || maxSeats > e.config.MaximumMutatingSeatsLimit {
		maxSeats = e.config.MaximumMutatingSeatsLimit
	}

	// TODO(wojtekt): 待算法调优到不会让
	// 扩展性测试失败后移除。
	if !e.config.Enabled {
		return WorkEstimate{
			InitialSeats: minSeats,
		}
	}

	requestInfo, ok := apirequest.RequestInfoFrom(r.Context())
	if !ok {
		// 不应出现没有 RequestInfo 的情况，但为安全起见
		// 返回一个较大的值。
		return WorkEstimate{
			InitialSeats:      minSeats,
			FinalSeats:        maxSeats,
			AdditionalLatency: e.config.eventAdditionalDuration(),
		}
	}

	if isRequestExemptFromWatchEvents(requestInfo) {
		return WorkEstimate{
			InitialSeats:      minSeats,
			FinalSeats:        0,
			AdditionalLatency: time.Duration(0),
		}
	}

	watchCount := e.countFn(requestInfo)
	metrics.ObserveWatchCount(r.Context(), priorityLevelName, flowSchemaName, watchCount)

	// 与该事件的 watcher 相关联的请求开销
	// 由三部分组成：
	// - 走完事件变更逻辑的开销
	// - 事件序列化的开销
	// - 每个 watcher 处理事件对象的开销（如过滤、
	//     通过网络发送数据）
	// 先从简单方案入手积累运行经验，
	// 之后再调优算法。处理 watch 事件的实际工作
	// 发生在多个 goroutine 中（数量与 watcher 数成正比），
	// 且它们同时恢复运行。作为起点，假设每个这样的 goroutine
	// 在 M 毫秒内占用 1/N 个 seat。
	// 出于实际考虑，允许在 P&F 中把这部分工作的记账
	// 重塑为面积相等的另一个矩形。
	var finalSeats uint64
	var additionalLatency time.Duration

	// TODO：算法调优更好后，将其改为无条件执行。
	//   严格来说，即使 watcher 数量很少，请求结束后处理事件
	//   仍然存在开销。
	//   但在调优估算之前，我们希望保守一些，
	//   避免给几乎每个请求都引入额外延迟。
	if watchCount >= int(e.config.WatchesPerSeat) {
		// TODO：如 KEP 所述，应考虑事件并非一律相同，
		//   并尝试根据事件大小的历史数据估算单个事件的开销。
		//
		finalSeats = uint64(math.Ceil(float64(watchCount) / e.config.WatchesPerSeat))
		finalWork := SeatsTimesDuration(float64(finalSeats), e.config.eventAdditionalDuration())

		// 虽然单个事件的处理高度并行，
		// 但 P&F 的设计/实现有几个限制，
		// 使得在 P&F 实现中使用这一假设非常低效，原因如下：
		//
		// - 我们在执行请求的两个阶段期间，都预留
		//   max(initialSeats, finalSeats)
		// - 更重要的是，当某个 `wide` 请求被选中分发时，
		//   在凑够足够 seats 分发它之前，不会分发其他请求，
		//   即使当前空闲的 seats 本可以分发其他请求。
		//
		// 因此，请求越宽，在分发和执行期间
		// 被有效阻塞和浪费的容量就越多。
		//
		// 被有效阻塞和浪费的容量就越多。
		//
		// 为缓解这一影响，我们限制单个请求可分配的最大 seats。这样做：
		//
		// 1) 减少在分发与执行初始阶段"浪费"的 seat-seconds
		//
		// 2) 不改变 finalWork 估算，只是可能把它重塑得更窄更长。只要
		// 最大 seats 设置能防止一次分发过多请求，
		// 从而避免 kube-apiserver（以及 etcd、VM 或物理机）过载，
		// 我们就认为这个放宽后的版本足以达成 P&F 的目标。
		//
		// 我们就认为这个放宽后的版本足以达成 P&F 的目标。
		//
		// TODO：确认当前 maximumSeats 上限确实能达到上述目标。
		//
		if finalSeats > maxSeats {
			finalSeats = maxSeats
		}
		additionalLatency = finalWork.DurationPerSeat(float64(finalSeats))
	}

	return WorkEstimate{
		InitialSeats:      1,
		FinalSeats:        finalSeats,
		AdditionalLatency: additionalLatency,
	}
}

// isRequestExemptFromWatchEvents 标记不会产生 watch 事件的写请求。
// serviceaccounts/token 只生成令牌、不写入存储，因此无需按 watcher 数计费。
func isRequestExemptFromWatchEvents(requestInfo *apirequest.RequestInfo) bool {
	// 为 service account 创建 token 不会产生任何事件，
	// 但 serviceaccount 仍可能有多个 watcher。
	if requestInfo.Resource == "serviceaccounts" && requestInfo.Subresource == "token" {
		return true
	}
	return false
}
