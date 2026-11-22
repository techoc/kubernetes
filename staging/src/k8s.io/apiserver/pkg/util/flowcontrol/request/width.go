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
	"fmt"
	"net/http"
	"time"

	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"

	"k8s.io/klog/v2"
)

// WorkEstimate carries three of the four parameters that determine the work in a request.
// The fourth parameter is the duration of the initial phase of execution.
//
// 中文说明：APF（API Priority and Fairness）用「座位（seat）」衡量请求成本。
// 请求在初始阶段占用 InitialSeats，在收尾阶段（可能更长）占用 FinalSeats；
// 第四维——初始阶段时长——由调用方另行测量，不在此结构中。
// 多阶段设计是为了让写请求在返回客户端后仍能为其后续的 watch 通知预留容量，
// 避免「已返回但仍在扩散」的请求瞬间压垮队列。
type WorkEstimate struct {
	// InitialSeats is the number of seats occupied while the server is
	// executing this request.
	InitialSeats uint64

	// FinalSeats is the number of seats occupied at the end,
	// during the AdditionalLatency.
	FinalSeats uint64

	// AdditionalLatency specifies the additional duration the seats allocated
	// to this request must be reserved after the given request had finished.
	// AdditionalLatency should not have any impact on the user experience, the
	// caller must not experience this additional latency.
	//
	// 中文说明：这段时间是「后台传播成本」，对客户端不可见；
	// 之所以要求不影响用户体验，是因为它不应计入请求的响应时延预算。
	AdditionalLatency time.Duration
}

// MaxSeats returns the maximum number of seats the request occupies over the
// phases of being served.
//
// 中文说明：调度时按峰值而非平均值预留，防止阶段切换时出现超额占用。
func (we *WorkEstimate) MaxSeats() int {
	if we.InitialSeats >= we.FinalSeats {
		return int(we.InitialSeats)
	}

	return int(we.FinalSeats)
}

// statsGetterFunc represents a function that gets the total
// number of objects for a given resource.
type statsGetterFunc func(string) (storage.Stats, error)

// watchCountGetterFunc represents a function that gets the total
// number of watchers potentially interested in a given request.
type watchCountGetterFunc func(*apirequest.RequestInfo) int

// MaxSeatsFunc represents a function that returns the maximum seats
// allowed for the work estimator for a given priority level.
type maxSeatsFunc func(priorityLevelName string) uint64

// NewWorkEstimator estimates the work that will be done by a given request,
// if no WorkEstimatorFunc matches the given request then the default
// work estimate of 1 seat is allocated to the request.
//
// 中文说明：两个子估算器共享同一配置与上限函数；maximumSeatsLimit 取 list 与 mutating
// 两个上限的较大值，因为这里只是粗略的兜底边界，精确上限由子估算器各自执行。
// 返回函数值而非结构体，使调用方无需感知内部状态。
func NewWorkEstimator(objectCountFn statsGetterFunc, watchCountFn watchCountGetterFunc, config *WorkEstimatorConfig, maxSeatsFn maxSeatsFunc) WorkEstimatorFunc {
	estimator := &workEstimator{
		maxSeatsFn:            maxSeatsFn,
		minimumSeats:          config.MinimumSeats,
		maximumSeatsLimit:     max(config.MaximumListSeatsLimit, config.MaximumMutatingSeatsLimit),
		listWorkEstimator:     newListWorkEstimator(objectCountFn, config, maxSeatsFn).estimate,
		mutatingWorkEstimator: newMutatingWorkEstimator(watchCountFn, config, maxSeatsFn),
	}
	return estimator.estimate
}

// WorkEstimatorFunc returns the estimated work of a given request.
// This function will be used by the Priority & Fairness filter to
// estimate the work of incoming requests.
type WorkEstimatorFunc func(request *http.Request, flowSchemaName, priorityLevelName string) WorkEstimate

// EstimateWork 让函数类型满足接口，便于在测试中替换为固定估算值。
func (e WorkEstimatorFunc) EstimateWork(r *http.Request, flowSchemaName, priorityLevelName string) WorkEstimate {
	return e(r, flowSchemaName, priorityLevelName)
}

type workEstimator struct {
	maxSeatsFn maxSeatsFunc
	// the minimum number of seats a request must occupy
	minimumSeats uint64
	// the default maximum number of seats a request can occupy
	maximumSeatsLimit uint64
	// listWorkEstimator estimates work for list request(s)
	listWorkEstimator WorkEstimatorFunc
	// mutatingWorkEstimator calculates the width of mutating request(s)
	mutatingWorkEstimator WorkEstimatorFunc
}

// estimate 按请求动词选择估算策略。
// 中文说明：取不到 RequestInfo 属于不应发生的内部错误；此时按优先级上限
// （再受全局上限约束）计费，宁可保守多收，也不能让未知请求免费通过而破坏公平性。
// watch 只有在启用 WatchList 特性时才按 list 计费，因为只有该模式下 watch 初期
// 会发送全量初始事件；否则 watch 本身几乎不占座位。
func (e *workEstimator) estimate(r *http.Request, flowSchemaName, priorityLevelName string) WorkEstimate {
	requestInfo, ok := apirequest.RequestInfoFrom(r.Context())
	if !ok {
		klog.ErrorS(fmt.Errorf("no RequestInfo found in context"), "Failed to estimate work for the request", "URI", r.RequestURI)
		// no RequestInfo should never happen, but to be on the safe side let's return maximumSeats
		maxSeats := e.maxSeatsFn(priorityLevelName)
		if maxSeats == 0 || maxSeats > e.maximumSeatsLimit {
			maxSeats = e.maximumSeatsLimit
		}
		return WorkEstimate{InitialSeats: maxSeats}
	}

	switch requestInfo.Verb {
	case "list":
		return e.listWorkEstimator.EstimateWork(r, flowSchemaName, priorityLevelName)
	case "watch":
		// WATCH supports `SendInitialEvents` option, which effectively means
		// that is starts with sending of the contents of a corresponding LIST call.
		// From that perspective, given that the watch only consumes APF seats
		// during its initialization (sending init events), its cost should then
		// be computed the same way as for a regular list.
		if utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
			return e.listWorkEstimator.EstimateWork(r, flowSchemaName, priorityLevelName)
		}
	case "create", "update", "patch", "delete":
		return e.mutatingWorkEstimator.EstimateWork(r, flowSchemaName, priorityLevelName)
	}

	// 读单个对象等其余请求成本可忽略，按最小座位计费即可。
	return WorkEstimate{InitialSeats: e.minimumSeats}
}
