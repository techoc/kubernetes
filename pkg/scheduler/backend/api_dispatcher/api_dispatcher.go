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

// Package apidispatcher 负责调度器内部的 API 调用分发。
package apidispatcher

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// APIDispatcher 实现 fwk.APIDispatcher 接口，允许将 API 调用排队并异步派发。
type APIDispatcher struct {
	cancel func()

	client    clientset.Interface
	callQueue *callQueue
}

// New 返回一个新的 APIDispatcher 对象。
func New(client clientset.Interface, parallelization int, apiCallRelevances fwk.APICallRelevances) *APIDispatcher {
	d := APIDispatcher{
		client:    client,
		callQueue: newCallQueue(apiCallRelevances),
	}

	return &d
}

// Add 把一个 API 调用加入派发器的队列。如果调用没有入队（例如被跳过），则返回错误。
// 调用方应当处理返回的 ErrCallSkipped。
func (ad *APIDispatcher) Add(newAPICall fwk.APICall, opts fwk.APICallOptions) error {
	apiCall := &queuedAPICall{
		APICall:  newAPICall,
		onFinish: opts.OnFinish,
	}
	return ad.callQueue.add(apiCall)
}

// SyncObject 在给定对象与派发器中待处理的 API 调用之间做双向同步，返回修改后的对象。
func (ad *APIDispatcher) SyncObject(obj metav1.Object) (metav1.Object, error) {
	return ad.callQueue.syncObject(obj)
}

// Run 启动 APIDispatcher 的主处理循环，从队列中取出调用并交给 worker goroutine 执行。
func (ad *APIDispatcher) Run(logger klog.Logger) {
	// 使用独立的 context，这样关闭 APIDispatcher 时可以取消正在执行的 API 调用。
	ctx, cancel := context.WithCancel(context.Background())
	ad.cancel = cancel

	go func() {
		for {
			select {
			case <-ctx.Done():
				// APIDispatcher 已关闭。
				return
			default:
			}

			apiCall, err := ad.callQueue.pop()
			if err != nil {
				utilruntime.HandleErrorWithLogger(logger, err, "popping API call from call controller failed")
				continue
			}
			if apiCall == nil {
				// callController 已关闭。
				return
			}

			go func() {
				startTime := time.Now()

				err := apiCall.Execute(ctx, ad.client)

				result := metrics.GoroutineResultSuccess
				if err != nil {
					result = metrics.GoroutineResultError
				}
				callType := string(apiCall.CallType())
				metrics.AsyncAPICallsTotal.WithLabelValues(callType, result).Inc()
				metrics.AsyncAPICallDuration.WithLabelValues(callType, result).Observe(time.Since(startTime).Seconds())

				ad.callQueue.finalize(apiCall)
				apiCall.sendOnFinish(err)
			}()
		}
	}()
}

// Close 关闭 APIDispatcher。
func (ad *APIDispatcher) Close() {
	ad.callQueue.close()
	if ad.cancel != nil {
		ad.cancel()
	}
}
