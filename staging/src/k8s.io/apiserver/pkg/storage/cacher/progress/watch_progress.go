/*
Copyright 2023 The Kubernetes Authors.

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

// Package progress 实现按需向 etcd 请求 watch 进度（bookmark）的机制，
// 用于让缓存尽快追上最新资源版本。
//
// 中文说明：只有存在等待者时才周期性请求，避免在空闲时产生无谓的 etcd 负载。
package progress

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// progressRequestPeriod determines period of requesting progress
	// from etcd when there is a request waiting for watch cache to be fresh.
	// 中文说明：仅在有等待者时才以该周期请求 progress（bookmark），避免空转流量。
	progressRequestPeriod = 100 * time.Millisecond
)

// NewConditionalProgressRequester 构造条件式 progress 请求器；cond 绑定 pr.mux，
// 所有等待/唤醒都必须在锁内进行。
func NewConditionalProgressRequester(requestWatchProgress WatchProgressRequester, clock TickerFactory, contextMetadata metadata.MD) *ConditionalProgressRequester {
	pr := &ConditionalProgressRequester{
		clock:                clock,
		requestWatchProgress: requestWatchProgress,
		contextMetadata:      contextMetadata,
	}
	pr.cond = sync.NewCond(&pr.mux)
	return pr
}

type WatchProgressRequester func(ctx context.Context) error

// TickerFactory 抽象时钟，测试可注入假时钟避免真实等待。
type TickerFactory interface {
	NewTimer(time.Duration) clock.Timer
}

// ConditionalProgressRequester will request progress notification if there
// is a request waiting for watch cache to be fresh.
//
// 中文说明：waiting 是“等待缓存追平的调用方数量”，由 Add/Remove 维护；
// 只有大于 0 才向 etcd 请求 progress，避免无意义的 bookmark 流量。
type ConditionalProgressRequester struct {
	clock                TickerFactory
	requestWatchProgress WatchProgressRequester
	contextMetadata      metadata.MD

	mux     sync.Mutex
	cond    *sync.Cond
	waiting int
	stopped bool
}

// Run 启动请求循环，直到 stopCh 关闭。
// 中文说明：stopCh 关闭时设置 stopped 并唤醒 cond，保证等待循环不会漏掉退出信号。
func (pr *ConditionalProgressRequester) Run(stopCh <-chan struct{}) {
	ctx := wait.ContextForChannel(stopCh)
	if pr.contextMetadata != nil {
		ctx = metadata.NewOutgoingContext(ctx, pr.contextMetadata)
	}
	go func() {
		defer utilruntime.HandleCrashWithContext(ctx)
		<-stopCh
		pr.mux.Lock()
		defer pr.mux.Unlock()
		pr.stopped = true
		pr.cond.Signal()
	}()
	timer := pr.clock.NewTimer(progressRequestPeriod)
	defer timer.Stop()
	for {
		// 无等待者时阻塞在 cond 上，不消耗定时器；这是“条件式”的关键。
		stopped := func() bool {
			pr.mux.Lock()
			defer pr.mux.Unlock()
			for pr.waiting == 0 && !pr.stopped {
				pr.cond.Wait()
			}
			return pr.stopped
		}()
		if stopped {
			return
		}

		select {
		case <-timer.C():
			shouldRequest := func() bool {
				pr.mux.Lock()
				defer pr.mux.Unlock()
				return pr.waiting > 0 && !pr.stopped
			}()
			if !shouldRequest {
				timer.Reset(0)
				continue
			}
			timer.Reset(progressRequestPeriod)
			err := pr.requestWatchProgress(ctx)
			if err != nil {
				klog.V(4).InfoS("Error requesting bookmark", "err", err)
			}
		case <-stopCh:
			return
		}
	}
}

func (pr *ConditionalProgressRequester) Add() {
	// 唤醒 Run 中的 cond.Wait，让请求循环重新开始繁忙状态。
	pr.mux.Lock()
	defer pr.mux.Unlock()
	pr.waiting += 1
	pr.cond.Signal()
}

func (pr *ConditionalProgressRequester) Remove() {
	// 减少等待者；Run 循环会在下次 tick 时自行判断是否停止请求。
	pr.mux.Lock()
	defer pr.mux.Unlock()
	pr.waiting -= 1
}
