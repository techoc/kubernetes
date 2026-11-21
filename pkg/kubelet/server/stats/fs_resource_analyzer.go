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

package stats

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"

	"k8s.io/klog/v2"
)

// statCache 是 Pod UID 到卷统计计算器的映射。
type statCache map[types.UID]*volumeStatCalculator

// fsResourceAnalyzerInterface 用于把文件系统统计能力嵌入 ResourceAnalyzer。
type fsResourceAnalyzerInterface interface {
	// GetPodVolumeStats 返回指定 Pod 的卷统计。
	GetPodVolumeStats(uid types.UID) (PodVolumeStats, bool)
}

// fsResourceAnalyzer 提供文件系统资源使用统计。
type fsResourceAnalyzer struct {
	// statsProvider 是 Pod 统计信息来源。
	statsProvider Provider
	// calcPeriod 是卷统计的计算周期。
	calcPeriod time.Duration
	// cachedVolumeStats 原子地保存 statCache。
	cachedVolumeStats atomic.Value
	// startOnce 保证后台采集协程只启动一次。
	startOnce sync.Once
	// eventRecorder 用于记录卷统计相关事件。
	eventRecorder record.EventRecorder
}

// 编译期断言：fsResourceAnalyzer 实现了 fsResourceAnalyzerInterface 接口。
var _ fsResourceAnalyzerInterface = &fsResourceAnalyzer{}

// newFsResourceAnalyzer 创建 fsResourceAnalyzer 实例。
func newFsResourceAnalyzer(statsProvider Provider, calcVolumePeriod time.Duration, eventRecorder record.EventRecorder) *fsResourceAnalyzer {
	r := &fsResourceAnalyzer{
		statsProvider: statsProvider,
		calcPeriod:    calcVolumePeriod,
		eventRecorder: eventRecorder,
	}
	r.cachedVolumeStats.Store(make(statCache))
	return r
}

// Start 启动卷统计的后台缓存。
func (s *fsResourceAnalyzer) Start(ctx context.Context) {
	logger := klog.FromContext(ctx)
	s.startOnce.Do(func() {
		// 计算周期非正数时禁用卷统计采集。
		if s.calcPeriod <= 0 {
			logger.Info("Volume stats collection disabled")
			return
		}
		logger.Info("Starting FS ResourceAnalyzer")
		// 后台按周期刷新缓存。
		go wait.Forever(func() { s.updateCachedPodVolumeStats(logger) }, s.calcPeriod)
	})
}

// updateCachedPodVolumeStats 计算并缓存 kubelet 已知所有 Pod 的卷统计。
func (s *fsResourceAnalyzer) updateCachedPodVolumeStats(logger klog.Logger) {
	oldCache := s.cachedVolumeStats.Load().(statCache)
	newCache := make(statCache)

	// 复制已有条目，并为缓存中缺失的 Pod 创建并启动新的计算器。
	for _, pod := range s.statsProvider.GetPods() {
		if value, found := oldCache[pod.GetUID()]; !found {
			newCache[pod.GetUID()] = newVolumeStatCalculator(s.statsProvider, s.calcPeriod, pod, s.eventRecorder).StartOnce(logger)
		} else {
			newCache[pod.GetUID()] = value
		}
	}

	// 停止已删除 Pod 对应的计算器。
	for uid, entry := range oldCache {
		if _, found := newCache[uid]; !found {
			entry.StopOnce()
		}
	}

	// 原子替换缓存引用。
	s.cachedVolumeStats.Store(newCache)
}

// GetPodVolumeStats 返回指定 Pod 的卷统计。
// 结果从后台预热的缓存读取，不会现场计算。
func (s *fsResourceAnalyzer) GetPodVolumeStats(uid types.UID) (PodVolumeStats, bool) {
	// 缓存只由后台协程替换，读取无需加锁。
	cache := s.cachedVolumeStats.Load().(statCache)
	statCalc, found := cache[uid]
	if !found {
		// TODO: Differentiate between stats being empty
		// See issue #20679
		return PodVolumeStats{}, false
	}
	return statCalc.GetLatest()
}
