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
	"time"

	"k8s.io/client-go/tools/record"
)

// ResourceAnalyzer 提供节点资源消耗统计。
type ResourceAnalyzer interface {
	// Start 启动资源分析所需的后台任务。
	Start(context.Context)

	fsResourceAnalyzerInterface
	SummaryProvider
}

// resourceAnalyzer 是 ResourceAnalyzer 的实现。
type resourceAnalyzer struct {
	*fsResourceAnalyzer
	SummaryProvider
}

var _ ResourceAnalyzer = &resourceAnalyzer{}

// NewResourceAnalyzer 创建新的 ResourceAnalyzer。
func NewResourceAnalyzer(ctx context.Context, statsProvider Provider, calVolumeFrequency time.Duration, eventRecorder record.EventRecorder) ResourceAnalyzer {
	fsAnalyzer := newFsResourceAnalyzer(statsProvider, calVolumeFrequency, eventRecorder)
	summaryProvider := NewSummaryProvider(ctx, statsProvider)
	return &resourceAnalyzer{fsAnalyzer, summaryProvider}
}

// Start 启动 ResourceAnalyzer 运行所需的后台任务。
func (ra *resourceAnalyzer) Start(ctx context.Context) {
	ra.fsResourceAnalyzer.Start(ctx)
}
