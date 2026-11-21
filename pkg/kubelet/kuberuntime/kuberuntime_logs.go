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

package kuberuntime

import (
	"context"
	"io"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/cri-client/pkg/logs"
)

// ReadLogs 读取容器日志并写入 stdout 与 stderr。
// 注意：只有在 follow 日志时才需要 containerID，否则传空字符串即可。
func (m *kubeGenericRuntimeManager) ReadLogs(ctx context.Context, path, containerID string, apiOpts *v1.PodLogOptions, stdout, stderr io.Writer) error {
	// 将 v1.PodLogOptions 转换为内部日志选项。
	now := time.Now()
	opts := &logs.LogOptions{
		TailLines:  apiOpts.TailLines,
		LimitBytes: apiOpts.LimitBytes,
		Follow:     apiOpts.Follow,
		Timestamp:  apiOpts.Timestamps,
	}
	if apiOpts.SinceSeconds != nil {
		opts.Since = now.Add(-time.Duration(*apiOpts.SinceSeconds) * time.Second)
	}
	if apiOpts.SinceTime != nil && apiOpts.SinceTime.After(opts.Since) {
		opts.Since = apiOpts.SinceTime.Time
	}

	return logs.ReadLogs(ctx, path, containerID, opts, m.runtimeService, stdout, stderr)
}
