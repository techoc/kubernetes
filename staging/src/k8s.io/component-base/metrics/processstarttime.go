/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"time"

	"k8s.io/klog/v2"
)

// processStartTime 记录进程启动时间（unix 秒）。
// 与计数器配合可计算速率的分母；它是 lazy metric，注册前不会真正创建。
var processStartTime = NewGaugeVec(
	&GaugeOpts{
		Name:           "process_start_time_seconds",
		Help:           "Start time of the process since unix epoch in seconds.",
		StabilityLevel: ALPHA,
	},
	[]string{},
)

// RegisterProcessStartTime 将 process_start_time_seconds 注册到 prometheus registry，以确保计数器数据完整。
// RegisterProcessStartTime 把进程启动时间注册到给定 registry。
// 该指标用于推算进程生命周期内的计数器总量：仅凭 counter 的当前值
// 无法判断进程是否重启过导致了计数回退。
func RegisterProcessStartTime(registrationFunc func(Registerable) error) error {
	start, err := GetProcessStart()
	if err != nil {
		// 拿不到启动时间时退化为当前时间：宁可指标略有偏差，也不要让注册失败。
		klog.Errorf("Could not get process start time, %v", err)
		start = float64(time.Now().Unix())
	}
	// processStartTime 是延迟指标，注册后才会初始化，因此必须先注册再设置值。
	if err = registrationFunc(processStartTime); err != nil {
		return err
	}

	// 注册之后底层 gauge 才存在，此时再写入样本值。
	processStartTime.WithLabelValues().Set(start)
	return nil
}
