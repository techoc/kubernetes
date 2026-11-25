/*
Copyright 2018 The Kubernetes Authors.

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

package workloads

import (
	"time"

	v1 "k8s.io/api/core/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

// NodePerfWorkload 提供运行节点性能测试工作负载所需的全部信息。
type NodePerfWorkload interface {
	// Name 返回工作负载名称。
	Name() string
	// PodSpec 返回运行该工作负载所用的 PodSpec。
	PodSpec() v1.PodSpec
	// Timeout 返回该工作负载预期完成所需的时间。
	Timeout() time.Duration
	// KubeletConfig 返回该工作负载所需的 Kubelet 配置。
	KubeletConfig(old *kubeletconfig.KubeletConfiguration) (new *kubeletconfig.KubeletConfiguration, err error)
	// PreTestExec 定义在以工作负载所需的新 Kubelet 配置重启 Kubelet 之前
	// 需要执行的逻辑。
	PreTestExec() error
	// PostTestExec 定义在工作负载完成后需要执行的逻辑。
	PostTestExec() error
	// ExtractPerformanceFromLogs 从 Pod 日志中提取工作负载的性能数据。
	// 目前只支持以 time.Duration 格式报告的性能数据。
	ExtractPerformanceFromLogs(logs string) (perf time.Duration, err error)
}

// NodePerfWorkloads 是所有节点性能测试工作负载的集合。
var NodePerfWorkloads = []NodePerfWorkload{npbISWorkload{}, npbEPWorkload{}, pytorchWideDeepWorkload{}}
