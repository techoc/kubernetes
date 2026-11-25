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

package options

import (
	"time"

	"github.com/spf13/pflag"
	componentbaseconfig "k8s.io/component-base/config"
)

// DeprecatedOptions 保存仍需兼容的旧命令行参数；新部署应优先使用 KubeSchedulerConfiguration。
// TODO: 废弃参数全部移除后删除这些字段。
type DeprecatedOptions struct {
	componentbaseconfig.DebuggingConfiguration
	componentbaseconfig.ClientConnectionConfiguration
	// PodMaxInUnschedulablePodsDuration 限制 Pod 在 unschedulablePods 中的停留时间；
	// 超时后移入 backoffQ 或 activeQ，零值使用默认的 5 分钟。
	PodMaxInUnschedulablePodsDuration time.Duration
}

// AddFlags 将废弃参数注册到指定 FlagSet；nil 接收者表示未启用该兼容容器。
func (o *DeprecatedOptions) AddFlags(fs *pflag.FlagSet) {
	// 旧参数容器是可选依赖；未启用时不注册参数，避免对 nil 接收者解引用。
	if o == nil {
		return
	}

	// 这些参数仅用于兼容旧脚本，帮助文本会明确标记为 DEPRECATED。
	fs.BoolVar(&o.EnableProfiling, "profiling", true, "DEPRECATED: enable profiling via web interface host:port/debug/pprof/. This parameter is ignored if a config file is specified in --config.")
	fs.BoolVar(&o.EnableContentionProfiling, "contention-profiling", true, "DEPRECATED: enable block profiling, if profiling is enabled. This parameter is ignored if a config file is specified in --config.")
	fs.StringVar(&o.Kubeconfig, "kubeconfig", "", "DEPRECATED: path to kubeconfig file with authorization and master location information. This parameter is ignored if a config file is specified in --config.")
	fs.StringVar(&o.ContentType, "kube-api-content-type", "application/vnd.kubernetes.protobuf", "DEPRECATED: content type of requests sent to apiserver. This parameter is ignored if a config file is specified in --config.")
	fs.Float32Var(&o.QPS, "kube-api-qps", 50.0, "DEPRECATED: QPS to use while talking with kubernetes apiserver. This parameter is ignored if a config file is specified in --config.")
	fs.Int32Var(&o.Burst, "kube-api-burst", 100, "DEPRECATED: burst to use while talking with kubernetes apiserver. This parameter is ignored if a config file is specified in --config.")
	// 该参数是特定插件组合下 Pod 长时间停留在 unschedulablePods 队列的临时兼容开关。
	// 超过设定时间后，队列会把 Pod 移入 backoffQ 或 activeQ，使其重新获得调度机会。
	// TODO: https://github.com/kubernetes/kubernetes/issues/110175 完成后移除此参数。
	fs.DurationVar(&o.PodMaxInUnschedulablePodsDuration, "pod-max-in-unschedulable-pods-duration", 5*time.Minute, "DEPRECATED: the maximum time a pod can stay in unschedulablePods. If a pod stays in unschedulablePods for longer than this value, the pod will be moved from unschedulablePods to backoffQ or activeQ. This flag is deprecated and will be removed in a future version.")
}
