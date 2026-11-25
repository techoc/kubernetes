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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EndpointsResourceLock 是历史默认的 leader election 资源锁类型；
// 新组件应使用 leases，保留该常量仅为兼容旧配置。
const EndpointsResourceLock = "endpoints"

// LeaderElectionConfiguration 描述启用了 leader election 的组件的选主参数。
// 仅当 LeaderElect 为 true 时其余字段才有意义（默认化与校验也据此跳过）。
type LeaderElectionConfiguration struct {
	// leaderElect 让选主客户端在执行主循环前获取领导权；运行高可用副本组件时应启用。
	LeaderElect *bool `json:"leaderElect"`
	// leaseDuration 是非领导者候选者观察到续租后，尝试获取已失去续租的领导席位前
	// 等待的时长，实质上也是领导者停止后被其他候选者替换的最长时长。仅在启用选主时适用。
	LeaseDuration metav1.Duration `json:"leaseDuration"`
	// renewDeadline 是当前领导者在停止领导前尝试续租的间隔，必须小于或等于租期。
	// 仅在启用选主时适用。
	RenewDeadline metav1.Duration `json:"renewDeadline"`
	// retryPeriod 是客户端尝试获取与续租领导权之间等待的时长，仅在启用选主时适用。
	RetryPeriod metav1.Duration `json:"retryPeriod"`
	// resourceLock 指示选主周期中用于加锁的资源对象类型。
	ResourceLock string `json:"resourceLock"`
	// resourceName 指示选主周期中用于加锁的资源对象名称。
	ResourceName string `json:"resourceName"`
	// resourceNamespace 指示选主周期中用于加锁的资源对象命名空间。
	ResourceNamespace string `json:"resourceNamespace"`
}

// DebuggingConfiguration 保存调试相关功能的配置，控制 pprof 相关端点。
// 两个开关都是指针，以区分“未配置（走默认值）”与“显式设为 false”。
type DebuggingConfiguration struct {
	// enableProfiling 通过 Web 界面 host:port/debug/pprof/ 启用性能分析。
	EnableProfiling *bool `json:"enableProfiling,omitempty"`
	// enableContentionProfiling 在 enableProfiling 为真时启用阻塞性能分析。
	EnableContentionProfiling *bool `json:"enableContentionProfiling,omitempty"`
}

// ClientConnectionConfiguration 包含构造客户端的详细信息。
// 它是构造 client-go 配置所需的公共参数；内部版本位于 k8s.io/component-base/config，
// 此处是对外暴露的版本。
type ClientConnectionConfiguration struct {
	// kubeconfig 是 KubeConfig 文件路径。
	Kubeconfig string `json:"kubeconfig"`
	// acceptContentTypes 定义客户端连接服务器时发送的 Accept 头，覆盖默认的
	// 'application/json'，控制该客户端到服务器的全部连接。
	AcceptContentTypes string `json:"acceptContentTypes"`
	// contentType 是客户端向服务器发送数据时使用的内容类型。
	ContentType string `json:"contentType"`
	// qps 控制此连接允许的每秒查询数。
	QPS float32 `json:"qps"`
	// burst 允许客户端在超过速率时累积额外查询。
	Burst int32 `json:"burst"`
}
