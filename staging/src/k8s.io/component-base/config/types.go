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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClientConnectionConfiguration 包含构造客户端的详细信息。
// 供各组件在自身配置文件中嵌入，统一控制连接 apiserver 的 kubeconfig、内容类型与限流参数。
type ClientConnectionConfiguration struct {
	// kubeconfig 是 KubeConfig 文件路径；为空时客户端使用 in-cluster 配置。
	Kubeconfig string
	// acceptContentTypes 定义客户端连接服务器时发送的 Accept 头，覆盖默认的
	// 'application/json'，控制该客户端到服务器的全部连接。
	AcceptContentTypes string
	// contentType 是客户端向服务器发送数据时使用的内容类型。
	ContentType string
	// qps 控制此连接允许的每秒查询数，与 Burst 共同构成客户端令牌桶限流。
	QPS float32
	// burst 允许客户端在超过速率时累积额外查询。
	Burst int32
}

// LeaderElectionConfiguration 定义可启用 leader election 的组件的选主配置。
type LeaderElectionConfiguration struct {
	// leaderElect 让选主客户端在执行主循环前获取领导权；运行高可用副本组件时应启用。
	// 关闭后所有副本都会同时执行主循环，仅适用于单副本或无竞争场景。
	LeaderElect bool
	// leaseDuration 是非领导者候选者观察到续租后，尝试获取已失去续租的领导席位前
	// 等待的时长，实质上也是领导者停止后被其他候选者替换的最长时长。仅在启用选主时适用。
	LeaseDuration metav1.Duration
	// renewDeadline 是当前领导者在停止领导前尝试续租的间隔，必须小于或等于租期。
	// 仅在启用选主时适用。
	RenewDeadline metav1.Duration
	// retryPeriod 是客户端尝试获取与续租领导权之间等待的时长，仅在启用选主时适用。
	RetryPeriod metav1.Duration
	// resourceLock 指示选主周期中用于加锁的资源对象类型。
	ResourceLock string
	// resourceName 指示选主周期中用于加锁的资源对象名称。
	// 多个组件共享同一锁类型时，必须使用不同名字以避免互相抢占。
	ResourceName string
	// resourceNamespace 指示选主周期中用于加锁的资源对象命名空间。
	ResourceNamespace string
}

// DebuggingConfiguration 保存调试相关功能的配置。
type DebuggingConfiguration struct {
	// enableProfiling 通过 Web 界面 host:port/debug/pprof/ 启用性能分析。
	EnableProfiling bool
	// enableContentionProfiling 在 enableProfiling 为真时启用阻塞性能分析。
	// 两者共享 pprof HTTP 处理函数，因此仅在 EnableProfiling 为真时生效。
	EnableContentionProfiling bool
}
