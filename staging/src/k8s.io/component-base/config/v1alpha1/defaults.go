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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// RecommendedDefaultLeaderElectionConfiguration 为 LeaderElectionConfiguration 指针填充推荐默认值。
// 推荐值可能随 API 版本变化。本函数刻意不作为普通 `SetDefaults_Foo` 注册到 scheme，
// 以允许使用者为嵌入的配置自行设置默认值：scheme 默认化是转换的一部分，强制使用这些值
// 将使调用方难以退出。需要此默认化逻辑时，应在本类型包装结构体的 `SetDefaults_` 方法中调用。
// 默认值刻意保持保守：租约 15s、续约 10s、重试 2s，在多数部署下能容忍短暂网络抖动。
func RecommendedDefaultLeaderElectionConfiguration(obj *LeaderElectionConfiguration) {
	// 仅当字段为零值时才填充，避免覆盖调用方或配置文件已提供的值。
	zero := metav1.Duration{}
	if obj.LeaseDuration == zero {
		obj.LeaseDuration = metav1.Duration{Duration: 15 * time.Second}
	}
	if obj.RenewDeadline == zero {
		obj.RenewDeadline = metav1.Duration{Duration: 10 * time.Second}
	}
	if obj.RetryPeriod == zero {
		obj.RetryPeriod = metav1.Duration{Duration: 2 * time.Second}
	}
	if obj.ResourceLock == "" {
		// TODO(#80289)：确定此处如何迁移到 LeaseLock，很可能需要先经过 EndpointsLease。
		// 保留 Endpoints 锁作为默认，是因为直接切换会影响仍在用旧锁的组件。
		obj.ResourceLock = EndpointsResourceLock
	}
	if obj.LeaderElect == nil {
		obj.LeaderElect = ptr.To(true)
	}
}

// RecommendedDefaultClientConnectionConfiguration 为 ClientConnectionConfiguration 指针填充推荐默认值。
// 推荐值可能随 API 版本变化。本函数刻意不作为普通 `SetDefaults_Foo` 注册到 scheme，
// 以允许使用者为嵌入的配置自行设置默认值：scheme 默认化是转换的一部分，强制使用这些值
// 将使调用方难以退出。需要此默认化逻辑时，应在本类型包装结构体的 `SetDefaults_` 方法中调用。
// 默认使用 protobuf 并提供 50 QPS/100 Burst 的宽松限流，适合控制平面内部通信。
func RecommendedDefaultClientConnectionConfiguration(obj *ClientConnectionConfiguration) {
	if len(obj.ContentType) == 0 {
		obj.ContentType = "application/vnd.kubernetes.protobuf"
	}
	if obj.QPS == 0.0 {
		obj.QPS = 50.0
	}
	if obj.Burst == 0 {
		obj.Burst = 100
	}
}

// RecommendedDebuggingConfiguration 为性能分析与调试配置填充推荐默认值。
// 推荐值可能随 API 版本变化。本函数刻意不作为普通 `SetDefaults_Foo` 注册到 scheme，
// 以允许使用者为嵌入的配置自行设置默认值：scheme 默认化是转换的一部分，强制使用这些值
// 将使调用方难以退出。需要此默认化逻辑时，应在本类型包装结构体的 `SetDefaults_` 方法中调用。
// 默认开启 profiling：pprof 端点开销低，且是排查线上问题的常规手段。
func RecommendedDebuggingConfiguration(obj *DebuggingConfiguration) {
	if obj.EnableProfiling == nil {
		obj.EnableProfiling = ptr.To(true) // 性能分析端点开销低，且是 Kubernetes 二进制的标准配置。
	}
}

// NewRecommendedDebuggingConfiguration 返回当前推荐的 DebuggingConfiguration。
// 推荐值可能随版本发布而变化。返回独立副本，调用方可安全修改而不影响其他使用者。
func NewRecommendedDebuggingConfiguration() *DebuggingConfiguration {
	ret := &DebuggingConfiguration{}
	RecommendedDebuggingConfiguration(ret)
	return ret
}
