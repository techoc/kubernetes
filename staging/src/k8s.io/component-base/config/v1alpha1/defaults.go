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

// RecommendedDefaultLeaderElectionConfiguration defaults a pointer to a
// LeaderElectionConfiguration struct. This will set the recommended default
// values, but they may be subject to change between API versions. This function
// is intentionally not registered in the scheme as a "normal" `SetDefaults_Foo`
// function to allow consumers of this type to set whatever defaults for their
// embedded configs. Forcing consumers to use these defaults would be problematic
// as defaulting in the scheme is done as part of the conversion, and there would
// be no easy way to opt-out. Instead, if you want to use this defaulting method
// run it in your wrapper struct of this type in its `SetDefaults_` method.
//
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
		// TODO(#80289): Figure out how to migrate to LeaseLock at this point.
		//   This will most probably require going through EndpointsLease first.
		// 保留 Endpoints 锁作为默认，是因为直接切换会影响仍在用旧锁的组件。
		obj.ResourceLock = EndpointsResourceLock
	}
	if obj.LeaderElect == nil {
		obj.LeaderElect = ptr.To(true)
	}
}

// RecommendedDefaultClientConnectionConfiguration defaults a pointer to a
// ClientConnectionConfiguration struct. This will set the recommended default
// values, but they may be subject to change between API versions. This function
// is intentionally not registered in the scheme as a "normal" `SetDefaults_Foo`
// function to allow consumers of this type to set whatever defaults for their
// embedded configs. Forcing consumers to use these defaults would be problematic
// as defaulting in the scheme is done as part of the conversion, and there would
// be no easy way to opt-out. Instead, if you want to use this defaulting method
// run it in your wrapper struct of this type in its `SetDefaults_` method.
//
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

// RecommendedDebuggingConfiguration defaults profiling and debugging configuration.
// This will set the recommended default
// values, but they may be subject to change between API versions. This function
// is intentionally not registered in the scheme as a "normal" `SetDefaults_Foo`
// function to allow consumers of this type to set whatever defaults for their
// embedded configs. Forcing consumers to use these defaults would be problematic
// as defaulting in the scheme is done as part of the conversion, and there would
// be no easy way to opt-out. Instead, if you want to use this defaulting method
// run it in your wrapper struct of this type in its `SetDefaults_` method.
//
// 默认开启 profiling：pprof 端点开销低，且是排查线上问题的常规手段。
func RecommendedDebuggingConfiguration(obj *DebuggingConfiguration) {
	if obj.EnableProfiling == nil {
		obj.EnableProfiling = ptr.To(true) // profile debugging is cheap to have exposed and standard on kube binaries
	}
}

// NewRecommendedDebuggingConfiguration returns the current recommended DebuggingConfiguration.
// This may change between releases as recommendations shift.
//
// 返回独立副本，调用方可安全修改而不影响其他使用者。
func NewRecommendedDebuggingConfiguration() *DebuggingConfiguration {
	ret := &DebuggingConfiguration{}
	RecommendedDebuggingConfiguration(ret)
	return ret
}
