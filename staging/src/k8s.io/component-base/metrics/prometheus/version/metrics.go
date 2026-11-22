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

// Package version 注册 kubernetes_build_info 指标，用于对外暴露二进制的构建信息。
//
// 该指标是 "constant 1" 型 gauge：值恒为 1，全部信息都放在 label 中，
// 便于按版本、平台等维度做聚合与告警。
package version

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/version"
)

var (
	// buildInfo 恒为 1，label 携带构建时的版本信息。
	// 保持 BETA：label 集合的变更受 metrics 稳定性策略约束。
	buildInfo = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Name:           "kubernetes_build_info",
			Help:           "A metric with a constant '1' value labeled by major, minor, git version, git commit, git tree state, build date, Go version, and compiler from which Kubernetes was built, and platform on which it is running.",
			StabilityLevel: metrics.BETA,
		},
		[]string{"major", "minor", "git_version", "git_commit", "git_tree_state", "build_date", "go_version", "compiler", "platform"},
	)
)

// RegisterBuildInfo registers the build and version info in a metadata metric in prometheus
//
// init 在包导入时完成注册并立即写入唯一一条样本：
// 构建信息在进程生命周期内不会变化，因此不存在后续更新。
func init() {
	info := version.Get()
	// 使用 legacyregistry，确保默认 /metrics 端点无需额外配置即可看到该指标。
	legacyregistry.MustRegister(buildInfo)
	buildInfo.WithLabelValues(info.Major, info.Minor, info.GitVersion, info.GitCommit, info.GitTreeState, info.BuildDate, info.GoVersion, info.Compiler, info.Platform).Set(1)
}
