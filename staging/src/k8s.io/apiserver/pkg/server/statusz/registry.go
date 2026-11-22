/*
Copyright 2024 The Kubernetes Authors.

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

// Package statusz 为 /statusz 端点提供构建版本、模拟版本、进程启动时间
// 与已注册路径等运行时信息。
package statusz

import (
	"time"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/compatibility"
	"k8s.io/klog/v2"

	compbasemetrics "k8s.io/component-base/metrics"
	utilversion "k8s.io/component-base/version"
)

// statuszRegistry 是 statusz 处理器读取数据的只读视图。抽成接口便于
// 测试注入固定值，避免依赖真实进程状态。
type statuszRegistry interface {
	processStartTime() time.Time
	goVersion() string
	binaryVersion() *version.Version
	emulationVersion() *version.Version
	paths() []string
	deprecatedVersions() map[string]bool
}

// registry 是 statuszRegistry 的默认实现。
type registry struct {
	// componentGlobalsRegistry compatibility.ComponentGlobalsRegistry
	effectiveVersion compatibility.EffectiveVersion
	// listedPaths is an alphabetically sorted list of paths to be reported at /.
	listedPaths []string
	// deprecatedVersionsMap is a map of deprecated statusz versions.
	deprecatedVersionsMap map[string]bool
}

// Option is a function to configure registry.
type Option func(reg *registry)

// WithListedPaths returns an Option to configure the ListedPaths.
//
// WithListedPaths 复制传入切片，防止调用方后续修改影响已构建的 registry。
func WithListedPaths(listedPaths []string) Option {
	cpyListedPaths := make([]string, len(listedPaths))
	copy(cpyListedPaths, listedPaths)

	return func(reg *registry) { reg.listedPaths = cpyListedPaths }
}

// processStartTime 返回进程启动时间；获取失败时记录错误并按 0 处理，
// 保证端点仍可响应而不是整体失败。
func (*registry) processStartTime() time.Time {
	start, err := compbasemetrics.GetProcessStart()
	if err != nil {
		klog.Errorf("Could not get process start time, %v", err)
	}

	return time.Unix(int64(start), 0)
}

// goVersion 返回构建该二进制时使用的 Go 版本。
func (*registry) goVersion() string {
	return utilversion.Get().GoVersion
}

// binaryVersion 优先返回特性门控体系中的二进制版本；未配置时回退到
// 编译期写入的版本字符串。
func (r *registry) binaryVersion() *version.Version {
	if r.effectiveVersion != nil {
		return r.effectiveVersion.BinaryVersion()
	}
	return version.MustParse(utilversion.Get().String())
}

// emulationVersion 返回模拟版本；不支持版本模拟的组件返回 nil。
func (r *registry) emulationVersion() *version.Version {
	if r.effectiveVersion != nil {
		return r.effectiveVersion.EmulationVersion()
	}

	return nil
}

// paths 返回对外公布的路径清单；未配置时返回 nil。
func (r *registry) paths() []string {
	if r.listedPaths != nil {
		return r.listedPaths
	}

	return nil
}

// deprecatedVersions 返回 statusz 各版本的弃用状态。
func (r *registry) deprecatedVersions() map[string]bool {
	return r.deprecatedVersionsMap
}
