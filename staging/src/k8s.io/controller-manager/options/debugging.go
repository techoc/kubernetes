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
	"github.com/spf13/pflag"

	componentbaseconfig "k8s.io/component-base/config"
)

// DebuggingOptions holds the Debugging options.
// DebuggingOptions 封装与调试相关的命令行选项，
// 内部直接嵌入 component-base 的 DebuggingConfiguration 以复用其字段。
type DebuggingOptions struct {
	*componentbaseconfig.DebuggingConfiguration
}

// RecommendedDebuggingOptions returns the currently recommended debugging options.  These are subject to change
// between releases as we add options and decide which features should be exposed or not by default.
// 返回当前推荐的调试选项默认值（默认开启 profiling）。
// 这些默认值可能随版本演进而调整。
func RecommendedDebuggingOptions() *DebuggingOptions {
	return &DebuggingOptions{
		DebuggingConfiguration: &componentbaseconfig.DebuggingConfiguration{
			// profiling 的暴露成本很低，且是 Kubernetes 二进制的常规配置，因此默认打开。
			EnableProfiling: true, // profile debugging is cheap to have exposed and standard on kube binaries
		},
	}
}

// AddFlags adds flags related to debugging for controller manager to the specified FlagSet.
// 把调试相关 flag 注册到给定的 FlagSet 上；o 为 nil 时直接跳过。
func (o *DebuggingOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	// nil 接收者表示“不注册任何 flag”。
	fs.BoolVar(&o.EnableProfiling, "profiling", o.EnableProfiling,
		"Enable profiling via web interface host:port/debug/pprof/")
	// 锁竞争分析仅在 profiling 开启时才有意义，描述中明确说明这一依赖。
	fs.BoolVar(&o.EnableContentionProfiling, "contention-profiling", o.EnableContentionProfiling,
		"Enable block profiling, if profiling is enabled")
}

// ApplyTo fills up Debugging config with options.
// 把选项值写入正式配置对象；nil 接收者视为“无需修改”。
func (o *DebuggingOptions) ApplyTo(cfg *componentbaseconfig.DebuggingConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.EnableProfiling = o.EnableProfiling
	cfg.EnableContentionProfiling = o.EnableContentionProfiling

	return nil
}

// Validate checks validation of DebuggingOptions.
// 调试选项目前没有跨字段约束，因此只返回空错误列表；保留该方法是为了与其它选项接口一致。
func (o *DebuggingOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
