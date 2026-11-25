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

// Package globalflag 负责把 klog 等库注册到 Go 全局 flag.CommandLine 的标志
// 显式地迁移到组件的 pflag.FlagSet，避免全局标志隐式泄漏。
package globalflag

import (
	"flag"
	"fmt"

	"github.com/spf13/pflag"
	"k8s.io/component-base/logs"
)

// AddGlobalFlags 显式注册 klog、verflag 等库添加到 "flag" 和
// "k8s.io/klog/v2" 全局标志集中的标志，防止不需要的标志泄漏到组件标志集。
// 程序同时使用 LoggingConfiguration 配置日志时，应传入
// k8s.io/component-base/logs.SkipLoggingConfigurationFlags 选项，仅添加未被它覆盖的标志。
// 默认只添加日志与 help 标志；需要其他库标志时用 Register 逐个引入。
func AddGlobalFlags(fs *pflag.FlagSet, name string, opts ...logs.Option) {
	logs.AddFlags(fs, opts...)

	// help 标志由组件自行定义，以便帮助文本带上组件名。
	fs.BoolP("help", "h", false, fmt.Sprintf("help for %s", name))
}

// Register 向 local 添加一个标志，其目标为 flag.CommandLine 中名为 globalName 的标志值。
// 找不到全局标志时 panic：这属于编程错误（忘记导入注册包），应在启动期暴露。
func Register(local *pflag.FlagSet, globalName string) {
	if f := flag.CommandLine.Lookup(globalName); f != nil {
		pflagFlag := pflag.PFlagFromGoFlag(f)
		// 应用本地的名称规范化函数（如把下划线转连字符），保持与其他标志一致。
		normalizeFunc := local.GetNormalizeFunc()
		pflagFlag.Name = string(normalizeFunc(local, pflagFlag.Name))
		local.AddFlag(pflagFlag)
	} else {
		panic(fmt.Sprintf("failed to find flag in global flagset (flag): %s", globalName))
	}
}
