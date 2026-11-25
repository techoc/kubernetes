/*
Copyright 2014 The Kubernetes Authors.

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

// Package logs 提供日志选项、flag 和初始化支持。
// 命令必须显式启用命令行 flag，导入本包不再自动添加这些 flag。
package logs

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/spf13/pflag"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/logs/internal/setverbositylevel"
	"k8s.io/component-base/logs/klogflags"
	"k8s.io/klog/v2"
)

const vmoduleUsage = " (only works for the default text log format)"

var (
	// packageFlags 保存 klog flag 的定义，作为 AddFlags/AddGoFlags 注册的来源。
	packageFlags = flag.NewFlagSet("logging", flag.ContinueOnError)

	// 定期刷新通过本文件中的全局 flag 或 LoggingConfiguration 配置。
	logFlushFreq time.Duration
)

func init() {
	// klog 只在初始化时暴露 flag；后续按需复制到不同 FlagSet。
	klogflags.Init(packageFlags)
	packageFlags.DurationVar(&logFlushFreq, logsapi.LogFlushFreqFlagName, logsapi.LogFlushFreqDefault, "Maximum number of seconds between log flushes")
}

// addFlagsOptions 收集 AddFlags 的可选项。
type addFlagsOptions struct {
	skipLoggingConfigurationFlags bool
}

// Option 是 AddFlags/AddGoFlags 的配置函数。
type Option func(*addFlagsOptions)

// 当程序还使用 LoggingConfiguration 结构体配置日志时，
// 必须将 SkipLoggingConfigurationFlags 作为 AddFlags 的选项。
// 这样只会添加该结构体未覆盖的 flag。
func SkipLoggingConfigurationFlags() Option {
	return func(o *addFlagsOptions) {
		o.skipLoggingConfigurationFlags = true
	}
}

// Options 是 LoggingConfiguration 的别名，以符合 component-base 惯例。
// 别名（而非新类型）使组件可以按习惯使用 logs.Options 而无需转换。
type Options = logsapi.LoggingConfiguration

// NewOptions 是 NewLoggingConfiguration 的别名。
var NewOptions = logsapi.NewLoggingConfiguration

// AddFlags 在任意 FlagSet 上注册本包的 flag，包括 klog flag，单词间保留原有下划线。
// 若命令希望使用连字符分隔，可在调用 AddFlags 前为 flag 集合设置
// k8s.io/component-base/cli/flag/WordSepNormalizeFunc 作为规范化函数。
//
// 可以多次调用。
func AddFlags(fs *pflag.FlagSet, opts ...Option) {
	o := addFlagsOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	// 添加所有受支持的 flag。
	packageFlags.VisitAll(func(f *flag.Flag) {
		pf := pflag.PFlagFromGoFlag(f)
		switch f.Name {
		case "v", logsapi.LogFlushFreqFlagName:
			// 保持不变，可视情况跳过。
			if o.skipLoggingConfigurationFlags {
				return
			}
		case "vmodule":
			if o.skipLoggingConfigurationFlags {
				return
			}
			// vmodule 仅 text 格式有效，提前在 usage 中说明以免误用。
			pf.Usage += vmoduleUsage
		}
		// 不覆盖命令已显式注册的同名 flag。
		if fs.Lookup(pf.Name) == nil {
			fs.AddFlag(pf)
		}
	})
}

// AddGoFlags 是 AddFlags 针对传统 Go flag.FlagSet 的变体。
// 为保持一致，命令应尽可能使用 pflag。
// 需要本函数的场景包括测试（必须在 flag.CommandLine 中设置 flag），
// 以及因历史原因使用 Go flag.Parse、无法切换到 pflag 的命令，
// 因为切换会破坏其命令行接口。
func AddGoFlags(fs *flag.FlagSet, opts ...Option) {
	o := addFlagsOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	// 添加 flag，并在部分 klog flag 的用法文本中加入弃用说明。
	packageFlags.VisitAll(func(f *flag.Flag) {
		usage := f.Usage
		switch f.Name {
		case "v", logsapi.LogFlushFreqFlagName:
			// 保持不变。
			if o.skipLoggingConfigurationFlags {
				return
			}
		case "vmodule":
			if o.skipLoggingConfigurationFlags {
				return
			}
			usage += vmoduleUsage
		}
		// 标准库 FlagSet 不支持 pflag 的类型元数据，只能复用底层 Value。
		fs.Var(f.Value, f.Name, usage)
	})
}

// KlogWriter 充当标准 log 包与 glog 包之间的桥接。
type KlogWriter struct{}

// Write 实现 io.Writer 接口。
func (writer KlogWriter) Write(data []byte) (n int, err error) {
	// 深度 1 跳过本包装层，使 caller 指向实际的 log.Printf 调用点。
	klog.InfoDepth(1, string(data))
	return len(data), nil
}

// InitLogs 按 Kubernetes 所需方式初始化日志。
// 应在解析 flag 后调用；若提前调用，则使用默认日志设置。
//
// 在该 Kubernetes 特性被认为稳定之前，InitLogs 会禁用 klog 的上下文日志支持。
// 希望支持上下文日志的命令可以：
//   - 在调用 InitLogs 后调用 klog.EnableContextualLogging，
//     传入固定的 `true`，或根据命令行 flag 或特性门控检查结果决定
//   - 设置 FeatureGate 实例，通过 Options 使用高级日志配置，
//     并带上 FeatureGate 调用 Options.ValidateAndApply；
//     k8s.io/component-base/logs/example/cmd 演示了具体做法
func InitLogs() {
	// 让标准库 log 的输出进入 klog，统一格式与级别。
	log.SetOutput(KlogWriter{})
	log.SetFlags(0)

	// 现在启动定期刷新。若使用 LoggingConfiguration.ApplyAndValidate，
	// 它会按其中定义的日志刷新间隔重启守护进程。
	klog.StartFlushDaemon(logFlushFreq)

	// 这是 Kubernetes 的默认值，Options.ValidateAndApply 会用特性门控检查结果覆盖它。
	klog.EnableContextualLogging(false)
}

// FlushLogs 立即刷新日志。应在 main 函数末尾通过 defer 调用，
// 确保所有待处理日志在程序退出前输出。
func FlushLogs() {
	klog.Flush()
}

// NewLogger 创建将日志发送到 klog.Info 的新 log.Logger。
func NewLogger(prefix string) *log.Logger {
	return log.New(KlogWriter{}, prefix, 0)
}

// GlogSetter 修改整个程序的详细程度阈值。
// 某些组件提供基于 HTTP 的接口，在运行时调用此功能。
func GlogSetter(val string) (string, error) {
	// 先解析为 uint32 以便回调使用，再交给 klog.Level 做实际校验。
	v, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return "", err
	}

	var level klog.Level
	if err := level.Set(val); err != nil {
		return "", fmt.Errorf("failed set klog.logging.verbosity %s: %v", val, err)
	}

	// 同步通知已注册的 logger 实现（如 zapr），使动态调级对所有输出生效。
	setverbositylevel.Mutex.Lock()
	defer setverbositylevel.Mutex.Unlock()
	for _, cb := range setverbositylevel.Callbacks {
		if err := cb(uint32(v)); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("successfully set klog.logging.verbosity to %s", val), nil
}
