/*
Copyright 2021 The Kubernetes Authors.

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

package v1

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/validation/field"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs/internal/setverbositylevel"
	"k8s.io/component-base/logs/klogflags"
)

const (
	// LogFlushFreqDefault 是相应命令行参数的默认值。
	// 5 秒是在日志时效性与 I/O 开销之间的折中，也是历史默认值。
	LogFlushFreqDefault = 5 * time.Second
)

const (
	// LogFlushFreqFlagName 是命令行参数的名称。
	// 根据 flag 的添加方式，它既可以是独立值（logs.AddFlags），
	// 也可以是 LoggingConfiguration 的一部分。
	LogFlushFreqFlagName = "log-flush-frequency"
)

// NewLoggingConfiguration 返回包含默认日志配置的结构体。
func NewLoggingConfiguration() *LoggingConfiguration {
	c := LoggingConfiguration{}
	SetRecommendedLoggingConfiguration(&c)
	return &c
}

// 除非能够保证没有 goroutine 调用日志函数，否则多次应用配置并不安全。
// ValidateAndApply 与 ValidateAndApplyWithOptions 默认在重复调用时返回错误，
// 但程序和单元测试可以覆盖该行为。
var ReapplyHandling = ReapplyHandlingError

// ReapplyHandlingType 控制重复调用 ValidateAndApply 时的策略。
type ReapplyHandlingType int

const (
	// ReapplyHandlingError 是默认值：再次调用 ValidateAndApply 或
	// ValidateAndApplyWithOptions 会返回错误。
	// 默认拒绝重复应用：日志配置涉及全局 logger，热切换可能导致数据竞争。
	ReapplyHandlingError ReapplyHandlingType = iota
	// ReapplyHandlingIgnoreUnchanged 在配置未变化时静默忽略对 ValidateAndApply
	// 或 ValidateAndApplyWithOptions 的额外调用，否则返回错误。
	ReapplyHandlingIgnoreUnchanged
)

// ValidateAndApply 校验并应用日志配置。
// 应尽早调用，这样程序启动的其余部分（包括其他选项的校验）
// 就已经使用最终的日志配置。
//
// 可选的 FeatureGate 控制日志特性；为 nil 时使用这些特性的默认值。
//
// 日志选项必须在程序启动期间尽早应用。有些修改是全局性的，
// 在已有 goroutine 运行时无法安全完成。
func ValidateAndApply(c *LoggingConfiguration, featureGate featuregate.FeatureGate) error {
	return validateAndApply(c, nil, featureGate, nil)
}

// ValidateAndApplyWithOptions 是 ValidateAndApply 的变体，接受 API 无法配置的
// 额外选项，主要用于测试。
//
// 日志选项必须在程序启动期间尽早应用。有些修改是全局性的，
// 在已有 goroutine 运行时无法安全完成。
func ValidateAndApplyWithOptions(c *LoggingConfiguration, options *LoggingOptions, featureGate featuregate.FeatureGate) error {
	return validateAndApply(c, options, featureGate, nil)
}

// +k8s:deepcopy-gen=false

// LoggingOptions 可与 ValidateAndApplyWithOptions 一起使用，以覆盖某些全局默认值。
// 通过注入 writer 让测试能够捕获输出，而不去重定向进程的 stdout/stderr。
type LoggingOptions struct {
	// ErrorStream 可用于覆盖 os.Stderr 默认值。
	ErrorStream io.Writer

	// InfoStream 可用于覆盖 os.Stdout 默认值。
	InfoStream io.Writer
}

// 当 LoggingConfiguration 嵌入更大的配置结构时，应使用 ValidateAndApplyAsField
// 这一 ValidateAndApply 变体。
// fldPath 用于把错误定位到外层配置结构中的具体字段。
func ValidateAndApplyAsField(c *LoggingConfiguration, featureGate featuregate.FeatureGate, fldPath *field.Path) error {
	return validateAndApply(c, nil, featureGate, fldPath)
}

// validateAndApply 先做完整校验，全部通过后才应用，避免部分配置生效。
func validateAndApply(c *LoggingConfiguration, options *LoggingOptions, featureGate featuregate.FeatureGate, fldPath *field.Path) error {
	errs := Validate(c, featureGate, fldPath)
	if len(errs) > 0 {
		return errs.ToAggregate()
	}
	return apply(c, options, featureGate)
}

// Validate 可检查无效设置而不应用配置。
// 大多数程序应通过 ValidateAndApply 尽早校验并应用日志配置。
// 字段路径是可选的；结构体未嵌入更大的结构时可传入 nil。
func Validate(c *LoggingConfiguration, featureGate featuregate.FeatureGate, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if c.FlushFrequency.Duration.Duration <= 0 {
		errs = append(errs, field.Invalid(fldPath.Child("flushFrequency"), c.FlushFrequency, "Must be greater than zero"))
	}
	if c.Format != DefaultLogFormat {
		// WordSepNormalizeFunc 只是一种推测，命令应当使用它，但无法确定。
		// 非 text 格式会忽略 -v 之外的 klog flag，这里把“用户设置了却不会生效”的 flag 报错。
		allFlags := unsupportedLoggingFlags(cliflag.WordSepNormalizeFunc)
		for _, f := range allFlags {
			if f.DefValue != f.Value.String() {
				errs = append(errs, field.Invalid(fldPath.Child("format"), c.Format, fmt.Sprintf("Non-default format doesn't honor flag: %s", f.Name)))
			}
		}
	}
	format, err := logRegistry.get(c.Format)
	if err != nil {
		errs = append(errs, field.Invalid(fldPath.Child("format"), c.Format, "Unsupported log format"))
	} else if format != nil {
		// 非稳定格式受 feature gate 控制；未提供 gate 时使用该格式定义中的默认值。
		if format.feature != LoggingStableOptions {
			enabled := featureGates()[format.feature][len(featureGates()[format.feature])-1].Default
			if featureGate != nil {
				enabled = featureGate.Enabled(format.feature)
			}
			if !enabled {
				errs = append(errs, field.Forbidden(fldPath.Child("format"), fmt.Sprintf("Log format %s is disabled, see %s feature", c.Format, format.feature)))
			}
		}
	}

	// 结构体中的类型是 uint32，但 klog 只接受正整数 int32。
	if c.Verbosity > math.MaxInt32 {
		errs = append(errs, field.Invalid(fldPath.Child("verbosity"), c.Verbosity, fmt.Sprintf("Must be <= %d", math.MaxInt32)))
	}
	vmoduleFldPath := fldPath.Child("vmodule")
	// vmodule 依赖 text 格式的源代码位置信息；其它格式下无法按文件过滤。
	if len(c.VModule) > 0 && c.Format != "" && c.Format != "text" {
		errs = append(errs, field.Forbidden(vmoduleFldPath, "Only supported for text log format"))
	}
	for i, item := range c.VModule {
		// 空 pattern 会匹配所有文件，等于全局提级，视为配置错误。
		if item.FilePattern == "" {
			errs = append(errs, field.Required(vmoduleFldPath.Index(i), "File pattern must not be empty"))
		}
		if strings.ContainsAny(item.FilePattern, "=,") {
			errs = append(errs, field.Invalid(vmoduleFldPath.Index(i), item.FilePattern, "File pattern must not contain equal sign or comma"))
		}
		if item.Verbosity > math.MaxInt32 {
			errs = append(errs, field.Invalid(vmoduleFldPath.Index(i), item.Verbosity, fmt.Sprintf("Must be <= %d", math.MaxInt32)))
		}
	}

	errs = append(errs, validateFormatOptions(c, featureGate, fldPath.Child("options"))...)
	return errs
}

func validateFormatOptions(c *LoggingConfiguration, featureGate featuregate.FeatureGate, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	// text 与 JSON 的路由选项相同，但命名空间不同，错误路径需分别定位。
	errs = append(errs, validateTextOptions(c, featureGate, fldPath.Child("text"))...)
	errs = append(errs, validateJSONOptions(c, featureGate, fldPath.Child("json"))...)
	return errs
}

// validateTextOptions 校验 text 格式的 alpha 选项是否被 gate 允许。
func validateTextOptions(c *LoggingConfiguration, featureGate featuregate.FeatureGate, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if gate := LoggingAlphaOptions; c.Options.Text.SplitStream && !featureEnabled(featureGate, gate) {
		errs = append(errs, field.Forbidden(fldPath.Child("splitStream"), fmt.Sprintf("Feature %s is disabled", gate)))
	}
	if gate := LoggingAlphaOptions; c.Options.Text.InfoBufferSize.Value() != 0 && !featureEnabled(featureGate, gate) {
		errs = append(errs, field.Forbidden(fldPath.Child("infoBufferSize"), fmt.Sprintf("Feature %s is disabled", gate)))
	}
	return errs
}

// validateJSONOptions 校验 JSON 格式的 alpha 选项是否被 gate 允许。
func validateJSONOptions(c *LoggingConfiguration, featureGate featuregate.FeatureGate, fldPath *field.Path) field.ErrorList {
	errs := field.ErrorList{}
	if gate := LoggingAlphaOptions; c.Options.JSON.SplitStream && !featureEnabled(featureGate, gate) {
		errs = append(errs, field.Forbidden(fldPath.Child("splitStream"), fmt.Sprintf("Feature %s is disabled", gate)))
	}
	if gate := LoggingAlphaOptions; c.Options.JSON.InfoBufferSize.Value() != 0 && !featureEnabled(featureGate, gate) {
		errs = append(errs, field.Forbidden(fldPath.Child("infoBufferSize"), fmt.Sprintf("Feature %s is disabled", gate)))
	}
	return errs
}

// featureEnabled 在 featureGate 为 nil 时返回 false：
// 未提供 gate 的调用方（例如只做静态校验的工具）应使用默认关闭值。
func featureEnabled(featureGate featuregate.FeatureGate, feature featuregate.Feature) bool {
	enabled := false
	if featureGate != nil {
		enabled = featureGate.Enabled(feature)
	}
	return enabled
}

// apply 把配置落地到 klog/全局 logger。该函数修改进程级状态，
// 因此必须在启动早期、没有其它 goroutine 调用日志函数时执行。
func apply(c *LoggingConfiguration, options *LoggingOptions, featureGate featuregate.FeatureGate) error {
	p := &parameters{
		C:                        c,
		Options:                  options,
		ContextualLoggingEnabled: true,
	}
	if featureGate != nil {
		p.ContextualLoggingEnabled = featureGate.Enabled(ContextualLogging)
	}

	// 用原子指针记录已应用参数，以便按 ReapplyHandling 策略拒绝或忽略重复应用。
	oldP := applyParameters.Load()
	if oldP != nil {
		switch ReapplyHandling {
		case ReapplyHandlingError:
			return errors.New("logging configuration was already applied earlier, changing it is not allowed")
		case ReapplyHandlingIgnoreUnchanged:
			if diff := diff.Diff(oldP, p); diff != "" {
				return fmt.Errorf("the logging configuration should not be changed after setting it once (- old setting, + new setting):\n%s", diff)
			}
			return nil
		default:
			return fmt.Errorf("invalid value %d for ReapplyHandling", ReapplyHandling)
		}
	}
	applyParameters.Store(p)

	// 若日志格式不存在，则使用 nil logger。
	// 未注册的格式（理论上已被 Validate 拦截）退回 klog 默认 logger。
	format, _ := logRegistry.get(c.Format)
	if format.factory == nil {
		klog.ClearLogger()
	} else {
		// 未注入 options 时使用进程真实的 stdout/stderr。
		if options == nil {
			options = &LoggingOptions{
				ErrorStream: os.Stderr,
				InfoStream:  os.Stdout,
			}
		}
		log, control := format.factory.Create(*c, *options)
		if control.SetVerbosityLevel != nil {
			// 注册回调，让 GlogSetter 等动态调级入口能同步到新 logger。
			setverbositylevel.Mutex.Lock()
			defer setverbositylevel.Mutex.Unlock()
			setverbositylevel.Callbacks = append(setverbositylevel.Callbacks, control.SetVerbosityLevel)
		}
		opts := []klog.LoggerOption{
			klog.ContextualLogger(p.ContextualLoggingEnabled),
			klog.FlushLogger(control.Flush),
		}
		// textlogger 支持 klog 的缓冲写入路径，保留后可减少格式化开销。
		if writer, ok := log.GetSink().(textlogger.KlogBufferWriter); ok {
			opts = append(opts, klog.WriteKlogBuffer(writer.WriteKlogBuffer))
		}
		klog.SetLoggerWithOptions(log, opts...)
	}
	// 同步 verbosity 与 vmodule 到 klog flag 状态，使运行期反射读取一致。
	if err := loggingFlags.Lookup("v").Value.Set(VerbosityLevelPflag(&c.Verbosity).String()); err != nil {
		return fmt.Errorf("internal error while setting klog verbosity: %v", err)
	}
	if err := loggingFlags.Lookup("vmodule").Value.Set(VModuleConfigurationPflag(&c.VModule).String()); err != nil {
		return fmt.Errorf("internal error while setting klog vmodule: %v", err)
	}
	setSlogDefaultLogger()
	// 用配置的间隔重启 flush daemon；InitLogs 可能已用默认间隔启动过一次。
	klog.StartFlushDaemon(c.FlushFrequency.Duration.Duration)
	klog.EnableContextualLogging(p.ContextualLoggingEnabled)
	return nil
}

// parameters 是已应用配置的快照，用于判断是否重复应用。
type parameters struct {
	C                        *LoggingConfiguration
	Options                  *LoggingOptions
	ContextualLoggingEnabled bool
}

var applyParameters atomic.Pointer[parameters]

// ResetForTest 恢复默认设置。该函数不是线程安全的，只能在没有 goroutine 运行时使用，
// 主要供其他包的单元测试调用。
func ResetForTest(featureGate featuregate.FeatureGate) error {
	oldP := applyParameters.Load()
	if oldP == nil {
		// 无需处理。
		return nil
	}

	// 清空快照后可再次调用 apply，不会触发错误。
	// 先清空快照，再应用默认配置，否则会触发“重复应用”错误。
	applyParameters.Store(nil)

	// 恢复默认配置；正常不应失败，但仍需检查。
	config := NewLoggingConfiguration()
	if err := ValidateAndApply(config, featureGate); err != nil {
		return fmt.Errorf("apply default configuration: %v", err)
	}

	// 再次清空快照，让后续测试可以重新应用配置。
	applyParameters.Store(nil)

	return nil
}

// AddFlags 为配置添加命令行 flag。
func AddFlags(c *LoggingConfiguration, fs *pflag.FlagSet) {
	addFlags(c, fs)
}

// AddGoFlags 是 AddFlags 针对标准库 FlagSet 的变体。
func AddGoFlags(c *LoggingConfiguration, fs *flag.FlagSet) {
	addFlags(c, goFlagSet{FlagSet: fs})
}

// flagSet 是 pflag.FlagSet 实现的接口，
// 仅定义 addFlags 所需的方法。
type flagSet interface {
	BoolVar(p *bool, name string, value bool, usage string)
	DurationVar(p *time.Duration, name string, value time.Duration, usage string)
	StringVar(p *string, name string, value string, usage string)
	Var(value pflag.Value, name string, usage string)
	VarP(value pflag.Value, name, shorthand, usage string)
}

// goFlagSet 为标准库 flag.FlagSet 实现 flagSet。
// goFlagSet 把标准库 FlagSet 适配为 flagSet；
// 标准库不支持 shorthand，VarP 退化为 Var。
type goFlagSet struct {
	*flag.FlagSet
}

func (fs goFlagSet) Var(value pflag.Value, name string, usage string) {
	fs.FlagSet.Var(value, name, usage)
}

func (fs goFlagSet) VarP(value pflag.Value, name, shorthand, usage string) {
	// 忽略 shorthand，因为此处既不需要也不支持。
	fs.FlagSet.Var(value, name, usage)
}

// addFlags 可用于 flag.FlagSet 和 pflag.FlagSet；
// 内部接口定义避免了重复代码。
func addFlags(c *LoggingConfiguration, fs flagSet) {
	formats := logRegistry.list()
	fs.StringVar(&c.Format, "logging-format", c.Format, fmt.Sprintf("Sets the log format. Permitted formats: %s.", formats))
	// 生成 flag 选项后不应再添加新的日志格式。
	// 冻结注册表，确保 --help 展示的格式集合与后续实际可选集合一致。
	logRegistry.freeze()

	fs.DurationVar(&c.FlushFrequency.Duration.Duration, LogFlushFreqFlagName, c.FlushFrequency.Duration.Duration, "Maximum number of seconds between log flushes")
	fs.VarP(VerbosityLevelPflag(&c.Verbosity), "v", "v", "number for the log level verbosity")
	fs.Var(VModuleConfigurationPflag(&c.VModule), "vmodule", "comma-separated list of pattern=N settings for file-filtered logging (only works for text log format)")

	fs.BoolVar(&c.Options.Text.SplitStream, "log-text-split-stream", false, "[Alpha] In text format, write error messages to stderr and info messages to stdout. The default is to write a single stream to stdout. Enable the LoggingAlphaOptions feature gate to use this.")
	fs.Var(&c.Options.Text.InfoBufferSize, "log-text-info-buffer-size", "[Alpha] In text format with split output streams, the info messages can be buffered for a while to increase performance. The default value of zero bytes disables buffering. The size can be specified as number of bytes (512), multiples of 1000 (1K), multiples of 1024 (2Ki), or powers of those (3M, 4G, 5Mi, 6Gi). Enable the LoggingAlphaOptions feature gate to use this.")

	// JSON 选项仅在 "json" 是有效格式时才注册，
	// 但配置文件 API 始终包含这些选项。
	// JSON 格式由可选包注册，未导入时不要暴露无效 flag。
	if _, err := logRegistry.get("json"); err == nil {
		fs.BoolVar(&c.Options.JSON.SplitStream, "log-json-split-stream", false, "[Alpha] In JSON format, write error messages to stderr and info messages to stdout. The default is to write a single stream to stdout. Enable the LoggingAlphaOptions feature gate to use this.")
		fs.Var(&c.Options.JSON.InfoBufferSize, "log-json-info-buffer-size", "[Alpha] In JSON format with split output streams, the info messages can be buffered for a while to increase performance. The default value of zero bytes disables buffering. The size can be specified as number of bytes (512), multiples of 1000 (1K), multiples of 1024 (2Ki), or powers of those (3M, 4G, 5Mi, 6Gi). Enable the LoggingAlphaOptions feature gate to use this.")
	}
}

// SetRecommendedLoggingConfiguration 为未设置的字段填充默认日志配置。
//
// 将 LoggingConfiguration 嵌入自身配置结构的使用者可先设置自定义默认值，
// 再调用本函数补充全局默认值。
func SetRecommendedLoggingConfiguration(c *LoggingConfiguration) {
	// 只在未设置时填充，允许调用方先设置自定义默认值。
	if c.Format == "" {
		c.Format = "text"
	}
	if c.FlushFrequency.Duration.Duration == 0 {
		c.FlushFrequency.Duration.Duration = LogFlushFreqDefault
		c.FlushFrequency.SerializeAsString = true
	}
	setRecommendedOutputRouting(&c.Options.Text.OutputRoutingOptions)
	setRecommendedOutputRouting(&c.Options.JSON.OutputRoutingOptions)
}

func setRecommendedOutputRouting(o *OutputRoutingOptions) {
	var empty resource.QuantityValue
	if o.InfoBufferSize == empty {
		// 显式构造 0 而非保留零值，确保 JSON 序列化为 "0" 且反射比较一致。
		o.InfoBufferSize = resource.QuantityValue{
			// 这与默认构造的实例相似，但不完全相同。
			Quantity: *resource.NewQuantity(0, resource.DecimalSI),
		}
		// 这会设置未导出的 Quantity.s，某些测试会通过 reflect.DeepEqual 比较它。
		_ = o.InfoBufferSize.String()
	}
}

// loggingFlags 保存日志 flag 的状态，尤其是解析前的默认值，
// 供 unsupportedLoggingFlags 使用。
var loggingFlags pflag.FlagSet

func init() {
	// 捕获 klog flag 的默认值，供 Validate 判断用户是否修改了非 text 格式不支持的选项。
	var fs flag.FlagSet
	klogflags.Init(&fs)
	loggingFlags.AddGoFlagSet(&fs)
}

// 所有日志格式都支持的日志 flag 列表（k8s.io/klog + k8s.io/component-base/logs）。
var supportedLogsFlags = map[string]struct{}{
	"v": {},
}

// unsupportedLoggingFlags 列出不支持的日志 flag，normalize 函数是可选的。
func unsupportedLoggingFlags(normalizeFunc func(f *pflag.FlagSet, name string) pflag.NormalizedName) []*pflag.Flag {
	// k8s.io/component-base/logs 和 klog 的 flag。
	pfs := &pflag.FlagSet{}
	loggingFlags.VisitAll(func(flag *pflag.Flag) {
		if _, found := supportedLogsFlags[flag.Name]; !found {
			// 规范化会修改 flag.Name，因此先复制。
			clone := *flag
			pfs.AddFlag(&clone)
		}
	})

	// 应用规范化。
	pfs.SetNormalizeFunc(normalizeFunc)

	var allFlags []*pflag.Flag
	pfs.VisitAll(func(flag *pflag.Flag) {
		allFlags = append(allFlags, flag)
	})
	return allFlags
}
