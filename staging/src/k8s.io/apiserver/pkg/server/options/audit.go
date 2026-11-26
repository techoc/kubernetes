/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"gopkg.in/natefinch/lumberjack.v2"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/audit/policy"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/util/webhook"
	pluginbuffered "k8s.io/apiserver/plugin/pkg/audit/buffered"
	pluginlog "k8s.io/apiserver/plugin/pkg/audit/log"
	plugintruncate "k8s.io/apiserver/plugin/pkg/audit/truncate"
	pluginwebhook "k8s.io/apiserver/plugin/pkg/audit/webhook"
)

const (
	// ModeBatch 的默认配置值。
	defaultBatchBufferSize = 10000 // Buffer up to 10000 events before starting discarding.
	// 这些批量参数只对 webhook 后端生效。
	defaultBatchMaxSize       = 400              // Only send up to 400 events at a time.
	defaultBatchMaxWait       = 30 * time.Second // Send events at least twice a minute.
	defaultBatchThrottleQPS   = 10               // Limit the send rate by 10 QPS.
	defaultBatchThrottleBurst = 15               // Allow up to 15 QPS burst.
)

// appendBackend 把新后端并入已有后端；任一为 nil 时直接返回另一个，
// 避免构造出只含空成员的 Union 后端。
func appendBackend(existing, newBackend audit.Backend) audit.Backend {
	if existing == nil {
		return newBackend
	}
	if newBackend == nil {
		return existing
	}
	return audit.Union(existing, newBackend)
}

// AuditOptions 汇总审计策略文件与各输出后端（log、webhook）的配置。
type AuditOptions struct {
	// 用于筛选所记录审计事件的策略文件；未指定时提供默认值。
	//
	// 未配置策略文件时不会产生任何审计事件（evaluator 为 nil），后端即便启用也是空转。
	PolicyFile string

	// Plugin options
	LogOptions     AuditLogOptions
	WebhookOptions AuditWebhookOptions
}

const (
	// 后端内部缓冲事件，积累到一定条数或等待一定时间后批量上报。
	ModeBatch = "batch"
	// 每批事件都同步处理完成才返回，API 请求需等待审计落盘后才能响应。
	ModeBlocking = "blocking"
	// 与 ModeBlocking 类似，但在 RequestReceived 阶段审计失败时，
	// 整个 API 请求也会失败。
	//
	// 严格模式是唯一会让审计失败影响请求结果的模式，用于强合规场景。
	ModeBlockingStrict = "blocking-strict"
)

// AllowedModes 是审计后端支持的模式列表。
var AllowedModes = []string{
	ModeBatch,
	ModeBlocking,
	ModeBlockingStrict,
}

// AuditBatchOptions 控制后端是同步阻塞还是异步批量写事件。
type AuditBatchOptions struct {
	// 后端异步批量上报，还是同步阻塞响应？
	//
	// 默认为异步批量上报。
	Mode string
	// 批量配置，仅在 batch 模式下生效。
	BatchConfig pluginbuffered.BatchConfig
}

// AuditTruncateOptions 控制单事件/单批次过大时的截断行为，防止超大对象撑爆后端。
type AuditTruncateOptions struct {
	// 是否启用事件截断。
	Enabled bool

	// Truncating configuration.
	TruncateConfig plugintruncate.Config
}

// AuditLogOptions 决定结构化审计日志的默认输出方式。
type AuditLogOptions struct {
	Path       string
	MaxAge     int
	MaxBackups int
	MaxSize    int
	Format     string
	Compress   bool

	BatchOptions    AuditBatchOptions
	TruncateOptions AuditTruncateOptions

	// 序列化审计事件使用的 API 组版本。
	GroupVersionString string
}

// AuditWebhookOptions 控制审计事件的 Webhook 配置。
type AuditWebhookOptions struct {
	ConfigFile     string
	InitialBackoff time.Duration

	BatchOptions    AuditBatchOptions
	TruncateOptions AuditTruncateOptions

	// 序列化审计事件使用的 API 组版本。
	GroupVersionString string
}

// NewAuditOptions 返回默认配置：webhook 默认批量异步，日志默认阻塞（写本地文件成本低且要求不丢事件）。
func NewAuditOptions() *AuditOptions {
	return &AuditOptions{
		WebhookOptions: AuditWebhookOptions{
			InitialBackoff: pluginwebhook.DefaultInitialBackoffDelay,
			BatchOptions: AuditBatchOptions{
				Mode:        ModeBatch,
				BatchConfig: defaultWebhookBatchConfig(),
			},
			TruncateOptions:    NewAuditTruncateOptions(),
			GroupVersionString: "audit.k8s.io/v1",
		},
		LogOptions: AuditLogOptions{
			MaxSize:    100,
			MaxAge:     366,
			MaxBackups: 100,

			Format: pluginlog.FormatJson,
			BatchOptions: AuditBatchOptions{
				Mode:        ModeBlocking,
				BatchConfig: defaultLogBatchConfig(),
			},
			TruncateOptions:    NewAuditTruncateOptions(),
			GroupVersionString: "audit.k8s.io/v1",
		},
	}
}

// NewAuditTruncateOptions 返回默认的截断配置：默认关闭，开启时单批 10MB、单事件 100KB。
func NewAuditTruncateOptions() AuditTruncateOptions {
	return AuditTruncateOptions{
		Enabled: false,
		TruncateConfig: plugintruncate.Config{
			MaxBatchSize: 10 * 1024 * 1024, // 10MB
			MaxEventSize: 100 * 1024,       // 100KB
		},
	}
}

// Validate checks invalid config combination
func (o *AuditOptions) Validate() []error {
	if o == nil {
		return nil
	}

	var allErrors []error
	allErrors = append(allErrors, o.LogOptions.Validate()...)
	allErrors = append(allErrors, o.WebhookOptions.Validate()...)

	return allErrors
}

// validateBackendMode 校验模式字符串；错误信息里列出全部合法值，便于用户直接修正。
func validateBackendMode(pluginName string, mode string) error {
	for _, m := range AllowedModes {
		if m == mode {
			return nil
		}
	}
	return fmt.Errorf("invalid audit %s mode %s, allowed modes are %q", pluginName, mode, strings.Join(AllowedModes, ","))
}

// validateBackendBatchOptions 校验批量参数；非 batch 模式下跳过，避免对未使用字段误报。
func validateBackendBatchOptions(pluginName string, options AuditBatchOptions) error {
	if err := validateBackendMode(pluginName, options.Mode); err != nil {
		return err
	}
	if options.Mode != ModeBatch {
		// 未使用的选项无需校验。
		return nil
	}
	config := options.BatchConfig
	if config.BufferSize <= 0 {
		return fmt.Errorf("invalid audit batch %s buffer size %v, must be a positive number", pluginName, config.BufferSize)
	}
	if config.MaxBatchSize <= 0 {
		return fmt.Errorf("invalid audit batch %s max batch size %v, must be a positive number", pluginName, config.MaxBatchSize)
	}
	if config.ThrottleEnable {
		if config.ThrottleQPS <= 0 {
			return fmt.Errorf("invalid audit batch %s throttle QPS %v, must be a positive number", pluginName, config.ThrottleQPS)
		}
		if config.ThrottleBurst <= 0 {
			return fmt.Errorf("invalid audit batch %s throttle burst %v, must be a positive number", pluginName, config.ThrottleBurst)
		}
	}
	return nil
}

// knownGroupVersions 是审计事件序列化允许的组版本白名单；目前只剩 v1。
var knownGroupVersions = []schema.GroupVersion{
	auditv1.SchemeGroupVersion,
}

// validateGroupVersionString 校验并告警：非首选版本只打 warning 而不拒绝，保持向后兼容。
func validateGroupVersionString(groupVersion string) error {
	gv, err := schema.ParseGroupVersion(groupVersion)
	if err != nil {
		return err
	}
	if !knownGroupVersion(gv) {
		return fmt.Errorf("invalid group version, allowed versions are %q", knownGroupVersions)
	}
	if gv != auditv1.SchemeGroupVersion {
		klog.Warningf("%q is deprecated and will be removed in a future release, use %q instead", gv, auditv1.SchemeGroupVersion)
	}
	return nil
}

// knownGroupVersion 判断组版本是否在白名单内。
func knownGroupVersion(gv schema.GroupVersion) bool {
	for _, knownGv := range knownGroupVersions {
		if gv == knownGv {
			return true
		}
	}
	return false
}

// AddFlags 注册审计相关的全部命令行参数（策略文件、日志后端、webhook 后端）。
func (o *AuditOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.StringVar(&o.PolicyFile, "audit-policy-file", o.PolicyFile,
		"Path to the file that defines the audit policy configuration.")

	o.LogOptions.AddFlags(fs)
	o.LogOptions.BatchOptions.AddFlags(pluginlog.PluginName, fs)
	o.LogOptions.TruncateOptions.AddFlags(pluginlog.PluginName, fs)
	o.WebhookOptions.AddFlags(fs)
	o.WebhookOptions.BatchOptions.AddFlags(pluginwebhook.PluginName, fs)
	o.WebhookOptions.TruncateOptions.AddFlags(pluginwebhook.PluginName, fs)
}

// ApplyTo 把选项转换为 server.Config 上的策略评估器与审计后端。
//
// 构建顺序体现依赖关系：先有策略才能决定记录哪些事件，因此没有 evaluator 时
// 即使配置了后端也不会创建（否则每个事件都会被策略过滤掉，纯属浪费资源）。
func (o *AuditOptions) ApplyTo(
	c *server.Config,
) error {
	if o == nil {
		return nil
	}
	if c == nil {
		return fmt.Errorf("server config must be non-nil")
	}

	// 1. Build policy evaluator
	evaluator, err := o.newPolicyRuleEvaluator()
	if err != nil {
		return err
	}

	// 2. Build log backend
	var logBackend audit.Backend
	w, err := o.LogOptions.getWriter()
	if err != nil {
		return err
	}
	if w != nil {
		if evaluator == nil {
			klog.V(2).Info("No audit policy file provided, no events will be recorded for log backend")
		} else {
			logBackend = o.LogOptions.newBackend(w)
		}
	}

	// 3. Build webhook backend
	var webhookBackend audit.Backend
	if o.WebhookOptions.enabled() {
		if evaluator == nil {
			klog.V(2).Info("No audit policy file provided, no events will be recorded for webhook backend")
		} else {
			// 配置了 EgressSelector 时必须走它建立连接，以满足控制面出网的安全策略。
			if c.EgressSelector != nil {
				var egressDialer utilnet.DialFunc
				egressDialer, err = c.EgressSelector.Lookup(egressselector.ControlPlane.AsNetworkContext())
				if err != nil {
					return err
				}
				webhookBackend, err = o.WebhookOptions.newUntruncatedBackend(egressDialer)
			} else {
				webhookBackend, err = o.WebhookOptions.newUntruncatedBackend(nil)
			}
			if err != nil {
				return err
			}
		}
	}

	groupVersion, err := schema.ParseGroupVersion(o.WebhookOptions.GroupVersionString)
	if err != nil {
		return err
	}

	// 4. Apply dynamic options.
	var dynamicBackend audit.Backend
	if webhookBackend != nil {
		// 仅启用 webhook 时，也要套上截断包装。
		//
		// 截断包装放在最外层：批次合并后再判断大小，才能把超大批次拆分。
		dynamicBackend = o.WebhookOptions.TruncateOptions.wrapBackend(webhookBackend, groupVersion)
	}

	// 5. 设置策略规则求值器
	c.AuditPolicyRuleEvaluator = evaluator

	// 6. 把日志后端与 Webhook 合并
	c.AuditBackend = appendBackend(logBackend, dynamicBackend)

	if c.AuditBackend != nil {
		klog.V(2).Infof("Using audit backend: %s", c.AuditBackend)
	}
	return nil
}

// newPolicyRuleEvaluator 加载策略文件；未指定文件时返回 (nil, nil) 表示不审计。
func (o *AuditOptions) newPolicyRuleEvaluator() (audit.PolicyRuleEvaluator, error) {
	if o.PolicyFile == "" {
		return nil, nil
	}

	p, err := policy.LoadPolicyFromFile(o.PolicyFile)
	if err != nil {
		return nil, fmt.Errorf("loading audit policy file: %v", err)
	}
	return policy.NewPolicyRuleEvaluator(p), nil
}

// AddFlags 注册某个后端（log/webhook）的批量模式相关参数。
func (o *AuditBatchOptions) AddFlags(pluginName string, fs *pflag.FlagSet) {
	fs.StringVar(&o.Mode, fmt.Sprintf("audit-%s-mode", pluginName), o.Mode,
		"Strategy for sending audit events. Blocking indicates sending events should block"+
			" server responses. Batch causes the backend to buffer and write events"+
			" asynchronously. Known modes are "+strings.Join(AllowedModes, ",")+".")
	fs.IntVar(&o.BatchConfig.BufferSize, fmt.Sprintf("audit-%s-batch-buffer-size", pluginName),
		o.BatchConfig.BufferSize, "The size of the buffer to store events before "+
			"batching and writing. Only used in batch mode.")
	fs.IntVar(&o.BatchConfig.MaxBatchSize, fmt.Sprintf("audit-%s-batch-max-size", pluginName),
		o.BatchConfig.MaxBatchSize, "The maximum size of a batch. Only used in batch mode.")
	fs.DurationVar(&o.BatchConfig.MaxBatchWait, fmt.Sprintf("audit-%s-batch-max-wait", pluginName),
		o.BatchConfig.MaxBatchWait, "The amount of time to wait before force writing the "+
			"batch that hadn't reached the max size. Only used in batch mode.")
	fs.BoolVar(&o.BatchConfig.ThrottleEnable, fmt.Sprintf("audit-%s-batch-throttle-enable", pluginName),
		o.BatchConfig.ThrottleEnable, "Whether batching throttling is enabled. Only used in batch mode.")
	fs.Float32Var(&o.BatchConfig.ThrottleQPS, fmt.Sprintf("audit-%s-batch-throttle-qps", pluginName),
		o.BatchConfig.ThrottleQPS, "Maximum average number of batches per second. "+
			"Only used in batch mode.")
	fs.IntVar(&o.BatchConfig.ThrottleBurst, fmt.Sprintf("audit-%s-batch-throttle-burst", pluginName),
		o.BatchConfig.ThrottleBurst, "Maximum number of requests sent at the same "+
			"moment if ThrottleQPS was not utilized before. Only used in batch mode.")
}

// ignoreErrorsBackend 把后端的失败吞掉并始终返回成功，对应 ModeBlocking：
// 请求会等待写入完成，但审计失败不会导致请求失败。
type ignoreErrorsBackend struct {
	audit.Backend
}

// ProcessEvents 忽略底层返回的成败，始终报告成功。
func (i *ignoreErrorsBackend) ProcessEvents(ev ...*auditinternal.Event) bool {
	i.Backend.ProcessEvents(ev...)
	return true
}

// String 在日志中标识这层包装，便于排查为什么错误没有向上传播。
func (i *ignoreErrorsBackend) String() string {
	return fmt.Sprintf("ignoreErrors<%s>", i.Backend)
}

// wrapBackend 按模式选择包装层：严格模式不包装（错误直接上抛），
// 普通阻塞模式忽略错误，批量模式改为异步缓冲。
func (o *AuditBatchOptions) wrapBackend(delegate audit.Backend) audit.Backend {
	if o.Mode == ModeBlockingStrict {
		return delegate
	}
	if o.Mode == ModeBlocking {
		return &ignoreErrorsBackend{Backend: delegate}
	}
	return pluginbuffered.NewBackend(delegate, o.BatchConfig)
}

// Validate 校验截断参数：单批必须能容纳至少一个事件，否则任何事件都会被丢弃。
func (o *AuditTruncateOptions) Validate(pluginName string) error {
	config := o.TruncateConfig
	if config.MaxEventSize <= 0 {
		return fmt.Errorf("invalid audit truncate %s max event size %v, must be a positive number", pluginName, config.MaxEventSize)
	}
	if config.MaxBatchSize < config.MaxEventSize {
		return fmt.Errorf("invalid audit truncate %s max batch size %v, must be greater than "+
			"max event size (%v)", pluginName, config.MaxBatchSize, config.MaxEventSize)
	}
	return nil
}

// AddFlags 注册截断相关参数。
func (o *AuditTruncateOptions) AddFlags(pluginName string, fs *pflag.FlagSet) {
	fs.BoolVar(&o.Enabled, fmt.Sprintf("audit-%s-truncate-enabled", pluginName),
		o.Enabled, "Whether event and batch truncating is enabled.")
	fs.Int64Var(&o.TruncateConfig.MaxBatchSize, fmt.Sprintf("audit-%s-truncate-max-batch-size", pluginName),
		o.TruncateConfig.MaxBatchSize, "Maximum size of the batch sent to the underlying backend. "+
			"Actual serialized size can be several hundreds of bytes greater. If a batch exceeds this limit, "+
			"it is split into several batches of smaller size.")
	fs.Int64Var(&o.TruncateConfig.MaxEventSize, fmt.Sprintf("audit-%s-truncate-max-event-size", pluginName),
		o.TruncateConfig.MaxEventSize, "Maximum size of the audit event sent to the underlying backend. "+
			"If the size of an event is greater than this number, first request and response are removed, and "+
			"if this doesn't reduce the size enough, event is discarded.")
}

// wrapBackend 在启用截断时套一层截断后端；关闭时原样返回。
func (o *AuditTruncateOptions) wrapBackend(delegate audit.Backend, gv schema.GroupVersion) audit.Backend {
	if !o.Enabled {
		return delegate
	}
	return plugintruncate.NewBackend(delegate, o.TruncateConfig, gv)
}

// AddFlags 注册日志后端参数（输出路径、轮转、格式、版本、压缩）。
func (o *AuditLogOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.Path, "audit-log-path", o.Path,
		"If set, all requests coming to the apiserver will be logged to this file.  '-' means standard out.")
	fs.IntVar(&o.MaxAge, "audit-log-maxage", o.MaxAge,
		"The maximum number of days to retain old audit log files based on the timestamp encoded in their filename. Setting a value of 0 means old audit log files are not removed based on age.")
	fs.IntVar(&o.MaxBackups, "audit-log-maxbackup", o.MaxBackups,
		"The maximum number of old audit log files to retain. Setting a value of 0 will mean there's no restriction on the number of files.")
	fs.IntVar(&o.MaxSize, "audit-log-maxsize", o.MaxSize,
		"The maximum size in megabytes of the audit log file before it gets rotated. Setting to 0 disables rotation (not recommended).")
	fs.StringVar(&o.Format, "audit-log-format", o.Format,
		"Format of saved audits. \"legacy\" indicates 1-line text format for each event."+
			" \"json\" indicates structured json format. Known formats are "+
			strings.Join(pluginlog.AllowedFormats, ",")+".")
	fs.StringVar(&o.GroupVersionString, "audit-log-version", o.GroupVersionString,
		"API group and version used for serializing audit events written to log.")
	fs.BoolVar(&o.Compress, "audit-log-compress", o.Compress, "If set, the rotated log files will be compressed using gzip.")
}

// Validate 只在日志后端启用时校验，避免未启用的后端报出无关错误。
func (o *AuditLogOptions) Validate() []error {
	// 根据配置判断日志后端是否启用。
	if !o.enabled() {
		return nil
	}

	var allErrors []error

	if err := validateBackendBatchOptions(pluginlog.PluginName, o.BatchOptions); err != nil {
		allErrors = append(allErrors, err)
	}
	if err := o.TruncateOptions.Validate(pluginlog.PluginName); err != nil {
		allErrors = append(allErrors, err)
	}

	if err := validateGroupVersionString(o.GroupVersionString); err != nil {
		allErrors = append(allErrors, err)
	}

	// Check log format
	if !sets.NewString(pluginlog.AllowedFormats...).Has(o.Format) {
		allErrors = append(allErrors, fmt.Errorf("invalid audit log format %s, allowed formats are %q", o.Format, strings.Join(pluginlog.AllowedFormats, ",")))
	}

	// 文件日志后端启用时，校验 MaxAge、MaxBackups 与 MaxSize。
	if o.MaxAge < 0 {
		allErrors = append(allErrors, fmt.Errorf("--audit-log-maxage %v can't be a negative number", o.MaxAge))
	}
	if o.MaxBackups < 0 {
		allErrors = append(allErrors, fmt.Errorf("--audit-log-maxbackup %v can't be a negative number", o.MaxBackups))
	}
	if o.MaxSize < 0 {
		allErrors = append(allErrors, fmt.Errorf("--audit-log-maxsize %v can't be a negative number", o.MaxSize))
	}

	return allErrors
}

// 根据配置判断日志后端是否启用。
func (o *AuditLogOptions) enabled() bool {
	return o != nil && o.Path != ""
}

// getWriter 返回日志后端的目标 writer；未启用时返回 (nil, nil)。
//
// MaxSize 为 0 表示禁用轮转，此时直接追加写文件；否则交给 lumberjack 负责轮转。
func (o *AuditLogOptions) getWriter() (io.Writer, error) {
	if !o.enabled() {
		return nil, nil
	}

	if o.Path == "-" {
		return os.Stdout, nil
	}

	if err := o.ensureLogFile(); err != nil {
		return nil, fmt.Errorf("ensureLogFile: %w", err)
	}

	if o.MaxSize == 0 {
		return os.OpenFile(o.Path, os.O_APPEND|os.O_WRONLY, 0644)
	}

	return &lumberjack.Logger{
		Filename:   o.Path,
		MaxAge:     o.MaxAge,
		MaxBackups: o.MaxBackups,
		MaxSize:    o.MaxSize,
		Compress:   o.Compress,
	}, nil
}

// ensureLogFile 预创建日志文件并设置权限：审计日志可能含敏感信息，权限必须是 0600。
//
// 提前创建是为了在启动阶段就暴露路径/权限问题，而不是等到第一条事件写入时才失败。
func (o *AuditLogOptions) ensureLogFile() error {
	if err := os.MkdirAll(filepath.Dir(o.Path), 0700); err != nil {
		return err
	}
	mode := os.FileMode(0600)
	f, err := os.OpenFile(o.Path, os.O_CREATE|os.O_APPEND|os.O_RDWR, mode)
	if err != nil {
		return err
	}
	return f.Close()
}

// newBackend 组装日志后端：底层写文件，外层依次套批量与截断。
func (o *AuditLogOptions) newBackend(w io.Writer) audit.Backend {
	groupVersion, _ := schema.ParseGroupVersion(o.GroupVersionString)
	log := pluginlog.NewBackend(w, o.Format, groupVersion)
	log = o.BatchOptions.wrapBackend(log)
	log = o.TruncateOptions.wrapBackend(log, groupVersion)
	return log
}

// AddFlags 注册 webhook 后端参数。
func (o *AuditWebhookOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "audit-webhook-config-file", o.ConfigFile,
		"Path to a kubeconfig formatted file that defines the audit webhook configuration.")
	fs.DurationVar(&o.InitialBackoff, "audit-webhook-initial-backoff",
		o.InitialBackoff, "The amount of time to wait before retrying the first failed request.")
	fs.DurationVar(&o.InitialBackoff, "audit-webhook-batch-initial-backoff",
		o.InitialBackoff, "The amount of time to wait before retrying the first failed request.")
	fs.MarkDeprecated("audit-webhook-batch-initial-backoff",
		"Deprecated, use --audit-webhook-initial-backoff instead.")
	fs.StringVar(&o.GroupVersionString, "audit-webhook-version", o.GroupVersionString,
		"API group and version used for serializing audit events written to webhook.")
}

// Validate 只在 webhook 后端启用时校验。
func (o *AuditWebhookOptions) Validate() []error {
	if !o.enabled() {
		return nil
	}

	var allErrors []error
	if err := validateBackendBatchOptions(pluginwebhook.PluginName, o.BatchOptions); err != nil {
		allErrors = append(allErrors, err)
	}
	if err := o.TruncateOptions.Validate(pluginwebhook.PluginName); err != nil {
		allErrors = append(allErrors, err)
	}

	if err := validateGroupVersionString(o.GroupVersionString); err != nil {
		allErrors = append(allErrors, err)
	}
	return allErrors
}

func (o *AuditWebhookOptions) enabled() bool {
	return o != nil && o.ConfigFile != ""
}

// newUntruncatedBackend 返回未套用截断选项的 Webhook 后端，
// 以便同一个截断后端能同时包装 webhook 与动态后端。
//
// customDial 为 nil 时使用默认拨号器；由 ApplyTo 根据 EgressSelector 决定是否传入。
func (o *AuditWebhookOptions) newUntruncatedBackend(customDial utilnet.DialFunc) (audit.Backend, error) {
	groupVersion, _ := schema.ParseGroupVersion(o.GroupVersionString)
	webhook, err := pluginwebhook.NewBackend(o.ConfigFile, groupVersion, webhook.DefaultRetryBackoffWithInitialDelay(o.InitialBackoff), customDial)
	if err != nil {
		return nil, fmt.Errorf("initializing audit webhook: %v", err)
	}
	webhook = o.BatchOptions.wrapBackend(webhook)
	return webhook, nil
}

// defaultWebhookBatchConfig 返回 Webhook 后端使用的默认批量配置。
func defaultWebhookBatchConfig() pluginbuffered.BatchConfig {
	return pluginbuffered.BatchConfig{
		BufferSize:   defaultBatchBufferSize,
		MaxBatchSize: defaultBatchMaxSize,
		MaxBatchWait: defaultBatchMaxWait,

		ThrottleEnable: true,
		ThrottleQPS:    defaultBatchThrottleQPS,
		ThrottleBurst:  defaultBatchThrottleBurst,

		AsyncDelegate: true,
	}
}

// defaultLogBatchConfig 返回日志后端使用的默认批量配置。
//
// 日志后端刻意关闭批量与异步：本地文件写入本身很快，批量只会增加延迟与锁竞争，
// 因此 MaxBatchSize 取 1 等价于逐条写入。
func defaultLogBatchConfig() pluginbuffered.BatchConfig {
	return pluginbuffered.BatchConfig{
		BufferSize: defaultBatchBufferSize,
		// 文件日志后端无需批量。
		// MaxBatchWait 被忽略。
		MaxBatchSize:   1,
		ThrottleEnable: false,
		// 异步写日志只会引入锁竞争。
		AsyncDelegate: false,
	}
}
