/*
Copyright 2016 The Kubernetes Authors.

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
	"net"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	basecompatibility "k8s.io/component-base/compatibility"

	"github.com/spf13/pflag"
)

const (
	corsAllowedOriginsHelpText = "List of allowed origins for CORS, comma separated. " +
		"An allowed origin can be a regular expression to support subdomain matching. " +
		"If this list is empty CORS will not be enabled. " +
		"Please ensure each expression matches the entire hostname by anchoring " +
		"to the start with '^' or including the '//' prefix, and by anchoring to the " +
		"end with '$' or including the ':' port separator suffix. " +
		"Examples of valid expressions are '//example\\.com(:|$)' and '^https://example\\.com(:|$)'"
)

// ServerRunOptions 是 generic apiserver 的通用运行参数，最终映射到
// server.Config；它们决定限流、超时、优雅终止与版本模拟行为。
type ServerRunOptions struct {
	AdvertiseAddress net.IP

	CorsAllowedOriginList        []string
	HSTSDirectives               []string
	ExternalHost                 string
	MaxRequestsInFlight          int
	MaxMutatingRequestsInFlight  int
	RequestTimeout               time.Duration
	GoawayChance                 float64
	LivezGracePeriod             time.Duration
	MinRequestTimeout            int
	StorageInitializationTimeout time.Duration
	ShutdownDelayDuration        time.Duration
	// JSON Patch 应用前允许复制的最大字节数，防止超大 patch 放大内存占用；
	// 库使用方自行决定是否暴露 flag。
	JSONPatchMaxCopyBytes int64
	// The limit on the request body size that would be accepted and
	// decoded in a write request. 0 means no limit.
	// We intentionally did not add a flag for this option. Users of the
	// apiserver library can wire it to a flag.
	MaxRequestBodyBytes int64

	// ShutdownSendRetryAfter dictates when to initiate shutdown of the HTTP
	// Server during the graceful termination of the apiserver. If true, we wait
	// for non longrunning requests in flight to be drained and then initiate a
	// shutdown of the HTTP Server. If false, we initiate a shutdown of the HTTP
	// Server as soon as ShutdownDelayDuration has elapsed.
	// If enabled, after ShutdownDelayDuration elapses, any incoming request is
	// rejected with a 429 status code and a 'Retry-After' response.
	ShutdownSendRetryAfter bool

	// ShutdownWatchTerminationGracePeriod, if set to a positive value,
	// is the maximum duration the apiserver will wait for all active
	// watch request(s) to drain.
	// Once this grace period elapses, the apiserver will no longer
	// wait for any active watch request(s) in flight to drain, it will
	// proceed to the next step in the graceful server shutdown process.
	// If set to a positive value, the apiserver will keep track of the
	// number of active watch request(s) in flight and during shutdown
	// it will wait, at most, for the specified duration and allow these
	// active watch requests to drain with some rate limiting in effect.
	// The default is zero, which implies the apiserver will not keep
	// track of active watch request(s) in flight and will not wait
	// for them to drain, this maintains backward compatibility.
	// This grace period is orthogonal to other grace periods, and
	// it is not overridden by any other grace period.
	ShutdownWatchTerminationGracePeriod time.Duration

	// 组件级有效版本与 feature gate 的全局注册表；共享它才能保证同一进程内
	// 各组件看到一致的版本配置。
	ComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry
	// ComponentName is name under which the server's global variabled are registered in the ComponentGlobalsRegistry.
	ComponentName string
	// EmulationForwardCompatible 是一个隐式启用所有在模拟版本之后引入、且优先级高于模拟版本下已启用的同组同资源 API 的选项。
	// 为 true 时，所有优先级高于模拟版本下已启用的同组同资源 API（beta+）都会被安装；不适用于 alpha API。
	// 当控制器实现已在二进制版本中迁移到更新的 API 版本、即使设置了模拟版本也要继续使用新版本时，需要开启该选项。
	// 仅在模拟版本低于二进制版本时才有意义（见 Validate）。
	EmulationForwardCompatible bool
	// RuntimeConfigEmulationForwardCompatible is an option to explicitly enable specific APIs introduced after the emulation version through the runtime-config.
	// If true, APIs identified by group/version that are enabled in the --runtime-config flag will be installed even if it is introduced after the emulation version. --runtime-config flag values that identify multiple APIs, such as api/all,api/ga,api/beta, are not influenced by this flag and will only enable APIs available at the current emulation version.
	// If false, error would be thrown if any GroupVersion or GroupVersionResource explicitly enabled in the --runtime-config flag is introduced after the emulation version.
	RuntimeConfigEmulationForwardCompatible bool
}

// NewServerRunOptions 创建默认 kube 组件的运行选项；若全局注册表尚未登记
// 该组件，则先注册有效版本与 feature gate，保证后续 SetFallback 有对象可操作。
func NewServerRunOptions() *ServerRunOptions {
	if compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent) == nil {
		featureGate := utilfeature.DefaultMutableFeatureGate
		effectiveVersion := compatibility.DefaultBuildEffectiveVersion()
		utilruntime.Must(compatibility.DefaultComponentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, featureGate))
	}

	return NewServerRunOptionsForComponent(basecompatibility.DefaultKubeComponent, compatibility.DefaultComponentGlobalsRegistry)
}

// NewServerRunOptionsForComponent 为指定组件构造选项；默认值取自
// server.NewConfig，确保库内默认与 server.Config 一致。
func NewServerRunOptionsForComponent(componentName string, componentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry) *ServerRunOptions {
	defaults := server.NewConfig(serializer.CodecFactory{})
	return &ServerRunOptions{
		MaxRequestsInFlight:                 defaults.MaxRequestsInFlight,
		MaxMutatingRequestsInFlight:         defaults.MaxMutatingRequestsInFlight,
		RequestTimeout:                      defaults.RequestTimeout,
		LivezGracePeriod:                    defaults.LivezGracePeriod,
		MinRequestTimeout:                   defaults.MinRequestTimeout,
		StorageInitializationTimeout:        defaults.StorageInitializationTimeout,
		ShutdownDelayDuration:               defaults.ShutdownDelayDuration,
		ShutdownWatchTerminationGracePeriod: defaults.ShutdownWatchTerminationGracePeriod,
		JSONPatchMaxCopyBytes:               defaults.JSONPatchMaxCopyBytes,
		MaxRequestBodyBytes:                 defaults.MaxRequestBodyBytes,
		ShutdownSendRetryAfter:              false,
		ComponentName:                       componentName,
		ComponentGlobalsRegistry:            componentGlobalsRegistry,
	}
}

// ApplyTo 将运行参数应用到接收者并返回自身。
// 先把注册表状态固定为 fallback，再一次性拷贝到 Config；版本与 feature gate
// 按 ComponentName 取出，避免组件间相互覆盖。
func (s *ServerRunOptions) ApplyTo(c *server.Config) error {
	if err := s.ComponentGlobalsRegistry.SetFallback(); err != nil {
		return err
	}
	c.CorsAllowedOriginList = s.CorsAllowedOriginList
	c.HSTSDirectives = s.HSTSDirectives
	c.ExternalAddress = s.ExternalHost
	c.MaxRequestsInFlight = s.MaxRequestsInFlight
	c.MaxMutatingRequestsInFlight = s.MaxMutatingRequestsInFlight
	c.LivezGracePeriod = s.LivezGracePeriod
	c.RequestTimeout = s.RequestTimeout
	c.GoawayChance = s.GoawayChance
	c.MinRequestTimeout = s.MinRequestTimeout
	c.StorageInitializationTimeout = s.StorageInitializationTimeout
	c.ShutdownDelayDuration = s.ShutdownDelayDuration
	c.JSONPatchMaxCopyBytes = s.JSONPatchMaxCopyBytes
	c.MaxRequestBodyBytes = s.MaxRequestBodyBytes
	c.PublicAddress = s.AdvertiseAddress
	c.ShutdownSendRetryAfter = s.ShutdownSendRetryAfter
	c.ShutdownWatchTerminationGracePeriod = s.ShutdownWatchTerminationGracePeriod
	c.EffectiveVersion = s.ComponentGlobalsRegistry.EffectiveVersionFor(s.ComponentName)
	c.FeatureGate = s.ComponentGlobalsRegistry.FeatureGateFor(s.ComponentName)
	c.EmulationForwardCompatible = s.EmulationForwardCompatible
	c.RuntimeConfigEmulationForwardCompatible = s.RuntimeConfigEmulationForwardCompatible

	return nil
}

// 未指定或为未指定地址（0.0.0.0/::）时，从 secure serving 的绑定地址推导
// 一个集群内可达的地址，否则对外的 advertised URL 不可用。
func (s *ServerRunOptions) DefaultAdvertiseAddress(secure *SecureServingOptions) error {
	if secure == nil {
		return nil
	}

	if s.AdvertiseAddress == nil || s.AdvertiseAddress.IsUnspecified() {
		hostIP, err := secure.DefaultExternalAddress()
		if err != nil {
			return fmt.Errorf("Unable to find suitable network address.error='%v'. "+
				"Try to set the AdvertiseAddress directly or provide a valid BindAddress to fix this.", err)
		}
		s.AdvertiseAddress = hostIP
	}

	return nil
}

// Validate 校验 ServerRunOptions 并返回错误列表：所有数值型参数统一拒绝负值；goaway 概率额外限制在 [0, 0.02]，避免误配导致大面积断连。版本相关选项只在真正处于模拟模式时才允许开启。
func (s *ServerRunOptions) Validate() []error {
	errors := []error{}

	if s.LivezGracePeriod < 0 {
		errors = append(errors, fmt.Errorf("--livez-grace-period can not be a negative value"))
	}

	if s.MaxRequestsInFlight < 0 {
		errors = append(errors, fmt.Errorf("--max-requests-inflight can not be negative value"))
	}
	if s.MaxMutatingRequestsInFlight < 0 {
		errors = append(errors, fmt.Errorf("--max-mutating-requests-inflight can not be negative value"))
	}

	if s.RequestTimeout.Nanoseconds() < 0 {
		errors = append(errors, fmt.Errorf("--request-timeout can not be negative value"))
	}

	if s.GoawayChance < 0 || s.GoawayChance > 0.02 {
		errors = append(errors, fmt.Errorf("--goaway-chance can not be less than 0 or greater than 0.02"))
	}

	if s.MinRequestTimeout < 0 {
		errors = append(errors, fmt.Errorf("--min-request-timeout can not be negative value"))
	}

	if s.StorageInitializationTimeout < 0 {
		errors = append(errors, fmt.Errorf("--storage-initialization-timeout can not be negative value"))
	}

	if s.ShutdownDelayDuration < 0 {
		errors = append(errors, fmt.Errorf("--shutdown-delay-duration can not be negative value"))
	}

	if s.ShutdownWatchTerminationGracePeriod < 0 {
		errors = append(errors, fmt.Errorf("shutdown-watch-termination-grace-period, if provided, can not be a negative value"))
	}

	if s.JSONPatchMaxCopyBytes < 0 {
		errors = append(errors, fmt.Errorf("ServerRunOptions.JSONPatchMaxCopyBytes can not be negative value"))
	}

	if s.MaxRequestBodyBytes < 0 {
		errors = append(errors, fmt.Errorf("ServerRunOptions.MaxRequestBodyBytes can not be negative value"))
	}

	if err := validateHSTSDirectives(s.HSTSDirectives); err != nil {
		errors = append(errors, err)
	}

	if err := validateCorsAllowedOriginList(s.CorsAllowedOriginList); err != nil {
		errors = append(errors, err)
	}
	if errs := s.ComponentGlobalsRegistry.Validate(); len(errs) != 0 {
		errors = append(errors, errs...)
	}
	effectiveVersion := s.ComponentGlobalsRegistry.EffectiveVersionFor(s.ComponentName)
	if effectiveVersion == nil {
		return errors
	}
	// 模拟版本与二进制版本相同即未启用模拟，此时 forward-compatible
	// 选项没有语义，开启只会让配置看起来生效却毫无作用，故拒绝。
	notEmulationMode := effectiveVersion.BinaryVersion().WithPatch(0).EqualTo(effectiveVersion.EmulationVersion())
	if notEmulationMode && s.EmulationForwardCompatible {
		errors = append(errors, fmt.Errorf("ServerRunOptions.EmulationForwardCompatible cannot be set to true if the emulation version is the same as the binary version"))
	}
	if notEmulationMode && s.RuntimeConfigEmulationForwardCompatible {
		errors = append(errors, fmt.Errorf("ServerRunOptions.RuntimeConfigEmulationForwardCompatible cannot be set to true if the emulation version is the same as the binary version"))
	}
	return errors
}

// validateHSTSDirectives 校验 HSTS 指令：允许 includeSubDomains/preload，或
// 形如 max-age=N 的单个指令；其他写法一律报错，避免下发无效安全头。
func validateHSTSDirectives(hstsDirectives []string) error {
	// HSTS Headers format: Strict-Transport-Security:max-age=expireTime [;includeSubDomains] [;preload]
	// See https://tools.ietf.org/html/rfc6797#section-6.1 for more information
	allErrors := []error{}
	for _, hstsDirective := range hstsDirectives {
		if len(strings.TrimSpace(hstsDirective)) == 0 {
			allErrors = append(allErrors, fmt.Errorf("empty value in strict-transport-security-directives"))
			continue
		}
		if hstsDirective != "includeSubDomains" && hstsDirective != "preload" {
			maxAgeDirective := strings.Split(hstsDirective, "=")
			if len(maxAgeDirective) != 2 || maxAgeDirective[0] != "max-age" {
				allErrors = append(allErrors, fmt.Errorf("--strict-transport-security-directives invalid, allowed values: max-age=expireTime, includeSubDomains, preload. see https://tools.ietf.org/html/rfc6797#section-6.1 for more information"))
			}
		}
	}
	return errors.NewAggregate(allErrors)
}

// validateCorsAllowedOriginList 校验 CORS 白名单正则。强制把表达式锚定到
// 主机名首尾，是对 CVE-2022-1996 的缓解：过宽的正则（如 .*example.com）
// 会匹配攻击者控制的子域，从而允许其跨域读取 API 响应。
func validateCorsAllowedOriginList(corsAllowedOriginList []string) error {
	allErrors := []error{}
	validateRegexFn := func(regexpStr string) error {
		if _, err := regexp.Compile(regexpStr); err != nil {
			return err
		}

		// 正则表达式应当锚定 Origin 头中 host 的起止位置，这可以防御 CVE-2022-1996。
		// 锚定 origin 中 host 起始位置的可能方式：
		//   - 用 '^' 匹配 origin 的开头
		//   - 用 '//' 或 '://' 匹配 scheme 与 host 的分隔符，
		//     这样同样锚定到 origin 头中 host 的起始位置。
		// 匹配 origin 中 host 结尾的可能方式：
		//   - 用 '$' 匹配 origin 的结尾
		//   - 用捕获组匹配 host 与 port 的分隔符 '(:|$)'
		// 我们将放宽校验，只检查用户指定的表达式中是否包含这些正则标记。
		// 这是启发式检查：只确认标记存在，不保证正则语义上真的锚定；
		// 更严格的判定成本高且容易误伤，故保持现状。
		var pinStart, pinEnd bool
		for _, prefix := range []string{"^", "//"} {
			if strings.Contains(regexpStr, prefix) {
				pinStart = true
				break
			}
		}
		for _, suffix := range []string{"$", ":"} {
			if strings.Contains(regexpStr, suffix) {
				pinEnd = true
				break
			}
		}
		if !pinStart || !pinEnd {
			return fmt.Errorf("regular expression does not pin to start/end of host in the origin header")
		}
		return nil
	}

	for _, regexp := range corsAllowedOriginList {
		if len(regexp) == 0 {
			allErrors = append(allErrors, fmt.Errorf("empty value in --cors-allowed-origins, help: %s", corsAllowedOriginsHelpText))
			continue
		}

		if err := validateRegexFn(regexp); err != nil {
			err = fmt.Errorf("--cors-allowed-origins has an invalid regular expression: %v, help: %s", err, corsAllowedOriginsHelpText)
			allErrors = append(allErrors, err)
		}
	}
	return errors.NewAggregate(allErrors)
}

// AddUniversalFlags 把特定 APIServer 的 flag 添加到指定 FlagSet；这些 flag 是所有 apiserver 共享的通用参数，组件特有的 flag 由各自 options 结构补充。
func (s *ServerRunOptions) AddUniversalFlags(fs *pflag.FlagSet) {
	// Note: the weird ""+ in below lines seems to be the only way to get gofmt to
	// arrange these text blocks sensibly. Grrr.

	fs.IPVar(&s.AdvertiseAddress, "advertise-address", s.AdvertiseAddress, ""+
		"The IP address on which to advertise the apiserver to members of the cluster. This "+
		"address must be reachable by the rest of the cluster. If blank, the --bind-address "+
		"will be used. If --bind-address is unspecified, the host's default interface will "+
		"be used.")

	fs.StringSliceVar(&s.CorsAllowedOriginList, "cors-allowed-origins", s.CorsAllowedOriginList, corsAllowedOriginsHelpText)

	fs.StringSliceVar(&s.HSTSDirectives, "strict-transport-security-directives", s.HSTSDirectives, ""+
		"List of directives for HSTS, comma separated. If this list is empty, then HSTS directives will not "+
		"be added. Example: 'max-age=31536000,includeSubDomains,preload'")

	fs.StringVar(&s.ExternalHost, "external-hostname", s.ExternalHost,
		"The hostname to use when generating externalized URLs for this master (e.g. Swagger API Docs or OpenID Discovery).")

	fs.IntVar(&s.MaxRequestsInFlight, "max-requests-inflight", s.MaxRequestsInFlight, ""+
		"This and --max-mutating-requests-inflight are summed to determine the server's total concurrency limit "+
		"(which must be positive) if --enable-priority-and-fairness is true. "+
		"Otherwise, this flag limits the maximum number of non-mutating requests in flight, "+
		"or a zero value disables the limit completely.")

	fs.IntVar(&s.MaxMutatingRequestsInFlight, "max-mutating-requests-inflight", s.MaxMutatingRequestsInFlight, ""+
		"This and --max-requests-inflight are summed to determine the server's total concurrency limit "+
		"(which must be positive) if --enable-priority-and-fairness is true. "+
		"Otherwise, this flag limits the maximum number of mutating requests in flight, "+
		"or a zero value disables the limit completely.")

	fs.DurationVar(&s.RequestTimeout, "request-timeout", s.RequestTimeout, ""+
		"An optional field indicating the duration a handler must keep a request open before timing "+
		"it out. This is the default request timeout for requests but may be overridden by flags such as "+
		"--min-request-timeout for specific types of requests.")

	fs.Float64Var(&s.GoawayChance, "goaway-chance", s.GoawayChance, ""+
		"To prevent HTTP/2 clients from getting stuck on a single apiserver, randomly close a connection (GOAWAY). "+
		"The client's other in-flight requests won't be affected, and the client will reconnect, likely landing on a different apiserver after going through the load balancer again. "+
		"This argument sets the fraction of requests that will be sent a GOAWAY. Clusters with single apiservers, or which don't use a load balancer, should NOT enable this. "+
		"Min is 0 (off), Max is .02 (1/50 requests); .001 (1/1000) is a recommended starting point.")

	fs.DurationVar(&s.LivezGracePeriod, "livez-grace-period", s.LivezGracePeriod, ""+
		"This option represents the maximum amount of time it should take for apiserver to complete its startup sequence "+
		"and become live. From apiserver's start time to when this amount of time has elapsed, /livez will assume "+
		"that unfinished post-start hooks will complete successfully and therefore return true.")

	fs.IntVar(&s.MinRequestTimeout, "min-request-timeout", s.MinRequestTimeout, ""+
		"An optional field indicating the minimum number of seconds a handler must keep "+
		"a request open before timing it out. Currently only honored by the watch request "+
		"handler, which picks a randomized value above this number as the connection timeout, "+
		"to spread out load.")

	fs.DurationVar(&s.StorageInitializationTimeout, "storage-initialization-timeout", s.StorageInitializationTimeout,
		"Maximum amount of time to wait for storage initialization before declaring apiserver ready. Defaults to 1m.")

	fs.DurationVar(&s.ShutdownDelayDuration, "shutdown-delay-duration", s.ShutdownDelayDuration, ""+
		"Time to delay the termination. During that time the server keeps serving requests normally. The endpoints /healthz and /livez "+
		"will return success, but /readyz immediately returns failure. Graceful termination starts after this delay "+
		"has elapsed. This can be used to allow load balancer to stop sending traffic to this server.")

	fs.BoolVar(&s.ShutdownSendRetryAfter, "shutdown-send-retry-after", s.ShutdownSendRetryAfter, ""+
		"If true the HTTP Server will continue listening until all non long running request(s) in flight have been drained, "+
		"during this window all incoming requests will be rejected with a status code 429 and a 'Retry-After' response header, "+
		"in addition 'Connection: close' response header is set in order to tear down the TCP connection when idle.")

	fs.DurationVar(&s.ShutdownWatchTerminationGracePeriod, "shutdown-watch-termination-grace-period", s.ShutdownWatchTerminationGracePeriod, ""+
		"This option, if set, represents the maximum amount of grace period the apiserver will wait "+
		"for active watch request(s) to drain during the graceful server shutdown window.")

	s.ComponentGlobalsRegistry.AddFlags(fs)
	fs.BoolVar(&s.EmulationForwardCompatible, "emulation-forward-compatible", s.EmulationForwardCompatible, ""+
		"If true, for any beta+ APIs enabled by default or by --runtime-config at the emulation version, their future versions with higher priority/stability will be auto enabled even if they introduced after the emulation version. "+
		"Can only be set to true if the emulation version is lower than the binary version.")
	fs.BoolVar(&s.RuntimeConfigEmulationForwardCompatible, "runtime-config-emulation-forward-compatible", s.RuntimeConfigEmulationForwardCompatible, ""+
		"If true, APIs identified by group/version that are enabled in the --runtime-config flag will be installed even if it is introduced after the emulation version. "+
		"If false, server would fail to start if any APIs identified by group/version that are enabled in the --runtime-config flag are introduced after the emulation version. "+
		"Can only be set to true if the emulation version is lower than the binary version.")
}

// Complete 用默认值填充缺失字段：SetFallback 会为尚未显式设置的版本/feature gate 选择默认值；必须在 ApplyTo 之前完成，否则 Config 会拿到空版本。
func (s *ServerRunOptions) Complete() error {
	return s.ComponentGlobalsRegistry.SetFallback()
}
