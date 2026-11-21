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
	"strings"
	"time"

	authorizationcel "k8s.io/apiserver/pkg/authorization/cel"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	authzconfig "k8s.io/apiserver/pkg/apis/apiserver"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	versionedinformers "k8s.io/client-go/informers"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer"
	authzmodes "k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
)

const (
	defaultWebhookName                      = "default"
	authorizationModeFlag                   = "authorization-mode"
	authorizationWebhookConfigFileFlag      = "authorization-webhook-config-file"
	authorizationWebhookVersionFlag         = "authorization-webhook-version"
	authorizationWebhookAuthorizedTTLFlag   = "authorization-webhook-cache-authorized-ttl"
	authorizationWebhookUnauthorizedTTLFlag = "authorization-webhook-cache-unauthorized-ttl"
	authorizationPolicyFileFlag             = "authorization-policy-file"
	authorizationConfigFlag                 = "authorization-config"
)

// BuiltInAuthorizationOptions 包含 API Server 所有内建的授权选项。
type BuiltInAuthorizationOptions struct {
	Modes                       []string
	PolicyFile                  string
	WebhookConfigFile           string
	WebhookVersion              string
	WebhookCacheAuthorizedTTL   time.Duration
	WebhookCacheUnauthorizedTTL time.Duration
	// WebhookRetryBackoff 指定授权 webhook 重试逻辑的退避参数。
	// 通过配置每次迭代的休眠时间与最大重试次数，可在系统降级时限制请求放大效应。
	WebhookRetryBackoff *wait.Backoff

	// AuthorizationConfigurationFile 与以下所有字段互斥：
	//	- Modes
	//	- WebhookConfigFile
	//	- WebHookVersion
	//	- WebhookCacheAuthorizedTTL
	//	- WebhookCacheUnauthorizedTTL
	AuthorizationConfigurationFile string

	AreLegacyFlagsSet func() bool
}

// NewBuiltInAuthorizationOptions 创建一个带默认值的 BuiltInAuthorizationOptions。
func NewBuiltInAuthorizationOptions() *BuiltInAuthorizationOptions {
	return &BuiltInAuthorizationOptions{
		Modes:                       []string{},
		WebhookVersion:              "v1beta1",
		WebhookCacheAuthorizedTTL:   5 * time.Minute,
		WebhookCacheUnauthorizedTTL: 30 * time.Second,
		WebhookRetryBackoff:         genericoptions.DefaultAuthWebhookRetryBackoff(),
	}
}

// Complete 补全授权选项：未指定任何模式且未使用配置文件时，默认采用 AlwaysAllow。
func (o *BuiltInAuthorizationOptions) Complete() []error {
	if o == nil {
		return nil
	}

	if len(o.AuthorizationConfigurationFile) == 0 && len(o.Modes) == 0 {
		o.Modes = []string{authzmodes.ModeAlwaysAllow}
	}
	return nil
}

// Validate 检查非法的配置组合。
func (o *BuiltInAuthorizationOptions) Validate() []error {
	if o == nil {
		return nil
	}
	var allErrors []error

	// 若设置了 --authorization-config，需检查旧标志未被设置且配置文件合法可加载。
	if o.AuthorizationConfigurationFile != "" {
		// 与旧标志互斥，同时指定则报错。
		if o.AreLegacyFlagsSet != nil && o.AreLegacyFlagsSet() {
			return append(allErrors, fmt.Errorf("--%s can not be specified when --%s or --authorization-webhook-* flags are defined", authorizationConfigFlag, authorizationModeFlag))
		}

		// 加载并校验 kube-apiserver 授权配置，此处不强制要求特定模式。
		_, _, err := authorizer.LoadAndValidateFile(o.AuthorizationConfigurationFile, authorizationcel.NewDefaultCompiler(), nil)
		if err != nil {
			return append(allErrors, err)
		}

		return allErrors
	}

	// 未使用 --authorization-config 时，按旧模式校验旧标志。
	if len(o.Modes) == 0 {
		allErrors = append(allErrors, fmt.Errorf("at least one authorization-mode must be passed"))
	}

	modes := sets.NewString(o.Modes...)
	for _, mode := range o.Modes {
		if !authzmodes.IsValidAuthorizationMode(mode) {
			allErrors = append(allErrors, fmt.Errorf("authorization-mode %q is not a valid mode", mode))
		}
		if mode == authzmodes.ModeABAC && o.PolicyFile == "" {
			allErrors = append(allErrors, fmt.Errorf("authorization-mode ABAC's authorization policy file not passed"))
		}
		if mode == authzmodes.ModeWebhook && o.WebhookConfigFile == "" {
			allErrors = append(allErrors, fmt.Errorf("authorization-mode Webhook's authorization config file not passed"))
		}
	}

	if o.PolicyFile != "" && !modes.Has(authzmodes.ModeABAC) {
		allErrors = append(allErrors, fmt.Errorf("cannot specify --authorization-policy-file without mode ABAC"))
	}

	if o.WebhookConfigFile != "" && !modes.Has(authzmodes.ModeWebhook) {
		allErrors = append(allErrors, fmt.Errorf("cannot specify --authorization-webhook-config-file without mode Webhook"))
	}

	if len(o.Modes) != modes.Len() {
		allErrors = append(allErrors, fmt.Errorf("authorization-mode %q has mode specified more than once", o.Modes))
	}

	if o.WebhookRetryBackoff != nil && o.WebhookRetryBackoff.Steps <= 0 {
		allErrors = append(allErrors, fmt.Errorf("number of webhook retry attempts must be greater than 0, but is: %d", o.WebhookRetryBackoff.Steps))
	}

	return allErrors
}

// AddFlags 将 API Server 授权相关的标志添加到指定的 FlagSet。
func (o *BuiltInAuthorizationOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.StringSliceVar(&o.Modes, authorizationModeFlag, o.Modes, ""+
		"Ordered list of plug-ins to do authorization on secure port. Defaults to AlwaysAllow if --authorization-config is not used. Comma-delimited list of: "+
		strings.Join(authzmodes.AuthorizationModeChoices, ",")+".")

	fs.StringVar(&o.PolicyFile, authorizationPolicyFileFlag, o.PolicyFile, ""+
		"File with authorization policy in json line by line format, used with --authorization-mode=ABAC, on the secure port.")

	fs.StringVar(&o.WebhookConfigFile, authorizationWebhookConfigFileFlag, o.WebhookConfigFile, ""+
		"File with webhook configuration in kubeconfig format, used with --authorization-mode=Webhook. "+
		"The API server will query the remote service to determine access on the API server's secure port.")

	fs.StringVar(&o.WebhookVersion, authorizationWebhookVersionFlag, o.WebhookVersion, ""+
		"The API version of the authorization.k8s.io SubjectAccessReview to send to and expect from the webhook.")

	fs.DurationVar(&o.WebhookCacheAuthorizedTTL, authorizationWebhookAuthorizedTTLFlag,
		o.WebhookCacheAuthorizedTTL,
		"The duration to cache 'authorized' responses from the webhook authorizer.")

	fs.DurationVar(&o.WebhookCacheUnauthorizedTTL,
		authorizationWebhookUnauthorizedTTLFlag, o.WebhookCacheUnauthorizedTTL,
		"The duration to cache 'unauthorized' responses from the webhook authorizer.")

	fs.StringVar(&o.AuthorizationConfigurationFile, authorizationConfigFlag, o.AuthorizationConfigurationFile, ""+
		"File with Authorization Configuration to configure the authorizer chain. "+
		"This flag is mutually exclusive with the other --authorization-mode and --authorization-webhook-* flags.")

	// 保留初始化时可能已设置的方法，以便兼容外部注入的校验逻辑。
	oldAreLegacyFlagsSet := o.AreLegacyFlagsSet
	o.AreLegacyFlagsSet = func() bool {
		if oldAreLegacyFlagsSet != nil && oldAreLegacyFlagsSet() {
			return true
		}

		return fs.Changed(authorizationModeFlag) ||
			fs.Changed(authorizationWebhookConfigFileFlag) ||
			fs.Changed(authorizationWebhookVersionFlag) ||
			fs.Changed(authorizationWebhookAuthorizedTTLFlag) ||
			fs.Changed(authorizationWebhookUnauthorizedTTLFlag)
	}
}

// ToAuthorizationConfig 将 BuiltInAuthorizationOptions 转换为 authorizer.Config。
func (o *BuiltInAuthorizationOptions) ToAuthorizationConfig(versionedInformerFactory versionedinformers.SharedInformerFactory) (*authorizer.Config, error) {
	if o == nil {
		return nil, nil
	}

	var authorizationConfiguration *authzconfig.AuthorizationConfiguration
	var err error
	var authorizationConfigData string

	// 使用 --authorization-config 时校验旧标志未设置并加载配置文件；
	// 否则从旧标志构建 AuthorizationConfig。
	if o.AuthorizationConfigurationFile != "" {
		// 与旧标志互斥，同时指定则报错。
		if o.AreLegacyFlagsSet != nil && o.AreLegacyFlagsSet() {
			return nil, fmt.Errorf("--%s can not be specified when --%s or --authorization-webhook-* flags are defined", authorizationConfigFlag, authorizationModeFlag)
		}
		// 加载并校验 kube-apiserver 授权配置，此处不强制要求特定模式。
		authorizationConfiguration, authorizationConfigData, err = authorizer.LoadAndValidateFile(o.AuthorizationConfigurationFile, authorizationcel.NewDefaultCompiler(), nil)
		if err != nil {
			return nil, err
		}
	} else {
		authorizationConfiguration, err = o.buildAuthorizationConfiguration()
		if err != nil {
			return nil, fmt.Errorf("failed to build authorization config: %s", err)
		}
	}

	return &authorizer.Config{
		PolicyFile:               o.PolicyFile,
		VersionedInformerFactory: versionedInformerFactory,
		WebhookRetryBackoff:      o.WebhookRetryBackoff,

		ReloadFile:                            o.AuthorizationConfigurationFile,
		AuthorizationConfiguration:            authorizationConfiguration,
		InitialAuthorizationConfigurationData: authorizationConfigData,
	}, nil
}

// buildAuthorizationConfiguration 将现有标志转换为 AuthorizationConfiguration 格式。
func (o *BuiltInAuthorizationOptions) buildAuthorizationConfiguration() (*authzconfig.AuthorizationConfiguration, error) {
	var authorizers []authzconfig.AuthorizerConfiguration

	if len(o.Modes) != sets.NewString(o.Modes...).Len() {
		return nil, fmt.Errorf("modes should not be repeated in --authorization-mode")
	}

	for _, mode := range o.Modes {
		switch mode {
		case authzmodes.ModeWebhook:
			authorizers = append(authorizers, authzconfig.AuthorizerConfiguration{
				Type: authzconfig.TypeWebhook,
				Name: defaultWebhookName,
				Webhook: &authzconfig.WebhookConfiguration{
					AuthorizedTTL:             metav1.Duration{Duration: o.WebhookCacheAuthorizedTTL},
					CacheAuthorizedRequests:   o.WebhookCacheAuthorizedTTL != 0,
					UnauthorizedTTL:           metav1.Duration{Duration: o.WebhookCacheUnauthorizedTTL},
					CacheUnauthorizedRequests: o.WebhookCacheUnauthorizedTTL != 0,
					// 新配置要求显式设置 Timeout 与 FailurePolicy。
					// 此处隐式赋予默认值，以保持向后兼容。
					Timeout:                    metav1.Duration{Duration: 30 * time.Second},
					FailurePolicy:              authzconfig.FailurePolicyNoOpinion,
					SubjectAccessReviewVersion: o.WebhookVersion,
					ConnectionInfo: authzconfig.WebhookConnectionInfo{
						Type:           authzconfig.AuthorizationWebhookConnectionInfoTypeKubeConfigFile,
						KubeConfigFile: &o.WebhookConfigFile,
					},
				},
			})
		default:
			authorizers = append(authorizers, authzconfig.AuthorizerConfiguration{
				Type: authzconfig.AuthorizerType(mode),
				Name: authorizer.GetNameForAuthorizerMode(mode),
			})
		}
	}

	return &authzconfig.AuthorizationConfiguration{Authorizers: authorizers}, nil
}
