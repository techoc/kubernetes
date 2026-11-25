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
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-base/featuregate"
)

// RecommendedOptions 包含运行 API Server 所需的推荐选项。
// 如果要在该列表中加入新选项，应按逻辑分组添加。
// 各子选项可为 nil，表示不启用对应功能；聚合型 apiserver
// 通过替换个别字段即可获得与 kube-apiserver 一致的基础行为。
type RecommendedOptions struct {
	Etcd           *EtcdOptions
	SecureServing  *SecureServingOptionsWithLoopback
	Authentication *DelegatingAuthenticationOptions
	Authorization  *DelegatingAuthorizationOptions
	Audit          *AuditOptions
	Features       *FeatureOptions
	CoreAPI        *CoreAPIOptions

	// FeatureGate is a way to plumb feature gate through if you have them.
	FeatureGate featuregate.FeatureGate
	// ExtraAdmissionInitializers is called once after all ApplyTo from the options above, to pass the returned
	// admission plugin initializers to Admission.ApplyTo.
	ExtraAdmissionInitializers func(c *server.RecommendedConfig) ([]admission.PluginInitializer, error)
	Admission                  *AdmissionOptions
	// API Server Egress Selector is used to control outbound traffic from the API Server
	EgressSelector *EgressSelectorOptions
	// Traces contains options to control distributed request tracing.
	Traces *TracingOptions
}

// NewRecommendedOptions 构造一组相互匹配的默认选项；prefix/codec 决定 etcd
// 键前缀与存储编码。
func NewRecommendedOptions(prefix string, codec runtime.Codec) *RecommendedOptions {
	sso := NewSecureServingOptions()

	// We are composing recommended options for an aggregated api-server,
	// whose client is typically a proxy multiplexing many operations ---
	// notably including long-running ones --- into one HTTP/2 connection
	// into this server.  So allow many concurrent operations.
	sso.HTTP2MaxStreamsPerConnection = 1000

	return &RecommendedOptions{
		Etcd:           NewEtcdOptions(storagebackend.NewDefaultConfig(prefix, codec)),
		SecureServing:  sso.WithLoopback(),
		Authentication: NewDelegatingAuthenticationOptions(),
		Authorization:  NewDelegatingAuthorizationOptions(),
		Audit:          NewAuditOptions(),
		Features:       NewFeatureOptions(),
		CoreAPI:        NewCoreAPIOptions(),
		// Wired a global by default that sadly people will abuse to have different meanings in different repos.
		// Please consider creating your own FeatureGate so you can have a consistent meaning for what a variable contains
		// across different repos.  Future you will thank you.
		FeatureGate:                feature.DefaultFeatureGate,
		ExtraAdmissionInitializers: func(c *server.RecommendedConfig) ([]admission.PluginInitializer, error) { return nil, nil },
		Admission:                  NewAdmissionOptions(),
		EgressSelector:             NewEgressSelectorOptions(),
		Traces:                     NewTracingOptions(),
	}
}

// AddFlags 把各子选项的标志注册到同一 FlagSet，保证命令行接口一致。
func (o *RecommendedOptions) AddFlags(fs *pflag.FlagSet) {
	o.Etcd.AddFlags(fs)
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.Authorization.AddFlags(fs)
	o.Audit.AddFlags(fs)
	o.Features.AddFlags(fs)
	o.CoreAPI.AddFlags(fs)
	o.Admission.AddFlags(fs)
	o.EgressSelector.AddFlags(fs)
	o.Traces.AddFlags(fs)
}

// ApplyTo 把 RecommendedOptions 添加到服务器配置中。
// pluginInitializers 可以为空，只有在需要额外初始化器时才用到它。
// 应用顺序有依赖——egress selector 要先于 tracing（tracing 依赖
// 出站连接），客户端要早于 features/admission（二者需要 kube client）。
func (o *RecommendedOptions) ApplyTo(config *server.RecommendedConfig) error {
	if err := o.Etcd.ApplyTo(&config.Config); err != nil {
		return err
	}
	if err := o.EgressSelector.ApplyTo(&config.Config); err != nil {
		return err
	}
	if err := o.Traces.ApplyTo(config.Config.EgressSelector, &config.Config); err != nil {
		return err
	}
	if err := o.SecureServing.ApplyToConfig(&config.Config); err != nil {
		return err
	}
	if err := o.Authentication.ApplyTo(&config.Config.Authentication, config.SecureServing, config.OpenAPIConfig); err != nil {
		return err
	}
	if err := o.Authorization.ApplyTo(&config.Config.Authorization); err != nil {
		return err
	}
	if err := o.Audit.ApplyTo(&config.Config); err != nil {
		return err
	}
	if err := o.CoreAPI.ApplyTo(config); err != nil {
		return err
	}
	// 没有 ClientConfig（例如仅本地测试）时跳过客户端构造，features 与
	// admission 需能接受 nil 客户端。
	var kubeClient kubernetes.Interface
	var dynamicClient dynamic.Interface
	if config.ClientConfig != nil {
		var err error
		kubeClient, err = kubernetes.NewForConfig(config.ClientConfig)
		if err != nil {
			return err
		}
		dynamicClient, err = dynamic.NewForConfig(config.ClientConfig)
		if err != nil {
			return err
		}
	}
	if err := o.Features.ApplyTo(&config.Config, kubeClient, config.SharedInformerFactory); err != nil {
		return err
	}
	// 额外初始化器必须在所有 ApplyTo 之后执行，才能读到已填充的配置。
	initializers, err := o.ExtraAdmissionInitializers(config)
	if err != nil {
		return err
	}
	if err := o.Admission.ApplyTo(&config.Config, config.SharedInformerFactory, kubeClient, dynamicClient, o.FeatureGate,
		config.EffectiveVersion,
		initializers...); err != nil {
		return err
	}
	return nil
}

// Validate 汇总所有子选项的错误；不短路，便于一次性报告全部配置问题。
func (o *RecommendedOptions) Validate() []error {
	errors := []error{}
	errors = append(errors, o.Etcd.Validate()...)
	errors = append(errors, o.SecureServing.Validate()...)
	errors = append(errors, o.Authentication.Validate()...)
	errors = append(errors, o.Authorization.Validate()...)
	errors = append(errors, o.Audit.Validate()...)
	errors = append(errors, o.Features.Validate()...)
	errors = append(errors, o.CoreAPI.Validate()...)
	errors = append(errors, o.Admission.Validate()...)
	errors = append(errors, o.EgressSelector.Validate()...)
	errors = append(errors, o.Traces.Validate()...)

	return errors
}
