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

	// FeatureGate 用于在已有特性门控时将其传递进来。
	FeatureGate featuregate.FeatureGate
	// ExtraAdmissionInitializers 在上述所有选项的 ApplyTo 之后调用一次，
	// 把返回的准入插件 initializer 传给 Admission.ApplyTo。
	ExtraAdmissionInitializers func(c *server.RecommendedConfig) ([]admission.PluginInitializer, error)
	Admission                  *AdmissionOptions
	// API Server Egress Selector 用于控制 API Server 的出站流量
	EgressSelector *EgressSelectorOptions
	// Traces 包含控制分布式请求追踪的选项。
	Traces *TracingOptions
}

// NewRecommendedOptions 构造一组相互匹配的默认选项；prefix/codec 决定 etcd
// 键前缀与存储编码。
func NewRecommendedOptions(prefix string, codec runtime.Codec) *RecommendedOptions {
	sso := NewSecureServingOptions()

	// 这里为聚合 api-server 组合推荐选项：其客户端通常是代理，
	// 会把许多操作（尤其是长时运行的操作）复用
	// 到同一个指向本服务器的 HTTP/2 连接上，
	// 因此需要允许大量并发操作。
	sso.HTTP2MaxStreamsPerConnection = 1000

	return &RecommendedOptions{
		Etcd:           NewEtcdOptions(storagebackend.NewDefaultConfig(prefix, codec)),
		SecureServing:  sso.WithLoopback(),
		Authentication: NewDelegatingAuthenticationOptions(),
		Authorization:  NewDelegatingAuthorizationOptions(),
		Audit:          NewAuditOptions(),
		Features:       NewFeatureOptions(),
		CoreAPI:        NewCoreAPIOptions(),
		// 默认接一个全局变量，可惜人们会在不同 repo 中赋予它不同含义。
		// 建议创建自己的 FeatureGate，这样跨 repo 时变量含义才能保持一致。
		// 未来的你会感谢现在的你。
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
