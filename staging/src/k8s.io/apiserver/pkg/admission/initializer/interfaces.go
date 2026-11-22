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

package initializer

import (
	admissionregistrationv1api "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
)

// WantsExternalKubeClientSet defines a function which sets external ClientSet for admission plugins that need it
//
// 中文说明：可选接口模式——插件按需实现，初始化器通过类型断言注入依赖，
// 避免所有插件被迫依赖庞大的 clientset。
type WantsExternalKubeClientSet interface {
	SetExternalKubeClientSet(kubernetes.Interface)
	admission.InitializationValidator
}

// WantsExternalKubeInformerFactory defines a function which sets InformerFactory for admission plugins that need it
//
// 中文说明：共享同一 informer factory 可让多个插件复用底层 watch，
// 显著降低 apiserver 对 API 对象的监听开销。
type WantsExternalKubeInformerFactory interface {
	SetExternalKubeInformerFactory(informers.SharedInformerFactory)
	admission.InitializationValidator
}

// WantsUnconditionalAuthorizer defines a function which sets a narrower, conditions-unaware UnconditionalAuthorizer for admission plugins that need it.
//
// 中文说明：不带条件（conditions）的授权器语义更简单，适合那些只需要
// allow/deny 结论、且不能容忍授权结果被条件延迟的插件。
type WantsUnconditionalAuthorizer interface {
	SetUnconditionalAuthorizer(authorizer.UnconditionalAuthorizer)
	admission.InitializationValidator
}

// WantsAuthorizer defines a function which sets a standard, possibly conditions-aware Authorizer for admission plugins that need it.
//
// 中文说明：与 WantsUnconditionalAuthorizer 相对，允许返回带条件的授权决定；
// 插件需自行处理条件语义，不能当作简单的布尔判断。
type WantsAuthorizer interface {
	SetAuthorizer(authorizer.Authorizer)
	admission.InitializationValidator
}

// WantsQuotaConfiguration defines a function which sets quota configuration for admission plugins that need it.
//
// 中文说明：目前主要由 ResourceQuota 插件使用，用于解析配额作用域与资源映射。
type WantsQuotaConfiguration interface {
	SetQuotaConfiguration(quota.Configuration)
	admission.InitializationValidator
}

// WantsDrainedNotification defines a function which sets the notification of where the apiserver
// has already been drained for admission plugins that need it.
// After receiving that notification, Admit/Validate calls won't be called anymore.
//
// 中文说明：关闭通道而非发送值，是 Go 中一对多广播的标准做法；插件收到
// 关闭信号后应停止后台工作并释放资源，无需担心遗漏消息。
type WantsDrainedNotification interface {
	SetDrainedNotification(<-chan struct{})
	admission.InitializationValidator
}

// WantsFeatureGate defines a function which passes the featureGates for inspection by an admission plugin.
// Admission plugins should not hold a reference to the featureGates.  Instead, they should query a particular one
// and assign it to a simple bool in the admission plugin struct.
//
//	func (a *admissionPlugin) InspectFeatureGates(features featuregate.FeatureGate){
//	    a.myFeatureIsOn = features.Enabled("my-feature")
//	}
//
// 中文说明：特性开关在进程生命周期内不可变，缓存为 bool 可避免每次准入
// 请求都走一遍 map 查找，也防止插件意外持有全局状态。
type WantsFeatures interface {
	InspectFeatureGates(featuregate.FeatureGate)
	admission.InitializationValidator
}

// WantsEffectiveVersion defines a function which passes the effective version for inspection by an admission plugin.
//
// 中文说明：有效版本（effective version）反映 --emulated-version 等配置后的
// 实际行为版本，插件据此选择兼容旧版语义的分支。
type WantsEffectiveVersion interface {
	InspectEffectiveVersion(compatibility.EffectiveVersion)
	admission.InitializationValidator
}

// WantsDynamicClient 由需要以非类型化方式访问任意资源的插件实现。
//
// 中文说明：动态客户端不依赖编译期的 API 类型，适合处理 CRD 等运行时
// 才可知的资源；代价是失去类型安全，调用方需自行校验返回值。
type WantsDynamicClient interface {
	SetDynamicClient(dynamic.Interface)
	admission.InitializationValidator
}

// WantsRESTMapper defines a function which sets RESTMapper for admission plugins that need it.
//
// 中文说明：RESTMapper 负责 GVK 与 GVR/作用域之间的映射，是插件判断
// 资源是否命名空间级、是否已发现的前提。
type WantsRESTMapper interface {
	SetRESTMapper(meta.RESTMapper)
	admission.InitializationValidator
}

// WantsSchemaResolver defines a function which sets the SchemaResolver for
// an admission plugin that needs it.
//
// 中文说明：策略类插件在编译 CEL 表达式前需要目标对象的 OpenAPI schema，
// 以确定字段类型；resolver 由外部注入以便复用已缓存的发现数据。
type WantsSchemaResolver interface {
	SetSchemaResolver(resolver resolver.SchemaResolver)
	admission.InitializationValidator
}

// WantsExcludedAdmissionResources defines a function which sets the ExcludedAdmissionResources
// for an admission plugin that needs it.
//
// 中文说明：排除列表用于避免对 apiserver 自身内部资源（如事件、租约）
// 重复执行策略，减少自激与性能损耗。
type WantsExcludedAdmissionResources interface {
	SetExcludedAdmissionResources(excludedAdmissionResources []schema.GroupResource)
	admission.InitializationValidator
}

// WantsAPIServerID defines a function which sets the API server ID for admission plugins
// that need it (e.g., for metrics labeling in HA setups).
type WantsAPIServerID interface {
	SetAPIServerID(apiServerID string)
	admission.InitializationValidator
}

// ValidatingWebhookManifestLoadFunc loads ValidatingWebhookConfiguration manifests from a directory.
//
// 中文说明：返回内容哈希供调用方做变更检测；哈希为静态配置目录的内容摘要，
// 目录未变时可跳过重新编译/重建插件内部状态。
type ValidatingWebhookManifestLoadFunc func(dir string) ([]*admissionregistrationv1api.ValidatingWebhookConfiguration, string, error)

// MutatingWebhookManifestLoadFunc loads MutatingWebhookConfiguration manifests from a directory.
//
// 中文说明：与 Validating 版本对称，便于两个 webhook 插件共享同一加载约定。
type MutatingWebhookManifestLoadFunc func(dir string) ([]*admissionregistrationv1api.MutatingWebhookConfiguration, string, error)

// ValidatingPolicyManifestLoadFunc loads ValidatingAdmissionPolicy manifests from a directory.
//
// 中文说明：策略与绑定需一起返回，因为绑定通过名字引用策略，分开加载
// 容易产生短暂的不一致状态。
type ValidatingPolicyManifestLoadFunc func(dir string) ([]*admissionregistrationv1api.ValidatingAdmissionPolicy, []*admissionregistrationv1api.ValidatingAdmissionPolicyBinding, string, error)

// MutatingPolicyManifestLoadFunc loads MutatingAdmissionPolicy manifests from a directory.
//
// 中文说明：同样成对返回策略与绑定，保持引用完整性。
type MutatingPolicyManifestLoadFunc func(dir string) ([]*admissionregistrationv1api.MutatingAdmissionPolicy, []*admissionregistrationv1api.MutatingAdmissionPolicyBinding, string, error)

// ManifestLoaders provides functions to load admission configurations from static manifest files
// with scheme-based defaulting and validation.
//
// 中文说明：以函数字段而非接口暴露，便于测试替换，也避免插件直接依赖
// manifest 包的内部实现与文件系统。
type ManifestLoaders struct {
	// LoadValidatingWebhookManifests loads ValidatingWebhookConfiguration manifests.
	// 中文说明：为 nil 表示该插件未配置静态清单目录，调用方需判空。
	LoadValidatingWebhookManifests ValidatingWebhookManifestLoadFunc
	// LoadMutatingWebhookManifests loads MutatingWebhookConfiguration manifests.
	// 中文说明：同上，nil 值表示未启用。
	LoadMutatingWebhookManifests MutatingWebhookManifestLoadFunc
	// LoadValidatingPolicyManifests loads ValidatingAdmissionPolicy manifests.
	// 中文说明：nil 值表示未启用静态策略配置。
	LoadValidatingPolicyManifests ValidatingPolicyManifestLoadFunc
	// LoadMutatingPolicyManifests loads MutatingAdmissionPolicy manifests.
	// 中文说明：nil 值表示未启用静态变更策略配置。
	LoadMutatingPolicyManifests MutatingPolicyManifestLoadFunc
}

// WantsManifestLoaders is implemented by admission plugins that load configurations
// from static manifest files and need scheme-based defaulting and validation.
//
// 中文说明：此接口不嵌入 InitializationValidator，因为加载器只是可选能力，
// 与插件自身初始化校验无关。
type WantsManifestLoaders interface {
	SetManifestLoaders(loaders *ManifestLoaders)
}
