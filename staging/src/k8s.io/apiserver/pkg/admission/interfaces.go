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

package admission

import (
	"context"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// Attributes 是准入控制器用于获取请求信息、进而做出准入决策的接口，
// 是一次准入请求的只读视图：插件只能通过它观察请求，任何对对象的修改都必须经由
// 返回 patch/新对象完成，从而让准入链能比较修改前后差异并写入审计。实现返回的对象可能被
// 多个插件共享，插件不得原地修改它们。
type Attributes interface {
	// GetName 返回请求中出现的对象名。CREATE 操作时客户端可能省略名字、
	// 依赖服务端生成，此时返回空字符串。
	GetName() string
	// GetNamespace 返回请求关联的命名空间（如有）。
	GetNamespace() string
	// GetResource 返回被请求的资源名，不是 kind，例如 pods。
	GetResource() schema.GroupVersionResource
	// GetSubresource 返回被请求的子资源名。子资源依附于父资源，但 kind 可能不同：
	// /pods 的资源是 pods、kind 是 Pod；/pods/foo/status 的资源仍是 pods、子资源是
	// status、kind 是 Pod（status 作用于 pod）；而 /pods/foo/binding 的资源是 pods、
	GetSubresource() string
	// GetOperation 返回正在执行的操作。
	GetOperation() Operation
	// GetOperationOptions 返回该操作附带的选项对象。
	GetOperationOptions() runtime.Object
	// IsDryRun 表示本请求的修改一定不会持久化，用于避免有副作用且有
	// 调和机制的准入控制器被试运行请求压垮。但返回 false 不代表修改
	// 一定会被持久化，后续校验步骤仍可能拒绝该请求。
	IsDryRun() bool
	// GetObject 返回请求中默认值填充之前的对象。
	// 返回的是反序列化后、默认值填充前的对象，因此插件看到的是客户端原始意图。
	GetObject() runtime.Object
	// GetOldObject 返回已存在的旧对象，仅 UPDATE 和 DELETE 请求有值。
	GetOldObject() runtime.Object
	// GetKind 返回被操作对象的类型，例如 Pod。
	GetKind() schema.GroupVersionKind
	// GetUserInfo 返回发起请求的用户信息。
	GetUserInfo() user.Info

	// AddAnnotation 按键值对设置注解。键必须是限定名，例如
	// podsecuritypolicy.admission.k8s.io/admit-policy："podsecuritypolicy" 是
	// 插件名，"admission.k8s.io" 是组织名，"admit-policy" 是键名。键格式非法
	// 或用新值覆盖已有注解时返回错误。ValidationInterface 与 MutationInterface
	// 都可以添加注解。默认情况下，请求审计级别不低于 Metadata 时注解会记入审计事件。
	// 禁止覆盖已有键是为了避免插件之间静默互相覆盖；键必须带域名前缀，防止不同插件冲突。
	AddAnnotation(key, value string) error

	// AddAnnotationWithLevel 按键值对设置注解，并指定预期的审计级别；
	// 当请求的审计级别不低于该预期级别时，注解会记入审计事件。
	AddAnnotationWithLevel(key, value string, level auditinternal.Level) error

	// GetReinvocationContext 返回与重调用策略相关的准入请求信息。
	// 重调用上下文在整条准入链中共享，用于判断某插件在对象被其他插件修改后是否需要再次执行。
	GetReinvocationContext() ReinvocationContext
}

// ObjectInterfaces 是准入控制器用于获取 Converter、Defaulter 等对象接口的接口。
// 这些接口来自请求作用域（Request Scope）而非全局注册表，因此对 CRD 等动态类型同样适用：
// 插件不能假设自己拿到的是编译期已知的类型。
type ObjectInterfaces interface {
	// GetObjectCreater 返回适用于被请求对象的 ObjectCreator。
	GetObjectCreater() runtime.ObjectCreater
	// GetObjectTyper 返回适用于被请求对象的 ObjectTyper。
	GetObjectTyper() runtime.ObjectTyper
	// GetObjectDefaulter 返回适用于被请求对象的 ObjectDefaulter。
	GetObjectDefaulter() runtime.ObjectDefaulter
	// GetObjectConvertor 返回适用于被请求对象的 ObjectConvertor。
	GetObjectConvertor() runtime.ObjectConvertor
	// GetEquivalentResourceMapper 返回用于查找被请求对象等价资源与预期 kind 的 EquivalentResourceMapper。
	GetEquivalentResourceMapper() runtime.EquivalentResourceMapper
}

// privateAnnotationsGetter 是允许从 Attributes 读取注解的私有接口。
// 私有接口用于在不扩大公开 API 的前提下，让同包代码读取注解；外部实现应改用 AnnotationsGetter。
type privateAnnotationsGetter interface {
	getAnnotations(maxLevel auditinternal.Level) map[string]string
}

// AnnotationsGetter 允许从 Attributes 读取注解；自定义的 Attributes 实现应实现本接口。
type AnnotationsGetter interface {
	GetAnnotations(maxLevel auditinternal.Level) map[string]string
}

// ReinvocationContext 提供实现重调用策略所需的准入状态访问。
type ReinvocationContext interface {
	// IsReinvoke 当前一次准入检查是否为重调用。
	IsReinvoke() bool
	// SetIsReinvoke 将当前准入检查标记为重调用。
	SetIsReinvoke()
	// ShouldReinvoke 有插件请求过重调用时返回 true。
	ShouldReinvoke() bool
	// SetShouldReinvoke 发出需要重调用的信号。
	SetShouldReinvoke()
	// SetValue 为指定插件名设置值，可能覆盖之前的值。
	SetValue(plugin string, v interface{})
	// Value 读取指定插件保存的值。
	Value(plugin string) interface{}
}

// Interface 是准入控制决策的抽象可插拔接口。
type Interface interface {
	// Handles 当本准入控制器能处理给定操作（CREATE、UPDATE、DELETE 或 CONNECT）时返回 true。
	Handles(operation Operation) bool
}

// MutationInterface 是允许修改请求对象的准入插件接口。
type MutationInterface interface {
	Interface

	// Admit 基于请求属性做出准入决策。
	// ctx 仅用于超时/截止时间/取消以及追踪信息。
	Admit(ctx context.Context, a Attributes, o ObjectInterfaces) (err error)
}

// ValidationInterface 是准入控制决策的抽象可插拔接口（仅校验）。
// 校验插件必须保持无副作用：它们可能在同一请求中被重调用，且不得修改对象。
type ValidationInterface interface {
	Interface

	// Validate 基于请求属性做出准入决策，不允许修改对象。
	// ctx 仅用于超时/截止时间/取消以及追踪信息。
	Validate(ctx context.Context, a Attributes, o ObjectInterfaces) (err error)
}

// Operation 表示准入控制正在检查的资源操作类型。
// 该类型取自 authorizer 包，以避免导入循环。
type Operation = authorizer.AdmissionOperation

// 操作类型常量。
const (
	Create  Operation = "CREATE"
	Update  Operation = "UPDATE"
	Delete  Operation = "DELETE"
	Connect Operation = "CONNECT"
)

// PluginInitializer 用于初始化准入插件之间共享的资源；初始化完成后资源需分别设置。
// 所有插件共享同一个初始化器实例，因此 Initialize 必须并发安全，且不应持有 plugin 引用
// 之外的长期状态。
type PluginInitializer interface {
	Initialize(plugin Interface)
}

// InitializationValidator 提供 ValidateInitialization 方法，负责校验已初始化的共享资源，应由准入插件实现。
type InitializationValidator interface {
	ValidateInitialization() error
}

// ConfigProvider 提供按插件名获取准入插件配置的方式。
type ConfigProvider interface {
	ConfigFor(pluginName string) (io.Reader, error)
}
