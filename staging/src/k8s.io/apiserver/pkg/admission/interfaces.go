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

// Attributes is an interface used by AdmissionController to get information about a request
// that is used to make an admission decision.
//
// Attributes 是一次准入请求的只读视图：插件只能通过它观察请求，任何对对象的修改都必须经由
// 返回 patch/新对象完成，从而让准入链能比较修改前后差异并写入审计。实现返回的对象可能被
// 多个插件共享，插件不得原地修改它们。
type Attributes interface {
	// GetName returns the name of the object as presented in the request.  On a CREATE operation, the client
	// may omit name and rely on the server to generate the name.  If that is the case, this method will return
	// the empty string
	//
	// 注意：这里返回的是请求中出现的名字，而不是默认值或服务端生成的名字。
	GetName() string
	// GetNamespace is the namespace associated with the request (if any)
	GetNamespace() string
	// GetResource is the name of the resource being requested.  This is not the kind.  For example: pods
	GetResource() schema.GroupVersionResource
	// GetSubresource is the name of the subresource being requested.  This is a different resource, scoped to the parent resource, but it may have a different kind.
	// For instance, /pods has the resource "pods" and the kind "Pod", while /pods/foo/status has the resource "pods", the sub resource "status", and the kind "Pod"
	// (because status operates on pods). The binding resource for a pod though may be /pods/foo/binding, which has resource "pods", subresource "binding", and kind "Binding".
	GetSubresource() string
	// GetOperation is the operation being performed
	GetOperation() Operation
	// GetOperationOptions is the options for the operation being performed
	GetOperationOptions() runtime.Object
	// IsDryRun indicates that modifications will definitely not be persisted for this request. This is to prevent
	// admission controllers with side effects and a method of reconciliation from being overwhelmed.
	// However, a value of false for this does not mean that the modification will be persisted, because it
	// could still be rejected by a subsequent validation step.
	IsDryRun() bool
	// GetObject is the object from the incoming request prior to default values being applied
	//
	// 返回的是反序列化后、默认值填充前的对象，因此插件看到的是客户端原始意图。
	GetObject() runtime.Object
	// GetOldObject is the existing object. Only populated for UPDATE and DELETE requests.
	GetOldObject() runtime.Object
	// GetKind is the type of object being manipulated.  For example: Pod
	GetKind() schema.GroupVersionKind
	// GetUserInfo is information about the requesting user
	GetUserInfo() user.Info

	// AddAnnotation sets annotation according to key-value pair. The key should be qualified, e.g., podsecuritypolicy.admission.k8s.io/admit-policy, where
	// "podsecuritypolicy" is the name of the plugin, "admission.k8s.io" is the name of the organization, "admit-policy" is the key name.
	// An error is returned if the format of key is invalid. When trying to overwrite annotation with a new value, an error is returned.
	// Both ValidationInterface and MutationInterface are allowed to add Annotations.
	// By default, an annotation gets logged into audit event if the request's audit level is greater or
	// equal to Metadata.
	//
	// 禁止覆盖已有键是为了避免插件之间静默互相覆盖；键必须带域名前缀，防止不同插件冲突。
	AddAnnotation(key, value string) error

	// AddAnnotationWithLevel sets annotation according to key-value pair with additional intended audit level.
	// An Annotation gets logged into audit event if the request's audit level is greater or equal to the
	// intended audit level.
	AddAnnotationWithLevel(key, value string, level auditinternal.Level) error

	// GetReinvocationContext tracks the admission request information relevant to the re-invocation policy.
	//
	// 重调用上下文在整条准入链中共享，用于判断某插件在对象被其他插件修改后是否需要再次执行。
	GetReinvocationContext() ReinvocationContext
}

// ObjectInterfaces is an interface used by AdmissionController to get object interfaces
// such as Converter or Defaulter. These interfaces are normally coming from Request Scope
// to handle special cases like CRDs.
//
// 这些接口来自请求作用域（Request Scope）而非全局注册表，因此对 CRD 等动态类型同样适用：
// 插件不能假设自己拿到的是编译期已知的类型。
type ObjectInterfaces interface {
	// GetObjectCreater is the ObjectCreator appropriate for the requested object.
	GetObjectCreater() runtime.ObjectCreater
	// GetObjectTyper is the ObjectTyper appropriate for the requested object.
	GetObjectTyper() runtime.ObjectTyper
	// GetObjectDefaulter is the ObjectDefaulter appropriate for the requested object.
	GetObjectDefaulter() runtime.ObjectDefaulter
	// GetObjectConvertor is the ObjectConvertor appropriate for the requested object.
	GetObjectConvertor() runtime.ObjectConvertor
	// GetEquivalentResourceMapper is the EquivalentResourceMapper appropriate for finding equivalent resources and expected kind for the requested object.
	GetEquivalentResourceMapper() runtime.EquivalentResourceMapper
}

// privateAnnotationsGetter is a private interface which allows users to get annotations from Attributes.
//
// 私有接口用于在不扩大公开 API 的前提下，让同包代码读取注解；外部实现应改用 AnnotationsGetter。
type privateAnnotationsGetter interface {
	getAnnotations(maxLevel auditinternal.Level) map[string]string
}

// AnnotationsGetter allows users to get annotations from Attributes. An alternate Attribute should implement
// this interface.
type AnnotationsGetter interface {
	GetAnnotations(maxLevel auditinternal.Level) map[string]string
}

// ReinvocationContext provides access to the admission related state required to implement the re-invocation policy.
type ReinvocationContext interface {
	// IsReinvoke returns true if the current admission check is a re-invocation.
	IsReinvoke() bool
	// SetIsReinvoke sets the current admission check as a re-invocation.
	SetIsReinvoke()
	// ShouldReinvoke returns true if any plugin has requested a re-invocation.
	ShouldReinvoke() bool
	// SetShouldReinvoke signals that a re-invocation is desired.
	SetShouldReinvoke()
	// AddValue set a value for a plugin name, possibly overriding a previous value.
	SetValue(plugin string, v interface{})
	// Value reads a value for a webhook.
	Value(plugin string) interface{}
}

// Interface is an abstract, pluggable interface for Admission Control decisions.
type Interface interface {
	// Handles returns true if this admission controller can handle the given operation
	// where operation can be one of CREATE, UPDATE, DELETE, or CONNECT
	Handles(operation Operation) bool
}

// MutationInterface 是允许修改请求对象的准入插件接口。
type MutationInterface interface {
	Interface

	// Admit makes an admission decision based on the request attributes.
	// Context is used only for timeout/deadline/cancellation and tracing information.
	Admit(ctx context.Context, a Attributes, o ObjectInterfaces) (err error)
}

// ValidationInterface is an abstract, pluggable interface for Admission Control decisions.
//
// 校验插件必须保持无副作用：它们可能在同一请求中被重调用，且不得修改对象。
type ValidationInterface interface {
	Interface

	// Validate makes an admission decision based on the request attributes.  It is NOT allowed to mutate
	// Context is used only for timeout/deadline/cancellation and tracing information.
	Validate(ctx context.Context, a Attributes, o ObjectInterfaces) (err error)
}

// Operation is the type of resource operation being checked for admission control
// This type is sourced from the authorizer package to avoid import cycles.
type Operation = authorizer.AdmissionOperation

// Operation constants
const (
	Create  Operation = "CREATE"
	Update  Operation = "UPDATE"
	Delete  Operation = "DELETE"
	Connect Operation = "CONNECT"
)

// PluginInitializer is used for initialization of shareable resources between admission plugins.
// After initialization the resources have to be set separately
//
// 所有插件共享同一个初始化器实例，因此 Initialize 必须并发安全，且不应持有 plugin 引用
// 之外的长期状态。
type PluginInitializer interface {
	Initialize(plugin Interface)
}

// InitializationValidator holds ValidateInitialization functions, which are responsible for validation of initialized
// shared resources and should be implemented on admission plugins
type InitializationValidator interface {
	ValidateInitialization() error
}

// ConfigProvider provides a way to get configuration for an admission plugin based on its name
type ConfigProvider interface {
	ConfigFor(pluginName string) (io.Reader, error)
}
