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

package api

import (
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Attributes exposes the admission request parameters consumed by the PodSecurity admission controller.
// 只暴露控制器真正需要的字段，避免与具体的准入请求类型强耦合，也方便测试时直接构造。
type Attributes interface {
	// GetName is the name of the object associated with the request.
	GetName() string
	// GetNamespace is the namespace associated with the request (if any)
	GetNamespace() string
	// GetResource is the name of the resource being requested.  This is not the kind.  For example: pods
	GetResource() schema.GroupVersionResource
	// GetKind is the name of the kind being requested.  For example: Pod
	GetKind() schema.GroupVersionKind
	// GetSubresource is the name of the subresource being requested.  This is a different resource, scoped to the parent resource, but it may have a different kind.
	// For instance, /pods has the resource "pods" and the kind "Pod", while /pods/foo/status has the resource "pods", the sub resource "status", and the kind "Pod"
	// (because status operates on pods). The binding resource for a pod though may be /pods/foo/binding, which has resource "pods", subresource "binding", and kind "Binding".
	// 子资源名决定是否要对请求体做 Pod 安全检查，必须和 kind 分开看。
	GetSubresource() string
	// GetOperation is the operation being performed
	GetOperation() admissionv1.Operation

	// GetObject returns the typed Object from incoming request.
	// For objects in the core API group, the result must use the v1 API.
	// 统一到 v1 才能用同一套检查逻辑，不必为每个 API 版本各写一份。
	GetObject() (runtime.Object, error)
	// GetOldObject returns the typed existing object. Only populated for UPDATE requests.
	// For objects in the core API group, the result must use the v1 API.
	// 更新场景要靠旧对象判断字段是否被改动，创建请求里它为空。
	GetOldObject() (runtime.Object, error)
	// GetUserName is the requesting user's authenticated name.
	// 豁免名单按用户名匹配，因此这里必须是认证后的身份而非请求里自称的名字。
	GetUserName() string
}

// AttributesRecord is a simple struct implementing the Attributes interface.
// 字段直接对应接口方法，省去解码步骤，主要给测试和非 admission.Request 场景使用。
type AttributesRecord struct {
	Name        string
	Namespace   string
	Kind        schema.GroupVersionKind
	Resource    schema.GroupVersionResource
	Subresource string
	Operation   admissionv1.Operation
	Object      runtime.Object
	OldObject   runtime.Object
	Username    string
}

func (a *AttributesRecord) GetName() string {
	return a.Name
}
func (a *AttributesRecord) GetNamespace() string {
	return a.Namespace
}
func (a *AttributesRecord) GetKind() schema.GroupVersionKind {
	return a.Kind
}
func (a *AttributesRecord) GetResource() schema.GroupVersionResource {
	return a.Resource
}
func (a *AttributesRecord) GetSubresource() string {
	return a.Subresource
}
func (a *AttributesRecord) GetOperation() admissionv1.Operation {
	return a.Operation
}
func (a *AttributesRecord) GetUserName() string {
	return a.Username
}
func (a *AttributesRecord) GetObject() (runtime.Object, error) {
	return a.Object, nil
}
func (a *AttributesRecord) GetOldObject() (runtime.Object, error) {
	return a.OldObject, nil
}

// 编译期断言，防止接口新增方法时漏改实现。
var _ Attributes = &AttributesRecord{}

// RequestAttributes adapts an admission.Request to the Attributes interface.
// 解码器由调用方传入，便于用不同的 scheme 或测试桩替换默认行为。
func RequestAttributes(request *admissionv1.AdmissionRequest, decoder runtime.Decoder) Attributes {
	return &attributes{
		r:       request,
		decoder: decoder,
	}
}

// attributes is an interface used by AdmissionController to get information about a request
// that is used to make an admission decision.
// 包装原始请求并缓存解码器，对象只在被真正读取时才解码。
type attributes struct {
	r       *admissionv1.AdmissionRequest
	decoder runtime.Decoder
}

func (a *attributes) GetName() string {
	return a.r.Name
}
func (a *attributes) GetNamespace() string {
	return a.r.Namespace
}
func (a *attributes) GetKind() schema.GroupVersionKind {
	return schema.GroupVersionKind(a.r.Kind)
}
func (a *attributes) GetResource() schema.GroupVersionResource {
	return schema.GroupVersionResource(a.r.Resource)
}
func (a *attributes) GetSubresource() string {
	return a.r.RequestSubResource
}
func (a *attributes) GetOperation() admissionv1.Operation {
	return a.r.Operation
}
func (a *attributes) GetUserName() string {
	return a.r.UserInfo.Username
}
func (a *attributes) GetObject() (runtime.Object, error) {
	return a.decode(a.r.Object)
}
func (a *attributes) GetOldObject() (runtime.Object, error) {
	return a.decode(a.r.OldObject)
}

// decode 把请求里的原始字节按其 Kind 解成具体对象。
func (a *attributes) decode(in runtime.RawExtension) (runtime.Object, error) {
	// 创建请求没有旧对象，nil 表示“无”而不是解码失败。
	if in.Raw == nil {
		return nil, nil
	}
	gvk := schema.GroupVersionKind(a.r.Kind)
	out, _, err := a.decoder.Decode(in.Raw, &gvk, nil)
	return out, err
}

// 编译期断言，防止接口新增方法时漏改实现。
var _ Attributes = &attributes{}
