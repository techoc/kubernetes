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

// Attributes 抽象 PodSecurity 准入控制器消费的请求参数。
// 只暴露控制器真正需要的字段，避免与具体的准入请求类型强耦合，也方便测试时直接构造。
type Attributes interface {
	// GetName 返回请求关联的对象名称。
	GetName() string
	// GetNamespace 返回请求关联的命名空间；没有命名空间时返回空字符串。
	GetNamespace() string
	// GetResource 返回请求的资源名，而不是 Kind；例如 Pod 的资源名是 pods。
	GetResource() schema.GroupVersionResource
	// GetKind 返回请求的 Kind；例如 Pod。
	GetKind() schema.GroupVersionKind
	// GetSubresource 返回子资源名；它决定是否需要对请求体执行 Pod 安全检查，不能与 Kind 混为一谈。
	GetSubresource() string
	// GetOperation 返回请求执行的操作。
	GetOperation() admissionv1.Operation

	// GetObject 解码并返回请求中的对象；core API group 中的对象必须使用 v1 API，以便复用同一套检查逻辑。
	GetObject() (runtime.Object, error)
	// GetOldObject 返回更新前已存在的对象，仅 UPDATE 请求会填充；core API group 中必须使用 v1 API。
	// 创建请求没有旧对象，更新检查依赖它判断字段是否变化。
	GetOldObject() (runtime.Object, error)
	// GetUserName 返回请求方已认证的用户名；豁免按该身份匹配，不能采用请求中未经认证的自称值。
	GetUserName() string
}

// AttributesRecord 是 Attributes 的简单实现，字段直接对应接口方法。
// 它跳过解码步骤，主要供测试和非 admission.Request 场景使用。
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

// GetName 返回记录中的对象名称。
func (a *AttributesRecord) GetName() string {
	return a.Name
}

// GetNamespace 返回记录中的命名空间。
func (a *AttributesRecord) GetNamespace() string {
	return a.Namespace
}

// GetKind 返回记录中的 Kind。
func (a *AttributesRecord) GetKind() schema.GroupVersionKind {
	return a.Kind
}

// GetResource 返回记录中的资源名。
func (a *AttributesRecord) GetResource() schema.GroupVersionResource {
	return a.Resource
}

// GetSubresource 返回记录中的子资源名。
func (a *AttributesRecord) GetSubresource() string {
	return a.Subresource
}

// GetOperation 返回记录中的操作。
func (a *AttributesRecord) GetOperation() admissionv1.Operation {
	return a.Operation
}

// GetUserName 返回记录中的用户名。
func (a *AttributesRecord) GetUserName() string {
	return a.Username
}

// GetObject 直接返回记录中的对象，不执行解码。
func (a *AttributesRecord) GetObject() (runtime.Object, error) {
	return a.Object, nil
}

// GetOldObject 直接返回记录中的旧对象，不执行解码。
func (a *AttributesRecord) GetOldObject() (runtime.Object, error) {
	return a.OldObject, nil
}

// 编译期断言，防止接口新增方法时漏改实现。
var _ Attributes = &AttributesRecord{}

// RequestAttributes 把 admission.Request 适配为 Attributes。
// 解码器由调用方传入，便于用不同的 scheme 或测试桩替换默认行为。
func RequestAttributes(request *admissionv1.AdmissionRequest, decoder runtime.Decoder) Attributes {
	return &attributes{
		r:       request,
		decoder: decoder,
	}
}

// attributes 包装原始准入请求，供 AdmissionController 提取决策所需信息。
// 对象只在被真正读取时才解码。
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
