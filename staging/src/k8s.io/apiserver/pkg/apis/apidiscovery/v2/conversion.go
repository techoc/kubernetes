/*
Copyright 2024 The Kubernetes Authors.

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

// This file was duplicated from the auto-generated file by conversion-gen in
// k8s.io/kubernetes/pkg/apis/apidiscovery Unlike most k8s types discovery is
// served by all apiservers and conversion is needed by all apiservers. The
// concept of internal/hub type does not exist for discovery as we work directly
// with the versioned types.

// The conversion code here facilities conversion strictly between v2beta1 and
// v2 types. It is only necessary in k8s versions where mixed state could be
// possible before the full removal of the v2beta1 types. It is placed in this
// directory such that all apiservers can benefit from the conversion without
// having to implement their own if the client/server they're communicating with
// only supports one version.

// Once the v2beta1 types are removed (intended for Kubernetes v1.33), this file
// will be removed.

package v2

import (
	unsafe "unsafe"

	v2 "k8s.io/api/apidiscovery/v2"
	v2beta1 "k8s.io/api/apidiscovery/v2beta1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// RegisterConversions adds conversion functions to the given scheme.
// Public to allow building arbitrary schemes.
//
// 注册全部五个类型的双向转换；任一注册失败立即返回，避免留下半注册的 scheme。
// 聚合发现由每个 apiserver 各自提供，没有 internal/hub 类型，因此转换直接在
// 两个已版本化类型之间进行。本文件为手工复制的 conversion-gen 产物，修改
// 结构体时应通过 `make update` 重新生成，而不是手工调整函数体。
func RegisterConversions(s *runtime.Scheme) error {
	if err := s.AddGeneratedConversionFunc((*v2beta1.APIGroupDiscovery)(nil), (*v2.APIGroupDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2beta1APIGroupDiscoveryTov2APIGroupDiscovery(a.(*v2beta1.APIGroupDiscovery), b.(*v2.APIGroupDiscovery), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2.APIGroupDiscovery)(nil), (*v2beta1.APIGroupDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2APIGroupDiscoveryTov2beta1APIGroupDiscovery(a.(*v2.APIGroupDiscovery), b.(*v2beta1.APIGroupDiscovery), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2beta1.APIGroupDiscoveryList)(nil), (*v2.APIGroupDiscoveryList)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2beta1APIGroupDiscoveryListTov2APIGroupDiscoveryList(a.(*v2beta1.APIGroupDiscoveryList), b.(*v2.APIGroupDiscoveryList), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2.APIGroupDiscoveryList)(nil), (*v2beta1.APIGroupDiscoveryList)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2APIGroupDiscoveryListTov2beta1APIGroupDiscoveryList(a.(*v2.APIGroupDiscoveryList), b.(*v2beta1.APIGroupDiscoveryList), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2beta1.APIResourceDiscovery)(nil), (*v2.APIResourceDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2beta1APIResourceDiscoveryTov2APIResourceDiscovery(a.(*v2beta1.APIResourceDiscovery), b.(*v2.APIResourceDiscovery), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2.APIResourceDiscovery)(nil), (*v2beta1.APIResourceDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2APIResourceDiscoveryTov2beta1APIResourceDiscovery(a.(*v2.APIResourceDiscovery), b.(*v2beta1.APIResourceDiscovery), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2beta1.APISubresourceDiscovery)(nil), (*v2.APISubresourceDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2beta1APISubresourceDiscoveryTov2APISubresourceDiscovery(a.(*v2beta1.APISubresourceDiscovery), b.(*v2.APISubresourceDiscovery), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2.APISubresourceDiscovery)(nil), (*v2beta1.APISubresourceDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2APISubresourceDiscoveryTov2beta1APISubresourceDiscovery(a.(*v2.APISubresourceDiscovery), b.(*v2beta1.APISubresourceDiscovery), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2beta1.APIVersionDiscovery)(nil), (*v2.APIVersionDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2beta1APIVersionDiscoveryTov2APIVersionDiscovery(a.(*v2beta1.APIVersionDiscovery), b.(*v2.APIVersionDiscovery), scope)
	}); err != nil {
		return err
	}
	if err := s.AddGeneratedConversionFunc((*v2.APIVersionDiscovery)(nil), (*v2beta1.APIVersionDiscovery)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convertv2APIVersionDiscoveryTov2beta1APIVersionDiscovery(a.(*v2.APIVersionDiscovery), b.(*v2beta1.APIVersionDiscovery), scope)
	}); err != nil {
		return err
	}
	return nil
}

// autoConvertv2beta1APIGroupDiscoveryTov2APIGroupDiscovery 做浅拷贝转换。
//
// 使用 unsafe 指针转换是 conversion-gen 的惯例：v2beta1 与 v2 的字段布局
// 必须完全一致（除少数枚举类型外），否则会读到错误内存。修改任一版本的
// 结构体时，必须同步更新本文件（由 `make update` 重新生成）。
func autoConvertv2beta1APIGroupDiscoveryTov2APIGroupDiscovery(in *v2beta1.APIGroupDiscovery, out *v2.APIGroupDiscovery, s conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.Versions = *(*[]v2.APIVersionDiscovery)(unsafe.Pointer(&in.Versions))
	return nil
}

// Convertv2beta1APIGroupDiscoveryTov2APIGroupDiscovery is an autogenerated conversion function.
// v2beta1 -> v2：字段一一对应，仅做浅拷贝。
func Convertv2beta1APIGroupDiscoveryTov2APIGroupDiscovery(in *v2beta1.APIGroupDiscovery, out *v2.APIGroupDiscovery, s conversion.Scope) error {
	return autoConvertv2beta1APIGroupDiscoveryTov2APIGroupDiscovery(in, out, s)
}

func autoConvertv2APIGroupDiscoveryTov2beta1APIGroupDiscovery(in *v2.APIGroupDiscovery, out *v2beta1.APIGroupDiscovery, s conversion.Scope) error {
	out.ObjectMeta = in.ObjectMeta
	out.Versions = *(*[]v2beta1.APIVersionDiscovery)(unsafe.Pointer(&in.Versions))
	return nil
}

// Convertv2APIGroupDiscoveryTov2beta1APIGroupDiscovery is an autogenerated conversion function.
// v2 -> v2beta1：与反向转换对称，保持布局兼容。
func Convertv2APIGroupDiscoveryTov2beta1APIGroupDiscovery(in *v2.APIGroupDiscovery, out *v2beta1.APIGroupDiscovery, s conversion.Scope) error {
	return autoConvertv2APIGroupDiscoveryTov2beta1APIGroupDiscovery(in, out, s)
}

func autoConvertv2beta1APIGroupDiscoveryListTov2APIGroupDiscoveryList(in *v2beta1.APIGroupDiscoveryList, out *v2.APIGroupDiscoveryList, s conversion.Scope) error {
	out.ListMeta = in.ListMeta
	out.Items = *(*[]v2.APIGroupDiscovery)(unsafe.Pointer(&in.Items))
	return nil
}

// Convertv2beta1APIGroupDiscoveryListTov2APIGroupDiscoveryList is an autogenerated conversion function.
// 列表转换同样依赖 Items 切片布局一致。
func Convertv2beta1APIGroupDiscoveryListTov2APIGroupDiscoveryList(in *v2beta1.APIGroupDiscoveryList, out *v2.APIGroupDiscoveryList, s conversion.Scope) error {
	return autoConvertv2beta1APIGroupDiscoveryListTov2APIGroupDiscoveryList(in, out, s)
}

func autoConvertv2APIGroupDiscoveryListTov2beta1APIGroupDiscoveryList(in *v2.APIGroupDiscoveryList, out *v2beta1.APIGroupDiscoveryList, s conversion.Scope) error {
	out.ListMeta = in.ListMeta
	out.Items = *(*[]v2beta1.APIGroupDiscovery)(unsafe.Pointer(&in.Items))
	return nil
}

// Convertv2APIGroupDiscoveryListTov2beta1APIGroupDiscoveryList is an autogenerated conversion function.
// 列表反向转换。
func Convertv2APIGroupDiscoveryListTov2beta1APIGroupDiscoveryList(in *v2.APIGroupDiscoveryList, out *v2beta1.APIGroupDiscoveryList, s conversion.Scope) error {
	return autoConvertv2APIGroupDiscoveryListTov2beta1APIGroupDiscoveryList(in, out, s)
}

func autoConvertv2beta1APIResourceDiscoveryTov2APIResourceDiscovery(in *v2beta1.APIResourceDiscovery, out *v2.APIResourceDiscovery, s conversion.Scope) error {
	out.Resource = in.Resource
	out.ResponseKind = (*v1.GroupVersionKind)(unsafe.Pointer(in.ResponseKind))
	out.Scope = v2.ResourceScope(in.Scope)
	out.SingularResource = in.SingularResource
	out.Verbs = *(*[]string)(unsafe.Pointer(&in.Verbs))
	out.ShortNames = *(*[]string)(unsafe.Pointer(&in.ShortNames))
	out.Categories = *(*[]string)(unsafe.Pointer(&in.Categories))
	out.Subresources = *(*[]v2.APISubresourceDiscovery)(unsafe.Pointer(&in.Subresources))
	return nil
}

// Convertv2beta1APIResourceDiscoveryTov2APIResourceDiscovery is an autogenerated conversion function.
// 注意 Scope 是唯一需要显式类型转换的字段，其余为指针/切片浅拷贝。
func Convertv2beta1APIResourceDiscoveryTov2APIResourceDiscovery(in *v2beta1.APIResourceDiscovery, out *v2.APIResourceDiscovery, s conversion.Scope) error {
	return autoConvertv2beta1APIResourceDiscoveryTov2APIResourceDiscovery(in, out, s)
}

func autoConvertv2APIResourceDiscoveryTov2beta1APIResourceDiscovery(in *v2.APIResourceDiscovery, out *v2beta1.APIResourceDiscovery, s conversion.Scope) error {
	out.Resource = in.Resource
	out.ResponseKind = (*v1.GroupVersionKind)(unsafe.Pointer(in.ResponseKind))
	out.Scope = v2beta1.ResourceScope(in.Scope)
	out.SingularResource = in.SingularResource
	out.Verbs = *(*[]string)(unsafe.Pointer(&in.Verbs))
	out.ShortNames = *(*[]string)(unsafe.Pointer(&in.ShortNames))
	out.Categories = *(*[]string)(unsafe.Pointer(&in.Categories))
	out.Subresources = *(*[]v2beta1.APISubresourceDiscovery)(unsafe.Pointer(&in.Subresources))
	return nil
}

// Convertv2APIResourceDiscoveryTov2beta1APIResourceDiscovery is an autogenerated conversion function.
// Scope 枚举在 v2 与 v2beta1 中取值相同，转换是安全的。
func Convertv2APIResourceDiscoveryTov2beta1APIResourceDiscovery(in *v2.APIResourceDiscovery, out *v2beta1.APIResourceDiscovery, s conversion.Scope) error {
	return autoConvertv2APIResourceDiscoveryTov2beta1APIResourceDiscovery(in, out, s)
}

func autoConvertv2beta1APISubresourceDiscoveryTov2APISubresourceDiscovery(in *v2beta1.APISubresourceDiscovery, out *v2.APISubresourceDiscovery, s conversion.Scope) error {
	out.Subresource = in.Subresource
	out.ResponseKind = (*v1.GroupVersionKind)(unsafe.Pointer(in.ResponseKind))
	out.AcceptedTypes = *(*[]v1.GroupVersionKind)(unsafe.Pointer(&in.AcceptedTypes))
	out.Verbs = *(*[]string)(unsafe.Pointer(&in.Verbs))
	return nil
}

// Convertv2beta1APISubresourceDiscoveryTov2APISubresourceDiscovery is an autogenerated conversion function.
// 子资源转换：AcceptedTypes 与 Verbs 为切片浅拷贝。
func Convertv2beta1APISubresourceDiscoveryTov2APISubresourceDiscovery(in *v2beta1.APISubresourceDiscovery, out *v2.APISubresourceDiscovery, s conversion.Scope) error {
	return autoConvertv2beta1APISubresourceDiscoveryTov2APISubresourceDiscovery(in, out, s)
}

func autoConvertv2APISubresourceDiscoveryTov2beta1APISubresourceDiscovery(in *v2.APISubresourceDiscovery, out *v2beta1.APISubresourceDiscovery, s conversion.Scope) error {
	out.Subresource = in.Subresource
	out.ResponseKind = (*v1.GroupVersionKind)(unsafe.Pointer(in.ResponseKind))
	out.AcceptedTypes = *(*[]v1.GroupVersionKind)(unsafe.Pointer(&in.AcceptedTypes))
	out.Verbs = *(*[]string)(unsafe.Pointer(&in.Verbs))
	return nil
}

// Convertv2APISubresourceDiscoveryTov2beta1APISubresourceDiscovery is an autogenerated conversion function.
// 子资源反向转换。
func Convertv2APISubresourceDiscoveryTov2beta1APISubresourceDiscovery(in *v2.APISubresourceDiscovery, out *v2beta1.APISubresourceDiscovery, s conversion.Scope) error {
	return autoConvertv2APISubresourceDiscoveryTov2beta1APISubresourceDiscovery(in, out, s)
}

func autoConvertv2beta1APIVersionDiscoveryTov2APIVersionDiscovery(in *v2beta1.APIVersionDiscovery, out *v2.APIVersionDiscovery, s conversion.Scope) error {
	out.Version = in.Version
	out.Resources = *(*[]v2.APIResourceDiscovery)(unsafe.Pointer(&in.Resources))
	out.Freshness = v2.DiscoveryFreshness(in.Freshness)
	return nil
}

// Convertv2beta1APIVersionDiscoveryTov2APIVersionDiscovery is an autogenerated conversion function.
// Freshness 枚举显式转换，其余为浅拷贝。
func Convertv2beta1APIVersionDiscoveryTov2APIVersionDiscovery(in *v2beta1.APIVersionDiscovery, out *v2.APIVersionDiscovery, s conversion.Scope) error {
	return autoConvertv2beta1APIVersionDiscoveryTov2APIVersionDiscovery(in, out, s)
}

func autoConvertv2APIVersionDiscoveryTov2beta1APIVersionDiscovery(in *v2.APIVersionDiscovery, out *v2beta1.APIVersionDiscovery, s conversion.Scope) error {
	out.Version = in.Version
	out.Resources = *(*[]v2beta1.APIResourceDiscovery)(unsafe.Pointer(&in.Resources))
	out.Freshness = v2beta1.DiscoveryFreshness(in.Freshness)
	return nil
}

// Convertv2APIVersionDiscoveryTov2beta1APIVersionDiscovery is an autogenerated conversion function.
// 版本发现的反向转换。
func Convertv2APIVersionDiscoveryTov2beta1APIVersionDiscovery(in *v2.APIVersionDiscovery, out *v2beta1.APIVersionDiscovery, s conversion.Scope) error {
	return autoConvertv2APIVersionDiscoveryTov2beta1APIVersionDiscovery(in, out, s)
}
