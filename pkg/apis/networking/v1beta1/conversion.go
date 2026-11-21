/*
Copyright 2020 The Kubernetes Authors.

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

package v1beta1

import (
	v1beta1 "k8s.io/api/networking/v1beta1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/util/intstr"
	networking "k8s.io/kubernetes/pkg/apis/networking"
)

// Convert_v1beta1_IngressBackend_To_networking_IngressBackend 将 v1beta1 的扁平后端字段转换为内部版本的嵌套 Service 结构。
// 只有服务相关字段确实被设置时才构造 Service，避免把空后端转换成非空的无效对象。
func Convert_v1beta1_IngressBackend_To_networking_IngressBackend(in *v1beta1.IngressBackend, out *networking.IngressBackend, s conversion.Scope) error {
	if err := autoConvert_v1beta1_IngressBackend_To_networking_IngressBackend(in, out, s); err != nil {
		return err
	}
	// 字符串类型的端口即使值为空也代表用户显式指定，需要保留该意图。
	if len(in.ServiceName) > 0 || in.ServicePort.IntVal != 0 || in.ServicePort.StrVal != "" || in.ServicePort.Type == intstr.String {
		out.Service = &networking.IngressServiceBackend{}
		out.Service.Name = in.ServiceName
		out.Service.Port.Name = in.ServicePort.StrVal
		out.Service.Port.Number = in.ServicePort.IntVal
	}
	return nil
}

// Convert_networking_IngressBackend_To_v1beta1_IngressBackend 是后端字段从内部版本到 v1beta1 的反向转换。
// v1beta1 用独立的 ServiceName 与 ServicePort 字段表达，因此需要把嵌套结构摊平。
func Convert_networking_IngressBackend_To_v1beta1_IngressBackend(in *networking.IngressBackend, out *v1beta1.IngressBackend, s conversion.Scope) error {
	if err := autoConvert_networking_IngressBackend_To_v1beta1_IngressBackend(in, out, s); err != nil {
		return err
	}
	if in.Service != nil {
		out.ServiceName = in.Service.Name
		// 端口名非空时按字符串端口表达，否则使用数字端口，保持与旧版字段语义一致。
		if len(in.Service.Port.Name) > 0 {
			out.ServicePort = intstr.FromString(in.Service.Port.Name)
		} else {
			out.ServicePort = intstr.FromInt32(in.Service.Port.Number)
		}
	}
	return nil
}

// Convert_v1beta1_IngressSpec_To_networking_IngressSpec 将 v1beta1 的 IngressSpec 转换为内部版本。
// 旧版的 Backend 就是新版语义中的 DefaultBackend，需走字段转换函数完成结构映射。
func Convert_v1beta1_IngressSpec_To_networking_IngressSpec(in *v1beta1.IngressSpec, out *networking.IngressSpec, s conversion.Scope) error {
	if err := autoConvert_v1beta1_IngressSpec_To_networking_IngressSpec(in, out, s); err != nil {
		return err
	}
	if in.Backend != nil {
		out.DefaultBackend = &networking.IngressBackend{}
		if err := Convert_v1beta1_IngressBackend_To_networking_IngressBackend(in.Backend, out.DefaultBackend, s); err != nil {
			return err
		}
	}
	return nil
}

// Convert_networking_IngressSpec_To_v1beta1_IngressSpec 是 IngressSpec 从内部版本回退到 v1beta1 的转换。
func Convert_networking_IngressSpec_To_v1beta1_IngressSpec(in *networking.IngressSpec, out *v1beta1.IngressSpec, s conversion.Scope) error {
	if err := autoConvert_networking_IngressSpec_To_v1beta1_IngressSpec(in, out, s); err != nil {
		return err
	}
	if in.DefaultBackend != nil {
		out.Backend = &v1beta1.IngressBackend{}
		if err := Convert_networking_IngressBackend_To_v1beta1_IngressBackend(in.DefaultBackend, out.Backend, s); err != nil {
			return err
		}
	}
	return nil
}
