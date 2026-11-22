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

package v1beta1

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_CustomResourceDefinition 为 CustomResourceDefinition 设置默认值。
func SetDefaults_CustomResourceDefinition(obj *CustomResourceDefinition) {
	SetDefaults_CustomResourceDefinitionSpec(&obj.Spec)
	if len(obj.Status.StoredVersions) == 0 {
		for _, v := range obj.Spec.Versions {
			if v.Storage {
				obj.Status.StoredVersions = append(obj.Status.StoredVersions, v.Name)
				break
			}
		}
	}
}

// SetDefaults_CustomResourceDefinitionSpec 为 CustomResourceDefinitionSpec 设置默认值。
func SetDefaults_CustomResourceDefinitionSpec(obj *CustomResourceDefinitionSpec) {
	if len(obj.Scope) == 0 {
		obj.Scope = NamespaceScoped
	}
	if len(obj.Names.Singular) == 0 {
		obj.Names.Singular = strings.ToLower(obj.Names.Kind)
	}
	if len(obj.Names.ListKind) == 0 && len(obj.Names.Kind) > 0 {
		obj.Names.ListKind = obj.Names.Kind + "List"
	}
	// 若没有 versions 列表，则使用已废弃的 Version 字段创建一个。
	if len(obj.Versions) == 0 && len(obj.Version) != 0 {
		obj.Versions = []CustomResourceDefinitionVersion{{
			Name:    obj.Version,
			Storage: true,
			Served:  true,
		}}
	}
	// 为向后兼容，将 version 字段设为 versions 列表的第一项。
	if len(obj.Version) == 0 && len(obj.Versions) != 0 {
		obj.Version = obj.Versions[0].Name
	}
	if obj.Conversion == nil {
		obj.Conversion = &CustomResourceConversion{
			Strategy: NoneConverter,
		}
	}
	if obj.Conversion.Strategy == WebhookConverter && len(obj.Conversion.ConversionReviewVersions) == 0 {
		obj.Conversion.ConversionReviewVersions = []string{SchemeGroupVersion.Version}
	}
	if obj.PreserveUnknownFields == nil {
		obj.PreserveUnknownFields = ptr.To(true)
	}
}

// SetDefaults_ServiceReference 为 Webhook 的 ServiceReference 设置默认值。
func SetDefaults_ServiceReference(obj *ServiceReference) {
	if obj.Port == nil {
		obj.Port = ptr.To[int32](443)
	}
}
