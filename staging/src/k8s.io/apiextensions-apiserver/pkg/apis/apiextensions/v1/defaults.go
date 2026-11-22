/*
Copyright 2019 The Kubernetes Authors.

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

// 本文件定义 CRD v1 版本的默认值设置逻辑。
// 在 CRD 创建或更新时，API Server 会自动填充缺失的字段，
// 减少用户需要手动指定的配置项。
package v1

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 注册所有默认值函数到 Scheme 中。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_CustomResourceDefinition 设置 CRD 的默认值。
// 包括 Spec 默认值和 StoredVersions 初始化（新 CRD 的 storedVersions 应包含存储版本）。
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

// SetDefaults_CustomResourceDefinitionSpec 设置 CRD Spec 的默认值：
// - Singular 名称默认为小写的 Kind
// - ListKind 默认为 Kind + "List"
// - Conversion 策略默认为 None（使用 API Server 内置转换）
func SetDefaults_CustomResourceDefinitionSpec(obj *CustomResourceDefinitionSpec) {
	if len(obj.Names.Singular) == 0 {
		obj.Names.Singular = strings.ToLower(obj.Names.Kind)
	}
	if len(obj.Names.ListKind) == 0 && len(obj.Names.Kind) > 0 {
		obj.Names.ListKind = obj.Names.Kind + "List"
	}
	if obj.Conversion == nil {
		obj.Conversion = &CustomResourceConversion{
			Strategy: NoneConverter,
		}
	}
}

// SetDefaults_ServiceReference 设置 Webhook ServiceReference 的默认值。
// Webhook 默认使用 443 端口（HTTPS），符合安全最佳实践。
func SetDefaults_ServiceReference(obj *ServiceReference) {
	if obj.Port == nil {
		// Webhook 必须使用 HTTPS，默认端口 443
		obj.Port = ptr.To[int32](443)
	}
}
