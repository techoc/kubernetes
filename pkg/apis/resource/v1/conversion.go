/*
Copyright 2025 The Kubernetes Authors.

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

package v1

import (
	"fmt"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// addConversionFuncs 向 Scheme 注册 v1 版本的自定义转换函数。
// 这里注册的是字段标签（field label）转换，用于把客户端传入的选择器标签
// 翻译成服务端可识别的字段路径；未显式注册的标签会被拒绝，避免误用。
func addConversionFuncs(scheme *runtime.Scheme) error {
	// 为 ResourceSlice 注册字段标签转换：只有下列白名单标签可用于选择器。
	if err := scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("ResourceSlice"),
		func(label, value string) (string, string, error) {
			switch label {
			// 这些标签对应真实的资源字段，可以直接透传。
			case "metadata.name", resourceapi.ResourceSliceSelectorNodeName, resourceapi.ResourceSliceSelectorDriver, resourceapi.ResourceSliceSelectorPoolName:
				return label, value, nil
			default:
				// 未知标签直接报错，防止查询静默返回错误的结果集。
				return "", "", fmt.Errorf("field label not supported for %s: %s", SchemeGroupVersion.WithKind("ResourceSlice"), label)
			}
		}); err != nil {
		return err
	}

	return nil
}
