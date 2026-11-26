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

package autoscalingv1

import (
	autoscalingapiv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API group 名。
const GroupName = autoscalingapiv1.GroupName

// SchemeGroupVersion 是注册这些对象所用的 group version。
//
// 版本固定为 v1：scale 子资源在客户端侧始终以 autoscaling/v1 表示，
// 与服务器的 scale 接口约定保持一致。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource 把裸资源名补全为带 group 的 GroupResource。
//
// 用于把 "deployments" 这类裸资源名补全为 "deployments.autoscaling"，
// 以匹配 scale 子资源请求中的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// 复用 autoscaling/v1 的 SchemeBuilder，使转换函数与 API 类型注册落在同一处。
	localSchemeBuilder = &autoscalingapiv1.SchemeBuilder
	// AddToScheme 把 autoscaling/v1 类型与转换函数注册到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写函数，生成函数的注册放在生成文件中。两者分离，使生成文件缺失时仍可编译。
	//
	// 这里传入的转换函数（若有）需手写；conversion-gen 生成的版本由
	// zz_generated.conversion.go 自行注册，二者分离保证缺少生成文件时仍可编译。
	localSchemeBuilder.Register()
}
