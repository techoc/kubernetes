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

package extensionsv1beta1

import (
	extensionsapiv1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
// 直接复用 k8s.io/api 的结果，避免两处常量不同步。
const GroupName = extensionsapiv1beta1.GroupName

// SchemeGroupVersion 是注册这些对象时使用的组/版本。
// 版本字符串固定为 "v1beta1"，与包名保持一致。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource 接收不带组限定的资源名，返回带组限定的 GroupResource。
// 例如把 "deployments" 变成 "deployments.extensions"。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// 复用 k8s.io/api 对应包的 SchemeBuilder，使本包注册的转换函数
	// 与 API 类型落在同一个 scheme 构建器中。
	localSchemeBuilder = &extensionsapiv1beta1.SchemeBuilder
	// AddToScheme 将本包中的类型与转换函数注册到目标 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册在生成文件中完成。
	// 这种拆分保证生成文件缺失时包仍可编译。
	localSchemeBuilder.Register()
}
