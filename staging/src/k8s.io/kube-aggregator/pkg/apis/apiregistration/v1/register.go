/*
Copyright 2016 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是 apiregistration 的 API 组名。
const GroupName = "apiregistration.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource 把非限定资源名转为带组名的 GroupResource。
//
// 中文补充：调用方通常只写 "apiservices"，但存储层要按组限定，
// 用 SchemeGroupVersion 包一层可以保证组名跟着版本定义走，不散落在各调用点。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 收集本 API 包需要执行的 scheme 初始化函数。
	// TODO: 把 SchemeBuilder 与 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 与 AddToScheme 仍留在 k8s.io/kubernetes。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是通用的注册入口，把本包的 group/version 与类型映射到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写函数；生成函数的注册放在生成文件中。
	// 这样即使生成文件缺失，代码依然能编译。
	// 中文补充：addDefaultingFuncs 由默认值生成器产出，手写部分只列名字；
	// 这样代码生成还没跑过时，本包仍能编译，方便本地迭代。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 把已知类型列表注册到给定 scheme。
//
// 中文补充：APIService 和 APIServiceList 都要注册，前者用于单对象编解码，
// 后者用于 List 请求；漏掉 List 会让客户端的 list/watch 直接找不到类型。
// 同时调用 metav1.AddToGroupVersion 把 ListOptions、DeleteOptions 等元类型挂到本组，
// 这是通用 REST 处理的隐含前提。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&APIService{},
		&APIServiceList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
