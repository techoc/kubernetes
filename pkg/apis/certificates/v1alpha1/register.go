/*
Copyright 2022 The Kubernetes Authors.

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

package v1alpha1

import (
	certificatesv1alpha1 "k8s.io/api/certificates/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包对应的 API 组名。
const GroupName = "certificates.k8s.io"

// SchemeGroupVersion 是本包使用的组和版本。
var SchemeGroupVersion = schema.GroupVersion{
	Group:   GroupName,
	Version: "v1alpha1",
}

var (
	localSchemeBuilder = &certificatesv1alpha1.SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只手动注册手写函数，生成器负责注册其余部分：
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	localSchemeBuilder.Register(addDefaultingFuncs, addConversionFuncs)
}
