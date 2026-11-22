/*
Copyright 2015 The Kubernetes Authors.

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

// Package install installs the experimental API group, making it available as
// an option to all of the API encoding/decoding machinery.
// 该包把 internal 与 versioned 类型都注册进 scheme，并设置版本优先级。
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/code-generator/examples/apiserver/apis/example"
	"k8s.io/code-generator/examples/apiserver/apis/example/v1"
)

// Install registers the API group and adds types to a scheme
func Install(scheme *runtime.Scheme) {
	// 先注册 internal（hub）类型，再注册 versioned 类型，二者缺一不可。
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	// 声明 v1 为优先序列化版本，确保输出使用外部版本。
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
