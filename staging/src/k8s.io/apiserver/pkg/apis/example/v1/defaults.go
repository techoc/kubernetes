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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// addDefaultingFuncs 是 example/v1 的默认值注册入口，供生成的 zz_generated.defaults.go 调用。
//
// example 组只是测试与示例用的类型，不需要任何默认值，因此这里保持空实现；
// 保留该函数是为了让生成器（defaulter-gen）的调用点保持统一，删掉会破坏生成代码的编译。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	// return RegisterDefaults(scheme)
	return nil
}
