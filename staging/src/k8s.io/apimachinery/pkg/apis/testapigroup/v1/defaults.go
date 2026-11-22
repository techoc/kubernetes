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

// addDefaultingFuncs 注册该版本的默认值函数。
//
// testapigroup 是测试用的 API 组，没有需要默认化的字段，因此这里不做任何事。
// 保留函数是因为 scheme 构建代码按统一约定调用它；返回值 nil 表示注册成功。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	// return RegisterDefaults(scheme)
	return nil
}
