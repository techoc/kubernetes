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

// addConversionFuncs 是测试 API 组各版本之间手写转换的入口。
// 该 fixture 目前所有转换均由代码生成，因此无需添加。
func addConversionFuncs(scheme *runtime.Scheme) error {
	// 在此添加非生成的转换函数；目前没有。
	return nil
}
