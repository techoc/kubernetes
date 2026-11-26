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

// addConversionFuncs 注册本版本需要的、无法由代码生成器产出的转换函数。
//
// 之所以保留这个钩子，是因为 conversion-gen 只能处理字段一一对应的机械转换；
// 一旦将来需要特殊处理（例如字段重命名、默认值填充），就在这里补充手写函数，
// 而不必改动 generated 代码。当前没有此类需求。
func addConversionFuncs(scheme *runtime.Scheme) error {
	// 在此添加非生成的转换函数。当前没有。
	return nil
}
