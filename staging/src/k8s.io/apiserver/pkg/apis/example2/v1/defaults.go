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

// addDefaultingFuncs 由生成的 RegisterDefaults 调用；example2/v1 目前没有默认值，
// 因此保持空实现，仅保留注册钩子以便将来添加字段默认值时不改调用点。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	// return RegisterDefaults(scheme)
	return nil
}
