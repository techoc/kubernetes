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

// Package install installs the experimental API group, making it available as
// an option to all of the API encoding/decoding machinery.
package install

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/apis/audit/v1"
)

// Install registers the API group and adds types to a scheme
func Install(scheme *runtime.Scheme) {
	// 内部版本用于策略求值，外部版本 v1 用于磁盘/网络序列化；两者都需注册才能转换。
	utilruntime.Must(audit.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	// 只声明 v1 为优先版本，确保写入时始终选择 v1 而非内部版本。
	utilruntime.Must(scheme.SetVersionPriority(v1.SchemeGroupVersion))
}
