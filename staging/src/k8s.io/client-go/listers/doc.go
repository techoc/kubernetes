/*
Copyright 2023 The Kubernetes Authors.

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

// Package listers provides generated listers for Kubernetes APIs.
//
// Package listers 提供由代码生成器产出的 Lister：它们从本地 Informer 缓存读取对象，
// 不发起网络请求，因此读取结果可能滞后于 apiserver，且返回的对象应视为只读，
// 修改缓存对象会破坏其他使用者看到的数据。
package listers
