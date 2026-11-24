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

// Package sets 提供泛型集合和若干具体类型集合。随着代码迁移，泛型集合将逐步替代具体类型集合；
// 具体类型集合已弃用。
//
// 中文补充：新代码应使用泛型 Set[T]/New[T]，带具体类型的 Set 变体仅为兼容保留。
package sets
