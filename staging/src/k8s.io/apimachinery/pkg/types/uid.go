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

package types

// UID 用于保存唯一标识符，包括 UUID。由于 Kubernetes 使用的唯一标识并不只有 UUID，
// 因此底层使用 string；定义为独立类型可以明确表达语义，避免 UID 与普通名称混用。
//
// UID 不限于 UUID，因此用 string 承载；定义成独立类型可以防止与 name 等
// 字符串字段混用。
type UID string
