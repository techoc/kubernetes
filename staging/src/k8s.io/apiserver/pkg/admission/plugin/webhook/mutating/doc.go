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

// Package mutating 在准入过程中调用变更型 webhook。
//
// 该包负责 MutatingWebhookConfiguration 的执行：匹配规则、调用 webhook、
// 校验并应用返回的 JSONPatch，以及失败策略（failurePolicy）的处理。
package mutating
