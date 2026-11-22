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

// Package waitgroup implements SafeWaitGroup wrap of sync.WaitGroup.
// Add with positive delta when waiting will fail, to prevent sync.WaitGroup race issue.
//
// 中文说明：Sync.WaitGroup 在 Wait 期间调用 Add 属于未定义行为（可能 panic
// 或漏计）。SafeWaitGroup 选择在 Wait 中直接让 Add 失败，让调用方显式处理，
// 而不是把竞态藏起来。
package waitgroup
