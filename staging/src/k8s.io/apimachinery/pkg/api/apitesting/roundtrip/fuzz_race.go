//go:build race

/*
Copyright 2021 The Kubernetes Authors.

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

package roundtrip

// 在竞态检测（-race）构建下，模糊测试单次迭代的开销远高于普通构建，
// 因此降低迭代次数，保证 CI 中竞态检测任务的运行时长可控。
const defaultFuzzIters = 5
