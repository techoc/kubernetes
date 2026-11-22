//go:build !race

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

// 未启用竞态检测时，单次往返测试的开销较低，可以多迭代一些轮次以提高
// 随机数据覆盖率。启用 -race 时对应的常量定义在 fuzz_race.go 中，数值更小。
const defaultFuzzIters = 20
