/*
Copyright 2014 The Kubernetes Authors.

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

// Package watch 定义通用的可 watch 接口，并为使用该接口的代码提供测试用的 fake 实现。
//
// 该包把 watch 的事件流抽象为 Interface，使客户端、缓存与控制器无需关心
// 底层是 HTTP 流、内存实现还是测试桩。
package watch
