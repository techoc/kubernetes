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

// Package generic 提供通用的对象存储接口和通用的标签/字段匹配类型。
// generic 包是各资源 REST 存储的公共骨架。它把「与具体资源无关」的部分
// （键编码、准入/校验、更新策略、回收、事件分发）与资源特有的转换、策略、选择器解耦，
// 使上层 registry 包只需提供 Storage 对象即可复用 etcd 存储实现。
// 这样做的代价是大量回调（CreateStrategy/UpdateStrategy/AttrFunc 等），
// 但换来的是资源间行为一致性，以及 API 语义（例如乐观并发、资源版本）只实现一次。
package generic
