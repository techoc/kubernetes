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

// Package podgc 包含一个非常简单的 Pod "垃圾回收器" 实现 PodGCController，
// 它运行在 controller manager 中。当处于终止相位（当前为 Failed 或 Succeeded）
// 的 Pod 数量超过可配置阈值时，控制器会删除处于终止状态的 Pod，
// 直到系统重新回到允许的阈值以内。PodGCController 按创建时间戳排序来确定
// 删除优先级，优先删除最旧的对象。PodGCController 不会删除未终止的 Pod。
package podgc
