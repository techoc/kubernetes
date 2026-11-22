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

// Package workqueue provides a simple queue that supports the following
// features:
//   - Fair: items processed in the order in which they are added.
//   - Stingy: a single item will not be processed multiple times concurrently,
//     and if an item is added multiple times before it can be processed, it
//     will only be processed once.
//   - Multiple consumers and producers. In particular, it is allowed for an
//     item to be reenqueued while it is being processed.
//     In this case it will be processed again.
//   - Shutdown notifications.
//
// Package workqueue 提供控制器常用的工作队列：同一对象在处理期间不会被并发处理，
// 重复入队会被去重，但处理中再次入队保证会被再次处理。多个消费者可安全并行，
// 失败重试由 RateLimitingQueue 的退避策略配合 Done/Dirty 语义实现。
package workqueue
