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

// Package workqueue 提供控制器常用的工作队列，具有以下特性：
//   - 公平：按入队顺序处理。
//   - 去重：同一项不会并发处理，处理前的重复入队只处理一次。
//   - 多生产者/消费者：处理中允许重新入队，此时该项会被再次处理。
//   - 关闭通知。
//
// 去重避免同一对象被并发 reconcile；处理中重新入队则保证最新变更不会丢失。
// 失败重试由 RateLimitingQueue 的退避策略配合 Done/Dirty 语义实现。
package workqueue
