/*
Copyright 2019 The Kubernetes Authors.

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

// Package events 包含记录与上报 "k8s.io/api/events/v1".Event 事件的全部客户端逻辑。
// 本包面向 events.k8s.io/v1 的 Event 类型，提供事件记录器
// （EventRecorder）与广播器（EventBroadcaster），负责批量聚合、限流与重试上报。
// 与 core/v1 Events 不同，这里的 Event 通过 regarding 字段关联对象。
package events
