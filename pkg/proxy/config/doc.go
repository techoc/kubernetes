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

// Package config 将各种配置来源（etcd、文件等）与真正关心这些配置的组件
// （负载均衡器、代理）解耦。Config 接收一个或多个配置来源，支持来自各来源的
// 增量（添加/删除）和全量替换（set）变更，然后对这些配置取并集，
// 为 service 处理器和 endpoint 处理器提供统一视图。它不尝试解决任何形式的配置冲突。
// 基本思路是：每个配置来源从 Config 服务获得一个 channel，并通过该 channel 推送更新；
// Config 记录增量与替换变更，再酌情分发给各个监听者。
package config
