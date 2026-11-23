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

// +k8s:deepcopy-gen=package

// 这个包定义调度器 extender 的线上通信契约：请求、响应以及分值边界。
// 它刻意不依赖 scheduler 内部类型，外部进程按这套结构体序列化即可接入。

// Package v1 contains scheduler API objects.
package v1
