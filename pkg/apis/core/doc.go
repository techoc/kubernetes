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

// +k8s:deepcopy-gen=package
// +groupName=

// Package core 包含最新（即“内部”）版本的
// Kubernetes API 对象，即对象在内存中的表示形式。
// 提供给客户端的契约位于带版本的包中，
// 它们是子目录，第一个是 "v1"。这些包
// 描述特定版本如何被序列化到存储/网络中。
package core
