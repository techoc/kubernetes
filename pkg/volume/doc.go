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

// Package volume 提供外部存储卷类型的内部抽象，
// 以及 kubelet 挂载/卸载卷所需的工具方法。
// 每个卷插件通过 VolumePlugin 接口接入，插件管理器负责按名称查找和探测。
package volume
