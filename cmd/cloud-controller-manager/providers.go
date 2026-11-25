/*
Copyright 2020 The Kubernetes Authors.

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

// 外部 controller manager 负责运行依赖云提供商的 controller 循环。
// 它通过 API 监听资源上的新事件。

package main

// NOTE: 实现 out-of-tree cloud-provider 时无需导入所有 in-tree cloud-provider。
// 保留此空文件仅供参考。

// 注入云提供商的方式如下：首先
// 在诸如 "k8s.io/legacy-cloud-providers/gce" 的包中
// 使用一个调用 `cloudprovider.RegisterCloudProvider()` 的 init() 方法，
// 然后在本文件中添加相应的 import。
//
// import _ "k8s.io/legacy-cloud-providers/gce"
