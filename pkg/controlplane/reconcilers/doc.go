/*
Copyright 2017 The Kubernetes Authors.

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

// Package reconcilers 提供用于管理活跃 master 列表的对象。
// 注意：Lease reconciler 并非 kube-apiserver 之外的其他 apiserver 完成 Endpoint
// 注册的推荐方式，目前这只是一种特殊处理。
package reconcilers
