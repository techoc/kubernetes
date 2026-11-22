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

// Package route contains code for syncing cloud routing rules with
// the list of registered nodes.
//
// 中文说明：route controller 把每个节点的 PodCIDR 映射为一条云侧路由，并通过
// NodeNetworkUnavailable condition 反映路由是否就绪；云侧路由由本控制器独占管理，
// 判断归属时以 --cluster-cidr 为边界。
package route
