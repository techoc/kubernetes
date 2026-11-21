/*
Copyright The Kubernetes Authors.

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

// Package resourcepoolstatusrequest 包含处理 ResourcePoolStatusRequest 对象的控制器。
// 控制器监听新的请求，通过聚合 ResourceSlice 中的数据并跟踪 ResourceClaim 的分配情况，
// 计算出资源池的状态信息。
// 之所以需要同时关注两类资源，是因为池容量来自 ResourceSlice，而实际占用来自 ResourceClaim。
package resourcepoolstatusrequest
