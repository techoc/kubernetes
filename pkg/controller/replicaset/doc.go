/*
Copyright 2016 The Kubernetes Authors.

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

// Package replicaset 包含监听并同步 ReplicaSet 的逻辑。
//
// ReplicaSet 控制器通过 watch 感知 ReplicaSet 与 Pod 的变化，
// 将实际副本数收敛到期望副本数，并处理 Pod 的所有权与隔离。
package replicaset
