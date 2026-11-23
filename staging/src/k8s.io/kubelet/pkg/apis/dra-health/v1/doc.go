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

// Package v1 contains the stable implementation of the DRA health gRPC
// interface. It supersedes v1alpha1.
//
// 本包是 DRA 设备健康检查 gRPC 接口的稳定版本，与 v1alpha1 共用同一份 proto。
// v1alpha1 的转换与兼容包装放在本包中，便于过渡期后一处删除。
package v1
