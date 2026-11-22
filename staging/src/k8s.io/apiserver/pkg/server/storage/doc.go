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

// Package storage contains the plumbing to setup the etcd storage of the apiserver.
//
// 本包负责把存储配置（storagebackend.Config）转换成可用的 storage.Interface，
// 包括编解码、加密转换、前缀与资源组映射。它是启动期一次性装配逻辑，
// 不承载任何请求期行为。
package storage
