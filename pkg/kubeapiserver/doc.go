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

// Package kubeapiserver 存放 kube-apiserver 与 federation-apiserver 共用、
// 但不属于通用 API server 的代码。
// 例如非委托授权选项被这两个 server 使用，而通用 API server 通常不会用到它们。
package kubeapiserver
