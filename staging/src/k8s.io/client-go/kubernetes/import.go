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

// This file exists to enforce this clientset's vanity import path.
//
// 本文件不定义任何类型，只用空导入声明固定 clientset 的“虚荣”导入路径
//（k8s.io/client-go/kubernetes），防止它被错误地以模块内部路径直接引用。
// 包级文档见同目录的 doc.go。

package kubernetes
