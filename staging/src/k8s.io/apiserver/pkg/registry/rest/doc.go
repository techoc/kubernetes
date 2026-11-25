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

// Package rest 定义 Kubernetes 风格资源变更相关的公共逻辑。
// 这里定义存储层与 HTTP 层之间的契约（Creater、Updater、Getter 等），
// 具体实现在 etcd 存储与各类聚合 API 中；包本身不含存储细节。
package rest
