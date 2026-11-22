/*
Copyright 2023 The Kubernetes Authors.

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

// Package informers 为 Kubernetes API 提供代码生成的 informer。
//
// informer 在本地维护一份共享缓存（通过 tools/cache 中的 SharedIndexInformer），
// 所有调用方共享同一份 watch 连接，从而避免对 apiserver 造成重复的 list/watch 压力。
// 本包中的各具体版本子包（如 core/v1、apps/v1）由代码生成产生，不要手工修改。
package informers
