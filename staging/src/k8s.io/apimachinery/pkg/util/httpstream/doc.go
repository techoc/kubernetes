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

// Package httpstream contains compatibility wrappers for streaming transport APIs.
//
// httpstream 包保留旧的流式传输 API 入口，实际实现已迁移到 k8s.io/streaming；
// 这里仅作为兼容层，新代码不应依赖。
//
// Deprecated: use k8s.io/streaming/pkg/httpstream directly.
package httpstream
