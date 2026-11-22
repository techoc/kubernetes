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

// Package routes holds a collection of optional genericapiserver http handlers.
//
// 中文说明：这些 handler 不属于 API 资源，而是调试/可观测性入口
// （如 /metrics、/debug/pprof），由各 apiserver 按需挂载。
package routes
