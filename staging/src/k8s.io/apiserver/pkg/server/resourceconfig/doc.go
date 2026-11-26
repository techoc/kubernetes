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

// Package resourceconfig 包含资源配置相关的辅助函数。
//
// 负责把 --enable-admission-plugins、--runtime-config 等命令行参数与
// 各 API 组的默认值合并，计算出最终启用的资源与准入插件集合。
package resourceconfig
