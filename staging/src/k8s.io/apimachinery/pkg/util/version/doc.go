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

// version 包提供版本号比较工具。
//
// version 包提供语义化版本号的解析、比较与区间判断，支持 Kubernetes 的
// "v1.2.3-alpha.1+meta" 这类带预发布与构建元数据的格式。
package version
