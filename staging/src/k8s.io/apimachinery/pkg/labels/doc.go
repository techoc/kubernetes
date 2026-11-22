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

// Package labels 实现一个简单的标签系统，负责解析 selector 并将其与标签集合进行匹配。
// 标签是不可变的键值对，selector 则是基于这些键值的表达式；二者共同构成
// Kubernetes 中资源筛选与关联的基础机制。
package labels
