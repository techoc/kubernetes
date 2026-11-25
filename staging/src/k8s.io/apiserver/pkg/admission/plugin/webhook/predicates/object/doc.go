/*
Copyright 2019 The Kubernetes Authors.

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

// Package object 定义 webhook 插件使用的工具，用于判断是否应当运行某个
// webhook：只要旧对象或新对象的标签命中 webhook 配置的 objectSelector，
// 就应当运行。
//
// Package object 提供 webhook 插件的 objectSelector 判定工具：只要旧对象
// 或新对象之一的标签与配置匹配，webhook 就应执行。对 UPDATE 同时检查
// 两者的原因是，选择器可能只命中变更前的标签，若只看新对象会漏掉本应
// 触发的 webhook。
package object
