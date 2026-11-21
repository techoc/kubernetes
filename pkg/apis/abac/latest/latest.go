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

package latest

import (
	// 初始化 abac API 包：空导入触发各版本的注册。
	_ "k8s.io/kubernetes/pkg/apis/abac"
	_ "k8s.io/kubernetes/pkg/apis/abac/v0"
	_ "k8s.io/kubernetes/pkg/apis/abac/v1beta1"
)

// 本包只是把 abac 各版本聚合到一起，结构与其他 latest 包不同。
// TODO: this file is totally wrong, it should look like other latest files.
// lavalamp 正在重写这段代码，先等待新的实现方式。
