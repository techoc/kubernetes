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

// Package auth 通过空导入（blank import）注册通用客户端认证插件。
//
// 使用方只需空导入本包，即可让 client-go 在解析 kubeconfig 时识别
// "oidc" 等认证提供者；插件通过 init() 向 client-go 的全局注册表注册，
// 因此导入顺序不会影响可用性。
package auth

import (
	// 触发通用客户端认证插件的初始化。
	// 空导入触发 oidc 插件的 init()，完成认证提供者注册。
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
)
