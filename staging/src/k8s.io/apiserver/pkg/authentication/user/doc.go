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

// Package user 提供认证相关包之间交换用户信息的工具，user.Info 接口定义了交换这些信息的契约。
// user.Info 是认证器与授权器之间的契约——认证器只负责填充身份，
// 授权器只读取身份，二者不依赖具体认证方式（证书、token、OIDC 等）。
package user
