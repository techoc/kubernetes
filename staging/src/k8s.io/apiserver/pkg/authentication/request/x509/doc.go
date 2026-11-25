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

// Package x509 提供一个请求认证器，用于校验客户端证书并从中提取用户信息。
// 用户名与组由证书的 Subject/Organization 映射而来，因此 TLS 终止点必须
// 只信任受控 CA；证书校验失败即视为认证失败，不能退化为匿名访问。
package x509
