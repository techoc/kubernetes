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

// Package routes 提供 API Server 的辅助 HTTP 路由处理器。
//
// 包含的主要路由：
//
//   - Logs：/logs 路径下的日志文件服务，用于访问节点上的 /var/log 目录
//   - OpenIDMetadataServer：OpenID Connect 元数据和 JWKS（JSON Web Key Set）端点，
//     支持 KSA（Kubernetes Service Account）Token 的验证
//
// 这些路由是可选的，由 API Server 按需安装到 restful.Container 中。
package routes
