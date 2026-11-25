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

// Package httpstream 通过连接升级，为 HTTP 请求和响应提供多路复用流传输能力。
// 本包只定义升级协商接口、头部常量和错误类型，具体协议实现留在子包中；
// 客户端使用 UpgradeRoundTripper，服务端使用 ResponseUpgrader，共享同一套抽象。
package httpstream
