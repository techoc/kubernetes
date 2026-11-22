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

// Package remotecommand adds support for executing commands in containers,
// with support for separate stdin, stdout, and stderr streams, as well as
// TTY.
//
// 中文说明：该包通过 SPDY/WebSocket 连接 API Server 的 exec/attach/port-forward
// 子资源。使用 TTY 时 stdout 与 stderr 在协议层合并为同一流，这是终端的固有
// 行为，调用方不能再区分两者。
package remotecommand
