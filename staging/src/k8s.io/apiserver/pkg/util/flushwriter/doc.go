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

// Package flushwriter implements a wrapper for a writer that flushes on every
// write if that writer implements the io.Flusher interface
//
// 该包服务于流式响应场景：apiserver 的 watch、日志、exec 等接口依赖每次写入后立即
// flush，才能让客户端及时看到增量数据。把 flush 与写入绑定在一起，调用方只需写入，
// 无需在每条记录后手动 Flush，也就不会因为漏调 Flush 而导致响应被缓冲吞掉。
package flushwriter
