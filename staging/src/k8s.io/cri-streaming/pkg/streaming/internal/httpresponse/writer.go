/*
Copyright The Kubernetes Authors.

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

package httpresponse

import "net/http"

// wrapper 是 http.ResponseWriter 装饰器的通用契约：
// 凡是包装了另一个 ResponseWriter 的类型，都通过 Unwrap 暴露被包装的内层对象。
type wrapper interface {
	Unwrap() http.ResponseWriter
}

// GetOriginal 沿着 ResponseWriter 的包装链一路向下，返回最内层的原始 writer。
// 这是必要的：某些中间件（如压缩、日志中间件）会包装 ResponseWriter，
// 而升级协议（HTTP Upgrade / WebSocket）需要拿到最原始的 writer 才能接管底层连接。
// 实现要点：
//   - 逐层判断当前 writer 是否实现了 wrapper 接口；
//   - 不是则返回当前 writer；
//   - 是则取内层继续循环；
//   - 若发现 Unwrap 返回自身（形成环），说明包装链异常，直接 panic 避免死循环。
func GetOriginal(w http.ResponseWriter) http.ResponseWriter {
	for {
		decorator, ok := w.(wrapper)
		// 到达链尾：当前 writer 没有被包装，即为原始 writer。
		if !ok {
			return w
		}
		inner := decorator.Unwrap()
		// 检测到环，防止无限循环。
		if inner == w {
			panic("http.ResponseWriter decorator chain has a cycle")
		}
		w = inner
	}
}
