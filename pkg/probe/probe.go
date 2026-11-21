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

package probe

// Result 表示探针执行结果，用于判断容器的就绪/存活/启动状态。
// Kubelet 根据 Result 值决定容器的后续操作：
//   - Success：容器健康，无操作
//   - Warning：逻辑上成功，但附带调试信息（如重定向终止），通常视为成功
//   - Failure：容器不健康，根据探针类型触发重启或从 Service 端点移除
//   - Unknown：探针执行本身出错（如命令无法执行），需进一步排查
type Result string

const (
	// Success 表示探针检查成功，容器健康
	Success Result = "success"
	// Warning 表示逻辑上成功，但附带额外调试信息
	// 例如 HTTP 探针遇到重定向时返回 Warning，而非直接失败
	Warning Result = "warning"
	// Failure 表示探针检查失败，容器不健康
	Failure Result = "failure"
	// Unknown 表示探针执行过程中出现意外错误，无法判断结果
	// 与 Failure 不同，Unknown 通常意味着探针本身的问题（如命令无法执行）
	Unknown Result = "unknown"
)
