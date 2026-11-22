/*
Copyright 2025 The Kubernetes Authors.

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

package securitycontext

// possibleCPUs 是 macOS 平台上的 stub 实现。
// macOS 没有 /sys/devices/system/cpu/possible 文件，
// 返回 nil 让调用方回退到 runtime.NumCPU()。
func possibleCPUs() []int {
	return nil
}
