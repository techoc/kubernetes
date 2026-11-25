//go:build !go1.21

/*
Copyright 2023 The Kubernetes Authors.

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

package v1

// setSlogDefaultLogger 在 Go < 1.21 时没有 slog 可用，因此什么都不做。
// 该文件通过构建标签保证旧版本编译通过。
func setSlogDefaultLogger() {
	// 使用 Go < 1.21 构建时无需设置 slog 默认 logger。
}
