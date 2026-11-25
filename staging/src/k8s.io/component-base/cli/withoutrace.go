//go:build !race

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

// Package cli 提供构建 Kubernetes 命令行程序（cobra）的公共样板代码：
// 统一日志初始化、flag 名称归一化以及版本信息输出。
package cli

// logRaceDetection 在未启用 -race 的构建中是空操作。
// 同名函数在 withrace.go 中另有实现，只有内建竞态检测时才会打印提示；
// 用构建标签拆分可以让带 race 与不带 race 的二进制行为各自明确。
func logRaceDetection() {
}
