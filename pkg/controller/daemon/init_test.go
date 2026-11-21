/*
Copyright 2020 The Kubernetes Authors.

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

// Package daemon 的测试初始化文件：空导入 ktesting/init 以在测试中安装 klog 的测试输出适配器，
// 使控制器日志进入测试日志流，便于失败时定位问题。
package daemon

import (
	_ "k8s.io/klog/v2/ktesting/init"
)
