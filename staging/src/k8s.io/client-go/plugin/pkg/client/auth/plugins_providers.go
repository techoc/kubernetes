/*
Copyright 2016 The Kubernetes Authors.

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

package auth

import (
	// Initialize client auth plugins for cloud providers.
	//
	// 通过空白导入触发各云厂商插件的 init()，把自身注册到 rest 的插件表；
	// 只有显式导入本文件（或 auth/plugins.go）的二进制才会带上这些插件，
	// 以免给不需要云认证的客户端增加依赖和二进制体积。
	_ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)
