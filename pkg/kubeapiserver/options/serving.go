/*
Copyright 2017 The Kubernetes Authors.

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

// Package options 包含初始化 kube-apiserver 所需的标志与选项。
package options

import (
	genericoptions "k8s.io/apiserver/pkg/server/options"
	netutils "k8s.io/utils/net"
)

// NewSecureServingOptions 返回 kube-apiserver 的安全服务默认值，
// 这些值与平台上运行的“普通” API server 所需的不同。
func NewSecureServingOptions() *genericoptions.SecureServingOptionsWithLoopback {
	// kube-apiserver 监听 6443 并启用 loopback，以便本地控制面组件安全访问。
	o := genericoptions.SecureServingOptions{
		BindAddress: netutils.ParseIPSloppy("0.0.0.0"),
		BindPort:    6443,
		Required:    true,
		ServerCert: genericoptions.GeneratableKeyCert{
			PairName:      "apiserver",
			CertDirectory: "/var/run/kubernetes",
		},
	}
	return o.WithLoopback()
}
