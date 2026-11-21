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

package ports

import (
	cpoptions "k8s.io/cloud-provider/options"
)

// 本文件汇总集群各组件的默认端口，也是一份重要的参考文档，不要轻易删除。
// 这些端口都可以在组件启动时通过命令行参数覆盖。
const (
	// ProxyStatusPort 是 kube-proxy metrics 服务的默认端口。
	// 可在启动时通过参数覆盖。
	ProxyStatusPort = 10249
	// KubeletPort 是每台主机上 kubelet 服务的默认端口。
	// 可在启动时通过参数覆盖。
	KubeletPort = 10250
	// KubeletReadOnlyPort 暴露 kubelet 的基础只读服务。
	// 可在启动时通过参数覆盖。
	// 在 heapster 切换到 SSL 端点之前，需要该端口供其采集 kubelet 监控数据。
	// TODO(roberthbailey): 待 heapster 有更好的方案后删除此端口。
	KubeletReadOnlyPort = 10255
	// KubeletHealthzPort 是 kubelet healthz 端点使用的端口。
	// 可在启动时通过参数覆盖。
	KubeletHealthzPort = 10248
	// ProxyHealthzPort 是 kube-proxy healthz 服务的默认端口。
	// 可在启动时通过参数覆盖。
	ProxyHealthzPort = 10256
	// KubeControllerManagerPort 是 kube-controller-manager 状态服务的默认端口。
	// 可在启动时通过参数覆盖。
	KubeControllerManagerPort = 10257
	// CloudControllerManagerPort 是 cloud-controller-manager 服务的默认端口。
	// 可在启动时通过参数覆盖。
	CloudControllerManagerPort = 10258
	// CloudControllerManagerWebhookPort 是 cloud-controller-manager webhook 服务的默认端口。
	// 可在启动时通过参数覆盖。该值直接复用 cloud-provider 包中的定义，
	// 以保证两个组件使用的端口一致。
	CloudControllerManagerWebhookPort = cpoptions.CloudControllerManagerWebhookPort
)
