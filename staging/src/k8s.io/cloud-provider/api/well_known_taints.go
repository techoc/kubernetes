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

package api

const (
	// TaintExternalCloudProvider sets this taint on a node to mark it as unusable,
	// when kubelet is started with the "external" cloud provider, until a controller
	// from the cloud-controller-manager intitializes this node, and then removes
	// the taint
	//
	// 中文说明：这是“云厂商尚未初始化该节点”的哨兵污点。cloud-node-controller
	// 只在节点仍带此污点时才会写入 providerID、地址与拓扑标签；写入成功后移除，
	// 此后该节点不再被重复初始化。kubelet 使用外部云提供商启动时必须带上它，
	// 否则节点可能在云侧信息补齐前就参与调度。
	TaintExternalCloudProvider = "node.cloudprovider.kubernetes.io/uninitialized"

	// TaintNodeShutdown when node is shutdown in external cloud provider
	//
	// 中文说明：云实例被关机但 Node 对象仍然存在时由 node-lifecycle-controller 添加，
	// 阻止新 Pod 调度到已关机的实例上。实例恢复后（Node 变为 Ready）该污点会被移除。
	TaintNodeShutdown = "node.cloudprovider.kubernetes.io/shutdown"
)
