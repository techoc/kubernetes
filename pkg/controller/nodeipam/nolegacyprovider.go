/*
Copyright 2019 The Kubernetes Authors.

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

package nodeipam

import (
	"context"
	"errors"
	"net"

	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	cloudprovider "k8s.io/cloud-provider"
)

// fakeController 是 legacy 云厂商支持被禁用时的占位控制器。
// 它只等待 ctx 结束，不执行任何实际分配逻辑。
type fakeController struct {
}

// Run 阻塞直到 ctx 被取消，使上层启动流程在“禁用 legacy 支持”的构建中仍能正常退出。
func (f *fakeController) Run(ctx context.Context) {
	<-ctx.Done()
}

// createLegacyIPAM 构建 legacy IPAM 控制器的桩实现。
// 在本构建（未启用旧版云提供程序支持）中它总是返回错误，
// 提示用户当前二进制不支持从集群/云获取 podCIDR 的旧模式。
func createLegacyIPAM(
	ctx context.Context,
	ic *Controller,
	nodeInformer coreinformers.NodeInformer,
	cloud cloudprovider.Interface,
	kubeClient clientset.Interface,
	clusterCIDRs []*net.IPNet,
	serviceCIDR *net.IPNet,
	nodeCIDRMaskSizes []int,
) (*fakeController, error) {
	return nil, errors.New("Error trying to Init(): legacy cloud provider support disabled at build time")
}
