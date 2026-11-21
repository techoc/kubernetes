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

package test

import (
	"net"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/testutil"
	netutils "k8s.io/utils/net"
)

// NodePollInterval 是测试中轮询节点更新结果的间隔。
// 使用较小间隔可缩短测试耗时，同时避免忙等。
const NodePollInterval = 10 * time.Millisecond

// AlwaysReady 是始终返回 true 的就绪检查函数，用于不需要等待就绪的测试场景。
var AlwaysReady = func() bool { return true }

// MustParseCIDR 解析 s 中的 CIDR 网段；字符串无法解析时直接 panic。
// 用于测试辅助函数中，输入应为测试代码提供的常量，出错应尽早暴露。
func MustParseCIDR(s string) *net.IPNet {
	_, ret, err := netutils.ParseCIDRSloppy(s)
	if err != nil {
		panic(err)
	}
	return ret
}

// FakeNodeInformer 使用给定的 fakeNodeHandler 创建一个伪造的 NodeInformer。
// 将已有节点直接写入 informer 的本地存储，避免依赖 API 服务器的 watch 事件，
// 使测试可以同步地获得可预测的 informer 状态。
func FakeNodeInformer(fakeNodeHandler *testutil.FakeNodeHandler) coreinformers.NodeInformer {
	fakeClient := &fake.Clientset{}
	fakeInformerFactory := informers.NewSharedInformerFactory(fakeClient, controller.NoResyncPeriodFunc())
	fakeNodeInformer := fakeInformerFactory.Core().V1().Nodes()

	for _, node := range fakeNodeHandler.Existing {
		fakeNodeInformer.Informer().GetStore().Add(node)
	}

	return fakeNodeInformer
}

// WaitForUpdatedNodeWithTimeout 等待 fakeNodeHandler 中累计出现至少 number 个更新后的节点。
// 超时则返回错误，避免测试因异步更新永久挂起。
func WaitForUpdatedNodeWithTimeout(nodeHandler *testutil.FakeNodeHandler, number int, timeout time.Duration) error {
	return wait.Poll(NodePollInterval, timeout, func() (bool, error) {
		if len(nodeHandler.GetUpdatedNodesCopy()) >= number {
			return true, nil
		}
		return false, nil
	})
}
