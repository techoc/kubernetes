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

package kubeadm

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// 定义容器，包含所有用于校验 kubeadm 按预期配置 control-plane 节点的测试规格。

var _ = Describe("control-plane node", func() {

	// 获取 k8s test framework 实例
	f := framework.NewDefaultFramework("control-plane node")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 该容器中的测试不应在集群中创建新对象，
	// 因此禁用命名空间创建以加快执行速度
	f.SkipNamespaceCreation = true

	// 重要！注意该测试不能在单节点集群上运行，
	// 可用 SKIP=multi-node 跳过
	ginkgo.It("should be labelled and tainted [multi-node]", func(ctx context.Context) {
		// 获取所有 control-plane 节点（同时隐式检查节点 label 是否正确）
		controlPlanes := framework.GetControlPlaneNodes(ctx, f.ClientSet)

		// 检查至少有一个 control-plane 节点
		gomega.Expect(controlPlanes.Items).NotTo(gomega.BeEmpty(), "at least one node with label %s should exist. if you are running test on a single-node cluster, you can skip this test with SKIP=multi-node", framework.ControlPlaneLabel)

		// 检查 control-plane 节点带有预期的 taint
		for _, cp := range controlPlanes.Items {
			e2enode.ExpectNodeHasTaint(ctx, f.ClientSet, cp.GetName(), &corev1.Taint{Key: framework.ControlPlaneLabel, Effect: corev1.TaintEffectNoSchedule})
		}
	})
})
