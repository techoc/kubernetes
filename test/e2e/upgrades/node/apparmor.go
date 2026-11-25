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

package node

import (
	"context"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2esecurity "k8s.io/kubernetes/test/e2e/framework/security"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/upgrades"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
)

// AppArmorUpgradeTest 测试 AppArmor profile 在升级过程中是否持续生效且可用。
type AppArmorUpgradeTest struct {
	pod *v1.Pod
}

// Name 返回该测试的跟踪名称。
func (AppArmorUpgradeTest) Name() string { return "apparmor-upgrade" }

// Skip 在可以跳过该测试时返回 true。
func (AppArmorUpgradeTest) Skip(upgCtx upgrades.UpgradeContext) bool {
	supportedImages := make(map[string]bool)
	for _, d := range e2eskipper.AppArmorDistros {
		supportedImages[d] = true
	}

	for _, vCtx := range upgCtx.Versions {
		if !supportedImages[vCtx.NodeImage] {
			return true
		}
	}
	return false
}

// Setup 创建 secret，然后验证 pod 可以使用它。
func (t *AppArmorUpgradeTest) Setup(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Loading AppArmor profiles to nodes")
	e2esecurity.LoadAppArmorProfiles(ctx, f.Namespace.Name, f.ClientSet)

	// 创建初始测试 pod。
	ginkgo.By("Creating a long-running AppArmor enabled pod.")
	pod := e2esecurity.AppArmorTestPod(f.Namespace.Name, false, false)
	t.pod = e2esecurity.RunAppArmorTestPod(ctx, pod, f.ClientSet, e2epod.NewPodClient(f), false)

	// 验证初始状态。
	t.verifyNodesAppArmorEnabled(ctx, f)
	t.verifyNewPodSucceeds(ctx, f)
}

// Test 等待升级完成，然后验证 pod 仍能使用该 secret。
func (t *AppArmorUpgradeTest) Test(ctx context.Context, f *framework.Framework, done <-chan struct{}, upgrade upgrades.UpgradeType) {
	<-done
	if upgrade == upgrades.MasterUpgrade {
		t.verifyPodStillUp(ctx, f)
	}
	t.verifyNodesAppArmorEnabled(ctx, f)
	t.verifyNewPodSucceeds(ctx, f)
}

// Teardown 清理剩余资源。
func (t *AppArmorUpgradeTest) Teardown(ctx context.Context, f *framework.Framework) {
	// 依赖删除 namespace 来清理所有资源
	ginkgo.By("Logging container failures")
	e2ekubectl.LogFailedContainers(ctx, f.ClientSet, f.Namespace.Name, framework.Logf)
}

func (t *AppArmorUpgradeTest) verifyPodStillUp(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Verifying an AppArmor profile is continuously enforced for a pod")
	pod, err := e2epod.NewPodClient(f).Get(ctx, t.pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "Should be able to get pod")
	gomega.Expect(pod.Status.Phase).To(gomega.Equal(v1.PodRunning), "Pod should stay running")
	gomega.Expect(pod.Status.ContainerStatuses[0].State.Running).NotTo(gomega.BeNil(), "Container should be running")
	gomega.Expect(pod.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero(), "Container should not need to be restarted")
}

func (t *AppArmorUpgradeTest) verifyNewPodSucceeds(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Verifying an AppArmor profile is enforced for a new pod")
	pod := e2esecurity.AppArmorTestPod(f.Namespace.Name, false, true)
	t.pod = e2esecurity.RunAppArmorTestPod(ctx, pod, f.ClientSet, e2epod.NewPodClient(f), true)
}

func (t *AppArmorUpgradeTest) verifyNodesAppArmorEnabled(ctx context.Context, f *framework.Framework) {
	ginkgo.By("Verifying nodes are AppArmor enabled")
	nodes, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err, "Failed to list nodes")
	for _, node := range nodes.Items {
		gomega.Expect(node.Status.Conditions).To(gstruct.MatchElements(conditionType, gstruct.IgnoreExtras, gstruct.Elements{
			"Ready": gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Message": gomega.ContainSubstring("AppArmor enabled"),
			}),
		}))
	}
}

func conditionType(condition interface{}) string {
	return string(condition.(v1.NodeCondition).Type)
}
