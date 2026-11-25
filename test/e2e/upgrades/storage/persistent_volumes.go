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

package storage

import (
	"context"

	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	"k8s.io/kubernetes/test/e2e/upgrades"

	"github.com/onsi/ginkgo/v2"
)

// PersistentVolumeUpgradeTest 测试集群升级前后 PV 是否可用。
type PersistentVolumeUpgradeTest struct {
	pvc *v1.PersistentVolumeClaim
}

// Name 返回测试的跟踪名称。
func (PersistentVolumeUpgradeTest) Name() string { return "[sig-storage] persistent-volume-upgrade" }

const (
	pvTestFile string = "/mnt/volume1/pv_upgrade_test"
	pvTestData string = "keep it pv"
	pvWriteCmd string = "echo \"" + pvTestData + "\" > " + pvTestFile
	pvReadCmd  string = "cat " + pvTestFile
)

// Setup 创建 PV，然后验证 Pod 可以使用它。Pod 向 volume 写入数据。
func (t *PersistentVolumeUpgradeTest) Setup(ctx context.Context, f *framework.Framework) {

	var err error
	// 目前该测试的使用者将执行范围限制在 GCE。

	ns := f.Namespace.Name

	ginkgo.By("Creating a PVC")
	pvcConfig := e2epv.PersistentVolumeClaimConfig{
		StorageClassName: nil,
	}
	t.pvc = e2epv.MakePersistentVolumeClaim(pvcConfig, ns)
	t.pvc, err = e2epv.CreatePVC(ctx, f.ClientSet, ns, t.pvc)
	framework.ExpectNoError(err)

	ginkgo.By("Consuming the PV before upgrade")
	t.testPod(ctx, f, pvWriteCmd+";"+pvReadCmd)
}

// Test 等待升级完成，然后验证 Pod 仍可使用该 PV，
// 且 volume 数据仍然存在。
func (t *PersistentVolumeUpgradeTest) Test(ctx context.Context, f *framework.Framework, done <-chan struct{}, upgrade upgrades.UpgradeType) {
	<-done
	ginkgo.By("Consuming the PV after upgrade")
	t.testPod(ctx, f, pvReadCmd)
}

// Teardown 清理所有剩余资源。
func (t *PersistentVolumeUpgradeTest) Teardown(ctx context.Context, f *framework.Framework) {
	errs := e2epv.PVPVCCleanup(ctx, f.ClientSet, f.Namespace.Name, nil, t.pvc)
	if len(errs) > 0 {
		framework.Failf("Failed to delete 1 or more PVs/PVCs. Errors: %v", utilerrors.NewAggregate(errs))
	}
}

// testPod 创建一个使用 PV 并打印其内容的 Pod，然后验证输出。
func (t *PersistentVolumeUpgradeTest) testPod(ctx context.Context, f *framework.Framework, cmd string) {
	pod := e2epod.MakePod(f.Namespace.Name, nil, []*v1.PersistentVolumeClaim{t.pvc}, f.NamespacePodSecurityLevel, cmd)
	expectedOutput := []string{pvTestData}
	e2eoutput.TestContainerOutput(ctx, f, "pod consumes pv", pod, 0, expectedOutput)
}
