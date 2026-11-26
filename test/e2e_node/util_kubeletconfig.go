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

package e2enode

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"

	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2enodekubelet "k8s.io/kubernetes/test/e2e_node/kubeletconfig"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// 返回当前的 KubeletConfiguration
func getCurrentKubeletConfig(ctx context.Context) (*kubeletconfig.KubeletConfiguration, error) {
	// namespace 仅在 useProxy==true 时有用，因此不必费心
	return e2enodekubelet.GetCurrentKubeletConfig(ctx, framework.TestContext.NodeName, "", false, framework.TestContext.StandaloneMode)
}

// 必须在 Context 中调用。允许函数在 Context 的 BeforeEach 期间修改 KubeletConfiguration。
// 修改会在 Context 的 AfterEach 中还原。
func tempSetCurrentKubeletConfig(f *framework.Framework, updateFunction func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration)) {
	var oldCfg *kubeletconfig.KubeletConfiguration

	ginkgo.BeforeEach(func(ctx context.Context) {
		var err error
		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)

		newCfg := oldCfg.DeepCopy()
		updateFunction(ctx, newCfg)
		if apiequality.Semantic.DeepEqual(*newCfg, *oldCfg) {
			return
		}

		updateKubeletConfig(ctx, f, newCfg, true)
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		if oldCfg != nil {
			// 更新 Kubelet 配置。
			updateKubeletConfig(ctx, f, oldCfg, true)
		}
	})
}

type updateKubeletOptions struct {
	deleteStateFiles          bool
	ensureConsistentReadyNode bool
	// TODO: 增加使用 systemctl stop 的选项，目前因历史原因只用 systemctl kill
}

func updateKubeletConfigWithOptions(ctx context.Context, f *framework.Framework, kubeletConfig *kubeletconfig.KubeletConfiguration, opts updateKubeletOptions) {
	ginkgo.GinkgoHelper()

	withStoppedKubelet(ctx, f, opts.ensureConsistentReadyNode, func() {
		// 删除 CPU 和 memory manager 状态文件，确保它们不会阻止 kubelet 重启
		if opts.deleteStateFiles {
			deleteStateFile(cpuManagerStateFile)
			deleteStateFile(memoryManagerStateFile)
			deleteStateFile(usernsStateFiles)
		}

		framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(kubeletConfig))
	})

}

func updateKubeletConfig(ctx context.Context, f *framework.Framework, kubeletConfig *kubeletconfig.KubeletConfiguration, deleteStateFiles bool) {
	updateKubeletConfigWithOptions(ctx, f, kubeletConfig, updateKubeletOptions{
		deleteStateFiles: deleteStateFiles,
	})
}

func tempRemoveImagePulledRecord(f *framework.Framework, imageID *string) {
	pulledRecordFilenameFromID := func(imageID string) string {
		return fmt.Sprintf("sha256-%x", sha256.Sum256([]byte(imageID)))
	}

	ginkgo.BeforeEach(func(ctx context.Context) {
		pulledRecordsDir := "/var/lib/kubelet/image_manager/pulled"
		fileName := pulledRecordFilenameFromID(*imageID)
		origPath := filepath.Join(pulledRecordsDir, fileName)
		tempPath := filepath.Join(pulledRecordsDir, "temp_"+fileName)

		// 等待 kubelet 创建记录
		err := wait.PollUntilContextTimeout(ctx, time.Second, 5*time.Second, true, func(_ context.Context) (bool, error) {
			if _, err := os.Stat(origPath); err != nil {
				f.Logf("failed to stat file %q: %v", origPath, err)
				dirEntries, err := os.ReadDir(pulledRecordsDir)
				if err != nil {
					f.Logf("failed to read directory contents for %q: %v", pulledRecordsDir, err)
					return false, nil
				}
				files := map[string]string{}
				for _, entry := range dirEntries {
					entryPath := filepath.Join(pulledRecordsDir, entry.Name())
					content, _ := os.ReadFile(entryPath)
					files[entryPath] = string(content)
				}
				f.Logf("contents of %q: %v", pulledRecordsDir, files)
				return false, nil
			}
			return true, nil
		})
		framework.ExpectNoError(err, "the file with the pulled record for the image ID never appeared")

		withStoppedKubelet(ctx, f, false, func() {
			err := os.Rename(origPath, tempPath)
			framework.ExpectNoError(err, "failed to move the ImagePulledRecord file")

			ginkgo.DeferCleanup(func(ctx context.Context) {
				withStoppedKubelet(ctx, f, false, func() {
					err := os.Rename(tempPath, origPath)
					framework.ExpectNoError(err, "failed to move the ImagePulledRecord file back to its original location")
				})
			})
		})
	})
}

// withStoppedKubelet 停止 kubelet，执行 `action`，再启动 kubelet
func withStoppedKubelet(ctx context.Context, f *framework.Framework, ensureConsistentReadyNode bool, action func()) {
	ginkgo.GinkgoHelper()

	kubeletStart := mustStopKubelet(ctx, f)
	defer func() {
		kubeletStart(ctx)
		if ensureConsistentReadyNode {
			gomega.Consistently(ctx, func(ctx context.Context) bool {
				return getNodeReadyStatus(ctx, f) && e2enode.HealthCheck(kubeletHealthCheckURL)
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.BeTrueBecause("node keeps reporting ready status"))
		}
	}()

	action()
}

func getNodeReadyStatus(ctx context.Context, f *framework.Framework) bool {
	ginkgo.GinkgoHelper()
	nodeList, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err)
	// 假定只有一个节点，因为这是 node e2e 测试。
	gomega.Expect(nodeList.Items).To(gomega.HaveLen(1), "the number of nodes is not as expected")
	return isNodeReady(&nodeList.Items[0])
}

// 节点 ready 时 isNodeReady 返回 true，否则返回 false。
func isNodeReady(node *v1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == v1.NodeReady {
			return c.Status == v1.ConditionTrue
		}
	}
	return false
}
