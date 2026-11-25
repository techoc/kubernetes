/*
Copyright The Kubernetes Authors.

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

package nodedeclaredfeatures

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	_ "k8s.io/component-base/logs/json/register"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	ndffeatures "k8s.io/component-helpers/nodedeclaredfeatures/features"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	perf "k8s.io/kubernetes/test/integration/scheduler_perf"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// 本测试评估 NodeDeclaredFeatures 对性能的影响。
// 它通过 'numNodeDeclaredFeatures' 参数模拟注册任意数量的节点 feature，但所有 feature 的推断函数
// 都只检查 Pod 级别资源（'spec.resources'）。测试创建的 Pod 带有 Pod 级资源，确保调度时
// 所有 feature 都会被推断。

func TestMain(m *testing.M) {
	if err := perf.InitTests(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	m.Run()
}

// mockFeature 是 Feature 接口的测试用模拟实现。
type mockFeature struct {
	name       string
	maxVersion *version.Version
}

func (f *mockFeature) Name() string {
	return f.name
}

func (f *mockFeature) Discover(cfg *ndf.NodeConfiguration) bool {
	// kubelet 正常会调用该函数发现节点 feature 并上报到 node status。测试中跳过发现流程，
	// 创建节点后直接更新 status（见 updateNodesWithDeclaredFeatures()），因此这里是空实现。
	return true
}

func (f *mockFeature) InferForScheduling(podInfo *ndf.PodInfo) bool {
	// 检查 Pod 是否使用了 Pod 级别资源。
	return podInfo.Spec != nil && podInfo.Spec.Resources != nil
}

func (f *mockFeature) InferForUpdate(oldPodInfo, newPodInfo *ndf.PodInfo) bool {
	return false
}

func (f *mockFeature) Requirements() *ndf.FeatureRequirements {
	return nil
}

func (f *mockFeature) MaxVersion() *version.Version {
	return f.maxVersion
}

func createMockFeature(name string, maxVersionStr string) ndf.Feature {
	var v *version.Version
	if maxVersionStr != "" {
		v = version.MustParseSemantic(maxVersionStr)
	}
	return &mockFeature{
		name:       name,
		maxVersion: v,
	}
}

func setupFeatures(numNodeDeclaredFeatures int) func() {
	nodeFeatures := make([]ndf.Feature, 0, numNodeDeclaredFeatures)
	for i := 1; i <= numNodeDeclaredFeatures; i++ {
		featureName := fmt.Sprintf("Feature%d", i)
		nodeFeatures = append(nodeFeatures, createMockFeature(featureName, ""))
	}
	originalAllFeatures := ndffeatures.AllFeatures
	ndffeatures.AllFeatures = nodeFeatures
	return func() {
		ndffeatures.AllFeatures = originalAllFeatures
	}
}

func preInitNodeDeclaredFeatures(t ktesting.TContext, w *perf.Workload) (func(), error) {
	if !w.FeatureGates[features.NodeDeclaredFeatures] {
		t.Logf("Skipping NodeDeclaredFeatures pre-init as the feature gate is disabled")
		return func() {}, nil
	}

	numNodeDeclaredFeatures, err := w.GetParam("numNodeDeclaredFeatures")
	if err != nil {
		t.Logf("numNodeDeclaredFeatures param not specified in workload config")
		numNodeDeclaredFeatures = 0
	}

	t.Logf("PreInit: Setting up %d mock features for %s", numNodeDeclaredFeatures, w.Name)
	// 注册到 NDF 库的模拟 feature。
	cleanupMockFeatures := setupFeatures(numNodeDeclaredFeatures)

	return cleanupMockFeatures, nil
}

func updateNodesWithDeclaredFeatures(tCtx ktesting.TContext, scheduler *scheduler.Scheduler, w *perf.Workload, nodes *v1.NodeList) error {
	if !w.FeatureGates[features.NodeDeclaredFeatures] {
		return nil
	}
	numNodeDeclaredFeatures, err := w.GetParam("numNodeDeclaredFeatures")
	if err != nil {
		tCtx.Logf("numNodeDeclaredFeatures param not specified in workload config")
		numNodeDeclaredFeatures = 0
	}

	var featureNames []string
	for i := 1; i <= numNodeDeclaredFeatures; i++ {
		featureNames = append(featureNames, fmt.Sprintf("Feature%d", i))
	}
	sort.Strings(featureNames)

	for _, node := range nodes.Items {
		nodeToUpdate := node.DeepCopy()
		nodeToUpdate.Status.DeclaredFeatures = featureNames
		_, err := tCtx.Client().CoreV1().Nodes().UpdateStatus(tCtx.Context, nodeToUpdate, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update node %s status: %w", node.Name, err)
		}
		tCtx.Logf("Updated node %s status with %d features", node.Name, len(featureNames))
	}

	schedulerCache := scheduler.Cache
	err = wait.PollUntilContextTimeout(tCtx.Context, 100*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		for _, n := range nodes.Items {
			nodeInfo, err := schedulerCache.GetNode(n.Name)
			if err != nil {
				return false, nil

			}
			cachedNode := nodeInfo.Node()
			if !slices.Equal(cachedNode.Status.DeclaredFeatures, featureNames) {
				return false, nil
			}
		}
		return true, nil
	})

	if err != nil {
		return fmt.Errorf("timeout waiting for all nodes to reflect status update: %w", err)
	}

	return nil
}

func TestSchedulerPerf(t *testing.T) {
	perf.RunIntegrationPerfScheduling(t, "performance-config.yaml", perf.WithPreRunFn(preInitNodeDeclaredFeatures), perf.WithNodeUpdateFn(updateNodesWithDeclaredFeatures))
}

func BenchmarkPerfScheduling(b *testing.B) {
	perf.RunBenchmarkPerfScheduling(b, "performance-config.yaml", "nodedeclaredfeatures", nil, perf.WithPreRunFn(preInitNodeDeclaredFeatures), perf.WithNodeUpdateFn(updateNodesWithDeclaredFeatures))
}
