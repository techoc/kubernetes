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

package scheduler

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutils "k8s.io/kubernetes/test/integration/util"
	"k8s.io/utils/ptr"
)

// 返回的 shutdown 函数会删除创建的资源和调度器；这些资源应是那些
// 会影响调度结果的资源，例如 Node、Pod 等。Namespace 不应在此处
// 删除，因为它与 apiserver 一起创建；它们应当同时删除，
// 否则我们将没有 Namespace 可用。
// 只有在需要单独停止调度器而不影响 apiserver 时才应调用此方法。
// 例如，在调度器集成测试中，重新创建 apiserver 的开销很大，
// 因此先停止调度器，再在每个测试用例之间重新创建它是更好的做法。
func InitTestSchedulerForFrameworkTest(t *testing.T, testCtx *testutils.TestContext, nodeCount int, runScheduler bool, opts ...scheduler.Option) (*testutils.TestContext, testutils.ShutdownFunc) {
	testCtx = testutils.InitTestSchedulerWithOptions(t, testCtx, 0, opts...)
	testutils.SyncSchedulerInformerFactory(testCtx)
	if runScheduler {
		go testCtx.Scheduler.Run(testCtx.SchedulerCtx)
	}

	if nodeCount > 0 {
		if _, err := testutils.CreateAndWaitForNodesInCache(testCtx, "test-node", st.MakeNode(), nodeCount); err != nil {
			// 初始化出错时确保清理资源。
			testutils.CleanupTest(t, testCtx)
			t.Fatal(err)
		}
	}

	teardown := func() {
		err := testCtx.ClientSet.CoreV1().Nodes().DeleteCollection(testCtx.SchedulerCtx, *metav1.NewDeleteOptions(0), metav1.ListOptions{})
		if err != nil {
			t.Errorf("error while deleting all nodes: %v", err)
		}
		err = testCtx.ClientSet.CoreV1().Pods(testCtx.NS.Name).DeleteCollection(testCtx.SchedulerCtx, *metav1.NewDeleteOptions(0), metav1.ListOptions{})
		if err != nil {
			t.Errorf("error while deleting pod: %v", err)
		}
		// 等待所有 Pod 被删除，否则其他测试用例中同名 Pod 的创建
		// 将会失败。
		err = wait.PollUntilContextTimeout(testCtx.SchedulerCtx, time.Millisecond, wait.ForeverTestTimeout, true,
			testutils.PodsCleanedUp(testCtx.SchedulerCtx, testCtx.ClientSet, testCtx.NS.Name))
		if err != nil {
			t.Errorf("error while waiting for all pods to be deleted: %v", err)
		}
		// 停止调度器。
		testCtx.SchedulerCloseFn()
	}

	return testCtx, teardown
}

// NewPlugin 返回一个使用指定 Plugin 的插件工厂。
func NewPlugin(plugin fwk.Plugin) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		return plugin, nil
	}
}

// InitRegistryAndConfig 根据给定的插件返回注册表和插件配置。
func InitRegistryAndConfig(t *testing.T, factory func(plugin fwk.Plugin) frameworkruntime.PluginFactory, plugins ...fwk.Plugin) (frameworkruntime.Registry, schedulerconfig.KubeSchedulerProfile) {
	if len(plugins) == 0 {
		return frameworkruntime.Registry{}, schedulerconfig.KubeSchedulerProfile{}
	}

	if factory == nil {
		factory = NewPlugin
	}

	registry := frameworkruntime.Registry{}
	pls := &configv1.Plugins{}

	for _, p := range plugins {
		registry.Register(p.Name(), factory(p))
		plugin := configv1.Plugin{Name: p.Name()}

		switch p.(type) {
		case fwk.QueueSortPlugin:
			pls.QueueSort.Enabled = append(pls.QueueSort.Enabled, plugin)
			// 这里是刻意禁用 PrioritySort 插件。
			pls.QueueSort.Disabled = []configv1.Plugin{{Name: queuesort.Name}}
		case fwk.PreEnqueuePlugin:
			pls.PreEnqueue.Enabled = append(pls.PreEnqueue.Enabled, plugin)
		case fwk.PreFilterPlugin:
			pls.PreFilter.Enabled = append(pls.PreFilter.Enabled, plugin)
		case fwk.FilterPlugin:
			pls.Filter.Enabled = append(pls.Filter.Enabled, plugin)
		case fwk.PreScorePlugin:
			pls.PreScore.Enabled = append(pls.PreScore.Enabled, plugin)
		case fwk.ScorePlugin:
			pls.Score.Enabled = append(pls.Score.Enabled, plugin)
		case fwk.ReservePlugin:
			pls.Reserve.Enabled = append(pls.Reserve.Enabled, plugin)
		case fwk.PreBindPlugin:
			pls.PreBind.Enabled = append(pls.PreBind.Enabled, plugin)
		case fwk.BindPlugin:
			pls.Bind.Enabled = append(pls.Bind.Enabled, plugin)
			// 这里是刻意禁用 DefaultBind 插件。否则 DefaultBinder 的失败会导致
			// Pod 调度失败，同时也会导致测试用 BindPlugin 的执行失败。
			pls.Bind.Disabled = []configv1.Plugin{{Name: defaultbinder.Name}}
		case fwk.PostBindPlugin:
			pls.PostBind.Enabled = append(pls.PostBind.Enabled, plugin)
		case fwk.PermitPlugin:
			pls.Permit.Enabled = append(pls.Permit.Enabled, plugin)
		}
	}

	versionedCfg := configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins:       pls,
		}},
	}
	cfg := configtesting.V1ToInternalWithDefaults(t, versionedCfg)
	return registry, cfg.Profiles[0]
}
