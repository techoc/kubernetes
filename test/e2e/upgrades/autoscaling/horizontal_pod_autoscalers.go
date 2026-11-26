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

package autoscaling

import (
	"context"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eautoscaling "k8s.io/kubernetes/test/e2e/framework/autoscaling"
	"k8s.io/kubernetes/test/e2e/upgrades"

	"github.com/onsi/ginkgo/v2"
)

// HPAUpgradeTest 测试集群升级前后 HPA 能否正确扩缩目标资源。
type HPAUpgradeTest struct {
	rc  *e2eautoscaling.ResourceConsumer
	hpa *autoscalingv2.HorizontalPodAutoscaler
}

// Name 返回测试的跟踪名称。
func (HPAUpgradeTest) Name() string { return "hpa-upgrade" }

// Setup 创建资源消费者和一个自动扩缩该消费者的 HPA 对象。
func (t *HPAUpgradeTest) Setup(ctx context.Context, f *framework.Framework) {
	t.rc = e2eautoscaling.NewDynamicResourceConsumer(ctx,
		"res-cons-upgrade",
		f.Namespace.Name,
		e2eautoscaling.KindReplicaSet,
		1,   /* replicas */
		250, /* initCPUTotal */
		0,
		0,
		500, /* cpuLimit */
		200, /* memLimit */
		f.ClientSet,
		f.ScalesGetter,
		e2eautoscaling.Disable,
		e2eautoscaling.Idle,
		nil,
	)
	t.hpa = e2eautoscaling.CreateCPUResourceHorizontalPodAutoscaler(ctx,
		t.rc,
		20, /* targetCPUUtilizationPercent */
		1,  /* minPods */
		5)  /* maxPods */

	t.rc.Pause()
	t.test(ctx)
}

// Test 等待升级完成并验证 HPA 是否正常工作。
func (t *HPAUpgradeTest) Test(ctx context.Context, f *framework.Framework, done <-chan struct{}, upgrade upgrades.UpgradeType) {
	// 阻塞直到升级完成
	ginkgo.By("Waiting for upgrade to finish before checking HPA")
	<-done
	t.test(ctx)
}

// Teardown 清理所有剩余资源。
func (t *HPAUpgradeTest) Teardown(ctx context.Context, f *framework.Framework) {
	// 依靠命名空间删除来清理所有内容
	e2eautoscaling.DeleteHorizontalPodAutoscaler(ctx, t.rc, t.hpa.Name)
	t.rc.CleanUp(ctx)
}

func (t *HPAUpgradeTest) test(ctx context.Context) {
	const timeToWait = 15 * time.Minute
	t.rc.Resume(ctx)

	ginkgo.By("HPA scales to 1 replica: consume 10 millicores, target per pod 100 millicores, min pods 1.")
	t.rc.ConsumeCPU(10) /* millicores */
	ginkgo.By("HPA waits for 1 replica")
	t.rc.WaitForReplicas(ctx, 1, timeToWait)

	ginkgo.By("HPA scales to 3 replicas: consume 250 millicores, target per pod 100 millicores.")
	t.rc.ConsumeCPU(250) /* millicores */
	ginkgo.By("HPA waits for 3 replicas")
	t.rc.WaitForReplicas(ctx, 3, timeToWait)

	ginkgo.By("HPA scales to 5 replicas: consume 700 millicores, target per pod 100 millicores, max pods 5.")
	t.rc.ConsumeCPU(700) /* millicores */
	ginkgo.By("HPA waits for 5 replicas")
	t.rc.WaitForReplicas(ctx, 5, timeToWait)

	// 必须暂停后台 goroutine，因为升级期间 master 不可用，它们发出的请求会失败。
	t.rc.Pause()
}
