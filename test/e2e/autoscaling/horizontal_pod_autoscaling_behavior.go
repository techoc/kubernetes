/*
Copyright 2022 The Kubernetes Authors.

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
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eautoscaling "k8s.io/kubernetes/test/e2e/framework/autoscaling"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	hpaName = "consumer"

	podCPURequest               = 500
	targetCPUUtilizationPercent = 25

	fullWindowOfNewUsage     = 30 * time.Second
	windowWithOldUsagePasses = 30 * time.Second
	newPodMetricsDelay       = 15 * time.Second
	metricsAvailableDelay    = fullWindowOfNewUsage + windowWithOldUsagePasses + newPodMetricsDelay

	hpaReconciliationInterval = 15 * time.Second
	actuationDelay            = 10 * time.Second
	maxHPAReactionTime        = metricsAvailableDelay + hpaReconciliationInterval + actuationDelay

	maxConsumeCPUDelay          = 30 * time.Second
	waitForReplicasPollInterval = 20 * time.Second
	maxResourceConsumerDelay    = maxConsumeCPUDelay + waitForReplicasPollInterval
)

var _ = SIGDescribe(feature.HPA, "Horizontal pod autoscaling (non-default behavior)", func() {
	f := framework.NewDefaultFramework("horizontal-pod-autoscaling")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	waitBuffer := 1 * time.Minute

	ginkgo.Describe("with short downscale stabilization window", func() {
		ginkgo.It("should scale down soon after the stabilization period", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 1
			initCPUUsageTotal := usageForReplicas(initPods)
			upScaleStabilization := 0 * time.Minute
			downScaleStabilization := 1 * time.Minute

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 5,
				e2eautoscaling.HPABehaviorWithStabilizationWindows(upScaleStabilization, downScaleStabilization),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			// 确保 HPA 已就绪、正常工作，并且在降低用量前
			// 已记录用于稳定逻辑的推荐值
			ginkgo.By("triggering scale up to record a recommendation")
			rc.ConsumeCPU(usageForReplicas(3))
			rc.WaitForReplicas(ctx, 3, maxHPAReactionTime+maxResourceConsumerDelay+waitBuffer)

			ginkgo.By("triggering scale down by lowering consumption")
			rc.ConsumeCPU(usageForReplicas(2))
			waitStart := time.Now()
			rc.WaitForReplicas(ctx, 2, downScaleStabilization+maxHPAReactionTime+maxResourceConsumerDelay+waitBuffer)
			timeWaited := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale down")
			framework.Logf("time waited for scale down: %s", timeWaited)
			gomega.Expect(timeWaited).To(gomega.BeNumerically(">", downScaleStabilization), "waited %s, wanted more than %s", timeWaited, downScaleStabilization)
			deadline := downScaleStabilization + maxHPAReactionTime + maxResourceConsumerDelay
			gomega.Expect(timeWaited).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaited, deadline)
		})
	})

	ginkgo.Describe("with long upscale stabilization window", func() {
		ginkgo.It("should scale up only after the stabilization period", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 2
			initCPUUsageTotal := usageForReplicas(initPods)
			upScaleStabilization := 3 * time.Minute
			downScaleStabilization := 0 * time.Minute

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 10,
				e2eautoscaling.HPABehaviorWithStabilizationWindows(upScaleStabilization, downScaleStabilization),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			// 确保 HPA 已就绪、正常工作，并且在降低用量前
			// 在提高用量前已记录用于稳定逻辑的推荐值
			ginkgo.By("triggering scale down to record a recommendation")
			rc.ConsumeCPU(usageForReplicas(1))
			rc.WaitForReplicas(ctx, 1, maxHPAReactionTime+maxResourceConsumerDelay+waitBuffer)

			ginkgo.By("triggering scale up by increasing consumption")
			rc.ConsumeCPU(usageForReplicas(3))
			waitStart := time.Now()
			rc.WaitForReplicas(ctx, 3, upScaleStabilization+maxHPAReactionTime+maxResourceConsumerDelay+waitBuffer)
			timeWaited := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale up")
			framework.Logf("time waited for scale up: %s", timeWaited)
			gomega.Expect(timeWaited).To(gomega.BeNumerically(">", upScaleStabilization), "waited %s, wanted more than %s", timeWaited, upScaleStabilization)
			deadline := upScaleStabilization + maxHPAReactionTime + maxResourceConsumerDelay
			gomega.Expect(timeWaited).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaited, deadline)
		})
	})

	ginkgo.Describe("with autoscaling disabled", func() {
		ginkgo.It("shouldn't scale up", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 1
			initCPUUsageTotal := usageForReplicas(initPods)

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 10, e2eautoscaling.HPABehaviorWithScaleDisabled(e2eautoscaling.ScaleUpDirection),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			waitDeadline := maxHPAReactionTime + maxResourceConsumerDelay + waitBuffer

			ginkgo.By("trying to trigger scale up")
			rc.ConsumeCPU(usageForReplicas(8))
			waitStart := time.Now()

			rc.EnsureDesiredReplicasInRange(ctx, initPods, initPods, waitDeadline, hpa.Name)
			timeWaited := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale up")
			framework.Logf("time waited for scale up: %s", timeWaited)
			gomega.Expect(timeWaited).To(gomega.BeNumerically(">", waitDeadline), "waited %s, wanted to wait more than %s", timeWaited, waitDeadline)

			ginkgo.By("verifying number of replicas")
			replicas, err := rc.GetReplicas(ctx)
			framework.ExpectNoError(err)
			gomega.Expect(replicas).To(gomega.BeNumerically("==", initPods), "had %s replicas, still have %s replicas after time deadline", initPods, replicas)
		})

		ginkgo.It("shouldn't scale down", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 3
			initCPUUsageTotal := usageForReplicas(initPods)

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 10, e2eautoscaling.HPABehaviorWithScaleDisabled(e2eautoscaling.ScaleDownDirection),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			defaultDownscaleStabilisation := 5 * time.Minute
			waitDeadline := maxHPAReactionTime + maxResourceConsumerDelay + defaultDownscaleStabilisation

			ginkgo.By("trying to trigger scale down")
			rc.ConsumeCPU(usageForReplicas(1))
			waitStart := time.Now()

			rc.EnsureDesiredReplicasInRange(ctx, initPods, initPods, waitDeadline, hpa.Name)
			timeWaited := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale down")
			framework.Logf("time waited for scale down: %s", timeWaited)
			gomega.Expect(timeWaited).To(gomega.BeNumerically(">", waitDeadline), "waited %s, wanted to wait more than %s", timeWaited, waitDeadline)

			ginkgo.By("verifying number of replicas")
			replicas, err := rc.GetReplicas(ctx)
			framework.ExpectNoError(err)
			gomega.Expect(replicas).To(gomega.BeNumerically("==", initPods), "had %s replicas, still have %s replicas after time deadline", initPods, replicas)
		})

	})

	ginkgo.Describe("with scale limited by number of Pods rate", func() {
		ginkgo.It("should scale up no more than given number of Pods per minute", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 1
			initCPUUsageTotal := usageForReplicas(initPods)
			limitWindowLength := 1 * time.Minute
			podsLimitPerMinute := 1

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 10,
				e2eautoscaling.HPABehaviorWithScaleLimitedByNumberOfPods(e2eautoscaling.ScaleUpDirection, int32(podsLimitPerMinute), int32(limitWindowLength.Seconds())),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			ginkgo.By("triggering scale up by increasing consumption")
			rc.ConsumeCPU(usageForReplicas(3))

			waitStart := time.Now()
			rc.WaitForReplicas(ctx, 2, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor2 := time.Since(waitStart)

			waitStart = time.Now()
			rc.WaitForReplicas(ctx, 3, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor3 := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale up to 2 replicas")
			deadline := limitWindowLength + maxHPAReactionTime + maxResourceConsumerDelay
			// 第一次扩缩容事件可以立即发生，因为过去没有扩缩容事件。
			gomega.Expect(timeWaitedFor2).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor2, deadline)

			ginkgo.By("verifying time waited for a scale up to 3 replicas")
			// 第二次扩缩容事件需要遵守 limit window。
			gomega.Expect(timeWaitedFor3).To(gomega.BeNumerically(">", limitWindowLength), "waited %s, wanted to wait more than %s", timeWaitedFor3, limitWindowLength)
			gomega.Expect(timeWaitedFor3).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor3, deadline)
		})

		ginkgo.It("should scale down no more than given number of Pods per minute", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 3
			initCPUUsageTotal := usageForReplicas(initPods)
			limitWindowLength := 1 * time.Minute
			podsLimitPerMinute := 1

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 10,
				e2eautoscaling.HPABehaviorWithScaleLimitedByNumberOfPods(e2eautoscaling.ScaleDownDirection, int32(podsLimitPerMinute), int32(limitWindowLength.Seconds())),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			ginkgo.By("triggering scale down by lowering consumption")
			rc.ConsumeCPU(usageForReplicas(1))

			waitStart := time.Now()
			rc.WaitForReplicas(ctx, 2, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor2 := time.Since(waitStart)

			waitStart = time.Now()
			rc.WaitForReplicas(ctx, 1, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor1 := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale down to 2 replicas")
			deadline := limitWindowLength + maxHPAReactionTime + maxResourceConsumerDelay
			// 第一次扩缩容事件可以立即发生，因为过去没有扩缩容事件。
			gomega.Expect(timeWaitedFor2).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor2, deadline)

			ginkgo.By("verifying time waited for a scale down to 1 replicas")
			// 第二次扩缩容事件需要遵守 limit window。
			gomega.Expect(timeWaitedFor1).To(gomega.BeNumerically(">", limitWindowLength), "waited %s, wanted more than %s", timeWaitedFor1, limitWindowLength)
			gomega.Expect(timeWaitedFor1).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor1, deadline)
		})
	})

	ginkgo.Describe("with scale limited by percentage", func() {
		ginkgo.It("should scale up no more than given percentage of current Pods per minute", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 2
			initCPUUsageTotal := usageForReplicas(initPods)
			limitWindowLength := 1 * time.Minute
			percentageLimitPerMinute := 50

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 10,
				e2eautoscaling.HPABehaviorWithScaleLimitedByPercentage(e2eautoscaling.ScaleUpDirection, int32(percentageLimitPerMinute), int32(limitWindowLength.Seconds())),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			ginkgo.By("triggering scale up by increasing consumption")
			rc.ConsumeCPU(usageForReplicas(8))

			waitStart := time.Now()
			rc.WaitForReplicas(ctx, 3, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor3 := time.Since(waitStart)

			waitStart = time.Now()
			// 按百分比限制的扩容取上整，因此新副本数为 ceil(3 * 1.5) = ceil(4.5) = 5
			rc.WaitForReplicas(ctx, 5, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor5 := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale up to 3 replicas")
			deadline := limitWindowLength + maxHPAReactionTime + maxResourceConsumerDelay
			// 第一次扩缩容事件可以立即发生，因为过去没有扩缩容事件。
			gomega.Expect(timeWaitedFor3).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor3, deadline)

			ginkgo.By("verifying time waited for a scale up to 5 replicas")
			// 第二次扩缩容事件需要遵守 limit window。
			gomega.Expect(timeWaitedFor5).To(gomega.BeNumerically(">", limitWindowLength), "waited %s, wanted to wait more than %s", timeWaitedFor5, limitWindowLength)
			gomega.Expect(timeWaitedFor5).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor5, deadline)
		})

		ginkgo.It("should scale down no more than given percentage of current Pods per minute", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 7
			initCPUUsageTotal := usageForReplicas(initPods)
			limitWindowLength := 1 * time.Minute
			percentageLimitPerMinute := 25

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 10,
				e2eautoscaling.HPABehaviorWithScaleLimitedByPercentage(e2eautoscaling.ScaleDownDirection, int32(percentageLimitPerMinute), int32(limitWindowLength.Seconds())),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			ginkgo.By("triggering scale down by lowering consumption")
			rc.ConsumeCPU(usageForReplicas(1))

			waitStart := time.Now()
			rc.WaitForReplicas(ctx, 5, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor5 := time.Since(waitStart)

			waitStart = time.Now()
			// 按百分比限制的缩容取下整，因此新副本数为 floor(5 * 0.75) = floor(3.75) = 3
			rc.WaitForReplicas(ctx, 3, maxHPAReactionTime+maxResourceConsumerDelay+limitWindowLength)
			timeWaitedFor3 := time.Since(waitStart)

			ginkgo.By("verifying time waited for a scale down to 5 replicas")
			deadline := limitWindowLength + maxHPAReactionTime + maxResourceConsumerDelay
			// 第一次扩缩容事件可以立即发生，因为过去没有扩缩容事件。
			gomega.Expect(timeWaitedFor5).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor5, deadline)

			ginkgo.By("verifying time waited for a scale down to 3 replicas")
			// 第二次扩缩容事件需要遵守 limit window。
			gomega.Expect(timeWaitedFor3).To(gomega.BeNumerically(">", limitWindowLength), "waited %s, wanted more than %s", timeWaitedFor3, limitWindowLength)
			gomega.Expect(timeWaitedFor3).To(gomega.BeNumerically("<", deadline), "waited %s, wanted less than %s", timeWaitedFor3, deadline)
		})
	})

	ginkgo.Describe("with both scale up and down controls configured", func() {
		waitBuffer := 2 * time.Minute

		ginkgo.It("should keep recommendation within the range over two stabilization windows", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 1
			initCPUUsageTotal := usageForReplicas(initPods)
			upScaleStabilization := 3 * time.Minute
			downScaleStabilization := 3 * time.Minute

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 1, 5,
				e2eautoscaling.HPABehaviorWithStabilizationWindows(upScaleStabilization, downScaleStabilization),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			ginkgo.By("triggering scale up by increasing consumption")
			rc.ConsumeCPU(usageForReplicas(3))
			waitDeadline := upScaleStabilization

			ginkgo.By("verifying number of replicas stay in desired range within stabilisation window")
			rc.EnsureDesiredReplicasInRange(ctx, 1, 1, waitDeadline, hpa.Name)

			ginkgo.By("waiting for replicas to scale up after stabilisation window passed")
			waitStart := time.Now()
			waitDeadline = maxHPAReactionTime + maxResourceConsumerDelay + waitBuffer
			rc.WaitForReplicas(ctx, 3, waitDeadline)
			timeWaited := time.Since(waitStart)
			framework.Logf("time waited for scale up: %s", timeWaited)
			gomega.Expect(timeWaited).To(gomega.BeNumerically("<", waitDeadline), "waited %s, wanted less than %s", timeWaited, waitDeadline)

			ginkgo.By("triggering scale down by lowering consumption")
			rc.ConsumeCPU(usageForReplicas(2))
			waitDeadline = downScaleStabilization

			ginkgo.By("verifying number of replicas stay in desired range within stabilisation window")
			rc.EnsureDesiredReplicasInRange(ctx, 3, 3, waitDeadline, hpa.Name)

			ginkgo.By("waiting for replicas to scale down after stabilisation window passed")
			waitStart = time.Now()
			waitDeadline = maxHPAReactionTime + maxResourceConsumerDelay + waitBuffer
			rc.WaitForReplicas(ctx, 2, waitDeadline)
			timeWaited = time.Since(waitStart)
			framework.Logf("time waited for scale down: %s", timeWaited)
			gomega.Expect(timeWaited).To(gomega.BeNumerically("<", waitDeadline), "waited %s, wanted less than %s", timeWaited, waitDeadline)
		})

		ginkgo.It("should keep recommendation within the range with stabilization window and pod limit rate", func(ctx context.Context) {
			ginkgo.By("setting up resource consumer and HPA")
			initPods := 2
			initCPUUsageTotal := usageForReplicas(initPods)
			downScaleStabilization := 3 * time.Minute
			limitWindowLength := 2 * time.Minute
			podsLimitPerMinute := 1

			rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
				hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
				initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
				f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
				nil)
			ginkgo.DeferCleanup(rc.CleanUp)

			scaleUpRule := e2eautoscaling.HPAScalingRuleWithScalingPolicy(autoscalingv2.PodsScalingPolicy, int32(podsLimitPerMinute), int32(limitWindowLength.Seconds()))
			scaleDownRule := e2eautoscaling.HPAScalingRuleWithStabilizationWindow(int32(downScaleStabilization.Seconds()))
			hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
				rc, int32(targetCPUUtilizationPercent), 2, 5,
				e2eautoscaling.HPABehaviorWithScaleUpAndDownRules(scaleUpRule, scaleDownRule),
			)
			ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

			ginkgo.By("triggering scale up by increasing consumption")
			rc.ConsumeCPU(usageForReplicas(4))
			waitDeadline := limitWindowLength

			ginkgo.By("verifying number of replicas stay in desired range with pod limit rate")
			rc.EnsureDesiredReplicasInRange(ctx, 2, 3, waitDeadline, hpa.Name)

			ginkgo.By("waiting for replicas to scale up")
			waitStart := time.Now()
			waitDeadline = limitWindowLength + maxHPAReactionTime + maxResourceConsumerDelay + waitBuffer
			rc.WaitForReplicas(ctx, 4, waitDeadline)
			timeWaited := time.Since(waitStart)
			framework.Logf("time waited for scale up: %s", timeWaited)
			gomega.Default.Expect(timeWaited).To(gomega.BeNumerically("<", waitDeadline), "waited %s, wanted less than %s", timeWaited, waitDeadline)

			ginkgo.By("triggering scale down by lowering consumption")
			rc.ConsumeCPU(usageForReplicas(2))

			ginkgo.By("verifying number of replicas stay in desired range within stabilisation window")
			waitDeadline = downScaleStabilization
			rc.EnsureDesiredReplicasInRange(ctx, 4, 4, waitDeadline, hpa.Name)

			ginkgo.By("waiting for replicas to scale down after stabilisation window passed")
			waitStart = time.Now()
			waitDeadline = maxHPAReactionTime + maxResourceConsumerDelay + waitBuffer
			rc.WaitForReplicas(ctx, 2, waitDeadline)
			timeWaited = time.Since(waitStart)
			framework.Logf("time waited for scale down: %s", timeWaited)
			gomega.Expect(timeWaited).To(gomega.BeNumerically("<", waitDeadline), "waited %s, wanted less than %s", timeWaited, waitDeadline)
		})
	})
})

var _ = SIGDescribe(feature.HPA, framework.WithSlow(), framework.WithFeatureGate(features.HPAConfigurableTolerance),
	"Horizontal pod autoscaling (configurable tolerance)", func() {
		f := framework.NewDefaultFramework("horizontal-pod-autoscaling")
		f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

		waitBuffer := 2 * time.Minute

		ginkgo.Describe("with large configurable tolerance", func() {
			ginkgo.It("should not scale", func(ctx context.Context) {
				ginkgo.By("setting up resource consumer and HPA")
				initPods := 1
				initCPUUsageTotal := usageForReplicas(initPods)

				rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
					hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
					initCPUUsageTotal, 0, 0, int64(podCPURequest), 200,
					f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
					nil)
				ginkgo.DeferCleanup(rc.CleanUp)

				scaleRule := e2eautoscaling.HPAScalingRuleWithToleranceMilli(10000)
				hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
					rc, int32(targetCPUUtilizationPercent), 1, 10,
					e2eautoscaling.HPABehaviorWithScaleUpAndDownRules(scaleRule, scaleRule),
				)
				ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

				waitDeadline := maxHPAReactionTime + maxResourceConsumerDelay + waitBuffer

				ginkgo.By("trying to trigger scale up")
				rc.ConsumeCPU(usageForReplicas(8))
				waitStart := time.Now()

				rc.EnsureDesiredReplicasInRange(ctx, initPods, initPods, waitDeadline, hpa.Name)
				timeWaited := time.Since(waitStart)

				ginkgo.By("verifying time waited for a scale up")
				framework.Logf("time waited for scale up: %s", timeWaited)
				gomega.Expect(timeWaited).To(gomega.BeNumerically(">", waitDeadline), "waited %s, wanted to wait more than %s", timeWaited, waitDeadline)

				ginkgo.By("verifying number of replicas")
				replicas, err := rc.GetReplicas(ctx)
				framework.ExpectNoError(err)
				gomega.Expect(replicas).To(gomega.BeNumerically("==", initPods), "had %s replicas, still have %s replicas after time deadline", initPods, replicas)
			})
		})

		ginkgo.Describe("with small scale-up, large scale-down tolerances", func() {
			ginkgo.It("should scale up but should not scale down", func(ctx context.Context) {
				ginkgo.By("setting up resource consumer and HPA")
				initPods := 10
				podCPURequest := 200
				targetCPUUtilizationPercent := 60
				// 计算 10 个 Pod 的初始负载。
				initCPUUsageTotal := usageForReplicasWithRequest(initPods, podCPURequest, targetCPUUtilizationPercent)
				waitDeadline := maxHPAReactionTime + maxResourceConsumerDelay + waitBuffer

				// initCPUTotal 传 0，让常规的 ConsumeCPU goroutine 保持空闲。
				// 完全通过 ConsumeCPUPerPod 驱动负载，以保证均匀分布。
				rc := e2eautoscaling.NewDynamicResourceConsumer(ctx,
					hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
					0, 0, 0, int64(podCPURequest), 200,
					f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
					nil)
				ginkgo.DeferCleanup(rc.CleanUp)

				scaleUpRule := e2eautoscaling.HPAScalingRuleWithToleranceMilli(20)    // 2%
				scaleDownRule := e2eautoscaling.HPAScalingRuleWithToleranceMilli(300) // 30%
				hpa := e2eautoscaling.CreateCPUHorizontalPodAutoscalerWithBehavior(ctx,
					rc, int32(targetCPUUtilizationPercent), int32(initPods), 12,
					e2eautoscaling.HPABehaviorWithScaleUpAndDownRules(scaleUpRule, scaleDownRule),
				)
				ginkgo.DeferCleanup(e2eautoscaling.DeleteHPAWithBehavior, rc, hpa.Name)

				ginkgo.By("waiting for deployment to start initial pods")
				rc.WaitForReplicas(ctx, initPods, waitDeadline)

				// 使用均匀的每 Pod 分布设置初始稳定负载，并等待
				// HPA 观察到它后再触发扩容。这确保 HPA 在我们将负载推到扩容量容忍度之上前
				// 已有基线测量值。
				rc.ConsumeCPUPerPod(initCPUUsageTotal)
				rc.EnsureDesiredReplicasInRange(ctx, initPods, initPods, maxHPAReactionTime+maxResourceConsumerDelay, hpa.Name)

				ginkgo.By("trying to trigger scale up to 11 replicas")
				rc.ConsumeCPUPerPod(usageForReplicasWithRequest(11, podCPURequest, targetCPUUtilizationPercent))

				ginkgo.By("waiting for replicas to scale up")
				waitStart := time.Now()
				rc.WaitForReplicas(ctx, 11, waitDeadline)
				timeWaited := time.Since(waitStart)
				framework.Logf("time waited for scale up: %s", timeWaited)
				gomega.Expect(timeWaited).To(gomega.BeNumerically("<", waitDeadline), "waited %s, wanted less than %s", timeWaited, waitDeadline)

				// 增加到 12 个副本。差值小于默认的 10% 容忍度，
				// 但由于自定义扩容量容忍度很小（2%），仍应扩容。
				ginkgo.By("trying to trigger scale up to 12 replicas")
				rc.ConsumeCPUPerPod(usageForReplicasWithRequest(12, podCPURequest, targetCPUUtilizationPercent))

				ginkgo.By("waiting for replicas to scale up")
				waitStart = time.Now()
				rc.WaitForReplicas(ctx, 12, waitDeadline)
				timeWaited = time.Since(waitStart)
				framework.Logf("time waited for scale up: %s", timeWaited)
				gomega.Expect(timeWaited).To(gomega.BeNumerically("<", waitDeadline), "waited %s, wanted less than %s", timeWaited, waitDeadline)

				// 将负载降回 10 副本的水平。不应缩容，因为
				// 差值在自定义缩容量容忍度（30%）范围内。
				ginkgo.By("triggering scale down by lowering consumption")
				waitStart = time.Now()
				rc.ConsumeCPUPerPod(usageForReplicasWithRequest(10, podCPURequest, targetCPUUtilizationPercent))

				rc.EnsureDesiredReplicasInRange(ctx, 12, 12, waitDeadline, hpa.Name)
				timeWaited = time.Since(waitStart)

				ginkgo.By("verifying time waited for a scale down")
				framework.Logf("time waited for scale down: %s", timeWaited)
				gomega.Expect(timeWaited).To(gomega.BeNumerically(">", waitDeadline), "waited %s, wanted to wait more than %s", timeWaited, waitDeadline)

			})
		})
	})

// usageForReplicas 返回 (n - 0.5) 个副本的用量，假设它们消耗了目标值以下的全部 CPU。
// 减少 0.5 个副本是为了容纳实际消耗的 CPU 与 ResourceConsumer 请求的用量之间的偏差。
// HPA 会对推荐值向上取整。因此，如果用量相当于 3.5 个副本，
// 推荐的副本数将是 4。
// 无 panic
func usageForReplicas(replicas int) int {
	return usageForReplicasWithRequest(replicas, podCPURequest, targetCPUUtilizationPercent)
}

func usageForReplicasWithRequest(replicas, podRequest, targetUtilizationPercent int) int {
	usagePerReplica := podRequest * targetUtilizationPercent / 100
	return replicas*usagePerReplica - usagePerReplica/2
}
