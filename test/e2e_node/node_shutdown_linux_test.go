//go:build linux

/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/kubectl/pkg/util/podutils"

	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"

	"github.com/godbus/dbus/v5"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"
	testutils "k8s.io/kubernetes/test/utils"
)

var _ = SIGDescribe("GracefulNodeShutdown", framework.WithSerial(), feature.GracefulNodeShutdown, feature.GracefulNodeShutdownBasedOnPodPriority, func() {
	f := framework.NewDefaultFramework("graceful-node-shutdown")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func() {
		if _, err := exec.LookPath("systemd-run"); err == nil {
			if version, verr := exec.Command("systemd-run", "--version").Output(); verr == nil {
				// $ systemd-run --version 的输出示例
				// systemd 245 (245.4-4ubuntu3.13)
				re := regexp.MustCompile(`systemd (\d+)`)
				if match := re.FindSubmatch(version); len(match) > 1 {
					systemdVersion, err := strconv.Atoi(string(match[1]))
					if err != nil {
						framework.Logf("failed to parse systemd version with error %v, 'systemd-run --version' output was [%s]", err, version)
					} else {
						// 见 issue 107043 的讨论：旧版 systemd 上该功能长期不可用，属于已知问题
						// https://github.com/kubernetes/kubernetes/issues/107043#issuecomment-997546598
						if systemdVersion < 245 {
							e2eskipper.Skipf("skipping GracefulNodeShutdown tests as we are running on an old version of systemd : %d", systemdVersion)
						}
					}
				}
			}
		}
	})

	f.Context("graceful node shutdown; baseline scenario to verify DisruptionTarget is added", func() {

		const (
			pollInterval            = 1 * time.Second
			podStatusUpdateTimeout  = 30 * time.Second
			nodeStatusUpdateTimeout = 30 * time.Second
			nodeShutdownGracePeriod = 30 * time.Second
		)

		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			if initialConfig.FeatureGates == nil {
				initialConfig.FeatureGates = map[string]bool{}
			}
			initialConfig.FeatureGates[string(features.GracefulNodeShutdown)] = true
			initialConfig.FeatureGates[string(features.GracefulNodeShutdownBasedOnPodPriority)] = false
			initialConfig.ShutdownGracePeriod = metav1.Duration{Duration: nodeShutdownGracePeriod}
		})

		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Wait for the node to be ready")
			waitForNodeReady(ctx)
		})

		ginkgo.AfterEach(func() {
			ginkgo.By("Emitting Shutdown false signal; cancelling the shutdown")
			err := emitSignalPrepareForShutdown(false)
			framework.ExpectNoError(err)
		})

		ginkgo.It("should add the DisruptionTarget pod failure condition to the evicted pods", func(ctx context.Context) {
			nodeName := getNodeName(ctx, f)
			nodeSelector := fields.Set{
				"spec.nodeName": nodeName,
			}.AsSelector().String()

			// 定义测试用 Pod
			pods := []*v1.Pod{
				getGracePeriodOverrideTestPod("pod-to-evict-"+string(uuid.NewUUID()), nodeName, 5, ""),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ginkgo.By("reating batch pods")
			e2epod.NewPodClient(f).CreateBatch(ctx, pods)

			list, err := e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
				FieldSelector: nodeSelector,
			})

			framework.ExpectNoError(err)
			gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")

			list, err = e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
				FieldSelector: nodeSelector,
			})
			if err != nil {
				framework.Failf("Failed to start batch pod: %q", err)
			}
			gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")

			for _, pod := range list.Items {
				framework.Logf("Pod (%v/%v) status conditions: %#v", pod.Namespace, pod.Name, pod.Status.Conditions)
			}

			ginkgo.By("Verifying batch pods are running")
			for _, pod := range list.Items {
				if podReady, err := testutils.PodRunningReady(&pod); err != nil || !podReady {
					framework.Failf("Failed to start batch pod: (%v/%v)", pod.Namespace, pod.Name)
				}
			}

			ginkgo.By("Emitting shutdown signal")
			err = emitSignalPrepareForShutdown(true)
			framework.ExpectNoError(err)

			ginkgo.By("Verifying that all pods are shutdown")
			// 所有 Pod 都应已关停
			gomega.Eventually(func() error {
				list, err = e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
					FieldSelector: nodeSelector,
				})
				if err != nil {
					return err
				}
				gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")

				for _, pod := range list.Items {
					if !isPodShutdown(&pod) {
						framework.Logf("Expecting pod to be shutdown, but it's not currently. Pod: (%v/%v), Pod Status Phase: %q, Pod Status Reason: %q", pod.Namespace, pod.Name, pod.Status.Phase, pod.Status.Reason)
						return fmt.Errorf("pod should be shutdown, phase: %s", pod.Status.Phase)
					}
					podDisruptionCondition := e2epod.FindPodConditionByType(&pod.Status, v1.DisruptionTarget)
					if podDisruptionCondition == nil {
						framework.Failf("pod (%v/%v) should have the condition: %q, pod status: %v", pod.Namespace, pod.Name, v1.DisruptionTarget, pod.Status)
					}
				}
				return nil
			}, podStatusUpdateTimeout+(nodeShutdownGracePeriod), pollInterval).Should(gomega.BeNil())
		})
	})

	ginkgo.Context("when gracefully shutting down", func() {

		const (
			pollInterval                        = 1 * time.Second
			podStatusUpdateTimeout              = 30 * time.Second
			nodeStatusUpdateTimeout             = 30 * time.Second
			nodeShutdownGracePeriod             = 20 * time.Second
			nodeShutdownGracePeriodCriticalPods = 10 * time.Second
		)

		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			if initialConfig.FeatureGates == nil {
				initialConfig.FeatureGates = map[string]bool{}
			}
			initialConfig.FeatureGates[string(features.GracefulNodeShutdown)] = true
			initialConfig.FeatureGates[string(features.GracefulNodeShutdownBasedOnPodPriority)] = false
			initialConfig.FeatureGates[string(features.PodReadyToStartContainersCondition)] = true

			initialConfig.ShutdownGracePeriod = metav1.Duration{Duration: nodeShutdownGracePeriod}
			initialConfig.ShutdownGracePeriodCriticalPods = metav1.Duration{Duration: nodeShutdownGracePeriodCriticalPods}
		})

		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Wait for the node to be ready")
			waitForNodeReady(ctx)
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			ginkgo.By("Emitting Shutdown false signal; cancelling the shutdown")
			err := emitSignalPrepareForShutdown(false)
			framework.ExpectNoError(err)
		})

		ginkgo.It("should be able to gracefully shutdown pods with various grace periods", func(ctx context.Context) {
			nodeName := getNodeName(ctx, f)
			nodeSelector := fields.Set{
				"spec.nodeName": nodeName,
			}.AsSelector().String()

			// 定义测试用 Pod
			pods := []*v1.Pod{
				getGracePeriodOverrideTestPod("period-120-"+string(uuid.NewUUID()), nodeName, 120, ""),
				getGracePeriodOverrideTestPod("period-5-"+string(uuid.NewUUID()), nodeName, 5, ""),
				getGracePeriodOverrideTestPod("period-critical-120-"+string(uuid.NewUUID()), nodeName, 120, scheduling.SystemNodeCritical),
				getGracePeriodOverrideTestPod("period-critical-5-"+string(uuid.NewUUID()), nodeName, 5, scheduling.SystemNodeCritical),
			}

			ginkgo.By("Creating batch pods")
			e2epod.NewPodClient(f).CreateBatch(ctx, pods)

			list, err := e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
				FieldSelector: nodeSelector,
			})
			framework.ExpectNoError(err)
			gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() {
				defer ginkgo.GinkgoRecover()
				w := &cache.ListWatch{
					WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
						return f.ClientSet.CoreV1().Pods(f.Namespace.Name).Watch(ctx, options)
					},
				}

				// 持续监听 Pod 事件，以便发现非法的状态更新
				_, err = watchtools.Until(ctx, list.ResourceVersion, w, func(event watch.Event) (bool, error) {
					if pod, ok := event.Object.(*v1.Pod); ok {
						if isPodStatusAffectedByIssue108594(pod) {
							return false, fmt.Errorf("failing test due to detecting invalid pod status")
						}
						// Watch 不会自行结束（只有测试因 context 取消而结束时才终止）
						return false, nil
					}
					return false, nil
				})

				// context 会被显式取消，Watch 永远不会返回 true，因此忽略超时错误
				if err != nil && !wait.Interrupted(err) {
					framework.Failf("watch for invalid pod status failed: %v", err.Error())
				}
			}()

			ginkgo.By("Verifying batch pods are running")
			for _, pod := range list.Items {
				if podReady, err := testutils.PodRunningReady(&pod); err != nil || !podReady {
					framework.Failf("Failed to start batch pod: %v", pod.Name)
				}
			}

			ginkgo.By("Emitting shutdown signal")
			err = emitSignalPrepareForShutdown(true)
			framework.ExpectNoError(err)

			ginkgo.By("Verifying that non-critical pods are shutdown")
			// 非关键 Pod 应已关停
			gomega.Eventually(ctx, func(ctx context.Context) error {
				list, err = e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
					FieldSelector: nodeSelector,
				})
				if err != nil {
					return err
				}
				gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")

				for _, pod := range list.Items {
					if kubelettypes.IsCriticalPod(&pod) {
						if isPodShutdown(&pod) {
							framework.Logf("Expecting critical pod (%v/%v) to be running, but it's not currently. Pod Status %+v", pod.Namespace, pod.Name, pod.Status)
							return fmt.Errorf("critical pod (%v/%v) should not be shutdown, phase: %s", pod.Namespace, pod.Name, pod.Status.Phase)
						}
					} else {
						if !isPodShutdown(&pod) {
							framework.Logf("Expecting non-critical pod (%v/%v) to be shutdown, but it's not currently. Pod Status %+v", pod.Namespace, pod.Name, pod.Status)
							return fmt.Errorf("pod (%v/%v) should be shutdown, phase: %s", pod.Namespace, pod.Name, pod.Status.Phase)
						}
					}
				}
				return nil
			}, podStatusUpdateTimeout, pollInterval).Should(gomega.Succeed())

			ginkgo.By("Verifying that all pods are shutdown")
			// 所有 Pod 都应已关停
			gomega.Eventually(ctx, func(ctx context.Context) error {
				list, err = e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
					FieldSelector: nodeSelector,
				})
				if err != nil {
					return err
				}
				gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")

				for _, pod := range list.Items {
					if !isPodShutdown(&pod) {
						framework.Logf("Expecting pod (%v/%v) to be shutdown, but it's not currently: Pod Status %+v", pod.Namespace, pod.Name, pod.Status)
						return fmt.Errorf("pod (%v/%v) should be shutdown, phase: %s", pod.Namespace, pod.Name, pod.Status.Phase)
					}
				}
				return nil
			},
				// 关键 Pod 在 (nodeShutdownGracePeriod-nodeShutdownGracePeriodCriticalPods) 之后才开始关停
				podStatusUpdateTimeout+(nodeShutdownGracePeriod-nodeShutdownGracePeriodCriticalPods),
				pollInterval).Should(gomega.Succeed())

			ginkgo.By("Verify that all pod ready to start condition are set to false after terminating")
			// 所有 Pod 的 ready to start 条件都应置为 false
			gomega.Eventually(ctx, func(ctx context.Context) error {
				list, err = e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
					FieldSelector: nodeSelector,
				})
				if err != nil {
					return err
				}
				gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)))
				for _, pod := range list.Items {
					if !isPodReadyToStartConditionSetToFalse(&pod) {
						framework.Logf("Expecting pod (%v/%v) 's ready to start condition set to false, "+
							"but it's not currently: Pod Condition %+v", pod.Namespace, pod.Name, pod.Status.Conditions)
						return fmt.Errorf("pod (%v/%v) 's ready to start condition should be false, condition: %v, phase: %s",
							pod.Namespace, pod.Name, pod.Status.Conditions, pod.Status.Phase)
					}
				}
				return nil
			},
			).Should(gomega.Succeed())
		})

		ginkgo.It("should be able to handle a cancelled shutdown", func(ctx context.Context) {
			ginkgo.By("Emitting Shutdown signal")
			err := emitSignalPrepareForShutdown(true)
			framework.ExpectNoError(err)
			gomega.Eventually(ctx, func(ctx context.Context) error {
				isReady := getNodeReadyStatus(ctx, f)
				if isReady {
					return fmt.Errorf("node did not become shutdown as expected")
				}
				return nil
			}, nodeStatusUpdateTimeout, pollInterval).Should(gomega.Succeed())

			ginkgo.By("Emitting Shutdown false signal; cancelling the shutdown")
			err = emitSignalPrepareForShutdown(false)
			framework.ExpectNoError(err)
			gomega.Eventually(ctx, func(ctx context.Context) error {
				isReady := getNodeReadyStatus(ctx, f)
				if !isReady {
					return fmt.Errorf("node did not recover as expected")
				}
				return nil
			}, nodeStatusUpdateTimeout, pollInterval).Should(gomega.Succeed())
		})

	})

	f.Context("when gracefully shutting down with an extended shutdown window", func() {
		const (
			nodeStatusUpdateTimeout = 30 * time.Second
			nodeShutdownGracePeriod = 2 * time.Minute
			nodeLeaseDuration       = 10
			nodeLeaseCreateTimeout  = 30 * time.Second
		)

		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			if initialConfig.FeatureGates == nil {
				initialConfig.FeatureGates = map[string]bool{}
			}
			initialConfig.FeatureGates[string(features.GracefulNodeShutdown)] = true
			initialConfig.FeatureGates[string(features.GracefulNodeShutdownBasedOnPodPriority)] = false
			initialConfig.ShutdownGracePeriod = metav1.Duration{Duration: nodeShutdownGracePeriod}
			initialConfig.ShutdownGracePeriodCriticalPods = metav1.Duration{Duration: 0}
			// 固定 lease 续约节奏，确保测试能在关停窗口内观察到多次续约。
			initialConfig.NodeLeaseDurationSeconds = nodeLeaseDuration
		})

		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Wait for the node to be ready")
			waitForNodeReady(ctx)
		})

		ginkgo.AfterEach(func() {
			ginkgo.By("Emitting Shutdown false signal; cancelling the shutdown")
			err := emitSignalPrepareForShutdown(false)
			framework.ExpectNoError(err)
		})

		ginkgo.It("should continue renewing the node lease during graceful shutdown", func(ctx context.Context) {
			nodeName := getNodeName(ctx, f)
			leaseClient := f.ClientSet.CoordinationV1().Leases(v1.NamespaceNodeLease)

			ginkgo.By("Waiting for the node lease to exist")
			var lease *coordinationv1.Lease
			// 慢速测试机上 kubelet 启动并发布 node lease 需要较长时间，等待要足够久。
			gomega.Eventually(ctx, func(ctx context.Context) error {
				var err error
				lease, err = leaseClient.Get(ctx, nodeName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				return expectNodeLease(lease, nodeName)
			}, nodeLeaseCreateTimeout, framework.Poll).Should(gomega.Succeed())

			ginkgo.By("Emitting shutdown signal")
			err := emitSignalPrepareForShutdown(true)
			framework.ExpectNoError(err)

			ginkgo.By("Waiting for graceful shutdown to become active")
			gomega.Eventually(ctx, func(ctx context.Context) error {
				node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if err := expectNodeReadyCondition(node, v1.ConditionFalse, "KubeletNotReady"); err != nil {
					return fmt.Errorf("graceful shutdown not active yet: %w", err)
				}
				return nil
			}, nodeStatusUpdateTimeout, framework.Poll).Should(gomega.Succeed())

			shutdownLease, err := leaseClient.Get(ctx, nodeName, metav1.GetOptions{})
			framework.ExpectNoError(err)
			framework.ExpectNoError(expectNodeLease(shutdownLease, nodeName))
			framework.Logf("Graceful shutdown is active, starting node lease observation at renewTime=%v", shutdownLease.Spec.RenewTime.Time)

			ginkgo.By("Verifying the node lease continues to renew during graceful shutdown")
			lastRenewTime := shutdownLease.Spec.RenewTime.Time
			renewalsObserved := 0
			gomega.Eventually(ctx, func(ctx context.Context) error {
				newLease, err := leaseClient.Get(ctx, nodeName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if err := expectNodeLease(newLease, nodeName); err != nil {
					return err
				}
				if newLease.Spec.RenewTime.Time.After(lastRenewTime) {
					renewalsObserved++
					framework.Logf("Observed node lease renewal %d/2 during graceful shutdown: %v -> %v", renewalsObserved, lastRenewTime, newLease.Spec.RenewTime.Time)
					lastRenewTime = newLease.Spec.RenewTime.Time
				} else {
					framework.Logf("Node lease renewTime has not advanced yet during graceful shutdown: observed=%d current=%v last=%v", renewalsObserved, newLease.Spec.RenewTime.Time, lastRenewTime)
				}
				if renewalsObserved < 2 {
					return fmt.Errorf("observed %d node lease renewals during graceful shutdown, last renewTime=%v", renewalsObserved, lastRenewTime)
				}
				return nil
			}, nodeShutdownGracePeriod, framework.Poll).Should(gomega.Succeed())

		})
	})

	framework.Context("when gracefully shutting down with Pod priority", framework.WithFlaky(), func() {

		const (
			pollInterval                 = 1 * time.Second
			podStatusUpdateTimeout       = 30 * time.Second
			priorityClassesCreateTimeout = 10 * time.Second
		)

		var (
			customClassA = getPriorityClass("custom-class-a", 100000)
			customClassB = getPriorityClass("custom-class-b", 10000)
			customClassC = getPriorityClass("custom-class-c", 1000)
		)

		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			if initialConfig.FeatureGates == nil {
				initialConfig.FeatureGates = map[string]bool{}
			}
			initialConfig.FeatureGates[string(features.GracefulNodeShutdown)] = true
			initialConfig.FeatureGates[string(features.GracefulNodeShutdownBasedOnPodPriority)] = true

			initialConfig.ShutdownGracePeriodByPodPriority = []kubeletconfig.ShutdownGracePeriodByPodPriority{
				{
					Priority:                   scheduling.SystemCriticalPriority,
					ShutdownGracePeriodSeconds: int64(podStatusUpdateTimeout / time.Second),
				},
				{
					Priority:                   customClassA.Value,
					ShutdownGracePeriodSeconds: int64(podStatusUpdateTimeout / time.Second),
				},
				{
					Priority:                   customClassB.Value,
					ShutdownGracePeriodSeconds: int64(podStatusUpdateTimeout / time.Second),
				},
				{
					Priority:                   customClassC.Value,
					ShutdownGracePeriodSeconds: int64(podStatusUpdateTimeout / time.Second),
				},
				{
					Priority:                   scheduling.DefaultPriorityWhenNoDefaultClassExists,
					ShutdownGracePeriodSeconds: int64(podStatusUpdateTimeout / time.Second),
				},
			}

		})

		ginkgo.BeforeEach(func(ctx context.Context) {
			ginkgo.By("Wait for the node to be ready")
			waitForNodeReady(ctx)
			customClasses := []*schedulingv1.PriorityClass{customClassA, customClassB, customClassC}
			for _, customClass := range customClasses {
				_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, customClass, metav1.CreateOptions{})
				if err != nil && !apierrors.IsAlreadyExists(err) {
					framework.ExpectNoError(err)
				}
			}
			gomega.Eventually(ctx, func(ctx context.Context) error {
				for _, customClass := range customClasses {
					_, err := f.ClientSet.SchedulingV1().PriorityClasses().Get(ctx, customClass.Name, metav1.GetOptions{})
					if err != nil {
						return err
					}
				}
				return nil
			}, priorityClassesCreateTimeout, pollInterval).Should(gomega.Succeed())
		})

		ginkgo.AfterEach(func() {
			ginkgo.By("Emitting Shutdown false signal; cancelling the shutdown")
			err := emitSignalPrepareForShutdown(false)
			framework.ExpectNoError(err)
		})

		ginkgo.It("should be able to gracefully shutdown pods with various grace periods", func(ctx context.Context) {
			nodeName := getNodeName(ctx, f)
			nodeSelector := fields.Set{
				"spec.nodeName": nodeName,
			}.AsSelector().String()

			var (
				period5Name         = "period-5-" + string(uuid.NewUUID())
				periodC5Name        = "period-c-5-" + string(uuid.NewUUID())
				periodB5Name        = "period-b-5-" + string(uuid.NewUUID())
				periodA5Name        = "period-a-5-" + string(uuid.NewUUID())
				periodCritical5Name = "period-critical-5-" + string(uuid.NewUUID())
			)

			// 定义测试用 Pod
			pods := []*v1.Pod{
				getGracePeriodOverrideTestPod(period5Name, nodeName, 5, ""),
				getGracePeriodOverrideTestPod(periodC5Name, nodeName, 5, customClassC.Name),
				getGracePeriodOverrideTestPod(periodB5Name, nodeName, 5, customClassB.Name),
				getGracePeriodOverrideTestPod(periodA5Name, nodeName, 5, customClassA.Name),
				getGracePeriodOverrideTestPod(periodCritical5Name, nodeName, 5, scheduling.SystemNodeCritical),
			}

			// 预期的关停顺序
			downSteps := [][]string{
				{
					period5Name,
				},
				{
					period5Name,
					periodC5Name,
				},
				{

					period5Name,
					periodC5Name,
					periodB5Name,
				},
				{
					period5Name,
					periodC5Name,
					periodB5Name,
					periodA5Name,
				},
				{
					period5Name,
					periodC5Name,
					periodB5Name,
					periodA5Name,
					periodCritical5Name,
				},
			}

			ginkgo.By("Creating batch pods")
			e2epod.NewPodClient(f).CreateBatch(ctx, pods)

			list, err := e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
				FieldSelector: nodeSelector,
			})
			framework.ExpectNoError(err)
			gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")

			ginkgo.By("Verifying batch pods are running")
			for _, pod := range list.Items {
				if podReady, err := testutils.PodRunningReady(&pod); err != nil || !podReady {
					framework.Failf("Failed to start batch pod: (%v/%v)", pod.Namespace, pod.Name)
				}
			}

			ginkgo.By("Emitting shutdown signal")
			err = emitSignalPrepareForShutdown(true)
			framework.ExpectNoError(err)

			ginkgo.By("Verifying that pods are shutdown")

			for _, step := range downSteps {
				gomega.Eventually(ctx, func(ctx context.Context) error {
					list, err = e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{
						FieldSelector: nodeSelector,
					})
					if err != nil {
						return err
					}
					gomega.Expect(list.Items).To(gomega.HaveLen(len(pods)), "the number of pods is not as expected")
					for _, pod := range list.Items {
						shouldShutdown := false
						for _, podName := range step {
							if podName == pod.Name {
								shouldShutdown = true
								break
							}
						}
						if !shouldShutdown {
							if pod.Status.Phase != v1.PodRunning {
								framework.Logf("Expecting pod to be running, but it's not currently. Pod: (%v/%v), Pod Status Phase: %q, Pod Status Reason: %q", pod.Namespace, pod.Name, pod.Status.Phase, pod.Status.Reason)
								return fmt.Errorf("pod (%v/%v) should not be shutdown, phase: %s, reason: %s", pod.Namespace, pod.Name, pod.Status.Phase, pod.Status.Reason)
							}
						} else {
							if pod.Status.Reason != podShutdownReason {
								framework.Logf("Expecting pod to be shutdown, but it's not currently. Pod: (%v/%v), Pod Status Phase: %q, Pod Status Reason: %q", pod.Namespace, pod.Name, pod.Status.Phase, pod.Status.Reason)
								for _, item := range list.Items {
									framework.Logf("DEBUG %s, %s, %s", item.Name, item.Status.Phase, pod.Status.Reason)
								}
								return fmt.Errorf("pod (%v/%v) should be shutdown, reason: %s", pod.Namespace, pod.Name, pod.Status.Reason)
							}
						}
					}
					return nil
				}, podStatusUpdateTimeout, pollInterval).Should(gomega.Succeed())
			}

			ginkgo.By("should have state file")
			stateFile := "/var/lib/kubelet/graceful_node_shutdown_state"
			_, err = os.Stat(stateFile)
			framework.ExpectNoError(err)
		})
	})
})

func getPriorityClass(name string, value int32) *schedulingv1.PriorityClass {
	priority := &schedulingv1.PriorityClass{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PriorityClass",
			APIVersion: "scheduling.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Value: value,
	}
	return priority
}

// getGracePeriodOverrideTestPod 构造一个 Pod，其容器运行 shell 脚本并挂起进程直到收到 SIGTERM。
// 脚本等待 $PID 消失，以确认进程确实退出。
// 当 priorityClassName 为 scheduling.SystemNodeCritical 时，将该 Pod 标记为关键 Pod 并添加注解。
func getGracePeriodOverrideTestPod(name string, node string, gracePeriod int64, priorityClassName string) *v1.Pod {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    name,
					Image:   busyboxImage,
					Command: []string{"sh", "-c"},
					Args: []string{`
					sleep 9999999 &
					PID=$!
					_term() {
						echo "Caught SIGTERM signal!"
						wait $PID
					}
					
					trap _term SIGTERM
					wait $PID
					`},
				},
			},
			TerminationGracePeriodSeconds: &gracePeriod,
			NodeName:                      node,
		},
	}
	if priorityClassName == scheduling.SystemNodeCritical {
		pod.ObjectMeta.Annotations = map[string]string{
			kubelettypes.ConfigSourceAnnotationKey: kubelettypes.FileSource,
		}
		pod.Spec.PriorityClassName = priorityClassName
		if !kubelettypes.IsCriticalPod(pod) {
			framework.Failf("pod %q should be a critical pod", pod.Name)
		}
	} else {
		pod.Spec.PriorityClassName = priorityClassName
		if kubelettypes.IsCriticalPod(pod) {
			framework.Failf("pod %q should not be a critical pod", pod.Name)
		}
	}
	return pod
}

// 在系统 dbus 上发送伪造的 PrepareForShutdown 消息，使 kubelet 认为发生了关停事件并作出响应。
func emitSignalPrepareForShutdown(b bool) error {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Emit("/org/freedesktop/login1", "org.freedesktop.login1.Manager.PrepareForShutdown", b)
}

const (
	// https://github.com/kubernetes/kubernetes/blob/1dd781ddcad454cc381806fbc6bd5eba8fa368d7/pkg/kubelet/nodeshutdown/nodeshutdown_manager_linux.go#L43-L44
	podShutdownReason  = "Terminated"
	podShutdownMessage = "Pod was terminated in response to imminent node shutdown."
)

func isPodShutdown(pod *v1.Pod) bool {
	if pod == nil {
		return false
	}

	hasContainersNotReadyCondition := false
	for _, cond := range pod.Status.Conditions {
		if cond.Type == v1.ContainersReady && cond.Status == v1.ConditionFalse {
			hasContainersNotReadyCondition = true
		}
	}

	return pod.Status.Message == podShutdownMessage && pod.Status.Reason == podShutdownReason && hasContainersNotReadyCondition && pod.Status.Phase == v1.PodFailed
}

// Pod 不应同时处于 Failed 阶段且 ready 条件为 true（https://github.com/kubernetes/kubernetes/issues/108594）
func isPodStatusAffectedByIssue108594(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodFailed && podutils.IsPodReady(pod)
}

func isPodReadyToStartConditionSetToFalse(pod *v1.Pod) bool {
	if pod == nil {
		return false
	}
	readyToStartConditionSetToFalse := false
	for _, cond := range pod.Status.Conditions {
		if cond.Status == v1.ConditionFalse {
			readyToStartConditionSetToFalse = true
		}
	}

	return readyToStartConditionSetToFalse
}

func expectNodeLease(lease *coordinationv1.Lease, nodeName string) error {
	if lease.Spec.HolderIdentity == nil {
		return fmt.Errorf("Spec.HolderIdentity should not be nil")
	}
	if lease.Spec.LeaseDurationSeconds == nil {
		return fmt.Errorf("Spec.LeaseDurationSeconds should not be nil")
	}
	if lease.Spec.RenewTime == nil {
		return fmt.Errorf("Spec.RenewTime should not be nil")
	}
	// Node e2e 运行在单节点环境中，因此 lease 持有者应始终是被测试的唯一节点。
	if *lease.Spec.HolderIdentity != nodeName {
		return fmt.Errorf("Spec.HolderIdentity (%v) should match the node name (%v)", *lease.Spec.HolderIdentity, nodeName)
	}
	return nil
}

func expectNodeReadyCondition(node *v1.Node, expectedStatus v1.ConditionStatus, expectedReason string) error {
	for _, condition := range node.Status.Conditions {
		if condition.Type != v1.NodeReady {
			continue
		}
		if condition.Status != expectedStatus {
			return fmt.Errorf("NodeReady status=%q, want %q", condition.Status, expectedStatus)
		}
		if condition.Reason != expectedReason {
			return fmt.Errorf("NodeReady reason=%q, want %q", condition.Reason, expectedReason)
		}
		return nil
	}
	return fmt.Errorf("NodeReady condition not found")
}
