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

package csimock

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eevents "k8s.io/kubernetes/test/e2e/framework/events"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

// SELinuxMount 特性的测试。
// 这些测试带有显式 [Feature:SELinux]，除非显式请求否则跳过。
// 此文件中的所有测试都假设所有 worker 节点启用了 SELinux。
// 通过 --node-os-distro 传入的受支持节点操作系统有 "debian"、"ubuntu" 和 "custom"。
// "custom" 预期是 Fedora Linux 衍生版（RHEL、CentOS、Rocky、Alma 等）
//（欢迎为更多发行版提交补丁，作者无法在 "gci" 上测试 SELinux —— 它到底支持吗？）
//
// KEP: https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1710-selinux-relabeling
// 有三个 feature gate：SELinuxMountReadWriteOncePod、SELinuxChangePolicy 和 SELinuxMount。
// 测试中使用了这些标签：
//
// [FeatureGate:SELinuxMountReadWriteOncePod]
//   - 测试要求启用 SELinuxMountReadWriteOncePod。
//
// [FeatureGate:SELinuxMountReadWriteOncePod] [Feature:SELinuxMountReadWriteOncePodOnly]
//   - 测试要求启用 SELinuxMountReadWriteOncePod 且禁用 SELinuxMount，检查仅在禁用时发出的指标。
//
// [FeatureGate:SELinuxMountReadWriteOncePod] [Feature:SELinuxMountReadWriteOncePodOnly] [Feature:SELinuxChangePolicy]
//   - 测试要求启用前两者且禁用 SELinuxMount，检查仅在禁用时发出的指标。
//
// [FeatureGate:SELinuxMountReadWriteOncePod] [Feature:SELinuxChangePolicy] [FeatureGate:SELinuxMount]
//   - 测试要求启用 SELinuxMountReadWriteOncePod、Feature:SELinuxChangePolicy 和 SELinuxMount。
//
// 其他所有 feature gate 组合都应无效。

const (
	controllerSELinuxMetricName = "selinux_warning_controller_selinux_volume_conflict"
)

var (
	defaultSELinuxLabels = map[string]struct{ defaultProcessLabel, defaultFileLabel string }{
		"debian": {"svirt_lxc_net_t", "svirt_lxc_file_t"},
		"ubuntu": {"svirt_lxc_net_t", "svirt_lxc_file_t"},
		// 假定 "custom" 表示 Fedora 及其衍生版。`e2e.test --node-os-distro=` 没有 "fedora" 或 "rhel"。
		"custom": {"container_t", "container_file_t"},
	}
)

var _ = utils.SIGDescribe("CSI Mock selinux on mount", func() {
	f := framework.NewDefaultFramework("csi-mock-volumes-selinux")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	m := newMockDriverSetup(f)
	recursive := v1.SELinuxChangePolicyRecursive
	mount := v1.SELinuxChangePolicyMountOption
	f.Context("SELinuxMount [LinuxOnly]", feature.SELinux, func() {
		processLabel, fileLabel := getDefaultContainerSELinuxLabels()
		// 确保所有选项都已设置，以免使用系统特定的默认值。
		seLinuxOpts1 := v1.SELinuxOptions{
			User:  "system_u",
			Role:  "system_r",
			Type:  processLabel,
			Level: "s0:c0,c1",
		}
		seLinuxMountOption1 := fmt.Sprintf("context=\"system_u:object_r:%s:s0:c0,c1\"", fileLabel)
		seLinuxOpts2 := v1.SELinuxOptions{
			User:  "system_u",
			Role:  "system_r",
			Type:  processLabel,
			Level: "s0:c98,c99",
		}
		seLinuxMountOption2 := fmt.Sprintf("context=\"system_u:object_r:%s:s0:c98,c99\"", fileLabel)

		tests := []struct {
			name                       string
			csiDriverSELinuxEnabled    bool
			firstPodSELinuxOpts        *v1.SELinuxOptions
			firstPodChangePolicy       *v1.PodSELinuxChangePolicy
			startSecondPod             bool
			secondPodSELinuxOpts       *v1.SELinuxOptions
			secondPodChangePolicy      *v1.PodSELinuxChangePolicy
			mountOptions               []string
			volumeMode                 v1.PersistentVolumeAccessMode
			expectedFirstMountOptions  []string
			expectedSecondMountOptions []string
			expectedUnstage            bool
			testTags                   []interface{}
		}{
			// 只启动一个 Pod 并检查其 volume 是否正确挂载
			{
				name:                      "should pass SELinux mount option for RWOP volume and Pod with SELinux context set",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				volumeMode:                v1.ReadWriteOncePod,
				expectedFirstMountOptions: []string{seLinuxMountOption1},
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod)},
			},
			{
				name:                      "should add SELinux mount option to existing mount options",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				mountOptions:              []string{"noexec", "noatime"},
				volumeMode:                v1.ReadWriteOncePod,
				expectedFirstMountOptions: []string{"noexec", "noatime", seLinuxMountOption1},
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod)},
			},
			{
				name:                      "should not pass SELinux mount option for RWO volume with SELinuxMount disabled",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				volumeMode:                v1.ReadWriteOnce,
				expectedFirstMountOptions: nil,
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), feature.SELinuxMountReadWriteOncePodOnly},
			},
			{
				name:                      "should not pass SELinux mount option for RWO volume with only SELinuxChangePolicy enabled",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				volumeMode:                v1.ReadWriteOnce,
				expectedFirstMountOptions: nil,
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), feature.SELinuxMountReadWriteOncePodOnly, framework.WithFeatureGate(features.SELinuxChangePolicy)},
			},
			{
				name:                      "should pass SELinux mount option for RWO volume with SELinuxMount enabled and nil policy",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				volumeMode:                v1.ReadWriteOnce,
				expectedFirstMountOptions: []string{seLinuxMountOption1},
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), framework.WithFeatureGate(features.SELinuxChangePolicy), framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                      "should pass SELinux mount option for RWO volume with SELinuxMount enabled and MountOption policy",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				firstPodChangePolicy:      &mount,
				volumeMode:                v1.ReadWriteOnce,
				expectedFirstMountOptions: []string{seLinuxMountOption1},
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), framework.WithFeatureGate(features.SELinuxChangePolicy), framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                      "should not pass SELinux mount option for RWO volume with SELinuxMount disabled and Recursive policy",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				firstPodChangePolicy:      &recursive,
				volumeMode:                v1.ReadWriteOnce,
				expectedFirstMountOptions: nil,
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), framework.WithFeatureGate(features.SELinuxChangePolicy), framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                      "should not pass SELinux mount option for Pod without SELinux context",
				csiDriverSELinuxEnabled:   true,
				firstPodSELinuxOpts:       nil,
				volumeMode:                v1.ReadWriteOncePod,
				expectedFirstMountOptions: nil,
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod)},
			},
			{
				name:                      "should not pass SELinux mount option for CSI driver that does not support SELinux mount",
				csiDriverSELinuxEnabled:   false,
				firstPodSELinuxOpts:       &seLinuxOpts1,
				volumeMode:                v1.ReadWriteOncePod,
				expectedFirstMountOptions: nil,
				testTags:                  []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod)},
			},
			// 依次启动两个 Pod，检查其 volume 在两者之间是否被卸载
			{
				name:                       "should not unstage RWOP volume when starting a second pod with the same SELinux context",
				csiDriverSELinuxEnabled:    true,
				firstPodSELinuxOpts:        &seLinuxOpts1,
				startSecondPod:             true,
				secondPodSELinuxOpts:       &seLinuxOpts1,
				volumeMode:                 v1.ReadWriteOncePod,
				expectedFirstMountOptions:  []string{seLinuxMountOption1},
				expectedSecondMountOptions: []string{seLinuxMountOption1},
				expectedUnstage:            false,
				testTags:                   []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod)},
			},
			{
				name:                       "should unstage RWOP volume when starting a second pod with different SELinux context",
				csiDriverSELinuxEnabled:    true,
				firstPodSELinuxOpts:        &seLinuxOpts1,
				startSecondPod:             true,
				secondPodSELinuxOpts:       &seLinuxOpts2,
				volumeMode:                 v1.ReadWriteOncePod,
				expectedFirstMountOptions:  []string{seLinuxMountOption1},
				expectedSecondMountOptions: []string{seLinuxMountOption2},
				expectedUnstage:            true,
				testTags:                   []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod)},
			},
			{
				name:                       "should not unstage RWO volume when starting a second pod with the same SELinux context",
				csiDriverSELinuxEnabled:    true,
				firstPodSELinuxOpts:        &seLinuxOpts1,
				startSecondPod:             true,
				secondPodSELinuxOpts:       &seLinuxOpts1,
				volumeMode:                 v1.ReadWriteOnce,
				expectedFirstMountOptions:  []string{seLinuxMountOption1},
				expectedSecondMountOptions: []string{seLinuxMountOption1},
				expectedUnstage:            false,
				testTags:                   []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                       "should unstage RWO volume when starting a second pod with different SELinux context",
				csiDriverSELinuxEnabled:    true,
				firstPodSELinuxOpts:        &seLinuxOpts1,
				startSecondPod:             true,
				secondPodSELinuxOpts:       &seLinuxOpts2,
				volumeMode:                 v1.ReadWriteOnce,
				expectedFirstMountOptions:  []string{seLinuxMountOption1},
				expectedSecondMountOptions: []string{seLinuxMountOption2},
				expectedUnstage:            true,
				testTags:                   []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                       "should unstage RWO volume when starting a second pod with different policy (MountOption -> Recursive)",
				csiDriverSELinuxEnabled:    true,
				firstPodSELinuxOpts:        &seLinuxOpts1,
				firstPodChangePolicy:       &mount,
				startSecondPod:             true,
				secondPodSELinuxOpts:       &seLinuxOpts2,
				secondPodChangePolicy:      &recursive,
				volumeMode:                 v1.ReadWriteOnce,
				expectedFirstMountOptions:  []string{seLinuxMountOption1},
				expectedSecondMountOptions: nil,
				expectedUnstage:            true,
				testTags:                   []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                       "should unstage RWO volume when starting a second pod with different policy (Recursive -> MountOption)",
				csiDriverSELinuxEnabled:    true,
				firstPodSELinuxOpts:        &seLinuxOpts1,
				firstPodChangePolicy:       &recursive,
				startSecondPod:             true,
				secondPodSELinuxOpts:       &seLinuxOpts2,
				secondPodChangePolicy:      &mount,
				volumeMode:                 v1.ReadWriteOnce,
				expectedFirstMountOptions:  nil,
				expectedSecondMountOptions: []string{seLinuxMountOption2},
				expectedUnstage:            true,
				testTags:                   []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod), framework.WithFeatureGate(features.SELinuxMount)},
			},
		}
		for _, t := range tests {
			testFunc := func(ctx context.Context) {
				if processLabel == "" {
					e2eskipper.Skipf("SELinux tests are supported only on %+v", getSupportedSELinuxDistros())
				}

				var nodeStageMountOpts, nodePublishMountOpts []string
				var unstageCalls, stageCalls, unpublishCalls, publishCalls atomic.Int32
				m.init(ctx, testParameters{
					disableAttach:      true,
					registerDriver:     true,
					enableSELinuxMount: &t.csiDriverSELinuxEnabled,
					hooks:              createSELinuxMountPreHook(&nodeStageMountOpts, &nodePublishMountOpts, &stageCalls, &unstageCalls, &publishCalls, &unpublishCalls),
				})
				ginkgo.DeferCleanup(m.cleanup)

				// Act
				ginkgo.By("Starting the initial pod")
				accessModes := []v1.PersistentVolumeAccessMode{t.volumeMode}
				_, claim, pod := m.createPodWithSELinux(ctx, accessModes, t.mountOptions, t.firstPodSELinuxOpts, t.firstPodChangePolicy, false /* 特权 */, sleepCommand)
				err := e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod.Name, pod.Namespace)
				framework.ExpectNoError(err, "starting the initial pod")

				// Assert
				ginkgo.By("Checking the initial pod mount options")
				gomega.Expect(nodeStageMountOpts).To(gomega.Equal(t.expectedFirstMountOptions), "NodeStage MountFlags for the initial pod")
				gomega.Expect(nodePublishMountOpts).To(gomega.Equal(t.expectedFirstMountOptions), "NodePublish MountFlags for the initial pod")

				ginkgo.By("Checking the CSI driver calls for the initial pod")
				gomega.Expect(unstageCalls.Load()).To(gomega.BeNumerically("==", 0), "NodeUnstage call count for the initial pod")
				gomega.Expect(unpublishCalls.Load()).To(gomega.BeNumerically("==", 0), "NodeUnpublish call count for the initial pod")
				gomega.Expect(stageCalls.Load()).To(gomega.BeNumerically(">", 0), "NodeStage for the initial pod")
				gomega.Expect(publishCalls.Load()).To(gomega.BeNumerically(">", 0), "NodePublish for the initial pod")

				if !t.startSecondPod {
					return
				}

				// 安排测试的第二部分
				ginkgo.By("Starting the second pod to check if a volume used by the initial pod is / is not unmounted based on SELinux context")
				// 统计第一个和第二个 Pod 之间新的 CSI driver 调用
				nodeStageMountOpts = nil
				nodePublishMountOpts = nil
				unstageCalls.Store(0)
				unpublishCalls.Store(0)
				stageCalls.Store(0)
				publishCalls.Store(0)

				// 跳过 scheduler，否则它会阻止使用 ReadWriteOncePod PV 调度第二个 Pod。
				pod, err = m.cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "getting the initial pod")
				nodeSelection := e2epod.NodeSelection{Name: pod.Spec.NodeName}
				pod2, err := startPausePodWithSELinuxOptions(
					f.ClientSet,
					claim,
					nodeSelection,
					f.Namespace.Name,
					t.secondPodSELinuxOpts,
					t.secondPodChangePolicy,
					false, /* 特权 */
					sleepCommand)
				framework.ExpectNoError(err, "creating second pod with SELinux context %s", t.secondPodSELinuxOpts)
				m.pods = append(m.pods, pod2)

				// 仅在 kubelet 处理完第二个 Pod 并将其 volume 加入后才删除初始 Pod
				// DesiredStateOfWorld.
				// 在这种状态下，任何 volume UnPublish / UnStage 都必须因 SELinux 上下文而执行，而不是
				// 由于随机竞争，第二个 Pod 的 volume 尚未进入 DesiredStateOfWorld。
				ginkgo.By("Waiting for the second pod to start (or fail to start because of ReadWriteOncePod).")
				reason := events.FailedMountVolume
				var msg string
				if t.expectedUnstage {
					// 此消息在 kubelet 检查 ReadWriteOncePod 之前发出
					msg = "conflicting SELinux labels of volume"
				} else {
					// Kubelet 应复用已 stage 的 volume。
					if t.volumeMode == v1.ReadWriteOncePod {
						// 等待第二个 Pod 因 RWOP 而卡住。
						msg = "volume uses the ReadWriteOncePod access mode and is already in use by another pod"
					} else {
						// 没有什么阻止第二个 Pod 启动，等待它完全启动。
						reason = string(events.StartedContainer)
						msg = "" // 该消息在 Kubernetes 1.35 中已改变，Reason 必须足够。
					}
				}
				eventSelector := fields.Set{
					"involvedObject.kind":      "Pod",
					"involvedObject.name":      pod2.Name,
					"involvedObject.namespace": pod2.Namespace,
					"reason":                   reason,
					"source":                   "kubelet",
				}.AsSelector().String()
				err = e2eevents.WaitTimeoutForEvent(ctx, m.cs, pod2.Namespace, eventSelector, msg, f.Timeouts.PodStart)
				framework.ExpectNoError(err, "waiting for event %q in the second test pod", msg)

				// 执行测试的第二部分
				ginkgo.By("Deleting the initial pod")
				err = e2epod.DeletePodWithWait(ctx, m.cs, pod)
				framework.ExpectNoError(err, "deleting the initial pod")

				// 断言测试的第二部分
				ginkgo.By("Waiting for the second pod to start")
				err = e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod2.Name, pod2.Namespace)
				framework.ExpectNoError(err, "starting the second pod")

				ginkgo.By("Checking CSI driver calls for the second pod")
				if t.expectedUnstage {
					// 第一个和第二个 Pod 之间 volume 应被完全 unstage
					gomega.Expect(unstageCalls.Load()).To(gomega.BeNumerically(">", 0), "NodeUnstage calls after the first pod is deleted")
					gomega.Expect(stageCalls.Load()).To(gomega.BeNumerically(">", 0), "NodeStage calls for the second pod")
					// 第二个 Pod 获得了正确的挂载选项
					gomega.Expect(nodeStageMountOpts).To(gomega.Equal(t.expectedSecondMountOptions), "NodeStage MountFlags for the second pod")
				} else {
					// 第一个和第二个 Pod 之间 volume 不应被完全 unstage
					gomega.Expect(unstageCalls.Load()).To(gomega.BeNumerically("==", 0), "NodeUnstage calls after the first pod is deleted")
					gomega.Expect(stageCalls.Load()).To(gomega.BeNumerically("==", 0), "NodeStage calls for the second pod")
				}
				// 两种情况下都会调用 Unpublish 和 Publish，且挂载选项正确
				gomega.Expect(unpublishCalls.Load()).To(gomega.BeNumerically(">", 0), "NodeUnpublish calls after the first pod is deleted")
				gomega.Expect(publishCalls.Load()).To(gomega.BeNumerically(">", 0), "NodePublish calls for the second pod")
				gomega.Expect(nodePublishMountOpts).To(gomega.Equal(t.expectedSecondMountOptions), "NodePublish MountFlags for the second pod")
			}
			// t.testTags 是数组，不能直接使用 It("name", func(){}, t.testTags...)
			// 需单独组合 It() 的参数。
			args := []interface{}{
				t.name,
				testFunc,
			}
			args = append(args, t.testTags...)
			framework.It(args...)
		}
	})
})

var (
	// 带 volume_plugin 和 access_mode 标签的 SELinux 指标
	metricsWithVolumePluginLabel = sets.New[string](
		"volume_manager_selinux_volume_context_mismatch_errors_total",
		"volume_manager_selinux_volume_context_mismatch_warnings_total",
		"volume_manager_selinux_volumes_admitted_total",
	)
	// 仅有 access_mode 标签的 SELinux 指标
	metricsWithoutVolumePluginLabel = sets.New[string](
		"volume_manager_selinux_container_errors_total",
		"volume_manager_selinux_container_warnings_total",
		"volume_manager_selinux_pod_context_mismatch_errors_total",
		"volume_manager_selinux_pod_context_mismatch_warnings_total",
	)
	// 所有 SELinux 指标
	allSELinuxMetrics = metricsWithoutVolumePluginLabel.Union(metricsWithVolumePluginLabel)
)

// kubelet VolumeManager 和 KCM SELinuxWarningController 是不同的组件，
// 但它们的测试设置完全相同，因此在这里一起测试。
var _ = utils.SIGDescribe("CSI Mock selinux on mount metrics and SELinuxWarningController", func() {
	f := framework.NewDefaultFramework("csi-mock-volumes-selinux-metrics")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	m := newMockDriverSetup(f)

	// [Serial]：这些测试读取全局节点指标，因此没有其他测试并行修改它们。
	f.Context("SELinuxMount metrics [LinuxOnly]", feature.SELinux, f.WithSerial(), func() {
		processLabel, _ := getDefaultContainerSELinuxLabels()
		// 确保所有选项都已设置，以免使用系统特定的默认值。
		seLinuxOpts1 := v1.SELinuxOptions{
			User:  "system_u",
			Role:  "system_r",
			Type:  processLabel,
			Level: "s0:c0,c1",
		}
		seLinuxOpts2 := v1.SELinuxOptions{
			User:  "system_u",
			Role:  "system_r",
			Type:  processLabel,
			Level: "s0:c98,c99",
		}
		recursive := v1.SELinuxChangePolicyRecursive
		mount := v1.SELinuxChangePolicyMountOption

		tests := []struct {
			name                             string
			csiDriverSELinuxEnabled          bool
			firstPodSELinuxOpts              *v1.SELinuxOptions
			firstPodChangePolicy             *v1.PodSELinuxChangePolicy
			firstPodPrivileged               bool
			firstPodTargetPhase              v1.PodPhase // 创建第二个 Pod 前第一个 Pod 应达到的阶段。空值表示 Running
			secondPodSELinuxOpts             *v1.SELinuxOptions
			secondPodChangePolicy            *v1.PodSELinuxChangePolicy
			secondPodPrivileged              bool
			volumeMode                       v1.PersistentVolumeAccessMode
			waitForSecondPodStart            bool
			secondPodFailureEvent            string
			expectNodeIncreases              sets.Set[string] // 用于测试 kubelet 指标
			expectControllerConflictProperty string           // 用于测试 SELinuxWarningController 指标 + 事件
			testTags                         []interface{}    // SELinuxMountReadWriteOncePod 和 SELinuxChangePolicy 总是自动添加
		}{
			{
				name:                    "warning is not bumped on two Pods with the same context on RWO volume",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				secondPodSELinuxOpts:    &seLinuxOpts1,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{feature.SELinuxMountReadWriteOncePodOnly},
			},
			{
				name:                             "warning is bumped on two Pods with a different context on RWO volume",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				secondPodSELinuxOpts:             &seLinuxOpts2,
				volumeMode:                       v1.ReadWriteOnce,
				waitForSecondPodStart:            true,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_warnings_total"),
				expectControllerConflictProperty: "SELinuxLabel",
				testTags:                         []interface{}{feature.SELinuxMountReadWriteOncePodOnly},
			},
			{
				name:                             "warning is bumped on two Pods with different policies on RWO volume",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             nil,
				secondPodSELinuxOpts:             &seLinuxOpts1,
				secondPodChangePolicy:            &recursive,
				volumeMode:                       v1.ReadWriteOnce,
				waitForSecondPodStart:            true,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_warnings_total"),
				expectControllerConflictProperty: "SELinuxChangePolicy",
				testTags:                         []interface{}{feature.SELinuxMountReadWriteOncePodOnly},
			},
			{
				name:                             "warning is not bumped on two Pods with Recursive policy and a different context on RWO volume",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             &recursive,
				secondPodSELinuxOpts:             &seLinuxOpts2,
				secondPodChangePolicy:            &recursive,
				volumeMode:                       v1.ReadWriteOnce,
				waitForSecondPodStart:            true,
				expectNodeIncreases:              sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				expectControllerConflictProperty: "", /* SELinuxController 也未发出任何警告 */
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxChangePolicy), feature.SELinuxMountReadWriteOncePodOnly},
			},
			{
				name:                    "error is not bumped on two Pods with the same context on RWO volume and SELinuxMount enabled",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				secondPodSELinuxOpts:    &seLinuxOpts1,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on two Pods with a different context on RWO volume and SELinuxMount enabled",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				secondPodSELinuxOpts:             &seLinuxOpts2,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOnce,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxLabel",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on two Pods with a different policy on RWO volume and SELinuxMount enabled (nil + Recursive)",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             nil,
				secondPodSELinuxOpts:             &seLinuxOpts1,
				secondPodChangePolicy:            &recursive,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOnce,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxChangePolicy",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on two Pods with a different policy on RWO volume and SELinuxMount enabled (Recursive + nil)",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             &recursive,
				secondPodSELinuxOpts:             &seLinuxOpts1,
				secondPodChangePolicy:            nil,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOnce,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxChangePolicy",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on two Pods with a different policy on RWO volume and SELinuxMount enabled (Recursive + MountOption)",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             &recursive,
				secondPodSELinuxOpts:             &seLinuxOpts1,
				secondPodChangePolicy:            &mount,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOnce,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxChangePolicy",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on two Pods with a different context on RWX volume and SELinuxMount enabled",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				secondPodSELinuxOpts:             &seLinuxOpts2,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteMany,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxLabel",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is not bumped on two Pods with Recursive policy and a different context on RWX volume",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             &recursive,
				secondPodSELinuxOpts:             &seLinuxOpts2,
				secondPodChangePolicy:            &recursive,
				volumeMode:                       v1.ReadWriteMany,
				waitForSecondPodStart:            true,
				expectNodeIncreases:              sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				expectControllerConflictProperty: "", /* SELinuxController 也未发出任何警告 */
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "error is not bumped on two Pods with the same policy RWX volume (nil + MountOption)",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				firstPodChangePolicy:    &mount,
				secondPodSELinuxOpts:    &seLinuxOpts1,
				secondPodChangePolicy:   nil,
				volumeMode:              v1.ReadWriteMany,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "error is not bumped on two Pods with the same policy RWX volume (MountOption + MountOption)",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				firstPodChangePolicy:    &mount,
				secondPodSELinuxOpts:    &seLinuxOpts1,
				secondPodChangePolicy:   &mount,
				volumeMode:              v1.ReadWriteMany,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on two Pods with a different context on RWOP volume",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				secondPodSELinuxOpts:             &seLinuxOpts2,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOncePod,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxLabel",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod)},
			},
			{
				name:                             "error is bumped on two Pods with MountOption policy and a different context on RWOP volume",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             &mount,
				secondPodSELinuxOpts:             &seLinuxOpts2,
				secondPodChangePolicy:            &mount,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOncePod,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxLabel",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "error is not bumped on two privileged Pods with mount policy RWO volume",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     nil, /* privileged Pod 通常没有 SELinux 上下文 */
				firstPodPrivileged:      true,
				firstPodChangePolicy:    &recursive,
				secondPodSELinuxOpts:    nil, /* privileged Pod 通常没有 SELinux 上下文 */
				secondPodPrivileged:     true,
				secondPodChangePolicy:   &recursive,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "error is not bumped on two privileged Pods with recursive policy RWO volume",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     nil, /* privileged Pod 通常没有 SELinux 上下文 */
				firstPodPrivileged:      true,
				firstPodChangePolicy:    &mount,
				secondPodSELinuxOpts:    nil, /* privileged Pod 通常没有 SELinux 上下文 */
				secondPodPrivileged:     true,
				secondPodChangePolicy:   &mount,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "error is not bumped on a privileged and unprivileged Pod with given SELinux context and recursive policy",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     nil, /* privileged Pod 通常没有 SELinux 上下文 */
				firstPodPrivileged:      true,
				secondPodSELinuxOpts:    &seLinuxOpts1,
				secondPodChangePolicy:   &recursive,
				secondPodPrivileged:     false,
				volumeMode:              v1.ReadWriteMany,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on a privileged and unprivileged Pod with given SELinux with MountOption policy",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              nil, /* privileged Pod 通常没有 SELinux 上下文 */
				firstPodPrivileged:               true,
				secondPodSELinuxOpts:             &seLinuxOpts1,
				secondPodChangePolicy:            &mount,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOncePod,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxLabel",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                             "error is bumped on an unprivileged and privileged Pod with given SELinux with MountOption policy",
				csiDriverSELinuxEnabled:          true,
				firstPodSELinuxOpts:              &seLinuxOpts1,
				firstPodChangePolicy:             &mount,
				secondPodSELinuxOpts:             nil, /* privileged Pod 通常没有 SELinux 上下文 */
				secondPodPrivileged:              true,
				secondPodFailureEvent:            "conflicting SELinux labels of volume",
				volumeMode:                       v1.ReadWriteOncePod,
				waitForSecondPodStart:            false,
				expectNodeIncreases:              sets.New[string]("volume_manager_selinux_volume_context_mismatch_errors_total"),
				expectControllerConflictProperty: "SELinuxLabel",
				testTags:                         []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "error is not bumped on a finished Pod with a different context on RWO volume and SELinuxMount enabled",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				firstPodTargetPhase:     v1.PodSucceeded,
				secondPodSELinuxOpts:    &seLinuxOpts2,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				// 第一个和第二个 Pod 之间 volume 被卸载，因此 admitted_total 增加，
				expectNodeIncreases: sets.New[string]("volume_manager_selinux_volumes_admitted_total"),
				testTags:            []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "error is not bumped on a failed Pod with a different context on RWO volume and SELinuxMount enabled",
				csiDriverSELinuxEnabled: true,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				firstPodTargetPhase:     v1.PodFailed,
				secondPodSELinuxOpts:    &seLinuxOpts2,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				// 第一个和第二个 Pod 之间 volume 被卸载，因此 admitted_total 增加，
				expectNodeIncreases: sets.New[string]("volume_manager_selinux_volumes_admitted_total"),
				testTags:            []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "warning is not bumped on RWO volume with CSIDriver.SELinuxMount disabled and mismatched labels",
				csiDriverSELinuxEnabled: false,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				secondPodSELinuxOpts:    &seLinuxOpts2,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "warning is not bumped on RWX volume with CSIDriver.SELinuxMount disabled and mismatched labels",
				csiDriverSELinuxEnabled: false,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				secondPodSELinuxOpts:    &seLinuxOpts2,
				volumeMode:              v1.ReadWriteMany,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "warning is not bumped on RWO volume with CSIDriver.SELinuxMount disabled and mismatched policies",
				csiDriverSELinuxEnabled: false,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				firstPodChangePolicy:    &recursive,
				secondPodSELinuxOpts:    &seLinuxOpts1,
				secondPodChangePolicy:   &mount,
				volumeMode:              v1.ReadWriteOnce,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
			{
				name:                    "warning is not bumped on RWX volume with CSIDriver.SELinuxMount disabled and mismatched policies",
				csiDriverSELinuxEnabled: false,
				firstPodSELinuxOpts:     &seLinuxOpts1,
				firstPodChangePolicy:    &recursive,
				secondPodSELinuxOpts:    &seLinuxOpts1,
				secondPodChangePolicy:   &mount,
				volumeMode:              v1.ReadWriteMany,
				waitForSecondPodStart:   true,
				expectNodeIncreases:     sets.New[string]( /* 没有指标增加，admitted_total 在第一个 Pod 启动时已增加 */ ),
				testTags:                []interface{}{framework.WithFeatureGate(features.SELinuxMount)},
			},
		}
		for _, t := range tests {
			testFunc := func(ctx context.Context) {
				if processLabel == "" {
					e2eskipper.Skipf("SELinux tests are supported only on %+v", getSupportedSELinuxDistros())
				}
				if t.firstPodTargetPhase == "" {
					t.firstPodTargetPhase = v1.PodRunning
				}

				// 某些指标用 CSI driver 名称作标签，即 "csi-mock-" 加上命名空间名称。
				volumePluginLabel := "volume_plugin=\"kubernetes.io/csi/csi-mock-" + f.Namespace.Name + "\""
				grabber, err := e2emetrics.NewMetricsGrabber(ctx, f.ClientSet, nil, f.ClientConfig(), true /*kubelet*/, false /*scheduler*/, true /*controllers*/, false /*apiserver*/, false /*autoscaler*/, false /*snapshotController*/)
				framework.ExpectNoError(err, "creating the metrics grabber")

				var nodeStageMountOpts, nodePublishMountOpts []string
				var unstageCalls, stageCalls, unpublishCalls, publishCalls atomic.Int32
				m.init(ctx, testParameters{
					disableAttach:      true,
					registerDriver:     true,
					enableSELinuxMount: &t.csiDriverSELinuxEnabled,
					hooks:              createSELinuxMountPreHook(&nodeStageMountOpts, &nodePublishMountOpts, &stageCalls, &unstageCalls, &publishCalls, &unpublishCalls),
				})
				ginkgo.DeferCleanup(m.cleanup)

				ginkgo.By("Starting the first pod")
				accessModes := []v1.PersistentVolumeAccessMode{t.volumeMode}
				command := sleepCommand
				switch t.firstPodTargetPhase {
				case v1.PodSucceeded:
					command = []string{"/bin/true"}
				case v1.PodFailed:
					command = []string{"/bin/false"}
				}
				_, claim, pod := m.createPodWithSELinux(ctx, accessModes, []string{}, t.firstPodSELinuxOpts, t.firstPodChangePolicy, t.firstPodPrivileged, command)

				switch t.firstPodTargetPhase {
				case v1.PodRunning:
					err = e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod.Name, pod.Namespace)
					framework.ExpectNoError(err, "starting the initial pod")
				case v1.PodSucceeded, v1.PodFailed:
					ginkgo.By("Waiting for the first pod to complete")
					err = e2epod.WaitForPodNoLongerRunningInNamespace(ctx, m.cs, pod.Name, pod.Namespace)
					framework.ExpectNoError(err, "starting and completing the initial pod")
				}

				ginkgo.By("Grabbing initial metrics")
				pod, err = m.cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				framework.ExpectNoError(err, "getting the initial pod")
				metrics, err := grabNodeMetrics(ctx, grabber, pod.Spec.NodeName, allSELinuxMetrics, volumePluginLabel)
				framework.ExpectNoError(err, "collecting the initial metrics")
				dumpMetrics(metrics)

				// Act
				ginkgo.By("Starting the second pod")
				// 跳过 scheduler，否则它会阻止使用 ReadWriteOncePod PV 调度第二个 Pod。
				nodeSelection := e2epod.NodeSelection{Name: pod.Spec.NodeName}
				pod2, err := startPausePodWithSELinuxOptions(
					f.ClientSet,
					claim,
					nodeSelection,
					f.Namespace.Name,
					t.secondPodSELinuxOpts,
					t.secondPodChangePolicy,
					t.secondPodPrivileged,
					sleepCommand)
				framework.ExpectNoError(err, "creating second pod with SELinux context %s", t.secondPodSELinuxOpts)
				m.pods = append(m.pods, pod2)

				if t.waitForSecondPodStart {
					err := e2epod.WaitForPodNameRunningInNamespace(ctx, m.cs, pod2.Name, pod2.Namespace)
					framework.ExpectNoError(err, "starting the second pod")
				} else {
					ginkgo.By("Waiting for the second pod to fail to start")
					eventSelector := fields.Set{
						"involvedObject.kind":      "Pod",
						"involvedObject.name":      pod2.Name,
						"involvedObject.namespace": pod2.Namespace,
						"reason":                   events.FailedMountVolume,
					}.AsSelector().String()
					err = e2eevents.WaitTimeoutForEvent(ctx, m.cs, pod2.Namespace, eventSelector, t.secondPodFailureEvent, f.Timeouts.PodStart)
					framework.ExpectNoError(err, "waiting for event %q in the second test pod", t.secondPodFailureEvent)
				}

				// 断言：统计 kubelet 指标
				expectIncreaseWithLabels := addLabels(t.expectNodeIncreases, volumePluginLabel, t.volumeMode)
				framework.Logf("Waiting for changes of metrics %+v", expectIncreaseWithLabels)
				err = waitForNodeMetricIncrease(ctx, grabber, pod.Spec.NodeName, volumePluginLabel, allSELinuxMetrics, expectIncreaseWithLabels, metrics, framework.PodStartShortTimeout)
				framework.ExpectNoError(err, "waiting for metrics %s to increase", t.expectNodeIncreases)
				if t.expectControllerConflictProperty != "" {
					// 断言：统计 KCM 指标 + 事件
					// 无需比较 KCM 指标的初始值和最终值，
					// KCM 指标将 Pod 的命名空间+名称作为标签精确报告，指标值始终为 "1"。
					err = waitForControllerMetric(ctx, grabber, f.Namespace.Name, pod.Name, pod2.Name, t.expectControllerConflictProperty, framework.PodStartShortTimeout)
					framework.ExpectNoError(err, "while waiting for metrics from KCM")
					// 检查控制器在第一个 Pod 上生成了冲突事件
					err = waitForConflictEvent(ctx, m.cs, pod, pod2, t.expectControllerConflictProperty, f.Timeouts.PodStart)
					framework.ExpectNoError(err, "while waiting for an event on the first pod")
					// 检查控制器在第二个 Pod 上生成了事件
					err = waitForConflictEvent(ctx, m.cs, pod2, pod, t.expectControllerConflictProperty, f.Timeouts.PodStart)
					framework.ExpectNoError(err, "while waiting for an event on the second pod")
				} else {
					err := checkForNoConflictEvents(ctx, m.cs, pod, pod2)
					framework.ExpectNoError(err, "ensuring there are no SELinux conflict events")
				}
			}
			// t.testTags 是数组，不能直接使用 It("name", func(){xxx}, t.testTags...)
			// 需单独组合 It() 的参数。
			args := []interface{}{
				t.name,
				testFunc,
				framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod),
				framework.WithFeatureGate(features.SELinuxChangePolicy),
			}
			args = append(args, t.testTags...)
			framework.It(args...)
		}
	})
})

func grabNodeMetrics(ctx context.Context, grabber *e2emetrics.Grabber, nodeName string, metricNames sets.Set[string], volumePluginLabel string) (map[string]float64, error) {
	response, err := grabber.GrabFromKubelet(ctx, nodeName)
	framework.ExpectNoError(err)

	metrics := map[string]float64{}
	for _, samples := range response {
		if len(samples) == 0 {
			continue
		}
		// 对每个指标 + 标签组合，记住最后一个样本
		for i := range samples {
			// E.g. "volume_manager_selinux_pod_context_mismatch_errors_total"
			metricName := samples[i].Metric[testutil.MetricNameLabel]
			if metricNames.Has(string(metricName)) {
				// 例如 "volume_manager_selinux_pod_context_mismatch_errors_total{access_mode="RWOP",volume_plugin="kubernetes.io/csi/csi-mock-ns"}
				metricNameWithLabels := samples[i].Metric.String()
				// 过滤掉其他 volume plugin 的指标
				if strings.Contains(metricNameWithLabels, "volume_plugin=") && !strings.Contains(metricNameWithLabels, volumePluginLabel) {
					continue
				}
				// 覆盖之前的值，只保留最后一个。
				metrics[metricNameWithLabels] = float64(samples[i].Value)
			}
		}
	}

	return metrics, nil
}

func grabKCMSELinuxMetrics(ctx context.Context, grabber *e2emetrics.Grabber, namespace string) (map[string]float64, error) {
	response, err := grabber.GrabFromControllerManager(ctx)
	if err != nil {
		return nil, err
	}

	metrics := map[string]float64{}
	for _, samples := range response {
		if len(samples) == 0 {
			continue
		}
		// 对每个指标 + 标签组合，记住最后一个样本
		for i := range samples {
			// E.g. "selinux_warning_controller_selinux_volume_conflict"
			metricName := samples[i].Metric[testutil.MetricNameLabel]
			if metricName != controllerSELinuxMetricName {
				continue
			}

			metricNamespace := samples[0].Metric["pod1_namespace"]
			if string(metricNamespace) != namespace {
				continue
			}
			// 例如 selinux_warning_controller_selinux_volume_conflict{pod1_name="testpod-c1",pod1_namespace="default",pod1_value="system_u:object_r:container_file_t:s0:c0,c1",pod2_name="testpod-c2",pod2_namespace="default",pod2_value="system_u:object_r:container_file_t:s0:c0,c2",property="SELinuxLabel"} 1
			metricNameWithLabels := samples[i].Metric.String()
			// 覆盖之前的值，只保留最后一个。
			metrics[metricNameWithLabels] = float64(samples[i].Value)
		}
	}
	framework.Logf("KCM metrics")
	dumpMetrics(metrics)

	return metrics, nil
}

func waitForNodeMetricIncrease(ctx context.Context, grabber *e2emetrics.Grabber, nodeName string, volumePluginLabel string, allMetricNames, expectedIncreaseNames sets.Set[string], initialValues map[string]float64, timeout time.Duration) error {
	var noIncreaseMetrics sets.Set[string]
	var metrics map[string]float64

	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var err error
		metrics, err = grabNodeMetrics(ctx, grabber, nodeName, allMetricNames, volumePluginLabel)
		if err != nil {
			return false, err
		}

		noIncreaseMetrics = sets.New[string]()
		// 始终检查所有 SELinux 指标，确保其他指标没有意外增加。
		for name := range metrics {
			if expectedIncreaseNames.Has(name) {
				if metrics[name] <= initialValues[name] {
					noIncreaseMetrics.Insert(name)
				}
			} else {
				// 期望该指标保持稳定
				if initialValues[name] != metrics[name] {
					return false, fmt.Errorf("metric %s unexpectedly increased to %v", name, metrics[name])
				}
			}
		}
		return noIncreaseMetrics.Len() == 0, nil
	})

	ginkgo.By("Dumping final node metrics")
	dumpMetrics(metrics)

	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("timed out waiting for node metrics %v", noIncreaseMetrics.UnsortedList())
	}
	return err
}

func waitForControllerMetric(ctx context.Context, grabber *e2emetrics.Grabber, namespace, pod1Name, pod2Name, propertyName string, timeout time.Duration) error {
	var metrics map[string]float64

	expectLabels := []string{
		fmt.Sprintf("pod1_name=%q", pod1Name),
		fmt.Sprintf("pod2_name=%q", pod2Name),
		fmt.Sprintf("pod1_namespace=%q", namespace),
		fmt.Sprintf("pod2_namespace=%q", namespace),
		fmt.Sprintf("property=%q", propertyName),
	}
	framework.Logf("Waiting for KCM metric %s{%+v}", controllerSELinuxMetricName, expectLabels)

	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		var err error
		metrics, err = grabKCMSELinuxMetrics(ctx, grabber, namespace)
		if err != nil {
			return false, err
		}

		foundMatch := false
		for metric := range metrics {
			allLabelsMatched := true
			for _, expectedLabel := range expectLabels {
				if !strings.Contains(metric, expectedLabel) {
					allLabelsMatched = false
				}
			}
			if allLabelsMatched {
				foundMatch = true
			}
		}

		return foundMatch, nil
	})
	if errors.Is(err, e2emetrics.MetricsGrabbingDisabledError) {
		ginkgo.By("Cannot grab metrics from kube-controller-manager in this e2e job, skipping metrics checks")
		return nil
	}

	ginkgo.By("Dumping final KCM metrics")
	dumpMetrics(metrics)

	if err != nil {
		return fmt.Errorf("error waiting for KCM metrics %s{%+v}: %w", controllerSELinuxMetricName, expectLabels, err)
	}
	return err
}

func waitForConflictEvent(ctx context.Context, cs clientset.Interface, pod, otherPod *v1.Pod, expectControllerConflictProperty string, timeout time.Duration) error {
	eventSelector := fields.Set{
		"involvedObject.kind":      "Pod",
		"involvedObject.name":      pod.Name,
		"involvedObject.namespace": pod.Namespace,
		"reason":                   expectControllerConflictProperty + "Conflict",
	}.AsSelector().String()
	// msg 是完整事件消息的子串，不含实际 SELinux 标签（太长且多变）
	// 完整事件：SELinuxLabel "system_u:system_r:container_t:s0:c0,c1" 与 Pod pvc-volume-tester-djqqd 冲突，后者与本 Pod 使用同一 volume，其 SELinuxLabel 为 "system_u:system_r:container_t:s0:c98,c99"。如果两个 Pod 落在同一节点，只有一个能访问该 volume。
	msg := fmt.Sprintf("conflicts with pod %s that uses the same volume as this pod with %s", otherPod.Name, expectControllerConflictProperty)
	ginkgo.By(fmt.Sprintf("Waiting for the SELinux controller event on pod %q: %q", pod.Name, msg))
	return e2eevents.WaitTimeoutForEvent(ctx, cs, pod.Namespace, eventSelector, msg, timeout)
}

func checkForNoConflictEvents(ctx context.Context, cs clientset.Interface, pod, otherPod *v1.Pod) error {
	eventSelector := fields.Set{
		"involvedObject.kind":      "Pod",
		"involvedObject.name":      pod.Name,
		"involvedObject.namespace": pod.Namespace,
	}.AsSelector().String()
	options := metav1.ListOptions{FieldSelector: eventSelector}

	events, err := cs.CoreV1().Events(pod.Namespace).List(ctx, options)
	if err != nil {
		return fmt.Errorf("error getting events: %w", err)
	}

	msg := fmt.Sprintf("conflicts with pod %s that uses the same volume as this pod", otherPod.Name)
	ginkgo.By(fmt.Sprintf("Checking for the SELinux controller events on pod %q: %q", pod.Name, msg))
	for _, event := range events.Items {
		if strings.Contains(event.Message, msg) {
			return fmt.Errorf("conflict event found: %s", event.Message)
		}
	}
	return nil
}

func dumpMetrics(metrics map[string]float64) {
	// 按指标名排序打印指标，便于阅读
	keys := make([]string, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		framework.Logf("Metric %s: %v", key, metrics[key])
	}
}

// 根据当前测试用例为指标名添加标签
func addLabels(metricNames sets.Set[string], volumePluginLabel string, accessMode v1.PersistentVolumeAccessMode) sets.Set[string] {
	ret := sets.New[string]()
	accessModeShortString := helper.GetAccessModesAsString([]v1.PersistentVolumeAccessMode{accessMode})

	for metricName := range metricNames {
		var metricWithLabels string
		if metricsWithVolumePluginLabel.Has(metricName) {
			metricWithLabels = fmt.Sprintf("%s{access_mode=\"%s\", %s}", metricName, accessModeShortString, volumePluginLabel)
		} else {
			metricWithLabels = fmt.Sprintf("%s{access_mode=\"%s\"}", metricName, accessModeShortString)
		}

		ret.Insert(metricWithLabels)
	}

	return ret
}

func getDefaultContainerSELinuxLabels() (processLabel string, fileLabel string) {
	defaultLabels := defaultSELinuxLabels[framework.TestContext.NodeOSDistro]
	// 对未知发行版，此函数可能返回 ""！
	// 在这些发行版上应跳过 SELinux 测试的 ginkgo.It()。
	return defaultLabels.defaultProcessLabel, defaultLabels.defaultFileLabel
}

func getSupportedSELinuxDistros() []string {
	distros := make([]string, 0, len(defaultSELinuxLabels))
	for distro := range defaultSELinuxLabels {
		distros = append(distros, distro)
	}
	return distros
}
