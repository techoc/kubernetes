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

package e2enode

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

type cgroupTestParam struct {
	filename                  string
	manualValue               string
	altManualValue            string
	expectReconcileOnRestart  bool
	expectReconcilePeriodic   bool
	expectReconcileOnPodChurn bool
	comment                   string
}

// reconcileTrigger 描述一批手动设置的 cgroup 值如何被推回 Kubelet 的期望状态，
// 以及在该触发方式下每个属性适用哪种 reconciliation 预期。

type reconcileTrigger struct {
	label string
	// setup 在写入手动值之前运行，用于把 Pod 拉起来，
	// 使后续的删除能够触发 reconciliation。可为 nil。
	setup func(ctx context.Context)
	// fire 执行（可能引发）reconciliation 的动作。
	fire func(ctx context.Context)
	// shouldReconcile 报告给定属性是否预期会被该触发方式
	// 从手动值重置。
	shouldReconcile func(p cgroupTestParam) bool
}

var _ = SIGDescribe("Cgroup reconciliation", framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("cgroup-reconciliation")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		if !IsCgroup2UnifiedMode() {
			ginkgo.Skip("This test requires cgroups v2")
		}
		// Pod 根 cgroup 的 reconciliation 只有在 systemd driver 下才有意义：
		// 此时 UpdateQOSCgroups -> SetUnitProperties 会重置 slice 中未指定的属性。
		// 在 cgroupfs 上，对应的更新不写任何文件，
		// 因此这里编码的 reconciliation 预期不成立。
		if driver := getCgroupDriver(); driver != "systemd" {
			ginkgo.Skip(fmt.Sprintf("This test requires the systemd cgroup driver (detected %q)", driver))
		}
	})

	testCgroups := func(ctx context.Context, cgroupPath string, params []cgroupTestParam, trigger reconcileTrigger) {
		driver := getCgroupDriver()
		ginkgo.By(fmt.Sprintf("Running batch test for %d cgroups in %s (driver=%s, trigger=%s)", len(params), cgroupPath, driver, trigger.label))

		// 0. 确保 Kubelet 已启动并完成初始 reconciliation
		ginkgo.By("Ensuring Kubelet is started before capturing baseline")
		waitForKubeletToStart(ctx, f)

		// 0a. 确保节点处于静止状态：Pod 层级下任何位置都不得残留之前测试的 Pod cgroup。
		// 某个 Pod 完成 teardown 会让 systemd 重新实例化（祖先）root slice 并重置其
		// 值，从而破坏下面的 reconciliation 观察窗口。其他测试中的 DeleteSync
		// 只等待 API server，不等待 kubelet 完成节点上的清理，
		// 因此必须直接轮询 cgroup 路径。
		// 完成节点上的清理，因此必须直接轮询 cgroup 路径。
		ginkgo.By("Waiting for all pod cgroups from other tests to be removed")
		gomega.Eventually(ctx, func() ([]string, error) {
			return podCgroupDirs(cgroupPath)
		}).WithTimeout(f.Timeouts.PodDelete).WithPolling(2*time.Second).Should(gomega.BeEmpty(),
			"pod cgroups from other tests must be gone before verifying root cgroup reconciliation")

		// 0b. 在记录基线前运行触发方式特有的 setup（例如拉起 Pod 以便后续删除触发 reconciliation）。
		// 删除触发 reconciliation）之前执行。
		if trigger.setup != nil {
			trigger.setup(ctx)
		}

		type testState struct {
			filename        string
			filePath        string
			defaultValue    string
			effectiveManual string
			shouldReconcile bool
		}

		var states []testState
		for _, p := range params {
			filePath := filepath.Join(cgroupPath, p.filename)
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				framework.Logf("Skipping %s: file not found", p.filename)
				continue
			}

			// 1. 读取并记录初始值（由 Kubelet 稳定到期望状态）
			initialValueBytes, err := os.ReadFile(filePath)
			framework.ExpectNoError(err)
			defaultValue := strings.TrimSpace(string(initialValueBytes))
			framework.Logf("Initial (stabilized) value of %s: %s (%s)", filePath, defaultValue, p.comment)

			shouldReconcile := trigger.shouldReconcile(p)
			framework.Logf("Cgroup %s expectation: shouldReconcile=%v (trigger=%s, periodic=%v, restart=%v, podChurn=%v)",
				p.filename, shouldReconcile, trigger.label, p.expectReconcilePeriodic, p.expectReconcileOnRestart, p.expectReconcileOnPodChurn)

			// 2. 确定手动值。它必须与当前状态不同，才能观察到 reconciliation。
			effectiveManual := p.manualValue
			// 用精确匹配，避免子串造成误判（例如 "0-23" 包含 "0"）。
			if defaultValue == effectiveManual {
				effectiveManual = p.altManualValue
				framework.Logf("Manual value %s matches current state for %s; using alternative %s", p.manualValue, p.filename, effectiveManual)
			}

			states = append(states, testState{
				filename:        p.filename,
				filePath:        filePath,
				defaultValue:    defaultValue,
				effectiveManual: effectiveManual,
				shouldReconcile: shouldReconcile,
			})

			// 3. 设置手动值
			ginkgo.By(fmt.Sprintf("Setting manual value %s for %s", effectiveManual, filePath))
			err = os.WriteFile(filePath, []byte(effectiveManual), 0644)
			framework.ExpectNoError(err)
		}

		if len(states) == 0 {
			return
		}

		// 清理：把这批中所有文件的值恢复原状。
		defer func() {
			for _, s := range states {
				currentBytes, err := os.ReadFile(s.filePath)
				if err == nil && strings.TrimSpace(string(currentBytes)) != s.defaultValue {
					ginkgo.By(fmt.Sprintf("Cleanup: Restoring %s to original value %s (currently %s)", s.filePath, s.defaultValue, strings.TrimSpace(string(currentBytes))))
					if err := os.WriteFile(s.filePath, []byte(s.defaultValue), 0644); err != nil {
						framework.Logf("Warning: failed to restore %s: %v", s.filePath, err)
					}
				}
			}
		}()

		// 4. 触发 reconciliation（每批一次）
		ginkgo.By(fmt.Sprintf("Triggering reconciliation via %s", trigger.label))
		trigger.fire(ctx)

		// 5. 校验 —— 按预期分组，便于汇总报告
		var reconciledStates []testState
		var nonReconciledStates []testState
		for _, s := range states {
			if s.shouldReconcile {
				reconciledStates = append(reconciledStates, s)
			} else {
				nonReconciledStates = append(nonReconciledStates, s)
			}
		}

		// 5a. 校验应当 reconciliation 的那些（聚合 Eventually）
		if len(reconciledStates) > 0 {
			ginkgo.By(fmt.Sprintf("Polling for %d cgroups to be reconciled back", len(reconciledStates)))
			gomega.Eventually(ctx, func() []string {
				var failures []string
				for _, s := range reconciledStates {
					currentValueBytes, _ := os.ReadFile(s.filePath)
					currentValue := strings.TrimSpace(string(currentValueBytes))
					if currentValue == s.effectiveManual {
						failures = append(failures, fmt.Sprintf("%s (still at manual %s)", s.filename, s.effectiveManual))
					} else if !strings.Contains(currentValue, s.defaultValue) {
						framework.Logf("Notice: %s reconciled to %s (captured baseline: %s)", s.filename, currentValue, s.defaultValue)
					}
				}
				return failures
			}).WithTimeout(75*time.Second).WithPolling(2*time.Second).Should(gomega.BeEmpty(),
				"The following cgroups failed to reconcile back from manual values")
		}

		// 5b. 校验不应当 reconciliation 的那些（聚合 Consistently）
		if len(nonReconciledStates) > 0 {
			ginkgo.By(fmt.Sprintf("Verifying %d unmanaged cgroups stay at manual values (70s wait)", len(nonReconciledStates)))
			gomega.Consistently(ctx, func() []string {
				var failures []string
				for _, s := range nonReconciledStates {
					currentValueBytes, err := os.ReadFile(s.filePath)
					if err != nil {
						failures = append(failures, fmt.Sprintf("%s (read error: %v)", s.filename, err))
						continue
					}
					currentValue := strings.TrimSpace(string(currentValueBytes))
					if !strings.Contains(currentValue, s.effectiveManual) {
						failures = append(failures, fmt.Sprintf("%s (was reset! got %s, expected %s)", s.filename, currentValue, s.effectiveManual))
					}
				}
				return failures
			}).WithTimeout(70*time.Second).WithPolling(2*time.Second).Should(gomega.BeEmpty(),
				"The following cgroups were unexpectedly reconciled (reset to defaults)")
		}
	}

	ginkgo.It("should follow reconciliation rules (Periodic)", func(ctx context.Context) {
		// 没有显式触发：在校验窗口内等待时间流逝，
		// 借此检验 Kubelet 的周期性 QoS cgroup 更新循环。
		trigger := reconcileTrigger{
			label:           "periodic",
			fire:            func(ctx context.Context) {},
			shouldReconcile: func(p cgroupTestParam) bool { return p.expectReconcilePeriodic },
		}
		testCgroups(ctx, getPodRootCgroupPath(), getCgroupParams(), trigger)
	})

	ginkgo.It("should follow reconciliation rules (Restart)", func(ctx context.Context) {
		trigger := reconcileTrigger{
			label: "kubelet restart",
			fire: func(ctx context.Context) {
				ginkgo.By("Restarting Kubelet")
				restartKubelet(ctx, true)
				waitForKubeletToStart(ctx, f)
			},
			shouldReconcile: func(p cgroupTestParam) bool { return p.expectReconcileOnRestart },
		}
		testCgroups(ctx, getPodRootCgroupPath(), getCgroupParams(), trigger)
	})

	// Pod 生命周期事件经 killPod -> UpdateQOSCgroups（Pod 删除）和
	// pod admission -> UpdateQOSCgroups 汇聚，两者都会对 Pod 根 cgroup 调用
	// cgroupManager.Update。在 systemd driver 下，该 Update 发出 SetUnitProperties，
	// 重置 slice 上所有未指定的属性；在 cgroupfs 下，（空的）Update 不写文件，
	// 是空操作。本测试为每个 QoS class 创建一个 Pod 再全部删除，
	// 这样 Pod 变动就能确定性地触发 reconciliation，而不是依赖验证过程中
	// 恰好有遗留 Pod 终止。
	//
	ginkgo.It("should follow reconciliation rules (Pod Churn)", func(ctx context.Context) {
		var churnPods []*v1.Pod
		trigger := reconcileTrigger{
			label: "pod churn (create+delete guaranteed/burstable/besteffort pods)",
			setup: func(ctx context.Context) {
				churnPods = createQoSPods(ctx, f)
			},
			fire: func(ctx context.Context) {
				deleteQoSPods(ctx, f, churnPods)
			},
			shouldReconcile: func(p cgroupTestParam) bool { return p.expectReconcileOnPodChurn },
		}
		testCgroups(ctx, getPodRootCgroupPath(), getCgroupParams(), trigger)
	})
})

// createQoSPods 为每个 QoS class（Guaranteed、Burstable、BestEffort）各创建一个
// 运行中的 pause Pod 并返回，以便后续通过删除驱动 Kubelet 的 QoS cgroup reconciliation。

func createQoSPods(ctx context.Context, f *framework.Framework) []*v1.Pod {
	podClient := e2epod.NewPodClient(f)
	specs := []struct {
		qos       string
		resources v1.ResourceRequirements
	}{
		// Guaranteed：cpu 和 memory 的 requests == limits。
		{"guaranteed", getResourceRequirements(getResourceList("50m", "50Mi"), getResourceList("50m", "50Mi"))},
		// Burstable：设置了 requests，limits 更高。
		{"burstable", getResourceRequirements(getResourceList("50m", "50Mi"), getResourceList("100m", "100Mi"))},
		// BestEffort：没有 requests 或 limits。
		{"besteffort", v1.ResourceRequirements{}},
	}

	pods := make([]*v1.Pod, 0, len(specs))
	for _, s := range specs {
		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("cgroup-churn-%s-%s", s.qos, string(uuid.NewUUID())),
			},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyNever,
				Containers: []v1.Container{
					{
						Name:      "pause",
						Image:     imageutils.GetPauseImageName(),
						Resources: s.resources,
					},
				},
			},
		}
		ginkgo.By(fmt.Sprintf("Creating %s pod %s", s.qos, pod.Name))
		pods = append(pods, podClient.CreateSync(ctx, pod))
	}
	return pods
}

// deleteQoSPods 删除给定 Pod 并等待其终止。每次删除都会在 Pod 根 cgroup 上
// 触发 killPod -> UpdateQOSCgroups。
func deleteQoSPods(ctx context.Context, f *framework.Framework, pods []*v1.Pod) {
	podClient := e2epod.NewPodClient(f)
	for _, pod := range pods {
		ginkgo.By(fmt.Sprintf("Deleting pod %s to trigger QoS cgroup reconciliation on termination", pod.Name))
		podClient.DeleteSync(ctx, pod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
	}
}

// podCgroupDirs 遍历以 cgroupRoot 为根的整个 Pod cgroup 层级，
// 返回仍存在的所有 Pod 级 cgroup 目录。结果非空意味着某个 Pod（可能来自其他测试）
// 在节点上仍有 cgroup；其 teardown 会重新实例化 root slice，干扰 reconciliation
// 校验。
// 校验。
func podCgroupDirs(cgroupRoot string) ([]string, error) {
	var dirs []string
	err := filepath.WalkDir(cgroupRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// kubelet 清理时，Pod cgroup 目录可能在遍历过程中消失；
			// 把消失的条目当作 "不存在"。
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() && isPodCgroupName(d.Name()) {
			dirs = append(dirs, path)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil, nil
	}
	return dirs, err
}

// isPodCgroupName 报告某个 cgroup 目录基名是否为 Pod 级 cgroup
// （相对于 root 或 QoS 层 slice）。Pod cgroup 位于 pod root / QoS slice 下，
// 以 Pod UID 命名：
//   - systemd:  kubepods-pod<uid>.slice, kubepods-burstable-pod<uid>.slice, ...
//   - cgroupfs: pod<uid>
//
// QoS 层（kubepods-burstable.slice / kubepods-besteffort.slice、burstable /
// besteffort）以及 root 本身被有意排除。
func isPodCgroupName(name string) bool {
	if strings.HasSuffix(name, ".slice") {
		return strings.Contains(name, "-pod")
	}
	return strings.HasPrefix(name, "pod")
}

func getCgroupParams() []cgroupTestParam {
	isSystemd := getCgroupDriver() == "systemd"
	// Pod 变动驱动 UpdateQOSCgroups，它用（基本为空的）配置对 Pod 根 cgroup
	// 调用 cgroupManager.Update。在 cgroupfs 下该 Update 不写文件，所以什么都不会重置。
	// 在 systemd 下它发出 SetUnitProperties，重置 slice 上所有未指定属性。
	// 与 Kubelet 重启不同，Pod 变动不会重新运行 Node Allocatable 强制逻辑，
	// 因此即使是 Kubelet 管理的属性（cpu.weight、memory.max、pids.max、cpuset.cpus），
	// 也只会在 systemd 下（作为副作用）被重置，在 cgroupfs 下永远不会。
	// 也只会在 systemd 下（作为副作用）被重置，在 cgroupfs 下永远不会。
	return []cgroupTestParam{
		{
			filename:                  "cpu.max",
			manualValue:               "max 100000",
			altManualValue:            "100000 100000",
			expectReconcileOnRestart:  isSystemd,
			expectReconcilePeriodic:   false,
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Kubelet does not set a limit here; reconciliation on restart is a side-effect of systemd resetting unspecified properties to unbounded.",
		},
		{
			filename:                  "cpu.weight",
			manualValue:               "100",
			altManualValue:            "512",
			expectReconcileOnRestart:  true,
			expectReconcilePeriodic:   false, // Root cgroup 管理并非真正周期性（仅启动时）
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Node Allocatable enforcement sets this (via shares) to ensure the pod hierarchy gets its fair share of CPU.",
		},
		{
			filename:                  "memory.max",
			manualValue:               "1073741824", // 1Gi
			altManualValue:            "536870912",  // 512Mi
			expectReconcileOnRestart:  true,
			expectReconcilePeriodic:   false, // Root cgroup 管理并非真正周期性（仅启动时）
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Node Allocatable enforcement sets this to the Node Allocatable value to protect the host from pod aggregate usage.",
		},
		{
			filename:                  "pids.max",
			manualValue:               "1000",
			altManualValue:            "1234",
			expectReconcileOnRestart:  true,
			expectReconcilePeriodic:   false, // Root cgroup 管理并非真正周期性（仅启动时）
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Node Allocatable enforcement sets this to protect the node from PID exhaustion across all pods.",
		},
		{
			filename:                  "cpuset.cpus",
			manualValue:               "0",
			altManualValue:            "0-0", // 对 various 环境更稳健的备选值
			expectReconcileOnRestart:  true,
			expectReconcilePeriodic:   false, // Root cgroup 管理并非真正周期性（仅启动时）
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Node Allocatable enforcement sets this to restrict pods to the designated allocatable CPUs.",
		},
		{
			filename:                  "memory.min",
			manualValue:               "115343360", // 110Mi（页对齐）
			altManualValue:            "209715200", // 200Mi（页对齐）
			expectReconcileOnRestart:  isSystemd,
			expectReconcilePeriodic:   false,
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Kubelet skips memory.min management when MemoryQoS is disabled, but it is reconciled on systemd restart as a side-effect.",
		},
		{
			filename:                  "memory.high",
			manualValue:               "1073741824", // 1Gi
			altManualValue:            "536870912",  // 512Mi
			expectReconcileOnRestart:  isSystemd,
			expectReconcilePeriodic:   false,
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Not managed by Kubelet at the root level; reconciliation on restart is a side-effect of systemd resetting unspecified properties.",
		},
		{
			filename:                  "memory.oom.group",
			manualValue:               "1",
			altManualValue:            "0",
			expectReconcileOnRestart:  isSystemd,
			expectReconcilePeriodic:   false,
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Not managed by Kubelet at the root level; reconciliation on restart is a side-effect of systemd resetting unspecified properties.",
		},
		{
			filename:                  "io.weight",
			manualValue:               "200",
			altManualValue:            "150",
			expectReconcileOnRestart:  isSystemd,
			expectReconcilePeriodic:   false,
			expectReconcileOnPodChurn: isSystemd,
			comment:                   "Not managed by Kubelet; reconciliation on restart is a side-effect of systemd resetting unspecified properties.",
		},
	}
}

func getCgroupDriver() string {
	driver := "cgroupfs"
	source := "fallback"
	if kubeletCfg != nil && kubeletCfg.CgroupDriver != "" {
		driver = kubeletCfg.CgroupDriver
		source = "kubelet config"
	}
	framework.Logf("Detected cgroup driver: %s (source: %s)", driver, source)
	return driver
}

func getPodRootCgroupPath() string {
	// Kubelet root 在 cgroupfs 下是 "kubepods"，systemd 下是 "kubepods.slice"。
	// 先检查 Kubelet 配置，再回退到文件系统探测。
	driver := getCgroupDriver()

	var path string
	if driver == "systemd" {
		path = filepath.Join(cgroupRoot, "kubepods.slice")
	} else {
		path = filepath.Join(cgroupRoot, "kubepods")
	}

	// 再确认它是否存在，不存在则尝试另一个
	if _, err := os.Stat(path); err == nil {
		return path
	}

	// 回退探测
	for _, p := range []string{"kubepods.slice", "kubepods"} {
		path = filepath.Join(cgroupRoot, p)
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return filepath.Join(cgroupRoot, "kubepods") // 默认值
}
