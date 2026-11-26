//go:build linux

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

package e2enode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/stats/pidlimit"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2enodekubelet "k8s.io/kubernetes/test/e2e_node/kubeletconfig"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func setDesiredConfiguration(initialConfig *kubeletconfig.KubeletConfiguration, cgroupManager cm.CgroupManager) {
	initialConfig.EnforceNodeAllocatable = []string{"pods", kubeReservedCgroup, systemReservedCgroup}
	initialConfig.SystemReserved = map[string]string{
		string(v1.ResourceCPU):    "100m",
		string(v1.ResourceMemory): "100Mi",
		string(pidlimit.PIDs):     "1000",
	}
	initialConfig.KubeReserved = map[string]string{
		string(v1.ResourceCPU):    "100m",
		string(v1.ResourceMemory): "100Mi",
		string(pidlimit.PIDs):     "738",
	}
	initialConfig.EvictionHard = map[string]string{"memory.available": "100Mi"}
	// 创建 allocatable cgroup 所必需。
	initialConfig.CgroupsPerQOS = true
	initialConfig.KubeReservedCgroup = kubeReservedCgroup
	initialConfig.SystemReservedCgroup = systemReservedCgroup

	if initialConfig.CgroupDriver == "systemd" {
		initialConfig.KubeReservedCgroup = cm.NewCgroupName(cm.RootCgroupName, kubeReservedCgroup).ToSystemd()
		initialConfig.SystemReservedCgroup = cm.NewCgroupName(cm.RootCgroupName, systemReservedCgroup).ToSystemd()
	}
}

var _ = SIGDescribe("Node Container Manager", framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("node-container-manager")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	f.Describe("Validate Node Allocatable", feature.NodeAllocatable, func() {
		ginkgo.It("sets up the node and runs the test", func(ctx context.Context) {
			ginkgo.Skip("currently broken")
			framework.ExpectNoError(runTest(ctx, f))
		})
	})
	f.Describe("Validate CGroup management", func() {
		// 针对 https://issues.k8s.io/125923 的回归测试
		// 这个问题涉及 systemd 的竞态，似乎最可能（甚至只可能 —— 目前数据尚不确定）
		// 在机器首次启动时出现，因此仅重启 kubelet 看来不够。
		// 另一方面，精确的复现方法似乎需要只有本测试的专用 lane，或
		// 在运行测试前重启机器。这两种做法在 CI 中都不现实。
		// 最接近的近似就是当前这种通过重启 kubelet 的测试。至少
		// 能起到回归测试的作用，仍然有价值。
		ginkgo.It("should correctly start with cpumanager none policy in use with systemd", func(ctx context.Context) {
			ginkgo.Skip("currently broken")

			if !IsCgroup2UnifiedMode() {
				ginkgo.Skip("this test requires cgroups v2")
			}

			var err error
			var oldCfg *kubeletconfig.KubeletConfiguration
			// 获取当前的 kubelet 配置
			oldCfg, err = getCurrentKubeletConfig(ctx)
			framework.ExpectNoError(err)

			ginkgo.DeferCleanup(func(ctx context.Context) {
				if oldCfg != nil {
					// 更新 Kubelet 配置。
					framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(oldCfg))

					ginkgo.By("Restarting the kubelet")
					restartKubelet(ctx, true)

					waitForKubeletToStart(ctx, f)
					ginkgo.By("Started the kubelet")
				}
			})

			newCfg := oldCfg.DeepCopy()
			// 修改现有的 kubelet 配置
			newCfg.CPUManagerPolicy = "none"
			newCfg.CgroupDriver = "systemd"
			newCfg.FailCgroupV1 = true // 额外保险。但我们想避免假阴性，所以前面加了跳过检查

			// 更新 Kubelet 配置。
			framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(newCfg))

			ginkgo.By("Restarting the kubelet")
			restartKubelet(ctx, true)

			waitForKubeletToStart(ctx, f)
			ginkgo.By("Started the kubelet")

			gomega.Consistently(ctx, func(ctx context.Context) bool {
				return getNodeReadyStatus(ctx, f) && e2enode.HealthCheck(kubeletHealthCheckURL)
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.BeTrueBecause("node keeps reporting ready status"))
		})
	})
})

func expectFileValToEqual(filePath string, expectedValue, delta int64) error {
	out, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file %q", filePath)
	}
	actual, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse output %v", err)
	}

	// 校验数值在 delta 范围内，绕开舍入误差。
	if (actual < (expectedValue - delta)) || (actual > (expectedValue + delta)) {
		return fmt.Errorf("Expected value at %q to be between %d and %d. Got %d", filePath, (expectedValue - delta), (expectedValue + delta), actual)
	}
	return nil
}

func getAllocatableLimits(cpu, memory, pids string, capacity v1.ResourceList) (*resource.Quantity, *resource.Quantity, *resource.Quantity) {
	var allocatableCPU, allocatableMemory, allocatablePIDs *resource.Quantity
	// CPU 预留总量为 200m。
	for k, v := range capacity {
		if k == v1.ResourceCPU {
			c := v.DeepCopy()
			allocatableCPU = &c
			allocatableCPU.Sub(resource.MustParse(cpu))
		}
		if k == v1.ResourceMemory {
			c := v.DeepCopy()
			allocatableMemory = &c
			allocatableMemory.Sub(resource.MustParse(memory))
		}
	}
	// PID 不是 node allocatable 资源，只能这样临时处理
	pidlimits, err := pidlimit.Stats()
	if err == nil && pidlimits != nil && pidlimits.MaxPID != nil {
		allocatablePIDs = resource.NewQuantity(int64(*pidlimits.MaxPID), resource.DecimalSI)
		allocatablePIDs.Sub(resource.MustParse(pids))
	}
	return allocatableCPU, allocatableMemory, allocatablePIDs
}

const (
	kubeReservedCgroup    = "kube-reserved"
	systemReservedCgroup  = "system-reserved"
	nodeAllocatableCgroup = "kubepods"
)

func createIfNotExists(cm cm.CgroupManager, cgroupConfig *cm.CgroupConfig) error {
	if !cm.Exists(cgroupConfig.Name) {
		if err := cm.Create(klog.Background(), cgroupConfig); err != nil {
			return err
		}
	}
	return nil
}

func createTemporaryCgroupsForReservation(cgroupManager cm.CgroupManager) error {
	// 创建 kube reserved cgroup
	cgroupConfig := &cm.CgroupConfig{
		Name: cm.NewCgroupName(cm.RootCgroupName, kubeReservedCgroup),
	}
	if err := createIfNotExists(cgroupManager, cgroupConfig); err != nil {
		return err
	}
	// 创建 system reserved cgroup
	cgroupConfig.Name = cm.NewCgroupName(cm.RootCgroupName, systemReservedCgroup)

	return createIfNotExists(cgroupManager, cgroupConfig)
}

func destroyTemporaryCgroupsForReservation(cgroupManager cm.CgroupManager) error {
	// 创建 kube reserved cgroup
	cgroupConfig := &cm.CgroupConfig{
		Name: cm.NewCgroupName(cm.RootCgroupName, kubeReservedCgroup),
	}
	if err := cgroupManager.Destroy(klog.Background(), cgroupConfig); err != nil {
		return err
	}
	cgroupConfig.Name = cm.NewCgroupName(cm.RootCgroupName, systemReservedCgroup)
	return cgroupManager.Destroy(klog.Background(), cgroupConfig)
}

// convertSharesToWeight 把 cgroup v1 的 cpu.shares 转换为 cgroup v2 的 cpu.weight
func convertSharesToWeight(shares int64) int64 {
	return 1 + ((shares-2)*9999)/262142
}

func runTest(ctx context.Context, f *framework.Framework) error {
	var oldCfg *kubeletconfig.KubeletConfiguration
	subsystems, err := cm.GetCgroupSubsystems()
	if err != nil {
		return err
	}
	// 获取当前的 kubelet 配置
	oldCfg, err = getCurrentKubeletConfig(ctx)
	if err != nil {
		return err
	}

	// 创建用于操作 cgroup 的 cgroup manager 对象。
	cgroupManager := cm.NewCgroupManager(klog.Background(), subsystems, oldCfg.CgroupDriver)

	ginkgo.DeferCleanup(destroyTemporaryCgroupsForReservation, cgroupManager)
	ginkgo.DeferCleanup(func(ctx context.Context) {
		if oldCfg != nil {
			// 更新 Kubelet 配置。
			ginkgo.By("Stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			// 等待 kubelet 健康检查失败
			gomega.Eventually(ctx, func() bool {
				return e2enode.HealthCheck(kubeletHealthCheckURL)
			}, time.Minute, time.Second).Should(gomega.BeFalseBecause("expected kubelet health check to be failed"))

			framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(oldCfg))

			ginkgo.By("Restarting the kubelet")
			restartKubelet(ctx)
		}
	})
	if err := createTemporaryCgroupsForReservation(cgroupManager); err != nil {
		return err
	}

	newCfg := oldCfg.DeepCopy()
	// 修改现有的 kubelet 配置
	setDesiredConfiguration(newCfg, cgroupManager)
	// 设置新的 kubelet 配置。
	// 更新 Kubelet 配置。
	ginkgo.By("Stopping the kubelet")
	restartKubelet := mustStopKubelet(ctx, f)

	expectedNAPodCgroup := cm.NewCgroupName(cm.RootCgroupName, nodeAllocatableCgroup)

	// 清理上一个 kubelet 的遗留内容，验证新 kubelet 能正确创建它
	if err := cgroupManager.Destroy(klog.Background(), &cm.CgroupConfig{
		Name: cm.NewCgroupName(expectedNAPodCgroup),
	}); err != nil {
		return err
	}
	if cgroupManager.Exists(expectedNAPodCgroup) {
		return fmt.Errorf("Expected Node Allocatable Cgroup %q not to exist", expectedNAPodCgroup)
	}

	framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(newCfg))

	ginkgo.By("Starting the kubelet")
	restartKubelet(ctx)

	if err != nil {
		return err
	}
	// 设置新配置和当前配置。
	currentConfig := newCfg

	if !cgroupManager.Exists(expectedNAPodCgroup) {
		return fmt.Errorf("Expected Node Allocatable Cgroup %q to exist", expectedNAPodCgroup)
	}

	memoryLimitFile := "memory.limit_in_bytes"
	if IsCgroup2UnifiedMode() {
		memoryLimitFile = "memory.max"
	}

	// TODO: 让 cgroupManager 暴露 Status 接口，以获取当前 Cgroup 设置。
	// 节点可能还没更新 capacity 和 allocatable，因此用 Eventually 检查。
	gomega.Eventually(ctx, func(ctx context.Context) error {
		nodeList, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		if len(nodeList.Items) != 1 {
			return fmt.Errorf("Unexpected number of node objects for node e2e. Expects only one node: %+v", nodeList)
		}
		cgroupName := nodeAllocatableCgroup
		if currentConfig.CgroupDriver == "systemd" {
			cgroupName = nodeAllocatableCgroup + ".slice"
		}

		node := nodeList.Items[0]
		capacity := node.Status.Capacity
		allocatableCPU, allocatableMemory, allocatablePIDs := getAllocatableLimits("200m", "200Mi", "1738", capacity)
		// 内存预留总量为 200Mi，不含驱逐阈值。
		// 期望 node allocatable cgroup 的 CPU shares 等于 allocatable。
		shares := int64(cm.MilliCPUToShares(allocatableCPU.MilliValue()))
		if IsCgroup2UnifiedMode() {
			// 转换为 cgroup v2 的 cpu.weight 值
			if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["cpu"], cgroupName, "cpu.weight"), convertSharesToWeight(shares), 10); err != nil {
				return err
			}
		} else {
			if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["cpu"], cgroupName, "cpu.shares"), shares, 10); err != nil {
				return err
			}
		}
		// 期望 node allocatable cgroup 的 Memory limit 等于 allocatable。
		if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["memory"], cgroupName, memoryLimitFile), allocatableMemory.Value(), 0); err != nil {
			return err
		}
		// 期望 node allocatable cgroup 的 PID limit 等于 allocatable。
		if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["pids"], cgroupName, "pids.max"), allocatablePIDs.Value(), 0); err != nil {
			return err
		}

		// 检查上报给 scheduler 的 Allocatable 包含驱逐阈值。
		schedulerAllocatable := node.Status.Allocatable
		// 内存 allocatable 应考虑驱逐阈值。
		// PID 不是 scheduler 资源，因此这里无法测试。
		allocatableCPU, allocatableMemory, _ = getAllocatableLimits("200m", "300Mi", "1738", capacity)
		// 期望 allocatable 包含 capacity 中的所有资源。
		if len(schedulerAllocatable) != len(capacity) {
			return fmt.Errorf("Expected all resources in capacity to be found in allocatable")
		}
		// 不支持基于 CPU 的驱逐。
		if allocatableCPU.Cmp(schedulerAllocatable[v1.ResourceCPU]) != 0 {
			return fmt.Errorf("Unexpected cpu allocatable value exposed by the node. Expected: %v, got: %v, capacity: %v", allocatableCPU, schedulerAllocatable[v1.ResourceCPU], capacity[v1.ResourceCPU])
		}
		if allocatableMemory.Cmp(schedulerAllocatable[v1.ResourceMemory]) != 0 {
			return fmt.Errorf("Unexpected memory allocatable value exposed by the node. Expected: %v, got: %v, capacity: %v", allocatableMemory, schedulerAllocatable[v1.ResourceMemory], capacity[v1.ResourceMemory])
		}
		return nil
	}, time.Minute, 5*time.Second).Should(gomega.Succeed())

	cgroupPath := ""
	if currentConfig.CgroupDriver == "systemd" {
		cgroupPath = cm.NewCgroupName(cm.RootCgroupName, kubeReservedCgroup).ToSystemd()
	} else {
		cgroupPath = cgroupManager.Name(cm.NewCgroupName(cm.RootCgroupName, kubeReservedCgroup))
	}
	// 期望 kube reserved cgroup 的 CPU shares 等于其预留值 `100m`。
	kubeReservedCPU := resource.MustParse(currentConfig.KubeReserved[string(v1.ResourceCPU)])
	shares := int64(cm.MilliCPUToShares(kubeReservedCPU.MilliValue()))
	if IsCgroup2UnifiedMode() {
		if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["cpu"], cgroupPath, "cpu.weight"), convertSharesToWeight(shares), 10); err != nil {
			return err
		}
	} else {
		if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["cpu"], cgroupPath, "cpu.shares"), shares, 10); err != nil {
			return err
		}
	}
	// 期望 kube reserved cgroup 的 Memory limit 等于配置值 `100Mi`。
	kubeReservedMemory := resource.MustParse(currentConfig.KubeReserved[string(v1.ResourceMemory)])
	if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["memory"], cgroupPath, memoryLimitFile), kubeReservedMemory.Value(), 0); err != nil {
		return err
	}
	// 期望 kube reserved cgroup 的 PID limit 等于配置值 `738`。
	kubeReservedPIDs := resource.MustParse(currentConfig.KubeReserved[string(pidlimit.PIDs)])
	if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["pids"], cgroupPath, "pids.max"), kubeReservedPIDs.Value(), 0); err != nil {
		return err
	}

	if currentConfig.CgroupDriver == "systemd" {
		cgroupPath = cm.NewCgroupName(cm.RootCgroupName, systemReservedCgroup).ToSystemd()
	} else {
		cgroupPath = cgroupManager.Name(cm.NewCgroupName(cm.RootCgroupName, systemReservedCgroup))
	}

	// 期望 system reserved cgroup 的 CPU shares 等于其预留值 `100m`。
	systemReservedCPU := resource.MustParse(currentConfig.SystemReserved[string(v1.ResourceCPU)])
	shares = int64(cm.MilliCPUToShares(systemReservedCPU.MilliValue()))
	if IsCgroup2UnifiedMode() {
		if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["cpu"], cgroupPath, "cpu.weight"), convertSharesToWeight(shares), 10); err != nil {
			return err
		}
	} else {
		if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["cpu"], cgroupPath, "cpu.shares"), shares, 10); err != nil {
			return err
		}
	}
	// 期望 node allocatable cgroup 的 Memory limit 等于 allocatable。
	systemReservedMemory := resource.MustParse(currentConfig.SystemReserved[string(v1.ResourceMemory)])
	if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["memory"], cgroupPath, memoryLimitFile), systemReservedMemory.Value(), 0); err != nil {
		return err
	}
	// 期望 system reserved cgroup 的 PID limit 等于配置值 `1000`。
	systemReservedPIDs := resource.MustParse(currentConfig.SystemReserved[string(pidlimit.PIDs)])
	if err := expectFileValToEqual(filepath.Join(subsystems.MountPoints["pids"], cgroupPath, "pids.max"), systemReservedPIDs.Value(), 0); err != nil {
		return err
	}
	return nil
}
