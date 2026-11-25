/*
Copyright 2025 The Kubernetes Authors.

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

package workloads

import (
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// pytorchWideDeepWorkload 定义运行 PyTorch Wide-Deep 模型训练基准的 workload，
// 用于验证 CPU Manager
type pytorchWideDeepWorkload struct{}

// 确保 pytorchWideDeepWorkload 实现 NodePerfWorkload 接口
var _ NodePerfWorkload = &pytorchWideDeepWorkload{}

func (w pytorchWideDeepWorkload) Name() string {
	return "pytorch-wide-deep"
}

func (w pytorchWideDeepWorkload) PodSpec() v1.PodSpec {
	var containers []v1.Container
	ctn := v1.Container{
		Name:  fmt.Sprintf("%s-ctn", w.Name()),
		Image: imageutils.GetE2EImage(imageutils.NodePerfPytorchWideDeep),
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("15000m"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("16Gi"),
			},
			Limits: v1.ResourceList{
				v1.ResourceName(v1.ResourceCPU):    resource.MustParse("15000m"),
				v1.ResourceName(v1.ResourceMemory): resource.MustParse("16Gi"),
			},
		},
		// 容器入口已运行：`time -p python /train_wide_deep.py`
	}
	containers = append(containers, ctn)

	return v1.PodSpec{
		RestartPolicy: v1.RestartPolicyNever,
		Containers:    containers,
	}
}

func (w pytorchWideDeepWorkload) Timeout() time.Duration {
	return 15 * time.Minute
}

func (w pytorchWideDeepWorkload) KubeletConfig(oldCfg *kubeletconfig.KubeletConfiguration) (newCfg *kubeletconfig.KubeletConfiguration, err error) {
	// 在 kubelet 中以 static 策略启用 CPU Manager
	newCfg = oldCfg.DeepCopy()
	// 将 CPU Manager 策略设为 static
	newCfg.CPUManagerPolicy = string(cpumanager.PolicyStatic)
	// 将 CPU Manager 的 reconcile 周期设为 10 秒
	newCfg.CPUManagerReconcilePeriod = metav1.Duration{Duration: 10 * time.Second}

	// 启用 static CPU Manager 时，如果未设置 kube-reserved 或 system-reserved，
	// kubelet 会 panic。因此在 kube-reserved 中将 cpu 设为 > 0，避免 panic
	if newCfg.KubeReserved == nil {
		newCfg.KubeReserved = map[string]string{}
	}

	if _, ok := newCfg.KubeReserved["cpu"]; !ok {
		newCfg.KubeReserved["cpu"] = "200m"
	}

	return newCfg, nil
}

func (w pytorchWideDeepWorkload) PreTestExec() error {
	cmd := "/bin/sh"
	args := []string{"-c", "rm -f /var/lib/kubelet/cpu_manager_state"}
	err := runCmd(cmd, args)

	return err
}

func (w pytorchWideDeepWorkload) PostTestExec() error {
	cmd := "/bin/sh"
	args := []string{"-c", "rm -f /var/lib/kubelet/cpu_manager_state"}
	err := runCmd(cmd, args)

	return err
}

func (w pytorchWideDeepWorkload) ExtractPerformanceFromLogs(logs string) (perf time.Duration, err error) {
	perfLine, err := getMatchingLineFromLog(logs, "real")
	if err != nil {
		return perf, err
	}
	perfString := fmt.Sprintf("%ss", strings.TrimSpace(strings.TrimPrefix(perfLine, "real")))
	perf, err = time.ParseDuration(perfString)

	return perf, err
}
