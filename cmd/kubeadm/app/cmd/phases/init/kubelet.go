/*
Copyright 2019 The Kubernetes Authors.

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

package phases

import (
	"fmt"

	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeletphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/kubelet"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

var (
	kubeletStartPhaseExample = cmdutil.Examples(`
		# Writes a dynamic environment file with kubelet flags from a InitConfiguration file.
		kubeadm init phase kubelet-start --config config.yaml
		`)
)

// NewKubeletStartPhase 创建在节点上启动 kubelet 的 kubeadm 工作流阶段。
func NewKubeletStartPhase() workflow.Phase {
	return workflow.Phase{
		Name:    "kubelet-start",
		Short:   "Write kubelet settings and (re)start the kubelet",
		Long:    "Write a file with KubeletConfiguration and an environment file with node specific kubelet settings, and then (re)start kubelet.",
		Example: kubeletStartPhaseExample,
		Run:     runKubeletStart,
		InheritFlags: []string{
			options.CfgPath,
			options.ImageRepository,
			options.NodeCRISocket,
			options.NodeName,
			options.Patches,
			options.DryRun,
		},
	}
}

// runKubeletStart 执行 kubelet 启动逻辑。
func runKubeletStart(c workflow.RunData) error {
	data, ok := c.(InitData)
	if !ok {
		return errors.New("kubelet-start phase invoked with an invalid data struct")
	}

	// 首先配置 kubelet。在这段短时间内，kubeadm 会尝试停止并重启 kubelet。
	// 先停止 kubelet 服务，避免配置时发生竞态。
	if !data.DryRun() {
		klog.V(1).Infoln("Stopping the kubelet")
		kubeletphase.TryStopKubelet()
	}

	// 写入 kubelet 使用的参数环境文件。控制平面无需写入 --register-with-taints，
	// 因为我们在 mark-control-plane 阶段自行处理。
	// TODO: 未来可考虑在此处理，以移除 mark-control-plane 阶段的部分逻辑。
	if err := kubeletphase.WriteKubeletDynamicEnvFile(&data.Cfg().ClusterConfiguration, &data.Cfg().NodeRegistration, false, data.KubeletDir()); err != nil {
		return errors.Wrap(err, "error writing a dynamic environment file for the kubelet")
	}

	// 将实例 kubelet 配置文件写入磁盘。
	kubeletConfig := &kubeletconfig.KubeletConfiguration{
		ContainerRuntimeEndpoint: data.Cfg().NodeRegistration.CRISocket,
	}
	if err := kubeletphase.WriteInstanceConfigToDisk(kubeletConfig, data.KubeletDir()); err != nil {
		return errors.Wrap(err, "error writing instance kubelet configuration to disk")
	}

	// 将 kubelet 配置文件写入磁盘。
	if err := kubeletphase.WriteConfigToDisk(&data.Cfg().ClusterConfiguration, data.KubeletDir(), data.PatchesDir(), data.OutputWriter()); err != nil {
		return errors.Wrap(err, "error writing kubelet configuration to disk")
	}

	// 若 kubelet 服务未运行，则尝试启动。
	if !data.DryRun() {
		fmt.Println("[kubelet-start] Starting the kubelet")
		kubeletphase.TryStartKubelet()
	}

	return nil
}
