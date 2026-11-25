/*
Copyright 2024 The Kubernetes Authors.

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

// Package upgrade 包含 kubeadm upgrade 的公共阶段。
package upgrade

import (
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/upgrade"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// NewPostUpgradePhase 返回新的 post-upgrade 阶段。
func NewPostUpgradePhase() workflow.Phase {
	return workflow.Phase{
		Name:  "post-upgrade",
		Short: "Run post upgrade tasks",
		Run:   runPostUpgrade,
		InheritFlags: []string{
			options.CfgPath,
			options.KubeconfigPath,
			options.DryRun,
		},
	}
}

func runPostUpgrade(c workflow.RunData) error {
	data, ok := c.(Data)
	if !ok {
		return errors.New("post-upgrade phase invoked with an invalid data struct")
	}
	// 占位：此阶段应包含各发布版本特有的升级后任务。

	// 将不含多余参数的 kubelet 环境文件重写至磁盘；dry-run 时改为打印剩余参数。
	// 非 dry-run 时，kubelet 环境文件会备份到 /etc/kubernetes/tmp/，以便出错后恢复。
	unwantedFlags := []string{}
	err := upgrade.RemoveKubeletArgsFromFile(data.KubeletDir(), data.KubeConfigDir(), unwantedFlags, data.DryRun(), data.OutputWriter())
	if err != nil {
		return err
	}

	return nil
}
