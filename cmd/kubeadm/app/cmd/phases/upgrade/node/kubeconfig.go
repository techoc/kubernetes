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

// Package node 实现 kubeadm upgrade node 的阶段。
package node

import (
	"fmt"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/upgrade"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// NewKubeconfigPhase 返回新的 kubeconfig 阶段。
func NewKubeconfigPhase() workflow.Phase {
	phase := workflow.Phase{
		Name:   "kubeconfig",
		Short:  "Upgrade kubeconfig files for this node",
		Run:    runKubeconfig(),
		Hidden: true,
		InheritFlags: []string{
			options.CfgPath,
			options.DryRun,
			options.KubeconfigPath,
		},
	}
	return phase
}

func runKubeconfig() func(c workflow.RunData) error {
	return func(c workflow.RunData) error {
		data, ok := c.(Data)
		if !ok {
			return errors.New("kubeconfig phase invoked with an invalid data struct")
		}

		// 若不是控制平面节点，则不应执行此阶段。
		if !data.IsControlPlaneNode() {
			fmt.Println("[upgrade/kubeconfig] Skipping phase. Not a control plane node.")
			return nil
		}

		// 否则，获取 kubeconfig 升级所需的全部信息。
		cfg := data.InitCfg()

		if err := upgrade.UpdateKubeletKubeconfigServer(cfg, data.DryRun()); err != nil {
			return errors.Wrap(err, "failed to update kubelet local mode")
		}

		fmt.Println("[upgrade/kubeconfig] The kubeconfig files for this node were successfully upgraded!")

		return nil
	}
}
