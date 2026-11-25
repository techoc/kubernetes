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

// Package node 实现 kubeadm upgrade node 的阶段。
package node

import (
	"fmt"
	"os"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/upgrade"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// NewControlPlane 返回新的 control-plane 阶段。
func NewControlPlane() workflow.Phase {
	phase := workflow.Phase{
		Name:  "control-plane",
		Short: "Upgrade the control plane instance deployed on this node, if any",
		Run:   runControlPlane(),
		InheritFlags: []string{
			options.CfgPath,
			options.DryRun,
			options.KubeconfigPath,
			options.CertificateRenewal,
			options.EtcdUpgrade,
			options.Patches,
		},
	}
	return phase
}

func runControlPlane() func(c workflow.RunData) error {
	return func(c workflow.RunData) error {
		data, ok := c.(Data)
		if !ok {
			return errors.New("control-plane phase invoked with an invalid data struct")
		}

		// 若不是控制平面节点，则不应执行此阶段。
		if !data.IsControlPlaneNode() {
			fmt.Println("[upgrade/control-plane] Skipping phase. Not a control plane node.")
			return nil
		}

		// 否则，获取控制平面升级所需的全部信息。
		cfg := data.InitCfg()
		client := data.Client()
		dryRun := data.DryRun()
		etcdUpgrade := data.EtcdUpgrade()
		renewCerts := data.RenewCerts()
		patchesDir := data.PatchesDir()

		// 升级此节点上安装的控制平面和 etcd。
		fmt.Printf("[upgrade/control-plane] Upgrading your Static Pod-hosted control plane instance to version %q...\n", cfg.KubernetesVersion)
		if dryRun {
			fmt.Printf("[dryrun] Would upgrade your static Pod-hosted control plane to version %q", cfg.KubernetesVersion)
			return upgrade.DryRunStaticPodUpgrade(patchesDir, cfg)
		}

		waiter := apiclient.NewKubeWaiter(data.Client(), data.Cfg().Timeouts.UpgradeManifests.Duration, os.Stdout)

		if err := upgrade.PerformStaticPodUpgrade(client, waiter, cfg, etcdUpgrade, renewCerts, patchesDir); err != nil {
			return errors.Wrap(err, "couldn't complete the static pod upgrade")
		}

		fmt.Println("[upgrade/control-plane] The control plane instance for this node was successfully upgraded!")

		return nil
	}
}
