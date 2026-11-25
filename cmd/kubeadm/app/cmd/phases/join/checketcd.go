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

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	etcdphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/etcd"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// NewCheckEtcdPhase 是在 control-plane-prepare 之后、bootstrap-kubelet 之前运行的隐藏阶段，
// 用于确保 etcd 健康。
func NewCheckEtcdPhase() workflow.Phase {
	return workflow.Phase{
		Name:   "check-etcd",
		Run:    runCheckEtcdPhase,
		Hidden: true,
	}
}

func runCheckEtcdPhase(c workflow.RunData) error {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("check-etcd phase invoked with an invalid data struct")
	}

	// 若不是控制平面则跳过。
	if data.Cfg().ControlPlane == nil {
		return nil
	}

	cfg, err := data.InitCfg()
	if err != nil {
		return err
	}

	if cfg.Etcd.External != nil {
		fmt.Println("[check-etcd] Skipping etcd check in external mode")
		return nil
	}

	if data.DryRun() {
		fmt.Println("[dryrun] Would check that the etcd cluster is healthy")
		return nil
	}

	fmt.Println("[check-etcd] Checking that the etcd cluster is healthy")

	// 检查 etcd 集群是否健康。
	// 注意：此检查无法更早执行，因为它要求 admin.conf 和连接 etcd 所需的全部证书已就绪。
	client, err := data.Client()
	if err != nil {
		return err
	}

	return etcdphase.CheckLocalEtcdClusterStatus(client, data.CertificateWriteDir())
}
