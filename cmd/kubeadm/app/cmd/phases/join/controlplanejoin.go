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

	"github.com/spf13/cobra"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	etcdphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/etcd"
	markcontrolplanephase "k8s.io/kubernetes/cmd/kubeadm/app/phases/markcontrolplane"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	etcdutil "k8s.io/kubernetes/cmd/kubeadm/app/util/etcd"
)

var controlPlaneJoinExample = cmdutil.Examples(`
	# Joins a machine as a control plane instance
	kubeadm join phase control-plane-join all
`)

var etcdJoinExample = cmdutil.Examples(`
	# Joins etcd for a control plane instance
	kubeadm join phase control-plane-join-etcd all
`)

func getControlPlaneJoinPhaseFlags(name string) []string {
	flags := []string{
		options.CfgPath,
		options.ControlPlane,
		options.NodeName,
	}
	if name == "etcd" || name == "all" {
		flags = append(flags, options.Patches)
	}
	if name != "mark-control-plane" {
		flags = append(flags, options.APIServerAdvertiseAddress)
	}
	if name != "update-status" {
		flags = append(flags, options.DryRun)
	}
	return flags
}

// NewControlPlaneJoinPhase 创建实现将机器加入为控制平面实例的 kubeadm 工作流阶段。
func NewControlPlaneJoinPhase() workflow.Phase {
	return workflow.Phase{
		Name:    "control-plane-join",
		Short:   "Join a machine as a control plane instance",
		Example: controlPlaneJoinExample,
		Phases: []workflow.Phase{
			{
				Name:           "all",
				Short:          "Join a machine as a control plane instance",
				InheritFlags:   getControlPlaneJoinPhaseFlags("all"),
				RunAllSiblings: true,
				ArgsValidator:  cobra.NoArgs,
			},
			newMarkControlPlaneSubphase(),
		},
	}
}

// NewEtcdJoinPhase 创建实现加入 etcd 的 kubeadm 工作流阶段。
func NewEtcdJoinPhase() workflow.Phase {
	return workflow.Phase{
		Name:          "etcd-join",
		Short:         "Join etcd for control plane nodes",
		Run:           runEtcdPhase,
		Example:       etcdJoinExample,
		InheritFlags:  getControlPlaneJoinPhaseFlags("etcd"),
		ArgsValidator: cobra.NoArgs,
	}
}

func newMarkControlPlaneSubphase() workflow.Phase {
	return workflow.Phase{
		Name:          "mark-control-plane",
		Short:         "Mark a node as a control-plane",
		Run:           runMarkControlPlanePhase,
		InheritFlags:  getControlPlaneJoinPhaseFlags("mark-control-plane"),
		ArgsValidator: cobra.NoArgs,
	}
}

func runEtcdPhase(c workflow.RunData) error {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("control-plane-join phase invoked with an invalid data struct")
	}

	if data.Cfg().ControlPlane == nil {
		return nil
	}

	// 使用 admin.conf 中定义的身份访问集群。
	client, err := data.Client()
	if err != nil {
		return errors.Wrap(err, "couldn't create Kubernetes client")
	}
	cfg, err := data.InitCfg()
	if err != nil {
		return err
	}
	// 使用外部 etcd 时。
	if cfg.Etcd.External != nil {
		fmt.Println("[control-plane-join] Using external etcd - no local stacked instance added")
		return nil
	}

	if !data.DryRun() {
		// 创建 etcd 数据目录。
		if err := etcdutil.CreateDataDirectory(cfg.Etcd.Local.DataDir); err != nil {
			return err
		}
	} else {
		fmt.Printf("[control-plane-join] Would ensure that %q directory is present\n", cfg.Etcd.Local.DataDir)
	}

	// 添加新 etcd 实例前，必须先向现有 etcd 成员“宣告”该实例，然后才能创建它。
	// 此操作必须在 kubelet 启动后执行，以缩短新成员从宣告到运行新成员的静态 Pod 启动之间的时间。
	// 在这段时间内 etcd 会暂时不可用（仅发生在 etcd 集群从 1 个成员扩展到 2 个成员时）。
	// 参考 https://coreos.com/etcd/docs/latest/v2/runtime-configuration.html ：
	// “向单节点集群添加新成员后，在新成员启动前集群无法继续工作，
	// 因为共识需要两个成员作为多数，所以这种情况只会出现在 etcdctl member add 通知集群新成员，
	// 到新成员成功与现有成员建立连接之间。”
	if err := etcdphase.CreateStackedEtcdStaticPodManifestFile(client, data.ManifestDir(), data.PatchesDir(), cfg.NodeRegistration.Name, &cfg.ClusterConfiguration, &cfg.LocalAPIEndpoint, data.DryRun(), data.CertificateWriteDir()); err != nil {
		return errors.Wrap(err, "error creating local etcd static pod manifest file")
	}

	return nil
}

func runMarkControlPlanePhase(c workflow.RunData) error {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("control-plane-join phase invoked with an invalid data struct")
	}

	if data.Cfg().ControlPlane == nil {
		return nil
	}

	// 使用 admin.conf 中定义的身份访问集群。
	client, err := data.Client()
	if err != nil {
		return errors.Wrap(err, "couldn't create Kubernetes client")
	}
	cfg, err := data.InitCfg()
	if err != nil {
		return err
	}

	if !data.DryRun() {
		if err := markcontrolplanephase.MarkControlPlane(client, cfg.NodeRegistration.Name, cfg.NodeRegistration.Taints); err != nil {
			return errors.Wrap(err, "error applying control-plane label and taints")
		}
	} else {
		fmt.Printf("[control-plane-join] Would mark node %s as a control-plane\n", cfg.NodeRegistration.Name)
	}

	return nil
}
