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
	"os"
	"path/filepath"

	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	etcdphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/etcd"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	utilstaticpod "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

// NewRemoveETCDMemberPhase 创建 remove-etcd-member 的 kubeadm 工作流阶段。
func NewRemoveETCDMemberPhase() workflow.Phase {
	return workflow.Phase{
		Name:  "remove-etcd-member",
		Short: "Remove a local etcd member.",
		Long:  "Remove a local etcd member for a control plane node.",
		Run:   runRemoveETCDMemberPhase,
		InheritFlags: []string{
			options.KubeconfigPath,
			options.DryRun,
		},
	}
}

func runRemoveETCDMemberPhase(c workflow.RunData) error {
	r, ok := c.(resetData)
	if !ok {
		return errors.New("remove-etcd-member-phase phase invoked with an invalid data struct")
	}
	cfg := r.Cfg()

	// 仅在使用本地 etcd 时清理 etcd 数据。
	klog.V(1).Infoln("[reset] Checking for etcd config")
	etcdManifestPath := filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.ManifestsSubDirName, "etcd.yaml")
	etcdDataDir, err := getEtcdDataDir(etcdManifestPath, cfg)
	if err == nil {
		if cfg != nil {
			if !r.DryRun() {
				err := etcdphase.RemoveStackedEtcdMemberFromCluster(r.Client(), cfg)
				if err != nil {
					klog.Warningf("[reset] Failed to remove etcd member: %v, please manually remove this etcd member using etcdctl", err)
				} else {
					if err := CleanDir(etcdDataDir); err != nil {
						klog.Warningf("[reset] Failed to delete contents of the etcd directory: %q, error: %v", etcdDataDir, err)
					} else {
						fmt.Printf("[reset] Deleted contents of the etcd data directory: %v\n", etcdDataDir)
					}
				}
			} else {
				fmt.Println("[dryrun] Would remove the etcd member on this node from the etcd cluster")
				fmt.Printf("[dryrun] Would delete contents of the etcd data directory: %v\n", etcdDataDir)
			}
		}
		// 若先运行 cleanup-node 阶段再运行 remove-etcd-member，则可能发生这种情况。
		// 清理 etcd 数据目录中的文件，避免残留文件导致下次创建集群失败。
		empty, _ := IsDirEmpty(etcdDataDir)
		if !empty && !r.DryRun() {
			if err := CleanDir(etcdDataDir); err != nil {
				klog.Warningf("[reset] Failed to delete contents of the etcd directory: %q, error: %v", etcdDataDir, err)
			} else {
				fmt.Printf("[reset] Deleted contents of the etcd data directory: %v\n", etcdDataDir)
			}
		}
	} else {
		fmt.Println("[reset] No etcd config found. Assuming external etcd")
		fmt.Println("[reset] Please, manually reset etcd to prevent further issues")
	}

	return nil
}

func getEtcdDataDir(manifestPath string, cfg *kubeadmapi.InitConfiguration) (string, error) {
	const etcdVolumeName = "etcd-data"
	var dataDir string

	if cfg != nil && cfg.Etcd.Local != nil {
		return cfg.Etcd.Local.DataDir, nil
	}
	klog.Warningln("[reset] No kubeadm config, using etcd pod spec to get data directory")

	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		// 若 etcd.yaml 不存在，则回退到默认集群配置；
		// 该文件可能已被 cleanup-node 等 reset 阶段删除。
		cfg := &kubeadmapiv1.ClusterConfiguration{}
		scheme.Scheme.Default(cfg)
		return cfg.Etcd.Local.DataDir, nil
	}
	etcdPod, err := utilstaticpod.ReadStaticPodFromDisk(manifestPath)
	if err != nil {
		return "", err
	}
	for _, volumeMount := range etcdPod.Spec.Volumes {
		if volumeMount.Name == etcdVolumeName {
			dataDir = volumeMount.HostPath.Path
			break
		}
	}
	if dataDir == "" {
		return dataDir, errors.New("invalid etcd pod manifest")
	}
	return dataDir, nil
}
