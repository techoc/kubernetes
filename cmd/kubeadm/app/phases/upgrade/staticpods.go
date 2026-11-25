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

package upgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	certsphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/certs/renewal"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/controlplane"
	etcdphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/etcd"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	dryrunutil "k8s.io/kubernetes/cmd/kubeadm/app/util/dryrun"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	etcdutil "k8s.io/kubernetes/cmd/kubeadm/app/util/etcd"
	filesutil "k8s.io/kubernetes/cmd/kubeadm/app/util/files"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/image"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

// StaticPodPathManager 负责跟踪静态 Pod 升级过程中使用的目录。
type StaticPodPathManager interface {
	// MoveFile 将文件从 oldPath 移动到 newPath。
	MoveFile(oldPath, newPath string) error
	// KubernetesDir 是 Kubernetes 用于存储各种配置文件的目录。
	KubernetesDir() string
	// PatchesDir 应指向存储组件补丁的目录。
	PatchesDir() string
	// RealManifestPath 返回 kubelet 使用的 "实际" 静态 Pod 清单目录中组件的文件路径。
	RealManifestPath(component string) string
	// RealManifestDir 应指向 kubelet 使用的静态 Pod 清单目录。
	RealManifestDir() string
	// TempManifestPath 返回升级期间为生成新清单而创建的临时目录中组件的文件路径。
	TempManifestPath(component string) string
	// TempManifestDir 应指向升级期间为生成新清单而创建的临时目录。
	TempManifestDir() string
	// BackupManifestPath 返回升级过渡期间用于备份清单的备份目录中组件的文件路径。
	BackupManifestPath(component string) string
	// BackupManifestDir 应指向升级过渡期间用于备份清单的备份目录。
	BackupManifestDir() string
	// BackupEtcdDir 应指向升级过渡期间用于备份 etcd 的目录。
	BackupEtcdDir() string
	// CleanupDirs 清理所有临时目录。
	CleanupDirs() error
}

// KubeStaticPodPathManager 是 StaticPodPathManager 的实际实现，用于升级静态 Pod 集群。
type KubeStaticPodPathManager struct {
	kubernetesDir     string
	patchesDir        string
	realManifestDir   string
	tempManifestDir   string
	backupManifestDir string
	backupEtcdDir     string

	keepManifestDir bool
	keepEtcdDir     bool
}

// NewKubeStaticPodPathManager 创建 KubeStaticPodPathManager 实例。
func NewKubeStaticPodPathManager(kubernetesDir, patchesDir, tempDir, backupDir, backupEtcdDir string, keepManifestDir, keepEtcdDir bool) StaticPodPathManager {
	return &KubeStaticPodPathManager{
		kubernetesDir:     kubernetesDir,
		patchesDir:        patchesDir,
		realManifestDir:   filepath.Join(kubernetesDir, constants.ManifestsSubDirName),
		tempManifestDir:   tempDir,
		backupManifestDir: backupDir,
		backupEtcdDir:     backupEtcdDir,
		keepManifestDir:   keepManifestDir,
		keepEtcdDir:       keepEtcdDir,
	}
}

// NewKubeStaticPodPathManagerUsingTempDirs 创建使用临时目录的 KubeStaticPodPathManager 实例。
func NewKubeStaticPodPathManagerUsingTempDirs(kubernetesDir, patchesDir string, saveManifestsDir, saveEtcdDir bool) (StaticPodPathManager, error) {
	upgradedManifestsDir, err := constants.CreateTempDir(kubernetesDir, "kubeadm-upgraded-manifests")
	if err != nil {
		return nil, err
	}
	backupManifestsDir, err := constants.CreateTimestampDir(kubernetesDir, "kubeadm-backup-manifests")
	if err != nil {
		return nil, err
	}
	backupEtcdDir, err := constants.CreateTimestampDir(kubernetesDir, "kubeadm-backup-etcd")
	if err != nil {
		return nil, err
	}

	return NewKubeStaticPodPathManager(kubernetesDir, patchesDir, upgradedManifestsDir, backupManifestsDir, backupEtcdDir, saveManifestsDir, saveEtcdDir), nil
}

// MoveFile 将文件从 oldPath 移动到 newPath。
func (spm *KubeStaticPodPathManager) MoveFile(oldPath, newPath string) error {
	return filesutil.MoveFile(oldPath, newPath)
}

// KubernetesDir 应指向 Kubernetes 用于存储各种配置文件的目录。
func (spm *KubeStaticPodPathManager) KubernetesDir() string {
	return spm.kubernetesDir
}

// PatchesDir 应指向存储组件补丁的目录。
func (spm *KubeStaticPodPathManager) PatchesDir() string {
	return spm.patchesDir
}

// RealManifestPath 返回 kubelet 使用的 "实际" 静态 Pod 清单目录中组件的文件路径。
func (spm *KubeStaticPodPathManager) RealManifestPath(component string) string {
	return constants.GetStaticPodFilepath(component, spm.realManifestDir)
}

// RealManifestDir 应指向 kubelet 使用的静态 Pod 清单目录。
func (spm *KubeStaticPodPathManager) RealManifestDir() string {
	return spm.realManifestDir
}

// TempManifestPath 返回升级期间为生成新清单而创建的临时目录中组件的文件路径。
func (spm *KubeStaticPodPathManager) TempManifestPath(component string) string {
	return constants.GetStaticPodFilepath(component, spm.tempManifestDir)
}

// TempManifestDir 应指向升级期间为生成新清单而创建的临时目录。
func (spm *KubeStaticPodPathManager) TempManifestDir() string {
	return spm.tempManifestDir
}

// BackupManifestPath 返回升级过渡期间用于备份清单的备份目录中组件的文件路径。
func (spm *KubeStaticPodPathManager) BackupManifestPath(component string) string {
	return constants.GetStaticPodFilepath(component, spm.backupManifestDir)
}

// BackupManifestDir 应指向升级过渡期间用于备份清单的备份目录。
func (spm *KubeStaticPodPathManager) BackupManifestDir() string {
	return spm.backupManifestDir
}

// BackupEtcdDir 应指向升级过渡期间用于备份 etcd 的目录。
func (spm *KubeStaticPodPathManager) BackupEtcdDir() string {
	return spm.backupEtcdDir
}

// CleanupDirs 清理所有临时目录，但保留用户要求保留的目录。
func (spm *KubeStaticPodPathManager) CleanupDirs() error {
	var errlist []error
	if err := os.RemoveAll(spm.TempManifestDir()); err != nil {
		errlist = append(errlist, err)
	}
	if !spm.keepManifestDir {
		if err := os.RemoveAll(spm.BackupManifestDir()); err != nil {
			errlist = append(errlist, err)
		}
	}

	if !spm.keepEtcdDir {
		if err := os.RemoveAll(spm.BackupEtcdDir()); err != nil {
			errlist = append(errlist, err)
		}
	}

	return utilerrors.NewAggregate(errlist)
}

func upgradeComponent(component string, certsRenewMgr *renewal.Manager, waiter apiclient.Waiter, pathMgr StaticPodPathManager, cfg *kubeadmapi.InitConfiguration, beforePodHash string, recoverManifests map[string]string) error {
	// etcd 需要特殊处理：rollbackOldManifests 仅在组件为 Etcd 时回滚 etcd 清单。
	// 同时比较 kube-apiserver 的清单，判断其是否需要升级，进而决定能否续期 etcd 证书并重启 etcd。
	recoverEtcd := false
	apiServerUpgrade := false
	if component == constants.Etcd {
		recoverEtcd = true
		equal, _, err := staticpod.ManifestFilesAreEqual(pathMgr.RealManifestPath(constants.KubeAPIServer),
			pathMgr.TempManifestPath(constants.KubeAPIServer))
		if err != nil {
			return err
		}
		apiServerUpgrade = !equal
	}

	fmt.Printf("[upgrade/staticpods] Preparing for %q upgrade\n", component)

	// 旧清单位于 /etc/kubernetes/manifests/。
	currentManifestPath := pathMgr.RealManifestPath(component)
	// 新的升级后清单将写入此处。
	newManifestPath := pathMgr.TempManifestPath(component)
	// 旧清单将移动到此处，即临时目录的子目录。
	// 若需要回滚，这些清单将放回原位。
	backupManifestPath := pathMgr.BackupManifestPath(component)

	// 将备份路径存入恢复列表。若此时出错，该组件将被回滚。
	recoverManifests[component] = backupManifestPath

	// 若当前清单与新清单相同，则跳过升级。
	equal, diff, err := staticpod.ManifestFilesAreEqual(currentManifestPath, newManifestPath)
	if err != nil {
		return err
	}
	if equal {
		if !apiServerUpgrade {
			fmt.Printf("[upgrade/staticpods] Current and new manifests of %s are equal, skipping upgrade\n", component)
			return nil
		}
		klog.V(4).Infof("[upgrade/staticpods] Current and new manifests of %s are equal, but %s will be upgraded\n",
			component, constants.KubeAPIServer)
	} else {
		klog.V(4).Infof("Pod manifest files diff:\n%s\n", diff)
	}

	// 判断是否应续期证书。
	if certsRenewMgr != nil {
		// 续期当前组件使用的所有证书。
		if err := renewCertsByComponent(cfg, component, certsRenewMgr); err != nil {
			return rollbackOldManifests(recoverManifests, errors.Wrapf(err, "failed to renew certificates for component %q", component), pathMgr, recoverEtcd)
		}
	}

	// 仅当组件为 etcd、证书续期已启用且 kube-apiserver 需要升级时，才会出现此情况。
	if equal && certsRenewMgr != nil {
		fmt.Printf("[upgrade/staticpods] Restarting the %s static pod and backing up its manifest to %q\n",
			component, backupManifestPath)
	} else {
		fmt.Printf("[upgrade/staticpods] Moving new manifest to %q and backing up old manifest to %q\n",
			currentManifestPath, backupManifestPath)
	}

	// 将旧清单移动到 old-manifests 目录。
	if err := pathMgr.MoveFile(currentManifestPath, backupManifestPath); err != nil {
		return rollbackOldManifests(recoverManifests, err, pathMgr, recoverEtcd)
	}

	// 将新清单移动到 manifests 目录。
	if err := pathMgr.MoveFile(newManifestPath, currentManifestPath); err != nil {
		return rollbackOldManifests(recoverManifests, err, pathMgr, recoverEtcd)
	}

	fmt.Println("[upgrade/staticpods] Waiting for the kubelet to restart the component")
	fmt.Printf("[upgrade/staticpods] This can take up to %v\n", kubeadmapi.GetActiveTimeouts().UpgradeManifests.Duration)

	// 等待镜像 Pod 的哈希值改变；否则，kubelet 尚未及时注意到 Static Pod 被移除时，
	// 下面的 API 端点健康检查可能出现误判。
	// 若不等待，可能在移除 Static Pod 清单后，kubelet 反应迟缓，而 kubeadm 已检查旧组件的 API 端点并继续执行，
	// 从而导致意外结果。
	if !equal {
		if err := waiter.WaitForStaticPodHashChange(cfg.NodeRegistration.Name, component, beforePodHash); err != nil {
			return rollbackOldManifests(recoverManifests, err, pathMgr, recoverEtcd)
		}
	}

	// 等待静态 Pod 组件启动并注册为镜像 Pod。
	if err := waiter.WaitForPodsWithLabel("component=" + component); err != nil {
		return rollbackOldManifests(recoverManifests, err, pathMgr, recoverEtcd)
	}

	fmt.Printf("[upgrade/staticpods] Component %q upgraded successfully!\n", component)

	return nil
}

// performEtcdStaticPodUpgrade 升级 etcd，并返回表示是否发生致命错误的布尔值及实际错误。
func performEtcdStaticPodUpgrade(certsRenewMgr *renewal.Manager, client clientset.Interface, waiter apiclient.Waiter, pathMgr StaticPodPathManager, cfg *kubeadmapi.InitConfiguration, recoverManifests map[string]string, oldEtcdClient, newEtcdClient etcdutil.ClusterInterrogator) (bool, error) {
	// 仅在未配置外部 etcd 时添加 etcd 静态 Pod 配置。
	if cfg.Etcd.External != nil {
		return false, errors.New("external etcd detected, won't try to change any etcd state")
	}

	// 升级前检查 etcd 健康状态。
	err := oldEtcdClient.CheckClusterHealth()
	if err != nil {
		return true, errors.Wrap(err, "etcd cluster is not healthy")
	}

	// 备份 etcd 数据存储。
	backupEtcdDir := pathMgr.BackupEtcdDir()
	runningEtcdDir := cfg.Etcd.Local.DataDir
	var (
		attempt   int
		lastError error
	)
	err = wait.PollUntilContextTimeout(context.Background(), time.Second, 30*time.Second, true,
		func(_ context.Context) (bool, error) {
			attempt++
			klog.V(2).Infof("[upgrade/etcd] Backing up etcd data, attempt %d", attempt)
			if err := os.RemoveAll(backupEtcdDir); err != nil {
				lastError = errors.Wrapf(err, "could not clear the etcd backup dir %s", backupEtcdDir)
				return false, lastError
			}
			if err := os.Mkdir(backupEtcdDir, 0700); err != nil {
				lastError = errors.Wrapf(err, "could not create the etcd backup dir %s", backupEtcdDir)
				return false, lastError
			}
			lastError = filesutil.CopyDir(runningEtcdDir, backupEtcdDir)
			return lastError == nil, nil
		})
	if err != nil {
		if lastError != nil {
			err = lastError
		}
		return true, errors.Wrapf(err, "failed to back up etcd data after %d attempts", attempt)
	}

	// 获取期望的 etcd 版本：用户在 cfg.Etcd.Local.ImageTag 中指定的版本，
	// 或 kubeadm 为期望 Kubernetes 版本推荐的版本。
	var desiredEtcdVersion *version.Version
	if cfg.Etcd.Local.ImageTag != "" {
		desiredEtcdVersion, err = version.ParseSemantic(
			convertImageTagMetadataToSemver(cfg.Etcd.Local.ImageTag))
		if err != nil {
			return true, errors.Wrapf(err, "failed to parse tag %q as a semantic version", cfg.Etcd.Local.ImageTag)
		}
	} else {
		// 比较当前版本与常量中的版本；若不同，则升级。
		var warning error
		desiredEtcdVersion, warning, err = constants.EtcdSupportedVersion(constants.SupportedEtcdVersion, cfg.KubernetesVersion)
		if err != nil {
			return true, errors.Wrap(err, "failed to retrieve an etcd version for the target Kubernetes version")
		}
		if warning != nil {
			klog.V(1).Infof("[upgrade/etcd] WARNING: %v", warning)
		}
	}

	// 获取当前机器上本地或堆叠 etcd 成员的版本。
	currentEtcdVersionStr, err := GetEtcdImageTagFromStaticPod(pathMgr.RealManifestDir())
	if err != nil {
		return true, errors.Wrap(err, "failed to retrieve the current etcd version")
	}

	cmpResult, err := desiredEtcdVersion.Compare(currentEtcdVersionStr)
	if err != nil {
		return true, errors.Wrapf(err, "failed comparing the current etcd version %q to the desired one %q", currentEtcdVersionStr, desiredEtcdVersion)
	}
	if cmpResult < 0 {
		return false, errors.Errorf("the desired etcd version %q is older than the currently installed %q. Skipping etcd upgrade", desiredEtcdVersion, currentEtcdVersionStr)
	}

	beforeEtcdPodHash, err := waiter.WaitForStaticPodSingleHash(cfg.NodeRegistration.Name, constants.Etcd)
	if err != nil {
		return true, err
	}

	// 将更新后的 etcd 静态 Pod 清单写入临时目录；此时 etcd 尚未发生任何变更。
	if err := etcdphase.CreateLocalEtcdStaticPodManifestFile(pathMgr.TempManifestDir(), pathMgr.PatchesDir(), cfg.NodeRegistration.Name, &cfg.ClusterConfiguration, &cfg.LocalAPIEndpoint, false /* 是否试运行 */); err != nil {
		return true, errors.Wrap(err, "error creating local etcd static pod manifest file")
	}

	retries := 10
	retryInterval := 15 * time.Second

	// 使用控制平面组件通用函数升级 etcd。
	if err := upgradeComponent(constants.Etcd, certsRenewMgr, waiter, pathMgr, cfg, beforeEtcdPodHash, recoverManifests); err != nil {
		fmt.Printf("[upgrade/etcd] Failed to upgrade etcd: %v\n", err)
		// 由于组件升级失败，旧 etcd 清单已恢复或从未修改。
		// 现在检查使用旧清单的 etcd 集群是否健康。
		fmt.Println("[upgrade/etcd] Waiting for previous etcd to become available")
		if _, err := oldEtcdClient.WaitForClusterAvailable(retries, retryInterval); err != nil {
			fmt.Printf("[upgrade/etcd] Failed to healthcheck previous etcd: %v\n", err)

			// 此时可知 etcd 集群已不可用，可以复制备份数据存储并回滚旧 etcd 清单。
			fmt.Println("[upgrade/etcd] Rolling back etcd data")
			if err := rollbackEtcdData(cfg, pathMgr); err != nil {
				// 若复制回数据存储也失败，则已无恢复选项，退出。
				return true, errors.Errorf("fatal error rolling back local etcd cluster datadir: %v, the backup of etcd database is stored here:(%s)", err, backupEtcdDir)
			}
			fmt.Println("[upgrade/etcd] Etcd data rollback successful")

			// 数据已回滚，现在检查集群能否启动。
			fmt.Println("[upgrade/etcd] Waiting for previous etcd to become available")
			if _, err := oldEtcdClient.WaitForClusterAvailable(retries, retryInterval); err != nil {
				fmt.Printf("[upgrade/etcd] Failed to healthcheck previous etcd: %v\n", err)
				// 已无其他方法可恢复 etcd 集群。
				return true, errors.Wrapf(err, "fatal error rolling back local etcd cluster manifest, the backup of etcd database is stored here:(%s)", backupEtcdDir)
			}

			// 本次已恢复到升级前的 etcd。
		}
		fmt.Println("[upgrade/etcd] Etcd was rolled back and is now available")

		// 由于 etcd 集群已使用旧清单恢复，返回升级失败错误。
		return true, errors.Wrap(err, "fatal error when trying to upgrade the etcd cluster, rolled the state back to pre-upgrade state")
	}

	// 若新 etcd 客户端尚未初始化，则进行初始化。
	if newEtcdClient == nil {
		etcdClient, err := etcdutil.NewFromCluster(client, cfg.CertificatesDir)
		if err != nil {
			return true, errors.Wrap(err, "fatal error creating etcd client")
		}
		newEtcdClient = etcdClient
	}

	// 升级后检查 etcd 健康状态。
	fmt.Println("[upgrade/etcd] Waiting for etcd to become available")
	if _, err = newEtcdClient.WaitForClusterAvailable(retries, retryInterval); err != nil {
		fmt.Printf("[upgrade/etcd] Failed to healthcheck etcd: %v\n", err)
		// 尽管 upgradeComponent 已成功，etcd 集群仍有异常。
		// 首先恢复数据存储备份。
		fmt.Println("[upgrade/etcd] Rolling back etcd data")
		if err := rollbackEtcdData(cfg, pathMgr); err != nil {
			// 若复制回数据存储也失败，则已无恢复选项，退出。
			return true, errors.Wrapf(err, "fatal error rolling back local etcd cluster datadir, the backup of etcd database is stored here:(%s)", backupEtcdDir)
		}
		fmt.Println("[upgrade/etcd] Etcd data rollback successful")

		// 旧数据存储已复制，正在回滚旧清单。
		fmt.Println("[upgrade/etcd] Rolling back etcd manifest")
		rollbackOldManifests(recoverManifests, err, pathMgr, true)
		// rollbackOldManifests() 始终返回错误；忽略该错误并继续。

		// 假定旧 etcd 清单回滚成功，再次检查 etcd 集群状态。
		fmt.Println("[upgrade/etcd] Waiting for previous etcd to become available")
		if _, err := oldEtcdClient.WaitForClusterAvailable(retries, retryInterval); err != nil {
			fmt.Printf("[upgrade/etcd] Failed to healthcheck previous etcd: %v\n", err)
			// 已无其他方法可恢复 etcd 集群。
			return true, errors.Wrapf(err, "fatal error rolling back local etcd cluster manifest, the backup of etcd database is stored here:(%s)", backupEtcdDir)
		}
		fmt.Println("[upgrade/etcd] Etcd was rolled back and is now available")

		// etcd 已成功回滚；现在返回描述升级失败的错误。
		return true, errors.Wrap(err, "fatal error upgrading local etcd cluster, rolled the state back to pre-upgrade state")
	}

	return false, nil
}

// StaticPodControlPlane 升级由静态 Pod 承载的控制平面。
func StaticPodControlPlane(client clientset.Interface, waiter apiclient.Waiter, pathMgr StaticPodPathManager, cfg *kubeadmapi.InitConfiguration, etcdUpgrade, renewCerts bool, oldEtcdClient, newEtcdClient etcdutil.ClusterInterrogator) error {
	recoverManifests := map[string]string{}
	var isExternalEtcd bool

	beforePodHashMap, err := waiter.WaitForStaticPodControlPlaneHashes(cfg.NodeRegistration.Name)
	if err != nil {
		return err
	}

	if oldEtcdClient == nil {
		if cfg.Etcd.External != nil {
			// 外部 etcd
			isExternalEtcd = true
			etcdClient, err := etcdutil.New(
				cfg.Etcd.External.Endpoints,
				cfg.Etcd.External.CAFile,
				cfg.Etcd.External.CertFile,
				cfg.Etcd.External.KeyFile,
			)
			if err != nil {
				return errors.Wrap(err, "failed to create etcd client for external etcd")
			}
			oldEtcdClient = etcdClient
			// 由于 etcd 由外部管理，新 etcd 客户端与旧客户端相同。
			if newEtcdClient == nil {
				newEtcdClient = etcdClient
			}
		} else {
			// etcd 静态 Pod。
			etcdClient, err := etcdutil.NewFromCluster(client, cfg.CertificatesDir)
			if err != nil {
				return errors.Wrap(err, "failed to create etcd client")
			}
			oldEtcdClient = etcdClient
		}
	}

	var certsRenewMgr *renewal.Manager
	if renewCerts {
		certsRenewMgr, err = renewal.NewManager(&cfg.ClusterConfiguration, pathMgr.KubernetesDir())
		if err != nil {
			return errors.Wrap(err, "failed to create the certificate renewal manager")
		}
	}

	// 将更新后的静态 Pod 清单写入临时目录。
	// 根据设计，必须在下面的 performEtcdStaticPodUpgrade() 调用之前执行。
	fmt.Printf("[upgrade/staticpods] Writing new Static Pod manifests to %q\n", pathMgr.TempManifestDir())
	err = controlplane.CreateInitStaticPodManifestFiles(pathMgr.TempManifestDir(), pathMgr.PatchesDir(), cfg, false /* 是否试运行 */)
	if err != nil {
		return errors.Wrap(err, "error creating init static pod manifest files")
	}

	// 先升级 etcd，再升级其他控制平面组件。
	if !isExternalEtcd && etcdUpgrade {
		// 使用控制平面组件通用函数升级 etcd。
		fatal, err := performEtcdStaticPodUpgrade(certsRenewMgr, client, waiter, pathMgr, cfg, recoverManifests, oldEtcdClient, newEtcdClient)
		if err != nil {
			if fatal {
				return err
			}
			fmt.Printf("[upgrade/etcd] Non fatal issue encountered during upgrade: %v\n", err)
		}
	}

	// 升级其余控制平面组件。
	for _, component := range constants.ControlPlaneComponents {
		if err = upgradeComponent(component, certsRenewMgr, waiter, pathMgr, cfg, beforePodHashMap[component], recoverManifests); err != nil {
			return err
		}
	}

	if renewCerts {
		// 续期 admin.conf 中嵌入的证书。
		renewed, err := certsRenewMgr.RenewUsingLocalCA(constants.AdminKubeConfigFileName)
		if err != nil {
			return rollbackOldManifests(recoverManifests, errors.Wrapf(err, "failed to upgrade the %s certificates", constants.AdminKubeConfigFileName), pathMgr, false)
		}

		if !renewed {
			// 若未报错但因检测到外部 CA 而未续期，则告知用户。
			fmt.Printf("[upgrade/staticpods] External CA detected, %s certificate can't be renewed\n", constants.AdminKubeConfigFileName)
		}

		// 对 super-admin.conf 执行相同操作，但仅在其存在时进行。
		if _, err := os.Stat(filepath.Join(pathMgr.KubernetesDir(), constants.SuperAdminKubeConfigFileName)); err == nil {
			// 续期 super-admin.conf 中嵌入的证书。
			renewed, err := certsRenewMgr.RenewUsingLocalCA(constants.SuperAdminKubeConfigFileName)
			if err != nil {
				return rollbackOldManifests(recoverManifests, errors.Wrapf(err, "failed to upgrade the %s certificates", constants.SuperAdminKubeConfigFileName), pathMgr, false)
			}

			if !renewed {
				// 若未报错但因检测到外部 CA 而未续期，则告知用户。
				fmt.Printf("[upgrade/staticpods] External CA detected, %s certificate can't be renewed\n", constants.SuperAdminKubeConfigFileName)
			}
		}
	}

	// 尽力删除使用的临时目录；删除失败不使升级失败。
	// 按设计在此处调用，而不在上方使用 "defer"，否则在 "失败并回滚" 场景下也会删除目录，
	// 而用户可能希望保留这些目录。
	return pathMgr.CleanupDirs()
}

// rollbackOldManifests 在出错时回滚已备份的清单。
// 它始终向调用方返回错误。
func rollbackOldManifests(oldManifests map[string]string, origErr error, pathMgr StaticPodPathManager, restoreEtcd bool) error {
	errs := []error{origErr}
	for component, backupPath := range oldManifests {
		// 仅当通过将 restoreEtcd 设置为 true 明确要求时，才恢复 etcd 清单。
		if component == constants.Etcd && !restoreEtcd {
			continue
		}
		// 备份清单应放回的位置。
		realManifestPath := pathMgr.RealManifestPath(component)

		// 将备份清单移回 manifests 目录。
		err := pathMgr.MoveFile(backupPath, realManifestPath)
		if err != nil {
			errs = append(errs, err)
		}
	}
	// 告知用户曾出现问题，但已尝试恢复。
	return errors.Wrap(utilerrors.NewAggregate(errs),
		"couldn't upgrade control plane. kubeadm has tried to recover everything into the earlier state. Errors faced")
}

// rollbackEtcdData 在出错时回滚 etcd 目录内容。
// 回滚成功返回 nil，否则返回错误。
func rollbackEtcdData(cfg *kubeadmapi.InitConfiguration, pathMgr StaticPodPathManager) error {
	backupEtcdDir := pathMgr.BackupEtcdDir()
	runningEtcdDir := cfg.Etcd.Local.DataDir

	// 只有在存在可恢复备份时，清空当前成员目录才安全。
	if _, err := os.Stat(filepath.Join(backupEtcdDir, "member")); err != nil {
		return errors.Wrapf(err, "will not roll back the etcd data dir, no member dir in the etcd backup: %s", backupEtcdDir)
	}

	// 备份后由 etcd 写入的文件不得在回滚后保留。
	if err := os.RemoveAll(filepath.Join(runningEtcdDir, "member")); err != nil {
		return errors.Wrapf(err, "could not clear the etcd member dir in %s", runningEtcdDir)
	}

	if err := filesutil.CopyDir(backupEtcdDir, runningEtcdDir); err != nil {
		return errors.Wrapf(err, "couldn't recover etcd database with error, the location of etcd backup: %s", backupEtcdDir)
	}

	return nil
}

// renewCertsByComponent 负责在组件静态 Pod 升级前续期该组件使用的证书。
func renewCertsByComponent(cfg *kubeadmapi.InitConfiguration, component string, certsRenewMgr *renewal.Manager) error {
	var certificates []string

	// 若为 etcd，则仅在本地 etcd 情况下续期服务器、对端和健康检查证书。
	if component == constants.Etcd {
		if cfg.Etcd.Local != nil {
			certificates = []string{
				certsphase.KubeadmCertEtcdServer().Name,
				certsphase.KubeadmCertEtcdPeer().Name,
				certsphase.KubeadmCertEtcdHealthcheck().Name,
			}
		}
	}

	// 若为 apiserver，则续期 apiserver 服务证书、kubelet 客户端证书和 front-proxy 客户端证书。
	// 若为本地 etcd，还要续期 etcd 客户端证书。
	if component == constants.KubeAPIServer {
		certificates = []string{
			certsphase.KubeadmCertAPIServer().Name,
			certsphase.KubeadmCertKubeletClient().Name,
			certsphase.KubeadmCertFrontProxyClient().Name,
		}
		if cfg.Etcd.Local != nil {
			certificates = append(certificates, certsphase.KubeadmCertEtcdAPIClient().Name)
		}
	}

	// 若为 controller-manager，则续期 controller-manager kubeConfig 文件中嵌入的证书。
	if component == constants.KubeControllerManager {
		certificates = []string{
			constants.ControllerManagerKubeConfigFileName,
		}
	}

	// 若为 scheduler，则续期 scheduler kubeConfig 文件中嵌入的证书。
	if component == constants.KubeScheduler {
		certificates = []string{
			constants.SchedulerKubeConfigFileName,
		}
	}

	// 续期所选组件的证书。
	for _, cert := range certificates {
		fmt.Printf("[upgrade/staticpods] Renewing %s certificate\n", cert)
		renewed, err := certsRenewMgr.RenewUsingLocalCA(cert)
		if err != nil {
			return err
		}
		if !renewed {
			// 若未报错但因检测到外部 CA 而未续期，则告知用户。
			fmt.Printf("[upgrade/staticpods] External CA detected, %s certificate can't be renewed\n", cert)
		}
	}

	return nil
}

// GetPathManagerForUpgrade 返回为给定 InitConfiguration 正确配置的路径管理器。
func GetPathManagerForUpgrade(kubernetesDir, patchesDir string, internalcfg *kubeadmapi.InitConfiguration, etcdUpgrade bool) (StaticPodPathManager, error) {
	isExternalEtcd := internalcfg.Etcd.External != nil
	return NewKubeStaticPodPathManagerUsingTempDirs(kubernetesDir, patchesDir, true, etcdUpgrade && !isExternalEtcd)
}

// PerformStaticPodUpgrade 升级静态 Pod 集群的控制平面组件。
func PerformStaticPodUpgrade(client clientset.Interface, waiter apiclient.Waiter, internalcfg *kubeadmapi.InitConfiguration, etcdUpgrade, renewCerts bool, patchesDir string) error {
	pathManager, err := GetPathManagerForUpgrade(constants.KubernetesDir, patchesDir, internalcfg, etcdUpgrade)
	if err != nil {
		return err
	}

	// oldEtcdClient 和 newEtdClient 参数未初始化，因为传入客户端便于在测试中模拟客户端。
	return StaticPodControlPlane(client, waiter, pathManager, internalcfg, etcdUpgrade, renewCerts, nil, nil)
}

// DryRunStaticPodUpgrade 模拟控制平面升级。
func DryRunStaticPodUpgrade(patchesDir string, internalcfg *kubeadmapi.InitConfiguration) error {
	dryRunManifestDir, err := constants.GetDryRunDir(constants.EnvVarUpgradeDryRunDir, "kubeadm-upgrade-dryrun", klog.Warningf)
	if err != nil {
		return err
	}

	defer os.RemoveAll(dryRunManifestDir)
	if err := controlplane.CreateInitStaticPodManifestFiles(dryRunManifestDir, patchesDir, internalcfg, true /* 是否试运行 */); err != nil {
		return err
	}

	// 打印升级后的清单内容，并假装它们位于 /etc/kubernetes/manifests。
	files := []dryrunutil.FileToPrint{}
	for _, component := range constants.ControlPlaneComponents {
		realPath := constants.GetStaticPodFilepath(component, dryRunManifestDir)
		outputPath := constants.GetStaticPodFilepath(component, constants.GetStaticPodDirectory())
		files = append(files, dryrunutil.NewFileToPrint(realPath, outputPath))
	}

	return dryrunutil.PrintDryRunFiles(files, os.Stdout)
}

// GetEtcdImageTagFromStaticPod 返回本地 etcd 静态 Pod 的镜像标签。
func GetEtcdImageTagFromStaticPod(manifestDir string) (string, error) {
	realPath := constants.GetStaticPodFilepath(constants.Etcd, manifestDir)
	pod, err := staticpod.ReadStaticPodFromDisk(realPath)
	if err != nil {
		return "", err
	}

	return convertImageTagMetadataToSemver(image.TagFromImage(pod.Spec.Containers[0].Image)), nil
}

// convertImageTagMetadataToSemver 将 semver_metadata 格式的镜像标签转换为 semver+metadata 格式。
func convertImageTagMetadataToSemver(tag string) string {
	// 容器注册表不支持标签名称中的 `+` 字符，因此镜像标签无法正确表示使用加号分隔构建元数据的语义版本。
	// Kubernetes 约定在镜像注册表中使用下划线，以在镜像标签中保留构建元数据。
	return strings.Replace(tag, "_", "+", 1)
}
