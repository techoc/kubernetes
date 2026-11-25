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
	"fmt"
	"strings"

	versionutil "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/klog/v2"

	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
)

// Upgrade 描述从当前版本升级到新版本的一种可能。
type Upgrade struct {
	Description string
	Before      ClusterState
	After       ClusterState
}

// CanUpgradeKubelets 返回集群中的 kubelet 是否可以升级。
func (u *Upgrade) CanUpgradeKubelets() bool {
	// 若当前存在多个不同版本，则可以升级，即使仅涉及部分节点。
	if len(u.Before.KubeletVersions) > 1 {
		return true
	}
	// 若不知道当前状态，则不报告可升级项。
	if len(u.Before.KubeletVersions) == 0 {
		return false
	}

	// 若升级前后版本号相同，则无需升级。
	_, sameVersionFound := u.Before.KubeletVersions[u.After.KubeVersion]
	return !sameVersionFound
}

// ClusterState 描述集群在升级期间各组件的版本状态。
type ClusterState struct {
	// KubeVersion 描述集群中最新 Kubernetes API 服务器的版本。
	KubeVersion string
	// DNSVersion 描述 DNS 插件的版本。
	DNSVersion string
	// KubeadmVersion 描述 kubeadm CLI 的版本。
	KubeadmVersion string
	// EtcdVersion 表示集群使用的 etcd 版本。
	EtcdVersion string

	// 以下映射描述集群中各组件的版本。
	// 键为版本字符串，值为使用该版本的节点列表。
	KubeAPIServerVersions         map[string][]string
	KubeControllerManagerVersions map[string][]string
	KubeSchedulerVersions         map[string][]string
	EtcdVersions                  map[string][]string
	KubeletVersions               map[string][]string
}

// GetAvailableUpgrades 从指定的 VersionGetter 获取所有版本，并计算可执行的升级类型。
func GetAvailableUpgrades(versionGetterImpl VersionGetter, experimentalUpgradesAllowed, rcUpgradesAllowed bool, printer output.Printer) ([]Upgrade, error) {
	printer.Printf("[upgrade] Fetching available versions to upgrade to\n")

	// 将 kubeadm 可执行的升级收集到此列表。
	var upgrades []Upgrade

	// 获取集群中的 kube-apiserver 版本。
	kubeAPIServerVersions, err := versionGetterImpl.ComponentVersions(kubeadmconstants.KubeAPIServer)
	if err != nil {
		return upgrades, err
	}
	if len(kubeAPIServerVersions) > 1 {
		verMsg := []string{}
		for version, nodes := range kubeAPIServerVersions {
			verMsg = append(verMsg, fmt.Sprintf("%s on nodes %v", version, nodes))
		}
		klog.Warningf("Different API server versions in the cluster were discovered: %v. Please upgrade your control plane"+
			" nodes to the same version of Kubernetes", strings.Join(verMsg, ", "))
	}

	// 获取最新集群版本。
	clusterVersion, err := getLatestClusterVersion(kubeAPIServerVersions)
	if err != nil {
		return upgrades, err
	}
	clusterVersionStr := clusterVersion.String()

	printer.Printf("[upgrade/versions] Cluster version: %s\n", clusterVersionStr)

	// 获取当前 kubeadm CLI 版本。
	kubeadmVersionStr, kubeadmVersion, err := versionGetterImpl.KubeadmVersion()
	if err != nil {
		return upgrades, err
	}
	printer.Printf("[upgrade/versions] kubeadm version: %s\n", kubeadmVersionStr)

	// 获取并输出当前最新稳定版本。
	stableVersionStr, stableVersion, err := versionGetterImpl.VersionFromCILabel("stable", "stable version")
	if err != nil {
		klog.Warningf("[upgrade/versions] WARNING: %v\n", err)
		klog.Warningf("[upgrade/versions] WARNING: Falling back to current kubeadm version as latest stable version")
		stableVersionStr, stableVersion = kubeadmVersionStr, kubeadmVersion
	} else {
		printer.Printf("[upgrade/versions] Target version: %s\n", stableVersionStr)
	}

	// 获取集群中的 kubelet 版本。
	kubeletVersions, err := versionGetterImpl.KubeletVersions()
	if err != nil {
		return upgrades, err
	}

	// 获取集群中的 kube-controller-manager 版本。
	kubeControllerManagerVersions, err := versionGetterImpl.ComponentVersions(kubeadmconstants.KubeControllerManager)
	if err != nil {
		return upgrades, err
	}

	// 获取集群中的 kube-scheduler 版本。
	kubeSchedulerVersions, err := versionGetterImpl.ComponentVersions(kubeadmconstants.KubeScheduler)
	if err != nil {
		return upgrades, err
	}

	// 获取集群中的 etcd 版本。
	etcdVersions, err := versionGetterImpl.ComponentVersions(kubeadmconstants.Etcd)
	if err != nil {
		return upgrades, err
	}
	isExternalEtcd := len(etcdVersions) == 0

	dnsVersion, err := versionGetterImpl.DNSAddonVersion()
	if err != nil {
		return nil, err
	}

	// 构建当前集群状态的描述符。
	beforeState := ClusterState{
		KubeVersion:                   clusterVersionStr,
		DNSVersion:                    dnsVersion,
		KubeadmVersion:                kubeadmVersionStr,
		KubeAPIServerVersions:         kubeAPIServerVersions,
		KubeControllerManagerVersions: kubeControllerManagerVersions,
		KubeSchedulerVersions:         kubeSchedulerVersions,
		KubeletVersions:               kubeletVersions,
		EtcdVersions:                  etcdVersions,
	}

	// 仅因最新稳定版本高于集群版本，便初步推测存在新的次版本升级。
	// 确认是否存在补丁版本后，将修正此推测。
	canDoMinorUpgrade := clusterVersion.LessThan(stableVersion)

	// 若集群版本高于或等于当前稳定版本，则不存在补丁版本。
	// 例如用户尝试从 v1.8.0-beta.2 升级到 v1.8.0-rc.1（假定我们以实验方式支持此类升级），
	// 此时 stable-1.8 分支尚不存在，因此需要此检查。
	if patchVersionBranchExists(clusterVersion, stableVersion) {
		currentBranch := getBranchFromVersion(clusterVersionStr)
		versionLabel := fmt.Sprintf("stable-%s", currentBranch)
		description := fmt.Sprintf("version in the v%s series", currentBranch)

		// 获取并输出集群对应分支的最新补丁版本。
		patchVersionStr, patchVersion, err := versionGetterImpl.VersionFromCILabel(versionLabel, description)
		if err != nil {
			klog.Warningf("[upgrade/versions] WARNING: %v\n", err)
		} else {
			printer.Printf("[upgrade/versions] Latest %s: %s\n", description, patchVersionStr)

			// 当存在补丁版本时，检查是否可以升级次版本。
			// 仅当最新补丁版本高于当前补丁版本时才可行。
			// 若满足条件，两者必属不同分支，因此可升级到更新的次版本。
			canDoMinorUpgrade = minorUpgradePossibleWithPatchRelease(stableVersion, patchVersion)

			// 若集群版本低于最新补丁版本，则提示存在可升级版本。
			if patchUpgradePossible(clusterVersion, patchVersion) {

				// kubeadm 版本必须升级到最新补丁版本。
				newKubeadmVer := patchVersionStr
				if kubeadmVersion.AtLeast(patchVersion) {
					// 此时 kubeadm CLI 版本已足够新。使 .NewKubeadmVersion 等于 .CurrentKubeadmVersion，避免显示 kubeadm 更新建议。
					newKubeadmVer = kubeadmVersionStr
				}

				upgrades = append(upgrades, Upgrade{
					Description: description,
					Before:      beforeState,
					After: ClusterState{
						KubeVersion:    patchVersionStr,
						DNSVersion:     kubeadmconstants.CoreDNSVersion,
						KubeadmVersion: newKubeadmVer,
						EtcdVersion:    getSuggestedEtcdVersion(isExternalEtcd, patchVersionStr),
					},
				})
			}
		}
	}

	if canDoMinorUpgrade {
		upgrades = append(upgrades, Upgrade{
			Description: "stable version",
			Before:      beforeState,
			After: ClusterState{
				KubeVersion:    stableVersionStr,
				DNSVersion:     kubeadmconstants.CoreDNSVersion,
				KubeadmVersion: stableVersionStr,
				EtcdVersion:    getSuggestedEtcdVersion(isExternalEtcd, stableVersionStr),
			},
		})
	}

	if experimentalUpgradesAllowed || rcUpgradesAllowed {
		// dl.k8s.io/release/latest.txt 始终为 alpha.X 版本。
		// dl.k8s.io/release/latest-1.X.txt 先指向 v1.X.0-alpha.0 至 v1.X.0-alpha.Y，
		// 再指向 v1.X.0-beta.0 至 v1.X.0-beta.Z，最后指向 v1.X.0-rc.1 至 v1.X.0-rc.W。
		// v1.X.0 发布后，latest-1.X.txt 始终为 beta.0 版本。例如 v1.7 分支最新稳定版本为 v1.7.3，
		// 则 latest-1.7 为 v1.7.4-beta.0。

		// 需注意，创建 release-1.X 分支时会标记两个版本：v1.X.0-beta.0 和 v1.(X+1).alpha.0。
		// v1.(X+1).alpha.0 基本无用，应忽略，因为后续 beta 版本可能比初始 alpha 版本包含更多功能。

		// 因此，下面先获取最新整体版本，其始终为 v1.X.0-alpha.Y；再获取 latest-1.(X-1)。
		// 该版本可能介于 v1.(X-1).0-beta.0 和 v1.(X-1).Z-beta.0 之间。
		// 当 latest-1.(X-1) 指向 v1.(X-1).0-rc.1 时，应将其展示出来。

		// 流程如下（横轴为时间）：
		// v1.8.0-alpha.1 -> v1.8.0-alpha.2 -> v1.8.0-alpha.3 | release-1.8 branch | v1.8.0-beta.0 -> v1.8.0-beta.1 -> v1.8.0-beta.2 -> v1.8.0-rc.1 -> v1.8.0 -> v1.8.1
		//                                                                           v1.9.0-alpha.0                                             -> v1.9.0-alpha.1 -> v1.9.0-alpha.2

		// 获取并输出当前最新不稳定版本。
		latestVersionStr, latestVersion, err := versionGetterImpl.VersionFromCILabel("latest", "experimental version")
		if err != nil {
			return upgrades, err
		}
		_, _ = printer.Printf("[upgrade/versions] Latest %s: %s\n", "experimental version", latestVersionStr)

		minorUnstable := latestVersion.Components()[1]
		// 获取并输出当前最新不稳定版本。
		previousBranch := fmt.Sprintf("latest-1.%d", minorUnstable-1)
		previousBranchLatestVersionStr, previousBranchLatestVersion, err := versionGetterImpl.VersionFromCILabel(previousBranch, "previous version")
		if err != nil {
			return upgrades, err
		}
		_, _ = printer.Printf("[upgrade/versions] Latest %s: %s\n", "previous version", previousBranchLatestVersionStr)

		// 若上一最新版本为 RC，且允许 RC、集群版本低于该 RC 版本，则展示此升级。
		if rcUpgradesAllowed && rcUpgradePossible(clusterVersion, previousBranchLatestVersion) {
			upgrades = append(upgrades, Upgrade{
				Description: "release candidate version",
				Before:      beforeState,
				After: ClusterState{
					KubeVersion:    previousBranchLatestVersionStr,
					DNSVersion:     kubeadmconstants.CoreDNSVersion,
					KubeadmVersion: previousBranchLatestVersionStr,
					EtcdVersion:    getSuggestedEtcdVersion(isExternalEtcd, previousBranchLatestVersionStr),
				},
			})
		}

		// 若允许实验性升级，则展示该升级可能性。
		if experimentalUpgradesAllowed && clusterVersion.LessThan(latestVersion) {

			// 默认假定要展示的实验性版本为不稳定版本。
			unstableKubeVersion := latestVersionStr

			// 不应展示 alpha.0。由于 kube 分支流程，上一分支的 beta/rc 版本更具相关性。
			if latestVersion.PreRelease() == "alpha.0" {
				unstableKubeVersion = previousBranchLatestVersionStr
			}

			upgrades = append(upgrades, Upgrade{
				Description: "experimental version",
				Before:      beforeState,
				After: ClusterState{
					KubeVersion:    unstableKubeVersion,
					DNSVersion:     kubeadmconstants.CoreDNSVersion,
					KubeadmVersion: unstableKubeVersion,
					EtcdVersion:    getSuggestedEtcdVersion(isExternalEtcd, unstableKubeVersion),
				},
			})
		}
	}

	// 在此输出末尾添加换行，与下一输出部分留出间距。
	printer.Println()

	return upgrades, nil
}

func getBranchFromVersion(version string) string {
	v := versionutil.MustParseGeneric(version)
	return fmt.Sprintf("%d.%d", v.Major(), v.Minor())
}

func patchVersionBranchExists(clusterVersion, stableVersion *versionutil.Version) bool {
	return stableVersion.AtLeast(clusterVersion)
}

func patchUpgradePossible(clusterVersion, patchVersion *versionutil.Version) bool {
	return clusterVersion.LessThan(patchVersion)
}

func rcUpgradePossible(clusterVersion, previousBranchLatestVersion *versionutil.Version) bool {
	return strings.HasPrefix(previousBranchLatestVersion.PreRelease(), "rc") && clusterVersion.LessThan(previousBranchLatestVersion)
}

func minorUpgradePossibleWithPatchRelease(stableVersion, patchVersion *versionutil.Version) bool {
	return patchVersion.LessThan(stableVersion)
}

func getSuggestedEtcdVersion(isExternalEtcd bool, kubernetesVersion string) string {
	if isExternalEtcd {
		return ""
	}
	etcdVersion, warning, err := kubeadmconstants.EtcdSupportedVersion(kubeadmconstants.SupportedEtcdVersion, kubernetesVersion)
	if err != nil {
		klog.Warningf("[upgrade/versions] could not retrieve an etcd version for the target Kubernetes version: %v", err)
		return "N/A"
	}
	if warning != nil {
		klog.V(1).Infof("[upgrade/versions] WARNING: %v", warning)
	}
	return etcdVersion.String()
}

func getLatestClusterVersion(kubeAPIServerVersions map[string][]string) (*versionutil.Version, error) {
	var latestVersion *versionutil.Version
	for versionStr, nodes := range kubeAPIServerVersions {
		ver, err := versionutil.ParseSemantic(versionStr)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse kube-apiserver version %s from nodes %v", versionStr, nodes)
		}
		if latestVersion == nil || ver.AtLeast(latestVersion) {
			latestVersion = ver
		}
	}

	return latestVersion, nil
}
