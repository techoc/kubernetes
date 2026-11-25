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

	"k8s.io/apimachinery/pkg/util/version"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// MaximumAllowedMinorVersionUpgradeSkew 描述 kubeadm 一次可升级控制平面版本的最大次版本数。
	MaximumAllowedMinorVersionUpgradeSkew = 1

	// MaximumAllowedMinorVersionDowngradeSkew 描述 kubeadm 一次可降级控制平面版本的最大次版本数。
	MaximumAllowedMinorVersionDowngradeSkew = 1

	// MaximumAllowedMinorVersionKubeletSkew 描述 kubeadm 集群中控制平面版本与 kubelet 版本允许的最大次版本差异。
	MaximumAllowedMinorVersionKubeletSkew = 3
)

// VersionSkewPolicyErrors 描述 EnforceVersionPolicies 验证过程中可能出现的版本偏差错误。
type VersionSkewPolicyErrors struct {
	Mandatory []error
	Skippable []error
}

// EnforceVersionPolicies 确保建议的新版本符合所有版本偏差策略。
func EnforceVersionPolicies(versionGetter VersionGetter, newK8sVersionStr string, newK8sVersion *version.Version, allowExperimentalUpgrades, allowRCUpgrades bool) *VersionSkewPolicyErrors {

	skewErrors := &VersionSkewPolicyErrors{
		Mandatory: []error{},
		Skippable: []error{},
	}

	clusterVersionStr, clusterVersion, err := versionGetter.ClusterVersion()
	if err != nil {
		// 此错误不可强制忽略：kubeadm 必须能够查询集群版本才能执行升级。
		skewErrors.Mandatory = append(skewErrors.Mandatory, errors.Wrap(err, "Unable to fetch cluster version"))
		return skewErrors
	}
	fmt.Printf("[upgrade/versions] Cluster version: %s\n", clusterVersionStr)

	kubeadmVersionStr, kubeadmVersion, err := versionGetter.KubeadmVersion()
	if err != nil {
		// 此错误不可强制忽略：kubeadm 必须能够查询自身版本才能执行升级。
		skewErrors.Mandatory = append(skewErrors.Mandatory, errors.Wrap(err, "Unable to fetch kubeadm version"))
		return skewErrors
	}
	fmt.Printf("[upgrade/versions] kubeadm version: %s\n", kubeadmVersionStr)

	kubeletVersions, err := versionGetter.KubeletVersions()
	if err != nil {
		// 此错误不严重；即使 kubeadm 无法查询，也继续执行。
		skewErrors.Skippable = append(skewErrors.Skippable, errors.Wrap(err, "Unable to fetch kubelet version"))
	}

	// 确保新版本受支持，即高于最低支持版本。
	if !newK8sVersion.AtLeast(constants.MinimumControlPlaneVersion) {
		// 不应出现此情况：kubeadm 始终支持某个最低版本，不能低于该版本。
		skewErrors.Mandatory = append(skewErrors.Mandatory, errors.Errorf("Specified version to upgrade to %q is equal to or lower than the minimum supported version %q. Please specify a higher version to upgrade to", newK8sVersionStr, clusterVersionStr))
	}

	// kubeadm 不支持一次跨越两个次版本升级，例如 v1.7 -> v1.9。
	if newK8sVersion.Minor() > clusterVersion.Minor()+MaximumAllowedMinorVersionUpgradeSkew {
		tooLargeUpgradeSkewErr := errors.Errorf("Specified version to upgrade to %q is too high; kubeadm can upgrade only %d minor version at a time", newK8sVersionStr, MaximumAllowedMinorVersionUpgradeSkew)
		// 若要升级到已发布版本，应完整执行此策略。
		// 若为 CI、开发或实验版本，可跨越两个次版本，但要求使用 -f 参数。
		if len(newK8sVersion.PreRelease()) == 0 {
			skewErrors.Mandatory = append(skewErrors.Mandatory, tooLargeUpgradeSkewErr)
		} else {
			skewErrors.Skippable = append(skewErrors.Skippable, tooLargeUpgradeSkewErr)
		}
	}

	// kubeadm 不支持一次跨越两个次版本降级，例如 v1.9 -> v1.7。
	if newK8sVersion.Minor() < clusterVersion.Minor()-MaximumAllowedMinorVersionDowngradeSkew {
		tooLargeDowngradeSkewErr := errors.Errorf("Specified version to downgrade to %q is too low; kubeadm can downgrade only %d minor version at a time", newK8sVersionStr, MaximumAllowedMinorVersionDowngradeSkew)
		// 若要降级到已发布版本，应完整执行此策略。
		// 若为 CI、开发或实验版本，可跨越两个次版本，但要求使用 -f 参数。
		if len(newK8sVersion.PreRelease()) == 0 {
			skewErrors.Mandatory = append(skewErrors.Mandatory, tooLargeDowngradeSkewErr)
		} else {
			skewErrors.Skippable = append(skewErrors.Skippable, tooLargeDowngradeSkewErr)
		}
	}

	// 若 kubeadm 版本低于目标升级版本，则返回错误。
	if kubeadmVersion.LessThan(newK8sVersion) {
		if newK8sVersion.Minor() > kubeadmVersion.Minor() {
			tooLargeKubeadmSkew := errors.Errorf("Specified version to upgrade to %q is at least one minor release higher than the kubeadm minor release (%d > %d). Such an upgrade is not supported", newK8sVersionStr, newK8sVersion.Minor(), kubeadmVersion.Minor())
			// 此情况不受支持：kubeadm 不知道如何处理比自身更新的次版本。
			// 不过，若目标为 CI、开发或实验版本，可降低此检查的严重性，但要求使用 -f 参数。
			if len(newK8sVersion.PreRelease()) == 0 {
				skewErrors.Mandatory = append(skewErrors.Mandatory, tooLargeKubeadmSkew)
			} else {
				skewErrors.Skippable = append(skewErrors.Skippable, tooLargeKubeadmSkew)
			}
		} else {
			// 用户指定 --force 时，可升级到高于 kubeadm 的补丁版本。虽不推荐，但可行。
			skewErrors.Skippable = append(skewErrors.Skippable, errors.Errorf("Specified version to upgrade to %q is higher than the kubeadm version %q. Upgrade kubeadm first using the tool you used to install kubeadm", newK8sVersionStr, kubeadmVersionStr))
		}
	}

	if kubeadmVersion.Major() > newK8sVersion.Major() ||
		kubeadmVersion.Minor() > newK8sVersion.Minor() {
		skewErrors.Skippable = append(skewErrors.Skippable, errors.Errorf("Kubeadm version %s can only be used to upgrade to Kubernetes version %d.%d", kubeadmVersionStr, kubeadmVersion.Major(), kubeadmVersion.Minor()))
	}

	// 检查版本是否不稳定，以及用户是否不允许使用该版本。
	if err = detectUnstableVersionError(newK8sVersion, newK8sVersionStr, allowExperimentalUpgrades, allowRCUpgrades); err != nil {
		skewErrors.Skippable = append(skewErrors.Skippable, err)
	}

	// 检查集群中是否存在过于老旧的 kubelet。
	// 此处检查 nil：只有 KubeletVersions() 返回错误时，kubeletVersions 才可能为 nil。
	// 此时可跳过此检查。
	if kubeletVersions != nil {
		if err = detectTooOldKubelets(newK8sVersion, kubeletVersions); err != nil {
			skewErrors.Skippable = append(skewErrors.Skippable, err)
		}
	}

	// 若未发现错误，则返回 nil。
	if len(skewErrors.Skippable) == 0 && len(skewErrors.Mandatory) == 0 {
		return nil
	}

	// 若遇到一个或多个错误，则返回这些错误。
	return skewErrors
}

// detectUnstableVersionError 检查是否允许使用指定的不稳定版本。
func detectUnstableVersionError(newK8sVersion *version.Version, newK8sVersionStr string, allowExperimentalUpgrades, allowRCUpgrades bool) error {
	// 若并非不稳定版本，则立即返回。
	if len(newK8sVersion.PreRelease()) == 0 {
		return nil
	}
	// 若用户已明确允许不稳定版本，则不返回错误。
	if allowExperimentalUpgrades {
		return nil
	}
	// 若为允许使用的发布候选版本，则检查通过。
	if strings.HasPrefix(newK8sVersion.PreRelease(), "rc") && allowRCUpgrades {
		return nil
	}

	return errors.Errorf("Specified version to upgrade to %q is an unstable version and such upgrades weren't allowed via setting the --allow-*-upgrades flags", newK8sVersionStr)
}

// detectTooOldKubelets 在 kubelet 版本过旧、升级后会产生不受支持的版本偏差时报错。
func detectTooOldKubelets(newK8sVersion *version.Version, kubeletVersions map[string][]string) error {
	var tooOldKubeletVersions []string
	for versionStr := range kubeletVersions {

		kubeletVersion, err := version.ParseSemantic(versionStr)
		if err != nil {
			return errors.Errorf("couldn't parse kubelet version %s", versionStr)
		}

		if newK8sVersion.Minor() > kubeletVersion.Minor()+MaximumAllowedMinorVersionKubeletSkew {
			tooOldKubeletVersions = append(tooOldKubeletVersions, versionStr)
		}
	}
	if len(tooOldKubeletVersions) == 0 {
		return nil
	}

	return errors.Errorf("There are kubelets in this cluster that are too old that have these versions %v", tooOldKubeletVersions)
}
