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

// Package apply 实现 kubeadm upgrade apply 的阶段。
package apply

import (
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/klog/v2"
	utilsexec "k8s.io/utils/exec"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/features"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/upgrade"
	"k8s.io/kubernetes/cmd/kubeadm/app/preflight"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
)

// NewPreflightPhase 返回新的 preflight 阶段。
func NewPreflightPhase() workflow.Phase {
	return workflow.Phase{
		Name:  "preflight",
		Short: "Run preflight checks before upgrade",
		Run:   runPreflight,
		InheritFlags: []string{
			options.CfgPath,
			options.KubeconfigPath,
			options.Force,
			options.DryRun,
			options.IgnorePreflightErrors,
			"allow-experimental-upgrades",
			"allow-release-candidate-upgrades",
			"yes",
		},
	}
}

func runPreflight(c workflow.RunData) error {
	data, ok := c.(Data)
	if !ok {
		return errors.New("preflight phase invoked with an invalid data struct")
	}
	fmt.Println("[upgrade/preflight] Running preflight checks")

	printer := &output.TextPrinter{}

	initCfg, client, ignorePreflightErrors := data.InitCfg(), data.Client(), data.IgnorePreflightErrors()

	// 首先单独检查是否以 root 身份运行，以便快速失败。
	if err := preflight.RunRootCheckOnly(ignorePreflightErrors); err != nil {
		return err
	}
	if err := preflight.RunUpgradeChecks(utilsexec.New(), initCfg, ignorePreflightErrors); err != nil {
		return err
	}

	// 运行 CoreDNS 迁移检查。
	if err := upgrade.RunCoreDNSMigrationCheck(client, ignorePreflightErrors); err != nil {
		return err
	}

	// 对集群运行健康检查。
	klog.V(1).Infoln("[upgrade/preflight] Verifying the cluster health")
	if err := upgrade.CheckClusterHealth(client, &initCfg.ClusterConfiguration, ignorePreflightErrors, data.DryRun(), printer); err != nil {
		return err
	}

	// 检查集群使用的功能开关是否与 kubeadm 当前支持的功能集合一致。
	if msg := features.CheckDeprecatedFlags(&features.InitFeatureGates, initCfg.FeatureGates); len(msg) > 0 {
		for _, m := range msg {
			_, _ = printer.Printf("[upgrade/preflight] %s\n", m)
		}
	}

	// 校验请求的版本和实际版本。
	klog.V(1).Infoln("[upgrade/preflight] Validating requested and actual version")
	if err := configutil.NormalizeKubernetesVersion(&initCfg.ClusterConfiguration); err != nil {
		return err
	}

	// 后续代码统一使用规范化后的版本字符串。
	upgradeVersion, err := version.ParseSemantic(initCfg.KubernetesVersion)
	if err != nil {
		return errors.Errorf("unable to parse normalized version %q as a semantic version", initCfg.KubernetesVersion)
	}

	if err := features.ValidateVersion(features.InitFeatureGates, initCfg.FeatureGates, initCfg.KubernetesVersion); err != nil {
		return err
	}

	versionGetter := upgrade.NewOfflineVersionGetter(upgrade.NewKubeVersionGetter(client), initCfg.KubernetesVersion)
	if err := enforceVersionPolicies(initCfg.KubernetesVersion, upgradeVersion, data.AllowExperimentalUpgrades(), data.AllowRCUpgrades(), data.ForceUpgrade(), versionGetter); err != nil {
		return err
	}

	if data.SessionIsInteractive() {
		if err := cmdutil.InteractivelyConfirmAction("upgrade", "Are you sure you want to proceed?", os.Stdin); err != nil {
			return err
		}
	}

	if !data.DryRun() {
		fmt.Println("[upgrade/preflight] Pulling images required for setting up a Kubernetes cluster")
		fmt.Println("[upgrade/preflight] This might take a minute or two, depending on the speed of your internet connection")
		fmt.Println("[upgrade/preflight] You can also perform this action beforehand using 'kubeadm config images pull'")
		if err := preflight.RunPullImagesCheck(utilsexec.New(), initCfg, ignorePreflightErrors); err != nil {
			return err
		}
	} else {
		fmt.Println("[dryrun] Would pull the required images (like 'kubeadm config images pull')")
	}

	return nil
}

// enforceVersionPolicies 确保用户指定的版本可升级。
// 它同时处理致命错误和可跳过错误（使用 --force 跳过）。
func enforceVersionPolicies(newK8sVersionStr string, newK8sVersion *version.Version, allowExperimentalUpgrades, allowRCUpgrades, force bool, versionGetter upgrade.VersionGetter) error {
	fmt.Printf("[upgrade/preflight] You have chosen to upgrade the cluster version to %q\n", newK8sVersionStr)

	versionSkewErrs := upgrade.EnforceVersionPolicies(versionGetter, newK8sVersionStr, newK8sVersion, allowExperimentalUpgrades, allowRCUpgrades)
	if versionSkewErrs != nil {

		if len(versionSkewErrs.Mandatory) > 0 {
			return errors.Errorf("the version argument is invalid due to these fatal errors:\n\n%v\nPlease fix the misalignments highlighted above and try upgrading again",
				formatErrorMsg(versionSkewErrs.Mandatory))
		}

		if len(versionSkewErrs.Skippable) > 0 {
			// 若用户未指定 --force，则返回错误。
			if !force {
				return errors.Errorf("the version argument is invalid due to these errors:\n\n%v\nCan be bypassed if you pass the --force flag",
					formatErrorMsg(versionSkewErrs.Skippable))
			}
			// 发现可跳过错误，但用户指定了 --force。
			fmt.Printf("[upgrade/preflight] Found %d potential version compatibility errors but skipping since the --force flag is set: \n\n%v", len(versionSkewErrs.Skippable), formatErrorMsg(versionSkewErrs.Skippable))
		}
	}
	return nil
}

// formatErrorMsg 返回对传入错误切片的可读描述。
func formatErrorMsg(errs []error) string {
	var errMsg string
	for _, err := range errs {
		errMsg = fmt.Sprintf("%s\t- %s\n", errMsg, err.Error())
	}
	return errMsg
}
