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
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/sets"
	fakediscovery "k8s.io/client-go/discovery/fake"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/features"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/upgrade"
	"k8s.io/kubernetes/cmd/kubeadm/app/preflight"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

// enforceRequirements 确认可以升级，并返回后续流程所需的变量。
func enforceRequirements(flagSet *pflag.FlagSet, flags *applyPlanFlags, args []string, dryRun bool, upgradeApply bool, printer output.Printer) (clientset.Interface, upgrade.VersionGetter, *kubeadmapi.InitConfiguration, *kubeadmapi.UpgradeConfiguration, error) {
	externalCfg := &kubeadmapiv1.UpgradeConfiguration{}
	opt := configutil.LoadOrDefaultConfigurationOptions{}
	upgradeCfg, err := configutil.LoadOrDefaultUpgradeConfiguration(flags.cfgPath, externalCfg, opt)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "[upgrade/upgrade config] FATAL")
	}

	// kubeadm plan 的 dryRun 应始终为 false。
	isDryRun := ptr.To(false)
	printConfigCfg := upgradeCfg.Plan.PrintConfig
	ignoreErrCfg := upgradeCfg.Plan.IgnorePreflightErrors
	ok := false
	if upgradeApply {
		printConfigCfg = upgradeCfg.Apply.PrintConfig
		ignoreErrCfg = upgradeCfg.Apply.IgnorePreflightErrors
		isDryRun, ok = cmdutil.ValueFromFlagsOrConfig(flagSet, options.DryRun, upgradeCfg.Apply.DryRun, &dryRun).(*bool)
		if !ok {
			return nil, nil, nil, nil, cmdutil.TypeMismatchErr("dryRun", "bool")
		}
	}

	client, err := getClient(flags.kubeConfigPath, *isDryRun, printer)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrapf(err, "couldn't create a Kubernetes client from file %q", flags.kubeConfigPath)
	}

	ignorePreflightErrorsSet, err := validation.ValidateIgnorePreflightErrors(flags.ignorePreflightErrors, ignoreErrCfg)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// 同时将预检错误的并集写入 UpgradeConfiguration，使运行时配置视图保持一致。
	// Plan.IgnorePreflightErrors 未被使用，因此不设置。
	if upgradeApply {
		upgradeCfg.Apply.IgnorePreflightErrors = sets.List(ignorePreflightErrorsSet)
	}

	// 确保用户为 root。
	klog.V(1).Info("running preflight checks")
	if err := runPreflightChecks(client, ignorePreflightErrorsSet, printer); err != nil {
		return nil, nil, nil, nil, err
	}

	getNodeRegistration := true
	getAPIEndpoint := staticpodutil.IsControlPlaneNode()
	getComponentConfigs := true
	initCfg, err := configutil.FetchInitConfigurationFromCluster(client, printer, "upgrade/config", getNodeRegistration, getAPIEndpoint, getComponentConfigs, false)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "[upgrade/init config] FATAL")
	}

	// 将 UpgradeApplyConfiguration 中的 ImagePullPolicy 和 ImagePullSerial
	// 写入 InitConfiguration，供运行 apply 时的 preflight.RunPullImagesCheck() 使用。
	if upgradeApply {
		initCfg.NodeRegistration.ImagePullPolicy = upgradeCfg.Apply.ImagePullPolicy
		initCfg.NodeRegistration.ImagePullSerial = upgradeCfg.Apply.ImagePullSerial
	}

	newK8sVersion := upgradeCfg.Plan.KubernetesVersion
	if upgradeApply {
		newK8sVersion = upgradeCfg.Apply.KubernetesVersion
		// 升级期间，version 参数为必填项，除非配置文件中已指定。
		if newK8sVersion == "" {
			if err := cmdutil.ValidateExactArgNumber(args, []string{"version"}); err != nil {
				return nil, nil, nil, nil, err
			}
		}
	}

	// 若参数和配置文件都指定了选项，则参数覆盖配置文件。
	if len(args) == 1 {
		newK8sVersion = args[0]
	}

	if upgradeApply {
		// upgrade apply 的版本参数总会用目标版本覆盖返回 cfg 中的 KubernetesVersion。
		// upgrade plan 则不同，其 KubernetesVersion 应保持旧版本，
		// 因为 getComponentConfigVersionStates 需要当前已安装的版本。
		// 这也使 upgrade plan 返回的 KubernetesVersion 保持一致：该命令允许不指定目标版本，
		// 此时 KubernetesVersion 始终为当前已安装版本。
		initCfg.KubernetesVersion = newK8sVersion
	}

	// 对集群运行健康检查。
	if err := upgrade.CheckClusterHealth(client, &initCfg.ClusterConfiguration, ignorePreflightErrorsSet, dryRun, printer); err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "[upgrade/health] FATAL")
	}

	// 检查集群使用的功能开关是否与 kubeadm 当前支持的功能集合一致。
	if msg := features.CheckDeprecatedFlags(&features.InitFeatureGates, initCfg.FeatureGates); len(msg) > 0 {
		for _, m := range msg {
			printer.Printf("[upgrade/config] %s\n", m)
		}
	}

	// 若用户要求打印此信息，则执行打印。
	printConfig, ok := cmdutil.ValueFromFlagsOrConfig(flagSet, options.PrintConfig, printConfigCfg, &flags.printConfig).(*bool)
	if ok && *printConfig {
		printConfiguration(&initCfg.ClusterConfiguration, os.Stdout, printer)
	} else if !ok {
		return nil, nil, nil, nil, cmdutil.TypeMismatchErr("printConfig", "bool")
	}

	// 使用真实版本获取接口，查询 API 服务器、kubeadm 客户端和 Kubernetes CI 系统中的最新版本。
	return client, upgrade.NewOfflineVersionGetter(upgrade.NewKubeVersionGetter(client), newK8sVersion), initCfg, upgradeCfg, nil
}

// printConfiguration 将 API 的外部版本打印为 YAML。
func printConfiguration(clustercfg *kubeadmapi.ClusterConfiguration, w io.Writer, printer output.Printer) {
	// 若 cfg 为 nil 则提前返回，以便安全解引用下方指针。
	if clustercfg == nil {
		return
	}

	cfgYaml, err := configutil.MarshalKubeadmConfigObject(clustercfg, kubeadmapiv1.SchemeGroupVersion)
	if err == nil {
		printer.Fprintln(w, "[upgrade/config] Configuration used:")

		scanner := bufio.NewScanner(bytes.NewReader(cfgYaml))
		for scanner.Scan() {
			printer.Fprintf(w, "\t%s\n", scanner.Text())
		}
	}
}

// runPreflightChecks 运行 root 预检。
func runPreflightChecks(client clientset.Interface, ignorePreflightErrors sets.Set[string], printer output.Printer) error {
	printer.Printf("[preflight] Running pre-flight checks.\n")
	err := preflight.RunRootCheckOnly(ignorePreflightErrors)
	if err != nil {
		return err
	}
	return upgrade.RunCoreDNSMigrationCheck(client, ignorePreflightErrors)
}

// getClient 根据用户是否 dry-run 返回真实或伪客户端。
func getClient(file string, dryRun bool, printer output.Printer) (clientset.Interface, error) {
	if dryRun {
		// 默认将服务器版本设为 kubeadm 版本。
		serverVersion := constants.CurrentKubernetesVersion.Info()

		dryRun := apiclient.NewDryRun()
		dryRun.WithDefaultMarshalFunction().
			WithWriter(os.Stdout).
			PrependReactor(dryRun.HealthCheckJobReactor()).
			PrependReactor(dryRun.PatchNodeReactor())

		// 若 kubeconfig 存在，则据此构造真实客户端并获取真实 serverVersion。
		if _, err := os.Stat(file); err == nil {
			_, _ = printer.Printf("[dryrun] Creating a real client from %q\n", file)
			if err := dryRun.WithKubeConfigFile(file); err != nil {
				return nil, err
			}
			serverVersion, err = dryRun.Client().Discovery().ServerVersion()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get server version")
			}
		} else if os.IsNotExist(err) {
			// 若文件（应为 admin.conf）不存在，则添加更多反应器。
			// ListPodsReactor 需要节点名，因此尝试使用 kubelet.conf 客户端；
			// 若该文件也不存在，则回退到主机名。
			_, _ = printer.Printf("[dryrun] Dryrunning without a real client\n")
			kubeconfigPath := filepath.Join(constants.KubernetesDir, constants.KubeletKubeConfigFileName)
			nodeName, err := configutil.GetNodeName(kubeconfigPath)
			if err != nil {
				return nil, err
			}
			dryRun.PrependReactor(dryRun.GetKubeadmConfigReactor()).
				PrependReactor(dryRun.GetKubeletConfigReactor()).
				PrependReactor(dryRun.GetKubeProxyConfigReactor()).
				PrependReactor(dryRun.GetNodeReactor()).
				PrependReactor(dryRun.ListPodsReactor(nodeName)).
				PrependReactor(dryRun.GetCoreDNSConfigReactor()).
				PrependReactor(dryRun.ListDeploymentsReactor())
		} else {
			// 若文件存在但 stat 返回其他错误，则抛出该错误。
			return nil, errors.Wrapf(err, "could not create a client from %q", file)
		}

		// 获取此伪客户端的 FakeDiscovery 对象。
		fakeClient := dryRun.FakeClient()
		fakeClientDiscovery, ok := fakeClient.Discovery().(*fakediscovery.FakeDiscovery)
		if !ok {
			return nil, errors.New("could not set fake discovery's server version")
		}
		// 为其设置正确的服务器版本。
		fakeClientDiscovery.FakedServerVersion = serverVersion

		return fakeClient, nil
	}
	return kubeconfigutil.ClientSetFromFile(file)
}
