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
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/lithammer/dedent"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	outputapischeme "k8s.io/kubernetes/cmd/kubeadm/app/apis/output/scheme"
	outputapiv1alpha3 "k8s.io/kubernetes/cmd/kubeadm/app/apis/output/v1alpha3"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/upgrade"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
)

type planFlags struct {
	*applyPlanFlags
}

var upgradePlanLongDesc = cmdutil.LongDesc(`
	Check which versions are available to upgrade to and validate whether your current cluster is upgradeable.
	This command can only run on the control plane nodes where the kubeconfig file "admin.conf" exists.
	To skip the internet check, pass in the optional [version] parameter.
`)

// newCmdPlan 返回 kubeadm upgrade plan 的 cobra 命令。
func newCmdPlan(apf *applyPlanFlags) *cobra.Command {
	flags := &planFlags{
		applyPlanFlags: apf,
	}

	outputFlags := output.NewOutputFlags(&upgradePlanTextPrintFlags{}).WithTypeSetter(outputapischeme.Scheme).WithDefaultOutput(output.TextOutput)

	cmd := &cobra.Command{
		Use:   "plan [version] [flags]",
		Short: "Check which versions are available to upgrade to and validate whether your current cluster is upgradeable.",
		Long:  upgradePlanLongDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			printer, err := outputFlags.ToPrinter()
			if err != nil {
				return errors.Wrap(err, "could not construct output printer")
			}

			if err := validation.ValidateMixedArguments(cmd.Flags()); err != nil {
				return err
			}
			return runPlan(cmd.Flags(), flags, args, printer)
		},
	}

	outputFlags.AddFlags(cmd)

	// 注册 apply 和 plan 的公共参数。
	addApplyPlanFlags(cmd.Flags(), flags.applyPlanFlags)
	return cmd
}

// newComponentUpgradePlan 辅助函数创建 outputapiv1alpha3.ComponentUpgradePlan 对象。
func newComponentUpgradePlan(name, currentVersion, newVersion, nodeName string) outputapiv1alpha3.ComponentUpgradePlan {
	return outputapiv1alpha3.ComponentUpgradePlan{
		Name:           name,
		CurrentVersion: currentVersion,
		NewVersion:     newVersion,
		NodeName:       nodeName,
	}
}

// runPlan 负责向用户输出可升级到的版本。
func runPlan(flagSet *pflag.FlagSet, flags *planFlags, args []string, printer output.Printer) error {
	// 从基础检查开始：验证集群健康、构建客户端和版本获取器。规划时绝不 dry-run。
	klog.V(1).Infoln("[upgrade/plan] verifying health of cluster")
	klog.V(1).Infoln("[upgrade/plan] retrieving configuration from cluster")
	client, versionGetter, initCfg, upgradeCfg, err := enforceRequirements(flagSet, flags.applyPlanFlags, args, false, false, printer)
	if err != nil {
		return err
	}

	// 计算可用的升级可能性。
	klog.V(1).Infoln("[upgrade/plan] computing upgrade possibilities")

	// 尊重参数设置，同时保持配置文件不变。
	allowRCUpgrades, ok := cmdutil.ValueFromFlagsOrConfig(flagSet, options.AllowRCUpgrades, upgradeCfg.Plan.AllowRCUpgrades, &flags.allowRCUpgrades).(*bool)
	if !ok {
		return cmdutil.TypeMismatchErr("allowRCUpgrades", "bool")
	}

	allowExperimentalUpgrades, ok := cmdutil.ValueFromFlagsOrConfig(flagSet, options.AllowExperimentalUpgrades, upgradeCfg.Plan.AllowExperimentalUpgrades, &flags.allowExperimentalUpgrades).(*bool)
	if !ok {
		return cmdutil.TypeMismatchErr("allowExperimentalUpgrades", "bool")
	}

	availUpgrades, err := upgrade.GetAvailableUpgrades(versionGetter, *allowExperimentalUpgrades, *allowRCUpgrades, printer)
	if err != nil {
		return errors.Wrap(err, "[upgrade/versions] FATAL")
	}

	// 获取组件配置的当前状态。
	klog.V(1).Infoln("[upgrade/plan] analysing component config version states")
	configVersionStates, err := componentconfigs.GetVersionStates(&initCfg.ClusterConfiguration, client)
	if err != nil {
		return errors.WithMessage(err, "[upgrade/versions] FATAL")
	}

	// 没有可用升级。
	if len(availUpgrades) == 0 {
		klog.V(1).Infoln("[upgrade/plan] Awesome, you're up-to-date! Enjoy!")
		return nil
	}

	// 生成并打印升级计划。
	etcdUpgrade, ok := cmdutil.ValueFromFlagsOrConfig(flagSet, options.EtcdUpgrade, upgradeCfg.Plan.EtcdUpgrade, &flags.etcdUpgrade).(*bool)
	if !ok {
		return cmdutil.TypeMismatchErr("etcdUpgrade", "bool")
	}
	plan := genUpgradePlan(availUpgrades, configVersionStates, *etcdUpgrade)
	return printer.PrintObj(plan, os.Stdout)
}

// genUpgradePlan 根据可用升级和组件配置版本状态生成升级计划。
func genUpgradePlan(availUpgrades []upgrade.Upgrade, configVersions []outputapiv1alpha3.ComponentConfigVersionState, etcdUpgrade bool) *outputapiv1alpha3.UpgradePlan {
	plan := &outputapiv1alpha3.UpgradePlan{ConfigVersions: configVersions}
	for _, up := range availUpgrades {
		plan.AvailableUpgrades = append(plan.AvailableUpgrades, genAvailableUpgrade(&up, etcdUpgrade))
	}
	return plan
}

// TODO: 目前无法清晰输出涉及添加、删除或更改组件的升级。
// 已创建 https://github.com/kubernetes/kubeadm/issues/810 跟踪此问题。
func appendDNSComponent(components []outputapiv1alpha3.ComponentUpgradePlan, up *upgrade.Upgrade, name string) []outputapiv1alpha3.ComponentUpgradePlan {
	beforeVersion := up.Before.DNSVersion
	afterVersion := up.After.DNSVersion

	if beforeVersion != "" || afterVersion != "" {
		components = append(components, newComponentUpgradePlan(name, beforeVersion, afterVersion, ""))
	}
	return components
}

// appendKubeadmComponent 将 kubeadm 组件添加到组件列表。
func appendKubeadmComponent(components []outputapiv1alpha3.ComponentUpgradePlan, up *upgrade.Upgrade, name string) []outputapiv1alpha3.ComponentUpgradePlan {
	beforeVersion := up.Before.KubeadmVersion
	afterVersion := up.After.KubeadmVersion

	if beforeVersion != "" || afterVersion != "" {
		components = append(components, newComponentUpgradePlan(name, beforeVersion, afterVersion, ""))
	}
	return components
}

// genAvailableUpgrade 从 upgrade 对象生成可用升级信息。
func genAvailableUpgrade(up *upgrade.Upgrade, etcdUpgrade bool) outputapiv1alpha3.AvailableUpgrade {
	components := []outputapiv1alpha3.ComponentUpgradePlan{}

	if up.CanUpgradeKubelets() {
		// 映射形式为 <old-version>:<node-names>。将所有键放入切片并排序，
		// 以确保始终获得正确顺序，然后分别提取映射值。
		for _, oldVersion := range sortedSliceFromStringStringArrayMap(up.Before.KubeletVersions) {
			nodeNames := up.Before.KubeletVersions[oldVersion]
			for _, nodeName := range nodeNames {
				components = append(components, newComponentUpgradePlan(constants.Kubelet, oldVersion, up.After.KubeVersion, nodeName))
			}
		}
	}

	for _, oldVersion := range sortedSliceFromStringStringArrayMap(up.Before.KubeAPIServerVersions) {
		nodeNames := up.Before.KubeAPIServerVersions[oldVersion]
		for _, nodeName := range nodeNames {
			components = append(components, newComponentUpgradePlan(constants.KubeAPIServer, oldVersion, up.After.KubeVersion, nodeName))
		}
	}

	for _, oldVersion := range sortedSliceFromStringStringArrayMap(up.Before.KubeControllerManagerVersions) {
		nodeNames := up.Before.KubeControllerManagerVersions[oldVersion]
		for _, nodeName := range nodeNames {
			components = append(components, newComponentUpgradePlan(constants.KubeControllerManager, oldVersion, up.After.KubeVersion, nodeName))
		}
	}

	for _, oldVersion := range sortedSliceFromStringStringArrayMap(up.Before.KubeSchedulerVersions) {
		nodeNames := up.Before.KubeSchedulerVersions[oldVersion]
		for _, nodeName := range nodeNames {
			components = append(components, newComponentUpgradePlan(constants.KubeScheduler, oldVersion, up.After.KubeVersion, nodeName))
		}
	}

	components = append(components, newComponentUpgradePlan(constants.KubeProxy, up.Before.KubeVersion, up.After.KubeVersion, ""))
	components = appendDNSComponent(components, up, constants.CoreDNS)
	components = appendKubeadmComponent(components, up, constants.Kubeadm)

	// 若 etcd 不是外部的，则应将其纳入升级计划。
	if etcdUpgrade {
		for _, oldVersion := range sortedSliceFromStringStringArrayMap(up.Before.EtcdVersions) {
			nodeNames := up.Before.EtcdVersions[oldVersion]
			for _, nodeName := range nodeNames {
				components = append(components, newComponentUpgradePlan(constants.Etcd, oldVersion, up.After.EtcdVersion, nodeName))
			}
		}
	}

	return outputapiv1alpha3.AvailableUpgrade{Description: up.Description, Components: components}
}

// sortedSliceFromStringStringArrayMap 返回按字母顺序排序的映射键切片。
func sortedSliceFromStringStringArrayMap(strMap map[string][]string) []string {
	strSlice := make([]string, 0, len(strMap))
	for k := range strMap {
		strSlice = append(strSlice, k)
	}
	sort.Strings(strSlice)
	return strSlice
}

// upgradePlanTextPrintFlags 提供以文本形式打印升级计划所需的参数。
type upgradePlanTextPrintFlags struct{}

// ToPrinter 返回用于文本输出格式的 kubeadm 打印机。
func (tpf *upgradePlanTextPrintFlags) ToPrinter(outputFormat string) (output.Printer, error) {
	if outputFormat == output.TextOutput {
		return &upgradePlanTextPrinter{}, nil
	}
	return nil, genericclioptions.NoCompatiblePrinterError{OutputFormat: &outputFormat, AllowedFormats: []string{output.TextOutput}}
}

// upgradePlanTextPrinter 以文本形式打印升级计划。
type upgradePlanTextPrinter struct {
	output.TextPrinter
}

// PrintObj 实现 ResourcePrinter.PrintObj，用于升级计划的纯文本输出。
func (printer *upgradePlanTextPrinter) PrintObj(obj runtime.Object, writer io.Writer) error {
	plan, ok := obj.(*outputapiv1alpha3.UpgradePlan)
	if !ok {
		return errors.Errorf("expected UpgradePlan, but got %+v", obj)
	}

	for _, au := range plan.AvailableUpgrades {
		if err := printer.printAvailableUpgrade(writer, &au); err != nil {
			return err
		}
	}
	printer.printComponentConfigVersionStates(writer, plan.ConfigVersions)
	return nil
}

// printUpgradePlan 以便于用户理解的方式概述可升级到的版本。
func (printer *upgradePlanTextPrinter) printAvailableUpgrade(writer io.Writer, au *outputapiv1alpha3.AvailableUpgrade) error {
	var kubeVersion string
	var beforeKubeadmVersion, afterKubeadmVersion string
	for _, component := range au.Components {
		if component.Name == constants.KubeAPIServer {
			kubeVersion = component.NewVersion
		}
		if component.Name == constants.Kubeadm {
			beforeKubeadmVersion = component.CurrentVersion
			afterKubeadmVersion = component.NewVersion
		}
	}

	newK8sVersion, err := version.ParseSemantic(kubeVersion)
	if err != nil {
		return errors.Wrapf(err, "Unable to parse normalized version %q as a semantic version", kubeVersion)
	}

	unstableVersionFlag := ""
	if len(newK8sVersion.PreRelease()) != 0 {
		if strings.HasPrefix(newK8sVersion.PreRelease(), "rc") {
			unstableVersionFlag = " --allow-release-candidate-upgrades"
		} else {
			unstableVersionFlag = " --allow-experimental-upgrades"
		}
	}

	_, _ = printer.Fprintln(writer, "Components that must be upgraded manually after you have upgraded the control plane with 'kubeadm upgrade apply':")
	tabw := tabwriter.NewWriter(writer, 10, 4, 3, ' ', 0)
	_, _ = printer.Fprintln(tabw, strings.Join([]string{"COMPONENT", "NODE", "CURRENT", "TARGET"}, "\t"))
	for _, component := range au.Components {
		if component.Name != constants.Kubelet {
			continue
		}
		_, _ = printer.Fprintf(tabw, "%s\t%s\t%s\t%s\n", component.Name, component.NodeName, component.CurrentVersion, component.NewVersion)
	}
	_ = tabw.Flush()

	_, _ = printer.Fprintln(writer, "")
	_, _ = printer.Fprintf(writer, "Upgrade to the latest %s:\n", au.Description)
	_, _ = printer.Fprintln(writer, "")
	_, _ = printer.Fprintln(tabw, strings.Join([]string{"COMPONENT", "NODE", "CURRENT", "TARGET"}, "\t"))
	for _, component := range au.Components {
		if component.Name == constants.Kubelet || component.Name == constants.Kubeadm {
			continue
		}
		_, _ = printer.Fprintf(tabw, "%s\t%s\t%s\t%s\n", component.Name, component.NodeName, component.CurrentVersion, component.NewVersion)
	}
	_ = tabw.Flush()

	_, _ = printer.Fprintln(writer, "")
	_, _ = printer.Fprintln(writer, "You can now apply the upgrade by executing the following command:")
	_, _ = printer.Fprintln(writer, "")
	_, _ = printer.Fprintf(writer, "\tkubeadm upgrade apply %s%s\n", kubeVersion, unstableVersionFlag)
	_, _ = printer.Fprintln(writer, "")

	if beforeKubeadmVersion != afterKubeadmVersion {
		_, _ = printer.Fprintf(writer, "Note: Before you can perform this upgrade, you have to update kubeadm to %s.\n", afterKubeadmVersion)
		_, _ = printer.Fprintln(writer, "")
	}

	printer.printLineSeparator(writer)
	return nil
}

// printComponentConfigVersionStates 以便于用户理解的方式概述组件配置的当前状态。
func (printer *upgradePlanTextPrinter) printComponentConfigVersionStates(w io.Writer, versionStates []outputapiv1alpha3.ComponentConfigVersionState) {
	if len(versionStates) == 0 {
		_, _ = printer.Fprintln(w, "No information available on component configs.")
		return
	}

	_, _ = printer.Fprintln(w, dedent.Dedent(`
		The table below shows the current state of component configs as understood by this version of kubeadm.
		Configs that have a "yes" mark in the "MANUAL UPGRADE REQUIRED" column require manual config upgrade or
		resetting to kubeadm defaults before a successful upgrade can be performed. The version to manually
		upgrade to is denoted in the "PREFERRED VERSION" column.
	`))

	tabw := tabwriter.NewWriter(w, 10, 4, 3, ' ', 0)
	_, _ = printer.Fprintln(tabw, strings.Join([]string{"API GROUP", "CURRENT VERSION", "PREFERRED VERSION", "MANUAL UPGRADE REQUIRED"}, "\t"))

	for _, state := range versionStates {
		_, _ = printer.Fprintf(tabw,
			"%s\t%s\t%s\t%s\n",
			state.Group,
			strOrDash(state.CurrentVersion),
			strOrDash(state.PreferredVersion),
			yesOrNo(state.ManualUpgradeRequired),
		)
	}

	_ = tabw.Flush()
	printer.printLineSeparator(w)
}

func (printer *upgradePlanTextPrinter) printLineSeparator(w io.Writer) {
	_, _ = printer.Fprintf(w, "_____________________________________________________________________\n\n")
}

func strOrDash(s string) string {
	if s != "" {
		return s
	}
	return "-"
}

func yesOrNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
