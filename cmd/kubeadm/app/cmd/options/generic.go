/*
Copyright 2018 The Kubernetes Authors.

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

package options

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/features"
)

// AddKubeConfigFlag 将 --kubeconfig 参数添加到指定参数集。
func AddKubeConfigFlag(fs *pflag.FlagSet, kubeConfigFile *string) {
	fs.StringVar(kubeConfigFile, KubeconfigPath, *kubeConfigFile, "The kubeconfig file to use when talking to the cluster. If the flag is not set, a set of standard locations can be searched for an existing kubeconfig file.")
	// 注意：DefValue 是终端显示的文本，而不是赋给参数的实际默认值。
	fs.Lookup(KubeconfigPath).DefValue = constants.GetAdminKubeConfigPath()
}

// AddKubeConfigDirFlag 将 --kubeconfig-dir 参数添加到指定参数集。
func AddKubeConfigDirFlag(fs *pflag.FlagSet, kubeConfigDir *string) {
	fs.StringVar(kubeConfigDir, KubeconfigDir, *kubeConfigDir, "The path where to save the kubeconfig file.")
}

// AddConfigFlag 将 --config 参数添加到指定参数集。
func AddConfigFlag(fs *pflag.FlagSet, cfgPath *string) {
	fs.StringVar(cfgPath, CfgPath, *cfgPath, "Path to a kubeadm configuration file.")
}

// AddIgnorePreflightErrorsFlag 将 --ignore-preflight-errors 参数添加到指定参数集。
func AddIgnorePreflightErrorsFlag(fs *pflag.FlagSet, ignorePreflightErrors *[]string) {
	fs.StringSliceVar(
		ignorePreflightErrors, IgnorePreflightErrors, *ignorePreflightErrors,
		"A list of checks whose errors will be shown as warnings. Example: 'IsPrivilegedUser,Swap'. Value 'all' ignores errors from all checks.",
	)
}

// AddControlPlanExtraArgsFlags 添加控制平面组件的 ExtraArgs 参数。
func AddControlPlanExtraArgsFlags(fs *pflag.FlagSet, apiServerExtraArgs, controllerManagerExtraArgs, schedulerExtraArgs *[]kubeadmapiv1.Arg) {
	// TODO: 这些参数已弃用，应移除它们及相关逻辑：
	// - AddControlPlanExtraArgsFlag()
	// - 文件 app/cmd/options/argslice*.go
	// - options.*ExtraArgs
	// - app/cmd/init.go 和 app/cmd/phases/init/controlplane.go 中的用法
	fs.Var(newArgSlice(apiServerExtraArgs), APIServerExtraArgs, "A set of extra flags to pass to the API Server or override default ones in form of <flagname>=<value>")
	fs.Var(newArgSlice(controllerManagerExtraArgs), ControllerManagerExtraArgs, "A set of extra flags to pass to the Controller Manager or override default ones in form of <flagname>=<value>")
	fs.Var(newArgSlice(schedulerExtraArgs), SchedulerExtraArgs, "A set of extra flags to pass to the Scheduler or override default ones in form of <flagname>=<value>")
	const future = "This flag will be removed in a future version"
	_ = fs.MarkDeprecated(APIServerExtraArgs, fmt.Sprintf("use 'ClusterConfiguration.apiServer.extraArgs' instead. %s", future))
	_ = fs.MarkDeprecated(ControllerManagerExtraArgs, fmt.Sprintf("use 'ClusterConfiguration.controllerManager.extraArgs' instead. %s", future))
	_ = fs.MarkDeprecated(SchedulerExtraArgs, fmt.Sprintf("use 'ClusterConfiguration.scheduler.extraArgs' instead. %s", future))
}

// AddImageMetaFlags 将 --image-repository 参数添加到指定参数集。
func AddImageMetaFlags(fs *pflag.FlagSet, imageRepository *string) {
	fs.StringVar(imageRepository, ImageRepository, *imageRepository, "Choose a container registry to pull control plane images from")
}

// AddFeatureGatesStringFlag 将 --feature-gates 参数添加到指定参数集。
func AddFeatureGatesStringFlag(fs *pflag.FlagSet, featureGatesString *string) {
	if knownFeatures := features.KnownFeatures(&features.InitFeatureGates); len(knownFeatures) > 0 {
		fs.StringVar(featureGatesString, FeatureGatesString, *featureGatesString, "A set of key=value pairs that describe feature gates for various features. "+
			"Options are:\n"+strings.Join(knownFeatures, "\n"))
	} else {
		fs.StringVar(featureGatesString, FeatureGatesString, *featureGatesString, "A set of key=value pairs that describe feature gates for various features. "+
			"No feature gates are available in this release.")
	}
}

// AddKubernetesVersionFlag 将 --kubernetes-version 参数添加到指定参数集。
func AddKubernetesVersionFlag(fs *pflag.FlagSet, kubernetesVersion *string) {
	fs.StringVar(
		kubernetesVersion, KubernetesVersion, *kubernetesVersion,
		`Choose a specific Kubernetes version for the control plane.`,
	)
}

// AddKubeadmOtherFlags 将不绑定到配置文件的参数添加到指定参数集。
func AddKubeadmOtherFlags(flagSet *pflag.FlagSet, rootfsPath *string) {
	flagSet.StringVar(
		rootfsPath, "rootfs", *rootfsPath,
		"The path to the 'real' host root filesystem. This will cause kubeadm to chroot into the provided path.",
	)
}

// AddPatchesFlag 将 --patches 参数添加到指定参数集。
func AddPatchesFlag(fs *pflag.FlagSet, patchesDir *string) {
	const usage = `Path to a directory that contains files named ` +
		`"target[suffix][+patchtype].extension". For example, ` +
		`"kube-apiserver0+merge.yaml" or just "etcd.json". ` +
		`"target" can be one of "kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd", "kubeletconfiguration", "corednsdeployment", "kubeproxydaemonset". ` +
		`"patchtype" can be one of "strategic", "merge" or "json" and they match the patch formats ` +
		`supported by kubectl. The default "patchtype" is "strategic". "extension" must be either ` +
		`"json" or "yaml". "suffix" is an optional string that can be used to determine ` +
		`which patches are applied first alpha-numerically.`
	fs.StringVar(patchesDir, Patches, *patchesDir, usage)
}
