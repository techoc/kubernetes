/*
Copyright 2016 The Kubernetes Authors.

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

package controlplane

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	utilsnet "k8s.io/utils/net"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/features"
	"k8s.io/kubernetes/cmd/kubeadm/app/images"
	certphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/users"
)

// CreateInitStaticPodManifestFiles 写入启动控制平面所需的全部静态 Pod 清单文件。
func CreateInitStaticPodManifestFiles(manifestDir, patchesDir string, cfg *kubeadmapi.InitConfiguration, isDryRun bool) error {
	klog.V(1).Infoln("[control-plane] creating static Pod files")
	return CreateStaticPodFiles(manifestDir, patchesDir, &cfg.ClusterConfiguration, &cfg.LocalAPIEndpoint, isDryRun, kubeadmconstants.KubeAPIServer, kubeadmconstants.KubeControllerManager, kubeadmconstants.KubeScheduler)
}

// GetStaticPodSpecs 返回按当前配置填充的全部 staticPodSpecs。
// 注意：此方法包含 kubeadm 创建静态 Pod 清单所需的信息。
func GetStaticPodSpecs(cfg *kubeadmapi.ClusterConfiguration, endpoint *kubeadmapi.APIEndpoint, proxyEnvs []kubeadmapi.EnvVar) map[string]v1.Pod {
	// 获取所需的 hostPath 挂载
	mounts := getHostPathVolumesForTheControlPlane(cfg)
	if proxyEnvs == nil {
		proxyEnvs = kubeadmutil.GetProxyEnvVars(nil)
	}
	componentHealthCheckTimeout := kubeadmapi.GetActiveTimeouts().ControlPlaneComponentHealthCheck

	// 准备静态 Pod 配置
	staticPodSpecs := map[string]v1.Pod{
		kubeadmconstants.KubeAPIServer: staticpodutil.ComponentPod(v1.Container{
			Name:            kubeadmconstants.KubeAPIServer,
			Image:           images.GetKubernetesImage(kubeadmconstants.KubeAPIServer, cfg),
			ImagePullPolicy: v1.PullIfNotPresent,
			Command:         getAPIServerCommand(cfg, endpoint),
			VolumeMounts:    staticpodutil.VolumeMountMapToSlice(mounts.GetVolumeMounts(kubeadmconstants.KubeAPIServer)),
			LivenessProbe:   staticpodutil.LivenessProbe(staticpodutil.GetAPIServerProbeAddress(endpoint), "/livez", kubeadmconstants.ProbePort, v1.URISchemeHTTPS),
			ReadinessProbe:  staticpodutil.ReadinessProbe(staticpodutil.GetAPIServerProbeAddress(endpoint), "/readyz", kubeadmconstants.ProbePort, v1.URISchemeHTTPS),
			StartupProbe:    staticpodutil.StartupProbe(staticpodutil.GetAPIServerProbeAddress(endpoint), "/livez", kubeadmconstants.ProbePort, v1.URISchemeHTTPS, componentHealthCheckTimeout),
			Resources:       staticpodutil.ComponentResources("250m"),
			Env:             kubeadmutil.MergeKubeadmEnvVars(proxyEnvs, cfg.APIServer.ExtraEnvs),
			Ports: []v1.ContainerPort{
				{
					Name:          kubeadmconstants.ProbePort,
					ContainerPort: endpoint.BindPort,
					Protocol:      v1.ProtocolTCP,
				},
			},
		}, mounts.GetVolumes(kubeadmconstants.KubeAPIServer),
			map[string]string{kubeadmconstants.KubeAPIServerAdvertiseAddressEndpointAnnotationKey: endpoint.String()}),
		kubeadmconstants.KubeControllerManager: staticpodutil.ComponentPod(v1.Container{
			Name:            kubeadmconstants.KubeControllerManager,
			Image:           images.GetKubernetesImage(kubeadmconstants.KubeControllerManager, cfg),
			ImagePullPolicy: v1.PullIfNotPresent,
			Command:         getControllerManagerCommand(cfg),
			VolumeMounts:    staticpodutil.VolumeMountMapToSlice(mounts.GetVolumeMounts(kubeadmconstants.KubeControllerManager)),
			LivenessProbe:   staticpodutil.LivenessProbe(staticpodutil.GetControllerManagerProbeAddress(cfg), "/healthz", kubeadmconstants.ProbePort, v1.URISchemeHTTPS),
			StartupProbe:    staticpodutil.StartupProbe(staticpodutil.GetControllerManagerProbeAddress(cfg), "/healthz", kubeadmconstants.ProbePort, v1.URISchemeHTTPS, componentHealthCheckTimeout),
			Resources:       staticpodutil.ComponentResources("200m"),
			Env:             kubeadmutil.MergeKubeadmEnvVars(proxyEnvs, cfg.ControllerManager.ExtraEnvs),
			Ports: []v1.ContainerPort{
				{
					Name:          kubeadmconstants.ProbePort,
					ContainerPort: kubeadmconstants.KubeControllerManagerPort,
					Protocol:      v1.ProtocolTCP,
				},
			},
		}, mounts.GetVolumes(kubeadmconstants.KubeControllerManager), nil),
		kubeadmconstants.KubeScheduler: staticpodutil.ComponentPod(v1.Container{
			Name:            kubeadmconstants.KubeScheduler,
			Image:           images.GetKubernetesImage(kubeadmconstants.KubeScheduler, cfg),
			ImagePullPolicy: v1.PullIfNotPresent,
			Command:         getSchedulerCommand(cfg),
			VolumeMounts:    staticpodutil.VolumeMountMapToSlice(mounts.GetVolumeMounts(kubeadmconstants.KubeScheduler)),
			LivenessProbe:   staticpodutil.LivenessProbe(staticpodutil.GetSchedulerProbeAddress(cfg), "/livez", kubeadmconstants.ProbePort, v1.URISchemeHTTPS),
			ReadinessProbe:  staticpodutil.ReadinessProbe(staticpodutil.GetSchedulerProbeAddress(cfg), "/readyz", kubeadmconstants.ProbePort, v1.URISchemeHTTPS),
			StartupProbe:    staticpodutil.StartupProbe(staticpodutil.GetSchedulerProbeAddress(cfg), "/livez", kubeadmconstants.ProbePort, v1.URISchemeHTTPS, componentHealthCheckTimeout),
			Resources:       staticpodutil.ComponentResources("100m"),
			Env:             kubeadmutil.MergeKubeadmEnvVars(proxyEnvs, cfg.Scheduler.ExtraEnvs),
			Ports: []v1.ContainerPort{
				{
					Name:          kubeadmconstants.ProbePort,
					ContainerPort: kubeadmconstants.KubeSchedulerPort,
					Protocol:      v1.ProtocolTCP,
				},
			},
		}, mounts.GetVolumes(kubeadmconstants.KubeScheduler), nil),
	}
	return staticPodSpecs
}

// CreateStaticPodFiles 创建请求的全部静态 Pod 文件。
func CreateStaticPodFiles(manifestDir, patchesDir string, cfg *kubeadmapi.ClusterConfiguration, endpoint *kubeadmapi.APIEndpoint, isDryRun bool, componentNames ...string) error {
	// 获取按当前 ClusterConfiguration 填充的 StaticPodSpec
	klog.V(1).Infoln("[control-plane] getting StaticPodSpecs")
	specs := GetStaticPodSpecs(cfg, endpoint, nil)

	var usersAndGroups *users.UsersAndGroups
	var err error
	if features.Enabled(cfg.FeatureGates, features.RootlessControlPlane) {
		if isDryRun {
			fmt.Printf("[control-plane] Would create users and groups for %+v to run as non-root\n", componentNames)
		} else {
			usersAndGroups, err = staticpodutil.GetUsersAndGroups()
			if err != nil {
				return errors.Wrap(err, "failed to create users and groups")
			}
		}
	}

	// 创建所需的静态 Pod 配置
	for _, componentName := range componentNames {
		// 获取指定组件的 StaticPodSpec
		spec, exists := specs[componentName]
		if !exists {
			return errors.Errorf("couldn't retrieve StaticPodSpec for %q", componentName)
		}

		// 打印所有已挂载的卷
		for _, v := range spec.Spec.Volumes {
			klog.V(2).Infof("[control-plane] adding volume %q for component %q", v.Name, componentName)
		}

		if features.Enabled(cfg.FeatureGates, features.RootlessControlPlane) {
			if isDryRun {
				fmt.Printf("[control-plane] Would update static pod manifest for %q to run run as non-root\n", componentName)
			} else if usersAndGroups != nil {
				if err := staticpodutil.RunComponentAsNonRoot(componentName, &spec, usersAndGroups, cfg); err != nil {
					return errors.Wrapf(err, "failed to run component %q as non-root", componentName)
				}
			}
		}

		// 如果定义了 patchesDir，则修补静态 Pod 清单
		if patchesDir != "" {
			patchedSpec, err := staticpodutil.PatchStaticPod(&spec, patchesDir, os.Stdout)
			if err != nil {
				return errors.Wrapf(err, "failed to patch static Pod manifest file for %q", componentName)
			}
			spec = *patchedSpec
		}

		// 将 StaticPodSpec 写入磁盘
		if err := staticpodutil.WriteStaticPodToDisk(componentName, manifestDir, spec); err != nil {
			return errors.Wrapf(err, "failed to create static pod manifest file for %q", componentName)
		}

		klog.V(1).Infof("[control-plane] wrote static Pod manifest for component %q to %q\n", componentName, kubeadmconstants.GetStaticPodFilepath(componentName, manifestDir))
	}

	return nil
}

// getAPIServerCommand 根据给定配置对象和版本构建 API Server 命令。
func getAPIServerCommand(cfg *kubeadmapi.ClusterConfiguration, localAPIEndpoint *kubeadmapi.APIEndpoint) []string {
	defaultArguments := []kubeadmapi.Arg{
		{Name: "advertise-address", Value: localAPIEndpoint.AdvertiseAddress},
		{Name: "enable-admission-plugins", Value: "NodeRestriction"},
		{Name: "service-cluster-ip-range", Value: cfg.Networking.ServiceSubnet},
		{Name: "service-account-key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.ServiceAccountPublicKeyName)},
		{Name: "service-account-signing-key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.ServiceAccountPrivateKeyName)},
		{Name: "service-account-issuer", Value: fmt.Sprintf("https://kubernetes.default.svc.%s", cfg.Networking.DNSDomain)},
		{Name: "client-ca-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.CACertName)},
		{Name: "tls-cert-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.APIServerCertName)},
		{Name: "tls-private-key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.APIServerKeyName)},
		{Name: "kubelet-client-certificate", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.APIServerKubeletClientCertName)},
		{Name: "kubelet-client-key", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.APIServerKubeletClientKeyName)},
		{Name: "enable-bootstrap-token-auth", Value: "true"},
		{Name: "secure-port", Value: fmt.Sprintf("%d", localAPIEndpoint.BindPort)},
		{Name: "allow-privileged", Value: "true"},
		{Name: "kubelet-preferred-address-types", Value: "InternalIP,ExternalIP,Hostname"},
		// 添加前端代理配置参数。缺少生成的客户端证书时前端代理将不可用，
		// 因此始终使用推荐值添加这些参数。
		{Name: "requestheader-username-headers", Value: "X-Remote-User"},
		{Name: "requestheader-group-headers", Value: "X-Remote-Group"},
		{Name: "requestheader-extra-headers-prefix", Value: "X-Remote-Extra-"},
		{Name: "requestheader-client-ca-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.FrontProxyCACertName)},
		{Name: "requestheader-allowed-names", Value: "front-proxy-client"},
		{Name: "proxy-client-cert-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.FrontProxyClientCertName)},
		{Name: "proxy-client-key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.FrontProxyClientKeyName)},
	}

	command := []string{"kube-apiserver"}

	// 如果用户设置了外部 etcd 集群的端点
	if cfg.Etcd.External != nil {
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-servers", strings.Join(cfg.Etcd.External.Endpoints, ","), 1)

		// 使用用户提供的 etcd 证书
		if cfg.Etcd.External.CAFile != "" {
			defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-cafile", cfg.Etcd.External.CAFile, 1)
		}
		if cfg.Etcd.External.CertFile != "" && cfg.Etcd.External.KeyFile != "" {
			defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-certfile", cfg.Etcd.External.CertFile, 1)
			defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-keyfile", cfg.Etcd.External.KeyFile, 1)

		}
	} else {
		// 默认使用 localhost 上的 etcd 静态 Pod。
		// localhost 的 IP 协议族应与 AdvertiseAddress 一致。
		etcdLocalhostAddress := "127.0.0.1"
		if utilsnet.IsIPv6String(localAPIEndpoint.AdvertiseAddress) {
			etcdLocalhostAddress = "::1"
		}
		etcdServers := fmt.Sprintf("https://%s", net.JoinHostPort(etcdLocalhostAddress, strconv.Itoa(kubeadmconstants.EtcdListenClientPort)))
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-servers", etcdServers, 1)
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-cafile", filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdCACertName), 1)
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-certfile", filepath.Join(cfg.CertificatesDir, kubeadmconstants.APIServerEtcdClientCertName), 1)
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-keyfile", filepath.Join(cfg.CertificatesDir, kubeadmconstants.APIServerEtcdClientKeyName), 1)

		// 应用用户为本地 etcd 提供的配置
		if cfg.Etcd.Local != nil {
			if _, idx := kubeadmapi.GetArgValue(cfg.Etcd.Local.ExtraArgs, "advertise-client-urls", -1); idx > -1 {
				// advertise-client-urls 可替换 etcd 客户端 URL，也可通过 append 或 prepend 扩展。
				// 对默认 etcd-servers 应用相同的合并方式。
				arg := cfg.Etcd.Local.ExtraArgs[idx]
				if arg.MergeMethod != "" {
					klog.Warningf("the local etcd --advertise-client-urls flag has a merge method %q and value %q; merging it with --etcd-servers %q",
						arg.MergeMethod, arg.Value, etcdServers)
				}
				etcdServers = kubeadmapi.MergeArgWithBase(etcdServers, arg)
				defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "etcd-servers", etcdServers, 1)
			}
		}
	}

	if cfg.APIServer.ExtraArgs == nil {
		cfg.APIServer.ExtraArgs = []kubeadmapi.Arg{}
	}
	_, authzIdx := kubeadmapi.GetArgValue(cfg.APIServer.ExtraArgs, "authorization-mode", -1)
	_, hasStructuredAuthzVal := kubeadmapi.GetArgValue(cfg.APIServer.ExtraArgs, "authorization-config", -1)
	if hasStructuredAuthzVal == -1 {
		var authzArg *kubeadmapi.Arg
		if authzIdx > -1 {
			authzArg = &cfg.APIServer.ExtraArgs[authzIdx]
		}
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "authorization-mode", getAuthzModes(authzArg), 1)
	}
	command = append(command, kubeadmutil.ArgumentsToCommand(defaultArguments, cfg.APIServer.ExtraArgs)...)

	return command
}

// getAuthzModes 获取 API Server 的授权相关参数。
// kubeadm 未收到参数时默认使用 Node,RBAC；用户提供的模式会覆盖默认值。
func getAuthzModes(arg *kubeadmapi.Arg) string {
	defaultMode := []string{
		kubeadmconstants.ModeNode,
		kubeadmconstants.ModeRBAC,
	}
	defaultModeString := strings.Join(defaultMode, ",")

	if arg == nil || len(arg.Value) == 0 {
		return defaultModeString
	}

	mode := []string{}
	for requested := range strings.SplitSeq(arg.Value, ",") {
		if len(requested) == 0 { // 跳过空值。
			continue
		}
		if isValidAuthzMode(requested) {
			mode = append(mode, requested)
		} else {
			klog.Warningf("ignoring unknown kube-apiserver authorization-mode %q", requested)
		}
	}

	if len(mode) == 0 {
		return defaultModeString
	}

	// 仅在没有合并方式时校验整个模式列表。
	if arg.MergeMethod == "" && !compareAuthzModes(defaultMode, mode) {
		modeString := strings.Join(mode, ",")
		klog.Warningf("the default kube-apiserver authorization-mode is %q; using %q",
			defaultModeString,
			modeString,
		)
		return modeString
	}

	return defaultModeString
}

// compareAuthzModes 比较两个授权模式，不一致时返回 false。
func compareAuthzModes(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, m := range a {
		if m != b[i] {
			return false
		}
	}
	return true
}

func isValidAuthzMode(authzMode string) bool {
	allModes := []string{
		kubeadmconstants.ModeNode,
		kubeadmconstants.ModeRBAC,
		kubeadmconstants.ModeWebhook,
		kubeadmconstants.ModeABAC,
		kubeadmconstants.ModeAlwaysAllow,
		kubeadmconstants.ModeAlwaysDeny,
	}

	return slices.Contains(allModes, authzMode)
}

// getControllerManagerCommand 根据给定配置对象和版本构建 Controller Manager 命令。
func getControllerManagerCommand(cfg *kubeadmapi.ClusterConfiguration) []string {

	kubeconfigFile := filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.ControllerManagerKubeConfigFileName)
	caFile := filepath.Join(cfg.CertificatesDir, kubeadmconstants.CACertName)

	defaultArguments := []kubeadmapi.Arg{
		{Name: "bind-address", Value: "127.0.0.1"},
		{Name: "leader-elect", Value: "true"},
		{Name: "kubeconfig", Value: kubeconfigFile},
		{Name: "authentication-kubeconfig", Value: kubeconfigFile},
		{Name: "authorization-kubeconfig", Value: kubeconfigFile},
		{Name: "client-ca-file", Value: caFile},
		{Name: "requestheader-client-ca-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.FrontProxyCACertName)},
		{Name: "root-ca-file", Value: caFile},
		{Name: "service-account-private-key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.ServiceAccountPrivateKeyName)},
		{Name: "cluster-signing-cert-file", Value: caFile},
		{Name: "cluster-signing-key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.CAKeyName)},
		{Name: "use-service-account-credentials", Value: "true"},
		{Name: "controllers", Value: "*,bootstrapsigner,tokencleaner"},
	}

	// 使用外部 CA 时，向 Controller Manager 传空字符串而非 ca.key/ca.crt 路径，
	// 使 csrsigning 控制器无法启动。
	if res, _ := certphase.UsingExternalCA(cfg); res {
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "cluster-signing-key-file", "", 1)
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "cluster-signing-cert-file", "", 1)
	}

	// 让 Controller Manager 为 Pod 网络分配节点 CIDR。
	// 每个节点从 --pod-network-cidr 提供的地址 CIDR 中获得一个子网。
	if cfg.Networking.PodSubnet != "" {
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "allocate-node-cidrs", "true", 1)
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "cluster-cidr", cfg.Networking.PodSubnet, 1)
		if cfg.Networking.ServiceSubnet != "" {
			defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "service-cluster-ip-range", cfg.Networking.ServiceSubnet, 1)
		}
	}

	// 设置集群名称
	if cfg.ClusterName != "" {
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "cluster-name", cfg.ClusterName, 1)
	}

	command := []string{"kube-controller-manager"}
	command = append(command, kubeadmutil.ArgumentsToCommand(defaultArguments, cfg.ControllerManager.ExtraArgs)...)

	return command
}

// getSchedulerCommand 根据给定配置对象和版本构建调度器命令。
func getSchedulerCommand(cfg *kubeadmapi.ClusterConfiguration) []string {
	kubeconfigFile := filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.SchedulerKubeConfigFileName)
	defaultArguments := []kubeadmapi.Arg{
		{Name: "bind-address", Value: "127.0.0.1"},
		{Name: "leader-elect", Value: "true"},
		{Name: "kubeconfig", Value: kubeconfigFile},
		{Name: "authentication-kubeconfig", Value: kubeconfigFile},
		{Name: "authorization-kubeconfig", Value: kubeconfigFile},
	}

	command := []string{"kube-scheduler"}
	command = append(command, kubeadmutil.ArgumentsToCommand(defaultArguments, cfg.Scheduler.ExtraArgs)...)
	return command
}
