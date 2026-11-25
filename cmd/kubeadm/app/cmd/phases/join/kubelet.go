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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeletphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/kubelet"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	dryrunutil "k8s.io/kubernetes/cmd/kubeadm/app/util/dryrun"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
)

// NewKubeletStartPhase 创建在节点上启动 kubelet 的 kubeadm 工作流阶段。
func NewKubeletStartPhase() workflow.Phase {
	return workflow.Phase{
		Name:  "kubelet-start [api-server-endpoint]",
		Short: "Write kubelet settings, certificates and (re)start the kubelet",
		Long:  "Write a file with KubeletConfiguration and an environment file with node specific kubelet settings, and then (re)start kubelet.",
		Run:   runKubeletStartJoinPhase,
		InheritFlags: []string{
			options.CfgPath,
			options.NodeCRISocket,
			options.NodeName,
			options.FileDiscovery,
			options.TokenDiscovery,
			options.TokenDiscoveryCAHash,
			options.TokenDiscoverySkipCAHash,
			options.TLSBootstrapToken,
			options.TokenStr,
			options.Patches,
			options.DryRun,
		},
	}
}

// NewKubeletWaitBootstrapPhase 创建等待 kubelet 在节点上完成 TLS 引导的 kubeadm 工作流阶段。
func NewKubeletWaitBootstrapPhase() workflow.Phase {
	return workflow.Phase{
		Name:  "kubelet-wait-bootstrap",
		Short: "Wait for the kubelet to bootstrap itself",
		Run:   runKubeletWaitBootstrapPhase,
		InheritFlags: []string{
			options.CfgPath,
			options.NodeCRISocket,
			options.DryRun,
		},
	}
}

func getKubeletStartJoinData(c workflow.RunData) (*kubeadmapi.JoinConfiguration, *kubeadmapi.InitConfiguration, *clientcmdapi.Config, error) {
	data, ok := c.(JoinData)
	if !ok {
		return nil, nil, nil, errors.New("kubelet-start phase invoked with an invalid data struct")
	}
	cfg := data.Cfg()
	initCfg, err := data.InitCfg()
	if err != nil {
		return nil, nil, nil, err
	}
	tlsBootstrapCfg, err := data.TLSBootstrapCfg()
	if err != nil {
		return nil, nil, nil, err
	}
	return cfg, initCfg, tlsBootstrapCfg, nil
}

// runKubeletStartJoinPhase 执行 kubelet TLS 引导过程。
// 此过程由 kubelet 执行，最终使节点凭节点授权器要求的专用凭据加入集群。
func runKubeletStartJoinPhase(c workflow.RunData) (returnErr error) {
	cfg, initCfg, tlsBootstrapCfg, err := getKubeletStartJoinData(c)
	if err != nil {
		return err
	}

	data, ok := c.(JoinData)
	if !ok {
		return errors.New("kubelet-start phase invoked with an invalid data struct")
	}
	bootstrapKubeConfigFile := filepath.Join(data.KubeConfigDir(), kubeadmconstants.KubeletBootstrapKubeConfigFileName)

	var client clientset.Interface
	// dry-run 时使用 joinData 中的客户端，否则创建新的引导客户端。
	if data.DryRun() {
		client, err = data.Client()
		if err != nil {
			return err
		}
	} else {
		client, err = kubeconfigutil.ToClientSet(tlsBootstrapCfg)
		if err != nil {
			return errors.Errorf("could not create client from bootstrap kubeconfig")
		}
	}

	// 启用功能开关时将服务器 URL 设为 LocalAPIEndpoint，
	// 使传给 kubelet 的配置强制其与本地 kube-apiserver 通信。
	if cfg.ControlPlane != nil {
		for c, conf := range tlsBootstrapCfg.Clusters {
			conf.Server, err = kubeadmutil.GetLocalAPIEndpoint(&cfg.ControlPlane.LocalAPIEndpoint)
			if err != nil {
				return errors.Wrapf(err, "could not get LocalAPIEndpoint")
			}
			tlsBootstrapCfg.Clusters[c] = conf
		}
	}

	// 将引导 kubelet 配置文件或 TLS 引导后的 kubelet 配置文件写入磁盘。
	klog.V(1).Infof("[kubelet-start] writing bootstrap kubelet config file at %s", bootstrapKubeConfigFile)
	if err := kubeconfigutil.WriteToDisk(bootstrapKubeConfigFile, tlsBootstrapCfg); err != nil {
		return errors.Wrap(err, "couldn't save bootstrap-kubelet.conf to disk")
	}

	// 将 CA 证书写入磁盘，供 kubelet 认证使用。
	cluster := tlsBootstrapCfg.Contexts[tlsBootstrapCfg.CurrentContext].Cluster

	// dry-run 时将 CA 证书写入临时目录。
	caPath := cfg.CACertPath
	if data.DryRun() {
		caPath = filepath.Join(data.CertificateWriteDir(), kubeadmconstants.CACertName)
	}

	if _, err := os.Stat(caPath); os.IsNotExist(err) {
		klog.V(1).Infof("[kubelet-start] writing CA certificate at %s", caPath)
		if err := certutil.WriteCert(caPath, tlsBootstrapCfg.Clusters[cluster].CertificateAuthorityData); err != nil {
			return errors.Wrap(err, "couldn't save the CA certificate to disk")
		}
	}

	// 获取此节点的名称。
	nodeName, _, err := kubeletphase.GetNodeNameAndHostname(&cfg.NodeRegistration)
	if err != nil {
		klog.Warning(err)
	}

	// 若集群中已存在同名且状态为 Ready 的节点，必须在 TLS 引导前退出。
	// 与现有控制平面节点同名的新节点可能导致未定义行为，最终使控制平面故障。
	klog.V(1).Infof("[kubelet-start] Checking for an existing Node in the cluster with name %q and status %q", nodeName, v1.NodeReady)
	node, err := client.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "cannot get Node %q", nodeName)
	}
	for _, cond := range node.Status.Conditions {
		if cond.Type == v1.NodeReady && cond.Status == v1.ConditionTrue {
			return errors.Errorf("a Node with name %q and status %q already exists in the cluster. "+
				"You must delete the existing Node or change the name of this new joining Node", nodeName, v1.NodeReady)
		}
	}

	// 配置 kubelet。在这段短时间内，kubeadm 会尝试停止并重启 kubelet。
	// 先停止 kubelet 服务，避免配置时发生竞态。
	if !data.DryRun() {
		klog.V(1).Infoln("[kubelet-start] Stopping the kubelet")
		kubeletphase.TryStopKubelet()
	} else {
		fmt.Println("[kubelet-start] Would stop the kubelet")
	}

	// 将实例 kubelet 配置文件写入磁盘。
	kubeletConfig := &kubeletconfig.KubeletConfiguration{
		ContainerRuntimeEndpoint: data.Cfg().NodeRegistration.CRISocket,
	}
	if err := kubeletphase.WriteInstanceConfigToDisk(kubeletConfig, data.KubeletDir()); err != nil {
		return errors.Wrap(err, "error writing instance kubelet configuration to disk")
	}

	// 将使用引导令牌凭据的 kubelet 配置写入磁盘，使 kubelet 能够启动。
	if err := kubeletphase.WriteConfigToDisk(&initCfg.ClusterConfiguration, data.KubeletDir(), data.PatchesDir(), data.OutputWriter()); err != nil {
		return err
	}

	// 写入 kubelet 使用的参数环境文件。仅在节点不是控制平面时，
	// 才按指定污渍注册加入节点；否则由 mark-control-plane 阶段注册污渍。
	registerTaintsUsingFlags := cfg.ControlPlane == nil
	if err := kubeletphase.WriteKubeletDynamicEnvFile(&initCfg.ClusterConfiguration, &initCfg.NodeRegistration, registerTaintsUsingFlags, data.KubeletDir()); err != nil {
		return err
	}

	if data.DryRun() {
		fmt.Println("[kubelet-start] Would start the kubelet")
		// dry-run 时打印 kubelet 配置清单；若加入控制平面，还打印静态 Pod 清单。
		// TODO: 考虑将此调用移到更合适的位置，例如隐藏阶段。
		if err := dryrunutil.PrintFilesIfDryRunning(cfg.ControlPlane != nil, data.ManifestDir(), data.OutputWriter()); err != nil {
			return errors.Wrap(err, "error printing files on dryrun")
		}
		return nil
	}

	// 若 kubelet 服务未运行，则尝试启动。
	fmt.Println("[kubelet-start] Starting the kubelet")
	kubeletphase.TryStartKubelet()

	return nil
}

// runKubeletWaitBootstrapPhase 等待 kubelet 完成 TLS 引导。
// 此过程由 kubelet 执行，最终使节点凭节点授权器要求的专用凭据加入集群。
func runKubeletWaitBootstrapPhase(c workflow.RunData) (returnErr error) {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("kubelet-start phase invoked with an invalid data struct")
	}

	if data.DryRun() {
		fmt.Println("[kubelet-wait] Would wait for the kubelet to be bootstrapped")
		return nil
	}

	cfg := data.Cfg()
	initCfg, err := data.InitCfg()
	if err != nil {
		return err
	}

	bootstrapKubeConfigFile := filepath.Join(data.KubeConfigDir(), kubeadmconstants.KubeletBootstrapKubeConfigFileName)
	// 删除 bootstrapKubeConfigFile，从磁盘移除用于 TLS 引导的凭据。
	defer func() {
		_ = os.Remove(bootstrapKubeConfigFile)
	}()

	// 对内存中的 kubelet 配置应用补丁，使下方等待过程尊重 kubelet 健康检查地址和端口等配置更改。
	// WriteConfigToDisk 已对写入磁盘的 kubelet.yaml 应用补丁。此操作必须在 WriteConfigToDisk 之后执行，
	// 因为两者使用同一内存配置，需避免对磁盘配置重复应用补丁。
	if err := kubeletphase.ApplyPatchesToConfig(&initCfg.ClusterConfiguration, data.PatchesDir()); err != nil {
		return errors.Wrap(err, "could not apply patches to the in-memory kubelet configuration")
	}

	// kubelet 随后执行 TLS 引导，将 /etc/kubernetes/bootstrap-kubelet.conf 转换为 /etc/kubernetes/kubelet.conf。
	// 等待 kubelet 创建 /etc/kubernetes/kubelet.conf；若超时，显示较友好的用户提示。
	waiter := apiclient.NewKubeWaiter(nil, 0, os.Stdout)
	waiter.SetTimeout(cfg.Timeouts.KubeletHealthCheck.Duration)
	kubeletConfig := initCfg.ClusterConfiguration.ComponentConfigs[componentconfigs.KubeletGroup].Get()
	kubeletConfigTyped, ok := kubeletConfig.(*kubeletconfig.KubeletConfiguration)
	if !ok {
		return errors.New("could not convert the KubeletConfiguration to a typed object")
	}
	if err := waiter.WaitForKubelet(kubeletConfigTyped.HealthzBindAddress, *kubeletConfigTyped.HealthzPort); err != nil {
		apiclient.PrintKubeletErrorHelpScreen(data.OutputWriter())
		return errors.Wrap(err, "failed while waiting for the kubelet to start")
	}

	if err := waitForTLSBootstrappedClient(cfg.Timeouts.TLSBootstrap.Duration); err != nil {
		apiclient.PrintKubeletErrorHelpScreen(data.OutputWriter())
		return errors.Wrap(err, "failed while waiting for TLS bootstrap")
	}

	return nil
}

// waitForTLSBootstrappedClient 等待 /etc/kubernetes/kubelet.conf 文件可用。
func waitForTLSBootstrappedClient(timeout time.Duration) error {
	fmt.Println("[kubelet-start] Waiting for the kubelet to perform the TLS Bootstrap")

	// 返回值为 false 时继续循环，返回错误时以该错误退出，返回 true 时成功退出。
	return wait.PollUntilContextTimeout(context.Background(),
		kubeadmconstants.TLSBootstrapRetryInterval, timeout,
		true, func(_ context.Context) (bool, error) {
			// 检查能否从 kubelet kubeconfig 创建客户端集。这不仅确认配置文件存在，
			// 也确认其引用的客户端证书和密钥等文件同样存在。
			_, err := kubeconfigutil.ClientSetFromFile(kubeadmconstants.GetKubeletKubeConfigPath())
			return (err == nil), nil
		})
}
