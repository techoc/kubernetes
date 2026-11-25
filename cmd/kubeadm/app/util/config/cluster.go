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

package config

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	certutil "k8s.io/client-go/util/cert"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"
	"sigs.k8s.io/yaml"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/config/strict"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
	kubeadmruntime "k8s.io/kubernetes/cmd/kubeadm/app/util/runtime"
)

// FetchInitConfigurationFromCluster 从集群中的 ConfigMap 获取配置。
// shortConfigMapGet 为真时，获取 kubeadm-config ConfigMap 仅短暂重试，
// 适用于 "kubeadm reset" 等不需要长时间重试的调用方。
func FetchInitConfigurationFromCluster(client clientset.Interface, printer output.Printer, logPrefix string, getNodeRegistration, getAPIEndpoint, getComponentConfigs, shortConfigMapGet bool) (*kubeadmapi.InitConfiguration, error) {
	if printer == nil {
		printer = &output.TextPrinter{}
	}
	_, _ = printer.Printf("[%s] Reading configuration from the %q ConfigMap in namespace %q...\n",
		logPrefix, constants.KubeadmConfigConfigMap, metav1.NamespaceSystem)
	_, _ = printer.Printf("[%s] Use 'kubeadm init phase upload-config kubeadm --config your-config-file' to re-upload it.\n", logPrefix)

	// 从集群获取实际配置。
	cfg, err := getInitConfigurationFromCluster(constants.KubernetesDir, constants.KubeletRunDirectory, client, getNodeRegistration, getAPIEndpoint, getComponentConfigs, shortConfigMapGet)
	if err != nil {
		return nil, err
	}

	// 应用动态默认值。
	// 注意：跳过 CRI 检测，因为此处不会使用且稍后会被覆盖。
	// 注意：调用方未请求端点时（如 worker join），跳过 LocalAPIEndpoint 默认值设置。
	if err := SetInitDynamicDefaults(cfg, true, !getAPIEndpoint); err != nil {
		return nil, err
	}

	return cfg, nil
}

// getInitConfigurationFromCluster 仅为测试拆分，请勿直接调用，应使用 FetchInitConfigurationFromCluster。
func getInitConfigurationFromCluster(kubeconfigDir string, kubeletRunDir string, client clientset.Interface, getNodeRegistration, getAPIEndpoint, getComponentConfigs, shortConfigMapGet bool) (*kubeadmapi.InitConfiguration, error) {
	// 此外，该 ConfigMap 更恰当的命名应是 KubeadmConfigConfigMap。
	var configMap *v1.ConfigMap
	var err error
	if shortConfigMapGet {
		configMap, err = apiclient.GetConfigMapWithShortRetry(client, metav1.NamespaceSystem, constants.KubeadmConfigConfigMap)
	} else {
		var lastErr error
		err = wait.PollUntilContextTimeout(context.Background(),
			constants.KubernetesAPICallRetryInterval,
			kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration,
			true, func(_ context.Context) (bool, error) {
				var err error
				configMap, err = client.CoreV1().ConfigMaps(metav1.NamespaceSystem).Get(
					context.Background(), constants.KubeadmConfigConfigMap, metav1.GetOptions{})
				if err == nil {
					return true, nil
				}
				lastErr = err
				return false, nil
			})
		if err != nil {
			err = lastErr
		}
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to get config map")
	}

	// 创建空的版本化 InitConfiguration，应用静态默认值后转换为内部类型。
	versionedInitcfg := &kubeadmapiv1.InitConfiguration{}
	kubeadmscheme.Scheme.Default(versionedInitcfg)
	initcfg := &kubeadmapi.InitConfiguration{}
	if err := kubeadmscheme.Scheme.Convert(versionedInitcfg, initcfg, nil); err != nil {
		return nil, errors.Wrap(err, "could not prepare a defaulted InitConfiguration")
	}

	// 从 kubeadm-config 获取 ClusterConfiguration。
	clusterConfigurationData, ok := configMap.Data[constants.ClusterConfigurationConfigMapKey]
	if !ok {
		return nil, errors.Errorf("unexpected error when reading kubeadm-config ConfigMap: %s key value pair missing", constants.ClusterConfigurationConfigMapKey)
	}
	// 若 ClusterConfiguration 被 kubeadm 之外的组件修改并产生错误，发出警告。
	if err := strict.VerifyUnmarshalStrict([]*runtime.Scheme{kubeadmscheme.Scheme},
		kubeadmapiv1.SchemeGroupVersion.WithKind(constants.ClusterConfigurationKind),
		[]byte(clusterConfigurationData)); err != nil {
		klog.Warning(err.Error())
	}
	if err := runtime.DecodeInto(kubeadmscheme.Codecs.UniversalDecoder(), []byte(clusterConfigurationData), &initcfg.ClusterConfiguration); err != nil {
		return nil, errors.Wrap(err, "failed to decode cluster configuration data")
	}

	if getComponentConfigs {
		// 从对应的 ConfigMap 获取组件配置。
		if err := componentconfigs.FetchFromCluster(&initcfg.ClusterConfiguration, client); err != nil {
			return nil, errors.Wrap(err, "failed to get component configs")
		}
	}

	if getNodeRegistration {
		if err := GetNodeRegistration(kubeconfigDir, kubeletRunDir, client, &initcfg.NodeRegistration); err != nil {
			return nil, errors.Wrap(err, "failed to get node registration")
		}
	}

	if getAPIEndpoint {
		// 获取当前控制平面节点的 APIEndpoint。
		if err := GetAPIEndpoint(client, initcfg.NodeRegistration.Name, &initcfg.LocalAPIEndpoint); err != nil {
			return nil, errors.Wrap(err, "failed to getAPIEndpoint")
		}
	}

	return initcfg, nil
}

// GetNodeName 依次尝试三种方式获取节点名称：先根据给定 kubeconfig 创建客户端并获取
// SelfSubjectReview（类似 "kubectl auth whoami"）；失败后解析 kubeconfig 客户端证书主题；
// 最后回退到主机名，但节点名称被覆盖时主机名可能不正确。
func GetNodeName(kubeconfigFile string) (string, error) {
	var (
		nodeName string
		err      error
	)
	if kubeconfigFile != "" {
		client, err := kubeconfig.ClientSetFromFile(kubeconfigFile)
		if err == nil {
			nodeName, err = getNodeNameFromSSR(client)
			if err == nil {
				return nodeName, nil
			}
		}
		nodeName, err = getNodeNameFromKubeletConfig(kubeconfigFile)
		if err == nil {
			return nodeName, nil
		}
	}
	nodeName, err = nodeutil.GetHostname("")
	if err != nil {
		return "", errors.Wrapf(err, "could not get node name")
	}
	return nodeName, nil
}

// GetNodeRegistration 返回当前节点的 nodeRegistration。
func GetNodeRegistration(kubeconfigDir, kubeletRunDir string, client clientset.Interface, nodeRegistration *kubeadmapi.NodeRegistrationOptions) error {
	// 获取当前节点名称。
	nodeName, err := GetNodeName(filepath.Join(kubeconfigDir, constants.KubeletKubeConfigFileName))
	if err != nil {
		return err
	}

	// 获取对应节点并读取其中存储的属性。
	node, err := client.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get corresponding node")
	}

	var criSocket string
	configFilePath := filepath.Join(kubeletRunDir, constants.KubeletInstanceConfigurationFileName)
	_, err = os.Stat(configFilePath)
	if os.IsNotExist(err) {
		klog.Warningf("Node %q lacks a kubelet instance config file %q; attempting auto-detection",
			nodeName, configFilePath)
		criSocket, err = kubeadmruntime.DetectCRISocket()
		if err != nil {
			klog.Warningf("Auto-detection of CRI socket failed for node %q: %v; falling back to default %q",
				nodeName, err, constants.DefaultCRISocket)
			criSocket = constants.DefaultCRISocket
		}
	} else if err != nil {
		return err
	} else {
		kubeletConfig, err := readKubeletConfig(kubeletRunDir, constants.KubeletInstanceConfigurationFileName)
		if err != nil {
			return errors.Wrapf(err, "could not read kubelet instance configuration on node %q", nodeName)
		}
		criSocket = kubeletConfig.ContainerRuntimeEndpoint
	}

	// 返回 nodeRegistration 属性。
	nodeRegistration.Name = nodeName
	nodeRegistration.CRISocket = criSocket
	nodeRegistration.Taints = node.Spec.Taints
	// 注意：nodeRegistration.KubeletExtraArgs 目前不存储在节点级别，仅在 kubeadm-flags.env 中，
	// 且升级时不会修改。未来可能重新考虑，从而允许升级时修改该文件。
	return nil
}

// getNodeNameFromKubeletConfig 从 kubelet 配置文件获取节点名称。
// TODO: 未来希望改用更规范的方式，例如将信息存放在本地 kubelet 配置文件中。
func getNodeNameFromKubeletConfig(fileName string) (string, error) {
	// 加载 kubelet.conf 文件。
	config, err := clientcmd.LoadFromFile(fileName)
	if err != nil {
		return "", err
	}

	// 获取当前用户信息。
	currentContext, exists := config.Contexts[config.CurrentContext]
	if !exists {
		return "", errors.Errorf("invalid kubeconfig file %s: missing context %s", fileName, config.CurrentContext)
	}
	authInfo, exists := config.AuthInfos[currentContext.AuthInfo]
	if !exists {
		return "", errors.Errorf("invalid kubeconfig file %s: missing AuthInfo %s", fileName, currentContext.AuthInfo)
	}

	// 获取包含当前用户凭据的 X509 证书。
	var certs []*x509.Certificate
	if len(authInfo.ClientCertificateData) > 0 {
		// 若配置文件内嵌 X509 证书（如 kubeadm 创建的 kubelet.conf），解析它。
		if certs, err = certutil.ParseCertsPEM(authInfo.ClientCertificateData); err != nil {
			return "", err
		}
	} else if len(authInfo.ClientCertificate) > 0 {
		// 若配置文件引用外部 X509 证书（如 TLS bootstrap 创建的 kubelet.conf），加载它。
		if certs, err = certutil.CertsFromFile(authInfo.ClientCertificate); err != nil {
			return "", err
		}
	} else {
		return "", errors.Errorf("invalid kubeconfig file %s. x509 certificate expected", fileName)
	}

	// 发送方证书必须位于列表首位，因此可安全取第一项。
	// 详见：https://www.rfc-editor.org/rfc/rfc4346#section-7.4.2
	cert := certs[0]

	// 从证书 common name 获取节点名称。
	return strings.TrimPrefix(cert.Subject.CommonName, constants.NodesUserPrefix), nil
}

// getNodeNameFromSSR 从给定客户端的 SelfSubjectReview 读取节点名称。
// 传入 kubelet.conf 时也可用于获取节点名称。
func getNodeNameFromSSR(client clientset.Interface) (string, error) {
	ssr, err := client.AuthenticationV1().SelfSubjectReviews().
		Create(context.Background(), &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	user := ssr.Status.UserInfo.Username
	if !strings.HasPrefix(user, constants.NodesUserPrefix) {
		return "", errors.Errorf("%q is not a node client, must have %q prefix in the name",
			user, constants.NodesUserPrefix)
	}
	return strings.TrimPrefix(user, constants.NodesUserPrefix), nil
}

// GetAPIEndpoint 获取给定节点的 API 端点。
func GetAPIEndpoint(client clientset.Interface, nodeName string, apiEndpoint *kubeadmapi.APIEndpoint) error {
	return getAPIEndpointWithRetry(client, nodeName, apiEndpoint,
		constants.KubernetesAPICallRetryInterval, kubeadmapi.GetActiveTimeouts().KubernetesAPICall.Duration)
}

func getAPIEndpointWithRetry(client clientset.Interface, nodeName string, apiEndpoint *kubeadmapi.APIEndpoint,
	interval, timeout time.Duration) error {
	err := getAPIEndpointFromPodAnnotation(client, nodeName, apiEndpoint, interval, timeout)
	if err != nil {
		return errors.WithMessagef(err, "could not retrieve API endpoints for node %q using pod annotations", nodeName)
	}
	return nil
}

func getAPIEndpointFromPodAnnotation(client clientset.Interface, nodeName string, apiEndpoint *kubeadmapi.APIEndpoint,
	interval, timeout time.Duration) error {
	var rawAPIEndpoint string
	var lastErr error
	// 容忍 API 服务器或负载均衡器的短暂异常；静态 Pod 尚未镜像到 API 服务器时也等待其传播完成。
	err := wait.PollUntilContextTimeout(context.Background(), interval, timeout, true,
		func(ctx context.Context) (bool, error) {
			rawAPIEndpoint, lastErr = getRawAPIEndpointFromPodAnnotationWithoutRetry(ctx, client, nodeName)
			return lastErr == nil, nil
		})
	if err != nil {
		return err
	}
	parsedAPIEndpoint, err := kubeadmapi.APIEndpointFromString(rawAPIEndpoint)
	if err != nil {
		return errors.Wrapf(err, "could not parse API endpoint for node %q", nodeName)
	}
	*apiEndpoint = parsedAPIEndpoint
	return nil
}

func getRawAPIEndpointFromPodAnnotationWithoutRetry(ctx context.Context, client clientset.Interface, nodeName string) (string, error) {
	podList, err := client.CoreV1().Pods(metav1.NamespaceSystem).List(
		ctx,
		metav1.ListOptions{
			FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
			LabelSelector: fmt.Sprintf("component=%s,tier=%s", constants.KubeAPIServer, constants.ControlPlaneTier),
		},
	)
	if err != nil {
		return "", errors.Wrap(err, "could not retrieve list of pods to determine api server endpoints")
	}
	if len(podList.Items) != 1 {
		return "", errors.Errorf("API server pod for node name %q has %d entries, only one was expected", nodeName, len(podList.Items))
	}
	if apiServerEndpoint, ok := podList.Items[0].Annotations[constants.KubeAPIServerAdvertiseAddressEndpointAnnotationKey]; ok {
		return apiServerEndpoint, nil
	}
	return "", errors.Errorf("API server pod for node name %q hasn't got a %q annotation, cannot retrieve API endpoint", nodeName, constants.KubeAPIServerAdvertiseAddressEndpointAnnotationKey)
}

// readKubeletConfig 从指定文件读取 KubeletConfiguration。
func readKubeletConfig(kubeletDir, fileName string) (*kubeletconfig.KubeletConfiguration, error) {
	kubeletFile := path.Join(kubeletDir, fileName)

	data, err := os.ReadFile(kubeletFile)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read kubelet configuration file %q", kubeletFile)
	}

	var config kubeletconfig.KubeletConfiguration
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, errors.Wrapf(err, "could not parse kubelet configuration file %q", kubeletFile)
	}

	return &config, nil
}
