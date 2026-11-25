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

package file

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
)

// RetrieveValidatedConfigInfo 连接 API Server，使用提供的 CA 证书确保能够安全通信，
// 并可选择从 cluster-info ConfigMap 刷新 cluster-info 信息。
func RetrieveValidatedConfigInfo(filepath string, discoveryTimeout time.Duration) (*clientcmdapi.Config, error) {
	config, err := clientcmd.LoadFromFile(filepath)
	if err != nil {
		return nil, err
	}
	return ValidateConfigInfo(config, discoveryTimeout)
}

// ValidateConfigInfo 连接 API Server，使用提供的 CA 证书或客户端证书确保能够安全通信，
// 并可选择从 cluster-info ConfigMap 刷新 cluster-info 信息。
func ValidateConfigInfo(config *clientcmdapi.Config, discoveryTimeout time.Duration) (*clientcmdapi.Config, error) {
	if len(config.Clusters) < 1 {
		return nil, errors.New("the provided kubeconfig file must have at least one Cluster defined")
	}
	currentClusterName, currentCluster, err := kubeconfigutil.GetClusterFromKubeConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "the provided kubeconfig file is malformed")
	}
	if err := clientcmd.Validate(*config); err != nil {
		return nil, err
	}

	// 如果 kubeconfig 中的 CA 指向文件，则确保 CA 文件内容已嵌入配置。
	if err := kubeconfigutil.EnsureCertificateAuthorityIsEmbedded(currentCluster); err != nil {
		return nil, err
	}

	// 如果发现文件配置包含认证凭据。
	if kubeconfigutil.HasAuthenticationCredentials(config) {
		klog.V(1).Info("[discovery] Using authentication credentials from the discovery file for validating TLS connection")

		// 应确保所有认证信息都嵌入配置文件，这样将 kubeconfig 文件存储到 /etc/kubernetes/boostrap-kubelet.conf 后仍能正常工作。
		if err := kubeconfigutil.EnsureAuthenticationInfoAreEmbedded(config); err != nil {
			return nil, err
		}
	} else {
		// 如果发现文件配置不包含认证凭据。
		klog.V(1).Info("[discovery] Discovery file does not contains authentication credentials, using unauthenticated request for validating TLS connection")
	}

	// 尝试读取 cluster-info ConfigMap；原始设计要求执行此步骤，以便尽早验证到服务器的 TLS 连接。
	client, err := kubeconfigutil.ToClientSet(config)
	if err != nil {
		return nil, err
	}

	klog.V(1).Infof("[discovery] Created cluster-info discovery client, requesting info from %q\n", currentCluster.Server)

	var clusterinfoCM *v1.ConfigMap

	var lastError error
	err = wait.PollUntilContextTimeout(context.Background(),
		constants.DiscoveryRetryInterval, discoveryTimeout,
		true, func(_ context.Context) (bool, error) {
			clusterinfoCM, lastError = client.CoreV1().ConfigMaps(metav1.NamespacePublic).Get(context.Background(), bootstrapapi.ConfigMapClusterInfo, metav1.GetOptions{})
			if lastError != nil {
				if apierrors.IsForbidden(lastError) {
					// 如果请求返回禁止访问错误，说明集群管理员未允许匿名客户端访问 cluster-info ConfigMap。
					// 此时信任集群管理员，不刷新 cluster-info 数据。
					klog.Warningf("[discovery] Could not access the %s ConfigMap for refreshing the cluster-info information, but the TLS cert is valid so proceeding...\n", bootstrapapi.ConfigMapClusterInfo)
					return true, nil
				}
				klog.V(1).Infof("[discovery] Error reading the %s ConfigMap, will try again: %v\n", bootstrapapi.ConfigMapClusterInfo, lastError)
				return false, nil
			}
			return true, nil
		})
	if err != nil {
		return nil, errors.Wrapf(lastError, "Abort reading the %s ConfigMap after timeout of %v",
			bootstrapapi.ConfigMapClusterInfo, discoveryTimeout)
	}

	// 如果无法获取 cluster-info ConfigMap，则直接返回用户提供的 cluster-info 对象。
	if clusterinfoCM == nil {
		return config, nil
	}

	// 已获取 ConfigMap，尝试从中读取数据；如果失败，则回退到用户提供的文件。
	refreshedBaseKubeConfig, err := tryParseClusterInfoFromConfigMap(clusterinfoCM)
	if err != nil {
		klog.V(1).Infof("[discovery] The %s ConfigMap isn't set up properly (%v), but the TLS cert is valid so proceeding...\n", bootstrapapi.ConfigMapClusterInfo, err)
		return config, nil
	}

	_, refreshedCluster, err := kubeconfigutil.GetClusterFromKubeConfig(refreshedBaseKubeConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "malformed kubeconfig in the %s ConfigMap", bootstrapapi.ConfigMapClusterInfo)
	}
	if currentCluster.Server != refreshedCluster.Server {
		klog.Warningf("[discovery] the API Server endpoint %q in use is different from the endpoint %q which defined in the %s ConfigMap", currentCluster.Server, refreshedCluster.Server, bootstrapapi.ConfigMapClusterInfo)
	}

	if len(currentCluster.CertificateAuthorityData) == 0 && len(refreshedCluster.CertificateAuthorityData) > 0 {
		config.Clusters[currentClusterName].CertificateAuthorityData = refreshedCluster.CertificateAuthorityData
		klog.V(1).Infof("[discovery] Synced CertificateAuthorityData from the %s ConfigMap", bootstrapapi.ConfigMapClusterInfo)
	}

	return config, nil
}

// tryParseClusterInfoFromConfigMap 尝试从 ConfigMap 的键中解析 kubeconfig 文件。
func tryParseClusterInfoFromConfigMap(cm *v1.ConfigMap) (*clientcmdapi.Config, error) {
	kubeConfigString, ok := cm.Data[bootstrapapi.KubeConfigKey]
	if !ok || len(kubeConfigString) == 0 {
		return nil, errors.Errorf("no %s key in ConfigMap", bootstrapapi.KubeConfigKey)
	}
	parsedKubeConfig, err := clientcmd.Load([]byte(kubeConfigString))
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse the kubeconfig file in the %s ConfigMap", bootstrapapi.ConfigMapClusterInfo)
	}
	return parsedKubeConfig, nil
}
