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

package kubeconfig

import (
	"fmt"
	"net"
	"os"
	"strconv"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// CreateBasic 创建可继续扩展的基础通用 KubeConfig 对象。
func CreateBasic(serverURL, clusterName, userName string, caCert []byte) *clientcmdapi.Config {
	// 使用集群和用户名作为 context 名称。
	contextName := fmt.Sprintf("%s@%s", userName, clusterName)

	return &clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			clusterName: {
				Server:                   serverURL,
				CertificateAuthorityData: caCert,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			contextName: {
				Cluster:  clusterName,
				AuthInfo: userName,
			},
		},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{},
		CurrentContext: contextName,
	}
}

// CreateWithCerts 创建使用客户端证书访问 API Server 的 KubeConfig。
func CreateWithCerts(serverURL, clusterName, userName string, caCert []byte, clientKey []byte, clientCert []byte) *clientcmdapi.Config {
	config := CreateBasic(serverURL, clusterName, userName, caCert)
	config.AuthInfos[userName] = &clientcmdapi.AuthInfo{
		ClientKeyData:         clientKey,
		ClientCertificateData: clientCert,
	}
	return config
}

// CreateWithToken 创建使用令牌访问 API Server 的 KubeConfig。
func CreateWithToken(serverURL, clusterName, userName string, caCert []byte, token string) *clientcmdapi.Config {
	config := CreateBasic(serverURL, clusterName, userName, caCert)
	config.AuthInfos[userName] = &clientcmdapi.AuthInfo{
		Token: token,
	}
	return config
}

// ClientSetFromFile 根据 kubeconfig 文件返回可直接使用的客户端。
func ClientSetFromFile(path string) (clientset.Interface, error) {
	config, err := clientcmd.LoadFromFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load admin kubeconfig")
	}
	return ToClientSet(config)
}

// ToClientSet 将 KubeConfig 对象转换为客户端。
func ToClientSet(config *clientcmdapi.Config) (clientset.Interface, error) {
	overrides := clientcmd.ConfigOverrides{Timeout: "10s"}
	clientConfig, err := clientcmd.NewDefaultClientConfig(*config, &overrides).ClientConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create API client configuration from kubeconfig")
	}

	client, err := clientset.NewForConfig(clientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create API client")
	}
	return client, nil
}

// WriteToDisk 以 0600 权限将 KubeConfig 写入磁盘。
func WriteToDisk(filename string, kubeconfig *clientcmdapi.Config) error {
	err := clientcmd.WriteToFile(*kubeconfig, filename)
	if err != nil {
		return err
	}

	return nil
}

// GetClusterFromKubeConfig 返回指定 KubeConfig 的默认 Cluster。
func GetClusterFromKubeConfig(config *clientcmdapi.Config) (string, *clientcmdapi.Cluster, error) {
	// 若存在未命名的集群对象，使用它。
	if config.Clusters[""] != nil {
		return "", config.Clusters[""], nil
	}

	currentContext := config.Contexts[config.CurrentContext]
	if currentContext != nil {
		if config.Clusters[currentContext.Cluster] != nil {
			return currentContext.Cluster, config.Clusters[currentContext.Cluster], nil
		}
		return "", nil, errors.Errorf("no matching cluster for the current context: %s", config.CurrentContext)
	}

	return "", nil, errors.Errorf("the current context is invalid: %s", config.CurrentContext)
}

// PointKubeConfigToLocalAPIEndpoint 将给定 kubeconfig 指向指定 APIEndpoint。
func PointKubeConfigToLocalAPIEndpoint(config *clientcmdapi.Config, lae *kubeadmapi.APIEndpoint) {
	for _, v := range config.Clusters {
		v.Server = fmt.Sprintf("https://%s",
			net.JoinHostPort(
				lae.AdvertiseAddress,
				strconv.Itoa(int(lae.BindPort)),
			),
		)
	}
}

// HasAuthenticationCredentials 判断当前用户是否具有有效的令牌、基本认证或 X509 凭据。
func HasAuthenticationCredentials(config *clientcmdapi.Config) bool {
	authInfo := getCurrentAuthInfo(config)
	if authInfo == nil {
		return false
	}

	// 令牌认证。
	if len(authInfo.Token) != 0 || len(authInfo.TokenFile) != 0 {
		return true
	}

	// 基本认证。
	if len(authInfo.Username) != 0 && len(authInfo.Password) != 0 {
		return true
	}

	// X509 认证。
	if (len(authInfo.ClientCertificate) != 0 || len(authInfo.ClientCertificateData) != 0) &&
		(len(authInfo.ClientKey) != 0 || len(authInfo.ClientKeyData) != 0) {
		return true
	}

	// exec 认证。
	if authInfo.Exec != nil && len(authInfo.Exec.Command) != 0 {
		return true
	}

	// authprovider 认证。
	if authInfo.AuthProvider != nil && len(authInfo.AuthProvider.Name) != 0 {
		return true
	}

	return false
}

// EnsureAuthenticationInfoAreEmbedded 检查认证信息是否由外部密钥或证书文件提供，
// 必要时将这些内容内嵌到 kubeconfig。
func EnsureAuthenticationInfoAreEmbedded(config *clientcmdapi.Config) error {
	authInfo := getCurrentAuthInfo(config)
	if authInfo == nil {
		return errors.New("invalid kubeconfig file. AuthInfo is not defined for the current user")
	}

	if len(authInfo.ClientCertificateData) == 0 && len(authInfo.ClientCertificate) != 0 {
		clientCert, err := os.ReadFile(authInfo.ClientCertificate)
		if err != nil {
			return errors.Wrap(err, "error while reading client cert file defined in kubeconfig")
		}
		authInfo.ClientCertificateData = clientCert
		authInfo.ClientCertificate = ""
	}
	if len(authInfo.ClientKeyData) == 0 && len(authInfo.ClientKey) != 0 {
		clientKey, err := os.ReadFile(authInfo.ClientKey)
		if err != nil {
			return errors.Wrap(err, "error while reading client key file defined in kubeconfig")
		}
		authInfo.ClientKeyData = clientKey
		authInfo.ClientKey = ""
	}
	if len(authInfo.Token) == 0 && len(authInfo.TokenFile) != 0 {
		tokenBytes, err := os.ReadFile(authInfo.TokenFile)
		if err != nil {
			return errors.Wrap(err, "error while reading token file defined in kubeconfig")
		}
		authInfo.Token = string(tokenBytes)
		authInfo.TokenFile = ""
	}

	return nil
}

// EnsureCertificateAuthorityIsEmbedded 检查 CA 是否由外部文件提供，必要时将其内嵌到 kubeconfig。
func EnsureCertificateAuthorityIsEmbedded(cluster *clientcmdapi.Cluster) error {
	if cluster == nil {
		return errors.New("received nil value for Cluster")
	}

	if len(cluster.CertificateAuthorityData) == 0 && len(cluster.CertificateAuthority) != 0 {
		ca, err := os.ReadFile(cluster.CertificateAuthority)
		if err != nil {
			return errors.Wrap(err, "error while reading certificate authority file defined in kubeconfig")
		}
		cluster.CertificateAuthorityData = ca
		cluster.CertificateAuthority = ""
	}

	return nil
}

// getCurrentAuthInfo 返回当前 authInfo；未定义时返回空值。
func getCurrentAuthInfo(config *clientcmdapi.Config) *clientcmdapi.AuthInfo {
	if config == nil || config.CurrentContext == "" ||
		len(config.Contexts) == 0 || config.Contexts[config.CurrentContext] == nil {
		return nil
	}
	user := config.Contexts[config.CurrentContext].AuthInfo

	if user == "" || len(config.AuthInfos) == 0 || config.AuthInfos[user] == nil {
		return nil
	}

	return config.AuthInfos[user]
}
