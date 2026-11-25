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

package discovery

import (
	"crypto/x509"
	"fmt"
	"net/url"

	clientset "k8s.io/client-go/kubernetes"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcertutil "k8s.io/client-go/util/cert"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/discovery/file"
	"k8s.io/kubernetes/cmd/kubeadm/app/discovery/https"
	"k8s.io/kubernetes/cmd/kubeadm/app/discovery/token"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pubkeypin"
)

// TokenUser 定义令牌用户。
const TokenUser = "tls-bootstrap-token-user"

// For 返回带有正确凭据、可用于执行 TLS Bootstrap 的 kubeconfig 对象。
// 返回前会确保能够信任 API Server，并在详细级别为 1 时输出 CA 证书信息供用户核验。
func For(client clientset.Interface, cfg *kubeadmapi.JoinConfiguration) (*clientcmdapi.Config, error) {
	config, err := DiscoverValidatedKubeConfig(client, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't validate the identity of the API Server")
	}

	// 如果用户提供了 TLSBootstrapToken，则在加入过程中使用它。
	// 这通常用于 Token 发现，也可用于未嵌入认证凭据的发现文件。
	var skipAuthCheck bool
	if len(cfg.Discovery.TLSBootstrapToken) != 0 {
		klog.V(1).Info("[discovery] Using provided TLSBootstrapToken as authentication credentials for the join process")

		_, clusterinfo, err := kubeconfigutil.GetClusterFromKubeConfig(config)
		if err != nil {
			return nil, errors.Wrapf(err, "malformed kubeconfig in the %s ConfigMap", bootstrapapi.ConfigMapClusterInfo)
		}
		config = kubeconfigutil.CreateWithToken(
			clusterinfo.Server,
			kubeadmapiv1.DefaultClusterName,
			TokenUser,
			clusterinfo.CertificateAuthorityData,
			cfg.Discovery.TLSBootstrapToken,
		)
		skipAuthCheck = true
	}

	// 如果发现返回的配置包含认证凭据，则继续执行 TLS Bootstrap 流程。
	if !skipAuthCheck && !kubeconfigutil.HasAuthenticationCredentials(config) {
		return nil, errors.New("could not find authentication credentials for the TLS bootstrap process. " +
			"Please use Token discovery, a discovery file with embedded authentication credentials or a discovery " +
			"file without authentication credentials but with the TLSBootstrapToken flag")
	}

	// 如果 kubeconfig 中存在 CA 证书，则输出该信息。
	var cert *x509.Certificate
	if cert, err = getCACertFromKubeconfig(config); err != nil {
		return nil, errors.Wrap(err, "could not get CA certificate from discovery kubeconfig")
	}
	klog.V(1).Infof("[discovery] Using CA certificate from discovery kubeconfig:\n%s", formatCACertInfo(cert))

	return config, nil
}

// DiscoverValidatedKubeConfig 返回经过验证的 Config 对象，其中指定集群位置和要信任的 CA 证书。
func DiscoverValidatedKubeConfig(dryRunClient clientset.Interface, cfg *kubeadmapi.JoinConfiguration) (*clientcmdapi.Config, error) {
	timeout := cfg.Timeouts.Discovery.Duration
	switch {
	case cfg.Discovery.File != nil:
		kubeConfigPath := cfg.Discovery.File.KubeConfigPath
		if isHTTPSURL(kubeConfigPath) {
			return https.RetrieveValidatedConfigInfo(kubeConfigPath, timeout)
		}
		return file.RetrieveValidatedConfigInfo(kubeConfigPath, timeout)
	case cfg.Discovery.BootstrapToken != nil:
		return token.RetrieveValidatedConfigInfo(dryRunClient, &cfg.Discovery, timeout)
	default:
		return nil, errors.New("couldn't find a valid discovery configuration")
	}
}

// isHTTPSURL 检查字符串是否可解析为 URL，以及 Scheme 是否为 https。
func isHTTPSURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && u.Scheme == "https"
}

// getCACertFromKubeconfig 在可用时从 kubeconfig 返回单个 CA 证书。
func getCACertFromKubeconfig(config *clientcmdapi.Config) (*x509.Certificate, error) {
	_, clusterInfo, err := kubeconfigutil.GetClusterFromKubeConfig(config)
	if err != nil {
		return nil, err
	}
	certs, err := clientcertutil.ParseCertsPEM(clusterInfo.CertificateAuthorityData)
	if err != nil {
		return nil, err
	}
	if len(certs) == 0 {
		return nil, errors.New("no CA certificate found in CertificateAuthorityData")
	}
	return certs[0], nil
}

// formatCACertInfo 格式化 CA 证书的摘要信息。
func formatCACertInfo(cert *x509.Certificate) string {
	return fmt.Sprintf("CA Certificate:\n"+
		"\tSubject: %v\n"+
		"\tIssuer: %v\n"+
		"\tSerialNumber: %v\n"+
		"\tNotBefore: %v\n"+
		"\tNotAfter: %v\n"+
		"\tSignatureAlgorithm: %v\n"+
		"\tPublicKeyAlgorithm: %v\n"+
		"\tHash: %v",
		cert.Subject, cert.Issuer, cert.SerialNumber, cert.NotBefore,
		cert.NotAfter, cert.SignatureAlgorithm, cert.PublicKeyAlgorithm,
		pubkeypin.Hash(cert),
	)
}
