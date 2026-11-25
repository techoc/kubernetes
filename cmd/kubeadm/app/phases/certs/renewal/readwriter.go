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

package renewal

import (
	"crypto"
	"crypto/x509"
	"os"
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

// certificateReadWriter 定义读取或写入文件中存储或嵌入的证书的接口。
type certificateReadWriter interface {
	// Exists 在证书存在时返回 true。
	Exists() (bool, error)

	// 读取文件中存储或嵌入的证书。
	Read() (*x509.Certificate, error)

	// 写入或更新文件中存储或嵌入的证书。
	Write(*x509.Certificate, crypto.Signer) error
}

// pkiCertificateReadWriter 定义用于 kubeadm 管理的 K8s PKI 证书文件的 certificateReadWriter。
type pkiCertificateReadWriter struct {
	baseName       string
	certificateDir string
}

// newPKICertificateReadWriter 返回新的 pkiCertificateReadWriter。
func newPKICertificateReadWriter(certificateDir string, baseName string) *pkiCertificateReadWriter {
	return &pkiCertificateReadWriter{
		baseName:       baseName,
		certificateDir: certificateDir,
	}
}

// Exists 检查证书是否存在。
func (rw *pkiCertificateReadWriter) Exists() (bool, error) {
	certificatePath, _ := pkiutil.PathsForCertAndKey(rw.certificateDir, rw.baseName)
	return fileExists(certificatePath)
}

func fileExists(filename string) (bool, error) {
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !info.IsDir(), nil
}

// 从 kubeadm 管理的 K8s PKI 文件读取证书。
func (rw *pkiCertificateReadWriter) Read() (*x509.Certificate, error) {
	certificatePath, _ := pkiutil.PathsForCertAndKey(rw.certificateDir, rw.baseName)
	certs, err := certutil.CertsFromFile(certificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load existing certificate %s", rw.baseName)
	}

	// 安全地取第一项，因为发送方证书必须位于列表首位。
	// 详见 https://www.rfc-editor.org/rfc/rfc4346#section-7.4.2
	return certs[0], nil
}

// 将证书写入 kubeadm 管理的 K8s PKI 文件。
func (rw *pkiCertificateReadWriter) Write(newCert *x509.Certificate, newKey crypto.Signer) error {
	if err := pkiutil.WriteCertAndKey(rw.certificateDir, rw.baseName, newCert, newKey); err != nil {
		return errors.Wrapf(err, "failed to write new certificate %s", rw.baseName)
	}
	return nil
}

// kubeConfigReadWriter 定义用于 kubeadm 管理的 kubeConfig 文件中嵌入证书的 certificateReadWriter，
// 具体处理 AuthInfo 的客户端证书。
type kubeConfigReadWriter struct {
	kubernetesDir      string
	kubeConfigFileName string
	kubeConfigFilePath string
	kubeConfig         *clientcmdapi.Config
	baseName           string
	certificateDir     string
	caCert             *x509.Certificate
}

// newKubeconfigReadWriter 返回新的 kubeConfigReadWriter。
func newKubeconfigReadWriter(kubernetesDir string, kubeConfigFileName string, certificateDir, baseName string) *kubeConfigReadWriter {
	return &kubeConfigReadWriter{
		kubernetesDir:      kubernetesDir,
		kubeConfigFileName: kubeConfigFileName,
		kubeConfigFilePath: filepath.Join(kubernetesDir, kubeConfigFileName),
		certificateDir:     certificateDir,
		baseName:           baseName,
	}
}

// Exists 检查 kubeConfig 文件中是否嵌有证书。
func (rw *kubeConfigReadWriter) Exists() (bool, error) {
	return fileExists(rw.kubeConfigFilePath)
}

// 读取 kubeadm 管理的 kubeConfig 文件中嵌入的证书。
// ReadWriter 状态会保留 kubeConfig 文件本身，以便保留 Context、Servers、AuthInfo 等属性。
func (rw *kubeConfigReadWriter) Read() (*x509.Certificate, error) {
	// 尝试加载 kubeConfig 文件
	kubeConfig, err := clientcmd.LoadFromFile(rw.kubeConfigFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load kubeConfig file %s", rw.kubeConfigFilePath)
	}

	// 更新 kubeconfig 文件需要 CA 证书。
	// 本地 CA 续签时，磁盘上的 CA 可能已变化，因此需要重新加载。
	// CSR 续签则假定磁盘上的同一 CA 已挂载，供 KCM 的 --cluster-signing-cert-file 参数使用。
	certificatePath, _ := pkiutil.PathsForCertAndKey(rw.certificateDir, rw.baseName)
	caCerts, err := certutil.CertsFromFile(certificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load existing certificate %s", rw.baseName)
	}

	// 安全地取第一项，因为发送方证书必须位于列表首位。
	// 详见 https://www.rfc-editor.org/rfc/rfc4346#section-7.4.2
	rw.caCert = caCerts[0]

	// 获取当前上下文
	if _, ok := kubeConfig.Contexts[kubeConfig.CurrentContext]; !ok {
		return nil, errors.Errorf("invalid kubeConfig file %s: missing context %s", rw.kubeConfigFilePath, kubeConfig.CurrentContext)
	}

	// 获取当前上下文的集群信息，并确保其中嵌有服务器证书
	clusterName := kubeConfig.Contexts[kubeConfig.CurrentContext].Cluster
	if _, ok := kubeConfig.Clusters[clusterName]; !ok {
		return nil, errors.Errorf("invalid kubeConfig file %s: missing cluster %s", rw.kubeConfigFilePath, clusterName)
	}

	cluster := kubeConfig.Clusters[clusterName]
	if len(cluster.CertificateAuthorityData) == 0 {
		return nil, errors.Errorf("kubeConfig file %s does not have an embedded server certificate", rw.kubeConfigFilePath)
	}

	// 获取当前上下文的认证信息，并确保其中嵌有客户端证书
	authInfoName := kubeConfig.Contexts[kubeConfig.CurrentContext].AuthInfo
	if _, ok := kubeConfig.AuthInfos[authInfoName]; !ok {
		return nil, errors.Errorf("invalid kubeConfig file %s: missing authInfo %s", rw.kubeConfigFilePath, authInfoName)
	}

	authInfo := kubeConfig.AuthInfos[authInfoName]
	if len(authInfo.ClientCertificateData) == 0 {
		return nil, errors.Errorf("kubeConfig file %s does not have an embedded client certificate", rw.kubeConfigFilePath)
	}

	// 解析客户端证书、提取证书配置并续签
	certs, err := certutil.ParseCertsPEM(authInfo.ClientCertificateData)
	if err != nil {
		return nil, errors.Wrapf(err, "kubeConfig file %s does not contain a valid client certificate", rw.kubeConfigFilePath)
	}

	rw.kubeConfig = kubeConfig

	return certs[0], nil
}

// 将证书写入 kubeadm 管理的 kubeConfig 文件。
// 写入前必须调用 Read，以便保留 kubeConfig 文件的其他属性。
func (rw *kubeConfigReadWriter) Write(newCert *x509.Certificate, newKey crypto.Signer) error {
	// 检查写入前是否调用过 Read
	if rw.kubeConfig == nil {
		return errors.Errorf("failed to Write kubeConfig file with renewed certs. It is necessary to call Read before Write")
	}

	// 编码新私钥
	encodedClientKey, err := keyutil.MarshalPrivateKeyToPEM(newKey)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal private key to PEM")
	}

	// 更新 kubeconfig 文件中嵌入的 CA。
	// 假定用户已将当前上下文设为目标上下文。
	clusterName := rw.kubeConfig.Contexts[rw.kubeConfig.CurrentContext].Cluster
	cluster := rw.kubeConfig.Clusters[clusterName]
	cluster.CertificateAuthorityData = pkiutil.EncodeCertPEM(rw.caCert)

	// 获取当前上下文的认证信息，并确保其中嵌有客户端证书
	authInfoName := rw.kubeConfig.Contexts[rw.kubeConfig.CurrentContext].AuthInfo

	// 创建包含新客户端证书的 kubeConfig 副本
	newConfig := rw.kubeConfig.DeepCopy()
	newConfig.AuthInfos[authInfoName].ClientKeyData = encodedClientKey
	newConfig.AuthInfos[authInfoName].ClientCertificateData = pkiutil.EncodeCertPEM(newCert)

	// 将 kubeConfig 写入磁盘
	return clientcmd.WriteToFile(*newConfig, rw.kubeConfigFilePath)
}
