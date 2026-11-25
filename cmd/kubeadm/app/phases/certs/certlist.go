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

package certs

import (
	"crypto"
	"crypto/x509"
	"fmt"
	"io"
	"path/filepath"
	"time"

	certutil "k8s.io/client-go/util/cert"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

const (
	errInvalid = "invalid argument"
	errExist   = "file already exists"
)

type configMutatorsFunc func(*kubeadmapi.InitConfiguration, *pkiutil.CertConfig) error

// KubeadmCert 表示 kubeadm 为正常运行而需要创建的证书。
type KubeadmCert struct {
	Name     string
	LongName string
	BaseName string
	CAName   string
	// 部分属性取决于 InitConfiguration，只能在运行时确定。
	// 这些函数将按顺序执行，并接收 InitConfiguration 和证书 Config。
	configMutators []configMutatorsFunc
	config         pkiutil.CertConfig
	// 仅供单元测试使用。
	creationTime time.Time
}

// GetConfig 根据给定的 InitConfiguration 返回指定证书的定义。
func (k *KubeadmCert) GetConfig(ic *kubeadmapi.InitConfiguration) (*pkiutil.CertConfig, error) {
	for _, f := range k.configMutators {
		if err := f(ic, &k.config); err != nil {
			return nil, err
		}
	}

	// creationTime 应仅在单元测试中设置，否则应使用 kubeadm 的启动时间。
	if k.creationTime.IsZero() {
		k.creationTime = kubeadmutil.StartTimeUTC()
	}

	// 将证书生效时间提前，以容忍小幅时间回退。
	k.config.NotBefore = k.creationTime.Add(-kubeadmconstants.CertificateBackdate)

	// 使用 ClusterConfiguration 中定义的有效期。
	// 如果 CAName 为空，则此证书为 CA 证书。
	if len(k.CAName) != 0 {
		if ic.ClusterConfiguration.CertificateValidityPeriod != nil {
			k.config.NotAfter = k.creationTime.
				Add(ic.ClusterConfiguration.CertificateValidityPeriod.Duration)
		}
	} else {
		if ic.ClusterConfiguration.CACertificateValidityPeriod != nil {
			k.config.NotAfter = k.creationTime.
				Add(ic.ClusterConfiguration.CACertificateValidityPeriod.Duration)
		}
	}

	// 使用 ClusterConfiguration 中指定的加密算法。
	k.config.EncryptionAlgorithm = ic.ClusterConfiguration.EncryptionAlgorithm
	return &k.config, nil
}

// CreateFromCA 使用给定的 CA 证书和密钥生成证书并写入磁盘。
func (k *KubeadmCert) CreateFromCA(ic *kubeadmapi.InitConfiguration, caCert *x509.Certificate, caKey crypto.Signer) error {
	cfg, err := k.GetConfig(ic)
	if err != nil {
		return errors.Wrapf(err, "couldn't create %q certificate", k.Name)
	}
	cert, key, err := pkiutil.NewCertAndKey(caCert, caKey, cfg)
	if err != nil {
		return err
	}
	err = writeCertificateFilesIfNotExist(
		ic.CertificatesDir,
		k.BaseName,
		caCert,
		cert,
		key,
		cfg,
	)

	if err != nil {
		return errors.Wrapf(err, "failed to write or validate certificate %q", k.Name)
	}

	return nil
}

// CreateAsCA 创建证书颁发机构，将文件写入磁盘并返回该 CA，以便为子证书签名。
func (k *KubeadmCert) CreateAsCA(ic *kubeadmapi.InitConfiguration) (*x509.Certificate, crypto.Signer, error) {
	cfg, err := k.GetConfig(ic)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't get configuration for %q CA certificate", k.Name)
	}
	caCert, caKey, err := pkiutil.NewCertificateAuthority(cfg)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't generate %q CA certificate", k.Name)
	}

	err = writeCertificateAuthorityFilesIfNotExist(
		ic.CertificatesDir,
		k.BaseName,
		caCert,
		caKey,
	)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't write out %q CA certificate", k.Name)
	}

	return caCert, caKey, nil
}

// CertificateTree 表示一层深的证书树，将 CA 映射到依赖它的证书。
type CertificateTree map[*KubeadmCert]Certificates

// CreateTree 创建 CA、由 CA 签名的证书，并将它们全部写入磁盘。
func (t CertificateTree) CreateTree(ic *kubeadmapi.InitConfiguration) error {
	for ca, leaves := range t {
		cfg, err := ca.GetConfig(ic)
		if err != nil {
			return err
		}

		var caKey crypto.Signer

		caCert, err := pkiutil.TryLoadCertFromDisk(ic.CertificatesDir, ca.BaseName)
		if err == nil {
			// 校验有效期
			CheckCertificatePeriodValidity(ca.BaseName, caCert)

			// 证书已存在，确保其有效
			if !caCert.IsCA {
				return errors.Errorf("certificate %q is not a CA", ca.Name)
			}
			// 尝试加载 CA 私钥
			caKey, err = pkiutil.TryLoadKeyFromDisk(ic.CertificatesDir, ca.BaseName)
			if err != nil {
				// 如果没有 CA 私钥，确保所有证书均已存在。
				for _, leaf := range leaves {
					cl := certKeyLocation{
						pkiDir:   ic.CertificatesDir,
						baseName: leaf.BaseName,
						uxName:   leaf.Name,
					}
					if err := validateSignedCertWithCA(cl, caCert); err != nil {
						return errors.Wrapf(err, "could not load expected certificate %q or validate the existence of key %q for it", leaf.Name, ca.Name)
					}
				}
				continue
			}
			// CA 私钥已存在，直接用它创建新证书。
			klog.V(1).Infof("[certs] Using the existing CA certificate %q and key %q\n", filepath.Join(ic.CertificatesDir, fmt.Sprintf("%s.crt", ca.BaseName)), filepath.Join(ic.CertificatesDir, fmt.Sprintf("%s.key", ca.BaseName)))
		} else {
			// CA 证书尚不存在，创建新证书和私钥。
			caCert, caKey, err = pkiutil.NewCertificateAuthority(cfg)
			if err != nil {
				return err
			}

			err = writeCertificateAuthorityFilesIfNotExist(
				ic.CertificatesDir,
				ca.BaseName,
				caCert,
				caKey,
			)
			if err != nil {
				return err
			}
		}

		for _, leaf := range leaves {
			if err := leaf.CreateFromCA(ic, caCert, caKey); err != nil {
				return err
			}
		}
	}
	return nil
}

// CertificateMap 是以 Name 为键的扁平证书映射。
type CertificateMap map[string]*KubeadmCert

// CertTree 返回一层深的证书树，将 CA 证书映射到应由它签名的证书数组。
func (m CertificateMap) CertTree() (CertificateTree, error) {
	caMap := make(CertificateTree)

	for _, cert := range m {
		if cert.CAName == "" {
			if _, ok := caMap[cert]; !ok {
				caMap[cert] = []*KubeadmCert{}
			}
		} else {
			ca, ok := m[cert.CAName]
			if !ok {
				return nil, errors.Errorf("certificate %q references unknown CA %q", cert.Name, cert.CAName)
			}
			caMap[ca] = append(caMap[ca], cert)
		}
	}

	return caMap, nil
}

// Certificates 是 kubeadm 应创建的证书列表。
type Certificates []*KubeadmCert

// AsMap 返回以名称为键的证书映射。
func (c Certificates) AsMap() CertificateMap {
	certMap := make(map[string]*KubeadmCert)
	for _, cert := range c {
		certMap[cert.Name] = cert
	}

	return certMap
}

// GetDefaultCertList 返回 kubeadm 正常运行所需的全部证书。
func GetDefaultCertList() Certificates {
	return Certificates{
		KubeadmCertRootCA(),
		KubeadmCertAPIServer(),
		KubeadmCertKubeletClient(),
		// 前端代理证书
		KubeadmCertFrontProxyCA(),
		KubeadmCertFrontProxyClient(),
		// etcd 证书
		KubeadmCertEtcdCA(),
		KubeadmCertEtcdServer(),
		KubeadmCertEtcdPeer(),
		KubeadmCertEtcdHealthcheck(),
		KubeadmCertEtcdAPIClient(),
	}
}

// GetCertsWithoutEtcd 返回 etcd 由外部托管时 kubeadm 所需的全部证书。
func GetCertsWithoutEtcd() Certificates {
	return Certificates{
		KubeadmCertRootCA(),
		KubeadmCertAPIServer(),
		KubeadmCertKubeletClient(),
		// 前端代理证书
		KubeadmCertFrontProxyCA(),
		KubeadmCertFrontProxyClient(),
	}
}

// KubeadmCertRootCA 定义 API Server 和 kubelet 使用的 Kubernetes 根 CA。
func KubeadmCertRootCA() *KubeadmCert {
	return &KubeadmCert{
		Name:     "ca",
		LongName: "self-signed Kubernetes CA to provision identities for other Kubernetes components",
		BaseName: kubeadmconstants.CACertAndKeyBaseName,
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: "kubernetes",
			},
		},
	}
}

// KubeadmCertAPIServer 定义用于提供 Kubernetes API 服务的证书。
func KubeadmCertAPIServer() *KubeadmCert {
	return &KubeadmCert{
		Name:     "apiserver",
		LongName: "certificate for serving the Kubernetes API",
		BaseName: kubeadmconstants.APIServerCertAndKeyBaseName,
		CAName:   "ca",
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: kubeadmconstants.APIServerCertCommonName,
				Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			},
		},
		configMutators: []configMutatorsFunc{
			makeAltNamesMutator(pkiutil.GetAPIServerAltNames),
		},
	}
}

// KubeadmCertKubeletClient 定义 API Server 访问 kubelet 时使用的证书。
func KubeadmCertKubeletClient() *KubeadmCert {
	return &KubeadmCert{
		Name:     "apiserver-kubelet-client",
		LongName: "certificate for the API server to connect to kubelet",
		BaseName: kubeadmconstants.APIServerKubeletClientCertAndKeyBaseName,
		CAName:   "ca",
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: kubeadmconstants.APIServerKubeletClientCertCommonName,
				Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			},
		},
	}
}

// KubeadmCertFrontProxyCA 定义前端代理使用的 CA。
func KubeadmCertFrontProxyCA() *KubeadmCert {
	return &KubeadmCert{
		Name:     "front-proxy-ca",
		LongName: "self-signed CA to provision identities for front proxy",
		BaseName: kubeadmconstants.FrontProxyCACertAndKeyBaseName,
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: "front-proxy-ca",
			},
		},
	}
}

// KubeadmCertFrontProxyClient 定义 API Server 访问前端代理时使用的证书。
func KubeadmCertFrontProxyClient() *KubeadmCert {
	return &KubeadmCert{
		Name:     "front-proxy-client",
		BaseName: kubeadmconstants.FrontProxyClientCertAndKeyBaseName,
		LongName: "certificate for the front proxy client",
		CAName:   "front-proxy-ca",
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: kubeadmconstants.FrontProxyClientCertCommonName,
				Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			},
		},
	}
}

// KubeadmCertEtcdCA 定义托管 etcd 服务器使用的根 CA。
func KubeadmCertEtcdCA() *KubeadmCert {
	return &KubeadmCert{
		Name:     "etcd-ca",
		LongName: "self-signed CA to provision identities for etcd",
		BaseName: kubeadmconstants.EtcdCACertAndKeyBaseName,
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: "etcd-ca",
			},
		},
	}
}

// KubeadmCertEtcdServer 定义用于向客户端提供 etcd 服务的证书。
func KubeadmCertEtcdServer() *KubeadmCert {
	return &KubeadmCert{
		Name:     "etcd-server",
		LongName: "certificate for serving etcd",
		BaseName: kubeadmconstants.EtcdServerCertAndKeyBaseName,
		CAName:   "etcd-ca",
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				// TODO: etcd 3.2 对服务器证书提出了未记录的 ClientAuth 使用要求：
				// https://github.com/etcd-io/etcd/issues/9785#issuecomment-396715692
				// 上游问题解决后，应恢复为仅允许 ServerAuth。
				Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			},
		},
		configMutators: []configMutatorsFunc{
			makeAltNamesMutator(pkiutil.GetEtcdAltNames),
			setCommonNameToNodeName(),
		},
	}
}

// KubeadmCertEtcdPeer 定义 etcd 对等节点相互访问时使用的证书。
func KubeadmCertEtcdPeer() *KubeadmCert {
	return &KubeadmCert{
		Name:     "etcd-peer",
		LongName: "certificate for etcd nodes to communicate with each other",
		BaseName: kubeadmconstants.EtcdPeerCertAndKeyBaseName,
		CAName:   "etcd-ca",
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			},
		},
		configMutators: []configMutatorsFunc{
			makeAltNamesMutator(pkiutil.GetEtcdPeerAltNames),
			setCommonNameToNodeName(),
		},
	}
}

// KubeadmCertEtcdHealthcheck 定义 Kubernetes 检查 etcd 服务器健康状态时使用的证书。
func KubeadmCertEtcdHealthcheck() *KubeadmCert {
	return &KubeadmCert{
		Name:     "etcd-healthcheck-client",
		LongName: "certificate for liveness probes to healthcheck etcd",
		BaseName: kubeadmconstants.EtcdHealthcheckClientCertAndKeyBaseName,
		CAName:   "etcd-ca",
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: kubeadmconstants.EtcdHealthcheckClientCertCommonName,
				Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			},
		},
	}
}

// KubeadmCertEtcdAPIClient 定义 API Server 访问 etcd 时使用的证书。
func KubeadmCertEtcdAPIClient() *KubeadmCert {
	return &KubeadmCert{
		Name:     "apiserver-etcd-client",
		LongName: "certificate the apiserver uses to access etcd",
		BaseName: kubeadmconstants.APIServerEtcdClientCertAndKeyBaseName,
		CAName:   "etcd-ca",
		config: pkiutil.CertConfig{
			Config: certutil.Config{
				CommonName: kubeadmconstants.APIServerEtcdClientCertCommonName,
				Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			},
		},
	}
}

func makeAltNamesMutator(f func(*kubeadmapi.InitConfiguration) (*certutil.AltNames, error)) configMutatorsFunc {
	return func(mc *kubeadmapi.InitConfiguration, cc *pkiutil.CertConfig) error {
		altNames, err := f(mc)
		if err != nil {
			return err
		}
		cc.AltNames = *altNames
		return nil
	}
}

func setCommonNameToNodeName() configMutatorsFunc {
	return func(mc *kubeadmapi.InitConfiguration, cc *pkiutil.CertConfig) error {
		cc.CommonName = mc.NodeRegistration.Name
		return nil
	}
}

// leafCertificates 从给定证书列表中返回非 CA 证书。
func leafCertificates(c Certificates) (Certificates, error) {
	certTree, err := c.AsMap().CertTree()
	if err != nil {
		return nil, err
	}

	var out Certificates
	for _, leafCertificates := range certTree {
		out = append(out, leafCertificates...)
	}
	return out, nil
}

func createKeyAndCSR(kubeadmConfig *kubeadmapi.InitConfiguration, cert *KubeadmCert) error {
	if kubeadmConfig == nil {
		return errors.Errorf("%s: kubeadmConfig was nil", errInvalid)
	}
	if cert == nil {
		return errors.Errorf("%s: cert was nil", errInvalid)
	}
	certDir := kubeadmConfig.CertificatesDir
	name := cert.BaseName
	if pkiutil.CSROrKeyExist(certDir, name) {
		return errors.Errorf("%s: key or CSR %s/%s", errExist, certDir, name)
	}
	cfg, err := cert.GetConfig(kubeadmConfig)
	if err != nil {
		return err
	}
	csr, key, err := pkiutil.NewCSRAndKey(cfg)
	if err != nil {
		return err
	}
	err = pkiutil.WriteKey(certDir, name, key)
	if err != nil {
		return err
	}
	return pkiutil.WriteCSR(certDir, name, csr)
}

// CreateDefaultKeysAndCSRFiles 用于 ExternalCA 模式，创建私钥文件及同目录的 CSR 文件。
func CreateDefaultKeysAndCSRFiles(out io.Writer, config *kubeadmapi.InitConfiguration) error {
	certificates, err := leafCertificates(GetDefaultCertList())
	if err != nil {
		return err
	}
	if out != nil {
		fmt.Fprintf(out, "generating keys and CSRs in %s\n", config.CertificatesDir)
	}
	for _, cert := range certificates {
		if err := createKeyAndCSR(config, cert); err != nil {
			return err
		}
		if out != nil {
			fmt.Fprintf(out, "  %s\n", cert.BaseName)
		}
	}
	return nil
}
