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

package certs

import (
	"crypto"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/keyutil"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

var (
	// certPeriodValidation 记录证书是否已完成有效期校验。
	certPeriodValidationMutex sync.Mutex
	certPeriodValidation      = map[string]struct{}{}
)

// CreatePKIAssets 创建建立控制平面所需的全部 PKI 资产并写入磁盘。
// 如果目标目录中已有 PKI 资产，仅在内容一致时复用，否则返回错误。
func CreatePKIAssets(cfg *kubeadmapi.InitConfiguration) error {
	klog.V(1).Infoln("creating PKI assets")

	// 此结构无法处理多级 CA 层次，目前尚无影响，但将来可能成为问题。

	var certList Certificates

	if cfg.Etcd.Local == nil {
		certList = GetCertsWithoutEtcd()
	} else {
		certList = GetDefaultCertList()
	}

	certTree, err := certList.AsMap().CertTree()
	if err != nil {
		return err
	}

	if err := certTree.CreateTree(cfg); err != nil {
		return errors.Wrap(err, "error creating PKI assets")
	}

	fmt.Printf("[certs] Valid certificates and keys now exist in %q\n", cfg.CertificatesDir)

	// 服务账户不是 x509 证书，因此单独处理
	return CreateServiceAccountKeyAndPublicKeyFiles(cfg.CertificatesDir, cfg.ClusterConfiguration.EncryptionAlgorithm)
}

// CreateServiceAccountKeyAndPublicKeyFiles 创建用于服务账户用户签名的公钥和私钥文件。
// 如果目标目录中已有服务账户密钥文件，仅在内容一致时复用，否则返回错误。
func CreateServiceAccountKeyAndPublicKeyFiles(certsDir string, keyType kubeadmapi.EncryptionAlgorithmType) error {
	klog.V(1).Infoln("creating new public/private key files for signing service account users")
	_, err := keyutil.PrivateKeyFromFile(filepath.Join(certsDir, kubeadmconstants.ServiceAccountPrivateKeyName))
	if err == nil {
		// kubeadm 对现有证书私钥仅做此校验：
		// 如果找到路径相同的私钥文件，就认为文件一致，不再写入新文件。
		fmt.Printf("[certs] Using the existing %q key\n", kubeadmconstants.ServiceAccountKeyBaseName)
		return nil
	} else if !os.IsNotExist(err) {
		return errors.Wrapf(err, "file %s existed but it could not be loaded properly", kubeadmconstants.ServiceAccountPrivateKeyName)
	}

	// 私钥不存在，现在生成
	key, err := pkiutil.NewPrivateKey(keyType)
	if err != nil {
		return err
	}

	// 将 .key 和 .pub 文件写入磁盘
	fmt.Printf("[certs] Generating %q key and public key\n", kubeadmconstants.ServiceAccountKeyBaseName)

	if err := pkiutil.WriteKey(certsDir, kubeadmconstants.ServiceAccountKeyBaseName, key); err != nil {
		return err
	}

	return pkiutil.WritePublicKey(certsDir, kubeadmconstants.ServiceAccountKeyBaseName, key.Public())
}

// CreateCACertAndKeyFiles 生成指定的证书颁发机构并写入磁盘。
// certSpec 应是本包中的变量之一。
func CreateCACertAndKeyFiles(certSpec *KubeadmCert, cfg *kubeadmapi.InitConfiguration) error {
	if certSpec.CAName != "" {
		return errors.Errorf("this function should only be used for CAs, but cert %s has CA %s", certSpec.Name, certSpec.CAName)
	}
	klog.V(1).Infof("creating a new certificate authority for %s", certSpec.Name)

	certConfig, err := certSpec.GetConfig(cfg)
	if err != nil {
		return err
	}

	caCert, caKey, err := pkiutil.NewCertificateAuthority(certConfig)
	if err != nil {
		return err
	}

	return writeCertificateAuthorityFilesIfNotExist(
		cfg.CertificatesDir,
		certSpec.BaseName,
		caCert,
		caKey,
	)
}

// CreateCertAndKeyFilesWithCA 从磁盘加载指定的 CA，然后生成并写入指定的证书和私钥。
// certSpec 和 caCertSpec 都应为本包中的变量。
func CreateCertAndKeyFilesWithCA(certSpec *KubeadmCert, caCertSpec *KubeadmCert, cfg *kubeadmapi.InitConfiguration) error {
	if certSpec.CAName != caCertSpec.Name {
		return errors.Errorf("expected CAname for %s to be %q, but was %s", certSpec.Name, certSpec.CAName, caCertSpec.Name)
	}

	caCert, caKey, err := LoadCertificateAuthority(cfg.CertificatesDir, caCertSpec.BaseName)
	if err != nil {
		return errors.Wrapf(err, "couldn't load CA certificate %s", caCertSpec.Name)
	}

	return certSpec.CreateFromCA(cfg, caCert, caKey)
}

// LoadCertificateAuthority 尝试从给定目录加载指定名称的 CA。
func LoadCertificateAuthority(pkiDir string, baseName string) (*x509.Certificate, crypto.Signer, error) {
	// 检查 PKI 目录中是否存在证书颁发机构
	if !pkiutil.CertOrKeyExist(pkiDir, baseName) {
		return nil, nil, errors.Errorf("couldn't load %s certificate authority from %s", baseName, pkiDir)
	}

	// 尝试从 PKI 目录加载 CA 的 .crt 和 .key 文件
	caCert, caKey, err := pkiutil.TryLoadCertAndKeyFromDisk(pkiDir, baseName)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failure loading %s certificate authority", baseName)
	}
	// 校验有效期
	CheckCertificatePeriodValidity(baseName, caCert)

	// 确保加载的 CA 证书确实是 CA 证书
	if !caCert.IsCA {
		return nil, nil, errors.Errorf("%s certificate is not a certificate authority", baseName)
	}

	return caCert, caKey, nil
}

// writeCertificateAuthorityFilesIfNotExist 将新的 CA 写入给定路径。
// 如果路径已有证书文件，kubeadm 会加载并比较现有证书与预期证书。
// 内容一致时跳过写入，否则返回错误。
func writeCertificateAuthorityFilesIfNotExist(pkiDir string, baseName string, caCert *x509.Certificate, caKey crypto.Signer) error {

	// 如果证书或私钥已存在，尝试加载
	if pkiutil.CertOrKeyExist(pkiDir, baseName) {

		// 尝试从 PKI 目录加载 .crt 和 .key 文件
		caCert, _, err := pkiutil.TryLoadCertAndKeyFromDisk(pkiDir, baseName)
		if err != nil {
			return errors.Wrapf(err, "failure loading %s certificate", baseName)
		}
		// 校验有效期
		CheckCertificatePeriodValidity(baseName, caCert)

		// 检查现有证书是否为 CA 证书
		if !caCert.IsCA {
			return errors.Errorf("certificate %s is not a CA", baseName)
		}

		// kubeadm 对现有 CA 仅做此校验：
		// 如果找到路径相同且确为 CA 的证书文件，就认为文件一致，不再写入新文件。
		fmt.Printf("[certs] Using the existing %q certificate and key\n", baseName)
	} else {
		// 将 .crt 和 .key 文件写入磁盘
		fmt.Printf("[certs] Generating %q certificate and key\n", baseName)

		if err := pkiutil.WriteCertAndKey(pkiDir, baseName, caCert, caKey); err != nil {
			return errors.Wrapf(err, "failure while saving %s certificate and key", baseName)
		}
	}
	return nil
}

// writeCertificateFilesIfNotExist 将新证书写入给定路径。
// 如果路径已有证书文件，kubeadm 会加载并比较现有证书与预期证书。
// 内容一致时跳过写入，否则返回错误。
func writeCertificateFilesIfNotExist(pkiDir string, baseName string, signingCert *x509.Certificate, cert *x509.Certificate, key crypto.Signer, cfg *pkiutil.CertConfig) error {

	// 检查 PKI 目录中是否存在已签名证书
	if pkiutil.CertOrKeyExist(pkiDir, baseName) {
		// 尝试从 PKI 目录加载私钥
		_, err := pkiutil.TryLoadKeyFromDisk(pkiDir, baseName)
		if err != nil {
			return errors.Wrapf(err, "failure loading %s key", baseName)
		}

		// 尝试从 PKI 目录加载证书
		signedCert, intermediates, err := pkiutil.TryLoadCertChainFromDisk(pkiDir, baseName)
		if err != nil {
			return errors.Wrapf(err, "failure loading %s certificate", baseName)
		}
		// 校验有效期
		CheckCertificatePeriodValidity(baseName, signedCert)

		// 检查现有证书是否由给定 CA 签名
		if err := pkiutil.VerifyCertChain(signedCert, intermediates, signingCert); err != nil {
			return errors.Errorf("certificate %s is not signed by corresponding CA", baseName)
		}

		// 检查证书属性是否正确
		if err := validateCertificateWithConfig(signedCert, baseName, cfg); err != nil {
			return err
		}

		fmt.Printf("[certs] Using the existing %q certificate and key\n", baseName)
	} else {
		// 将 .crt 和 .key 文件写入磁盘
		fmt.Printf("[certs] Generating %q certificate and key\n", baseName)

		if err := pkiutil.WriteCertAndKey(pkiDir, baseName, cert, key); err != nil {
			return errors.Wrapf(err, "failure while saving %s certificate and key", baseName)
		}
		if pkiutil.HasServerAuth(cert) {
			fmt.Printf("[certs] %s serving cert is signed for DNS names %v and IPs %v\n", baseName, cert.DNSNames, cert.IPAddresses)
		}
	}

	return nil
}

type certKeyLocation struct {
	pkiDir     string
	caBaseName string
	baseName   string
	uxName     string
}

// SharedCertificateExists 检查共享证书是否存在且有效。各控制平面节点上的证书必须一致：
// ca.key、ca.crt、sa.key、sa.pub、front-proxy-ca.key、front-proxy-ca.crt；本地或堆叠 etcd 还需 etcd/ca.key、etcd/ca.crt。
// 缺少 CA 私钥不会致命，但会产生警告。
func SharedCertificateExists(cfg *kubeadmapi.ClusterConfiguration) (bool, error) {
	var errs []error
	if err := validateCACertAndKey(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName, "", "CA"}); err != nil {
		errs = append(errs, err)
	}

	if err := validatePrivatePublicKey(certKeyLocation{cfg.CertificatesDir, "", kubeadmconstants.ServiceAccountKeyBaseName, "service account"}); err != nil {
		errs = append(errs, err)
	}

	if err := validateCACertAndKey(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.FrontProxyCACertAndKeyBaseName, "", "front-proxy CA"}); err != nil {
		errs = append(errs, err)
	}

	// 适用于本地或堆叠 etcd
	if cfg.Etcd.External == nil {
		if err := validateCACertAndKey(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.EtcdCACertAndKeyBaseName, "", "etcd CA"}); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return false, utilerrors.NewAggregate(errs)
	}
	return true, nil
}

// UsingExternalCA 判断用户是否使用外部 CA。当前仅凭 CA 证书存在而 CA 私钥不存在来推断。
// 据此可跳过证书生成或无需启动 CSR 签名控制器。
// 如果使用外部 front-proxy CA，本函数还会校验用户应提供的、由该 CA 签名的证书。
func UsingExternalCA(cfg *kubeadmapi.ClusterConfiguration) (bool, error) {

	if err := validateCACert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName, "", "CA"}); err != nil {
		return false, err
	}

	caKeyPath := filepath.Join(cfg.CertificatesDir, kubeadmconstants.CAKeyName)
	if _, err := os.Stat(caKeyPath); !os.IsNotExist(err) {
		return false, nil
	}

	if err := validateSignedCert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName, kubeadmconstants.APIServerCertAndKeyBaseName, "API server"}); err != nil {
		return true, err
	}

	if err := validateSignedCert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName, kubeadmconstants.APIServerKubeletClientCertAndKeyBaseName, "API server kubelet client"}); err != nil {
		return true, err
	}

	return true, nil
}

// UsingExternalFrontProxyCA 判断用户是否使用外部 front-proxy CA。当前仅凭其证书存在而私钥不存在来推断。
// 如果使用外部 front-proxy CA，本函数还会校验用户应提供的、由该 CA 签名的证书。
func UsingExternalFrontProxyCA(cfg *kubeadmapi.ClusterConfiguration) (bool, error) {

	if err := validateCACert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.FrontProxyCACertAndKeyBaseName, "", "front-proxy CA"}); err != nil {
		return false, err
	}

	frontProxyCAKeyPath := filepath.Join(cfg.CertificatesDir, kubeadmconstants.FrontProxyCAKeyName)
	if _, err := os.Stat(frontProxyCAKeyPath); !os.IsNotExist(err) {
		return false, nil
	}

	if err := validateSignedCert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.FrontProxyCACertAndKeyBaseName, kubeadmconstants.FrontProxyClientCertAndKeyBaseName, "front-proxy client"}); err != nil {
		return true, err
	}

	return true, nil
}

// UsingExternalEtcdCA 判断用户是否使用外部 etcd CA。当前仅凭其证书存在而私钥不存在来推断。
// 如果使用外部 etcd CA，本函数还会校验用户应提供的、由该 CA 签名的证书。
func UsingExternalEtcdCA(cfg *kubeadmapi.ClusterConfiguration) (bool, error) {
	if err := validateCACert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.EtcdCACertAndKeyBaseName, "", "etcd CA"}); err != nil {
		return false, err
	}

	path := filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdCAKeyName)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return false, nil
	}

	if err := validateSignedCert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.EtcdCACertAndKeyBaseName, kubeadmconstants.APIServerEtcdClientCertAndKeyBaseName, "apiserver etcd client"}); err != nil {
		return true, err
	}

	if err := validateSignedCert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.EtcdCACertAndKeyBaseName, kubeadmconstants.EtcdServerCertAndKeyBaseName, "etcd server"}); err != nil {
		return true, err
	}

	if err := validateSignedCert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.EtcdCACertAndKeyBaseName, kubeadmconstants.EtcdPeerCertAndKeyBaseName, "etcd peer"}); err != nil {
		return true, err
	}

	if err := validateSignedCert(certKeyLocation{cfg.CertificatesDir, kubeadmconstants.EtcdCACertAndKeyBaseName, kubeadmconstants.EtcdHealthcheckClientCertAndKeyBaseName, "etcd health-check client"}); err != nil {
		return true, err
	}

	return true, nil
}

// validateCACert 尝试从 pkiDir 加载 x509 证书，并校验其是否为 CA
func validateCACert(l certKeyLocation) error {
	// 检查 CA 证书
	caCert, err := pkiutil.TryLoadCertFromDisk(l.pkiDir, l.caBaseName)
	if err != nil {
		return errors.Wrapf(err, "failure loading certificate for %s", l.uxName)
	}
	// 校验有效期
	CheckCertificatePeriodValidity(l.uxName, caCert)

	// 检查证书是否为 CA
	if !caCert.IsCA {
		return errors.Errorf("certificate %s is not a CA", l.uxName)
	}
	return nil
}

// validateCACertAndKey 尝试从 pkiDir 加载 x509 证书和私钥，并校验证书是否为 CA。
// 加载私钥失败时会产生警告。
func validateCACertAndKey(l certKeyLocation) error {
	if err := validateCACert(l); err != nil {
		return err
	}

	_, err := pkiutil.TryLoadKeyFromDisk(l.pkiDir, l.caBaseName)
	if err != nil {
		klog.Warningf("assuming external key for %s: %v", l.uxName, err)
	}
	return nil
}

// validateSignedCert 尝试从 pkiDir 加载 x509 证书和私钥，并校验证书是否由给定 CA 签名。
func validateSignedCert(l certKeyLocation) error {
	// 尝试加载 CA
	caCert, err := pkiutil.TryLoadCertFromDisk(l.pkiDir, l.caBaseName)
	if err != nil {
		return errors.Wrapf(err, "failure loading certificate authority for %s", l.uxName)
	}
	// 校验有效期
	CheckCertificatePeriodValidity(l.uxName, caCert)

	return validateSignedCertWithCA(l, caCert)
}

// validateSignedCertWithCA 尝试加载证书和私钥，并校验证书是否由给定 caCert 签名。
func validateSignedCertWithCA(l certKeyLocation, caCert *x509.Certificate) error {
	// 尝试从 PKI 目录加载私钥
	_, err := pkiutil.TryLoadKeyFromDisk(l.pkiDir, l.baseName)
	if err != nil {
		return errors.Wrapf(err, "failure loading key for %s", l.baseName)
	}

	// 尝试从 PKI 目录加载证书
	signedCert, intermediates, err := pkiutil.TryLoadCertChainFromDisk(l.pkiDir, l.baseName)
	if err != nil {
		return errors.Wrapf(err, "failure loading certificate for %s", l.uxName)
	}
	// 校验有效期
	CheckCertificatePeriodValidity(l.uxName, signedCert)

	// 检查证书是否由该 CA 签名
	if err := pkiutil.VerifyCertChain(signedCert, intermediates, caCert); err != nil {
		return errors.Wrapf(err, "certificate %s is not signed by corresponding CA", l.uxName)
	}
	return nil
}

// validatePrivatePublicKey 尝试从 pkiDir 加载私钥
func validatePrivatePublicKey(l certKeyLocation) error {
	// 尝试加载私钥
	_, _, err := pkiutil.TryLoadPrivatePublicKeyFromDisk(l.pkiDir, l.baseName)
	return errors.Wrapf(err, "failure loading key for %s", l.uxName)
}

// validateCertificateWithConfig 确保证书至少对配置中定义的 SAN 有效。
func validateCertificateWithConfig(cert *x509.Certificate, baseName string, cfg *pkiutil.CertConfig) error {
	for _, dnsName := range cfg.AltNames.DNSNames {
		if err := cert.VerifyHostname(dnsName); err != nil {
			return errors.Wrapf(err, "certificate %s is invalid", baseName)
		}
	}
	for _, ipAddress := range cfg.AltNames.IPs {
		if err := cert.VerifyHostname(ipAddress.String()); err != nil {
			return errors.Wrapf(err, "certificate %s is invalid", baseName)
		}
	}
	return nil
}

// CheckCertificatePeriodValidity 检查证书有效期是否覆盖当前时间，并在无效时输出警告。
// 通过缓存记录已校验的证书，避免重复校验。
func CheckCertificatePeriodValidity(baseName string, cert *x509.Certificate) {
	certPeriodValidationMutex.Lock()
	defer certPeriodValidationMutex.Unlock()
	if _, exists := certPeriodValidation[baseName]; exists {
		return
	}
	certPeriodValidation[baseName] = struct{}{}

	klog.V(5).Infof("validating certificate period for %s certificate", baseName)
	if err := pkiutil.ValidateCertPeriod(cert, 0); err != nil {
		klog.Warningf("WARNING: could not validate bounds for certificate %s: %v", baseName, err)
	}
}
