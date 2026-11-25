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
	"crypto/x509"
	"sort"

	certutil "k8s.io/client-go/util/cert"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	certsphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

// Manager 用于协调证书续签及相关流程，例如生成 CSR 和检查证书过期时间。
type Manager struct {
	// cfg 保存 kubeadm ClusterConfiguration。
	cfg *kubeadmapi.ClusterConfiguration

	// kubernetesDir 保存 kubeConfig 文件所在目录。
	kubernetesDir string

	// certificates 保存此管理器控制的 certificateRenewHandler。
	certificates map[string]*CertificateRenewHandler

	// cas 保存与此管理器控制的证书关联的 CAExpirationHandler。
	cas map[string]*CAExpirationHandler
}

type certConfigMutatorFunc func(*certutil.Config) error

// CertificateRenewHandler 定义续签证书所需的信息。
type CertificateRenewHandler struct {
	// 用于界面展示的证书名称。
	// 可用此值触发针对该证书的操作。
	Name string

	// 用于界面展示的证书长名称。
	LongName string

	// FileName 定义证书文件名（或 BaseName）。
	FileName string

	// CAName 定义此证书所依赖的 CA 名称。
	CAName string

	// CABaseName 定义续签时应使用的 CA 基础名称。
	CABaseName string

	// readwriter 定义用于证书续签的 CertificateReadWriter。
	readwriter certificateReadWriter

	// certConfigMutators 保存可应用于输入证书配置对象的修改函数。
	// 这些函数将按顺序执行。
	certConfigMutators []certConfigMutatorFunc
}

// CAExpirationHandler 定义检查 CA 过期所需的信息。
type CAExpirationHandler struct {
	// 用于界面展示的 CA 名称。
	// 可用此值触发针对该 CA 的操作。
	Name string

	// 用于界面展示的 CA 长名称。
	LongName string

	// FileName 定义 CA 文件名（或 BaseName）。
	FileName string

	// readwriter 定义用于检查 CA 过期的 CertificateReadWriter。
	readwriter certificateReadWriter
}

// NewManager 返回可管理集群证书续签的新管理器。
func NewManager(cfg *kubeadmapi.ClusterConfiguration, kubernetesDir string) (*Manager, error) {
	rm := &Manager{
		cfg:           cfg,
		kubernetesDir: kubernetesDir,
		certificates:  map[string]*CertificateRenewHandler{},
		cas:           map[string]*CAExpirationHandler{},
	}

	// 根据当前集群配置获取预期证书列表
	certListFunc := certsphase.GetDefaultCertList
	if cfg.Etcd.External != nil {
		certListFunc = certsphase.GetCertsWithoutEtcd
	}
	certTree, err := certListFunc().AsMap().CertTree()
	if err != nil {
		return nil, err
	}

	// 为证书树中的每个已签名证书创建 CertificateRenewHandler；
	// 注意：不支持续签 CA，否则会造成严重后果。
	for ca, certs := range certTree {
		for _, cert := range certs {
			// 为 K8s 本地 PKI 中存储的证书创建 ReadWriter
			pkiReadWriter := newPKICertificateReadWriter(rm.cfg.CertificatesDir, cert.BaseName)
			certConfigMutators := loadCertConfigMutators(cert.BaseName)

			// 添加 CertificateRenewHandler。
			// PKI 证书以名称索引，该名称是 certsphase 包中定义的常量，可供整个 kubeadm 代码库复用。
			rm.certificates[cert.Name] = &CertificateRenewHandler{
				Name:               cert.Name,
				LongName:           cert.LongName,
				FileName:           cert.BaseName,
				CAName:             ca.Name,
				CABaseName:         ca.BaseName, // 注意：这是 etcd 证书的路径（它们存储在子目录中）。
				readwriter:         pkiReadWriter,
				certConfigMutators: certConfigMutators,
			}
		}

		pkiReadWriter := newPKICertificateReadWriter(rm.cfg.CertificatesDir, ca.BaseName)
		rm.cas[ca.Name] = &CAExpirationHandler{
			Name:       ca.Name,
			LongName:   ca.LongName,
			FileName:   ca.BaseName,
			readwriter: pkiReadWriter,
		}
	}

	// 获取需要考虑续签的证书列表
	kubeConfigs := []struct {
		longName string
		fileName string
	}{
		{
			longName: "certificate embedded in the kubeconfig file for the admin to use and for kubeadm itself",
			fileName: kubeadmconstants.AdminKubeConfigFileName,
		},
		{
			longName: "certificate embedded in the kubeconfig file for the super-admin",
			fileName: kubeadmconstants.SuperAdminKubeConfigFileName,
		},
		{
			longName: "certificate embedded in the kubeconfig file for the controller manager to use",
			fileName: kubeadmconstants.ControllerManagerKubeConfigFileName,
		},
		{
			longName: "certificate embedded in the kubeconfig file for the scheduler manager to use",
			fileName: kubeadmconstants.SchedulerKubeConfigFileName,
		},
		// 注意：KubeletKubeConfig 不纳入续签，因为其证书由 kubelet 管理。
	}

	// 为每个 kubeConfig 文件创建 CertificateRenewHandler
	for _, kubeConfig := range kubeConfigs {
		// 为嵌入 kubeConfig 文件的证书创建 ReadWriter
		kubeConfigReadWriter := newKubeconfigReadWriter(kubernetesDir, kubeConfig.fileName,
			rm.cfg.CertificatesDir, kubeadmconstants.CACertAndKeyBaseName)

		// 添加 CertificateRenewHandler。
		// 嵌入 kubeConfig 文件的证书以 fileName 索引，该名称是 kubeadm 常量包中定义的常量，可供整个 kubeadm 代码库复用。
		rm.certificates[kubeConfig.fileName] = &CertificateRenewHandler{
			Name:       kubeConfig.fileName, // 使用 fileName 作为名称，因为没有其他同类标识
			LongName:   kubeConfig.longName,
			FileName:   kubeConfig.fileName,
			CABaseName: kubeadmconstants.CACertAndKeyBaseName, // kubeConfig 文件中的所有证书均由 Kubernetes CA 签名
			CAName:     kubeadmconstants.CACertAndKeyBaseName,
			readwriter: kubeConfigReadWriter,
		}
	}

	return rm, nil
}

// Certificates 返回此管理器控制的证书列表。
func (rm *Manager) Certificates() []*CertificateRenewHandler {
	certificates := []*CertificateRenewHandler{}
	for _, h := range rm.certificates {
		certificates = append(certificates, h)
	}

	sort.Slice(certificates, func(i, j int) bool { return certificates[i].Name < certificates[j].Name })

	return certificates
}

// CAs 返回与此管理器控制的证书关联的 CA 列表。
func (rm *Manager) CAs() []*CAExpirationHandler {
	cas := []*CAExpirationHandler{}
	for _, h := range rm.cas {
		cas = append(cas, h)
	}

	sort.Slice(cas, func(i, j int) bool { return cas[i].Name < cas[j].Name })

	return cas
}

// RenewUsingLocalCA 使用本地 CA 生成新证书并执行续签。
// PKI 证书使用 certsphase 包中定义的名称；嵌入 kubeConfig 文件的证书使用 kubeadm 常量包中定义的文件名。
// 如果使用 Certificates 函数返回的 CertificateRenewHandler，handler.Name 已是正确的值。
func (rm *Manager) RenewUsingLocalCA(name string) (bool, error) {
	handler, ok := rm.certificates[name]
	if !ok {
		return false, errors.Errorf("%s is not a valid certificate for this cluster", name)
	}

	// 检查证书是否由外部管理（只提供 CA 证书而不提供私钥）
	externallyManaged, err := rm.IsExternallyManaged(handler.CABaseName)
	if err != nil {
		return false, err
	}

	// 外部 CA 无法续签证书，直接返回
	if externallyManaged {
		return false, nil
	}

	// 读取当前证书
	cert, err := handler.readwriter.Read()
	if err != nil {
		return false, err
	}

	// 提取证书配置
	certConfig := certToConfig(cert)
	for _, f := range handler.certConfigMutators {
		if err := f(&certConfig); err != nil {
			return false, err
		}
	}

	cfg := &pkiutil.CertConfig{
		Config:              certConfig,
		EncryptionAlgorithm: rm.cfg.EncryptionAlgorithm,
	}

	startTime := kubeadmutil.StartTimeUTC()

	// 将证书生效时间提前，以容忍小幅时间回退。
	cfg.NotBefore = startTime.Add(-kubeadmconstants.CertificateBackdate)

	// 使用 ClusterConfiguration 中定义的有效期。
	// 由于不支持 CA 续签，仅使用 CertificateValidityPeriod。
	if rm.cfg.CertificateValidityPeriod != nil {
		cfg.NotAfter = startTime.Add(rm.cfg.CertificateValidityPeriod.Duration)
	}

	// 读取 CA
	caCert, caKey, err := certsphase.LoadCertificateAuthority(rm.cfg.CertificatesDir, handler.CABaseName)
	if err != nil {
		return false, err
	}

	// 使用相同配置创建新证书
	newCert, newKey, err := NewFileRenewer(caCert, caKey).Renew(cfg)
	if err != nil {
		return false, errors.Wrapf(err, "failed to renew certificate %s", name)
	}

	// 将新证书写入磁盘
	err = handler.readwriter.Write(newCert, newKey)
	if err != nil {
		return false, err
	}

	return true, nil
}

// CreateRenewCSR 生成用于证书续签的 CSR。
// PKI 证书使用 certsphase 包中定义的名称；嵌入 kubeConfig 文件的证书使用 kubeadm 常量包中定义的文件名。
// 如果使用 Certificates 函数返回的 CertificateRenewHandler，handler.Name 已是正确的值。
func (rm *Manager) CreateRenewCSR(name, outdir string) error {
	handler, ok := rm.certificates[name]
	if !ok {
		return errors.Errorf("%s is not a known certificate", name)
	}

	// 读取当前证书
	cert, err := handler.readwriter.Read()
	if err != nil {
		return err
	}

	// 提取证书配置
	certConfig := certToConfig(cert)
	for _, f := range handler.certConfigMutators {
		if err := f(&certConfig); err != nil {
			return err
		}
	}
	cfg := &pkiutil.CertConfig{
		Config:              certConfig,
		EncryptionAlgorithm: rm.cfg.EncryptionAlgorithm,
	}

	// 生成并保存 CSR
	csr, key, err := pkiutil.NewCSRAndKey(cfg)
	if err != nil {
		return errors.Wrapf(err, "failure while generating %s CSR and key", name)
	}
	if err := pkiutil.WriteKey(outdir, name, key); err != nil {
		return errors.Wrapf(err, "failure while saving %s key", name)
	}

	if err := pkiutil.WriteCSR(outdir, name, csr); err != nil {
		return errors.Wrapf(err, "failure while saving %s CSR", name)
	}

	return nil
}

// CertificateExists 在证书存在时返回 true。
func (rm *Manager) CertificateExists(name string) (bool, error) {
	handler, ok := rm.certificates[name]
	if !ok {
		return false, errors.Errorf("%s is not a known certificate", name)
	}

	return handler.readwriter.Exists()
}

// GetCertificateExpirationInfo 返回证书过期信息。
// PKI 证书使用 certsphase 包中定义的名称；嵌入 kubeConfig 文件的证书使用 kubeadm 常量包中定义的文件名。
// 如果使用 Certificates 函数返回的 CertificateRenewHandler，handler.Name 已是正确的值。
func (rm *Manager) GetCertificateExpirationInfo(name string) (*ExpirationInfo, error) {
	handler, ok := rm.certificates[name]
	if !ok {
		return nil, errors.Errorf("%s is not a known certificate", name)
	}

	// 检查证书是否由外部管理（只提供 CA 证书而不提供私钥）
	externallyManaged, err := rm.IsExternallyManaged(handler.CABaseName)
	if err != nil {
		return nil, err
	}

	// 读取当前证书
	cert, err := handler.readwriter.Read()
	if err != nil {
		return nil, err
	}

	// 返回证书过期信息
	return newExpirationInfo(name, cert, externallyManaged), nil
}

// CAExists 在 CA 存在时返回 true。
func (rm *Manager) CAExists(name string) (bool, error) {
	handler, ok := rm.cas[name]
	if !ok {
		return false, errors.Errorf("%s is not a known certificate", name)
	}

	return handler.readwriter.Exists()
}

// GetCAExpirationInfo 返回 CA 过期信息。
func (rm *Manager) GetCAExpirationInfo(name string) (*ExpirationInfo, error) {
	handler, ok := rm.cas[name]
	if !ok {
		return nil, errors.Errorf("%s is not a known CA", name)
	}

	// 检查 CA 是否由外部管理（只提供 CA 证书而不提供私钥）
	externallyManaged, err := rm.IsExternallyManaged(handler.FileName)
	if err != nil {
		return nil, err
	}

	// 读取当前 CA
	ca, err := handler.readwriter.Read()
	if err != nil {
		return nil, err
	}

	// 返回 CA 过期信息
	return newExpirationInfo(name, ca, externallyManaged), nil
}

// IsExternallyManaged 检查是否使用外部 CA（只提供 CA 证书而不提供私钥）。
func (rm *Manager) IsExternallyManaged(caBaseName string) (bool, error) {
	switch caBaseName {
	case kubeadmconstants.CACertAndKeyBaseName:
		externallyManaged, err := certsphase.UsingExternalCA(rm.cfg)
		if err != nil {
			return false, errors.Wrapf(err, "Error checking external CA condition for %s certificate authority", caBaseName)
		}
		return externallyManaged, nil
	case kubeadmconstants.FrontProxyCACertAndKeyBaseName:
		externallyManaged, err := certsphase.UsingExternalFrontProxyCA(rm.cfg)
		if err != nil {
			return false, errors.Wrapf(err, "Error checking external CA condition for %s certificate authority", caBaseName)
		}
		return externallyManaged, nil
	case kubeadmconstants.EtcdCACertAndKeyBaseName:
		externallyManaged, err := certsphase.UsingExternalEtcdCA(rm.cfg)
		if err != nil {
			return false, errors.Wrapf(err, "Error checking external CA condition for %s certificate authority", caBaseName)
		}
		return externallyManaged, nil
	default:
		return false, errors.Errorf("unknown certificate authority %s", caBaseName)
	}
}

func certToConfig(cert *x509.Certificate) certutil.Config {
	return certutil.Config{
		CommonName:   cert.Subject.CommonName,
		Organization: cert.Subject.Organization,
		AltNames: certutil.AltNames{
			IPs:      cert.IPAddresses,
			DNSNames: cert.DNSNames,
		},
		Usages: cert.ExtKeyUsage,
	}
}

func loadCertConfigMutators(certBaseName string) []certConfigMutatorFunc {
	return nil
}
