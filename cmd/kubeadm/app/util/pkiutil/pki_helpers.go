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

package pkiutil

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

const (
	// PublicKeyBlockType 是 pem.Block.Type 的一个可能取值。
	PublicKeyBlockType = "PUBLIC KEY"
	// CertificateBlockType 是 pem.Block.Type 的一个可能取值。
	CertificateBlockType = "CERTIFICATE"
)

// CertConfig 包装 certutil.Config，并扩展 EncryptionAlgorithm 字段。
type CertConfig struct {
	certutil.Config
	NotAfter            time.Time
	EncryptionAlgorithm kubeadmapi.EncryptionAlgorithmType
}

// NewCertificateAuthority 创建新的 CA 证书和私钥。
func NewCertificateAuthority(config *CertConfig) (*x509.Certificate, crypto.Signer, error) {
	key, err := NewPrivateKey(config.EncryptionAlgorithm)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create private key while generating CA certificate")
	}
	cert, err := NewSelfSignedCACert(config, key)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create self-signed CA certificate")
	}

	return cert, key, nil
}

// NewIntermediateCertificateAuthority 创建新的中间 CA 证书和私钥。
func NewIntermediateCertificateAuthority(parentCert *x509.Certificate, parentKey crypto.Signer, config *CertConfig) (*x509.Certificate, crypto.Signer, error) {
	key, err := NewPrivateKey(config.EncryptionAlgorithm)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create private key while generating intermediate CA certificate")
	}

	cert, err := NewSignedCert(config, key, parentCert, parentKey, true)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to sign intermediate CA certificate")
	}

	return cert, key, nil
}

// NewCertAndKey 使用 CA 证书和私钥创建新证书和密钥。
func NewCertAndKey(caCert *x509.Certificate, caKey crypto.Signer, config *CertConfig) (*x509.Certificate, crypto.Signer, error) {
	if len(config.Usages) == 0 {
		return nil, nil, errors.New("must specify at least one ExtKeyUsage")
	}

	key, err := NewPrivateKey(config.EncryptionAlgorithm)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create private key")
	}

	cert, err := NewSignedCert(config, key, caCert, caKey, false)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to sign certificate")
	}

	return cert, key, nil
}

// NewCSRAndKey 生成可签署为指定证书的新密钥和 CSR。
func NewCSRAndKey(config *CertConfig) (*x509.CertificateRequest, crypto.Signer, error) {
	key, err := NewPrivateKey(config.EncryptionAlgorithm)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create private key")
	}

	csr, err := NewCSR(*config, key)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to generate CSR")
	}

	return csr, key, nil
}

// HasServerAuth 判断给定证书是否用于 ServerAuth。
func HasServerAuth(cert *x509.Certificate) bool {
	return slices.Contains(cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
}

// WriteCertAndKey 将证书和密钥保存到指定位置。
func WriteCertAndKey(pkiPath string, name string, cert *x509.Certificate, key crypto.Signer) error {
	if err := WriteKey(pkiPath, name, key); err != nil {
		return errors.Wrap(err, "couldn't write key")
	}

	return WriteCert(pkiPath, name, cert)
}

// WriteCert 将给定证书保存到指定位置。
func WriteCert(pkiPath, name string, cert *x509.Certificate) error {
	if cert == nil {
		return errors.New("certificate cannot be nil when writing to file")
	}

	certificatePath := pathForCert(pkiPath, name)
	if err := certutil.WriteCert(certificatePath, EncodeCertPEM(cert)); err != nil {
		return errors.Wrapf(err, "unable to write certificate to file %s", certificatePath)
	}

	return nil
}

// WriteCertBundle 将给定证书链保存到指定位置。
func WriteCertBundle(pkiPath, name string, certs []*x509.Certificate) error {
	for i, cert := range certs {
		if cert == nil {
			return errors.Errorf("found nil certificate at position %d when writing bundle to file", i)
		}
	}

	certificatePath := pathForCert(pkiPath, name)
	encoded, err := EncodeCertBundlePEM(certs)
	if err != nil {
		return errors.Wrapf(err, "unable to marshal certificate bundle to PEM")
	}
	if err := certutil.WriteCert(certificatePath, encoded); err != nil {
		return errors.Wrapf(err, "unable to write certificate bundle to file %s", certificatePath)
	}

	return nil
}

// WriteKey 将给定密钥保存到指定位置。
func WriteKey(pkiPath, name string, key crypto.Signer) error {
	if key == nil {
		return errors.New("private key cannot be nil when writing to file")
	}

	privateKeyPath := pathForKey(pkiPath, name)
	encoded, err := keyutil.MarshalPrivateKeyToPEM(key)
	if err != nil {
		return errors.Wrapf(err, "unable to marshal private key to PEM")
	}
	if err := keyutil.WriteKey(privateKeyPath, encoded); err != nil {
		return errors.Wrapf(err, "unable to write private key to file %s", privateKeyPath)
	}

	return nil
}

// WriteCSR 将 PEM 编码的 CSR 写入 csrPath。文件权限为 0600，已存在则覆盖；
// 必要时创建权限为 0700 的父目录。
func WriteCSR(csrDir, name string, csr *x509.CertificateRequest) error {
	if csr == nil {
		return errors.New("certificate request cannot be nil when writing to file")
	}

	csrPath := pathForCSR(csrDir, name)
	if err := os.MkdirAll(filepath.Dir(csrPath), os.FileMode(0700)); err != nil {
		return errors.Wrapf(err, "failed to make directory %s", filepath.Dir(csrPath))
	}

	if err := os.WriteFile(csrPath, EncodeCSRPEM(csr), os.FileMode(0600)); err != nil {
		return errors.Wrapf(err, "unable to write CSR to file %s", csrPath)
	}

	return nil
}

// WritePublicKey 将给定公钥保存到指定位置。
func WritePublicKey(pkiPath, name string, key crypto.PublicKey) error {
	if key == nil {
		return errors.New("public key cannot be nil when writing to file")
	}

	publicKeyBytes, err := EncodePublicKeyPEM(key)
	if err != nil {
		return err
	}
	publicKeyPath := pathForPublicKey(pkiPath, name)
	if err := keyutil.WriteKey(publicKeyPath, publicKeyBytes); err != nil {
		return errors.Wrapf(err, "unable to write public key to file %s", publicKeyPath)
	}

	return nil
}

// CertOrKeyExist 判断证书或密钥是否存在。
func CertOrKeyExist(pkiPath, name string) bool {
	certificatePath, privateKeyPath := PathsForCertAndKey(pkiPath, name)

	_, certErr := os.Stat(certificatePath)
	_, keyErr := os.Stat(privateKeyPath)

	return !(os.IsNotExist(certErr) && os.IsNotExist(keyErr))
}

// CSROrKeyExist 判断 CSR 或密钥是否存在。
func CSROrKeyExist(csrDir, name string) bool {
	csrPath := pathForCSR(csrDir, name)
	keyPath := pathForKey(csrDir, name)

	_, csrErr := os.Stat(csrPath)
	_, keyErr := os.Stat(keyPath)

	return !(os.IsNotExist(csrErr) && os.IsNotExist(keyErr))
}

// TryLoadCertAndKeyFromDisk 尝试从磁盘加载证书和密钥，并验证其有效性。
func TryLoadCertAndKeyFromDisk(pkiPath, name string) (*x509.Certificate, crypto.Signer, error) {
	cert, err := TryLoadCertFromDisk(pkiPath, name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to load certificate")
	}

	key, err := TryLoadKeyFromDisk(pkiPath, name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to load key")
	}

	return cert, key, nil
}

// TryLoadCertFromDisk 尝试从磁盘加载证书。
func TryLoadCertFromDisk(pkiPath, name string) (*x509.Certificate, error) {
	certificatePath := pathForCert(pkiPath, name)

	certs, err := certutil.CertsFromFile(certificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't load the certificate file %s", certificatePath)
	}

	// 发送方证书必须位于列表首位，因此可安全取第一项。
	// 详见：https://www.rfc-editor.org/rfc/rfc4346#section-7.4.2
	cert := certs[0]

	return cert, nil
}

// TryLoadCertChainFromDisk 尝试从磁盘加载证书链。
func TryLoadCertChainFromDisk(pkiPath, name string) (*x509.Certificate, []*x509.Certificate, error) {
	certificatePath := pathForCert(pkiPath, name)

	certs, err := certutil.CertsFromFile(certificatePath)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't load the certificate file %s", certificatePath)
	}

	cert := certs[0]
	intermediates := certs[1:]

	return cert, intermediates, nil
}

// TryLoadKeyFromDisk 尝试从磁盘加载密钥，并验证其有效性。
func TryLoadKeyFromDisk(pkiPath, name string) (crypto.Signer, error) {
	privateKeyPath := pathForKey(pkiPath, name)

	// 从文件解析私钥。
	privKey, err := keyutil.PrivateKeyFromFile(privateKeyPath)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't load the private key file %s", privateKeyPath)
	}

	// 仅允许 RSA 和 ECDSA 格式。
	var key crypto.Signer
	switch k := privKey.(type) {
	case *rsa.PrivateKey:
		key = k
	case *ecdsa.PrivateKey:
		key = k
	default:
		return nil, errors.Errorf("the private key file %s is neither in RSA nor ECDSA format", privateKeyPath)
	}

	return key, nil
}

// TryLoadPrivatePublicKeyFromDisk 尝试从磁盘加载密钥，并验证其有效性。
func TryLoadPrivatePublicKeyFromDisk(pkiPath, name string) (crypto.PrivateKey, crypto.PublicKey, error) {
	privateKeyPath := pathForKey(pkiPath, name)

	// 从文件解析私钥。
	privKey, err := keyutil.PrivateKeyFromFile(privateKeyPath)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't load the private key file %s", privateKeyPath)
	}

	publicKeyPath := pathForPublicKey(pkiPath, name)

	// 从文件解析公钥。
	pubKeys, err := keyutil.PublicKeysFromFile(publicKeyPath)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't load the public key file %s", publicKeyPath)
	}

	// 仅允许 RSA 和 ECDSA 格式。
	mismatchErrFmt := "the private key file %[2]s is in %[1]s format but the public key file %[3]s is not in %[1]s format"
	switch k := privKey.(type) {
	case *rsa.PrivateKey:
		pubKey, ok := pubKeys[0].(*rsa.PublicKey)
		if !ok {
			return nil, nil, errors.Errorf(mismatchErrFmt, "RSA", privateKeyPath, publicKeyPath)
		}
		return k, pubKey, nil
	case *ecdsa.PrivateKey:
		pubKey, ok := pubKeys[0].(*ecdsa.PublicKey)
		if !ok {
			return nil, nil, errors.Errorf(mismatchErrFmt, "ECDSA", privateKeyPath, publicKeyPath)
		}
		return k, pubKey, nil
	default:
		return nil, nil, errors.Errorf("the private key file %s is neither in RSA nor ECDSA format", privateKeyPath)
	}
}

// PathsForCertAndKey 根据路径和文件名返回证书及密钥的路径。
func PathsForCertAndKey(pkiPath, name string) (string, string) {
	return pathForCert(pkiPath, name), pathForKey(pkiPath, name)
}

func pathForCert(pkiPath, name string) string {
	return filepath.Join(pkiPath, fmt.Sprintf("%s.crt", name))
}

func pathForKey(pkiPath, name string) string {
	return filepath.Join(pkiPath, fmt.Sprintf("%s.key", name))
}

func pathForPublicKey(pkiPath, name string) string {
	return filepath.Join(pkiPath, fmt.Sprintf("%s.pub", name))
}

func pathForCSR(pkiPath, name string) string {
	return filepath.Join(pkiPath, fmt.Sprintf("%s.csr", name))
}

// GetAPIServerAltNames 构建生成 API Server 证书所用的 AltNames。
func GetAPIServerAltNames(cfg *kubeadmapi.InitConfiguration) (*certutil.AltNames, error) {
	// 公告地址。
	advertiseAddress := netutils.ParseIPSloppy(cfg.LocalAPIEndpoint.AdvertiseAddress)
	if advertiseAddress == nil {
		return nil, errors.Errorf("error parsing LocalAPIEndpoint AdvertiseAddress %v: is not a valid textual representation of an IP address",
			cfg.LocalAPIEndpoint.AdvertiseAddress)
	}

	internalAPIServerVirtualIP, err := kubeadmconstants.GetAPIServerVirtualIP(cfg.Networking.ServiceSubnet)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get first IP address from the given CIDR: %v", cfg.Networking.ServiceSubnet)
	}

	var dnsNames []string
	if len(cfg.NodeRegistration.Name) > 0 {
		dnsNames = append(dnsNames, cfg.NodeRegistration.Name)
	}
	dnsNames = append(dnsNames, "kubernetes", "kubernetes.default", "kubernetes.default.svc")
	if len(cfg.Networking.DNSDomain) > 0 {
		dnsNames = append(dnsNames, fmt.Sprintf("kubernetes.default.svc.%s", cfg.Networking.DNSDomain))
	}

	// 使用默认 DNSNames/IP 创建 AltNames。
	altNames := &certutil.AltNames{
		DNSNames: dnsNames,
		IPs: []net.IP{
			internalAPIServerVirtualIP,
			advertiseAddress,
		},
	}

	// 若存在集群 controlPlaneEndpoint（DNS 或 IP），将其加入。
	if len(cfg.ControlPlaneEndpoint) > 0 {
		if host, _, err := kubeadmutil.ParseHostPort(cfg.ControlPlaneEndpoint); err == nil {
			if ip := netutils.ParseIPSloppy(host); ip != nil {
				altNames.IPs = append(altNames.IPs, ip)
			} else {
				altNames.DNSNames = append(altNames.DNSNames, host)
			}
		} else {
			return nil, errors.Wrapf(err, "error parsing cluster controlPlaneEndpoint %q", cfg.ControlPlaneEndpoint)
		}
	}

	appendSANsToAltNames(altNames, cfg.APIServer.CertSANs, kubeadmconstants.APIServerCertName)

	return altNames, nil
}

// GetEtcdAltNames 构建生成 etcd 服务端证书所用的 AltNames。
// etcd 静态 Pod 监听公告地址和 localhost，因此 SAN 包含二者。
// 用户可通过 Etcd.ExtraArgs 覆盖监听地址，通过 Etcd.ServerCertSANs 添加 SAN。
func GetEtcdAltNames(cfg *kubeadmapi.InitConfiguration) (*certutil.AltNames, error) {
	return getAltNames(cfg, kubeadmconstants.EtcdServerCertName)
}

// GetEtcdPeerAltNames 构建生成 etcd peer 证书所用的 AltNames。
// 用户若将单节点 etcd 集群提升为多节点（堆叠 etcd），SAN 包含主机名和 API.AdvertiseAddress。
// 用户可通过 Etcd.ExtraArgs 覆盖监听地址，通过 Etcd.PeerCertSANs 添加 SAN。
func GetEtcdPeerAltNames(cfg *kubeadmapi.InitConfiguration) (*certutil.AltNames, error) {
	return getAltNames(cfg, kubeadmconstants.EtcdPeerCertName)
}

// getAltNames 根据 cfg 和 certName 构建 AltNames。
func getAltNames(cfg *kubeadmapi.InitConfiguration, certName string) (*certutil.AltNames, error) {
	// 公告地址。
	advertiseAddress := netutils.ParseIPSloppy(cfg.LocalAPIEndpoint.AdvertiseAddress)
	if advertiseAddress == nil {
		return nil, errors.Errorf("error parsing LocalAPIEndpoint AdvertiseAddress %v: is not a valid textual representation of an IP address",
			cfg.LocalAPIEndpoint.AdvertiseAddress)
	}

	var dnsNames []string
	if len(cfg.NodeRegistration.Name) > 0 {
		dnsNames = []string{cfg.NodeRegistration.Name, "localhost"}
	} else {
		dnsNames = []string{"localhost"}
	}

	// 使用默认 DNSNames/IP 创建 AltNames。
	altNames := &certutil.AltNames{
		DNSNames: dnsNames,
		IPs:      []net.IP{advertiseAddress, net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	if cfg.Etcd.Local != nil {
		if certName == kubeadmconstants.EtcdServerCertName {
			appendSANsToAltNames(altNames, cfg.Etcd.Local.ServerCertSANs, kubeadmconstants.EtcdServerCertName)
		} else if certName == kubeadmconstants.EtcdPeerCertName {
			appendSANsToAltNames(altNames, cfg.Etcd.Local.PeerCertSANs, kubeadmconstants.EtcdPeerCertName)
		}
	}
	return altNames, nil
}

// appendSANsToAltNames 解析字符串列表中的 SAN，并加入特定证书的 altNames。
// altNames 以指针传入并原地修改；有效 IP 加入 altNames.IPs，
// 符合 RFC-1123 的 DNS 名和通配 DNS 名加入 altNames.DNSNames。
// certNames 用于面向用户的警告，应为 altNames 对应证书的名称。
func appendSANsToAltNames(altNames *certutil.AltNames, SANs []string, certName string) {
	for _, altname := range SANs {
		if ip := netutils.ParseIPSloppy(altname); ip != nil {
			altNames.IPs = append(altNames.IPs, ip)
		} else if len(validation.IsDNS1123Subdomain(altname)) == 0 {
			altNames.DNSNames = append(altNames.DNSNames, altname)
		} else if len(validation.IsWildcardDNS1123Subdomain(altname)) == 0 {
			altNames.DNSNames = append(altNames.DNSNames, altname)
		} else {
			klog.Warningf(
				"[certificates] WARNING: '%s' was not added to the '%s' SAN, because it is not a valid IP or RFC-1123 compliant DNS entry\n",
				altname,
				certName,
			)
		}
	}
}

// EncodeCSRPEM 返回 PEM 编码的 CSR。
func EncodeCSRPEM(csr *x509.CertificateRequest) []byte {
	block := pem.Block{
		Type:  certutil.CertificateRequestBlockType,
		Bytes: csr.Raw,
	}
	return pem.EncodeToMemory(&block)
}

// NewCSR 创建新 CSR。
func NewCSR(cfg CertConfig, key crypto.Signer) (*x509.CertificateRequest, error) {
	RemoveDuplicateAltNames(&cfg.AltNames)

	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   cfg.CommonName,
			Organization: cfg.Organization,
		},
		DNSNames:    cfg.AltNames.DNSNames,
		IPAddresses: cfg.AltNames.IPs,
	}

	csrBytes, err := x509.CreateCertificateRequest(cryptorand.Reader, template, key)

	if err != nil {
		return nil, errors.Wrap(err, "failed to create a CSR")
	}

	return x509.ParseCertificateRequest(csrBytes)
}

// EncodeCertPEM 返回 PEM 编码的证书。
func EncodeCertPEM(cert *x509.Certificate) []byte {
	block := pem.Block{
		Type:  CertificateBlockType,
		Bytes: cert.Raw,
	}
	return pem.EncodeToMemory(&block)
}

// EncodeCertBundlePEM 返回 PEM 编码的证书链。
func EncodeCertBundlePEM(certs []*x509.Certificate) ([]byte, error) {
	buf := bytes.Buffer{}

	block := pem.Block{
		Type: CertificateBlockType,
	}

	for _, cert := range certs {
		block.Bytes = cert.Raw
		if err := pem.Encode(&buf, &block); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// EncodePublicKeyPEM 返回 PEM 编码的公钥。
func EncodePublicKeyPEM(key crypto.PublicKey) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return []byte{}, err
	}
	block := pem.Block{
		Type:  PublicKeyBlockType,
		Bytes: der,
	}
	return pem.EncodeToMemory(&block), nil
}

// NewPrivateKey 返回新私钥。
var NewPrivateKey = GeneratePrivateKey

// rsaKeySizeFromAlgorithmType 返回 kubeadm API 中已知 RSA 算法的密钥长度。
// 未知类型返回 0，空类型返回默认值 2048。
func rsaKeySizeFromAlgorithmType(keyType kubeadmapi.EncryptionAlgorithmType) int {
	switch keyType {
	case kubeadmapi.EncryptionAlgorithmRSA2048, "":
		return 2048
	case kubeadmapi.EncryptionAlgorithmRSA3072:
		return 3072
	case kubeadmapi.EncryptionAlgorithmRSA4096:
		return 4096
	default:
		return 0
	}
}

// GeneratePrivateKey 是生成私钥的默认函数。
func GeneratePrivateKey(keyType kubeadmapi.EncryptionAlgorithmType) (crypto.Signer, error) {
	switch keyType {
	case kubeadmapi.EncryptionAlgorithmECDSAP256:
		return ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	case kubeadmapi.EncryptionAlgorithmECDSAP384:
		return ecdsa.GenerateKey(elliptic.P384(), cryptorand.Reader)
	}

	rsaKeySize := rsaKeySizeFromAlgorithmType(keyType)
	if rsaKeySize == 0 {
		return nil, errors.Errorf("cannot obtain key size from unknown RSA algorithm: %q", keyType)
	}
	return rsa.GenerateKey(cryptorand.Reader, rsaKeySize)
}

// NewSignedCert 使用给定 CA 证书和私钥创建签名证书。
func NewSignedCert(cfg *CertConfig, key crypto.Signer, caCert *x509.Certificate, caKey crypto.Signer, isCA bool) (*x509.Certificate, error) {
	// 返回 [0, max-1) 内的均匀随机值，再加 1 使序列号均匀分布于 [1, max)。
	serial, err := cryptorand.Int(cryptorand.Reader, new(big.Int).SetInt64(math.MaxInt64-1))
	if err != nil {
		return nil, err
	}
	serial = new(big.Int).Add(serial, big.NewInt(1))
	if len(cfg.CommonName) == 0 {
		return nil, errors.New("must specify a CommonName")
	}

	keyUsage := x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature
	if isCA {
		keyUsage |= x509.KeyUsageCertSign
	}

	RemoveDuplicateAltNames(&cfg.AltNames)

	notBefore := caCert.NotBefore
	if !cfg.NotBefore.IsZero() {
		notBefore = cfg.NotBefore
	}

	notAfter := notBefore.Add(kubeadmconstants.CertificateValidityPeriod)
	if !cfg.NotAfter.IsZero() {
		notAfter = cfg.NotAfter
	}

	certTmpl := x509.Certificate{
		Subject: pkix.Name{
			CommonName:   cfg.CommonName,
			Organization: cfg.Organization,
		},
		DNSNames:              cfg.AltNames.DNSNames,
		IPAddresses:           cfg.AltNames.IPs,
		SerialNumber:          serial,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              keyUsage,
		ExtKeyUsage:           cfg.Usages,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	certDERBytes, err := x509.CreateCertificate(cryptorand.Reader, &certTmpl, caCert, key.Public(), caKey)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(certDERBytes)
}

// NewSelfSignedCACert 创建新的自签名 CA 证书。
func NewSelfSignedCACert(cfg *CertConfig, key crypto.Signer) (*x509.Certificate, error) {
	// 返回 [0, max-1) 内的均匀随机值，再加 1 使序列号均匀分布于 [1, max)。
	serial, err := cryptorand.Int(cryptorand.Reader, new(big.Int).SetInt64(math.MaxInt64-1))
	if err != nil {
		return nil, err
	}
	serial = new(big.Int).Add(serial, big.NewInt(1))

	keyUsage := x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign

	notBefore := time.Now().UTC()
	if !cfg.NotBefore.IsZero() {
		notBefore = cfg.NotBefore
	}

	notAfter := notBefore.Add(kubeadmconstants.CACertificateValidityPeriod)
	if !cfg.NotAfter.IsZero() {
		notAfter = cfg.NotAfter
	}

	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   cfg.CommonName,
			Organization: cfg.Organization,
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              keyUsage,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	if len(cfg.CommonName) > 0 {
		tmpl.DNSNames = []string{cfg.CommonName}
	}

	certDERBytes, err := x509.CreateCertificate(cryptorand.Reader, &tmpl, &tmpl, key.Public(), key)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(certDERBytes)
}

// RemoveDuplicateAltNames 移除 altNames 中的重复项。
func RemoveDuplicateAltNames(altNames *certutil.AltNames) {
	if altNames == nil {
		return
	}

	if altNames.DNSNames != nil {
		altNames.DNSNames = sets.List(sets.New(altNames.DNSNames...))
	}

	ipsKeys := make(map[string]struct{})
	var ips []net.IP
	for _, one := range altNames.IPs {
		if _, ok := ipsKeys[one.String()]; !ok {
			ipsKeys[one.String()] = struct{}{}
			ips = append(ips, one)
		}
	}
	altNames.IPs = ips
}

// ValidateCertPeriod 检查证书在当前时间的指定偏移范围内是否有效。
func ValidateCertPeriod(cert *x509.Certificate, offset time.Duration) error {
	period := fmt.Sprintf("NotBefore: %v, NotAfter: %v", cert.NotBefore, cert.NotAfter)
	now := time.Now().Add(offset).UTC()
	if now.Before(cert.NotBefore) {
		return errors.Errorf("the certificate is not valid yet: %s", period)
	}
	if now.After(cert.NotAfter) {
		return errors.Errorf("the certificate has expired: %s", period)
	}
	return nil
}

// VerifyCertChain 验证证书是否具有从中间 CA 到根 CA 的有效证书链。
func VerifyCertChain(cert *x509.Certificate, intermediates []*x509.Certificate, root *x509.Certificate) error {
	rootPool := x509.NewCertPool()
	rootPool.AddCert(root)

	intermediatePool := x509.NewCertPool()
	for _, c := range intermediates {
		intermediatePool.AddCert(c)
	}

	verifyOptions := x509.VerifyOptions{
		Roots:         rootPool,
		Intermediates: intermediatePool,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}

	if _, err := cert.Verify(verifyOptions); err != nil {
		return err
	}

	return nil
}
