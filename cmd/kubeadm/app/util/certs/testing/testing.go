/*
Copyright The Kubernetes Authors.

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

// Package testing 包含在测试中管理 kubeadm 证书的工具。
package testing

import (
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"net"
	"path/filepath"
	"slices"
	"testing"
	"time"

	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"

	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

// SetupCertificateAuthority 创建 CA 证书和密钥对，供 kubeadm 测试使用。
func SetupCertificateAuthority(t *testing.T) (*x509.Certificate, crypto.Signer) {
	caCert, caKey, err := pkiutil.NewCertificateAuthority(&pkiutil.CertConfig{
		Config: certutil.Config{CommonName: "kubernetes"},
	})
	if err != nil {
		t.Fatalf("Failure while generating CA certificate and key: %v", err)
	}

	return caCert, caKey
}

// SetupIntermediateCertificateAuthority 创建中间 CA 证书和密钥对，供 kubeadm 测试使用。
func SetupIntermediateCertificateAuthority(t *testing.T, parentCert *x509.Certificate, parentKey crypto.Signer, cn string) (*x509.Certificate, crypto.Signer) {
	caCert, caKey, err := pkiutil.NewIntermediateCertificateAuthority(parentCert, parentKey, &pkiutil.CertConfig{
		Config: certutil.Config{CommonName: cn},
	})
	if err != nil {
		t.Fatalf("Failure while generating intermediate CA certificate and key: %v", err)
	}

	return caCert, caKey
}

// SetupPkiDirWithCertificateAuthority 在给定临时目录的 /pki 子目录中创建 CA 证书和密钥对，
// 并返回所创建 pki 目录的路径。
func SetupPkiDirWithCertificateAuthority(t *testing.T, tmpdir string) string {
	caCert, caKey := SetupCertificateAuthority(t)

	certDir := filepath.Join(tmpdir, "pki")
	if err := pkiutil.WriteCertAndKey(certDir, kubeadmconstants.CACertAndKeyBaseName, caCert, caKey); err != nil {
		t.Fatalf("failure while saving CA certificate and key: %v", err)
	}

	return certDir
}

// AssertCertificateIsSignedByCa 断言给定证书由预期 CA 签署，供 kubeadm 测试使用。
func AssertCertificateIsSignedByCa(t *testing.T, cert *x509.Certificate, signingCa *x509.Certificate) {
	if err := cert.CheckSignatureFrom(signingCa); err != nil {
		t.Error("cert is not signed by signing CA as expected")
	}
}

// AssertCertificateHasNotBefore 断言证书的 NotBefore 符合预期。
// 证书最高精度为秒，因此将 expectedNotBefore 截断到 1 秒。
func AssertCertificateHasNotBefore(t *testing.T, cert *x509.Certificate, expectedNotBefore time.Time) {
	truncated := expectedNotBefore.Truncate(time.Second)
	if !cert.NotBefore.Equal(truncated) {
		t.Errorf("cert has NotBefore %v, expected %v", cert.NotBefore, truncated)
	}
}

// AssertCertificateHasNotAfter 断言证书的 NotAfter 符合预期。
// 证书最高精度为秒，因此将 expectedNotAfter 截断到 1 秒。
func AssertCertificateHasNotAfter(t *testing.T, cert *x509.Certificate, expectedNotAfter time.Time) {
	truncated := expectedNotAfter.Truncate(time.Second)
	if !cert.NotAfter.Equal(truncated) {
		t.Errorf("cert has NotAfter %v, expected %v", cert.NotAfter, truncated)
	}
}

// AssertCertificateHasCommonName 断言证书的 SubjectCommonName 符合预期，供 kubeadm 测试使用。
func AssertCertificateHasCommonName(t *testing.T, cert *x509.Certificate, commonName string) {
	if cert.Subject.CommonName != commonName {
		t.Errorf("cert has Subject.CommonName %s, expected %s", cert.Subject.CommonName, commonName)
	}
}

// AssertCertificateHasOrganizations 断言证书仅包含预期的 Subject.Organization，供 kubeadm 测试使用。
func AssertCertificateHasOrganizations(t *testing.T, cert *x509.Certificate, organizations ...string) {
	if len(cert.Subject.Organization) != len(organizations) {
		t.Fatalf("cert contains a different number of Subject.Organization, expected %v, got %v", organizations, cert.Subject.Organization)
	}
	for _, organization := range organizations {
		found := false
		for i := range cert.Subject.Organization {
			if cert.Subject.Organization[i] == organization {
				found = true
			}
		}
		if !found {
			t.Errorf("cert does not contain Subject.Organization %s as expected", organization)
		}
	}
}

// AssertCertificateHasClientAuthUsage 断言证书包含预期的 ExtKeyUsageClientAuth，供 kubeadm 测试使用。
func AssertCertificateHasClientAuthUsage(t *testing.T, cert *x509.Certificate) {
	if slices.Contains(cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return
	}
	t.Error("cert has not ClientAuth usage as expected")
}

// AssertCertificateHasServerAuthUsage 断言证书包含预期的 ExtKeyUsageServerAuth，供 kubeadm 测试使用。
func AssertCertificateHasServerAuthUsage(t *testing.T, cert *x509.Certificate) {
	if slices.Contains(cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		return
	}
	t.Error("cert is not a ServerAuth")
}

// AssertCertificateHasDNSNames 断言证书包含预期的 DNSNames，供 kubeadm 测试使用。
func AssertCertificateHasDNSNames(t *testing.T, cert *x509.Certificate, DNSNames ...string) {
	for _, DNSName := range DNSNames {
		found := slices.Contains(cert.DNSNames, DNSName)

		if !found {
			t.Errorf("cert does not contain DNSName %s", DNSName)
		}
	}
}

// AssertCertificateHasIPAddresses 断言证书包含预期的 IPAddresses，供 kubeadm 测试使用。
func AssertCertificateHasIPAddresses(t *testing.T, cert *x509.Certificate, IPAddresses ...net.IP) {
	for _, IPAddress := range IPAddresses {
		found := false
		for _, val := range cert.IPAddresses {
			if val.Equal(IPAddress) {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("cert does not contain IPAddress %s", IPAddress)
		}
	}
}

// CreateCACert 创建通用 CA 证书。
func CreateCACert(t *testing.T) (*x509.Certificate, crypto.Signer) {
	certCfg := &pkiutil.CertConfig{Config: certutil.Config{CommonName: "kubernetes"}}
	cert, key, err := pkiutil.NewCertificateAuthority(certCfg)
	if err != nil {
		t.Fatalf("couldn't create CA: %v", err)
	}
	return cert, key
}

// CreateTestCert 使用给定 CA 和备用名称创建通用证书。
func CreateTestCert(t *testing.T, caCert *x509.Certificate, caKey crypto.Signer, altNames certutil.AltNames) (*x509.Certificate, crypto.Signer, *pkiutil.CertConfig) {
	config := &pkiutil.CertConfig{
		Config: certutil.Config{
			CommonName: "testCert",
			Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			AltNames:   altNames,
		},
	}
	cert, key, err := pkiutil.NewCertAndKey(caCert, caKey, config)
	if err != nil {
		t.Fatalf("couldn't create test cert: %v", err)
	}
	return cert, key, config
}

// CertTestCase 描述证书配置及其预期是否可用。
type CertTestCase struct {
	Name        string
	Files       PKIFiles
	ExpectError bool
}

// GetSparseCertTestCases 生成一系列证书配置及预期结果。
func GetSparseCertTestCases(t *testing.T) []CertTestCase {

	caCert, caKey := CreateCACert(t)
	fpCACert, fpCAKey := CreateCACert(t)
	etcdCACert, etcdCAKey := CreateCACert(t)

	fpCert, fpKey, _ := CreateTestCert(t, fpCACert, fpCAKey, certutil.AltNames{})

	return []CertTestCase{
		{
			Name: "nothing present",
		},
		{
			Name: "CAs already exist",
			Files: PKIFiles{
				"ca.crt":             caCert,
				"ca.key":             caKey,
				"front-proxy-ca.crt": fpCACert,
				"front-proxy-ca.key": fpCAKey,
				"etcd/ca.crt":        etcdCACert,
				"etcd/ca.key":        etcdCAKey,
			},
		},
		{
			Name: "CA certs only",
			Files: PKIFiles{
				"ca.crt":             caCert,
				"front-proxy-ca.crt": fpCACert,
				"etcd/ca.crt":        etcdCACert,
			},
			ExpectError: true,
		},
		{
			Name: "FrontProxyCA with certs",
			Files: PKIFiles{
				"ca.crt":                 caCert,
				"ca.key":                 caKey,
				"front-proxy-ca.crt":     fpCACert,
				"front-proxy-client.crt": fpCert,
				"front-proxy-client.key": fpKey,
				"etcd/ca.crt":            etcdCACert,
				"etcd/ca.key":            etcdCAKey,
			},
		},
		{
			Name: "FrontProxy certs missing CA",
			Files: PKIFiles{
				"front-proxy-client.crt": fpCert,
				"front-proxy-client.key": fpKey,
			},
			ExpectError: true,
		},
	}
}

// PKIFiles 列出测试用例应创建的文件。
type PKIFiles map[string]interface{}

// WritePKIFiles 将给定文件写入指定目录。
func WritePKIFiles(t *testing.T, dir string, files PKIFiles) {
	for filename, body := range files {
		switch body := body.(type) {
		case *x509.Certificate:
			if err := certutil.WriteCert(filepath.Join(dir, filename), pkiutil.EncodeCertPEM(body)); err != nil {
				t.Errorf("unable to write certificate to file %q: [%v]", dir, err)
			}
		case *rsa.PublicKey:
			publicKeyBytes, err := pkiutil.EncodePublicKeyPEM(body)
			if err != nil {
				t.Errorf("unable to write public key to file %q: [%v]", filename, err)
			}
			if err := keyutil.WriteKey(filepath.Join(dir, filename), publicKeyBytes); err != nil {
				t.Errorf("unable to write public key to file %q: [%v]", filename, err)
			}
		case *rsa.PrivateKey:
			privateKey, err := keyutil.MarshalPrivateKeyToPEM(body)
			if err != nil {
				t.Errorf("unable to write private key to file %q: [%v]", filename, err)
			}
			if err := keyutil.WriteKey(filepath.Join(dir, filename), privateKey); err != nil {
				t.Errorf("unable to write private key to file %q: [%v]", filename, err)
			}
		}
	}
}
