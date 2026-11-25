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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	netutils "k8s.io/utils/net"

	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/pkiutil"
)

func TestPKICertificateReadWriter(t *testing.T) {
	// 创建临时目录
	dir := t.TempDir()

	// 创建证书
	cert := writeTestCertificate(t, dir, "test", testCACert, testCAKey, testCertOrganization, time.Time{}, time.Time{})

	// 创建 pkiCertificateReadWriter
	pkiReadWriter := newPKICertificateReadWriter(dir, "test")

	// 读取证书
	readCert, err := pkiReadWriter.Read()
	if err != nil {
		t.Fatalf("couldn't read certificate: %v", err)
	}

	// 检查从磁盘读取的证书是否与原始证书一致
	if !cert.Equal(readCert) {
		t.Errorf("read cert does not match with expected cert")
	}

	// 创建新证书
	newCert, newkey, err := pkiutil.NewCertAndKey(testCACert, testCAKey, testCertCfg)
	if err != nil {
		t.Fatalf("couldn't generate certificate: %v", err)
	}

	// 写入新证书
	err = pkiReadWriter.Write(newCert, newkey)
	if err != nil {
		t.Fatalf("couldn't write new certificate: %v", err)
	}

	// 读回新证书
	readCert, err = pkiReadWriter.Read()
	if err != nil {
		t.Fatalf("couldn't read new certificate: %v", err)
	}

	// 检查从磁盘读回的新证书是否与原始证书一致
	if !newCert.Equal(readCert) {
		t.Error("read cert does not match with expected new cert")
	}
}

func TestKubeconfigReadWriter(t *testing.T) {
	// 创建临时目录
	dirKubernetes := t.TempDir()
	dirPKI := t.TempDir()

	// 将 CA 证书和私钥写入临时 PKI 目录
	caName := kubeadmconstants.CACertAndKeyBaseName
	if err := pkiutil.WriteCertAndKey(
		dirPKI,
		caName,
		testCACert,
		testCAKey); err != nil {
		t.Fatalf("couldn't write out certificate %s to %s", caName, dirPKI)
	}

	// 创建证书并将其嵌入 kubeconfig 文件
	cert := writeTestKubeconfig(t, dirKubernetes, "test", testCACert, testCAKey, time.Time{}, time.Time{})

	// 创建 KubeconfigReadWriter
	kubeconfigReadWriter := newKubeconfigReadWriter(dirKubernetes, "test", dirPKI, caName)

	// 读取 kubeconfig 中嵌入的证书
	readCert, err := kubeconfigReadWriter.Read()
	if err != nil {
		t.Fatalf("couldn't read embedded certificate: %v", err)
	}

	// 检查从磁盘读取的证书是否与原始证书一致
	if !cert.Equal(readCert) {
		t.Errorf("read cert does not match with expected cert")
	}

	// 创建新证书
	newCert, newkey, err := pkiutil.NewCertAndKey(testCACert, testCAKey, testCertCfg)
	if err != nil {
		t.Fatalf("couldn't generate certificate: %v", err)
	}

	// 将新证书写入 kubeconfig
	err = kubeconfigReadWriter.Write(newCert, newkey)
	if err != nil {
		t.Fatalf("couldn't write new embedded certificate: %v", err)
	}

	// 确保 Read() 时不存在 CA 私钥，因为无需读取。
	// 此测试覆盖 CA 由外部提供且宿主机上没有该 CA 的情况。
	_, caKeyPath := pkiutil.PathsForCertAndKey(dirPKI, caName)
	os.Remove(caKeyPath)

	// 从 kubeconfig 写入器读回新嵌入的证书
	readCert, err = kubeconfigReadWriter.Read()
	if err != nil {
		t.Fatalf("couldn't read new embedded  certificate: %v", err)
	}

	// 检查从磁盘读回的新证书是否与原始证书一致
	if !newCert.Equal(readCert) {
		t.Errorf("read cert does not match with expected new cert")
	}
}

// writeTestCertificate 是创建测试证书的工具函数。
func writeTestCertificate(t *testing.T, dir, name string, caCert *x509.Certificate, caKey crypto.Signer, organization []string, notBefore, notAfter time.Time) *x509.Certificate {
	cert, key, err := pkiutil.NewCertAndKey(caCert, caKey, makeTestCertConfig(organization, notBefore, notAfter))
	if err != nil {
		t.Fatalf("couldn't generate certificate: %v", err)
	}

	if err := pkiutil.WriteCertAndKey(dir, name, cert, key); err != nil {
		t.Fatalf("couldn't write out certificate %s to %s", name, dir)
	}

	return cert
}

// writeTestKubeconfig 是创建含嵌入证书的测试 kubeconfig 的工具函数。
func writeTestKubeconfig(t *testing.T, dir, name string, caCert *x509.Certificate, caKey crypto.Signer, notBefore, notAfter time.Time) *x509.Certificate {

	cfg := &pkiutil.CertConfig{
		Config: certutil.Config{
			CommonName:   "test-common-name",
			Organization: testCertOrganization,
			Usages:       []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			AltNames: certutil.AltNames{
				IPs:      []net.IP{netutils.ParseIPSloppy("10.100.0.1")},
				DNSNames: []string{"test-domain.space"},
			},
			NotBefore: notBefore,
		},
		NotAfter: notAfter,
	}
	cert, key, err := pkiutil.NewCertAndKey(caCert, caKey, cfg)
	if err != nil {
		t.Fatalf("couldn't generate certificate: %v", err)
	}

	encodedClientKey, err := keyutil.MarshalPrivateKeyToPEM(key)
	if err != nil {
		t.Fatalf("failed to marshal private key to PEM: %v", err)
	}

	certificateAuthorityData := pkiutil.EncodeCertPEM(caCert)

	config := kubeconfigutil.CreateWithCerts(
		"https://localhost:1234",
		"kubernetes-test",
		"user-test",
		certificateAuthorityData,
		encodedClientKey,
		pkiutil.EncodeCertPEM(cert),
	)

	if err := clientcmd.WriteToFile(*config, filepath.Join(dir, name)); err != nil {
		t.Fatalf("couldn't write out certificate")
	}

	return cert
}

func TestFileExists(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("Couldn't create tmpdir: %v", err)
	}
	defer func() {
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Fatalf("Fail to remove tmpdir: %v", err)
		}
	}()
	tmpfile, err := os.CreateTemp(tmpdir, "")
	if err != nil {
		t.Fatalf("Couldn't create tmpfile: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("Couldn't close tmpfile: %v", err)
	}

	tests := []struct {
		name     string
		filename string
		want     bool
	}{
		{
			name:     "file exist",
			filename: tmpfile.Name(),
			want:     true,
		},
		{
			name:     "file does not exist",
			filename: "foo",
			want:     false,
		},
		{
			name:     "file path is a dir",
			filename: tmpdir,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := fileExists(tt.filename); got != tt.want {
				t.Errorf("fileExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPKICertificateReadWriterExists(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("Couldn't create tmpdir: %v", err)
	}
	defer func() {
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Fatalf("Fail to remove tmpdir: %v", err)
		}
	}()
	filename := "testfile"
	tmpfilepath := filepath.Join(tmpdir, filename+".crt")
	err = os.WriteFile(tmpfilepath, nil, 0644)
	if err != nil {
		t.Fatalf("Couldn't write file: %v", err)
	}
	type fields struct {
		baseName       string
		certificateDir string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "cert file exists",
			fields: fields{
				baseName:       filename,
				certificateDir: tmpdir,
			},
			want: true,
		},
		{
			name: "cert file does not exist",
			fields: fields{
				baseName:       "foo",
				certificateDir: tmpdir,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rw := &pkiCertificateReadWriter{
				baseName:       tt.fields.baseName,
				certificateDir: tt.fields.certificateDir,
			}
			if got, _ := rw.Exists(); got != tt.want {
				t.Errorf("pkiCertificateReadWriter.Exists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKubeConfigReadWriterExists(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("Couldn't create tmpdir: %v", err)
	}
	defer func() {
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Fatalf("Fail to remove tmpdir: %v", err)
		}
	}()
	tmpfile, err := os.CreateTemp(tmpdir, "")
	if err != nil {
		t.Fatalf("Couldn't create tmpfile: %v", err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatalf("Couldn't close tmpfile: %v", err)
	}

	tests := []struct {
		name               string
		kubeConfigFilePath string
		want               bool
	}{
		{
			name:               "file exists",
			kubeConfigFilePath: tmpfile.Name(),
			want:               true,
		},
		{
			name:               "file does not exist",
			kubeConfigFilePath: "foo",
			want:               false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rw := &kubeConfigReadWriter{
				kubeConfigFilePath: tt.kubeConfigFilePath,
			}
			if got, _ := rw.Exists(); got != tt.want {
				t.Errorf("kubeConfigReadWriter.Exists() = %v, want %v", got, tt.want)
			}
		})
	}
}
