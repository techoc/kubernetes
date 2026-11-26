/*
Copyright 2014 The Kubernetes Authors.

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

package cert

import (
	"bytes"
	"crypto"
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
	"strings"
	"time"

	"k8s.io/client-go/util/keyutil"
	netutils "k8s.io/utils/net"
)

const duration365d = time.Hour * 24 * 365

// Config 包含创建证书所需的基本字段。
//
// 仅用于 NewSelfSignedCACert；服务端证书请使用 SelfSignedCertKeyOptions。
type Config struct {
	CommonName   string
	Organization []string
	AltNames     AltNames
	Usages       []x509.ExtKeyUsage
	NotBefore    time.Time
}

// AltNames 包含要写入 API Server x509 证书 SAN 字段的域名与 IP，取值会直接传给 x509.Certificate。
type AltNames struct {
	DNSNames []string
	IPs      []net.IP
}

// NewSelfSignedCACert 创建自签名 CA 证书。
func NewSelfSignedCACert(cfg Config, key crypto.Signer) (*x509.Certificate, error) {
	now := time.Now()
	// 先取 [0, max-1) 内的均匀随机数再加 1，使序列号均匀分布在 [1, max)。
	// 随机序列号避免不同 CA 之间发生序列号冲突。
	serial, err := cryptorand.Int(cryptorand.Reader, new(big.Int).SetInt64(math.MaxInt64-1))
	if err != nil {
		return nil, err
	}
	serial = new(big.Int).Add(serial, big.NewInt(1))
	notBefore := now.UTC()
	// 允许调用方把生效时间提前，以便容忍客户端与服务端的时钟偏差。
	if !cfg.NotBefore.IsZero() {
		notBefore = cfg.NotBefore.UTC()
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   cfg.CommonName,
			Organization: cfg.Organization,
		},
		NotBefore:             notBefore,
		NotAfter:              now.Add(duration365d * 10).UTC(),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	// CommonName 同时作为 SAN，否则现代 TLS 校验（忽略 CN）不会认可该证书。
	if len(cfg.CommonName) > 0 {
		tmpl.DNSNames = []string{cfg.CommonName}
	}

	certDERBytes, err := x509.CreateCertificate(cryptorand.Reader, &tmpl, &tmpl, key.Public(), key)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(certDERBytes)
}

// SelfSignedCertKeyOptions 包含生成自签名证书的配置参数。
type SelfSignedCertKeyOptions struct {
	// Host 必填，标识服务证书的主机，可以是 DNS 名或 IP 地址。
	Host string
	// AlternateIPs 可选，指定服务证书额外的有效 IP。
	AlternateIPs []net.IP
	// AlternateDNS 可选，指定服务证书额外的有效 DNS 名。
	AlternateDNS []string

	// MaxAge 控制证书有效期，未设置时默认 1 年；设置 FixtureDirectory 时忽略。
	MaxAge time.Duration

	// FixtureDirectory 供测试使用。非空时指向存放预生成证书的目录，文件名格式为：
	// <host>_<ip>-<ip>_<alternateDNS>-<alternateDNS>.crt
	// <host>_<ip>-<ip>_<alternateDNS>-<alternateDNS>.key
	// 目录中缺失的证书/密钥会以 100 年有效期生成。
	//
	// 复用 fixture 可避免测试每次运行都重新生成密钥，显著缩短测试时间。
	FixtureDirectory string
}

// GenerateSelfSignedCertKey 为给定主机创建自签名证书与私钥，主机可以是 IP 或 DNS 名。
// 还可以通过 alternateIPs/alternateDNS 为证书指定额外的 subject alt name。
func GenerateSelfSignedCertKey(host string, alternateIPs []net.IP, alternateDNS []string) ([]byte, []byte, error) {
	return GenerateSelfSignedCertKeyWithOptions(SelfSignedCertKeyOptions{
		Host:         host,
		AlternateIPs: alternateIPs,
		AlternateDNS: alternateDNS,
	})
}

// GenerateSelfSignedCertKeyWithFixtures 为给定主机创建自签名证书与私钥，主机可以是 IP 或 DNS 名，
// 也可指定额外的 SAN（IP 或 DNS）。fixtureDirectory 非空时指向预生成证书的目录，文件名格式同上；
// 目录中缺失的证书/密钥会被创建。
func GenerateSelfSignedCertKeyWithFixtures(host string, alternateIPs []net.IP, alternateDNS []string, fixtureDirectory string) ([]byte, []byte, error) {
	return GenerateSelfSignedCertKeyWithOptions(SelfSignedCertKeyOptions{
		Host:             host,
		AlternateIPs:     alternateIPs,
		AlternateDNS:     alternateDNS,
		FixtureDirectory: fixtureDirectory,
	})
}

// GenerateSelfSignedCertKeyWithOptions 按给定选项生成自签名证书与私钥。
func GenerateSelfSignedCertKeyWithOptions(opts SelfSignedCertKeyOptions) ([]byte, []byte, error) {
	host := opts.Host
	alternateIPs := opts.AlternateIPs
	alternateDNS := opts.AlternateDNS
	fixtureDirectory := opts.FixtureDirectory
	maxAge := opts.MaxAge
	if maxAge == 0 {
		maxAge = 365 * 24 * time.Hour
	}

	// 提前一小时生效，避免因两端时钟轻微偏差导致刚生成的证书暂时不可用。
	validFrom := time.Now().Add(-time.Hour) // valid an hour earlier to avoid flakes due to clock skew

	baseName := fmt.Sprintf("%s_%s_%s", host, strings.Join(ipsToStrings(alternateIPs), "-"), strings.Join(alternateDNS, "-"))
	certFixturePath := filepath.Join(fixtureDirectory, baseName+".crt")
	keyFixturePath := filepath.Join(fixtureDirectory, baseName+".key")
	if len(fixtureDirectory) > 0 {
		cert, err := os.ReadFile(certFixturePath)
		if err == nil {
			key, err := os.ReadFile(keyFixturePath)
			if err == nil {
				// 命中缓存直接返回，避免重复生成 RSA 密钥带来的性能开销。
				return cert, key, nil
			}
			return nil, nil, fmt.Errorf("cert %s can be read, but key %s cannot: %v", certFixturePath, keyFixturePath, err)
		}
		maxAge = 100 * time.Hour * 24 * 365 // 100 years fixtures
	}

	caKey, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	// 先取 [0, max-1) 内的均匀随机数再加 1，使序列号均匀分布在 [1, max)。
	serial, err := cryptorand.Int(cryptorand.Reader, new(big.Int).SetInt64(math.MaxInt64-1))
	if err != nil {
		return nil, nil, err
	}
	serial = new(big.Int).Add(serial, big.NewInt(1))
	caTemplate := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("%s-ca@%d", host, time.Now().Unix()),
		},
		NotBefore: validFrom,
		NotAfter:  validFrom.Add(maxAge),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDERBytes, err := x509.CreateCertificate(cryptorand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	caCertificate, err := x509.ParseCertificate(caDERBytes)
	if err != nil {
		return nil, nil, err
	}

	priv, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	// 先取 [0, max-1) 内的均匀随机数再加 1，使序列号均匀分布在 [1, max)。
	serial, err = cryptorand.Int(cryptorand.Reader, new(big.Int).SetInt64(math.MaxInt64-1))
	if err != nil {
		return nil, nil, err
	}
	serial = new(big.Int).Add(serial, big.NewInt(1))
	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("%s@%d", host, time.Now().Unix()),
		},
		NotBefore: validFrom,
		NotAfter:  validFrom.Add(maxAge),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// host 既可能是 IP 也可能是 DNS 名，必须放入对应的 SAN 字段，
	// 放错字段会导致校验失败。
	if ip := netutils.ParseIPSloppy(host); ip != nil {
		template.IPAddresses = append(template.IPAddresses, ip)
	} else {
		template.DNSNames = append(template.DNSNames, host)
	}

	template.IPAddresses = append(template.IPAddresses, alternateIPs...)
	template.DNSNames = append(template.DNSNames, alternateDNS...)

	derBytes, err := x509.CreateCertificate(cryptorand.Reader, &template, caCertificate, &priv.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	// 先写叶子证书，再写 CA。
	// PEM 中依次写入叶子证书与 CA，便于使用者直接作为完整证书链加载。
	certBuffer := bytes.Buffer{}
	if err := pem.Encode(&certBuffer, &pem.Block{Type: CertificateBlockType, Bytes: derBytes}); err != nil {
		return nil, nil, err
	}
	if err := pem.Encode(&certBuffer, &pem.Block{Type: CertificateBlockType, Bytes: caDERBytes}); err != nil {
		return nil, nil, err
	}

	// 生成私钥。
	keyBuffer := bytes.Buffer{}
	if err := pem.Encode(&keyBuffer, &pem.Block{Type: keyutil.RSAPrivateKeyBlockType, Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		return nil, nil, err
	}

	if len(fixtureDirectory) > 0 {
		if err := os.WriteFile(certFixturePath, certBuffer.Bytes(), 0644); err != nil {
			return nil, nil, fmt.Errorf("failed to write cert fixture to %s: %v", certFixturePath, err)
		}
		// 私钥权限收紧为 0600，避免同机其他用户读取。
		if err := os.WriteFile(keyFixturePath, keyBuffer.Bytes(), 0600); err != nil {
			return nil, nil, fmt.Errorf("failed to write key fixture to %s: %v", certFixturePath, err)
		}
	}

	return certBuffer.Bytes(), keyBuffer.Bytes(), nil
}

// ipsToStrings 把 IP 切片转为字符串，用于拼接 fixture 文件名。
func ipsToStrings(ips []net.IP) []string {
	ss := make([]string, 0, len(ips))
	for _, ip := range ips {
		ss = append(ss, ip.String())
	}
	return ss
}
