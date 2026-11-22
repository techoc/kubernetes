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

package options

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/discovery"
	restclient "k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	basecompatibility "k8s.io/component-base/compatibility"
	baseversion "k8s.io/component-base/version"
	"k8s.io/klog/v2/ktesting"
	netutils "k8s.io/utils/net"
)

// setUp 构造一个最小可用的 server.Config，仅用于 TLS 相关测试。
func setUp(t *testing.T) server.Config {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)

	config := server.NewConfig(codecs)

	return *config
}

// TestCertSpec 描述一张自签测试证书：主机名、额外 DNS 名与 IP。
type TestCertSpec struct {
	host       string
	names, ips []string // in certificate
}

// NamedTestCertSpec 在 TestCertSpec 之上增加显式的 SNI 名称列表（对应 --tls-sni-cert-key）。
type NamedTestCertSpec struct {
	TestCertSpec
	explicitNames []string // as --tls-sni-cert-key explicit names
}

// TestServerRunWithSNI 验证 TLS 证书选择逻辑：客户端 SNI 命中则用对应证书，
// 未命中或客户端未发送 SNI 时回落到默认证书；同时验证 loopback 客户端可正常连接。
//
// 每个用例用真实 TLS 握手并比较返回证书的签名来确定服务端实际选了哪张证书，
// 因此能覆盖 crypto/tls 的真实匹配规则（含通配符与 IP 不参与 SNI 的行为）。
func TestServerRunWithSNI(t *testing.T) {
	tests := map[string]struct {
		Cert              TestCertSpec
		SNICerts          []NamedTestCertSpec
		ExpectedCertIndex int

		// passed in the client hello info, "localhost" if unset
		ServerName string

		// optional ip or hostname to pass to NewLoopbackClientConfig
		LoopbackClientBindAddressOverride string
	}{
		"only one cert": {
			Cert: TestCertSpec{
				host: "localhost",
				ips:  []string{"127.0.0.1"},
			},
			ExpectedCertIndex: -1,
		},
		"cert with multiple alternate names": {
			Cert: TestCertSpec{
				host:  "localhost",
				names: []string{"test.com"},
				ips:   []string{"127.0.0.1"},
			},
			ExpectedCertIndex: -1,
			ServerName:        "test.com",
		},
		"one SNI and the default cert with the same name": {
			Cert: TestCertSpec{
				host: "localhost",
				ips:  []string{"127.0.0.1"},
			},
			SNICerts: []NamedTestCertSpec{
				{
					TestCertSpec: TestCertSpec{
						host: "localhost",
					},
				},
			},
			ExpectedCertIndex: 0,
		},
		"matching SNI cert": {
			Cert: TestCertSpec{
				host: "localhost",
				ips:  []string{"127.0.0.1"},
			},
			SNICerts: []NamedTestCertSpec{
				{
					TestCertSpec: TestCertSpec{
						host: "test.com",
					},
				},
			},
			ExpectedCertIndex: 0,
			ServerName:        "test.com",
		},
		"matching IP in SNI cert and the server cert": {
			// IPs must not be passed via SNI. Hence, the ServerName in the
			// HELLO packet is empty and the server should select the non-SNI cert.
			Cert: TestCertSpec{
				host: "localhost",
				ips:  []string{"10.0.0.1", "127.0.0.1"},
			},
			SNICerts: []NamedTestCertSpec{
				{
					TestCertSpec: TestCertSpec{
						host: "test.com",
						ips:  []string{"10.0.0.1"},
					},
				},
			},
			ExpectedCertIndex: -1,
			ServerName:        "10.0.0.1",
		},
		"wildcards": {
			Cert: TestCertSpec{
				host: "localhost",
				ips:  []string{"127.0.0.1"},
			},
			SNICerts: []NamedTestCertSpec{
				{
					TestCertSpec: TestCertSpec{
						host:  "test.com",
						names: []string{"*.test.com"},
					},
				},
			},
			ExpectedCertIndex: 0,
			ServerName:        "www.test.com",
		},

		"loopback: LoopbackClientServerNameOverride not on any cert": {
			Cert: TestCertSpec{
				host: "test.com",
			},
			SNICerts: []NamedTestCertSpec{
				{
					TestCertSpec: TestCertSpec{
						host: "localhost",
					},
				},
			},
			ExpectedCertIndex: 0,
		},
		"loopback: LoopbackClientServerNameOverride on server cert": {
			Cert: TestCertSpec{
				host: server.LoopbackClientServerNameOverride,
			},
			SNICerts: []NamedTestCertSpec{
				{
					TestCertSpec: TestCertSpec{
						host: "localhost",
					},
				},
			},
			ExpectedCertIndex: 0,
		},
		"loopback: LoopbackClientServerNameOverride on SNI cert": {
			Cert: TestCertSpec{
				host: "localhost",
			},
			SNICerts: []NamedTestCertSpec{
				{
					TestCertSpec: TestCertSpec{
						host: server.LoopbackClientServerNameOverride,
					},
				},
			},
			ExpectedCertIndex: -1,
		},
		"loopback: bind to 0.0.0.0 => loopback uses localhost": {
			Cert: TestCertSpec{
				host: "localhost",
			},
			ExpectedCertIndex:                 -1,
			LoopbackClientBindAddressOverride: "0.0.0.0",
		},
	}

	specToName := func(spec TestCertSpec) string {
		name := spec.host + "_" + strings.Join(spec.names, ",") + "_" + strings.Join(spec.ips, ",")
		return strings.Replace(name, "*", "star", -1)
	}

	for title := range tests {
		test := tests[title]
		t.Run(title, func(t *testing.T) {
			t.Parallel()
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancelCause(ctx)
			defer cancel(errors.New("test has completed"))

			// create server cert
			certDir := "testdata/" + specToName(test.Cert)
			serverCertBundleFile := filepath.Join(certDir, "cert")
			serverKeyFile := filepath.Join(certDir, "key")
			err := getOrCreateTestCertFiles(serverCertBundleFile, serverKeyFile, test.Cert)
			if err != nil {
				t.Fatalf("failed to create server cert: %v", err)
			}
			ca, err := caCertFromBundle(serverCertBundleFile)
			if err != nil {
				t.Fatalf("failed to extract ca cert from server cert bundle: %v", err)
			}
			caCerts := []*x509.Certificate{ca}

			// create SNI certs
			var namedCertKeys []cliflag.NamedCertKey
			serverSig, err := certFileSignature(serverCertBundleFile, serverKeyFile)
			if err != nil {
				t.Fatalf("failed to get server cert signature: %v", err)
			}
			signatures := map[string]int{
				serverSig: -1,
			}
			for j, c := range test.SNICerts {
				sniDir := filepath.Join(certDir, specToName(c.TestCertSpec))
				certBundleFile := filepath.Join(sniDir, "cert")
				keyFile := filepath.Join(sniDir, "key")
				err := getOrCreateTestCertFiles(certBundleFile, keyFile, c.TestCertSpec)
				if err != nil {
					t.Fatalf("failed to create SNI cert %d: %v", j, err)
				}

				namedCertKeys = append(namedCertKeys, cliflag.NamedCertKey{
					KeyFile:  keyFile,
					CertFile: certBundleFile,
					Names:    c.explicitNames,
				})

				ca, err := caCertFromBundle(certBundleFile)
				if err != nil {
					t.Fatalf("failed to extract ca cert from SNI cert %d: %v", j, err)
				}
				caCerts = append(caCerts, ca)

				// store index in namedCertKeys with the signature as the key
				sig, err := certFileSignature(certBundleFile, keyFile)
				if err != nil {
					t.Fatalf("failed get SNI cert %d signature: %v", j, err)
				}
				signatures[sig] = j
			}

			// launch server
			config := setUp(t)
			info := fakeVersionInfo()
			config.EffectiveVersion = basecompatibility.NewEffectiveVersionFromString(fmt.Sprintf("%s.%s", info.Major, info.Minor), "", "")
			config.EnableIndex = true
			secureOptions := (&SecureServingOptions{
				BindAddress: netutils.ParseIPSloppy("127.0.0.1"),
				BindPort:    6443,
				ServerCert: GeneratableKeyCert{
					CertKey: CertKey{
						CertFile: serverCertBundleFile,
						KeyFile:  serverKeyFile,
					},
				},
				DisableHTTP2Serving: true,
				SNICertKeys:         namedCertKeys,
			}).WithLoopback()
			// use a random free port
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("failed to listen on 127.0.0.1:0")
			}

			secureOptions.Listener = ln
			// get port
			secureOptions.BindPort = ln.Addr().(*net.TCPAddr).Port
			config.LoopbackClientConfig = &restclient.Config{}
			if err := secureOptions.ApplyTo(&config.SecureServing, &config.LoopbackClientConfig); err != nil {
				t.Fatalf("failed applying the SecureServingOptions: %v", err)
			}

			s, err := config.Complete(nil).New("test", server.NewEmptyDelegate())
			if err != nil {
				t.Fatalf("failed creating the server: %v", err)
			}

			// add poststart hook to know when the server is up.
			startedCh := make(chan struct{})
			s.AddPostStartHookOrDie("test-notifier", func(context server.PostStartHookContext) error {
				close(startedCh)
				return nil
			})
			preparedServer := s.PrepareRun()
			preparedServerErrors := make(chan error)
			go func() {
				if err := preparedServer.RunWithContext(ctx); err != nil {
					preparedServerErrors <- err
				}
			}()

			// load ca certificates into a pool
			roots := x509.NewCertPool()
			for _, caCert := range caCerts {
				roots.AddCert(caCert)
			}

			<-startedCh

			// try to dial
			addr := fmt.Sprintf("localhost:%d", secureOptions.BindPort)
			t.Logf("Dialing %s as %q", addr, test.ServerName)
			conn, err := tls.Dial("tcp", addr, &tls.Config{
				RootCAs:    roots,
				ServerName: test.ServerName, // used for SNI in the client HELLO packet
			})
			if err != nil {
				t.Fatalf("failed to connect: %v", err)
			}
			defer conn.Close()

			// check returned server certificate
			sig := x509CertSignature(conn.ConnectionState().PeerCertificates[0])
			gotCertIndex, found := signatures[sig]
			if !found {
				t.Errorf("unknown signature returned from server: %s", sig)
			}
			if gotCertIndex != test.ExpectedCertIndex {
				t.Errorf("expected cert index %d, got cert index %d", test.ExpectedCertIndex, gotCertIndex)
			}

			// check that the loopback client can connect
			host := "127.0.0.1"
			if len(test.LoopbackClientBindAddressOverride) != 0 {
				host = test.LoopbackClientBindAddressOverride
			}
			s.LoopbackClientConfig.Host = net.JoinHostPort(host, strconv.Itoa(secureOptions.BindPort))

			client, err := discovery.NewDiscoveryClientForConfig(s.LoopbackClientConfig)
			if err != nil {
				t.Fatalf("failed to create loopback client: %v", err)
			}
			got, err := client.ServerVersion()
			if err != nil {
				t.Fatalf("failed to connect with loopback client: %v", err)
			}
			if expected := &info; !reflect.DeepEqual(got, expected) {
				t.Errorf("loopback client didn't get correct version info: expected=%v got=%v", *expected, *got)
			}

			select {
			case err := <-preparedServerErrors:
				t.Fatalf("preparedServer failed with error: %v", err)
			default:
			}
		})
	}
}

// parseIPList 把字符串 IP 列表解析为 net.IP；使用宽松解析以兼容历史配置。
func parseIPList(ips []string) []net.IP {
	var netIPs []net.IP
	for _, ip := range ips {
		netIPs = append(netIPs, netutils.ParseIPSloppy(ip))
	}
	return netIPs
}

// getOrCreateTestCertFiles 复用已存在的证书文件，否则生成并落盘。
//
// 复用而非每次重写，是为了让证书在多次测试运行间保持稳定，减少无谓的 RSA 生成开销。
func getOrCreateTestCertFiles(certFileName, keyFileName string, spec TestCertSpec) (err error) {
	if _, err := os.Stat(certFileName); err == nil {
		if _, err := os.Stat(keyFileName); err == nil {
			return nil
		}
	}

	certPem, keyPem, err := generateSelfSignedCertKey(spec.host, parseIPList(spec.ips), spec.names)
	if err != nil {
		return err
	}

	os.MkdirAll(filepath.Dir(certFileName), os.FileMode(0755))
	err = os.WriteFile(certFileName, certPem, os.FileMode(0755))
	if err != nil {
		return err
	}

	os.MkdirAll(filepath.Dir(keyFileName), os.FileMode(0755))
	err = os.WriteFile(keyFileName, keyPem, os.FileMode(0755))
	if err != nil {
		return err
	}

	return nil
}

// caCertFromBundle 从 PEM bundle 中取最后一张证书作为 CA。
//
// 约定 bundle 以叶子证书开头、CA 结尾（与 kubeadm 生成的 bundle 一致），
// 因此取最后一个 PEM 块。
func caCertFromBundle(bundlePath string) (*x509.Certificate, error) {
	pemData, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, err
	}

	// fetch last block
	var block *pem.Block
	for {
		var nextBlock *pem.Block
		nextBlock, pemData = pem.Decode(pemData)
		if nextBlock == nil {
			if block == nil {
				return nil, fmt.Errorf("no certificate found in %q", bundlePath)

			}
			return x509.ParseCertificate(block.Bytes)
		}
		block = nextBlock
	}
}

// x509CertSignature 用证书签名的 base64 作为指纹，用于在测试中识别服务端返回的证书。
func x509CertSignature(cert *x509.Certificate) string {
	return base64.StdEncoding.EncodeToString(cert.Signature)
}

// certFileSignature 加载证书文件并计算指纹。
func certFileSignature(certFile, keyFile string) (string, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return "", err
	}
	return certSignature(cert)
}

// certSignature 解析 tls.Certificate 的叶子证书并计算指纹。
func certSignature(cert tls.Certificate) (string, error) {
	x509Certs, err := x509.ParseCertificates(cert.Certificate[0])
	if err != nil {
		return "", err
	}
	return x509CertSignature(x509Certs[0]), nil
}

// fakeVersionInfo 返回固定的版本信息，便于断言 discovery 响应内容。
func fakeVersionInfo() version.Info {
	baseVer := baseversion.Get()
	baseVer.Major = "42"
	baseVer.Minor = "42"
	baseVer.EmulationMajor = "42"
	baseVer.EmulationMinor = "42"
	baseVer.MinCompatibilityMajor = "42"
	baseVer.MinCompatibilityMinor = "41"
	return baseVer
}

// generateSelfSignedCertKey creates a self-signed certificate and key for the given host.
// Host may be an IP or a DNS name
// You may also specify additional subject alt names (either ip or dns names) for the certificate
//
// 证书同时带 IsCA=true，便于测试直接把叶子证书当作信任根。
func generateSelfSignedCertKey(host string, alternateIPs []net.IP, alternateDNS []string) ([]byte, []byte, error) {
	priv, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("%s@%d", host, time.Now().Unix()),
		},
		NotBefore: time.Unix(0, 0),
		NotAfter:  time.Now().Add(time.Hour * 24 * 365 * 100),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	if ip := netutils.ParseIPSloppy(host); ip != nil {
		template.IPAddresses = append(template.IPAddresses, ip)
	} else {
		template.DNSNames = append(template.DNSNames, host)
	}

	template.IPAddresses = append(template.IPAddresses, alternateIPs...)
	template.DNSNames = append(template.DNSNames, alternateDNS...)

	derBytes, err := x509.CreateCertificate(cryptorand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	// Generate cert
	certBuffer := bytes.Buffer{}
	if err := pem.Encode(&certBuffer, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return nil, nil, err
	}

	// Generate key
	keyBuffer := bytes.Buffer{}
	if err := pem.Encode(&keyBuffer, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		return nil, nil, err
	}

	return certBuffer.Bytes(), keyBuffer.Bytes(), nil
}
