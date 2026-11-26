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

package cert

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"
)

// GetClientCANames 获取服务端接受的客户端证书的 CA 名称，用于检查服务端信任哪些 CA。
// 特定服务端的状态。apiHost 形如 "host:port"。
//
// GetClientCANames 获取服务端接受的客户端证书 CA 名称，用于检查特定服务端的配置。
// apiHost 形如 "host:port"。
func GetClientCANames(apiHost string) ([]string, error) {
	// 第二次运行时已知期望的是哪一个。
	acceptableCAs := []string{}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // this is insecure to always get to the GetClientCertificate
		// 服务端会在 CertificateRequest 中下发可接受的 CA 列表；这里直接返回空证书，
		// 只为了拿到该列表，不进行真正的双向认证。
		GetClientCertificate: func(hello *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			acceptableCAs = []string{}
			for _, curr := range hello.AcceptableCAs {
				acceptableCAs = append(acceptableCAs, string(curr))
			}
			return &tls.Certificate{}, nil
		},
	}

	conn, err := tls.Dial("tcp", apiHost, tlsConfig)
	if err != nil {
		return nil, err
	}
	if err := conn.Close(); err != nil {
		return nil, err
	}

	return acceptableCAs, nil
}

// GetClientCANamesForURL 是针对 kubeconfig 中 URL 字符串的 GetClientCANames 变体。
//
// GetClientCANamesForURL 是 GetClientCANames 的 URL 版本，接受 kubeconfig 中使用的 URL。
func GetClientCANamesForURL(kubeConfigURL string) ([]string, error) {
	apiserverURL, err := url.Parse(kubeConfigURL)
	if err != nil {
		return nil, err
	}
	return GetClientCANames(apiserverURL.Host)
}

// GetServingCertificates 返回服务端用作证书的 x509 证书及 PEM 编码字节。
// serverName 可选，用于获取不同名字的 SNI 证书；apiHost 形如 "host:port"。
//
// GetServingCertificates 返回服务端使用的 x509 证书，以及对应的 PEM 编码字节。
// serverName 可选，用于指定不同的 SNI 名称以获取对应证书；apiHost 形如 "host:port"。
func GetServingCertificates(apiHost, serverName string) ([]*x509.Certificate, [][]byte, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // this is insecure so that we always get connected
	}
	// 若指定了 SNI 名称，则设置它。
	if len(serverName) > 0 {
		tlsConfig.ServerName = serverName
	}

	conn, err := tls.Dial("tcp", apiHost, tlsConfig)
	if err != nil {
		return nil, nil, err
	}
	if err = conn.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close connection : %v", err)
	}

	// 连接关闭不影响已经保存在 ConnectionState 中的对端证书。
	peerCerts := conn.ConnectionState().PeerCertificates
	peerCertBytes := [][]byte{}
	for _, a := range peerCerts {
		actualCert, err := EncodeCertificates(a)
		if err != nil {
			return nil, nil, err
		}
		peerCertBytes = append(peerCertBytes, []byte(strings.TrimSpace(string(actualCert))))
	}

	return peerCerts, peerCertBytes, err
}

// GetServingCertificatesForURL 是针对 kubeconfig 中 URL 字符串的 GetServingCertificates 变体。
//
// GetServingCertificatesForURL 是 GetServingCertificates 的 URL 版本，接受 kubeconfig 中使用的 URL。
func GetServingCertificatesForURL(kubeConfigURL, serverName string) ([]*x509.Certificate, [][]byte, error) {
	apiserverURL, err := url.Parse(kubeConfigURL)
	if err != nil {
		return nil, nil, err
	}
	return GetServingCertificates(apiserverURL.Host, serverName)
}
