/*
Copyright 2015 The Kubernetes Authors.

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

// Package transport 构建 client-go 使用的 HTTP 传输层：
// TLS 配置、证书加载与缓存，以及认证/调试等 RoundTripper 包装。
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	clientgofeaturegate "k8s.io/client-go/features"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/klog/v2"
)

// New 返回提供配置所定义认证与传输层安全设置的 http.RoundTripper。
// 或由配置定义的传输层安全设置。
func New(config *Config) (http.RoundTripper, error) {
	metrics.EnsureRegistered()
	// 设置传输层安全参数。
	if config.Transport != nil && (config.HasCA() || config.HasCertAuth() || config.HasCertCallback() || config.TLS.Insecure) {
		// 自定义 transport 会绕过 TLS 配置，二者同时出现说明调用方意图冲突。
		return nil, fmt.Errorf("using a custom transport with TLS certificate options or the insecure flag is not allowed")
	}

	if !isValidHolders(config) {
		return nil, fmt.Errorf("misconfigured holder for dialer or cert callback")
	}

	var (
		rt  http.RoundTripper
		err error
	)

	if config.Transport != nil {
		rt = config.Transport
	} else {
		// tlsCache 复用相同 TLS 配置的底层连接，避免连接数随客户端数量膨胀。
		rt, err = tlsCache.get(config)
		if err != nil {
			return nil, err
		}
	}

	return HTTPWrappersForConfig(config, rt)
}

// isValidHolders 校验 holder 非 nil 时其函数字段也已设置，避免运行时空指针。
func isValidHolders(config *Config) bool {
	if config.TLS.GetCertHolder != nil && config.TLS.GetCertHolder.GetCert == nil {
		return false
	}

	if config.DialHolder != nil && config.DialHolder.Dial == nil {
		return false
	}

	return true
}

// TLSConfigFor 返回提供配置所定义传输层安全设置的 tls.Config；若不需要传输层
// 安全则返回 nil。
func TLSConfigFor(c *Config) (*tls.Config, error) {
	// 未请求任何 TLS 能力时返回 nil，让调用方使用默认 transport。
	if !(c.HasCA() || c.HasCertAuth() || c.HasCertCallback() || c.TLS.Insecure || len(c.TLS.ServerName) > 0 || len(c.TLS.NextProtos) > 0) {
		return nil, nil
	}
	if c.HasCA() && c.TLS.Insecure {
		return nil, fmt.Errorf("specifying a root certificates file with the insecure flag is not allowed")
	}
	// 把文件内容读入内存，后续 TLS 配置只依赖 Data 字段。
	if err := loadTLSFiles(c); err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		// 不能使用 SSLv3（POODLE/BEAST）；不能使用 TLSv1.0（POODLE/BEAST CBC）；
		// 不能使用 TLSv1.1（使用 RC4 密码套件）。
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.TLS.Insecure,
		ServerName:         c.TLS.ServerName,
		NextProtos:         c.TLS.NextProtos,
	}

	if c.HasCA() {
		/*
			kubernetes mutual (2-way) x509 between client and apiserver:

				1. apiserver sending its apiserver certificate along with its publickey to client
				>2. client verifies the apiserver certificate sent against its cluster certificate authority data
				3. client sending its client certificate along with its public key to the apiserver
				4. apiserver verifies the client certificate sent against its cluster certificate authority data

				description:
					here, with this block,
					cluster certificate authority data gets loaded into TLS before the handshake process
					for client to later during the handshake verify the apiserver certificate

				normal args related to this stage:
					--certificate-authority='':
						Path to a cert file for the certificate authority

					(retrievable from "kubectl options" command)
					(suggested by @deads2k)

				see also:
					- for the step 1, see: staging/src/k8s.io/apiserver/pkg/server/options/serving.go
					- for the step 3, see: a few lines below in this file
					- for the step 4, see: staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go
		*/

		rootCAs, err := rootCertPool(c.TLS.CAData)
		if err != nil {
			return nil, fmt.Errorf("unable to load root certificates: %w", err)
		}
		tlsConfig.RootCAs = rootCAs
	}

	var staticCert *tls.Certificate
	// 只要 key 或 cert 是以数据（而非文件）提供，就按静态证书处理。
	if c.HasCertAuth() && !c.TLS.ReloadTLSFiles {
		// 若提供了 key/cert，在设置 tlsConfig.GetClientCertificate 前先验校它们。
		cert, err := tls.X509KeyPair(c.TLS.CertData, c.TLS.KeyData)
		if err != nil {
			return nil, err
		}
		staticCert = &cert
	}

	var dynamicCertLoader func() (*tls.Certificate, error)
	if c.TLS.ReloadTLSFiles {
		// 仅当证书/私钥都来自文件时才启用热加载；Data 形式无法感知文件变化。
		dynamicCertLoader = cachingCertificateLoader(c.TLS.CertFile, c.TLS.KeyFile)
	}

	if c.HasCertAuth() || c.HasCertCallback() {

		/*
			    kubernetes mutual (2-way) x509 between client and apiserver:

					1. apiserver sending its apiserver certificate along with its publickey to client
					2. client verifies the apiserver certificate sent against its cluster certificate authority data
					>3. client sending its client certificate along with its public key to the apiserver
					4. apiserver verifies the client certificate sent against its cluster certificate authority data

					description:
						here, with this callback function,
						client certificate and pub key get loaded into TLS during the handshake process
						for apiserver to later in the step 4 verify the client certificate

					normal args related to this stage:
						--client-certificate='':
							Path to a client certificate file for TLS
						--client-key='':
							Path to a client key file for TLS

						(retrievable from "kubectl options" command)
						(suggested by @deads2k)

					see also:
						- for the step 1, see: staging/src/k8s.io/apiserver/pkg/server/options/serving.go
						- for the step 2, see: a few lines above in this file
						- for the step 4, see: staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go
		*/

		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			// 注意：静态 key/cert 数据始终优先于 cert 回调。
			if staticCert != nil {
				return staticCert, nil
			}
			// key/cert 来自文件时会设置 ReloadTLSFiles，优先于 cert 回调。
			if dynamicCertLoader != nil {
				return dynamicCertLoader()
			}
			if c.HasCertCallback() {
				cert, err := c.TLS.GetCertHolder.GetCert()
				if err != nil {
					return nil, err
				}
				// GetCert 可能返回空值，表示没有证书。
				if cert != nil {
					return cert, nil
				}
			}

			// CertData/KeyData 未设置且 GetCert 也未返回内容时，返回空的 tls.Certificate，
			// 不向服务端发送客户端证书。
			return &tls.Certificate{}, nil
		}
	}

	return tlsConfig, nil
}

// loadTLSFiles 把 CertFile、KeyFile、CAFile 字段的内容读入 CertData、KeyData、
// CAData 字段，失败时返回错误。成功时三个字段要么都被填充，要么原本就为空。
//
// 同时决定是否启用文件热加载：只有“纯文件来源”才安全，因为 Data 与 File 混用时无法判断优先级。
func loadTLSFiles(c *Config) error {
	// 检查是否纯粹从文件加载 CA。
	if clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowCARotation) {
		if len(c.TLS.CAFile) > 0 && len(c.TLS.CAData) == 0 {
			c.TLS.ReloadCAFiles = true
		}
	} else if c.TLS.ReloadCAFiles {
		return fmt.Errorf("ReloadCAFiles=true requires ClientsAllowCARotation to be enabled")
	}

	// 检查是否纯粹从文件加载证书与私钥。
	if len(c.TLS.CertFile) > 0 && len(c.TLS.CertData) == 0 && len(c.TLS.KeyFile) > 0 && len(c.TLS.KeyData) == 0 {
		c.TLS.ReloadTLSFiles = true
	}

	var err error
	c.TLS.CAData, err = dataFromSliceOrFile(c.TLS.CAData, c.TLS.CAFile)
	if err != nil {
		return err
	}

	c.TLS.CertData, err = dataFromSliceOrFile(c.TLS.CertData, c.TLS.CertFile)
	if err != nil {
		return err
	}

	c.TLS.KeyData, err = dataFromSliceOrFile(c.TLS.KeyData, c.TLS.KeyFile)
	return err
}

// dataFromSliceOrFile 从 slice（非空时）或文件返回数据；读文件出错时返回错误。
func dataFromSliceOrFile(data []byte, file string) ([]byte, error) {
	if len(data) > 0 {
		return data, nil
	}
	if len(file) > 0 {
		fileData, err := os.ReadFile(file)
		if err != nil {
			return []byte{}, err
		}
		return fileData, nil
	}
	return nil, nil
}

// rootCertPool 在 caData 为空时返回 nil；把 nil 继续传递下去即表示“使用系统根 CA”。
// caData 非空时，它是 CertPool 唯一的信息来源。
// 语义是“要么用系统根证书，要么只用给定 CA”，不支持二者并存。
func rootCertPool(caData []byte) (*x509.CertPool, error) {
	// 真正想要的是 x509.systemRootsPool 的副本，但它没有暴露；自行构造又很困难（跨平台
	// 实现相当复杂），因此利用 RootCAs == nil 会使用系统根 CA 这一点。这样无法同时信任
	// 两者，但希望不会成为问题。
	if len(caData) == 0 {
		// 启用 ClientsAllowCARotation 特性门控时，返回空但非 nil 的 pool；这样用户把 CAFile
		// 明确指向零字节文件时不会回退到系统根 CA。
		if clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowCARotation) {
			return x509.NewCertPool(), nil
		}
		return nil, nil
	}

	// 有 caData 就使用它。
	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(caData); !ok {
		return nil, createErrorParsingCAData(caData)
	}
	return certPool, nil
}

// createErrorParsingCAData 总是返回 error。之所以要调用它，是因为 AppendCertsFromPEM 只返回
// true/false，我们无法得知具体失败原因；这里逐个解析 PEM block，
// 以给出比 “AppendCertsFromPEM=false” 更可诊断的错误。
func createErrorParsingCAData(pemCerts []byte) error {
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			return fmt.Errorf("unable to parse bytes as PEM block")
		}

		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return fmt.Errorf("failed to parse certificate: %w", err)
		}
	}
	return fmt.Errorf("no valid certificate authority data seen")
}

// WrapperFunc 在创建新 transport 时包装 http.RoundTripper。
// 为客户端创建 transport 时调用，以便按连接注入行为。
type WrapperFunc func(rt http.RoundTripper) http.RoundTripper

// Wrappers 接受任意数量的 wrapper，返回一个等价于按顺序调用它们的 wrapper。
// nil 值会被忽略，便于逐步包装一个函数。
func Wrappers(fns ...WrapperFunc) WrapperFunc {
	if len(fns) == 0 {
		return nil
	}
	// 优化常见场景：给可能为空的 transport wrapper 再包一层。
	if len(fns) == 2 && fns[0] == nil {
		return fns[1]
	}
	return func(rt http.RoundTripper) http.RoundTripper {
		base := rt
		for _, fn := range fns {
			if fn != nil {
				base = fn(base)
			}
		}
		return base
	}
}

// ContextCanceller 阻止在给定 context 结束后发起新请求。context 关闭时返回 err，
// 让调用方给出与 context 相关的错误。
func ContextCanceller(ctx context.Context, err error) WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &contextCanceller{
			ctx: ctx,
			rt:  rt,
			err: err,
		}
	}
}

// contextCanceller 在每次 RoundTrip 前检查 context，已取消则不再发起请求。
type contextCanceller struct {
	ctx context.Context
	rt  http.RoundTripper
	err error
}

func (b *contextCanceller) RoundTrip(req *http.Request) (*http.Response, error) {
	select {
	case <-b.ctx.Done():
		return nil, b.err
	default:
		return b.rt.RoundTrip(req)
	}
}

// tryCancelRequest 递归剥开 RoundTripper 包装器，找到实现了 CancelRequest 的底层 transport。
func tryCancelRequest(rt http.RoundTripper, req *http.Request) {
	type canceler interface {
		CancelRequest(*http.Request)
	}
	switch rt := rt.(type) {
	case canceler:
		rt.CancelRequest(req)
	case utilnet.RoundTripperWrapper:
		tryCancelRequest(rt.WrappedRoundTripper(), req)
	default:
		klog.FromContext(req.Context()).Info("Warning: unable to cancel request", "roundTripperType", fmt.Sprintf("%T", rt))
	}
}

// certificateCacheEntry 缓存一次证书加载的结果与加载时间。
type certificateCacheEntry struct {
	cert  *tls.Certificate
	err   error
	birth time.Time
}

// 缓存条目过期时 isStale 返回 true。
func (c *certificateCacheEntry) isStale() bool {
	return time.Since(c.birth) > time.Second
}

// newCertificateCacheEntry 立即读取证书与私钥；错误也缓存，避免坏文件导致每秒重试风暴。
func newCertificateCacheEntry(certFile, keyFile string) certificateCacheEntry {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	return certificateCacheEntry{cert: &cert, err: err, birth: time.Now()}
}

// cachingCertificateLoader 确保打开大量连接时不会频繁访问文件系统；底层证书
// 文件最多每秒读取一次。
func cachingCertificateLoader(certFile, keyFile string) func() (*tls.Certificate, error) {
	current := newCertificateCacheEntry(certFile, keyFile)
	var currentMtx sync.RWMutex

	return func() (*tls.Certificate, error) {
		currentMtx.RLock()
		if current.isStale() {
			currentMtx.RUnlock()

			currentMtx.Lock()
			defer currentMtx.Unlock()

			if current.isStale() {
				current = newCertificateCacheEntry(certFile, keyFile)
			}
		} else {
			defer currentMtx.RUnlock()
		}

		return current.cert, current.err
	}
}
