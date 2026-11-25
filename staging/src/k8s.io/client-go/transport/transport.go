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

// New returns an http.RoundTripper that will provide the authentication
// or transport level security defined by the provided Config.
func New(config *Config) (http.RoundTripper, error) {
	metrics.EnsureRegistered()
	// Set transport level security
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

// TLSConfigFor returns a tls.Config that will provide the transport level security defined
// by the provided Config. Will return nil if no transport level security is requested.
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
		// Can't use SSLv3 because of POODLE and BEAST
		// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
		// Can't use TLSv1.1 because of RC4 cipher usage
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
	// Treat cert as static if either key or cert was data, not a file
	if c.HasCertAuth() && !c.TLS.ReloadTLSFiles {
		// If key/cert were provided, verify them before setting up
		// tlsConfig.GetClientCertificate.
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
			// Note: static key/cert data always take precedence over cert
			// callback.
			if staticCert != nil {
				return staticCert, nil
			}
			// key/cert files lead to ReloadTLSFiles being set - takes precedence over cert callback
			if dynamicCertLoader != nil {
				return dynamicCertLoader()
			}
			if c.HasCertCallback() {
				cert, err := c.TLS.GetCertHolder.GetCert()
				if err != nil {
					return nil, err
				}
				// GetCert may return empty value, meaning no cert.
				if cert != nil {
					return cert, nil
				}
			}

			// Both c.TLS.CertData/KeyData were unset and GetCert didn't return
			// anything. Return an empty tls.Certificate, no client cert will
			// be sent to the server.
			return &tls.Certificate{}, nil
		}
	}

	return tlsConfig, nil
}

// loadTLSFiles copies the data from the CertFile, KeyFile, and CAFile fields into the CertData,
// KeyData, and CAFile fields, or returns an error. If no error is returned, all three fields are
// either populated or were empty to start.
//
// 同时决定是否启用文件热加载：只有“纯文件来源”才安全，因为 Data 与 File 混用时无法判断优先级。
func loadTLSFiles(c *Config) error {
	// Check that we are purely loading CA from file
	if clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowCARotation) {
		if len(c.TLS.CAFile) > 0 && len(c.TLS.CAData) == 0 {
			c.TLS.ReloadCAFiles = true
		}
	} else if c.TLS.ReloadCAFiles {
		return fmt.Errorf("ReloadCAFiles=true requires ClientsAllowCARotation to be enabled")
	}

	// Check that we are purely loading certs and keys from files
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

// dataFromSliceOrFile returns data from the slice (if non-empty), or from the file,
// or an error if an error occurred reading the file
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
	// What we really want is a copy of x509.systemRootsPool, but that isn't exposed.  It's difficult to build (see the go
	// code for a look at the platform specific insanity), so we'll use the fact that RootCAs == nil gives us the system values
	// It doesn't allow trusting either/or, but hopefully that won't be an issue
	if len(caData) == 0 {
		// When the ClientsAllowCARotation feature gate is enabled, it returns an empty but non-nil pool.
		// This ensures we don't fall back to system roots when a user explicitly points CAFile to a zero-byte file.
		if clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowCARotation) {
			return x509.NewCertPool(), nil
		}
		return nil, nil
	}

	// if we have caData, use it
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

// WrapperFunc wraps an http.RoundTripper when a new transport
// is created for a client, allowing per connection behavior
// to be injected.
type WrapperFunc func(rt http.RoundTripper) http.RoundTripper

// Wrappers accepts any number of wrappers and returns a wrapper
// function that is the equivalent of calling each of them in order. Nil
// values are ignored, which makes this function convenient for incrementally
// wrapping a function.
func Wrappers(fns ...WrapperFunc) WrapperFunc {
	if len(fns) == 0 {
		return nil
	}
	// optimize the common case of wrapping a possibly nil transport wrapper
	// with an additional wrapper
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

// ContextCanceller prevents new requests after the provided context is finished.
// err is returned when the context is closed, allowing the caller to provide a context
// appropriate error.
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

// isStale returns true when this cache entry is too old to be usable
func (c *certificateCacheEntry) isStale() bool {
	return time.Since(c.birth) > time.Second
}

// newCertificateCacheEntry 立即读取证书与私钥；错误也缓存，避免坏文件导致每秒重试风暴。
func newCertificateCacheEntry(certFile, keyFile string) certificateCacheEntry {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	return certificateCacheEntry{cert: &cert, err: err, birth: time.Now()}
}

// cachingCertificateLoader ensures that we don't hammer the filesystem when opening many connections
// the underlying cert files are read at most once every second
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
