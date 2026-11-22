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

package transport

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
)

// Config holds various options for establishing a transport.
//
// Config 是构建 http.RoundTripper 的声明式配置：认证字段之间互斥或按优先级
// 覆盖（如 BearerTokenFile 的实时内容优先于 BearerToken），而 TLS 结构与
// 函数字段都刻意做成可比较类型，以便整份配置能作为 transport 缓存键。
type Config struct {
	// UserAgent is an optional field that specifies the caller of this
	// request.
	UserAgent string

	// The base TLS configuration for this transport.
	TLS TLSConfig

	// Username and password for basic authentication
	Username string
	Password string `datapolicy:"password"`

	// Bearer token for authentication
	BearerToken string `datapolicy:"token"`

	// Path to a file containing a BearerToken.
	// If set, the contents are periodically read.
	// The last successfully read value takes precedence over BearerToken.
	BearerTokenFile string

	// Impersonate is the config that this Config will impersonate using
	Impersonate ImpersonationConfig

	// DisableCompression bypasses automatic GZip compression requests to the
	// server.
	DisableCompression bool

	// Transport may be used for custom HTTP behavior. This attribute may
	// not be specified with the TLS client certificate options. Use
	// WrapTransport for most client level operations.
	Transport http.RoundTripper

	// WrapTransport will be invoked for custom HTTP behavior after the
	// underlying transport is initialized (either the transport created
	// from TLSClientConfig, Transport, or http.DefaultTransport). The
	// config may layer other RoundTrippers on top of the returned
	// RoundTripper.
	//
	// A future release will change this field to an array. Use config.Wrap()
	// instead of setting this value directly.
	WrapTransport WrapperFunc

	// DialHolder specifies the dial function for creating unencrypted TCP connections.
	// This struct indirection is used to make transport configs cacheable.
	DialHolder *DialHolder

	// Proxy is the proxy func to be used for all requests made by this
	// transport. If Proxy is nil, http.ProxyFromEnvironment is used. If Proxy
	// returns a nil *URL, no proxy is used.
	//
	// socks5 proxying does not currently support spdy streaming endpoints.
	Proxy func(*http.Request) (*url.URL, error)
}

// DialHolder is used to make the wrapped function comparable so that it can be used as a map key.
//
// 函数值本身不可比较，包一层指针后 Config 可以整体比较，从而复用缓存的 transport。
type DialHolder struct {
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

// ImpersonationConfig has all the available impersonation options
//
// 这些字段会被编码为 Impersonate-User/Groups/Extra 等请求头，
// 用于以其他身份向 API Server 发起请求。
type ImpersonationConfig struct {
	// UserName matches user.Info.GetName()
	UserName string
	// UID matches user.Info.GetUID()
	UID string
	// Groups matches user.Info.GetGroups()
	Groups []string
	// Extra matches user.Info.GetExtra()
	Extra map[string][]string
}

// HasCA returns whether the configuration has a certificate authority or not.
//
// 内联数据与文件路径任一存在即视为已配置 CA。
func (c *Config) HasCA() bool {
	return len(c.TLS.CAData) > 0 || len(c.TLS.CAFile) > 0
}

// HasBasicAuth returns whether the configuration has basic authentication or not.
func (c *Config) HasBasicAuth() bool {
	return len(c.Username) != 0
}

// HasTokenAuth returns whether the configuration has token authentication or not.
//
// 只看 BearerToken/BearerTokenFile，两者都存在时由运行时决定优先级。
func (c *Config) HasTokenAuth() bool {
	return len(c.BearerToken) != 0 || len(c.BearerTokenFile) != 0
}

// HasCertAuth returns whether the configuration has certificate authentication or not.
//
// 证书与私钥必须成对出现才算有效的客户端证书认证。
func (c *Config) HasCertAuth() bool {
	return (len(c.TLS.CertData) != 0 || len(c.TLS.CertFile) != 0) && (len(c.TLS.KeyData) != 0 || len(c.TLS.KeyFile) != 0)
}

// HasCertCallback returns whether the configuration has certificate callback or not.
func (c *Config) HasCertCallback() bool {
	// 只判断 holder 是否存在，不调用回调；回调可能有副作用或开销。
	return c.TLS.GetCertHolder != nil
}

// Wrap adds a transport middleware function that will give the caller
// an opportunity to wrap the underlying http.RoundTripper prior to the
// first API call being made. The provided function is invoked after any
// existing transport wrappers are invoked.
//
// 通过组合已有 wrapper 实现链式叠加，新加的 wrapper 位于最内层。
func (c *Config) Wrap(fn WrapperFunc) {
	c.WrapTransport = Wrappers(c.WrapTransport, fn)
}

// TLSConfig holds the information needed to set up a TLS transport.
//
// 以 *Data 结尾的内联数据优先于对应的 *File 路径；Reload* 标志用于要求
// transport 在文件变化时重新加载，而不是只在启动时读取一次。
type TLSConfig struct {
	CAFile         string // Path of the PEM-encoded server trusted root certificates.
	CertFile       string // Path of the PEM-encoded client certificate.
	KeyFile        string // Path of the PEM-encoded client key.
	ReloadTLSFiles bool   // Set to indicate that the original config provided files, and that they should be reloaded.
	ReloadCAFiles  bool   // Set to indicate that CA files should be reloaded from disk.

	Insecure   bool   // Server should be accessed without verifying the certificate. For testing only.
	ServerName string // Override for the server name passed to the server for SNI and used to verify certificates.

	CAData   []byte // Bytes of the PEM-encoded server trusted root certificates. Supercedes CAFile.
	CertData []byte // Bytes of the PEM-encoded client certificate. Supercedes CertFile.
	KeyData  []byte // Bytes of the PEM-encoded client key. Supercedes KeyFile.

	// NextProtos is a list of supported application level protocols, in order of preference.
	// Used to populate tls.Config.NextProtos.
	// To indicate to the server http/1.1 is preferred over http/2, set to ["http/1.1", "h2"] (though the server is free to ignore that preference).
	// To use only http/1.1, set to ["http/1.1"].
	NextProtos []string

	// Callback that returns a TLS client certificate. CertData, CertFile, KeyData and KeyFile supercede this field.
	// This struct indirection is used to make transport configs cacheable.
	GetCertHolder *GetCertHolder
}

// GetCertHolder is used to make the wrapped function comparable so that it can be used as a map key.
//
// 回调每次握手都会被调用，可用于轮换客户端证书。
type GetCertHolder struct {
	GetCert func() (*tls.Certificate, error)
}
