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

// Config 持有建立传输层所需的各项配置。
//
// Config 是构建 http.RoundTripper 的声明式配置：认证字段之间互斥或按优先级
// 覆盖（如 BearerTokenFile 的实时内容优先于 BearerToken），而 TLS 结构与
// 函数字段都刻意做成可比较类型，以便整份配置能作为 transport 缓存键。
type Config struct {
	// UserAgent 可选，用于标识发起请求的调用方。
	UserAgent string

	// 该传输层使用的基础 TLS 配置。
	TLS TLSConfig

	// Basic 认证的用户名与密码。
	Username string
	Password string `datapolicy:"password"`

	// 用于认证的 Bearer token。
	BearerToken string `datapolicy:"token"`

	// BearerTokenFile 指向包含 Bearer token 的文件；设置后内容会被定期读取，
	// 最近一次成功读取的值优先于 BearerToken。
	BearerTokenFile string

	// Impersonate 指定本配置要模拟的身份。
	Impersonate ImpersonationConfig

	// DisableCompression 关闭到服务端的自动 GZip 压缩请求。
	DisableCompression bool

	// Transport 用于自定义 HTTP 行为，不能与 TLS 客户端证书选项同时指定；
	// 大多数客户端级别的定制应使用 WrapTransport。
	Transport http.RoundTripper

	// WrapTransport 在底层 transport 初始化后调用，用于自定义 HTTP 行为；
	// 配置可在返回的 RoundTripper 之上继续叠加其他 RoundTripper。
	//
	// 未来版本会把该字段改为数组；请不要直接赋值，而应使用 config.Wrap()。
	WrapTransport WrapperFunc

	// DialHolder 指定创建未加密 TCP 连接的 dial 函数；这层结构体包装使传输层配置
	// 可比较，从而能作为 map 键。
	DialHolder *DialHolder

	// Proxy 是本客户端所有请求使用的代理函数；返回 nil *URL 表示不使用代理。
	//
	// socks5 代理目前不支持 spdy streaming 端点。
	Proxy func(*http.Request) (*url.URL, error)
}

// DialHolder 包装函数使其可比较，从而能作为 map 键。
//
// 函数值本身不可比较，包一层指针后 Config 可以整体比较，从而复用缓存的 transport。
type DialHolder struct {
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

// ImpersonationConfig 包含所有可用的模拟身份选项。
//
// 这些字段会被编码为 Impersonate-User/Groups/Extra 等请求头，
// 用于以其他身份向 API Server 发起请求。
type ImpersonationConfig struct {
	// UserName 对应 user.Info.GetName()。
	UserName string
	// UID 对应 user.Info.GetUID()。
	UID string
	// Groups 对应 user.Info.GetGroups()。
	Groups []string
	// Extra 对应 user.Info.GetExtra()。
	Extra map[string][]string
}

// HasCA 返回配置是否带有证书颁发机构。
//
// 内联数据与文件路径任一存在即视为已配置 CA。
func (c *Config) HasCA() bool {
	return len(c.TLS.CAData) > 0 || len(c.TLS.CAFile) > 0
}

// HasBasicAuth 返回配置是否启用 basic 认证。
func (c *Config) HasBasicAuth() bool {
	return len(c.Username) != 0
}

// HasTokenAuth 返回配置是否启用 token 认证。
//
// 只看 BearerToken/BearerTokenFile，两者都存在时由运行时决定优先级。
func (c *Config) HasTokenAuth() bool {
	return len(c.BearerToken) != 0 || len(c.BearerTokenFile) != 0
}

// HasCertAuth 返回配置是否启用证书认证。
//
// 证书与私钥必须成对出现才算有效的客户端证书认证。
func (c *Config) HasCertAuth() bool {
	return (len(c.TLS.CertData) != 0 || len(c.TLS.CertFile) != 0) && (len(c.TLS.KeyData) != 0 || len(c.TLS.KeyFile) != 0)
}

// HasCertCallback 返回配置是否设置了证书回调。
func (c *Config) HasCertCallback() bool {
	// 只判断 holder 是否存在，不调用回调；回调可能有副作用或开销。
	return c.TLS.GetCertHolder != nil
}

// Wrap 添加一个传输层中间件函数，让调用方在初次 API 调用前自定义客户端传输层；
// 它在所有已有 wrapper 之后被调用。
//
// 通过组合已有 wrapper 实现链式叠加，新加的 wrapper 位于最内层。
func (c *Config) Wrap(fn WrapperFunc) {
	c.WrapTransport = Wrappers(c.WrapTransport, fn)
}

// TLSConfig 持有建立 TLS 传输层所需的信息。
//
// 以 *Data 结尾的内联数据优先于对应的 *File 路径；Reload* 标志用于要求
// transport 在文件变化时重新加载，而不是只在启动时读取一次。
type TLSConfig struct {
	CAFile         string // PEM 编码的服务端可信根证书文件路径。
	CertFile       string // PEM 编码的客户端证书文件路径。
	KeyFile        string // PEM 编码的客户端私钥文件路径。
	ReloadTLSFiles bool   // 表示原始配置提供了文件，应重新加载。
	ReloadCAFiles  bool   // 表示 CA 文件应从磁盘重新加载。

	Insecure   bool   // 不校验证书直接访问服务端，仅供测试使用。
	ServerName string // 覆盖传给服务端的 SNI 主机名及证书校验所用的名字。

	CAData   []byte // PEM 编码的服务端可信根证书字节，优先于 CAFile。
	CertData []byte // PEM 编码的客户端证书字节，优先于 CertFile。
	KeyData  []byte // PEM 编码的客户端私钥字节，优先于 KeyFile。

	// NextProtos 是按优先级排列的应用层协议列表，用于填充 tls.Config.NextProtos。
	// 要告知服务端偏好 http/1.1 而非 http/2，可设为 ["http/1.1", "h2"]（服务端可忽略该偏好）。
	// 仅使用 http/1.1 时设为 ["http/1.1"]。
	NextProtos []string

	// 返回 TLS 客户端证书的回调；CertData、CertFile、KeyData、KeyFile 优先于该字段。
	// 这层结构体包装使传输层配置可比较，从而能作为 map 键。
	GetCertHolder *GetCertHolder
}

// GetCertHolder 包装函数使其可比较，从而能作为 map 键。
//
// 回调每次握手都会被调用，可用于轮换客户端证书。
type GetCertHolder struct {
	GetCert func() (*tls.Certificate, error)
}
