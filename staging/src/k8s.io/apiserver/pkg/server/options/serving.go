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
	"context"
	"fmt"
	"net"
	"path"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	cliflag "k8s.io/component-base/cli/flag"
)

// SecureServingOptions 描述 HTTPS 端点的绑定地址、监听器、证书与 TLS
// 参数。它与 GeneratableKeyCert 配合，既支持显式证书，也支持自签名
// 证书的生成与落盘。
type SecureServingOptions struct {
	BindAddress net.IP
	// 设置 Listener 时忽略 BindPort，此时即使端口为 0 也提供 HTTPS。
	//
	// 显式提供 Listener 时以监听器实际端口为准，此字段被忽略。
	BindPort int
	// BindNetwork 是要绑定的网络类型，默认为 "tcp"，可选 "tcp4" 与 "tcp6"。
	// 取值为 "tcp"、"tcp4" 与 "tcp6"。
	BindNetwork string
	// DisableHTTP2Serving 表示不应启用 http2。
	DisableHTTP2Serving bool
	// Required 为 true 时 BindPort 不能为 0。
	Required bool
	// ExternalAddress 是对外公布的地址，即使 BindAddress 是环回地址也照常公布。
	// 默认取 BindAddress（若非环回），否则取第一个主机接口地址。
	//
	// 对外公布的地址：绑定到环回地址时仍可公布一个集群可达地址，
	// 用于生成证书 SAN 与向客户端广播 endpoint。
	ExternalAddress net.IP

	// Listener 是安全服务端的网络监听器。
	// Listener 与 BindAddress/BindPort/BindNetwork 二者只能设其一；
	// 设置 Listener 时使用它并忽略 BindAddress/BindPort/BindNetwork。
	Listener net.Listener

	// ServerCert 是用于提供 HTTPS 服务的 TLS 证书信息。
	ServerCert GeneratableKeyCert
	// SNICertKeys 是支持 SNI 的具名 CertKey，用于按域名提供证书。
	SNICertKeys []cliflag.NamedCertKey
	// CipherSuites 是服务端允许的加密套件列表。
	// 取值来自 tls 包常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	CipherSuites []string
	// CurvePreferences 是服务端允许的密钥交换机制集合，
	// 以 Go crypto/tls CurveID 数值指定。
	// 支持哪些值取决于所使用的 Go 版本。
	// 各 Go 版本支持的值见 https://pkg.go.dev/crypto/tls#CurveID。
	// 列表顺序会被忽略；Go 会按内部优先级从这些值中选择密钥交换机制。
	//
	// 留空时使用 Go 默认曲线。
	CurvePreferences []int32
	// MinTLSVersion 是支持的最低 TLS 版本。
	// 取值来自 tls 包常量（https://golang.org/pkg/crypto/tls/#pkg-constants）。
	MinTLSVersion string

	// HTTP2MaxStreamsPerConnection 是 api server 对每个客户端施加的流数量上限。
	// 值为 0 时使用 golang HTTP/2 支持提供的默认值。
	HTTP2MaxStreamsPerConnection int

	// PermitPortSharing 控制绑定端口时是否使用 SO_REUSEPORT，
	// 它允许多个实例绑定同一地址与端口。
	PermitPortSharing bool

	// PermitAddressSharing 控制绑定端口时是否使用 SO_REUSEADDR。
	PermitAddressSharing bool
}

// CertKey 是一对 PEM 证书/私钥文件路径。
type CertKey struct {
	// CertFile 是包含 PEM 编码证书（可能含完整证书链）的文件。
	CertFile string
	// KeyFile 是包含 CertFile 所指定证书的 PEM 编码私钥的文件。
	KeyFile string
}

// GeneratableKeyCert 描述服务端证书的来源优先级：显式文件 > 目录中
// 已存在的文件 > 新生成并落盘 > 内存中生成。
type GeneratableKeyCert struct {
	// CertKey 允许设置要使用的显式证书/私钥文件。
	CertKey CertKey

	// CertDirectory 指定在 CertFile/KeyFile 未显式设置时，生成证书写入的目录。
	// PairName 用于确定 CertDirectory 内的文件名。
	// 若 CertDirectory 与 PairName 都未设置，则生成内存证书。
	CertDirectory string
	// PairName 与 CertDirectory 一起决定证书与私钥的文件名，
	// 即 CertDirectory/PairName.crt 与 CertDirectory/PairName.key。
	PairName string

	// 若 CertFile/KeyFile 未显式设置且 CertDirectory/PairName 未设置，
	GeneratedCert dynamiccertificates.CertKeyContentProvider

	// FixtureDirectory 是包含测试固定文件的目录，用于避免测试期间重复生成证书。
	// 格式为：
	// <host>_<ip>-<ip>_<alternateDNS>-<alternateDNS>.crt
	// <host>_<ip>-<ip>_<alternateDNS>-<alternateDNS>.key
	FixtureDirectory string
}

// NewSecureServingOptions 返回默认监听 0.0.0.0:443、证书位于
// apiserver.local.config/certificates 的选项。
func NewSecureServingOptions() *SecureServingOptions {
	return &SecureServingOptions{
		BindAddress: netutils.ParseIPSloppy("0.0.0.0"),
		BindPort:    443,
		ServerCert: GeneratableKeyCert{
			PairName:      "apiserver",
			CertDirectory: "apiserver.local.config/certificates",
		},
	}
}

// DefaultExternalAddress 返回对外公布的地址：优先显式配置的具体地址，
// 否则从绑定地址推导本机可达地址（跳过 unspecified 0.0.0.0）。
func (s *SecureServingOptions) DefaultExternalAddress() (net.IP, error) {
	if s.ExternalAddress != nil && !s.ExternalAddress.IsUnspecified() {
		return s.ExternalAddress, nil
	}
	return utilnet.ResolveBindAddress(s.BindAddress)
}

// Validate 校验端口范围与证书来源互斥。Required 模式下端口不能为 0，
// 避免配置意外关闭 HTTPS；显式证书文件与内存证书同时设置属于冲突。
func (s *SecureServingOptions) Validate() []error {
	if s == nil {
		return nil
	}

	errors := []error{}

	if s.Required && s.BindPort < 1 || s.BindPort > 65535 {
		errors = append(errors, fmt.Errorf("--secure-port %v must be between 1 and 65535, inclusive. It cannot be turned off with 0", s.BindPort))
	} else if s.BindPort < 0 || s.BindPort > 65535 {
		errors = append(errors, fmt.Errorf("--secure-port %v must be between 0 and 65535, inclusive. 0 for turning off secure port", s.BindPort))
	}

	if (len(s.ServerCert.CertKey.CertFile) != 0 || len(s.ServerCert.CertKey.KeyFile) != 0) && s.ServerCert.GeneratedCert != nil {
		errors = append(errors, fmt.Errorf("cert/key file and in-memory certificate cannot both be set"))
	}

	return errors
}

// AddFlags 注册 TLS 相关命令行参数。加密套件与 TLS 版本的可选值从
// cliflag 动态生成，保证帮助文本与 Go 版本支持范围一致。
func (s *SecureServingOptions) AddFlags(fs *pflag.FlagSet) {
	if s == nil {
		return
	}

	fs.IPVar(&s.BindAddress, "bind-address", s.BindAddress, ""+
		"The IP address on which to listen for the --secure-port port. The "+
		"associated interface(s) must be reachable by the rest of the cluster, and by CLI/web "+
		"clients. If blank or an unspecified address (0.0.0.0 or ::), all interfaces and IP address families will be used.")

	desc := "The port on which to serve HTTPS with authentication and authorization."
	if s.Required {
		desc += " It cannot be switched off with 0."
	} else {
		desc += " If 0, don't serve HTTPS at all."
	}
	fs.IntVar(&s.BindPort, "secure-port", s.BindPort, desc)

	fs.BoolVar(&s.DisableHTTP2Serving, "disable-http2-serving", s.DisableHTTP2Serving,
		"If true, HTTP2 serving will be disabled [default=false]")

	fs.StringVar(&s.ServerCert.CertDirectory, "cert-dir", s.ServerCert.CertDirectory, ""+
		"The directory where the TLS certs are located. "+
		"If --tls-cert-file and --tls-private-key-file are provided, this flag will be ignored.")

	fs.StringVar(&s.ServerCert.CertKey.CertFile, "tls-cert-file", s.ServerCert.CertKey.CertFile, ""+
		"File containing the default x509 Certificate for HTTPS. (CA cert, if any, concatenated "+
		"after server cert). If HTTPS serving is enabled, and --tls-cert-file and "+
		"--tls-private-key-file are not provided, a self-signed certificate and key "+
		"are generated for the public address and saved to the directory specified by --cert-dir.")

	fs.StringVar(&s.ServerCert.CertKey.KeyFile, "tls-private-key-file", s.ServerCert.CertKey.KeyFile,
		"File containing the default x509 private key matching --tls-cert-file.")

	tlsCipherPreferredValues := cliflag.PreferredTLSCipherNames()
	tlsCipherInsecureValues := cliflag.InsecureTLSCipherNames()
	fs.StringSliceVar(&s.CipherSuites, "tls-cipher-suites", s.CipherSuites,
		"Comma-separated list of cipher suites for the server. "+
			"If omitted, the default Go cipher suites will be used. \n"+
			"Preferred values: "+strings.Join(tlsCipherPreferredValues, ", ")+". \n"+
			"Insecure values: "+strings.Join(tlsCipherInsecureValues, ", ")+".")

	fs.Int32SliceVar(&s.CurvePreferences, "tls-curve-preferences", s.CurvePreferences,
		"Comma-separated list of numeric Go crypto/tls CurveID values, "+
			"as the allowed key exchange mechanisms for the server. "+
			"The supported values depend on the Go version used. "+
			"See https://pkg.go.dev/crypto/tls#CurveID for values supported for each Go version. "+
			"The order of the list is ignored, and key exchange mechanisms are chosen "+
			"by Go from this list using an internal preference order. "+
			"If omitted, the default Go curves will be used.")

	tlsPossibleVersions := cliflag.TLSPossibleVersions()
	fs.StringVar(&s.MinTLSVersion, "tls-min-version", s.MinTLSVersion,
		"Minimum TLS version supported. "+
			"Possible values: "+strings.Join(tlsPossibleVersions, ", "))

	fs.Var(cliflag.NewNamedCertKeyArray(&s.SNICertKeys), "tls-sni-cert-key", ""+
		"A pair of x509 certificate and private key file paths, optionally suffixed with a list of "+
		"domain patterns which are fully qualified domain names, possibly with prefixed wildcard "+
		"segments. The domain patterns also allow IP addresses, but IPs should only be used if "+
		"the apiserver has visibility to the IP address requested by a client. "+
		"If no domain patterns are provided, the names of the certificate are "+
		"extracted. Non-wildcard matches trump over wildcard matches, explicit domain patterns "+
		"trump over extracted names. For multiple key/certificate pairs, use the "+
		"--tls-sni-cert-key multiple times. "+
		"Examples: \"example.crt,example.key\" or \"foo.crt,foo.key:*.foo.com,foo.com\".")

	fs.IntVar(&s.HTTP2MaxStreamsPerConnection, "http2-max-streams-per-connection", s.HTTP2MaxStreamsPerConnection, ""+
		"The limit that the server gives to clients for "+
		"the maximum number of streams in an HTTP/2 connection. "+
		"Zero means to use golang's default.")

	fs.BoolVar(&s.PermitPortSharing, "permit-port-sharing", s.PermitPortSharing,
		"If true, SO_REUSEPORT will be used when binding the port, which allows "+
			"more than one instance to bind on the same address and port. [default=false]")

	fs.BoolVar(&s.PermitAddressSharing, "permit-address-sharing", s.PermitAddressSharing,
		"If true, SO_REUSEADDR will be used when binding the port. This allows binding "+
			"to wildcard IPs like 0.0.0.0 and specific IPs in parallel, and it avoids waiting "+
			"for the kernel to release sockets in TIME_WAIT state. [default=false]")
}

// ApplyTo 在服务端配置中填入服务信息。
//
// ApplyTo 创建或接管监听器并装配 SecureServingInfo。端口为 0 且未提供
// Listener 时表示不启用 HTTPS，直接返回。显式 Listener 必须是 TCP，
// 否则无法推导端口与地址。
func (s *SecureServingOptions) ApplyTo(config **server.SecureServingInfo) error {
	if s == nil {
		return nil
	}
	if s.BindPort <= 0 && s.Listener == nil {
		return nil
	}

	if s.Listener == nil {
		var err error
		addr := net.JoinHostPort(s.BindAddress.String(), strconv.Itoa(s.BindPort))

		c := net.ListenConfig{}

		ctls := multipleControls{}
		// SO_REUSEPORT/SO_REUSEADDR 需要按需组合；两者都关闭时不设置
		// Control，保持内核默认绑定语义。
		if s.PermitPortSharing {
			ctls = append(ctls, permitPortReuse)
		}
		if s.PermitAddressSharing {
			ctls = append(ctls, permitAddressReuse)
		}
		if len(ctls) > 0 {
			c.Control = ctls.Control
		}

		s.Listener, s.BindPort, err = CreateListener(s.BindNetwork, addr, c)
		if err != nil {
			return fmt.Errorf("failed to create listener: %v", err)
		}
	} else {
		// 端口可能由内核分配（例如 :0），必须回填实际值：后续生成证书
		// SAN 与对外公告都依赖它。
		if _, ok := s.Listener.Addr().(*net.TCPAddr); !ok {
			return fmt.Errorf("failed to parse ip and port from listener")
		}
		s.BindPort = s.Listener.Addr().(*net.TCPAddr).Port
		s.BindAddress = s.Listener.Addr().(*net.TCPAddr).IP
	}

	*config = &server.SecureServingInfo{
		Listener:                     s.Listener,
		HTTP2MaxStreamsPerConnection: s.HTTP2MaxStreamsPerConnection,
		DisableHTTP2:                 s.DisableHTTP2Serving,
	}
	c := *config

	serverCertFile, serverKeyFile := s.ServerCert.CertKey.CertFile, s.ServerCert.CertKey.KeyFile
	// 加载主证书 *原始说明，截至 2023-08-18*

	/*
		kubernetes 客户端与 apiserver 之间的双向（2-way）x509：

			>1. apiserver 把自身证书及公钥发送给客户端
			2. 客户端用集群 CA 数据校验 apiserver 发来的证书
			3. 客户端把客户端证书及公钥发送给 apiserver
			4. apiserver 用集群 CA 数据校验客户端发来的证书

			说明：
				在此处，apiserver 证书与公钥数据（连同私钥）被加载到 server.SecureServingInfo，
				以便客户端在后续第 2 步握手时校验 apiserver 证书。

				该阶段相关参数：
				--tls-cert-file string  包含 HTTPS 默认 x509 证书的文件。
					（若有 CA 证书，则拼接在服务端证书之后）。若启用了 HTTPS 服务，且
					未提供 --tls-cert-file 与 --tls-private-key-file，则会为公开地址生成自签名证书
					与私钥，并保存到 --cert-dir 指定的目录。
				--tls-private-key-file string  包含与 --tls-cert-file 匹配的默认 x509 私钥的文件。

				（可通过 "kube-apiserver --help" 命令获取）
				（由 @deads2k 建议）

			另见：
				- 第 2 步：staging/src/k8s.io/client-go/transport/transport.go
				- 第 3 步：staging/src/k8s.io/client-go/transport/transport.go
				- 第 4 步：staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go
	*/

	// 显式文件优先；文件内容由 dynamiccertificates 监听并在变化时热更新，
	// 因此证书轮换无需重启。否则使用此前生成的内存证书。
	if len(serverCertFile) != 0 || len(serverKeyFile) != 0 {
		var err error
		c.Cert, err = dynamiccertificates.NewDynamicServingContentFromFiles("serving-cert", serverCertFile, serverKeyFile)
		if err != nil {
			return err
		}
	} else if s.ServerCert.GeneratedCert != nil {
		c.Cert = s.ServerCert.GeneratedCert
	}

	if len(s.CipherSuites) != 0 {
		cipherSuites, err := cliflag.TLSCipherSuites(s.CipherSuites)
		if err != nil {
			return err
		}
		c.CipherSuites = cipherSuites
	}

	if len(s.CurvePreferences) != 0 {
		curvePreferences, err := cliflag.TLSCurvePreferences(s.CurvePreferences)
		if err != nil {
			return err
		}
		c.CurvePreferences = curvePreferences
	}

	var err error
	c.MinTLSVersion, err = cliflag.TLSVersion(s.MinTLSVersion)
	if err != nil {
		return err
	}

	// 加载 SNI 证书
	// SNI 证书同样使用动态提供器，支持按域名热更新。
	namedTLSCerts := make([]dynamiccertificates.SNICertKeyContentProvider, 0, len(s.SNICertKeys))
	for _, nck := range s.SNICertKeys {
		tlsCert, err := dynamiccertificates.NewDynamicSNIContentFromFiles("sni-serving-cert", nck.CertFile, nck.KeyFile, nck.Names...)
		namedTLSCerts = append(namedTLSCerts, tlsCert)
		if err != nil {
			return fmt.Errorf("failed to load SNI cert and key: %v", err)
		}
	}
	c.SNICerts = namedTLSCerts

	return nil
}

// MaybeDefaultWithSelfSignedCerts 在未显式提供证书时生成自签名证书。
// 若 CertDirectory/PairName 已配置且文件可读则复用已有文件；否则生成
// 并写入目录（目录可用时）或仅保存在内存。自签名仅用于 bootstrap 与
// 测试，生产部署应提供由集群 CA 签发的证书。
func (s *SecureServingOptions) MaybeDefaultWithSelfSignedCerts(publicAddress string, alternateDNS []string, alternateIPs []net.IP) error {
	if s == nil || (s.BindPort == 0 && s.Listener == nil) {
		return nil
	}
	keyCert := &s.ServerCert.CertKey
	if len(keyCert.CertFile) != 0 || len(keyCert.KeyFile) != 0 {
		return nil
	}

	canReadCertAndKey := false
	if len(s.ServerCert.CertDirectory) > 0 {
		if len(s.ServerCert.PairName) == 0 {
			return fmt.Errorf("PairName is required if CertDirectory is set")
		}
		keyCert.CertFile = path.Join(s.ServerCert.CertDirectory, s.ServerCert.PairName+".crt")
		keyCert.KeyFile = path.Join(s.ServerCert.CertDirectory, s.ServerCert.PairName+".key")
		if canRead, err := certutil.CanReadCertAndKey(keyCert.CertFile, keyCert.KeyFile); err != nil {
			return err
		} else {
			canReadCertAndKey = canRead
		}
	}

	if !canReadCertAndKey {
		// 把绑定地址或 localhost 加入合法备用名称。
		// 绑定到 unspecified 地址时客户端通常通过 localhost 访问，否则把
		// 具体绑定地址加入 SAN。
		if s.BindAddress.IsUnspecified() {
			alternateDNS = append(alternateDNS, "localhost")
		} else {
			alternateIPs = append(alternateIPs, s.BindAddress)
		}

		if cert, key, err := certutil.GenerateSelfSignedCertKeyWithFixtures(publicAddress, alternateIPs, alternateDNS, s.ServerCert.FixtureDirectory); err != nil {
			return fmt.Errorf("unable to generate self signed cert: %v", err)
		} else if len(keyCert.CertFile) > 0 && len(keyCert.KeyFile) > 0 {
			// 有目录则落盘，后续重启可直接复用，避免每次启动生成新证书
			// 导致已信任旧证书的客户端失效。
			if err := certutil.WriteCert(keyCert.CertFile, cert); err != nil {
				return err
			}
			if err := keyutil.WriteKey(keyCert.KeyFile, key); err != nil {
				return err
			}
			klog.Infof("Generated self-signed cert (%s, %s)", keyCert.CertFile, keyCert.KeyFile)
		} else {
			s.ServerCert.GeneratedCert, err = dynamiccertificates.NewStaticCertKeyContent("Generated self signed cert", cert, key)
			if err != nil {
				return err
			}
			klog.Infof("Generated self-signed cert in-memory")
		}
	}

	return nil
}

// CreateListener 按网络类型监听地址并返回实际端口。默认网络为 tcp；
// 非 TCP 监听器无法提供端口信息，因此关闭并报错。
func CreateListener(network, addr string, config net.ListenConfig) (net.Listener, int, error) {
	if len(network) == 0 {
		network = "tcp"
	}

	ln, err := config.Listen(context.TODO(), network, addr)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to listen on %v: %v", addr, err)
	}

	// 获取端口
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		ln.Close()
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}

// multipleControls 允许把多个 socket 选项（如 SO_REUSEPORT 与
// SO_REUSEADDR）串联到 net.ListenConfig.Control。
type multipleControls []func(network, addr string, conn syscall.RawConn) error

// Control 依次应用每个控制函数；任一失败立即返回，避免半配置的 socket。
func (mcs multipleControls) Control(network, addr string, conn syscall.RawConn) error {
	for _, c := range mcs {
		if err := c(network, addr, conn); err != nil {
			return err
		}
	}
	return nil
}
