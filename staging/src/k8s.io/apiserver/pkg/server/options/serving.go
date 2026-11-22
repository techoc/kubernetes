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
	// BindPort is ignored when Listener is set, will serve https even with 0.
	//
	// 显式提供 Listener 时以监听器实际端口为准，此字段被忽略。
	BindPort int
	// BindNetwork is the type of network to bind to - defaults to "tcp", accepts "tcp",
	// "tcp4", and "tcp6".
	BindNetwork string
	// DisableHTTP2Serving indicates that http2 serving should not be enabled.
	DisableHTTP2Serving bool
	// Required set to true means that BindPort cannot be zero.
	Required bool
	// ExternalAddress is the address advertised, even if BindAddress is a loopback. By default this
	// is set to BindAddress if the later no loopback, or to the first host interface address.
	//
	// 对外公布的地址：绑定到环回地址时仍可公布一个集群可达地址，
	// 用于生成证书 SAN 与向客户端广播 endpoint。
	ExternalAddress net.IP

	// Listener is the secure server network listener.
	// either Listener or BindAddress/BindPort/BindNetwork is set,
	// if Listener is set, use it and omit BindAddress/BindPort/BindNetwork.
	Listener net.Listener

	// ServerCert is the TLS cert info for serving secure traffic
	ServerCert GeneratableKeyCert
	// SNICertKeys are named CertKeys for serving secure traffic with SNI support.
	SNICertKeys []cliflag.NamedCertKey
	// CipherSuites is the list of allowed cipher suites for the server.
	// Values are from tls package constants (https://golang.org/pkg/crypto/tls/#pkg-constants).
	CipherSuites []string
	// CurvePreferences is the set of allowed key exchange mechanisms for the server,
	// specified as numeric Go crypto/tls CurveID values.
	// The supported values depend on the Go version used.
	// See https://pkg.go.dev/crypto/tls#CurveID for values supported for each Go version.
	// The order of the list is ignored, and key exchange mechanisms are
	// chosen by Go from this list using an internal preference order.
	// If empty, the default Go curves will be used.
	CurvePreferences []int32
	// MinTLSVersion is the minimum TLS version supported.
	// Values are from tls package constants (https://golang.org/pkg/crypto/tls/#pkg-constants).
	MinTLSVersion string

	// HTTP2MaxStreamsPerConnection is the limit that the api server imposes on each client.
	// A value of zero means to use the default provided by golang's HTTP/2 support.
	HTTP2MaxStreamsPerConnection int

	// PermitPortSharing controls if SO_REUSEPORT is used when binding the port, which allows
	// more than one instance to bind on the same address and port.
	PermitPortSharing bool

	// PermitAddressSharing controls if SO_REUSEADDR is used when binding the port.
	PermitAddressSharing bool
}

// CertKey 是一对 PEM 证书/私钥文件路径。
type CertKey struct {
	// CertFile is a file containing a PEM-encoded certificate, and possibly the complete certificate chain
	CertFile string
	// KeyFile is a file containing a PEM-encoded private key for the certificate specified by CertFile
	KeyFile string
}

// GeneratableKeyCert 描述服务端证书的来源优先级：显式文件 > 目录中
// 已存在的文件 > 新生成并落盘 > 内存中生成。
type GeneratableKeyCert struct {
	// CertKey allows setting an explicit cert/key file to use.
	CertKey CertKey

	// CertDirectory specifies a directory to write generated certificates to if CertFile/KeyFile aren't explicitly set.
	// PairName is used to determine the filenames within CertDirectory.
	// If CertDirectory and PairName are not set, an in-memory certificate will be generated.
	CertDirectory string
	// PairName is the name which will be used with CertDirectory to make a cert and key filenames.
	// It becomes CertDirectory/PairName.crt and CertDirectory/PairName.key
	PairName string

	// GeneratedCert holds an in-memory generated certificate if CertFile/KeyFile aren't explicitly set, and CertDirectory/PairName are not set.
	GeneratedCert dynamiccertificates.CertKeyContentProvider

	// FixtureDirectory is a directory that contains test fixture used to avoid regeneration of certs during tests.
	// The format is:
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

// ApplyTo fills up serving information in the server configuration.
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
	// load main cert *original description until 2023-08-18*

	/*
		kubernetes mutual (2-way) x509 between client and apiserver:

			>1. apiserver sending its apiserver certificate along with its publickey to client
			2. client verifies the apiserver certificate sent against its cluster certificate authority data
			3. client sending its client certificate along with its public key to the apiserver
			4. apiserver verifies the client certificate sent against its cluster certificate authority data

			description:
				here, with this block,
				apiserver certificate and pub key data (along with priv key)get loaded into server.SecureServingInfo
				for client to later in the step 2 verify the apiserver certificate during the handshake
				when making a request

			normal args related to this stage:
				--tls-cert-file string  File containing the default x509 Certificate for HTTPS.
					(CA cert, if any, concatenated after server cert). If HTTPS serving is enabled, and
					--tls-cert-file and --tls-private-key-file are not provided, a self-signed certificate
					and key are generated for the public address and saved to the directory specified by
					--cert-dir
				--tls-private-key-file string  File containing the default x509 private key matching --tls-cert-file.

				(retrievable from "kube-apiserver --help" command)
				(suggested by @deads2k)

			see also:
				- for the step 2, see: staging/src/k8s.io/client-go/transport/transport.go
				- for the step 3, see: staging/src/k8s.io/client-go/transport/transport.go
				- for the step 4, see: staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go
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

	// load SNI certs
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
		// add either the bind address or localhost to the valid alternates
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

	// get port
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
