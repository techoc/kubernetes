/*
Copyright 2022 The Kubernetes Authors.

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
	"strconv"
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/cloud-provider/config"
	cliflag "k8s.io/component-base/cli/flag"
	netutils "k8s.io/utils/net"
)

const (
	// CloudControllerManagerWebhookPort 是 CCM webhook 服务的默认监听端口。
	// 中文说明：与 CCM 主指标端口区分开，避免端口冲突；ProviderDefaults 未指定时使用。
	CloudControllerManagerWebhookPort = 10260
)

// WebhookOptions 保存 webhook 的启用/禁用列表。
type WebhookOptions struct {
	// Webhooks is the list of webhook names that should be enabled or disabled
	Webhooks []string
}

// NewWebhookOptions 返回空的 webhook 选项。
func NewWebhookOptions() *WebhookOptions {
	o := &WebhookOptions{}
	return o
}

// AddFlags 注册 --webhooks 参数。
//
// 中文说明：帮助文本里带上全部已知 webhook 与默认禁用列表，用户无需翻代码
// 就能构造合法取值。
func (o *WebhookOptions) AddFlags(fs *pflag.FlagSet, allWebhooks, disabledByDefaultWebhooks []string) {
	fs.StringSliceVar(&o.Webhooks, "webhooks", o.Webhooks, fmt.Sprintf(""+
		"A list of webhooks to enable. '*' enables all on-by-default webhooks, 'foo' enables the webhook "+
		"named 'foo', '-foo' disables the webhook named 'foo'.\nAll webhooks: %s\nDisabled-by-default webhooks: %s",
		strings.Join(allWebhooks, ", "), strings.Join(disabledByDefaultWebhooks, ", ")))
}

// Validate 检查用户指定的 webhook 名称是否都存在。
//
// 中文说明：默认禁用列表同样参与校验，因为它们最终也会被启用；'*' 是通配符，
// 不需要对应具体 webhook。
func (o *WebhookOptions) Validate(allWebhooks, disabledByDefaultWebhooks []string) []error {
	allErrors := []error{}

	allWebhooksSet := sets.NewString(allWebhooks...)
	toValidate := sets.NewString(o.Webhooks...)
	toValidate.Insert(disabledByDefaultWebhooks...)
	for _, webhook := range toValidate.List() {
		if webhook == "*" {
			continue
		}
		// '-' 前缀只表示禁用，校验时去掉前缀再比对名称。
		webhook = strings.TrimPrefix(webhook, "-")
		if !allWebhooksSet.Has(webhook) {
			allErrors = append(allErrors, fmt.Errorf("%q is not in the list of known webhooks", webhook))
		}
	}

	return allErrors
}

// ApplyTo 把 webhook 选项写入组件配置。
func (o *WebhookOptions) ApplyTo(cfg *config.WebhookConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.Webhooks = o.Webhooks

	return nil
}

// WebhookServingOptions 封装 webhook HTTPS 服务的监听与证书配置。
type WebhookServingOptions struct {
	*apiserveroptions.SecureServingOptions
}

// NewWebhookServingOptions 依据云厂商默认值构造 webhook 服务选项。
//
// 中文说明：未提供默认值时回退到 0.0.0.0:10260；证书目录留空表示由
// --webhook-tls-cert-file/--webhook-tls-private-key-file 或自签名生成决定。
func NewWebhookServingOptions(defaults ProviderDefaults) *WebhookServingOptions {
	var (
		bindAddress net.IP
		bindPort    int
	)

	if defaults.WebhookBindAddress != nil {
		bindAddress = *defaults.WebhookBindAddress
	} else {
		bindAddress = netutils.ParseIPSloppy("0.0.0.0")
	}

	if defaults.WebhookBindPort != nil {
		bindPort = *defaults.WebhookBindPort
	} else {
		bindPort = CloudControllerManagerWebhookPort
	}

	return &WebhookServingOptions{
		SecureServingOptions: &apiserveroptions.SecureServingOptions{
			BindAddress: bindAddress,
			BindPort:    bindPort,
			ServerCert: apiserveroptions.GeneratableKeyCert{
				CertDirectory: "",
				PairName:      "cloud-controller-manager-webhook",
			},
		},
	}
}

// AddFlags 注册 webhook 服务相关的 TLS 与监听参数。
func (o *WebhookServingOptions) AddFlags(fs *pflag.FlagSet) {
	fs.IPVar(&o.BindAddress, "webhook-bind-address", o.BindAddress, ""+
		"The IP address on which to listen for the --webhook-secure-port port. The "+
		"associated interface(s) must be reachable by the rest of the cluster, and by CLI/web "+
		fmt.Sprintf("clients. If set to an unspecified address (0.0.0.0 or ::), all interfaces will be used. If unset, defaults to %v.", o.BindAddress))

	fs.IntVar(&o.BindPort, "webhook-secure-port", o.BindPort, "Secure port to serve cloud provider webhooks. If 0, don't serve webhooks at all.")

	fs.BoolVar(&o.DisableHTTP2Serving, "webhook-disable-http2-serving", o.DisableHTTP2Serving,
		"If true, HTTP2 serving will be disabled for the webhook server [default=false]")

	fs.StringVar(&o.ServerCert.CertDirectory, "webhook-cert-dir", o.ServerCert.CertDirectory, ""+
		"The directory where the TLS certs are located. "+
		"If --webhook-tls-cert-file and --webhook-tls-private-key-file are provided, this flag will be ignored.")

	fs.StringVar(&o.ServerCert.CertKey.CertFile, "webhook-tls-cert-file", o.ServerCert.CertKey.CertFile, ""+
		"File containing the default x509 Certificate for HTTPS. (CA cert, if any, concatenated "+
		"after server cert). If HTTPS serving is enabled, and --webhook-tls-cert-file and "+
		"--webhook-tls-private-key-file are not provided, a self-signed certificate and key "+
		"are generated for the public address and saved to the directory specified by --webhook-cert-dir.")

	fs.StringVar(&o.ServerCert.CertKey.KeyFile, "webhook-tls-private-key-file", o.ServerCert.CertKey.KeyFile,
		"File containing the default x509 private key matching --webhook-tls-cert-file.")

	tlsCipherPreferredValues := cliflag.PreferredTLSCipherNames()
	tlsCipherInsecureValues := cliflag.InsecureTLSCipherNames()
	fs.StringSliceVar(&o.CipherSuites, "webhook-tls-cipher-suites", o.CipherSuites,
		"Comma-separated list of cipher suites for the webhook server. "+
			"If omitted, the default Go cipher suites will be used. \n"+
			"Preferred values: "+strings.Join(tlsCipherPreferredValues, ", ")+". \n"+
			"Insecure values: "+strings.Join(tlsCipherInsecureValues, ", ")+".")

	fs.Int32SliceVar(&o.CurvePreferences, "webhook-tls-curve-preferences", o.CurvePreferences,
		"Comma-separated list of numeric Go crypto/tls CurveID values, "+
			"as the allowed key exchange mechanisms for the webhook server. "+
			"The supported values depend on the Go version used. "+
			"See https://pkg.go.dev/crypto/tls#CurveID for values supported for each Go version. "+
			"The order of the list is ignored, and key exchange mechanisms are chosen "+
			"by Go from this list using an internal preference order. "+
			"If omitted, the default Go curves will be used.")

	tlsPossibleVersions := cliflag.TLSPossibleVersions()
	fs.StringVar(&o.MinTLSVersion, "webhook-tls-min-version", o.MinTLSVersion,
		"Minimum TLS version supported for the webhook server. "+
			"Possible values: "+strings.Join(tlsPossibleVersions, ", "))

	fs.Var(cliflag.NewNamedCertKeyArray(&o.SNICertKeys), "webhook-tls-sni-cert-key", ""+
		"A pair of x509 certificate and private key file paths, optionally suffixed with a list of "+
		"domain patterns which are fully qualified domain names, possibly with prefixed wildcard "+
		"segments. The domain patterns also allow IP addresses, but IPs should only be used if "+
		"the webhook server has visibility to the IP address requested by a client. "+
		"If no domain patterns are provided, the names of the certificate are "+
		"extracted. Non-wildcard matches trump over wildcard matches, explicit domain patterns "+
		"trump over extracted names. For multiple key/certificate pairs, use the "+
		"--webhook-tls-sni-cert-key multiple times. "+
		"Examples: \"example.crt,example.key\" or \"foo.crt,foo.key:*.foo.com,foo.com\".")
}

// Validate 校验 webhook 监听端口与证书配置的互斥关系。
func (o *WebhookServingOptions) Validate() []error {
	allErrors := []error{}
	// 0 表示关闭 webhook 服务，因此是合法值。
	if o.BindPort < 0 || o.BindPort > 65535 {
		allErrors = append(allErrors, fmt.Errorf("--webhook-secure-port %v must be between 0 and 65535, inclusive. A value of 0 disables the webhook endpoint entirely.", o.BindPort))
	}

	// 文件证书与内存中生成的证书只能二选一，否则 TLS 配置来源不明确。
	if (len(o.ServerCert.CertKey.CertFile) != 0 || len(o.ServerCert.CertKey.KeyFile) != 0) && o.ServerCert.GeneratedCert != nil {
		allErrors = append(allErrors, fmt.Errorf("cert/key file and in-memory certificate cannot both be set"))
	}

	return allErrors
}

// ApplyTo 创建 webhook 监听并组装 SecureServingInfo。
//
// 中文说明：端口为 0 或没有任何启用的 webhook 时不监听——避免仅为占位而
// 打开一个无人使用的端口。
func (o *WebhookServingOptions) ApplyTo(cfg **server.SecureServingInfo, webhookCfg config.WebhookConfiguration) error {
	if o == nil {
		return nil
	}

	if o.BindPort <= 0 {
		return nil
	}
	// no need to bind to the address if there are no webhook enabled.
	if len(webhookCfg.Webhooks) == 0 {
		return nil
	}

	var err error
	var listener net.Listener
	addr := net.JoinHostPort(o.BindAddress.String(), strconv.Itoa(o.BindPort))

	l := net.ListenConfig{}

	// 端口可能配置为 0（由内核分配），需把实际端口回写到 o.BindPort。
	listener, o.BindPort, err = createListener(addr, l)
	if err != nil {
		return fmt.Errorf("failed to create listener: %v", err)
	}

	*cfg = &server.SecureServingInfo{
		Listener:     listener,
		DisableHTTP2: o.DisableHTTP2Serving,
	}
	c := *cfg

	serverCertFile, serverKeyFile := o.ServerCert.CertKey.CertFile, o.ServerCert.CertKey.KeyFile
	if len(serverCertFile) != 0 || len(serverKeyFile) != 0 {
		var err error
		// 使用动态证书加载器，文件轮换后无需重启即可生效。
		c.Cert, err = dynamiccertificates.NewDynamicServingContentFromFiles("serving-cert", serverCertFile, serverKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load serving cert: %w", err)
		}
	} else if o.ServerCert.GeneratedCert != nil {
		c.Cert = o.ServerCert.GeneratedCert
	}

	if len(o.CipherSuites) != 0 {
		cipherSuites, err := cliflag.TLSCipherSuites(o.CipherSuites)
		if err != nil {
			return fmt.Errorf("failed to parse cipher suites: %w", err)
		}
		c.CipherSuites = cipherSuites
	}

	if len(o.CurvePreferences) != 0 {
		curvePreferences, err := cliflag.TLSCurvePreferences(o.CurvePreferences)
		if err != nil {
			return fmt.Errorf("failed to parse curve preferences: %w", err)
		}
		c.CurvePreferences = curvePreferences
	}

	c.MinTLSVersion, err = cliflag.TLSVersion(o.MinTLSVersion)
	if err != nil {
		return fmt.Errorf("failed to parse TLS version: %w", err)
	}

	// load SNI certs
	namedTLSCerts := make([]dynamiccertificates.SNICertKeyContentProvider, 0, len(o.SNICertKeys))
	for _, nck := range o.SNICertKeys {
		tlsCert, err := dynamiccertificates.NewDynamicSNIContentFromFiles("sni-serving-cert", nck.CertFile, nck.KeyFile, nck.Names...)
		if err != nil {
			return fmt.Errorf("failed to load SNI cert and key: %w", err)
		}
		namedTLSCerts = append(namedTLSCerts, tlsCert)
	}
	c.SNICerts = namedTLSCerts

	return nil
}

// createListener 在 addr 上监听并返回实际绑定的端口。
// 中文说明：当传入端口为 0 时，内核会分配临时端口，调用方必须使用返回的端口
// 对外通告，否则客户端无法连接。
func createListener(addr string, config net.ListenConfig) (net.Listener, int, error) {
	ln, err := config.Listen(context.TODO(), "tcp", addr)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to listen on %v: %v", addr, err)
	}

	// get port
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		// 非 TCP 地址说明配置异常，关闭已建立的监听避免泄漏。
		ln.Close()
		return nil, 0, fmt.Errorf("invalid listen address: %q", ln.Addr().String())
	}

	return ln, tcpAddr.Port, nil
}
