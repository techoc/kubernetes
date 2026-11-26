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

package x509

import (
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	asn1util "k8s.io/apimachinery/pkg/apis/asn1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

/*
    * 默认情况下，以下指标定义为
   * ALPHA 稳定性级别 https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
    * 提升指标的稳定性级别是组件所有者的责任，因为这需要
    * 依照指标稳定性策略，明确承诺在多个版本中支持该指标。
    *
*/
// clientCertificateExpirationHistogram 记录客户端证书剩余寿命的分布。
// 分桶覆盖 30 分钟到 1 年，便于运维在证书到期前收到告警。
var clientCertificateExpirationHistogram = metrics.NewHistogram(
	&metrics.HistogramOpts{
		Namespace: "apiserver",
		Subsystem: "client",
		Name:      "certificate_expiration_seconds",
		Help:      "Distribution of the remaining lifetime on the certificate used to authenticate a request.",
		Buckets: []float64{
			0,
			1800,     // 30 minutes
			3600,     // 1 hour
			7200,     // 2 hours
			21600,    // 6 hours
			43200,    // 12 hours
			86400,    // 1 day
			172800,   // 2 days
			345600,   // 4 days
			604800,   // 1 week
			2592000,  // 1 month
			7776000,  // 3 months
			15552000, // 6 months
			31104000, // 1 year
		},
		StabilityLevel: metrics.ALPHA,
	},
)

var registerMetricsOnce sync.Once

// registerMetrics 把 x509 认证指标注册到旧 registry。
// 不用 init()，因为特性门控（如 NativeHistograms）必须在
// 直方图指标创建并注册前完成解析。
//
// 使用 sync.Once 保证重复创建 authenticator（例如动态配置重建）不会重复注册指标。
func registerMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(clientCertificateExpirationHistogram)
	})
}

// UserConversion 定义从客户端证书链提取用户信息的接口
//
// 传入的是 Verify 返回的整条链（[0] 为客户端证书，其后为中间/根 CA），
// 实现方可以按需使用链上任意证书；返回 ok=false 表示不做认证决定。
type UserConversion interface {
	User(chain []*x509.Certificate) (*authenticator.Response, bool, error)
}

// UserConversionFunc 是实现 UserConversion 接口的函数。
type UserConversionFunc func(chain []*x509.Certificate) (*authenticator.Response, bool, error)

// User 实现 x509.UserConversion
func (f UserConversionFunc) User(chain []*x509.Certificate) (*authenticator.Response, bool, error) {
	return f(chain)
}

// columnSeparatedHex 把字节串编码为大写、冒号分隔的十六进制，便于人眼比对证书标识。
func columnSeparatedHex(d []byte) string {
	h := strings.ToUpper(hex.EncodeToString(d))
	var sb strings.Builder
	for i, r := range h {
		sb.WriteRune(r)
		if i%2 == 1 && i != len(h)-1 {
			sb.WriteRune(':')
		}
	}
	return sb.String()
}

// certificateIdentifier 生成稳定的证书标识（序列号 + SKID + AKID），
// 用于错误信息中定位具体证书，而不必记录整张证书内容。
func certificateIdentifier(c *x509.Certificate) string {
	return fmt.Sprintf(
		"SN=%d, SKID=%s, AKID=%s",
		c.SerialNumber,
		columnSeparatedHex(c.SubjectKeyId),
		columnSeparatedHex(c.AuthorityKeyId),
	)
}

// VerifyOptionFunc 向认证器提供 VerifyOptions 的浅拷贝，使选项（尤其是 CA）
// 可以动态变化。若 bool 为 false，则返回的 VerifyOptions
// 会被忽略，认证器表达“无意见”。这样可明确区分
// 预期将来会有 CertPool、但当前尚无的情形。
//
// 返回 ok=false 表示“目前无法给出验证选项”（例如 client CA 尚未加载），
// 此时认证器应表达无意见而不是拒绝请求，以便与动态 CA 重载配合。
type VerifyOptionFunc func() (x509.VerifyOptions, bool)

// Authenticator 通过从已验证的客户端证书中提取用户信息，实现 request.Authenticator
//
// 认证流程：从 TLS 握手中取出客户端证书链 → 用 verifyOptionsFn 提供的 CA 校验 →
// 用 UserConversion 把合法链转换为 user.Info。
type Authenticator struct {
	verifyOptionsFn VerifyOptionFunc
	user            UserConversion
}

// New 返回一个 request.Authenticator，用给定的 VerifyOptions 验证客户端证书，
// 并用给定的 UserConversion 将有效证书链转换为 user.Info。
//
// 静态选项的便捷构造函数；需要动态 CA 时使用 NewDynamic。
func New(opts x509.VerifyOptions, user UserConversion) *Authenticator {
	return NewDynamic(StaticVerifierFn(opts), user)
}

// NewDynamic 返回一个 request.Authenticator，用给定的 VerifyOptionFunc（可动态变化）
// 验证客户端证书，并用给定的 UserConversion 将有效证书链转换为 user.Info。
//
// 每次请求都会调用 verifyOptionsFn，因此 CA 轮换无需重建认证器。
func NewDynamic(verifyOptionsFn VerifyOptionFunc, user UserConversion) *Authenticator {
	registerMetrics()
	return &Authenticator{verifyOptionsFn, user}
}

// AuthenticateRequest 用客户端提交的证书认证请求
//
// 没有客户端证书时返回 (nil, false, nil)，表示本认证器无意见，由其它认证器接管。
func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
		return nil, false, nil
	}

	// 如提供中间证书，则一并使用
	optsCopy, ok := a.verifyOptionsFn()
	// 若有意不提供 verify options，则无法认证该请求
	if !ok {
		return nil, false, nil
	}
	if optsCopy.Intermediates == nil && len(req.TLS.PeerCertificates) > 1 {
		optsCopy.Intermediates = x509.NewCertPool()
		for _, intermediate := range req.TLS.PeerCertificates[1:] {
			optsCopy.Intermediates.AddCert(intermediate)
		}
	}

	/*
			 kubernetes mutual (2-way) x509 between client and apiserver:

				1. apiserver sending its apiserver certificate along with its publickey to client
				2. client verifies the apiserver certificate sent against its cluster certificate authority data
				3. client sending its client certificate along with its public key to the apiserver
				>4. apiserver verifies the client certificate sent against its cluster certificate authority data

		    	description:
					here, with this function,
					client certificate and pub key sent during the handshake process
					are verified by apiserver against its cluster certificate authority data

				normal args related to this stage:
					--client-ca-file string   If set, any request presenting a client certificate signed by
						one of the authorities in the client-ca-file is authenticated with an identity
						corresponding to the CommonName of the client certificate.

					(retrievable from "kube-apiserver --help" command)
					(suggested by @deads2k)

				see also:
					- for the step 1, see: staging/src/k8s.io/apiserver/pkg/server/options/serving.go
					- for the step 2, see: staging/src/k8s.io/client-go/transport/transport.go
					- for the step 3, see: staging/src/k8s.io/client-go/transport/transport.go
	*/

	// 观测客户端证书剩余有效期，用于提前发现即将过期的客户端证书。
	// remaining 可能为负（证书已过期），这是有意保留的信号。
	remaining := req.TLS.PeerCertificates[0].NotAfter.Sub(time.Now())
	clientCertificateExpirationHistogram.WithContext(req.Context()).Observe(remaining.Seconds())
	chains, err := req.TLS.PeerCertificates[0].Verify(optsCopy)
	if err != nil {
		return nil, false, fmt.Errorf(
			"verifying certificate %s failed: %w",
			certificateIdentifier(req.TLS.PeerCertificates[0]),
			err,
		)
	}

	// 一条证书可能产生多条合法链；依次尝试转换，任意一条成功即认证通过。
	// 若所有链都失败，则把错误聚合返回，保留每条链的失败原因。
	var errlist []error
	for _, chain := range chains {
		user, ok, err := a.user.User(chain)
		if err != nil {
			errlist = append(errlist, err)
			continue
		}

		if ok {
			return user, ok, err
		}
	}
	return nil, false, utilerrors.NewAggregate(errlist)
}

// Verifier 先验证请求中的客户端证书，再委派给包装的认证器，实现 request.Authenticator
//
// 与 Authenticator 的区别：这里只把证书校验当作“前置门槛”，真正的身份仍由
// 内层 authenticator（通常是 bearer token）决定，从而支持证书 + token 双重认证。
type Verifier struct {
	verifyOptionsFn VerifyOptionFunc
	auth            authenticator.Request

	// allowedCommonNames 是已验证证书允许的 CommonName 集合。
	// 为空时允许所有已验证证书。
	allowedCommonNames StringSliceProvider
}

// NewVerifier 创建一个 request.Authenticator，先验证请求中的客户端证书，再委派给包装的认证器
func NewVerifier(opts x509.VerifyOptions, auth authenticator.Request, allowedCommonNames sets.String) authenticator.Request {
	return NewDynamicCAVerifier(StaticVerifierFn(opts), auth, StaticStringSlice(allowedCommonNames.List()))
}

// NewDynamicCAVerifier 创建一个 request.Authenticator，先用可动态更新的 CA 验证请求中的客户端证书，再委派给包装的认证器
func NewDynamicCAVerifier(verifyOptionsFn VerifyOptionFunc, auth authenticator.Request, allowedCommonNames StringSliceProvider) authenticator.Request {
	return &Verifier{verifyOptionsFn, auth, allowedCommonNames}
}

// AuthenticateRequest 先验证客户端提交的证书，再委派给包装的认证器
//
// 校验失败直接返回错误（不降级为无意见），因为客户端已经主动出示了证书。
func (a *Verifier) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
		return nil, false, nil
	}

	// 如提供中间证书，则一并使用
	optsCopy, ok := a.verifyOptionsFn()
	// 若有意不提供 verify options，则无法认证该请求
	if !ok {
		return nil, false, nil
	}
	if optsCopy.Intermediates == nil && len(req.TLS.PeerCertificates) > 1 {
		optsCopy.Intermediates = x509.NewCertPool()
		for _, intermediate := range req.TLS.PeerCertificates[1:] {
			optsCopy.Intermediates.AddCert(intermediate)
		}
	}

	if _, err := req.TLS.PeerCertificates[0].Verify(optsCopy); err != nil {
		return nil, false, err
	}
	if err := a.verifySubject(req.TLS.PeerCertificates[0].Subject); err != nil {
		return nil, false, err
	}
	return a.auth.AuthenticateRequest(req)
}

// verifySubject 执行可选的 CN 白名单校验；白名单为空表示不限制。
func (a *Verifier) verifySubject(subject pkix.Name) error {
	// 不限制 CN
	if len(a.allowedCommonNames.Value()) == 0 {
		return nil
	}
	// 强制检查 CN
	for _, allowedCommonName := range a.allowedCommonNames.Value() {
		if allowedCommonName == subject.CommonName {
			return nil
		}
	}
	return fmt.Errorf("x509: subject with cn=%s is not in the allowed list", subject.CommonName)
}

// DefaultVerifyOptions 返回使用系统根证书和当前时间的 VerifyOptions，
// 并要求证书对客户端认证有效（x509.ExtKeyUsageClientAuth）
//
// 其它字段留空即采用 Go 的默认值：系统根证书、当前时间。
func DefaultVerifyOptions() x509.VerifyOptions {
	return x509.VerifyOptions{
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
}

// CommonNameUserConversion 用证书的 subject CommonName 构建用户信息
//
// 用户名取证书 Subject.CommonName，组取 Subject.Organization（这是 k8s 长期约定）；
// 同时把证书 SHA256 指纹写入 CredentialIDKey，便于审计时追踪具体凭据。
// 另外按特性门控尝试从证书解析 UID。
var CommonNameUserConversion = UserConversionFunc(func(chain []*x509.Certificate) (*authenticator.Response, bool, error) {
	if len(chain[0].Subject.CommonName) == 0 {
		return nil, false, nil
	}

	fp := sha256.Sum256(chain[0].Raw)
	id := "X509SHA256=" + hex.EncodeToString(fp[:])

	uid, err := parseUIDFromCert(chain[0])
	if err != nil {
		return nil, false, err
	}
	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   chain[0].Subject.CommonName,
			UID:    uid,
			Groups: chain[0].Subject.Organization,
			Extra: map[string][]string{
				user.CredentialIDKey: {id},
			},
		},
	}, true, nil
})

var uidOID = asn1util.X509UID()

// parseUIDFromCert 从证书 Subject 的自定义 OID 中解析 UID。
// 特性门控关闭时直接返回空串，保持与旧版本行为一致。
// 出现多个 UID 或空 UID 视为配置错误，必须拒绝而不是挑选其中一个。
func parseUIDFromCert(cert *x509.Certificate) (string, error) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.AllowParsingUserUIDFromCertAuth) {
		return "", nil
	}

	uids := []string{}
	for _, name := range cert.Subject.Names {
		if !name.Type.Equal(uidOID) {
			continue
		}
		uid, ok := name.Value.(string)
		if !ok {
			return "", fmt.Errorf("unable to parse UID into a string")
		}
		uids = append(uids, uid)
	}
	if len(uids) == 0 {
		return "", nil
	}
	if len(uids) != 1 {
		return "", fmt.Errorf("expected 1 UID, but found multiple: %v", uids)
	}
	if uids[0] == "" {
		return "", errors.New("UID cannot be an empty string")
	}
	return uids[0], nil
}
