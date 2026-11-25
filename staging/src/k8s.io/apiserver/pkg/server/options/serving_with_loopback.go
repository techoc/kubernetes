/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/utils/clock"
)

// SecureServingOptionsWithLoopback 在普通安全服务配置之上补充回环客户端配置，
// 供 apiserver 组件以自身身份访问本机 API。
type SecureServingOptionsWithLoopback struct {
	*SecureServingOptions
	clock clock.PassiveClock
}

// WithLoopback 返回带回环支持的包装；clock 可注入以便测试证书过期逻辑。
func (o *SecureServingOptions) WithLoopback() *SecureServingOptionsWithLoopback {
	return &SecureServingOptionsWithLoopback{
		SecureServingOptions: o,
		clock:                clock.RealClock{},
	}
}

// 回环证书的有效期设为约 3 年，避免 kube-apiserver 因证书过期而中断；
// 证书过期后，重启 kube-apiserver 会自动重新生成带有新有效期的证书。
// 回环证书是进程内自签的，重启即重新生成，因此长有效期是安全的。
const maxAge = (3*365 + 1) * 24 * time.Hour

// ApplyTo fills up serving information in the server configuration.
func (s *SecureServingOptionsWithLoopback) ApplyTo(secureServingInfo **server.SecureServingInfo, loopbackClientConfig **rest.Config) error {
	return s.applyTo(secureServingInfo, loopbackClientConfig, nil)
}

// HealthzLivezHealthChecksAdder 抽象健康检查注册接口，避免依赖完整 server.Config。
type HealthzLivezHealthChecksAdder interface {
	AddHealthzChecks(checks ...healthz.HealthChecker)
	AddLivezChecks(checks ...healthz.HealthChecker)
}

func (s *SecureServingOptionsWithLoopback) applyTo(secureServingInfo **server.SecureServingInfo, loopbackClientConfig **rest.Config, healthCheckAdder HealthzLivezHealthChecksAdder) error {
	// 三项任一缺失都意味着未启用安全服务，直接跳过而非报错。
	if s == nil || s.SecureServingOptions == nil || secureServingInfo == nil {
		return nil
	}

	if err := s.SecureServingOptions.ApplyTo(secureServingInfo); err != nil {
		return err
	}

	if *secureServingInfo == nil || loopbackClientConfig == nil {
		return nil
	}

	// 用伪主机名 server.LoopbackClientServerNameOverride 生成自签证书+私钥，
	// 并在回环客户端连接时由服务端返回该证书：
	// 证书仅用于本机回环，SNI 使用固定伪主机名，不需要（也无法）由外部 CA 签发。
	certPem, keyPem, err := certutil.GenerateSelfSignedCertKeyWithOptions(certutil.SelfSignedCertKeyOptions{
		Host:   server.LoopbackClientServerNameOverride,
		MaxAge: maxAge,
	})
	if err != nil {
		return fmt.Errorf("failed to generate self-signed certificate for loopback connection: %v", err)
	}
	certProvider, err := dynamiccertificates.NewStaticSNICertKeyContent("self-signed loopback", certPem, keyPem, server.LoopbackClientServerNameOverride)
	if err != nil {
		return fmt.Errorf("failed to generate self-signed certificate for loopback connection: %v", err)
	}

	// 插到 SNICerts 最前面，使其覆盖任何其他同名证书：
	// 必须排在前面：SNI 匹配按顺序命中，回环伪主机名不能被其他证书抢先。
	(*secureServingInfo).SNICerts = append([]dynamiccertificates.SNICertKeyContentProvider{certProvider}, (*secureServingInfo).SNICerts...)

	secureLoopbackClientConfig, err := (*secureServingInfo).NewLoopbackClientConfig(uuid.New().String(), certPem)
	switch {
	// if we failed and there's no fallback loopback client config, we need to fail
	case err != nil && *loopbackClientConfig == nil:
		// 回滚刚插入的 SNI 证书，避免留下无效配置。
		(*secureServingInfo).SNICerts = (*secureServingInfo).SNICerts[1:]
		return err

	// if we failed, but we already have a fallback loopback client config (usually insecure), allow it
	case err != nil && *loopbackClientConfig != nil:

	default:
		*loopbackClientConfig = secureLoopbackClientConfig
		if healthCheckAdder != nil {
			s.addLoopbackServingCertificateHealthCheck(healthCheckAdder)
		}
	}

	return nil
}

// ApplyToConfig 是 ApplyTo 的便捷版本，直接使用 server.Config 上的字段。
func (s *SecureServingOptionsWithLoopback) ApplyToConfig(cfg *server.Config) error {
	return s.applyTo(&cfg.SecureServing, &cfg.LoopbackClientConfig, cfg)
}

// addLoopbackServingCertificateHealthCheck adds a health check called `loopback-certificate-expiry` to the
// server that fails when the loopback client certificate has expired, enabling
// liveness probes to be used to automatically restart the apiserver.
func (s *SecureServingOptionsWithLoopback) addLoopbackServingCertificateHealthCheck(healthCheckAdder HealthzLivezHealthChecksAdder) {
	expirationDate := s.clock.Now().Add(maxAge)
	check := healthz.NamedCheck("loopback-serving-certificate", func(r *http.Request) error {
		// 过期后 liveness 失败，由 kubelet 重启容器以生成新证书。
		if s.clock.Now().After(expirationDate) {
			return LoopbackCertificateExpiredError{}
		}

		return nil
	})

	healthCheckAdder.AddHealthzChecks(check)
	healthCheckAdder.AddLivezChecks(check)
}

// LoopbackCertificateExpiredError 表示回环服务证书已过期，属于重启即可自愈的状态。
type LoopbackCertificateExpiredError struct{}

// Error 返回稳定的错误文案，便于健康检查消费者识别。
func (lcee LoopbackCertificateExpiredError) Error() string {
	return "loopback serving certificate is expired"
}
