/*
Copyright 2021 The Kubernetes Authors.

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

package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/certificate/csr"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/kubernetes/pkg/apis/certificates"
)

const (
	namespace = "apiserver"
	subsystem = "certificates_registry"
)

var (
	// csrDurationRequested 统计客户端指定证书有效期且最终成功签发的证书数量，并按签名者分类。
	csrDurationRequested = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "csr_requested_duration_total",
			Help:           "Total number of issued CSRs with a requested duration, sliced by signer (only kubernetes.io signer names are specifically identified)",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"signerName"},
	)

	// csrDurationHonored 统计签名者遵循客户端所请求有效期签发的证书数量，并按签名者分类。
	csrDurationHonored = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "csr_honored_duration_total",
			Help:           "Total number of issued CSRs with a requested duration that was honored, sliced by signer (only kubernetes.io signer names are specifically identified)",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"signerName"},
	)
)

func init() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(csrDurationRequested)
		legacyregistry.MustRegister(csrDurationHonored)
	})
}

var registerMetricsOnce sync.Once

type counterVecMetric interface {
	WithLabelValues(...string) metrics.CounterMetric
}

func countCSRDurationMetric(requested, honored counterVecMetric) genericregistry.BeginUpdateFunc {
	return func(ctx context.Context, obj, old runtime.Object, options *metav1.UpdateOptions) (genericregistry.FinishFunc, error) {
		return func(ctx context.Context, success bool) {
			if !success {
				return // 更新失败时不记录指标。
			}

			if dryrun.IsDryRun(options.DryRun) {
				return // Dry-run 请求不会持久化对象，因此不记录指标。
			}

			oldCSR, ok := old.(*certificates.CertificateSigningRequest)
			if !ok {
				return
			}

			// 旧 CSR 已包含签发证书，说明此前已统计过，避免重复计数。
			if len(oldCSR.Status.Certificate) > 0 {
				return
			}

			if oldCSR.Spec.ExpirationSeconds == nil {
				return // 未使用 CSR 有效期特性的请求不纳入统计。
			}

			newCSR, ok := obj.(*certificates.CertificateSigningRequest)
			if !ok {
				return
			}
			issuedCert := newCSR.Status.Certificate

			// 新 CSR 尚未签发证书，因此不计数。设置了有效期但最终未获批准或未被签名的 CSR 也会被忽略；
			// 这是有意的，因为这些指标用于衡量签名者是否遵循请求的有效期，而不是评估批准者的行为。
			if len(issuedCert) == 0 {
				return
			}

			signer := compressSignerName(oldCSR.Spec.SignerName)

			// 到这里已确认该 CSR 更新将被持久化、刚刚签发了证书，并且客户端明确请求了有效期。
			requested.WithLabelValues(signer).Inc()

			certs, err := cert.ParseCertsPEM(issuedCert)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("metrics recording failed to parse certificate for CSR %s: %w", oldCSR.Name, err))
				return
			}

			// 检查签名者实际签发的证书有效期是否符合客户端请求。
			certificate := certs[0]
			wantDuration := csr.ExpirationSecondsToDuration(*oldCSR.Spec.ExpirationSeconds)
			actualDuration := certificate.NotAfter.Sub(certificate.NotBefore)
			if isDurationHonored(wantDuration, actualDuration) {
				honored.WithLabelValues(signer).Inc()
			}
		}, nil
	}
}

func isDurationHonored(want, got time.Duration) bool {
	delta := want - got
	if delta < 0 {
		delta = -delta
	}

	// 容差由短有效期证书可能采用的回溯生效时间（5 分钟）加上请求有效期的 5% 组成。
	maxDelta := 5*time.Minute + (want / 20)

	return delta < maxDelta
}

func compressSignerName(name string) string {
	if strings.HasPrefix(name, "kubernetes.io/") {
		return name
	}

	return "other"
}
