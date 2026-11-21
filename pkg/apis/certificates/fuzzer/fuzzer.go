/*
Copyright 2017 The Kubernetes Authors.

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

package fuzzer

import (
	"time"

	"sigs.k8s.io/randfill"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/util/certificate/csr"
	"k8s.io/kubernetes/pkg/apis/certificates"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
)

// Funcs 返回 certificates API group 的 fuzzer 函数集合，
// 供 roundtrip 测试构造合法且可序列化的随机对象使用。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// CSR 的 usages、signerName、expirationSeconds 都有取值/格式约束，
		// 随机字节无法通过这些约束，因此显式赋合法值。
		func(obj *certificates.CertificateSigningRequestSpec, c randfill.Continue) {
			c.FillNoCustom(obj) // 填充除本函数定制字段外的其他字段，避免递归调用自身
			obj.Usages = []certificates.KeyUsage{certificates.UsageKeyEncipherment}
			obj.SignerName = "example.com/custom-sample-signer"
			obj.ExpirationSeconds = csr.DurationToExpirationSeconds(time.Hour + time.Minute + time.Second)
		},
		// Condition 的 status 不能为空，空值会在校验时被拒绝。
		func(obj *certificates.CertificateSigningRequestCondition, c randfill.Continue) {
			c.FillNoCustom(obj) // 填充除本函数定制字段外的其他字段，避免递归调用自身
			if len(obj.Status) == 0 {
				obj.Status = api.ConditionTrue
			}
		},
		// PodCertificateRequestSpec 的 fuzz 逻辑：保证字段在 roundtrip 前后保持一致。
		func(obj *certificates.PodCertificateRequestSpec, c randfill.Continue) {
			c.FillNoCustom(obj) // 填充除本函数定制字段外的其他字段，避免递归调用自身

			// MaxExpirationSeconds 带有字段默认值设定器，必须保证非 nil，
			// 否则 pkg/api/testing/serialization_test.go 的 TestRoundTripTypes
			// 会因默认值填充前后的差异而失败。
			if obj.MaxExpirationSeconds == nil {
				obj.MaxExpirationSeconds = ptr.To[int32](86400)
			}

			// 清空 PKIXPublicKey 与 ProofOfPossession：这两个字段在 v1 版本的
			// PodCertificateRequest 中会被丢弃，不清空将导致 roundtrip 比较失败。
			obj.PKIXPublicKey = nil     //nolint:staticcheck // 为满足 roundtrip 需要清空已废弃字段
			obj.ProofOfPossession = nil //nolint:staticcheck // 为满足 roundtrip 需要清空已废弃字段
		},
	}
}
