/*
Copyright 2020 The Kubernetes Authors.

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

package apiserver

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// 中文补充：Go 1.15 起证书必须带 SAN，仅靠 CN 校验会失败。
// 这个计数器用来量化有多少后端证书缺少 SAN，或者有多少连接因此失败；
// 两种口径取决于运行时环境（表达式 "either/or" 是字面意思，别当笔误）。
// 标记为 ALPHA 表示指标名和语义仍可能调整，告警规则不应依赖它做硬性判断。
var x509MissingSANCounter = metrics.NewCounter(
	&metrics.CounterOpts{
		Subsystem: "kube_aggregator",
		Namespace: "apiserver",
		Name:      "x509_missing_san_total",
		Help: "Counts the number of requests to servers missing SAN extension " +
			"in their serving certificate OR the number of connection failures " +
			"due to the lack of x509 certificate SAN extension missing " +
			"(either/or, based on the runtime environment)",
		StabilityLevel: metrics.ALPHA,
	},
)

// 中文补充：SHA1 签名已被视为不安全，客户端可能拒绝握手。
// 单独计数而不是并入 SAN 指标，是因为两者的修复动作不同：
// 前者要重签证书，后者要补 SAN，运维需要能分别定位。
var x509InsecureSHA1Counter = metrics.NewCounter(
	&metrics.CounterOpts{
		Subsystem: "kube_aggregator",
		Namespace: "apiserver",
		Name:      "x509_insecure_sha1_total",
		Help: "Counts the number of requests to servers with insecure SHA1 signatures " +
			"in their serving certificate OR the number of connection failures " +
			"due to the insecure SHA1 signatures (either/or, based on the runtime environment)",
		StabilityLevel: metrics.ALPHA,
	},
)

func init() {
	// 中文补充：注册到 legacy registry 才能被既有的 /metrics 端点抓取；
	// MustRegister 在重复注册时直接 panic，避免静默丢失指标。
	legacyregistry.MustRegister(x509MissingSANCounter)
	legacyregistry.MustRegister(x509InsecureSHA1Counter)
}
