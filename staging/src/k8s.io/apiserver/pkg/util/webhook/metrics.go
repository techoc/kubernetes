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

package webhook

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// x509MissingSANCounter 统计因服务端证书缺少 SAN 扩展而失败或受影响的 webhook 请求数。
// Help 文案保留 “either/or” 表述：在部分运行环境中能观测到请求失败，
// 在另一些环境中只能观测到连接失败，二者择一计数，避免重复统计。
var x509MissingSANCounter = metrics.NewCounter(
	&metrics.CounterOpts{
		Subsystem: "webhooks",
		Namespace: "apiserver",
		Name:      "x509_missing_san_total",
		Help: "Counts the number of requests to servers missing SAN extension " +
			"in their serving certificate OR the number of connection failures " +
			"due to the lack of x509 certificate SAN extension missing " +
			"(either/or, based on the runtime environment)",
		StabilityLevel: metrics.BETA,
	},
)

// x509InsecureSHA1Counter 统计服务端证书使用不安全 SHA1 签名导致失败或受影响的 webhook 请求数。
var x509InsecureSHA1Counter = metrics.NewCounter(
	&metrics.CounterOpts{
		Subsystem: "webhooks",
		Namespace: "apiserver",
		Name:      "x509_insecure_sha1_total",
		Help: "Counts the number of requests to servers with insecure SHA1 signatures " +
			"in their serving certificate OR the number of connection failures " +
			"due to the insecure SHA1 signatures (either/or, based on the runtime environment)",
		StabilityLevel: metrics.BETA,
	},
)

func init() {
	// 注册到 legacyregistry，与 apiserver 其他指标共用同一 /metrics 出口。
	legacyregistry.MustRegister(x509MissingSANCounter)
	legacyregistry.MustRegister(x509InsecureSHA1Counter)
}
