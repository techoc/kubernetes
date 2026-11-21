/*
Copyright 2025 The Kubernetes Authors.

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

package collectors

import (
	"k8s.io/component-base/metrics"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/podcertificate"
)

var (
	// podCertificateStatesDesc 是 gauge 向量的描述（由自定义 collector 实现），
	// 报告当前 kubelet 实例维护的 Pod 证书投影卷源数量。
	podCertificateStatesDesc = metrics.NewDesc(
		metrics.BuildFQName("", kubeletmetrics.KubeletSubsystem, kubeletmetrics.PodCertificateStatesKey),
		"Gauge vector reporting the number of pod certificate projected volume sources, faceted by signer_name and state.",
		[]string{"signer_name", "state"},
		nil,
		metrics.ALPHA,
		"",
	)
)

// podCertificateCollector 根据 podcertificate.Manager 的统计生成指标。
type podCertificateCollector struct {
	metrics.BaseStableCollector
	manager podcertificate.Manager
}

// PodCertificateCollectorFor 为给定 manager 创建指标采集器。
func PodCertificateCollectorFor(m podcertificate.Manager) *podCertificateCollector {
	return &podCertificateCollector{
		manager: m,
	}
}

// DescribeWithStability 向指标框架描述该 collector 输出的指标。
func (c *podCertificateCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- podCertificateStatesDesc
}

// CollectWithStability 按签名者与状态输出当前 Pod 证书投影卷源数量。
func (c *podCertificateCollector) CollectWithStability(ch chan<- metrics.Metric) {
	report := c.manager.MetricReport()

	for k, count := range report.PodCertificateStates {
		ch <- metrics.NewLazyConstMetric(
			podCertificateStatesDesc,
			metrics.GaugeValue,
			float64(count),
			k.SignerName,
			k.State,
		)
	}
}
