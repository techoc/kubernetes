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
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/kubelet/podcertificate"
)

// fakePodCertificateManager 是供测试使用的 PodCertificateManager。
type fakePodCertificateManager struct {
	// report 是预置的指标报告。
	report *podcertificate.MetricReport
}

// TrackPod 是空实现。
func (m *fakePodCertificateManager) TrackPod(ctx context.Context, pod *corev1.Pod) {}

// ForgetPod 是空实现。
func (m *fakePodCertificateManager) ForgetPod(ctx context.Context, pod *corev1.Pod) {}

// GetPodCertificateCredentialBundle 未实现，返回错误。
func (m *fakePodCertificateManager) GetPodCertificateCredentialBundle(ctx context.Context, namespace, podName, podUID, volumeName string, sourceIndex int) (privKey []byte, certChain []byte, err error) {
	return nil, nil, fmt.Errorf("unimplemented")
}

// MetricReport 返回预置的指标报告。
func (m *fakePodCertificateManager) MetricReport() *podcertificate.MetricReport {
	return m.report
}

// TestPodCertificateCollector 验证 Pod 证书指标采集器输出的指标内容。
func TestPodCertificateCollector(t *testing.T) {
	// 构造带有预置指标报告的 fake 管理器。
	collector := &podCertificateCollector{
		manager: &fakePodCertificateManager{
			report: &podcertificate.MetricReport{
				PodCertificateStates: map[podcertificate.SignerAndState]int{
					{SignerName: "example.com/foo", State: "fresh"}:               1,
					{SignerName: "example.com/foo", State: "overdue_for_refresh"}: 1,
					{SignerName: "example.com/foo", State: "expired"}:             0,
					{SignerName: "example.com/bar", State: "fresh"}:               2,
					{SignerName: "example.com/bar", State: "overdue_for_refresh"}: 0,
					{SignerName: "example.com/bar", State: "expired"}:             1,
				},
			},
		},
	}

	// 固定的类型与 help 文本元数据。
	// 把它前置到预期输出，后续调整只需改一处。
	const metadata = `
		# HELP kubelet_podcertificate_states [ALPHA] Gauge vector reporting the number of pod certificate projected volume sources, faceted by signer_name and state.
		# TYPE kubelet_podcertificate_states gauge
		`

	want := metadata + `
			kubelet_podcertificate_states{signer_name="example.com/foo",state="fresh"} 1.0
			kubelet_podcertificate_states{signer_name="example.com/foo",state="overdue_for_refresh"} 1.0
			kubelet_podcertificate_states{signer_name="example.com/foo",state="expired"} 0.0
			kubelet_podcertificate_states{signer_name="example.com/bar",state="fresh"} 2.0
			kubelet_podcertificate_states{signer_name="example.com/bar",state="overdue_for_refresh"} 0.0
			kubelet_podcertificate_states{signer_name="example.com/bar",state="expired"} 1.0
			`

	metrics := []string{
		"kubelet_podcertificate_states",
	}

	// 采集结果必须与预期指标文本完全一致。
	if err := testutil.CustomCollectAndCompare(collector, strings.NewReader(want), metrics...); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}
