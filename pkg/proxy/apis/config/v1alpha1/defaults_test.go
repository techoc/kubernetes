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

package v1alpha1

import (
	"testing"
	"time"

	"k8s.io/utils/ptr"

	"github.com/google/go-cmp/cmp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfig "k8s.io/component-base/config/v1alpha1"
	logsapi "k8s.io/component-base/logs/api/v1"
	kubeproxyconfigv1alpha1 "k8s.io/kube-proxy/config/v1alpha1"
)

// TestDefaultsKubeProxyConfiguration 验证 SetDefaults_KubeProxyConfiguration
// 能为空配置填写正确的默认值，并在用户只给出部分字段（如不带端口的监听地址）
// 时补齐其余字段。expected 中显式断言了 bind address、各类同步周期、OOM 分数、
// conntrack 超时和日志配置，防止默认值被无意改动。
func TestDefaultsKubeProxyConfiguration(t *testing.T) {
	// 用变量取地址，作为 OOM、conntrack 相关指针字段的期望值。
	oomScore := int32(-999)
	ctMaxPerCore := int32(32768)
	ctMin := int32(131072)
	// 表驱动用例：original 为输入配置，expected 为期望的默认化结果。
	testCases := []struct {
		name     string
		original *kubeproxyconfigv1alpha1.KubeProxyConfiguration
		expected *kubeproxyconfigv1alpha1.KubeProxyConfiguration
	}{
		// 全空配置场景：所有字段都应被填成默认常量值。
		{
			name:     "empty-config",
			original: &kubeproxyconfigv1alpha1.KubeProxyConfiguration{},
			expected: &kubeproxyconfigv1alpha1.KubeProxyConfiguration{
				FeatureGates:       map[string]bool{},
				BindAddress:        "0.0.0.0",
				HealthzBindAddress: "0.0.0.0:10256",
				MetricsBindAddress: "127.0.0.1:10249",
				ClientConnection: componentbaseconfig.ClientConnectionConfiguration{
					ContentType: "application/vnd.kubernetes.protobuf",
					QPS:         5,
					Burst:       10,
				},
				IPTables: kubeproxyconfigv1alpha1.KubeProxyIPTablesConfiguration{
					MasqueradeBit:      ptr.To[int32](14),
					MasqueradeAll:      false,
					LocalhostNodePorts: ptr.To(true),
					SyncPeriod:         metav1.Duration{Duration: 30 * time.Second},
					MinSyncPeriod:      metav1.Duration{Duration: 1 * time.Second},
				},
				IPVS: kubeproxyconfigv1alpha1.KubeProxyIPVSConfiguration{
					SyncPeriod: metav1.Duration{Duration: 30 * time.Second},
				},
				NFTables: kubeproxyconfigv1alpha1.KubeProxyNFTablesConfiguration{
					MasqueradeBit: ptr.To[int32](14),
					MasqueradeAll: false,
					SyncPeriod:    metav1.Duration{Duration: 30 * time.Second},
					MinSyncPeriod: metav1.Duration{Duration: 1 * time.Second},
				},
				OOMScoreAdj: &oomScore,
				Conntrack: kubeproxyconfigv1alpha1.KubeProxyConntrackConfiguration{
					MaxPerCore:            &ctMaxPerCore,
					Min:                   &ctMin,
					TCPEstablishedTimeout: &metav1.Duration{Duration: 24 * time.Hour},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 1 * time.Hour},
				},
				ConfigSyncPeriod: metav1.Duration{Duration: 15 * time.Minute},
				Logging: logsapi.LoggingConfiguration{
					Format:         "text",
					FlushFrequency: logsapi.TimeOrMetaDuration{Duration: metav1.Duration{Duration: 5 * time.Second}, SerializeAsString: true},
				},
			},
		},
		// 只给了 IP 没给端口：默认化必须补上标准端口，
		// 否则监听地址会被误判为不完整。
		{
			name: "metrics and healthz address with no port",
			original: &kubeproxyconfigv1alpha1.KubeProxyConfiguration{
				MetricsBindAddress: "127.0.0.1",
				HealthzBindAddress: "127.0.0.1",
			},
			expected: &kubeproxyconfigv1alpha1.KubeProxyConfiguration{
				FeatureGates:       map[string]bool{},
				BindAddress:        "0.0.0.0",
				HealthzBindAddress: "127.0.0.1:10256",
				MetricsBindAddress: "127.0.0.1:10249",
				ClientConnection: componentbaseconfig.ClientConnectionConfiguration{
					ContentType: "application/vnd.kubernetes.protobuf",
					QPS:         5,
					Burst:       10,
				},
				IPTables: kubeproxyconfigv1alpha1.KubeProxyIPTablesConfiguration{
					MasqueradeBit:      ptr.To[int32](14),
					MasqueradeAll:      false,
					LocalhostNodePorts: ptr.To(true),
					SyncPeriod:         metav1.Duration{Duration: 30 * time.Second},
					MinSyncPeriod:      metav1.Duration{Duration: 1 * time.Second},
				},
				IPVS: kubeproxyconfigv1alpha1.KubeProxyIPVSConfiguration{
					SyncPeriod: metav1.Duration{Duration: 30 * time.Second},
				},
				NFTables: kubeproxyconfigv1alpha1.KubeProxyNFTablesConfiguration{
					MasqueradeBit: ptr.To[int32](14),
					MasqueradeAll: false,
					SyncPeriod:    metav1.Duration{Duration: 30 * time.Second},
					MinSyncPeriod: metav1.Duration{Duration: 1 * time.Second},
				},
				OOMScoreAdj: &oomScore,
				Conntrack: kubeproxyconfigv1alpha1.KubeProxyConntrackConfiguration{
					MaxPerCore:            &ctMaxPerCore,
					Min:                   &ctMin,
					TCPEstablishedTimeout: &metav1.Duration{Duration: 24 * time.Hour},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 1 * time.Hour},
				},
				ConfigSyncPeriod: metav1.Duration{Duration: 15 * time.Minute},
				Logging: logsapi.LoggingConfiguration{
					Format:         "text",
					FlushFrequency: logsapi.TimeOrMetaDuration{Duration: metav1.Duration{Duration: 5 * time.Second}, SerializeAsString: true},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 就地默认化输入配置。
			SetDefaults_KubeProxyConfiguration(tc.original)
			// 逐字段比较，输出差异便于定位哪个默认值发生变化；
			// -want +got 的顺序与输出格式一致。
			if diff := cmp.Diff(tc.expected, tc.original); diff != "" {
				t.Errorf("Got unexpected defaults (-want, +got):\n%s", diff)
			}
		})
	}
}
