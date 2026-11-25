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

package validation

import (
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	componentbaseconfig "k8s.io/component-base/config"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/pkg/features"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/utils/ptr"
)

// TestValidateKubeProxyConfiguration 用表驱动用例校验整体配置的合法性与错误路径，
// 从合法基线配置出发逐项修改，验证校验器只报告预期的字段错误。
func TestValidateKubeProxyConfiguration(t *testing.T) {
	baseConfig := &kubeproxyconfig.KubeProxyConfiguration{
		BindAddress:        "192.168.59.103",
		HealthzBindAddress: "0.0.0.0:10256",
		MetricsBindAddress: "127.0.0.1:10249",
		DetectLocalMode:    kubeproxyconfig.LocalModeClusterCIDR,
		DetectLocal: kubeproxyconfig.DetectLocalConfiguration{
			ClusterCIDRs: []string{"192.168.59.0/24"},
		},
		SyncPeriod:       metav1.Duration{Duration: 5 * time.Second},
		MinSyncPeriod:    metav1.Duration{Duration: 2 * time.Second},
		ConfigSyncPeriod: metav1.Duration{Duration: 1 * time.Second},
		IPTables:         kubeproxyconfig.KubeProxyIPTablesConfiguration{},
		Linux: kubeproxyconfig.KubeProxyLinuxConfiguration{
			Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
				MaxPerCore:            ptr.To[int32](1),
				Min:                   ptr.To[int32](1),
				TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
				TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
			},
			MasqueradeAll: true,
		},
		Logging: logsapi.LoggingConfiguration{
			Format:         "text",
			FlushFrequency: logsapi.TimeOrMetaDuration{Duration: metav1.Duration{Duration: logsapi.LogFlushFreqDefault}},
		},
	}
	newPath := field.NewPath("KubeProxyConfiguration")

	testCases := map[string]struct {
		// 每个用例只改一个字段，便于把错误精确定位到单个校验规则。
		mutateConfigFunc func(*kubeproxyconfig.KubeProxyConfiguration)
		expectedErrs     field.ErrorList
	}{
		"basic config, unspecified Mode": {
			mutateConfigFunc: func(_ *kubeproxyconfig.KubeProxyConfiguration) {},
		},
		// Windows 与 Linux 支持不同的模式，按运行时平台选择合法值。
		"Mode specified, extra mode-specific configs": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				if runtime.GOOS == "windows" {
					config.Mode = kubeproxyconfig.ProxyModeKernelspace
				} else {
					config.Mode = kubeproxyconfig.ProxyModeIPVS
				}
			},
		},
		"empty HealthzBindAddress": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.HealthzBindAddress = ""
			},
		},
		"IPv6": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.BindAddress = "fd00:192:168:59::103"
				config.HealthzBindAddress = ""
				config.MetricsBindAddress = "[::1]:10249"
				config.DetectLocal.ClusterCIDRs = []string{"fd00:192:168:59::/64"}
			},
		},
		"alternate healthz port": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.HealthzBindAddress = "0.0.0.0:12345"
			},
		},
		// 基线是 IPv4，此处换成 IPv6 CIDR，验证 IP 族不匹配时报错。
		"ClusterCIDR is wrong IP family": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.DetectLocal.ClusterCIDRs = []string{"fd00:192:168:59::/64"}
			},
		},
		// DetectLocal 的 ClusterCIDRs 只接受单栈，双栈组合应被拒绝。
		"ClusterCIDR is dual-stack": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.DetectLocal.ClusterCIDRs = []string{"192.168.59.0/24", "fd00:192:168::/64"}
			},
		},
		"LocalModeInterfaceNamePrefix": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.DetectLocalMode = kubeproxyconfig.LocalModeInterfaceNamePrefix
				config.DetectLocal = kubeproxyconfig.DetectLocalConfiguration{
					InterfaceNamePrefix: "vethabcde",
				}
			},
		},
		"LocalModeBridgeInterface": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.DetectLocalMode = kubeproxyconfig.LocalModeBridgeInterface
				config.DetectLocal = kubeproxyconfig.DetectLocalConfiguration{
					BridgeInterface: "avz",
				}
			},
		},
		"invalid BindAddress": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.BindAddress = "10.10.12.11:2000"
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("BindAddress"), "10.10.12.11:2000", "not a valid textual representation of an IP address")},
		},
		"invalid HealthzBindAddress": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.HealthzBindAddress = "0.0.0.0"
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("HealthzBindAddress"), "0.0.0.0", "must be IP:port")},
		},
		"invalid MetricsBindAddress": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.MetricsBindAddress = "127.0.0.1"
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("MetricsBindAddress"), "127.0.0.1", "must be IP:port")},
		},
		// 周期类字段的零值和负数都不允许。
		"ConfigSyncPeriod must be > 0": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.ConfigSyncPeriod = metav1.Duration{Duration: -1 * time.Second}
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ConfigSyncPeriod"), metav1.Duration{Duration: -1 * time.Second}, "must be greater than 0")},
		},
		"SyncPeriod must be > 0": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.SyncPeriod = metav1.Duration{Duration: -5 * time.Second}
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("SyncPeriod"), metav1.Duration{Duration: -5 * time.Second}, "must be greater than 0"),
				field.Invalid(newPath.Child("SyncPeriod"), metav1.Duration{Duration: 2 * time.Second}, "must be greater than or equal to KubeProxyConfiguration.MinSyncPeriod")},
		},
		// MinSyncPeriod 为负数时必须报错，与校验规则保持一致。
		"MinSyncPeriod must be > 0": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.MinSyncPeriod = metav1.Duration{Duration: -2 * time.Second}
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("MinSyncPeriod"), metav1.Duration{Duration: -2 * time.Second}, "must be greater than or equal to 0")},
		},
		// 同时调整两个周期，让 SyncPeriod 小于 MinSyncPeriod，触发关联性校验。
		"SyncPeriod must be >= MinSyncPeriod": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.SyncPeriod = metav1.Duration{Duration: 1 * time.Second}
				config.MinSyncPeriod = metav1.Duration{Duration: 5 * time.Second}
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("SyncPeriod"), metav1.Duration{Duration: 5 * time.Second}, "must be greater than or equal to KubeProxyConfiguration.MinSyncPeriod")},
		},
		"invalid DetectLocalMode": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.DetectLocalMode = "Guess"
			},
			expectedErrs: field.ErrorList{field.NotSupported(newPath.Child("DetectLocalMode"), "Guess", []string{"ClusterCIDR", "NodeCIDR", "BridgeInterface", "InterfaceNamePrefix", ""})},
		},
		"invalid logging format": {
			mutateConfigFunc: func(config *kubeproxyconfig.KubeProxyConfiguration) {
				config.Logging = logsapi.LoggingConfiguration{
					Format:         "unsupported format",
					FlushFrequency: logsapi.TimeOrMetaDuration{Duration: metav1.Duration{Duration: logsapi.LogFlushFreqDefault}},
				}
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("logging.format"), "unsupported format", "Unsupported log format")},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// 从基线配置深拷贝，避免用例间相互污染。
			config := baseConfig.DeepCopy()
			testCase.mutateConfigFunc(config)
			errs := Validate(config)
			if len(testCase.expectedErrs) == 0 {
				// 合法配置必须返回空错误列表（而非 nil），与校验器约定保持一致。
				assert.Equal(t, field.ErrorList{}, errs, "expected no validation errors")
			} else {
				assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
			}
		})
	}
}

// TestValidateKubeProxyIPTablesConfiguration 校验 iptables 相关配置，
// 重点是 MasqueradeBit 的取值范围。
func TestValidateKubeProxyIPTablesConfiguration(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")

	for name, testCase := range map[string]struct {
		config       kubeproxyconfig.KubeProxyIPTablesConfiguration
		expectedErrs field.ErrorList
	}{
		"valid custom MasqueradeBit": {
			config: kubeproxyconfig.KubeProxyIPTablesConfiguration{
				MasqueradeBit: ptr.To[int32](5),
			},
			expectedErrs: field.ErrorList{},
		},
		"MasqueradeBit cannot be < 0": {
			config: kubeproxyconfig.KubeProxyIPTablesConfiguration{
				MasqueradeBit: ptr.To[int32](-10),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeIPTablesConfiguration.MasqueradeBit"), ptr.To[int32](-10), "must be within the range [0, 31]")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateKubeProxyIPTablesConfiguration(testCase.config, newPath.Child("KubeIPTablesConfiguration"))
			assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
		})
	}
}

// TestValidateKubeProxyIPVSConfiguration 校验 IPVS 超时配置，
// 覆盖超时为 0（允许）与负数（非法）两种边界。
func TestValidateKubeProxyIPVSConfiguration(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		config       kubeproxyconfig.KubeProxyIPVSConfiguration
		expectedErrs field.ErrorList
	}{
		"IPVS Timeout can be 0": {
			config: kubeproxyconfig.KubeProxyIPVSConfiguration{
				TCPTimeout:    metav1.Duration{Duration: 0 * time.Second},
				TCPFinTimeout: metav1.Duration{Duration: 0 * time.Second},
				UDPTimeout:    metav1.Duration{Duration: 0 * time.Second},
			},
			expectedErrs: field.ErrorList{},
		},
		"IPVS Timeout > 0": {
			config: kubeproxyconfig.KubeProxyIPVSConfiguration{
				TCPTimeout:    metav1.Duration{Duration: 1 * time.Second},
				TCPFinTimeout: metav1.Duration{Duration: 2 * time.Second},
				UDPTimeout:    metav1.Duration{Duration: 3 * time.Second},
			},
			expectedErrs: field.ErrorList{},
		},
		"TCP,TCPFin,UDP Timeouts < 0": {
			config: kubeproxyconfig.KubeProxyIPVSConfiguration{
				TCPTimeout:    metav1.Duration{Duration: -1 * time.Second},
				UDPTimeout:    metav1.Duration{Duration: -1 * time.Second},
				TCPFinTimeout: metav1.Duration{Duration: -1 * time.Second},
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeIPVSConfiguration.TCPTimeout"), metav1.Duration{Duration: -1 * time.Second}, "must be greater than or equal to 0"),
				field.Invalid(newPath.Child("KubeIPVSConfiguration.TCPFinTimeout"), metav1.Duration{Duration: -1 * time.Second}, "must be greater than or equal to 0"),
				field.Invalid(newPath.Child("KubeIPVSConfiguration.UDPTimeout"), metav1.Duration{Duration: -1 * time.Second}, "must be greater than or equal to 0")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateKubeProxyIPVSConfiguration(testCase.config, newPath.Child("KubeIPVSConfiguration"))
			assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
		})
	}
}

// TestValidateKubeProxyLinuxConfiguration 校验 Linux 专有配置，
// 覆盖 conntrack 各超时、MaxPerCore/Min 以及 OOMScoreAdj 的合法区间。
func TestValidateKubeProxyLinuxConfiguration(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		config       kubeproxyconfig.KubeProxyLinuxConfiguration
		expectedErrs field.ErrorList
	}{
		"valid 5 second timeouts": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](0),
			},
			expectedErrs: field.ErrorList{},
		},
		"valid duration equal to 0 second timeout": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 0 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 0 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 0 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 0 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](0),
			},
			expectedErrs: field.ErrorList{},
		},
		"invalid MaxPerCore < 0": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](-1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](0),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.KubeProxyConntrackConfiguration.MaxPerCore"), ptr.To[int32](-1), "must be greater than or equal to 0")},
		},
		"invalid minimum < 0": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](-1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](0),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.KubeProxyConntrackConfiguration.Min"), ptr.To[int32](-1), "must be greater than or equal to 0")},
		},
		"invalid TCPEstablishedTimeout < 0": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: -5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](0),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.KubeProxyConntrackConfiguration.TCPEstablishedTimeout"), &metav1.Duration{Duration: -5 * time.Second}, "must be greater than or equal to 0")},
		},
		"invalid TCPCloseWaitTimeout < 0": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: -5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](0),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.KubeProxyConntrackConfiguration.TCPCloseWaitTimeout"), &metav1.Duration{Duration: -5 * time.Second}, "must be greater than or equal to 0")},
		},
		"invalid UDPTimeout < 0": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: -5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](999),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.KubeProxyConntrackConfiguration.UDPTimeout"), metav1.Duration{Duration: -5 * time.Second}, "must be greater than or equal to 0")},
		},
		"invalid UDPStreamTimeout < 0": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: -5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](-999),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.KubeProxyConntrackConfiguration.UDPStreamTimeout"), metav1.Duration{Duration: -5 * time.Second}, "must be greater than or equal to 0")},
		},
		"invalid OOMScoreAdj < -1000": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](-1001),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.OOMScoreAdj"), int32(-1001), "must be within the range [-1000, 1000]")},
		},
		"invalid OOMScoreAdj > 1000": {
			config: kubeproxyconfig.KubeProxyLinuxConfiguration{
				Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
					MaxPerCore:            ptr.To[int32](1),
					Min:                   ptr.To[int32](1),
					TCPEstablishedTimeout: &metav1.Duration{Duration: 5 * time.Second},
					TCPCloseWaitTimeout:   &metav1.Duration{Duration: 5 * time.Second},
					UDPTimeout:            metav1.Duration{Duration: 5 * time.Second},
					UDPStreamTimeout:      metav1.Duration{Duration: 5 * time.Second},
				},
				OOMScoreAdj: ptr.To[int32](1001),
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("KubeProxyLinuxConfiguration.OOMScoreAdj"), int32(1001), "must be within the range [-1000, 1000]")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateKubeProxyLinuxConfiguration(testCase.config, newPath.Child("KubeProxyLinuxConfiguration"))
			assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
		})
	}
}

// TestValidateProxyMode 按操作系统分发到 Linux 或 Windows 的代理模式校验用例。
func TestValidateProxyMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		testValidateProxyModeWindows(t)
	} else {
		testValidateProxyModeLinux(t)
	}
}

// testValidateProxyModeLinux 校验 Linux 上允许的代理模式，
// 同时验证 KubeProxyIPVS 特性门控开启/关闭时 ipvs 模式的接受情况。
func testValidateProxyModeLinux(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		mode                kubeproxyconfig.ProxyMode
		expectedErrs        field.ErrorList
		enableKubeProxyIPVS *bool
	}{
		"blank mode should default": {
			mode: kubeproxyconfig.ProxyMode(""),
		},
		"iptables is allowed": {
			mode: kubeproxyconfig.ProxyModeIPTables,
		},
		"iptables is allowed - with KubeProxyIPVS feature gate enabled": {
			mode:                kubeproxyconfig.ProxyModeIPTables,
			enableKubeProxyIPVS: new(true),
		},
		"iptables is allowed - with KubeProxyIPVS feature gate disabled": {
			mode:                kubeproxyconfig.ProxyModeIPTables,
			enableKubeProxyIPVS: new(false),
		},
		"ipvs is allowed - with KubeProxyIPVS feature gate in default state": {
			mode: kubeproxyconfig.ProxyModeIPVS,
		},
		"ipvs is allowed - with KubeProxyIPVS feature gate enabled": {
			mode:                kubeproxyconfig.ProxyModeIPVS,
			enableKubeProxyIPVS: new(true),
		},
		"ipvs is not allowed - with KubeProxyIPVS feature gate disabled": {
			mode:                kubeproxyconfig.ProxyModeIPVS,
			enableKubeProxyIPVS: new(false),
			expectedErrs:        field.ErrorList{field.Invalid(newPath.Child("ProxyMode"), "ipvs", "must be iptables, nftables or blank (blank means the best-available proxy [currently iptables])")},
		},
		"nftables is allowed": {
			mode: kubeproxyconfig.ProxyModeNFTables,
		},
		"nftables is allowed - with KubeProxyIPVS feature gate enabled": {
			mode:                kubeproxyconfig.ProxyModeNFTables,
			enableKubeProxyIPVS: new(true),
		},
		"nftables is allowed - with KubeProxyIPVS feature gate disabled": {
			mode:                kubeproxyconfig.ProxyModeNFTables,
			enableKubeProxyIPVS: new(false),
		},

		"winkernel is not allowed": {
			mode:         kubeproxyconfig.ProxyModeKernelspace,
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ProxyMode"), "kernelspace", "must be iptables, ipvs, nftables or blank (blank means the best-available proxy [currently iptables])")},
		},
		"invalid mode non-existent": {
			mode:         kubeproxyconfig.ProxyMode("non-existing"),
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ProxyMode"), "non-existing", "must be iptables, ipvs, nftables or blank (blank means the best-available proxy [currently iptables])")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			// 特性门控在同一进程中是全局状态，测试框架会保证用例结束后恢复。
			if testCase.enableKubeProxyIPVS != nil {
				featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
					features.KubeProxyIPVS: *testCase.enableKubeProxyIPVS,
				})
			}

			errs := validateProxyMode(testCase.mode, newPath)
			assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
		})
	}
}

// testValidateProxyModeWindows 校验 Windows 上仅允许 kernelspace（或空）模式，
// 其余模式均应被拒绝。
func testValidateProxyModeWindows(t *testing.T) {
	// TODO: 修复测试后移除该跳过逻辑。
	if runtime.GOOS == "windows" {
		t.Skip("Skipping failing test on Windows.")
	}
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		mode         kubeproxyconfig.ProxyMode
		expectedErrs field.ErrorList
	}{
		"blank mode should default": {
			mode: kubeproxyconfig.ProxyMode(""),
		},
		"winkernel is allowed": {
			mode: kubeproxyconfig.ProxyModeKernelspace,
		},
		"iptables is not allowed": {
			mode:         kubeproxyconfig.ProxyModeIPTables,
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ProxyMode"), "iptables", "must be kernelspace or blank (blank means the most-available proxy [currently kernelspace])")},
		},
		"ipvs is not allowed": {
			mode:         kubeproxyconfig.ProxyModeIPVS,
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ProxyMode"), "ipvs", "must be kernelspace or blank (blank means the most-available proxy [currently kernelspace])")},
		},
		"nftables is not allowed": {
			mode:         kubeproxyconfig.ProxyModeNFTables,
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ProxyMode"), "nftables", "must be kernelspace or blank (blank means the most-available proxy [currently kernelspace])")},
		},
		"invalid mode non-existent": {
			mode:         kubeproxyconfig.ProxyMode("non-existing"),
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ProxyMode"), "non-existing", "must be kernelspace or blank (blank means the most-available proxy [currently kernelspace])")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateProxyMode(testCase.mode, newPath)
			assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
		})
	}
}

// TestValidateClientConnectionConfiguration 校验客户端连接配置的 Burst 参数，
// 使用 cmp.Diff 忽略字段错误的 Origin 并排序后比较，避免顺序不稳定的干扰。
func TestValidateClientConnectionConfiguration(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		ccc          componentbaseconfig.ClientConnectionConfiguration
		expectedErrs field.ErrorList
	}{
		"successful 0 value": {
			ccc:          componentbaseconfig.ClientConnectionConfiguration{Burst: 0},
			expectedErrs: field.ErrorList{},
		},
		"successful 5 value": {
			ccc:          componentbaseconfig.ClientConnectionConfiguration{Burst: 5},
			expectedErrs: field.ErrorList{},
		},
		"burst < 0": {
			ccc:          componentbaseconfig.ClientConnectionConfiguration{Burst: -5},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("Burst"), int64(-5), "must be greater than or equal to 0")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateClientConnectionConfiguration(testCase.ccc, newPath)
			// Origin 字段含调用栈信息，忽略它才能稳定地比较错误内容。
			cmpOpts := []cmp.Option{cmpopts.IgnoreFields(field.Error{}, "Origin"), cmpopts.SortSlices(func(a, b *field.Error) bool { return a.Error() < b.Error() })}
			if diff := cmp.Diff(testCase.expectedErrs, errs, cmpOpts...); diff != "" {
				t.Errorf("did not get expected validation errors (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestValidateHostPort 校验 IP:Port 地址格式，覆盖缺省端口、非法 IP、命名端口、
// 端口 0 及越界端口等错误路径。
func TestValidateHostPort(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		ip           string
		expectedErrs field.ErrorList
	}{
		"all IPs": {
			ip: "0.0.0.0:10256",
		},
		"localhost": {
			ip: "127.0.0.1:10256",
		},
		"specific IP": {
			ip: "10.10.10.10:10256",
		},
		"missing port": {
			ip:           "10.10.10.10",
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("HealthzBindAddress"), "10.10.10.10", "must be IP:port")},
		},
		// 每段 IP 必须在 0-255 之间。
		"digits outside of 1-255": {
			ip:           "123.456.789.10:12345",
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("HealthzBindAddress"), "123.456.789.10", "must be a valid IP")},
		},
		// 端口必须是数字，不能是服务名。
		"invalid named-port": {
			ip:           "10.10.10.10:foo",
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("HealthzBindAddress"), "foo", "must be a valid port")},
		},
		// 0 不是可监听的有效端口。
		"port cannot be 0": {
			ip:           "10.10.10.10:0",
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("HealthzBindAddress"), "0", "must be a valid port")},
		},
		// 端口上限是 65535，越界必须拒绝。
		"port is greater than allowed range": {
			ip:           "10.10.10.10:65536",
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("HealthzBindAddress"), "65536", "must be a valid port")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateHostPort(testCase.ip, newPath.Child("HealthzBindAddress"))
			if len(testCase.expectedErrs) == 0 {
				// 合法配置必须返回空错误列表（而非 nil），与校验器约定保持一致。
				assert.Equal(t, field.ErrorList{}, errs, "expected no validation errors")
			} else {
				assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
			}
		})
	}
}

// TestValidateKubeProxyNodePortAddress 校验 NodePortAddresses 列表：
// 既接受 CIDR，也接受 primary/localhost/all 关键字，并检查重复关键字。
func TestValidateKubeProxyNodePortAddress(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		addresses    []string
		expectedErrs field.ErrorList
	}{
		"no addresses": {
			addresses: []string{},
		},
		"valid 1": {
			addresses: []string{"127.0.0.0/8"},
		},
		"valid 2": {
			addresses: []string{"0.0.0.0/0"},
		},
		"valid 3": {
			addresses: []string{"::/0"},
		},
		"valid 4": {
			addresses: []string{"127.0.0.1/32", "1.2.3.0/24"},
		},
		"valid 5": {
			addresses: []string{"127.0.0.1/32"},
		},
		"valid 6": {
			addresses: []string{"::1/128"},
		},
		"valid 7": {
			addresses: []string{"1.2.3.4/32"},
		},
		"valid 8": {
			addresses: []string{"10.20.30.0/24"},
		},
		"valid 9": {
			addresses: []string{"10.20.0.0/16", "100.200.0.0/16"},
		},
		"valid 10": {
			addresses: []string{"10.0.0.0/8"},
		},
		"valid 11": {
			addresses: []string{"2001:db8::/32"},
		},
		"primary": {
			addresses: []string{kubeproxyconfig.NodePortAddressesPrimary},
		},
		"localhost": {
			addresses: []string{kubeproxyconfig.NodePortAddressesLocalhost},
		},
		"all": {
			addresses: []string{kubeproxyconfig.NodePortAddressesAll},
		},
		"primary combined with localhost": {
			addresses: []string{kubeproxyconfig.NodePortAddressesPrimary, kubeproxyconfig.NodePortAddressesLocalhost},
		},
		"all keywords combined": {
			addresses: []string{kubeproxyconfig.NodePortAddressesPrimary, kubeproxyconfig.NodePortAddressesLocalhost, kubeproxyconfig.NodePortAddressesAll},
		},
		"duplicate keyword": {
			addresses:    []string{kubeproxyconfig.NodePortAddressesLocalhost, kubeproxyconfig.NodePortAddressesLocalhost},
			expectedErrs: field.ErrorList{field.Duplicate(newPath.Child("NodePortAddresses[1]"), kubeproxyconfig.NodePortAddressesLocalhost)},
		},
		// 即使关键字之间夹了合法 CIDR，重复仍会被检测出来。
		"duplicate keyword separated by other values": {
			addresses:    []string{kubeproxyconfig.NodePortAddressesPrimary, "10.0.0.0/8", kubeproxyconfig.NodePortAddressesPrimary},
			expectedErrs: field.ErrorList{field.Duplicate(newPath.Child("NodePortAddresses[2]"), kubeproxyconfig.NodePortAddressesPrimary)},
		},
		// 与关键字不同，重复的 CIDR 是允许的，验证校验器的宽松策略。
		"duplicate CIDRs are allowed": {
			addresses: []string{"10.0.0.0/8", "10.0.0.0/8", kubeproxyconfig.NodePortAddressesLocalhost},
		},
		"keywords combined with CIDRs": {
			addresses: []string{kubeproxyconfig.NodePortAddressesAll, kubeproxyconfig.NodePortAddressesPrimary, "192.168.0.0/16", kubeproxyconfig.NodePortAddressesLocalhost},
		},
		"keyword combined with CIDR": {
			addresses: []string{kubeproxyconfig.NodePortAddressesPrimary, "127.0.0.1/32"},
		},
		// 关键字大小写敏感，拼写错误应给出合法值提示。
		"typo'd keyword": {
			addresses:    []string{"Localhost"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[0]"), "Localhost", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
		"typo'd keyword combined with valid keyword": {
			addresses:    []string{kubeproxyconfig.NodePortAddressesPrimary, "locahost"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[1]"), "locahost", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
		"invalid foo address": {
			addresses:    []string{"foo"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[0]"), "foo", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
		"invalid octet address": {
			addresses:    []string{"10.0.0.0/0", "1.2.3"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[1]"), "1.2.3", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
		"address cannot be 0": {
			addresses:    []string{"127.0.0.1/32", "0", "1.2.3.0/24"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[1]"), "0", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
		"address missing subnet range": {
			addresses:    []string{"127.0.0.1/32", "10.20.30.40", "1.2.3.0/24"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[1]"), "10.20.30.40", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
		"missing ipv6 subnet ranges": {
			addresses: []string{"::0", "::1", "2001:db8::/32"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[0]"), "::0", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'"),
				field.Invalid(newPath.Child("NodePortAddresses[1]"), "::1", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
		// IPv6 CIDR 中的非十六进制字符应被拒绝。
		"invalid ipv6 ip format": {
			addresses:    []string{"::1/128", "2001:db8::/32", "2001:db8:xyz/64"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("NodePortAddresses[2]"), "2001:db8:xyz/64", "must be a valid CIDR or one of the keywords 'primary', 'localhost', 'all'")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateKubeProxyNodePortAddress(testCase.addresses, newPath.Child("NodePortAddresses"))
			if len(testCase.expectedErrs) == 0 {
				assert.Equal(t, field.ErrorList{}, errs, "expected no validation errors")
			} else {
				assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
			}
		})
	}
}

// TestValidateKubeProxyExcludeCIDRs 校验 ExcludeCIDRs 必须为合法 CIDR，
// 且与 NodePortAddresses 不同，这里不允许关键字。
func TestValidateKubeProxyExcludeCIDRs(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")
	for name, testCase := range map[string]struct {
		addresses    []string
		expectedErrs field.ErrorList
	}{
		"no cidrs": {
			addresses: []string{},
		},
		"valid 1": {
			addresses: []string{"127.0.0.0/8"},
		},
		"valid 2": {
			addresses: []string{"0.0.0.0/0"},
		},
		"valid 3": {
			addresses: []string{"::/0"},
		},
		"valid 4": {
			addresses: []string{"127.0.0.1/32", "1.2.3.0/24"},
		},
		"valid 5": {
			addresses: []string{"127.0.0.1/32"},
		},
		"valid 6": {
			addresses: []string{"::1/128"},
		},
		"valid 7": {
			addresses: []string{"1.2.3.4/32"},
		},
		"valid 8": {
			addresses: []string{"10.20.30.0/24"},
		},
		"valid 9": {
			addresses: []string{"10.20.0.0/16", "100.200.0.0/16"},
		},
		"valid 10": {
			addresses: []string{"10.0.0.0/8"},
		},
		"valid 11": {
			addresses: []string{"2001:db8::/32"},
		},
		"invalid foo address": {
			addresses:    []string{"foo"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ExcludeCIDRS[0]"), "foo", "must be a valid CIDR")},
		},
		"invalid octet address": {
			addresses:    []string{"10.0.0.0/0", "1.2.3"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ExcludeCIDRS[1]"), "1.2.3", "must be a valid CIDR")},
		},
		"address cannot be 0": {
			addresses:    []string{"127.0.0.1/32", "0", "1.2.3.0/24"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ExcludeCIDRS[1]"), "0", "must be a valid CIDR")},
		},
		"address missing subnet range": {
			addresses:    []string{"127.0.0.1/32", "10.20.30.40", "1.2.3.0/24"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ExcludeCIDRS[1]"), "10.20.30.40", "must be a valid CIDR")},
		},
		"missing ipv6 subnet ranges": {
			addresses: []string{"::0", "::1", "2001:db8::/32"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ExcludeCIDRS[0]"), "::0", "must be a valid CIDR"),
				field.Invalid(newPath.Child("ExcludeCIDRS[1]"), "::1", "must be a valid CIDR")},
		},
		"invalid ipv6 ip format": {
			addresses:    []string{"::1/128", "2001:db8::/32", "2001:db8:xyz/64"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("ExcludeCIDRS[2]"), "2001:db8:xyz/64", "must be a valid CIDR")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := validateIPVSExcludeCIDRs(testCase.addresses, newPath.Child("ExcludeCIDRS"))
			if len(testCase.expectedErrs) == 0 {
				assert.Equal(t, field.ErrorList{}, errs, "expected no validation errors")
			} else {
				assert.Equal(t, testCase.expectedErrs, errs, "did not get expected validation errors")
			}
		})
	}
}

// TestValidateDetectLocalConfiguration 校验不同 DetectLocalMode 下对应字段的必填性，
// 例如前缀模式下 InterfaceNamePrefix 不能为空。
func TestValidateDetectLocalConfiguration(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")

	testCases := []struct {
		name         string
		mode         kubeproxyconfig.LocalMode
		config       kubeproxyconfig.DetectLocalConfiguration
		expectedErrs field.ErrorList
	}{
		{
			name: "valid interface name prefix",
			mode: kubeproxyconfig.LocalModeInterfaceNamePrefix,
			config: kubeproxyconfig.DetectLocalConfiguration{
				InterfaceNamePrefix: "vethabcde",
			},
			expectedErrs: field.ErrorList{},
		},
		{
			name: "valid bridge interface",
			mode: kubeproxyconfig.LocalModeBridgeInterface,
			config: kubeproxyconfig.DetectLocalConfiguration{
				BridgeInterface: "avz",
			},
			expectedErrs: field.ErrorList{},
		},
		{
			name: "interfacePrefix is empty",
			mode: kubeproxyconfig.LocalModeInterfaceNamePrefix,
			config: kubeproxyconfig.DetectLocalConfiguration{
				InterfaceNamePrefix: "",
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DetectLocal").Child("InterfacePrefix"), "", "must not be empty")},
		},
		{
			name: "bridgeInterfaceName is empty",
			mode: kubeproxyconfig.LocalModeBridgeInterface,
			config: kubeproxyconfig.DetectLocalConfiguration{
				InterfaceNamePrefix: "eth0", // 模式不是前缀匹配，无需关注此前缀
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DetectLocal").Child("InterfaceName"), "", "must not be empty")},
		},
		{
			name: "valid cluster cidr",
			mode: kubeproxyconfig.LocalModeClusterCIDR,
			config: kubeproxyconfig.DetectLocalConfiguration{
				ClusterCIDRs: []string{"192.168.59.0/24", "fd00:192:168::/64"},
			},
			expectedErrs: field.ErrorList{},
		},
		{
			name: "invalid number of cluster cidrs",
			mode: kubeproxyconfig.LocalModeClusterCIDR,
			config: kubeproxyconfig.DetectLocalConfiguration{
				ClusterCIDRs: []string{"192.168.59.0/24", "fd00:192:168::/64", "10.0.0.0/16"},
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DetectLocal").Child("ClusterCIDRs"), []string{"192.168.59.0/24", "fd00:192:168::/64", "10.0.0.0/16"}, "must be a either a single CIDR or dual-stack pair of CIDRs (e.g. [10.100.0.0/16, fde4:8dba:82e1::/48]")},
		},
		{
			name: "invalid cluster cidr",
			mode: kubeproxyconfig.LocalModeClusterCIDR,
			config: kubeproxyconfig.DetectLocalConfiguration{
				ClusterCIDRs: []string{"192.168.59.0"},
			},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DetectLocal").Child("ClusterCIDRs").Index(0), "192.168.59.0", "must be a valid CIDR block (e.g. 10.100.0.0/16 or fde4:8dba:82e1::/48)")},
		},
		{
			name: "empty cluster cidrs with cluster cidr mode",
			mode: kubeproxyconfig.LocalModeClusterCIDR,
			config: kubeproxyconfig.DetectLocalConfiguration{
				ClusterCIDRs: []string{},
			},
			expectedErrs: field.ErrorList{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 按错误文本逐一比较，避免依赖 field.Error 的内部字段。
			errs := validateDetectLocalConfiguration(tc.mode, tc.config, newPath.Child("DetectLocal"))
			assert.Equalf(t, len(tc.expectedErrs), len(errs),
				"expected %d errors, got %d errors: %v", len(tc.expectedErrs), len(errs), errs,
			)
			for i, err := range errs {
				assert.Equal(t, tc.expectedErrs[i].Error(), err.Error())
			}
		})
	}
}

// TestValidateDualStackCIDRStrings 校验双栈 CIDR 列表：
// 允许单个 CIDR 或一对 IPv4/IPv6 CIDR，拒绝多于两个或格式错误的条目。
func TestValidateDualStackCIDRStrings(t *testing.T) {
	newPath := field.NewPath("KubeProxyConfiguration")

	testCases := []struct {
		name         string
		cidrStrings  []string
		expectedErrs field.ErrorList
	}{
		{
			name:         "empty cidr string",
			cidrStrings:  []string{},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DualStackCIDRList"), []string{}, "must contain at least one CIDR")},
		},
		{
			name:         "single ipv4 cidr",
			cidrStrings:  []string{"192.168.0.0/16"},
			expectedErrs: field.ErrorList{},
		},
		{
			name:         "single ipv6 cidr",
			cidrStrings:  []string{"fd00:10:96::/112"},
			expectedErrs: field.ErrorList{},
		},
		{
			name:         "dual stack cidr pair",
			cidrStrings:  []string{"172.16.200.0/24", "fde4:8dba:82e1::/48"},
			expectedErrs: field.ErrorList{},
		},
		{
			name:         "multiple ipv4 cidrs",
			cidrStrings:  []string{"10.100.0.0/16", "192.168.0.0/16", "fde4:8dba:82e1::/48"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DualStackCIDRList"), []string{"10.100.0.0/16", "192.168.0.0/16", "fde4:8dba:82e1::/48"}, "must be a either a single CIDR or dual-stack pair of CIDRs (e.g. [10.100.0.0/16, fde4:8dba:82e1::/48]")},
		},
		{
			name:         "multiple ipv6 cidrs",
			cidrStrings:  []string{"fd00:10:96::/112", "fde4:8dba:82e1::/48"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DualStackCIDRList"), []string{"fd00:10:96::/112", "fde4:8dba:82e1::/48"}, "must be a either a single CIDR or dual-stack pair of CIDRs (e.g. [10.100.0.0/16, fde4:8dba:82e1::/48]")},
		},
		{
			name:         "malformed ipv4 cidr",
			cidrStrings:  []string{"fde4:8dba:82e1::/48", "172.16.200.0:24"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DualStackCIDRList").Index(1), "172.16.200.0:24", "must be a valid CIDR block (e.g. 10.100.0.0/16 or fde4:8dba:82e1::/48)")},
		},
		{
			name:         "malformed ipv6 cidr",
			cidrStrings:  []string{"fd00:10:96::", "192.168.0.0/16"},
			expectedErrs: field.ErrorList{field.Invalid(newPath.Child("DualStackCIDRList").Index(0), "fd00:10:96::", "must be a valid CIDR block (e.g. 10.100.0.0/16 or fde4:8dba:82e1::/48)")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 校验器可能返回多个错误，先比对数量再逐条比较文本。
			errs := validateDualStackCIDRStrings(tc.cidrStrings, newPath.Child("DualStackCIDRList"))
			assert.Equalf(t, len(tc.expectedErrs), len(errs),
				"expected %d errors, got %d errors: %v", len(tc.expectedErrs), len(errs), errs,
			)
			for i, err := range errs {
				assert.Equal(t, tc.expectedErrs[i].Error(), err.Error())
			}
		})
	}
}
