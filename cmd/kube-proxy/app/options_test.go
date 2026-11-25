/*
Copyright 2024 The Kubernetes Authors.

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

package app

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	componentbaseconfig "k8s.io/component-base/config"
	logsapi "k8s.io/component-base/logs/api/v1"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/utils/ptr"
)

// TestLoadConfig 验证 loadConfig 对各种地址、协议族和 v1alpha1 兼容输入的正确解析。
func TestLoadConfig(t *testing.T) {

	yamlTemplate := `apiVersion: kubeproxy.config.k8s.io/v1alpha1
bindAddress: %s
clientConnection:
  acceptContentTypes: "abc"
  burst: 100
  contentType: content-type
  kubeconfig: "/path/to/kubeconfig"
  qps: 7
clusterCIDR: "%s"
configSyncPeriod: 15s
conntrack:
  maxPerCore: 2
  min: 1
  tcpCloseWaitTimeout: 10s
  tcpEstablishedTimeout: 20s
healthzBindAddress: "%s"
hostnameOverride: "foo"
iptables:
  masqueradeAll: true
  masqueradeBit: 17
  minSyncPeriod: 10s
  syncPeriod: 60s
  localhostNodePorts: true
ipvs:
  minSyncPeriod: 10s
  syncPeriod: 60s
  excludeCIDRs:
    - "10.20.30.40/16"
    - "fd00:1::0/64"
nftables:
  masqueradeAll: true
  masqueradeBit: 18
  minSyncPeriod: 10s
  syncPeriod: 60s
kind: KubeProxyConfiguration
metricsBindAddress: "%s"
mode: "%s"
oomScoreAdj: 17
portRange: "2-7"
detectLocalMode: "ClusterCIDR"
detectLocal:
  bridgeInterface: "cbr0"
  interfaceNamePrefix: "veth"
nodePortAddresses:
  - "10.20.30.40/16"
  - "fd00:1::0/64"
`

	testCases := []struct {
		name               string
		mode               string
		bindAddress        string
		clusterCIDR        string
		healthzBindAddress string
		metricsBindAddress string
		extraConfig        string
	}{
		{
			name:               "iptables mode, IPv4 all-zeros bind address",
			mode:               "iptables",
			bindAddress:        "0.0.0.0",
			clusterCIDR:        "1.2.3.0/24",
			healthzBindAddress: "1.2.3.4:12345",
			metricsBindAddress: "2.3.4.5:23456",
		},
		{
			name:               "iptables mode, non-zeros IPv4 config",
			mode:               "iptables",
			bindAddress:        "9.8.7.6",
			clusterCIDR:        "1.2.3.0/24",
			healthzBindAddress: "1.2.3.4:12345",
			metricsBindAddress: "2.3.4.5:23456",
		},
		{
			// 测试 IPv6 通配地址 'bindAddress: "::"'；'bindAddress: ::' 不是合法 YAML，必须加引号。
			name:               "iptables mode, IPv6 \"::\" bind address",
			mode:               "iptables",
			bindAddress:        "\"::\"",
			clusterCIDR:        "fd00:1::0/64",
			healthzBindAddress: "[fd00:1::5]:12345",
			metricsBindAddress: "[fd00:2::5]:23456",
		},
		{
			// 测试带方括号的 IPv6 通配地址 'bindAddress: "[::]"'；YAML 值仍需整体加引号。
			name:               "iptables mode, IPv6 \"[::]\" bind address",
			mode:               "iptables",
			bindAddress:        "\"[::]\"",
			clusterCIDR:        "fd00:1::0/64",
			healthzBindAddress: "[fd00:1::5]:12345",
			metricsBindAddress: "[fd00:2::5]:23456",
		},
		{
			// 测试 IPv6 通配地址的另一种写法 'bindAddress: ::0'，该值无需引号。
			name:               "iptables mode, IPv6 ::0 bind address",
			mode:               "iptables",
			bindAddress:        "::0",
			clusterCIDR:        "fd00:1::0/64",
			healthzBindAddress: "[fd00:1::5]:12345",
			metricsBindAddress: "[fd00:2::5]:23456",
		},
		{
			name:               "ipvs mode, IPv6 config",
			mode:               "ipvs",
			bindAddress:        "2001:db8::1",
			clusterCIDR:        "fd00:1::0/64",
			healthzBindAddress: "[fd00:1::5]:12345",
			metricsBindAddress: "[fd00:2::5]:23456",
		},
		{
			// v1alpha1 未知字段走宽松解码：记录严格解码警告，但不阻止配置加载。
			name:               "unknown field",
			mode:               "iptables",
			bindAddress:        "9.8.7.6",
			clusterCIDR:        "1.2.3.0/24",
			healthzBindAddress: "1.2.3.4:12345",
			metricsBindAddress: "2.3.4.5:23456",
			extraConfig:        "foo: bar",
		},
		{
			// v1alpha1 重复字段同样走宽松解码，记录警告而非直接加载失败。
			name:               "duplicate field",
			mode:               "iptables",
			bindAddress:        "9.8.7.6",
			clusterCIDR:        "1.2.3.0/24",
			healthzBindAddress: "1.2.3.4:12345",
			metricsBindAddress: "2.3.4.5:23456",
			extraConfig:        "bindAddress: 9.8.7.6",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expBindAddr := tc.bindAddress
			if tc.bindAddress[0] == '"' {
				// YAML 解析器会剥除外层双引号。
				expBindAddr = expBindAddr[1 : len(tc.bindAddress)-1]
			}
			expected := &kubeproxyconfig.KubeProxyConfiguration{
				BindAddress: expBindAddr,
				ClientConnection: componentbaseconfig.ClientConnectionConfiguration{
					AcceptContentTypes: "abc",
					Burst:              100,
					ContentType:        "content-type",
					Kubeconfig:         "/path/to/kubeconfig",
					QPS:                7,
				},
				MinSyncPeriod:    metav1.Duration{Duration: 10 * time.Second},
				SyncPeriod:       metav1.Duration{Duration: 60 * time.Second},
				ConfigSyncPeriod: metav1.Duration{Duration: 15 * time.Second},
				Linux: kubeproxyconfig.KubeProxyLinuxConfiguration{
					Conntrack: kubeproxyconfig.KubeProxyConntrackConfiguration{
						MaxPerCore:            ptr.To[int32](2),
						Min:                   ptr.To[int32](1),
						TCPCloseWaitTimeout:   &metav1.Duration{Duration: 10 * time.Second},
						TCPEstablishedTimeout: &metav1.Duration{Duration: 20 * time.Second},
					},
					MasqueradeAll: true,
					OOMScoreAdj:   ptr.To[int32](17),
				},
				FeatureGates:       map[string]bool{},
				HealthzBindAddress: tc.healthzBindAddress,
				HostnameOverride:   "foo",
				IPTables: kubeproxyconfig.KubeProxyIPTablesConfiguration{
					MasqueradeBit:      ptr.To[int32](17),
					LocalhostNodePorts: ptr.To(true),
				},
				IPVS: kubeproxyconfig.KubeProxyIPVSConfiguration{
					ExcludeCIDRs: []string{"10.20.30.40/16", "fd00:1::0/64"},
				},
				NFTables: kubeproxyconfig.KubeProxyNFTablesConfiguration{
					MasqueradeBit: ptr.To[int32](18),
				},
				MetricsBindAddress: tc.metricsBindAddress,
				Mode:               kubeproxyconfig.ProxyMode(tc.mode),
				NodePortAddresses:  []string{"10.20.30.40/16", "fd00:1::0/64"},
				DetectLocalMode:    kubeproxyconfig.LocalModeClusterCIDR,
				DetectLocal: kubeproxyconfig.DetectLocalConfiguration{
					BridgeInterface:     "cbr0",
					ClusterCIDRs:        strings.Split(tc.clusterCIDR, ","),
					InterfaceNamePrefix: "veth",
				},
				Logging: logsapi.LoggingConfiguration{
					Format:         "text",
					FlushFrequency: logsapi.TimeOrMetaDuration{Duration: metav1.Duration{Duration: 5 * time.Second}, SerializeAsString: true},
				},
			}

			options := NewOptions()

			baseYAML := fmt.Sprintf(
				yamlTemplate, tc.bindAddress, tc.clusterCIDR,
				tc.healthzBindAddress, tc.metricsBindAddress, tc.mode)

			// 在基础 YAML 模板后追加用例专属配置。
			yaml := fmt.Sprintf("%s\n%s", baseYAML, tc.extraConfig)

			config, err := options.loadConfig([]byte(yaml))

			require.NoError(t, err, "unexpected error for %s: %v", tc.name, err)

			if diff := cmp.Diff(config, expected); diff != "" {
				t.Fatalf("unexpected config, diff = %s", diff)
			}
		})
	}
}

// TestLoadConfigFailures 覆盖 loadConfig 的语法错误、类型错误与无效地址输入。
func TestLoadConfigFailures(t *testing.T) {
	// TODO(phenixblue): v1alpha2+ 配置支持严格解码后取消下方模板注释。
	// 当前仅有一个配置版本且使用宽松解码，启用这些断言会失败。
	/*
			yamlTemplate := `bindAddress: 0.0.0.0
		clusterCIDR: "1.2.3.0/24"
		configSyncPeriod: 15s
		kind: KubeProxyConfiguration`
	*/

	testCases := []struct {
		name    string
		config  string
		expErr  string
		checkFn func(err error) bool
	}{
		{
			name:   "Decode error test",
			config: "Twas bryllyg, and ye slythy toves",
			expErr: "could not find expected ':'",
		},
		{
			name:   "Bad config type test",
			config: "kind: KubeSchedulerConfiguration",
			expErr: "no kind",
		},
		{
			name:   "Missing quotes around :: bindAddress",
			config: "bindAddress: ::",
			expErr: "mapping values are not allowed in this context",
		},
		// TODO(phenixblue): v1alpha2+ 支持严格解码后启用下方用例；
		// 当前宽松解码路径不会对重复或未知字段返回严格解码错误。
		/*
			{
				name:    "Duplicate fields",
				config:  fmt.Sprintf("%s\nbindAddress: 1.2.3.4", yamlTemplate),
				checkFn: kuberuntime.IsStrictDecodingError,
			},
			{
				name:    "Unknown field",
				config:  fmt.Sprintf("%s\nfoo: bar", yamlTemplate),
				checkFn: kuberuntime.IsStrictDecodingError,
			},
		*/
	}

	version := "apiVersion: kubeproxy.config.k8s.io/v1alpha1"
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			options := NewOptions()
			config := fmt.Sprintf("%s\n%s", version, tc.config)
			_, err := options.loadConfig([]byte(config))

			require.Error(t, err, tc.name)
			require.ErrorContains(t, err, tc.expErr)

			if tc.checkFn != nil {
				require.True(t, tc.checkFn(err), tc.name)
			}
		})
	}
}

// TestProcessHostnameOverrideFlag 验证 hostname-override 的覆盖、规范化和空白拒绝规则。
func TestProcessHostnameOverrideFlag(t *testing.T) {
	testCases := []struct {
		name                 string
		hostnameOverrideFlag string
		expectedHostname     string
		expectError          bool
	}{
		{
			name:                 "Hostname from config file",
			hostnameOverrideFlag: "",
			expectedHostname:     "foo",
			expectError:          false,
		},
		{
			name:                 "Hostname from flag",
			hostnameOverrideFlag: "  bar ",
			expectedHostname:     "bar",
			expectError:          false,
		},
		{
			name:                 "Hostname is space",
			hostnameOverrideFlag: "   ",
			expectError:          true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			options := NewOptions()
			options.config = &kubeproxyconfig.KubeProxyConfiguration{
				HostnameOverride: "foo",
			}

			options.hostnameOverride = tc.hostnameOverrideFlag

			err := options.processHostnameOverrideFlag()
			if tc.expectError {
				if err == nil {
					t.Fatalf("should error for this case %s", tc.name)
				}
			} else {
				require.NoError(t, err, "unexpected error %v", err)
				if tc.expectedHostname != options.config.HostnameOverride {
					t.Fatalf("expected hostname: %s, but got: %s", tc.expectedHostname, options.config.HostnameOverride)
				}
			}
		})
	}
}

// TestProcessV1Alpha1Flags 验证 v1alpha1 兼容标志按代理模式分别合并。
func TestProcessV1Alpha1Flags(t *testing.T) {
	testCases := []struct {
		name     string
		flags    []string
		validate func(*kubeproxyconfig.KubeProxyConfiguration) bool
	}{
		{
			name: "iptables configuration",
			flags: []string{
				"--iptables-sync-period=36s",
				"--iptables-min-sync-period=3s",
				"--proxy-mode=iptables",
			},
			validate: func(config *kubeproxyconfig.KubeProxyConfiguration) bool {
				return config.SyncPeriod == metav1.Duration{Duration: 36 * time.Second} &&
					config.MinSyncPeriod == metav1.Duration{Duration: 3 * time.Second}
			},
		},
		{
			name: "iptables + ipvs configuration with iptables mode",
			flags: []string{
				"--iptables-sync-period=36s",
				"--iptables-min-sync-period=3s",
				"--ipvs-sync-period=16s",
				"--ipvs-min-sync-period=7s",
				"--proxy-mode=iptables",
			},
			validate: func(config *kubeproxyconfig.KubeProxyConfiguration) bool {
				return config.SyncPeriod == metav1.Duration{Duration: 36 * time.Second} &&
					config.MinSyncPeriod == metav1.Duration{Duration: 3 * time.Second}
			},
		},
		{
			name: "winkernel configuration",
			flags: []string{
				"--iptables-sync-period=36s",
				"--iptables-min-sync-period=3s",
				"--proxy-mode=kernelspace",
			},
			validate: func(config *kubeproxyconfig.KubeProxyConfiguration) bool {
				return config.SyncPeriod == metav1.Duration{Duration: 36 * time.Second} &&
					config.MinSyncPeriod == metav1.Duration{Duration: 3 * time.Second}
			},
		},
		{
			name: "ipvs + iptables configuration with ipvs mode",
			flags: []string{
				"--iptables-sync-period=36s",
				"--iptables-min-sync-period=3s",
				"--ipvs-sync-period=16s",
				"--ipvs-min-sync-period=7s",
				"--proxy-mode=ipvs",
			},
			validate: func(config *kubeproxyconfig.KubeProxyConfiguration) bool {
				return config.SyncPeriod == metav1.Duration{Duration: 16 * time.Second} &&
					config.MinSyncPeriod == metav1.Duration{Duration: 7 * time.Second}
			},
		},
		{
			name: "ipvs configuration",
			flags: []string{
				"--ipvs-sync-period=16s",
				"--ipvs-min-sync-period=7s",
				"--proxy-mode=ipvs",
			},
			validate: func(config *kubeproxyconfig.KubeProxyConfiguration) bool {
				return config.SyncPeriod == metav1.Duration{Duration: 16 * time.Second} &&
					config.MinSyncPeriod == metav1.Duration{Duration: 7 * time.Second}
			},
		},
		{
			name: "cluster cidr",
			flags: []string{
				"--cluster-cidr=2002:0:0:1234::/64,10.0.0.0/14",
			},
			validate: func(config *kubeproxyconfig.KubeProxyConfiguration) bool {
				return reflect.DeepEqual(config.DetectLocal.ClusterCIDRs, []string{"2002:0:0:1234::/64", "10.0.0.0/14"})
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			options := NewOptions()
			fs := new(pflag.FlagSet)
			options.AddFlags(fs)
			require.NoError(t, fs.Parse(tc.flags))
			options.processV1Alpha1Flags(fs)
			require.True(t, tc.validate(options.config))
		})
	}
}

// TestOptionsComplete 验证命令行标志与配置文件按既定优先级正确合并。
func TestOptionsComplete(t *testing.T) {
	header := `apiVersion: kubeproxy.config.k8s.io/v1alpha1
kind: KubeProxyConfiguration
`

	// 先取得默认配置；具体值取决于平台默认值。
	o := NewOptions()
	require.NoError(t, o.Complete(new(pflag.FlagSet)))
	expected := o.config

	config := header + `logging:
  format: json
  flushFrequency: 1s
  verbosity: 10
  vmodule:
  - filePattern: foo.go
    verbosity: 6
  - filePattern: bar.go
    verbosity: 8
`
	expectedLoggingConfig := logsapi.LoggingConfiguration{
		Format:         "json",
		FlushFrequency: logsapi.TimeOrMetaDuration{Duration: metav1.Duration{Duration: time.Second}, SerializeAsString: true},
		Verbosity:      10,
		VModule: []logsapi.VModuleItem{
			{
				FilePattern: "foo.go",
				Verbosity:   6,
			},
			{
				FilePattern: "bar.go",
				Verbosity:   8,
			},
		},
		Options: logsapi.FormatOptions{
			JSON: logsapi.JSONOptions{
				OutputRoutingOptions: logsapi.OutputRoutingOptions{
					InfoBufferSize: resource.QuantityValue{Quantity: resource.MustParse("0")},
				},
			},
			Text: logsapi.TextOptions{
				OutputRoutingOptions: logsapi.OutputRoutingOptions{
					InfoBufferSize: resource.QuantityValue{Quantity: resource.MustParse("0")},
				},
			},
		},
	}

	for name, tc := range map[string]struct {
		config   string
		flags    []string
		expected *kubeproxyconfig.KubeProxyConfiguration
	}{
		"empty": {
			expected: expected,
		},
		"conntrack": {
			flags: []string{
				"--conntrack-max-per-core=0",
				"--conntrack-min=0",
				"--conntrack-tcp-timeout-established=0",
				"--conntrack-tcp-timeout-close-wait=0",
			},
			expected: func() *kubeproxyconfig.KubeProxyConfiguration {
				c := expected.DeepCopy()
				c.Linux.Conntrack.MaxPerCore = ptr.To(int32(0))
				c.Linux.Conntrack.Min = ptr.To(int32(0))
				c.Linux.Conntrack.TCPEstablishedTimeout = ptr.To(metav1.Duration{})
				c.Linux.Conntrack.TCPCloseWaitTimeout = ptr.To(metav1.Duration{})
				return c
			}(),
		},
		"empty-config": {
			config:   header,
			expected: expected,
		},
		"logging-config": {
			config: config,
			expected: func() *kubeproxyconfig.KubeProxyConfiguration {
				c := expected.DeepCopy()
				c.Logging = *expectedLoggingConfig.DeepCopy()
				return c
			}(),
		},
		"flags": {
			flags: []string{
				"-v=7",
				"--vmodule", "goo.go=8",
			},
			expected: func() *kubeproxyconfig.KubeProxyConfiguration {
				c := expected.DeepCopy()
				c.Logging.Verbosity = 7
				c.Logging.VModule = append(c.Logging.VModule, logsapi.VModuleItem{
					FilePattern: "goo.go",
					Verbosity:   8,
				})
				return c
			}(),
		},
		"both": {
			config: config,
			flags: []string{
				"-v=7",
				"--vmodule", "goo.go=8",
				"--ipvs-scheduler", "some-scheduler", // 会被配置文件覆盖。
			},
			expected: func() *kubeproxyconfig.KubeProxyConfiguration {
				c := expected.DeepCopy()
				c.Logging = *expectedLoggingConfig.DeepCopy()
				// 命令行标志优先。
				c.Logging.Verbosity = 7
				// 合并配置时命令行标志优先。
				c.Logging.VModule = append([]logsapi.VModuleItem{
					{
						FilePattern: "goo.go",
						Verbosity:   8,
					},
				}, c.Logging.VModule...)
				return c
			}(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			options := NewOptions()
			fs := new(pflag.FlagSet)
			options.AddFlags(fs)
			flags := tc.flags
			if len(tc.config) > 0 {
				tmp := t.TempDir()
				configFile := path.Join(tmp, "kube-proxy.conf")
				require.NoError(t, os.WriteFile(configFile, []byte(tc.config), 0666))
				flags = append(flags, "--config", configFile)
			}
			require.NoError(t, fs.Parse(flags))
			require.NoError(t, options.Complete(fs))
			require.Equal(t, tc.expected, options.config)
		})
	}
}

// TestAddressFromDeprecatedFlags 验证已弃用端口参数对 IPv4、IPv6 监听地址的合并规则。
func TestAddressFromDeprecatedFlags(t *testing.T) {
	testCases := []struct {
		name               string
		healthzPort        int32
		healthzBindAddress string
		metricsPort        int32
		metricsBindAddress string
		expHealthz         string
		expMetrics         string
	}{
		{
			name:               "IPv4 bind address",
			healthzBindAddress: "1.2.3.4",
			healthzPort:        12345,
			metricsBindAddress: "2.3.4.5",
			metricsPort:        23456,
			expHealthz:         "1.2.3.4:12345",
			expMetrics:         "2.3.4.5:23456",
		},
		{
			name:               "IPv4 bind address has port",
			healthzBindAddress: "1.2.3.4:12345",
			healthzPort:        23456,
			metricsBindAddress: "2.3.4.5:12345",
			metricsPort:        23456,
			expHealthz:         "1.2.3.4:12345",
			expMetrics:         "2.3.4.5:12345",
		},
		{
			name:               "IPv6 bind address",
			healthzBindAddress: "fd00:1::5",
			healthzPort:        12345,
			metricsBindAddress: "fd00:1::6",
			metricsPort:        23456,
			expHealthz:         "[fd00:1::5]:12345",
			expMetrics:         "[fd00:1::6]:23456",
		},
		{
			name:               "IPv6 bind address has port",
			healthzBindAddress: "[fd00:1::5]:12345",
			healthzPort:        56789,
			metricsBindAddress: "[fd00:1::6]:56789",
			metricsPort:        12345,
			expHealthz:         "[fd00:1::5]:12345",
			expMetrics:         "[fd00:1::6]:56789",
		},
		{
			name:               "Invalid IPv6 Config",
			healthzBindAddress: "[fd00:1::5]",
			healthzPort:        12345,
			metricsBindAddress: "[fd00:1::6]",
			metricsPort:        56789,
			expHealthz:         "[fd00:1::5]",
			expMetrics:         "[fd00:1::6]",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotHealthz := addressFromDeprecatedFlags(tc.healthzBindAddress, tc.healthzPort)
			gotMetrics := addressFromDeprecatedFlags(tc.metricsBindAddress, tc.metricsPort)

			require.Equal(t, tc.expHealthz, gotHealthz)
			require.Equal(t, tc.expMetrics, gotMetrics)
		})
	}
}
