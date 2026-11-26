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

package rest

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"
	clientauthenticationapi "k8s.io/client-go/pkg/apis/clientauthentication"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/transport"
	"k8s.io/client-go/util/flowcontrol"
	"sigs.k8s.io/randfill"
)

// TestConfigToExecCluster 验证把 REST Config 转换为 exec 插件可见的 Cluster
// 信息时的各条路径：CA 来自内存/文件、无 CA、proxy 为 nil/报错/返回 nil，
// 以及非法 Host 的错误前缀。
func TestConfigToExecCluster(t *testing.T) {
	t.Parallel()

	const proxyURL = "https://some-proxy-url.com/tuna/fish"
	proxy := func(r *http.Request) (*url.URL, error) {
		return url.Parse(proxyURL)
	}

	tests := []struct {
		name            string
		in              Config
		wantOut         clientauthenticationapi.Cluster
		wantErrorPrefix string
	}{
		{
			name: "CA data from memory",
			in: Config{
				ExecProvider: &clientcmdapi.ExecConfig{
					ProvideClusterInfo: true,
					Config: &runtime.Unknown{
						Raw: []byte("stuff"),
					},
				},
				Host: "some-host",
				TLSClientConfig: TLSClientConfig{
					ServerName: "some-server-name",
					Insecure:   true,
					CAData:     []byte("some-ca-data"),
				},
				Proxy: proxy,
			},
			wantOut: clientauthenticationapi.Cluster{
				Server:                   "some-host",
				TLSServerName:            "some-server-name",
				InsecureSkipTLSVerify:    true,
				CertificateAuthorityData: []byte("some-ca-data"),
				ProxyURL:                 proxyURL,
				Config: &runtime.Unknown{
					Raw: []byte("stuff"),
				},
			},
		},
		{
			name: "CA data from file",
			in: Config{
				ExecProvider: &clientcmdapi.ExecConfig{
					ProvideClusterInfo: true,
					Config: &runtime.Unknown{
						Raw: []byte("stuff"),
					},
				},
				Host: "some-host",
				TLSClientConfig: TLSClientConfig{
					ServerName: "some-server-name",
					Insecure:   true,
					CAFile:     "testdata/ca.pem",
				},
				Proxy: proxy,
			},
			wantOut: clientauthenticationapi.Cluster{
				Server:                   "some-host",
				TLSServerName:            "some-server-name",
				InsecureSkipTLSVerify:    true,
				CertificateAuthorityData: []byte("a CA bundle lives here"),
				ProxyURL:                 proxyURL,
				Config: &runtime.Unknown{
					Raw: []byte("stuff"),
				},
			},
		},
		{
			name: "no CA data",
			in: Config{
				ExecProvider: &clientcmdapi.ExecConfig{
					ProvideClusterInfo: true,
				},
				TLSClientConfig: TLSClientConfig{
					CAFile: "this-file-does-not-exist",
				},
			},
			wantErrorPrefix: "failed to load CA bundle for execProvider: ",
		},
		{
			name: "nil proxy",
			in: Config{
				ExecProvider: &clientcmdapi.ExecConfig{
					ProvideClusterInfo: true,
					Config: &runtime.Unknown{
						Raw: []byte("stuff"),
					},
				},
				Host: "some-host",
				TLSClientConfig: TLSClientConfig{
					ServerName: "some-server-name",
					Insecure:   true,
					CAFile:     "testdata/ca.pem",
				},
			},
			wantOut: clientauthenticationapi.Cluster{
				Server:                   "some-host",
				TLSServerName:            "some-server-name",
				InsecureSkipTLSVerify:    true,
				CertificateAuthorityData: []byte("a CA bundle lives here"),
				Config: &runtime.Unknown{
					Raw: []byte("stuff"),
				},
			},
		},
		{
			name: "bad proxy",
			in: Config{
				ExecProvider: &clientcmdapi.ExecConfig{
					ProvideClusterInfo: true,
				},
				Proxy: func(_ *http.Request) (*url.URL, error) {
					return nil, errors.New("some proxy error")
				},
			},
			wantErrorPrefix: "failed to get proxy URL for execProvider: some proxy error",
		},
		{
			name: "proxy returns nil",
			in: Config{
				ExecProvider: &clientcmdapi.ExecConfig{
					ProvideClusterInfo: true,
				},
				Proxy: func(_ *http.Request) (*url.URL, error) {
					return nil, nil
				},
				Host: "some-host",
				TLSClientConfig: TLSClientConfig{
					ServerName: "some-server-name",
					Insecure:   true,
					CAFile:     "testdata/ca.pem",
				},
			},
			wantOut: clientauthenticationapi.Cluster{
				Server:                   "some-host",
				TLSServerName:            "some-server-name",
				InsecureSkipTLSVerify:    true,
				CertificateAuthorityData: []byte("a CA bundle lives here"),
			},
		},
		{
			name: "invalid config host",
			in: Config{
				ExecProvider: &clientcmdapi.ExecConfig{
					ProvideClusterInfo: true,
				},
				Proxy: func(_ *http.Request) (*url.URL, error) {
					return nil, nil
				},
				Host: "invalid-config-host\n",
			},
			wantErrorPrefix: "failed to create proxy URL request for execProvider: ",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := ConfigToExecCluster(&test.in)
			if test.wantErrorPrefix != "" {
				if err == nil {
					t.Error("wanted error")
				} else if !strings.HasPrefix(err.Error(), test.wantErrorPrefix) {
					t.Errorf("wanted error prefix %q, got %q", test.wantErrorPrefix, err.Error())
				}
			} else if diff := cmp.Diff(&test.wantOut, out); diff != "" {
				t.Errorf("unexpected returned cluster: -got, +want:\n %s", diff)
			}
		})
	}
}

// TestConfigToExecClusterRoundtrip 用随机填充的 Config 做 Config -> Cluster -> Config
// 往返，确保转换不会丢字段；函数类型的字段需要注入可比较的假实现。
func TestConfigToExecClusterRoundtrip(t *testing.T) {
	t.Parallel()

	// NilChance/NumElements 控制随机数据的稀疏度，减少无意义的大集合。
	f := randfill.New().NilChance(0.5).NumElements(1, 1)
	f.Funcs(
		func(r *runtime.Codec, f randfill.Continue) {
			codec := &fakeCodec{}
			f.Fill(codec)
			*r = codec
		},
		func(r *http.RoundTripper, f randfill.Continue) {
			roundTripper := &fakeRoundTripper{}
			f.Fill(roundTripper)
			*r = roundTripper
		},
		func(fn *func(http.RoundTripper) http.RoundTripper, f randfill.Continue) {
			*fn = fakeWrapperFunc
		},
		func(fn *transport.WrapperFunc, f randfill.Continue) {
			*fn = fakeWrapperFunc
		},
		func(r *runtime.NegotiatedSerializer, f randfill.Continue) {
			serializer := &fakeNegotiatedSerializer{}
			f.Fill(serializer)
			*r = serializer
		},
		func(r *flowcontrol.RateLimiter, f randfill.Continue) {
			limiter := &fakeLimiter{}
			f.Fill(limiter)
			*r = limiter
		},
		func(h *WarningHandler, f randfill.Continue) {
			*h = &fakeWarningHandler{}
		},
		func(h *WarningHandlerWithContext, f randfill.Continue) {
			*h = &fakeWarningHandlerWithContext{}
		},
		// 认证字段无需 fuzz。
		func(r *AuthProviderConfigPersister, f randfill.Continue) {},
		func(r *clientcmdapi.AuthProviderConfig, f randfill.Continue) {
			r.Config = map[string]string{}
		},
		func(r *func(ctx context.Context, network, addr string) (net.Conn, error), f randfill.Continue) {
			*r = fakeDialFunc
		},
		func(r *func(*http.Request) (*url.URL, error), f randfill.Continue) {
			*r = fakeProxyFunc
		},
		func(r *runtime.Object, f randfill.Continue) {
			unknown := &runtime.Unknown{}
			f.Fill(unknown)
			*r = unknown
		},
	)
	for i := 0; i < 100; i++ {
		expected := &Config{}
		f.Fill(expected)

		// 以下字段不参与往返比较；新增不希望被往返转换的字段时应加入此列表。
		expected.APIPath = ""
		expected.ContentConfig = ContentConfig{}
		expected.Username = ""
		expected.Password = ""
		expected.BearerToken = ""
		expected.BearerTokenFile = ""
		expected.Impersonate = ImpersonationConfig{}
		expected.AuthProvider = nil
		expected.AuthConfigPersister = nil
		expected.ExecProvider = &clientcmdapi.ExecConfig{} // ConfigToExecCluster assumes != nil.
		expected.TLSClientConfig.CertFile = ""
		expected.TLSClientConfig.KeyFile = ""
		expected.TLSClientConfig.CAFile = ""
		expected.TLSClientConfig.CertData = nil
		expected.TLSClientConfig.KeyData = nil
		expected.TLSClientConfig.NextProtos = nil
		expected.UserAgent = ""
		expected.DisableCompression = false
		expected.Transport = nil
		expected.WrapTransport = nil
		expected.QPS = 0.0
		expected.Burst = 0
		expected.RateLimiter = nil
		expected.WarningHandler = nil
		expected.WarningHandlerWithContext = nil
		expected.Timeout = 0
		expected.Dial = nil

		// 手工设置 URL，避免往返解析时报错。
		if expected.Host != "" {
			expected.Host = "https://some-server-url.com/tuna/fish"
		}
		if expected.Proxy != nil {
			expected.Proxy = func(_ *http.Request) (*url.URL, error) {
				return url.Parse("https://some-proxy-url.com/tuna/fish")
			}
		}

		cluster, err := ConfigToExecCluster(expected)
		if err != nil {
			t.Fatal(err)
		}

		// 反向转换会丢弃 ExecProvider（它本身就是输入），因此这里不比较。
		actual, err := ExecClusterToConfig(cluster)
		if err != nil {
			t.Fatal(err)
		}

		if actual.Proxy != nil {
			actualURL, actualErr := actual.Proxy(nil)
			expectedURL, expectedErr := expected.Proxy(nil)
			if actualErr != nil {
				t.Fatalf("failed to get url from actual proxy func: %s", actualErr.Error())
			}
			if expectedErr != nil {
				t.Fatalf("failed to get url from expected proxy func: %s", actualErr.Error())
			}
			if diff := cmp.Diff(actualURL, expectedURL); diff != "" {
				t.Fatal("we dropped the Config.Proxy field during conversion")
			}
		}
		actual.Proxy = nil
		expected.Proxy = nil

		if actual.ExecProvider != nil {
			t.Fatal("expected actual Config.ExecProvider field to be set to nil")
		}
		actual.ExecProvider = nil
		expected.ExecProvider = nil

		if diff := cmp.Diff(actual, expected); diff != "" {
			t.Fatalf("we dropped some Config fields during roundtrip, -got, +want:\n %s", diff)
		}
	}
}

// TestExecClusterToConfigRoundtrip 验证 Cluster -> Config -> Cluster 往返稳定；
// Cluster.Config 不是 Config 能表达的字段，因此随机填充时固定为 nil。
func TestExecClusterToConfigRoundtrip(t *testing.T) {
	t.Parallel()

	f := randfill.New().NilChance(0.5).NumElements(1, 1)
	f.Funcs(
		func(r *runtime.Object, f randfill.Continue) {
			// clientauthentication.Cluster.Config 不会出现在往返后的 Config 中，因此固定设为 nil。
			*r = nil
		},
	)
	for i := 0; i < 100; i++ {
		expected := &clientauthenticationapi.Cluster{}
		f.Fill(expected)

		// 手工设置 URL，避免往返解析时报错。
		if expected.Server != "" {
			expected.Server = "https://some-server-url.com/tuna/fish"
		}
		if expected.ProxyURL != "" {
			expected.ProxyURL = "https://some-proxy-url.com/tuna/fish"
		}

		config, err := ExecClusterToConfig(expected)
		if err != nil {
			t.Fatal(err)
		}

		// ConfigToExecCluster 假定 config.ExecProvider 非 nil。
		config.ExecProvider = &clientcmdapi.ExecConfig{}

		actual, err := ConfigToExecCluster(config)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(actual, expected); diff != "" {
			t.Fatalf("we dropped some Cluster fields during roundtrip: -got, +want:\n %s", diff)
		}
	}
}
