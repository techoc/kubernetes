/*
Copyright 2016 The Kubernetes Authors.

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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/transport"
	"k8s.io/client-go/util/flowcontrol"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/randfill"
)

// TestIsConfigTransportTLS 覆盖 IsConfigTransportTLS 的判定分支：
// https scheme、显式证书、Insecure，以及非法 URL 的回退行为。
func TestIsConfigTransportTLS(t *testing.T) {
	testCases := []struct {
		Config       *Config
		TransportTLS bool
	}{
		{
			Config:       &Config{},
			TransportTLS: false,
		},
		{
			Config: &Config{
				Host: "https://localhost",
			},
			TransportTLS: true,
		},
		{
			Config: &Config{
				Host: "localhost",
				TLSClientConfig: TLSClientConfig{
					CertFile: "foo",
				},
			},
			TransportTLS: true,
		},
		{
			Config: &Config{
				Host: "///:://localhost",
				TLSClientConfig: TLSClientConfig{
					CertFile: "foo",
				},
			},
			TransportTLS: false,
		},
		{
			Config: &Config{
				Host: "1.2.3.4:567",
				TLSClientConfig: TLSClientConfig{
					Insecure: true,
				},
			},
			TransportTLS: true,
		},
	}
	for _, testCase := range testCases {
		if err := SetKubernetesDefaults(testCase.Config); err != nil {
			t.Errorf("setting defaults failed for %#v: %v", testCase.Config, err)
			continue
		}
		useTLS := IsConfigTransportTLS(*testCase.Config)
		if testCase.TransportTLS != useTLS {
			t.Errorf("expected %v for %#v", testCase.TransportTLS, testCase.Config)
		}
	}
}

// TestSetKubernetesDefaultsUserAgent 验证默认 User-Agent 会被设置为包含 "kubernetes/" 的字符串。
func TestSetKubernetesDefaultsUserAgent(t *testing.T) {
	config := &Config{}
	if err := SetKubernetesDefaults(config); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !strings.Contains(config.UserAgent, "kubernetes/") {
		t.Errorf("no user agent set: %#v", config)
	}
}

// TestAdjustVersion 验证版本号会被裁剪为三段，且预发布后缀被丢弃。
func TestAdjustVersion(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("1.2.3", adjustVersion("1.2.3-alpha4"))
	assert.Equal("1.2.3", adjustVersion("1.2.3-alpha"))
	assert.Equal("1.2.3", adjustVersion("1.2.3"))
	assert.Equal("unknown", adjustVersion(""))
}

// TestAdjustCommit 验证 commit 被截断为 7 位，空值返回 "unknown"。
func TestAdjustCommit(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("1234567", adjustCommit("1234567890"))
	assert.Equal("123456", adjustCommit("123456"))
	assert.Equal("unknown", adjustCommit(""))
}

// TestAdjustCommand 验证命令行只保留可执行文件名（base name），空值返回 "unknown"。
func TestAdjustCommand(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("beans", adjustCommand(filepath.Join("home", "bob", "Downloads", "beans")))
	assert.Equal("beans", adjustCommand(filepath.Join(".", "beans")))
	assert.Equal("beans", adjustCommand("beans"))
	assert.Equal("unknown", adjustCommand(""))
}

// TestBuildUserAgent 验证 User-Agent 的拼接格式：client/product (os/arch) kubernetes/commit。
func TestBuildUserAgent(t *testing.T) {
	assert.New(t).Equal(
		"lynx/nicest (beos/itanium) kubernetes/baaaaaaaaad",
		buildUserAgent(
			"lynx", "nicest",
			"beos", "itanium", "baaaaaaaaad"))
}

// 该函数不接受参数，无法直接测试。
//
// TestDefaultKubernetesUserAgent 仅验证默认 UA 中包含 "kubernetes"。
func TestDefaultKubernetesUserAgent(t *testing.T) {
	assert.New(t).Contains(DefaultKubernetesUserAgent(), "kubernetes")
}

// TestRESTClientRequires 验证构造 RESTClient 时必须同时提供 NegotiatedSerializer 与 GroupVersion。
func TestRESTClientRequires(t *testing.T) {
	if _, err := RESTClientFor(&Config{Host: "127.0.0.1", ContentConfig: ContentConfig{NegotiatedSerializer: scheme.Codecs}}); err == nil {
		t.Errorf("unexpected non-error")
	}
	if _, err := RESTClientFor(&Config{Host: "127.0.0.1", ContentConfig: ContentConfig{GroupVersion: &v1.SchemeGroupVersion}}); err == nil {
		t.Errorf("unexpected non-error")
	}
	if _, err := RESTClientFor(&Config{Host: "127.0.0.1", ContentConfig: ContentConfig{GroupVersion: &v1.SchemeGroupVersion, NegotiatedSerializer: scheme.Codecs}}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestRESTClientLimiter 验证 rate limiter 的选择：默认 QPS、显式 QPS、QPS=-1 表示不限流、
// 以及显式 RateLimiter 优先。versioned 与 unversioned 客户端都要覆盖。
func TestRESTClientLimiter(t *testing.T) {
	testCases := []struct {
		Name    string
		Config  Config
		Limiter flowcontrol.RateLimiter
	}{
		{
			Name:    "with no QPS",
			Config:  Config{},
			Limiter: flowcontrol.NewTokenBucketRateLimiter(5, 10),
		},
		{
			Name:    "with QPS:10",
			Config:  Config{QPS: 10},
			Limiter: flowcontrol.NewTokenBucketRateLimiter(10, 10),
		},
		{
			Name:    "with QPS:-1",
			Config:  Config{QPS: -1},
			Limiter: nil,
		},
		{
			Name: "with RateLimiter",
			Config: Config{
				RateLimiter: flowcontrol.NewTokenBucketRateLimiter(11, 12),
			},
			Limiter: flowcontrol.NewTokenBucketRateLimiter(11, 12),
		},
	}
	for _, testCase := range testCases {
		t.Run("Versioned_"+testCase.Name, func(t *testing.T) {
			config := testCase.Config
			config.Host = "127.0.0.1"
			config.ContentConfig = ContentConfig{GroupVersion: &v1.SchemeGroupVersion, NegotiatedSerializer: scheme.Codecs}
			client, err := RESTClientFor(&config)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(testCase.Limiter, client.rateLimiter) {
				t.Fatalf("unexpected rate limiter: %#v, expected %#v at %s", client.rateLimiter, testCase.Limiter, testCase.Name)
			}
		})
		t.Run("Unversioned_"+testCase.Name, func(t *testing.T) {
			config := testCase.Config
			config.Host = "127.0.0.1"
			config.ContentConfig = ContentConfig{GroupVersion: &v1.SchemeGroupVersion, NegotiatedSerializer: scheme.Codecs}
			client, err := UnversionedRESTClientFor(&config)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(testCase.Limiter, client.rateLimiter) {
				t.Fatalf("unexpected rate limiter: %#v, expected %#v at %s", client.rateLimiter, testCase.Limiter, testCase.Name)
			}
		})
	}
}

// fakeLimiter 是测试用的 RateLimiter，返回预设的 Saturation/QPS。
type fakeLimiter struct {
	FakeSaturation float64
	FakeQPS        float32
}

func (t *fakeLimiter) TryAccept() bool {
	return true
}

func (t *fakeLimiter) Saturation() float64 {
	return t.FakeSaturation
}

func (t *fakeLimiter) QPS() float32 {
	return t.FakeQPS
}

func (t *fakeLimiter) Wait(ctx context.Context) error {
	return nil
}

func (t *fakeLimiter) Stop() {}

func (t *fakeLimiter) Accept() {}

// fakeCodec 是仅实现接口、不做实际编解码的 codec。
type fakeCodec struct{}

func (c *fakeCodec) Decode([]byte, *schema.GroupVersionKind, runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	return nil, nil, nil
}

func (c *fakeCodec) Encode(obj runtime.Object, stream io.Writer) error {
	return nil
}

func (c *fakeCodec) Identifier() runtime.Identifier {
	return runtime.Identifier("fake")
}

// fakeRoundTripper 是测试用的空 RoundTripper。
type fakeRoundTripper struct{}

func (r *fakeRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil
}

var fakeWrapperFunc = func(http.RoundTripper) http.RoundTripper {
	return &fakeRoundTripper{}
}

type fakeWarningHandler struct{}

func (f fakeWarningHandler) HandleWarningHeader(code int, agent string, message string) {}

// fakeWarningHandlerWithLogging 记录收到的 warning 消息，便于断言。
type fakeWarningHandlerWithLogging struct {
	messages []string
}

func (f *fakeWarningHandlerWithLogging) HandleWarningHeader(code int, agent string, message string) {
	f.messages = append(f.messages, message)
}

type fakeWarningHandlerWithContext struct{}

func (f fakeWarningHandlerWithContext) HandleWarningHeaderWithContext(ctx context.Context, code int, agent string, message string) {
}

// fakeNegotiatedSerializer 返回 fakeCodec，用于避免真实序列化依赖。
type fakeNegotiatedSerializer struct{}

func (n *fakeNegotiatedSerializer) SupportedMediaTypes() []runtime.SerializerInfo {
	return nil
}

func (n *fakeNegotiatedSerializer) EncoderForVersion(serializer runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return &fakeCodec{}
}

func (n *fakeNegotiatedSerializer) DecoderToVersion(serializer runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	return &fakeCodec{}
}

var fakeDialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
	return nil, fakeDialerError
}

var fakeDialerError = errors.New("fakedialer")

func fakeProxyFunc(*http.Request) (*url.URL, error) {
	return nil, errors.New("fakeproxy")
}

type fakeAuthProviderConfigPersister struct{}

func (fakeAuthProviderConfigPersister) Persist(map[string]string) error {
	return fakeAuthProviderConfigPersisterError
}

var fakeAuthProviderConfigPersisterError = errors.New("fakeAuthProviderConfigPersisterError")

// TestAnonymousAuthConfig 用 fuzz 配置验证 AnonymousClientConfig 会清除所有敏感字段，
// 同时保留非敏感字段（Dial/Proxy 等通过行为比较验证）。
func TestAnonymousAuthConfig(t *testing.T) {
	f := randfill.New().NilChance(0.0).NumElements(1, 1)
	// 为无法直接比较的接口/函数字段注入可比较的 fake 实现。
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
		// 认证不需要 fuzzer。
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
	for i := 0; i < 20; i++ {
		original := &Config{}
		f.Fill(original)
		actual := AnonymousClientConfig(original)
		expected := *original

		// 这是已知安全相关字段的列表；若 Config 新增字段，需加入此列表，否则
		// AnonymousClientConfig 不会保留该字段。
		// 这些都是必须被清空的敏感字段；新增 Config 字段时需同步更新此列表。
		expected.Impersonate = ImpersonationConfig{}
		expected.BearerToken = ""
		expected.BearerTokenFile = ""
		expected.Username = ""
		expected.Password = ""
		expected.AuthProvider = nil
		expected.AuthConfigPersister = nil
		expected.ExecProvider = nil
		expected.TLSClientConfig.CertData = nil
		expected.TLSClientConfig.CertFile = ""
		expected.TLSClientConfig.KeyData = nil
		expected.TLSClientConfig.KeyFile = ""
		expected.Transport = nil
		expected.WrapTransport = nil

		// 函数字段无法用 DeepEqual 比较，改为调用后比较返回值。
		if actual.Dial != nil {
			_, actualError := actual.Dial(context.Background(), "", "")
			_, expectedError := expected.Dial(context.Background(), "", "")
			if !reflect.DeepEqual(expectedError, actualError) {
				t.Fatalf("AnonymousClientConfig dropped the Dial field")
			}
		}
		actual.Dial = nil
		expected.Dial = nil

		if actual.Proxy != nil {
			_, actualError := actual.Proxy(nil)
			_, expectedError := expected.Proxy(nil)
			if !reflect.DeepEqual(expectedError, actualError) {
				t.Fatalf("AnonymousClientConfig dropped the Proxy field")
			}
		}
		actual.Proxy = nil
		expected.Proxy = nil

		if diff := cmp.Diff(*actual, expected); diff != "" {
			t.Fatalf("AnonymousClientConfig dropped unexpected fields, identify whether they are security related or not (-got, +want): %s", diff)
		}
	}
}

// TestCopyConfig 用 fuzz 配置验证 CopyConfig 不丢失任何字段；
// 函数/接口字段通过调用结果比较，其余字段用 cmp.Diff 比较。
func TestCopyConfig(t *testing.T) {
	f := randfill.New().NilChance(0.0).NumElements(1, 1)
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
		func(r *AuthProviderConfigPersister, f randfill.Continue) {
			*r = fakeAuthProviderConfigPersister{}
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
	for i := 0; i < 20; i++ {
		original := &Config{}
		f.Fill(original)
		actual := CopyConfig(original)
		expected := *original

		// 这是已知风险字段的列表；若 Config 新增字段，需加入此列表，否则 CopyConfig
		// 不会保留该字段。

		// DeepEqual 无法比较函数，因此这里只验证是否满足：
		// 函数返回期望的对象。
		if actual.WrapTransport == nil || !reflect.DeepEqual(expected.WrapTransport(nil), &fakeRoundTripper{}) {
			t.Fatalf("CopyConfig dropped the WrapTransport field")
		}
		actual.WrapTransport = nil
		expected.WrapTransport = nil

		if actual.Dial != nil {
			_, actualError := actual.Dial(context.Background(), "", "")
			_, expectedError := expected.Dial(context.Background(), "", "")
			if !reflect.DeepEqual(expectedError, actualError) {
				t.Fatalf("CopyConfig  dropped the Dial field")
			}
		}
		actual.Dial = nil
		expected.Dial = nil

		if actual.AuthConfigPersister != nil {
			actualError := actual.AuthConfigPersister.Persist(nil)
			expectedError := expected.AuthConfigPersister.Persist(nil)
			if !reflect.DeepEqual(expectedError, actualError) {
				t.Fatalf("CopyConfig  dropped the Dial field")
			}
		}
		actual.AuthConfigPersister = nil
		expected.AuthConfigPersister = nil

		if actual.Proxy != nil {
			_, actualError := actual.Proxy(nil)
			_, expectedError := expected.Proxy(nil)
			if !reflect.DeepEqual(expectedError, actualError) {
				t.Fatalf("CopyConfig  dropped the Proxy field")
			}
		}
		actual.Proxy = nil
		expected.Proxy = nil

		if diff := cmp.Diff(*actual, expected); diff != "" {
			t.Fatalf("CopyConfig  dropped unexpected fields, identify whether they are security related or not (-got, +want): %s", diff)
		}
	}
}

// TestConfigStringer 验证 Config.String() 会输出非敏感信息、但对密码/令牌/证书私钥等打码。
func TestConfigStringer(t *testing.T) {
	formatBytes := func(b []byte) string {
		// 对 []byte 而言，%#v 总是前缀 "[]byte{"。
		// 对含 []byte 字段的结构体，%#v 总是前缀 "[]uint8{"。
		return strings.Replace(fmt.Sprintf("%#v", b), "byte", "uint8", 1)
	}
	tests := []struct {
		desc            string
		c               *Config
		expectContent   []string
		prohibitContent []string
	}{
		{
			desc:          "nil config",
			c:             nil,
			expectContent: []string{"<nil>"},
		},
		{
			desc: "non-sensitive config",
			c: &Config{
				Host:      "localhost:8080",
				APIPath:   "v1",
				UserAgent: "gobot",
			},
			expectContent: []string{"localhost:8080", "v1", "gobot"},
		},
		{
			desc: "sensitive config",
			c: &Config{
				Host:        "localhost:8080",
				Username:    "gopher",
				Password:    "g0ph3r",
				BearerToken: "1234567890",
				TLSClientConfig: TLSClientConfig{
					CertFile: "a.crt",
					KeyFile:  "a.key",
					CertData: []byte("fake cert"),
					KeyData:  []byte("fake key"),
				},
				AuthProvider: &clientcmdapi.AuthProviderConfig{
					Config: map[string]string{"secret": "s3cr3t"},
				},
				ExecProvider: &clientcmdapi.ExecConfig{
					Args:   []string{"secret"},
					Env:    []clientcmdapi.ExecEnvVar{{Name: "secret", Value: "s3cr3t"}},
					Config: &runtime.Unknown{Raw: []byte("here is some config data")},
				},
			},
			// 非敏感字段原样输出，敏感值替换为固定占位符。
			expectContent: []string{
				"localhost:8080",
				"gopher",
				"a.crt",
				"a.key",
				"--- REDACTED ---",
				formatBytes([]byte("--- REDACTED ---")),
				formatBytes([]byte("--- TRUNCATED ---")),
			},
			prohibitContent: []string{
				"g0ph3r",
				"1234567890",
				formatBytes([]byte("fake cert")),
				formatBytes([]byte("fake key")),
				"secret",
				"s3cr3t",
				"here is some config data",
				formatBytes([]byte("super secret password")),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := tt.c.String()
			t.Logf("formatted config: %q", got)

			for _, expect := range tt.expectContent {
				if !strings.Contains(got, expect) {
					t.Errorf("missing expected string %q", expect)
				}
			}
			for _, prohibit := range tt.prohibitContent {
				if strings.Contains(got, prohibit) {
					t.Errorf("found prohibited string %q", prohibit)
				}
			}
		})
	}
}

// TestConfigSprint 验证 %s/%v/%+v/%#v 四种格式下 Config 的输出一致，且敏感字段被替换。
func TestConfigSprint(t *testing.T) {
	c := &Config{
		Host:    "localhost:8080",
		APIPath: "v1",
		ContentConfig: ContentConfig{
			AcceptContentTypes: "application/json",
			ContentType:        "application/json",
		},
		Username:    "gopher",
		Password:    "g0ph3r",
		BearerToken: "1234567890",
		Impersonate: ImpersonationConfig{
			UserName: "gopher2",
			UID:      "uid123",
		},
		AuthProvider: &clientcmdapi.AuthProviderConfig{
			Name:   "gopher",
			Config: map[string]string{"secret": "s3cr3t"},
		},
		AuthConfigPersister: fakeAuthProviderConfigPersister{},
		ExecProvider: &clientcmdapi.ExecConfig{
			Command:            "sudo",
			Args:               []string{"secret"},
			Env:                []clientcmdapi.ExecEnvVar{{Name: "secret", Value: "s3cr3t"}},
			ProvideClusterInfo: true,
			Config:             &runtime.Unknown{Raw: []byte("super secret password")},
		},
		TLSClientConfig: TLSClientConfig{
			CertFile:   "a.crt",
			KeyFile:    "a.key",
			CertData:   []byte("fake cert"),
			KeyData:    []byte("fake key"),
			NextProtos: []string{"h2", "http/1.1"},
		},
		UserAgent:                 "gobot",
		Transport:                 &fakeRoundTripper{},
		WrapTransport:             fakeWrapperFunc,
		QPS:                       1,
		Burst:                     2,
		RateLimiter:               &fakeLimiter{},
		WarningHandler:            fakeWarningHandler{},
		WarningHandlerWithContext: fakeWarningHandlerWithContext{},
		Timeout:                   3 * time.Second,
		Dial:                      fakeDialFunc,
		Proxy:                     fakeProxyFunc,
	}
	want := fmt.Sprintf(
		`&rest.Config{Host:"localhost:8080", APIPath:"v1", ContentConfig:rest.ContentConfig{AcceptContentTypes:"application/json", ContentType:"application/json", GroupVersion:(*schema.GroupVersion)(nil), NegotiatedSerializer:runtime.NegotiatedSerializer(nil)}, Username:"gopher", Password:"--- REDACTED ---", BearerToken:"--- REDACTED ---", BearerTokenFile:"", Impersonate:rest.ImpersonationConfig{UserName:"gopher2", UID:"uid123", Groups:[]string(nil), Extra:map[string][]string(nil)}, AuthProvider:api.AuthProviderConfig{Name: "gopher", Config: map[string]string{--- REDACTED ---}}, AuthConfigPersister:rest.AuthProviderConfigPersister(--- REDACTED ---), ExecProvider:api.ExecConfig{Command: "sudo", Args: []string{"--- REDACTED ---"}, Env: []ExecEnvVar{--- REDACTED ---}, APIVersion: "", ProvideClusterInfo: true, Config: runtime.Object(--- REDACTED ---), StdinUnavailable: false}, TLSClientConfig:rest.sanitizedTLSClientConfig{Insecure:false, ServerName:"", CertFile:"a.crt", KeyFile:"a.key", CAFile:"", CertData:[]uint8{0x2d, 0x2d, 0x2d, 0x20, 0x54, 0x52, 0x55, 0x4e, 0x43, 0x41, 0x54, 0x45, 0x44, 0x20, 0x2d, 0x2d, 0x2d}, KeyData:[]uint8{0x2d, 0x2d, 0x2d, 0x20, 0x52, 0x45, 0x44, 0x41, 0x43, 0x54, 0x45, 0x44, 0x20, 0x2d, 0x2d, 0x2d}, CAData:[]uint8(nil), NextProtos:[]string{"h2", "http/1.1"}}, UserAgent:"gobot", DisableCompression:false, Transport:(*rest.fakeRoundTripper)(%p), WrapTransport:(transport.WrapperFunc)(%p), QPS:1, Burst:2, RateLimiter:(*rest.fakeLimiter)(%p), WarningHandler:rest.fakeWarningHandler{}, WarningHandlerWithContext:rest.fakeWarningHandlerWithContext{}, Timeout:3000000000, Dial:(func(context.Context, string, string) (net.Conn, error))(%p), Proxy:(func(*http.Request) (*url.URL, error))(%p)}`,
		c.Transport, fakeWrapperFunc, c.RateLimiter, fakeDialFunc, fakeProxyFunc,
	)

	for _, f := range []string{"%s", "%v", "%+v", "%#v"} {
		if got := fmt.Sprintf(f, c); want != got {
			t.Errorf("fmt.Sprintf(%q, c)\ngot:  %q\nwant: %q\ndiff: %s", f, got, want, cmp.Diff(want, got))
		}
	}
}

// TestConfigWarningHandler 验证 RESTClient 对新旧 warning handler 的选择：
// 未设置时为 nil；只设置新接口时直接使用；两者都设置时优先新接口。
func TestConfigWarningHandler(t *testing.T) {
	config := &Config{}
	config.GroupVersion = &schema.GroupVersion{}
	config.NegotiatedSerializer = &fakeNegotiatedSerializer{}
	handlerNoContext := &fakeWarningHandler{}
	handlerWithContext := &fakeWarningHandlerWithContext{}

	t.Run("none", func(t *testing.T) {
		client, err := RESTClientForConfigAndClient(config, nil)
		require.NoError(t, err)
		assert.Nil(t, client.warningHandler)
	})

	t.Run("no-context", func(t *testing.T) {
		config := CopyConfig(config)
		handler := &fakeWarningHandlerWithLogging{}
		config.WarningHandler = handler
		client, err := RESTClientForConfigAndClient(config, nil)
		require.NoError(t, err)
		client.warningHandler.HandleWarningHeaderWithContext(context.Background(), 0, "", "message")
		assert.Equal(t, []string{"message"}, handler.messages)

	})

	t.Run("with-context", func(t *testing.T) {
		config := CopyConfig(config)
		config.WarningHandlerWithContext = handlerWithContext
		client, err := RESTClientForConfigAndClient(config, nil)
		require.NoError(t, err)
		assert.Equal(t, handlerWithContext, client.warningHandler)
	})

	t.Run("both", func(t *testing.T) {
		config := CopyConfig(config)
		config.WarningHandler = handlerNoContext
		config.WarningHandlerWithContext = handlerWithContext
		client, err := RESTClientForConfigAndClient(config, nil)
		require.NoError(t, err)
		assert.NotNil(t, client.warningHandler)
		assert.Equal(t, handlerWithContext, client.warningHandler)
	})
}
