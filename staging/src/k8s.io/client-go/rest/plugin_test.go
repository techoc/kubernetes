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
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"testing"

	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// TestAuthPluginWrapTransport 验证 AuthProvider 插件与用户已有的 config.WrapTransport
// 能叠加生效，并覆盖插件缺失、加载失败等错误路径。
func TestAuthPluginWrapTransport(t *testing.T) {
	// 插件注册是包级全局状态，测试内注册的名字需避免与其他测试冲突。
	if err := RegisterAuthProviderPlugin("pluginA", pluginAProvider); err != nil {
		t.Errorf("Unexpected error: failed to register pluginA: %v", err)
	}
	if err := RegisterAuthProviderPlugin("pluginB", pluginBProvider); err != nil {
		t.Errorf("Unexpected error: failed to register pluginB: %v", err)
	}
	if err := RegisterAuthProviderPlugin("pluginFail", pluginFailProvider); err != nil {
		t.Errorf("Unexpected error: failed to register pluginFail: %v", err)
	}
	testCases := []struct {
		useWrapTransport bool
		plugin           string
		expectErr        bool
		expectPluginA    bool
		expectPluginB    bool
	}{
		// 没有任何插件时，只有用户自己的 WrapTransport 生效。
		{false, "", false, false, false},
		{false, "pluginA", false, true, false},
		{false, "pluginB", false, false, true},
		// 注册存在但加载失败，以及完全未知的插件名，都应在 TransportConfig 阶段报错。
		{false, "pluginFail", true, false, false},
		{false, "pluginUnknown", true, false, false},
	}
	for i, tc := range testCases {
		c := Config{}
		if tc.useWrapTransport {
			// Specify an existing WrapTransport in the config to make sure that
			// plugins play nicely.
			c.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
				return &wrapTransport{rt}
			}
		}
		if len(tc.plugin) != 0 {
			c.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: tc.plugin}
		}
		tConfig, err := c.TransportConfig()
		if err != nil {
			// Unknown/bad plugins are expected to fail here.
			if !tc.expectErr {
				t.Errorf("%d. Did not expect errors loading Auth Plugin: %q. Got: %v", i, tc.plugin, err)
			}
			continue
		}
		// 用空的 transport 做最内层实现，通过响应头判断各层包装是否依次执行。
		var fullyWrappedTransport http.RoundTripper
		fullyWrappedTransport = &emptyTransport{}
		if tConfig.WrapTransport != nil {
			fullyWrappedTransport = tConfig.WrapTransport(&emptyTransport{})
		}
		res, err := fullyWrappedTransport.RoundTrip(&http.Request{})
		if err != nil {
			t.Errorf("%d. Unexpected error in RoundTrip: %v", i, err)
			continue
		}
		hasWrapTransport := res.Header.Get("wrapTransport") == "Y"
		hasPluginA := res.Header.Get("pluginA") == "Y"
		hasPluginB := res.Header.Get("pluginB") == "Y"
		if hasWrapTransport != tc.useWrapTransport {
			t.Errorf("%d. Expected Existing config.WrapTransport: %t; Got: %t", i, tc.useWrapTransport, hasWrapTransport)
		}
		if hasPluginA != tc.expectPluginA {
			t.Errorf("%d. Expected Plugin A: %t; Got: %t", i, tc.expectPluginA, hasPluginA)
		}
		if hasPluginB != tc.expectPluginB {
			t.Errorf("%d. Expected Plugin B: %t; Got: %t", i, tc.expectPluginB, hasPluginB)
		}
	}
}

// TestAuthPluginPersist 验证插件的 Login 与 RoundTrip 都能通过 AuthProviderConfigPersister
// 把凭据写回配置，并且持久化发生在其声明的时机而非被提前触发。
func TestAuthPluginPersist(t *testing.T) {
	// register pluginA by a different name so we don't collide across tests.
	if err := RegisterAuthProviderPlugin("pluginA2", pluginAProvider); err != nil {
		t.Errorf("Unexpected error: failed to register pluginA: %v", err)
	}
	if err := RegisterAuthProviderPlugin("pluginPersist", pluginPersistProvider); err != nil {
		t.Errorf("Unexpected error: failed to register pluginPersist: %v", err)
	}
	fooBarConfig := map[string]string{"foo": "bar"}
	testCases := []struct {
		plugin                       string
		startingConfig               map[string]string
		expectedConfigAfterLogin     map[string]string
		expectedConfigAfterRoundTrip map[string]string
	}{
		// non-persisting plugins should work fine without modifying config.
		{"pluginA2", map[string]string{}, map[string]string{}, map[string]string{}},
		{"pluginA2", fooBarConfig, fooBarConfig, fooBarConfig},
		// 持久化型插件应在其选择的时机持久化。
		// 首次登录只写入 login 标记；请求发出后才累加 roundTrips，说明两次持久化点不同。
		{
			"pluginPersist",
			map[string]string{},
			map[string]string{
				"login": "Y",
			},
			map[string]string{
				"login":      "Y",
				"roundTrips": "1",
			},
		},
		{
			"pluginPersist",
			map[string]string{
				"login":      "Y",
				"roundTrips": "123",
			},
			map[string]string{
				"login":      "Y",
				"roundTrips": "123",
			},
			map[string]string{
				"login":      "Y",
				"roundTrips": "124",
			},
		},
	}
	for i, tc := range testCases {
		// 用内存 persister 代替将凭据写回 kubeconfig 的真实实现，便于断言写入内容。
		cfg := &clientcmdapi.AuthProviderConfig{
			Name:   tc.plugin,
			Config: tc.startingConfig,
		}
		persister := &inMemoryPersister{make(map[string]string)}
		persister.Persist(tc.startingConfig)
		plugin, err := GetAuthProvider("127.0.0.1", cfg, persister)
		if err != nil {
			t.Errorf("%d. Unexpected error: failed to get plugin %q: %v", i, tc.plugin, err)
		}
		if err := plugin.Login(); err != nil {
			t.Errorf("%d. Unexpected error calling Login() w/ plugin %q: %v", i, tc.plugin, err)
		}
		// Make sure the plugin persisted what we expect after Login().
		if !reflect.DeepEqual(persister.savedConfig, tc.expectedConfigAfterLogin) {
			t.Errorf("%d. Unexpected persisted config after calling %s.Login(): \nGot:\n%v\nExpected:\n%v",
				i, tc.plugin, persister.savedConfig, tc.expectedConfigAfterLogin)
		}
		if _, err := plugin.WrapTransport(&emptyTransport{}).RoundTrip(&http.Request{}); err != nil {
			t.Errorf("%d. Unexpected error round-tripping w/ plugin %q: %v", i, tc.plugin, err)
		}
		// Make sure the plugin persisted what we expect after RoundTrip().
		if !reflect.DeepEqual(persister.savedConfig, tc.expectedConfigAfterRoundTrip) {
			t.Errorf("%d. Unexpected persisted config after calling %s.WrapTransport.RoundTrip(): \nGot:\n%v\nExpected:\n%v",
				i, tc.plugin, persister.savedConfig, tc.expectedConfigAfterLogin)
		}
	}

}

// Test_WhenNilPersister_NoOpPersisterIsAssigned 确保调用方传入 nil persister 时不会 panic：
// GetAuthProvider 应替换为 no-op 实现，使插件在不具备持久化能力时仍可工作。
func Test_WhenNilPersister_NoOpPersisterIsAssigned(t *testing.T) {

	if err := RegisterAuthProviderPlugin("anyPlugin", pluginPersistProvider); err != nil {
		t.Errorf("unexpected error: failed to register 'anyPlugin': %v", err)
	}
	cfg := &clientcmdapi.AuthProviderConfig{
		Name:   "anyPlugin",
		Config: nil,
	}
	plugin, err := GetAuthProvider("127.0.0.1", cfg, nil)
	if err != nil {
		t.Errorf("unexpected error: failed to get 'anyPlugin': %v", err)
	}

	anyPlugin := plugin.(*pluginPersist)

	if _, ok := anyPlugin.persister.(*noopPersister); !ok {
		t.Errorf("expected to be No Operation persister")
	}

}

// emptyTransport 提供带已初始化 Header 的空响应，
// 这样各层包装可以直接 Add 头部而无需判空。
type emptyTransport struct{}

func (*emptyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res := &http.Response{
		Header: make(map[string][]string),
	}
	return res, nil
}

// wrapTransport 在响应上设置 "wrapTransport" = "Y"，
// 用来断言用户自带的 WrapTransport 是否被保留，并与插件链共存。
type wrapTransport struct {
	rt http.RoundTripper
}

func (w *wrapTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := w.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	res.Header.Add("wrapTransport", "Y")
	return res, nil
}

// wrapTransportA sets "pluginA" = "Y" on the response.
type wrapTransportA struct {
	rt http.RoundTripper
}

func (w *wrapTransportA) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := w.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	res.Header.Add("pluginA", "Y")
	return res, nil
}

// pluginA 是一个不持久化配置的示例插件，仅包装 transport。
// 它是 AuthProvider 的最小实现，便于在测试中区分各插件的效果。
type pluginA struct{}

func (*pluginA) WrapTransport(rt http.RoundTripper) http.RoundTripper {
	return &wrapTransportA{rt}
}

func (*pluginA) Login() error { return nil }

// pluginAProvider 是 pluginA 的构造工厂，忽略 address、config 与 persister。
func pluginAProvider(string, map[string]string, AuthProviderConfigPersister) (AuthProvider, error) {
	return &pluginA{}, nil
}

// wrapTransportB 在响应上设置 "pluginB" = "Y"，
// 与 wrapTransportA 相同，仅用不同的响应头区分两个插件。
type wrapTransportB struct {
	rt http.RoundTripper
}

func (w *wrapTransportB) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := w.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	res.Header.Add("pluginB", "Y")
	return res, nil
}

// pluginB 是第二个不持久化配置的示例插件，用于验证不同插件名各自生效、互不干扰。
type pluginB struct{}

func (*pluginB) WrapTransport(rt http.RoundTripper) http.RoundTripper {
	return &wrapTransportB{rt}
}

func (*pluginB) Login() error { return nil }

// pluginBProvider 是 pluginB 的构造工厂。
func pluginBProvider(string, map[string]string, AuthProviderConfigPersister) (AuthProvider, error) {
	return &pluginB{}, nil
}

// pluginFailProvider simulates a registered AuthPlugin that fails to load.
func pluginFailProvider(string, map[string]string, AuthProviderConfigPersister) (AuthProvider, error) {
	return nil, fmt.Errorf("Failed to load AuthProvider")
}

// inMemoryPersister 用内存 map 模拟凭据持久化，避免测试污染真实 kubeconfig。
type inMemoryPersister struct {
	savedConfig map[string]string
}

func (i *inMemoryPersister) Persist(config map[string]string) error {
	i.savedConfig = make(map[string]string)
	for k, v := range config {
		i.savedConfig[k] = v
	}
	return nil
}

// wrapTransportPersist 在 roundTrip 被调用时累加 config 中的 "roundTrips" 计数，
// 模拟插件在请求过程中刷新并保存令牌的行为。
type wrapTransportPersist struct {
	rt        http.RoundTripper
	config    map[string]string
	persister AuthProviderConfigPersister
}

func (w *wrapTransportPersist) RoundTrip(req *http.Request) (*http.Response, error) {
	roundTrips := 0
	if rtVal, ok := w.config["roundTrips"]; ok {
		var err error
		roundTrips, err = strconv.Atoi(rtVal)
		if err != nil {
			return nil, err
		}
	}
	roundTrips++
	w.config["roundTrips"] = fmt.Sprintf("%d", roundTrips)
	if err := w.persister.Persist(w.config); err != nil {
		return nil, err
	}
	return w.rt.RoundTrip(req)
}

// pluginPersist 演示会持久化配置的插件；Login 与 WrapTransport 各负责一次持久化。
type pluginPersist struct {
	config    map[string]string
	persister AuthProviderConfigPersister
}

func (p *pluginPersist) WrapTransport(rt http.RoundTripper) http.RoundTripper {
	return &wrapTransportPersist{rt, p.config, p.persister}
}

// Login sets the config entry "login" to "Y".
func (p *pluginPersist) Login() error {
	p.config["login"] = "Y"
	p.persister.Persist(p.config)
	return nil
}

// pluginPersistProvider 把调用方提供的 config 与 persister 直接交给插件实例，
// 使其后续修改可以写回同一份配置。
func pluginPersistProvider(_ string, config map[string]string, persister AuthProviderConfigPersister) (AuthProvider, error) {
	return &pluginPersist{config, persister}, nil
}
