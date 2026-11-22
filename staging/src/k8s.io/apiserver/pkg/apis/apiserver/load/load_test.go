/*
Copyright 2023 The Kubernetes Authors.

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

package load

import (
	"bytes"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "k8s.io/apiserver/pkg/apis/apiserver"
)

// defaultConfig 表示“未提供配置”时的期望结果：加载器返回空配置而非 nil。
var defaultConfig = &api.AuthorizationConfiguration{}

// writeTempFile 在系统临时目录写入内容并注册清理；文件名固定前缀为 config，
// 便于 LoadFromFile 按扩展名/内容推断解码方式。
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	file, err := os.CreateTemp("", "config")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(file.Name()); err != nil {
			t.Fatal(err)
		}
	})
	if err := os.WriteFile(file.Name(), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return file.Name()
}

// TestLoadFromFile 覆盖文件加载的边界：空路径、空文件、合法 JSON、文件不存在、
// 以及内容中的 apiVersion 非法。后两者都要求错误信息包含可定位的原始值。
func TestLoadFromFile(t *testing.T) {
	// no file
	{
		_, err := LoadFromFile("")
		if err == nil {
			t.Fatalf("expected err: %v", err)
		}
	}

	// empty file
	// 空文件等价于“未配置”，应返回零值配置而不是报错。
	{
		config, err := LoadFromFile(writeTempFile(t, ``))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, defaultConfig) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(defaultConfig, config))
		}
	}

	// valid file
	{
		input := `{
			"apiVersion":"apiserver.config.k8s.io/v1alpha1",
			"kind":"AuthorizationConfiguration",
			"authorizers":[{"type":"Webhook"}]}`
		expect := &api.AuthorizationConfiguration{
			Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
		}

		config, err := LoadFromFile(writeTempFile(t, input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// missing file
	// 错误必须包含文件路径，否则运维无法判断是哪个 --authorization-config 路径出错。
	{
		_, err := LoadFromFile(`bogus-missing-file`)
		if err == nil {
			t.Fatalf("expected err, got none")
		}
		if !strings.Contains(err.Error(), "bogus-missing-file") {
			t.Fatalf("expected missing file error, got %v", err)
		}
	}

	// invalid content file
	// 未注册的版本（v99）应在解码阶段被拒绝，而不是被静默忽略。
	{
		input := `{
			"apiVersion":"apiserver.config.k8s.io/v99",
			"kind":"AuthorizationConfiguration",
			"authorizers":{"type":"Webhook"}}`

		_, err := LoadFromFile(writeTempFile(t, input))
		if err == nil {
			t.Fatalf("expected err, got none")
		}
		if !strings.Contains(err.Error(), "apiserver.config.k8s.io/v99") {
			t.Fatalf("expected apiVersion error, got %v", err)
		}
	}
}

// TestLoadFromReader 验证 Reader 路径与文件路径行为一致：nil/空 Reader 返回默认配置，
// 非法版本返回错误。
func TestLoadFromReader(t *testing.T) {
	// no reader
	{
		config, err := LoadFromReader(nil)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, defaultConfig) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(defaultConfig, config))
		}
	}

	// empty reader
	{
		config, err := LoadFromReader(&bytes.Buffer{})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, defaultConfig) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(defaultConfig, config))
		}
	}

	// valid reader
	{
		input := `{
			"apiVersion":"apiserver.config.k8s.io/v1alpha1",
			"kind":"AuthorizationConfiguration",
			"authorizers":[{"type":"Webhook"}]}`
		expect := &api.AuthorizationConfiguration{
			Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
		}

		config, err := LoadFromReader(bytes.NewBufferString(input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// invalid reader
	{
		input := `{
			"apiVersion":"apiserver.config.k8s.io/v99",
			"kind":"AuthorizationConfiguration",
			"authorizers":[{"type":"Webhook"}]}`

		_, err := LoadFromReader(bytes.NewBufferString(input))
		if err == nil {
			t.Fatalf("expected err, got none")
		}
		if !strings.Contains(err.Error(), "apiserver.config.k8s.io/v99") {
			t.Fatalf("expected apiVersion error, got %v", err)
		}
	}
}

// TestLoadFromData 以表驱动方式覆盖 JSON/YAML、三个版本、默认值填充与各类解码错误。
// 重点保证：所有已注册版本反序列化到同一内部类型；未设置的 webhook 字段被默认化；
// 未知字段被严格拒绝（避免拼写错误静默生效）。
func TestLoadFromData(t *testing.T) {
	testcases := []struct {
		name         string
		data         []byte
		expectErr    string
		expectConfig *api.AuthorizationConfiguration
	}{
		{
			name:         "nil",
			data:         nil,
			expectConfig: defaultConfig,
		},
		{
			name:         "nil",
			data:         []byte{},
			expectConfig: defaultConfig,
		},
		{
			name: "v1alpha1 - json",
			data: []byte(`{
"apiVersion":"apiserver.config.k8s.io/v1alpha1",
"kind":"AuthorizationConfiguration",
"authorizers":[{"type":"Webhook"}]}`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
			},
		},
		{
			// 空的 webhook 块应被填充默认缓存参数：授权 5m、未授权 30s，且默认开启缓存。
			name: "v1alpha1 - defaults",
			data: []byte(`{
"apiVersion":"apiserver.config.k8s.io/v1alpha1",
"kind":"AuthorizationConfiguration",
"authorizers":[{"type":"Webhook","name":"default","webhook":{}}]}`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{
					Type: "Webhook",
					Name: "default",
					Webhook: &api.WebhookConfiguration{
						AuthorizedTTL:             metav1.Duration{Duration: 5 * time.Minute},
						CacheAuthorizedRequests:   true,
						UnauthorizedTTL:           metav1.Duration{Duration: 30 * time.Second},
						CacheUnauthorizedRequests: true,
					},
				}},
			},
		},
		{
			name: "v1alpha1 - yaml",
			data: []byte(`
apiVersion: apiserver.config.k8s.io/v1alpha1
kind: AuthorizationConfiguration
authorizers:
- type: Webhook
`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
			},
		},
		{
			name: "v1beta1 - json",
			data: []byte(`{
"apiVersion":"apiserver.config.k8s.io/v1beta1",
"kind":"AuthorizationConfiguration",
"authorizers":[{"type":"Webhook"}]}`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
			},
		},
		{
			// v1beta1 的默认值与 v1alpha1 保持一致，保证跨版本升级不改变运行时行为。
			name: "v1beta1 - defaults",
			data: []byte(`{
"apiVersion":"apiserver.config.k8s.io/v1beta1",
"kind":"AuthorizationConfiguration",
"authorizers":[{"type":"Webhook","name":"default","webhook":{}}]}`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{
					Type: "Webhook",
					Name: "default",
					Webhook: &api.WebhookConfiguration{
						AuthorizedTTL:             metav1.Duration{Duration: 5 * time.Minute},
						CacheAuthorizedRequests:   true,
						UnauthorizedTTL:           metav1.Duration{Duration: 30 * time.Second},
						CacheUnauthorizedRequests: true,
					},
				}},
			},
		},
		{
			name: "v1beta1 - yaml",
			data: []byte(`
apiVersion: apiserver.config.k8s.io/v1beta1
kind: AuthorizationConfiguration
authorizers:
- type: Webhook
`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
			},
		},
		{
			name: "v1 - json",
			data: []byte(`{
"apiVersion":"apiserver.config.k8s.io/v1",
"kind":"AuthorizationConfiguration",
"authorizers":[{"type":"Webhook"}]}`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
			},
		},
		{
			// v1 默认值与历史版本一致，避免同一配置文件在不同版本下产生不同缓存语义。
			name: "v1 - defaults",
			data: []byte(`{
"apiVersion":"apiserver.config.k8s.io/v1",
"kind":"AuthorizationConfiguration",
"authorizers":[{"type":"Webhook","name":"default","webhook":{}}]}`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{
					Type: "Webhook",
					Name: "default",
					Webhook: &api.WebhookConfiguration{
						AuthorizedTTL:             metav1.Duration{Duration: 5 * time.Minute},
						CacheAuthorizedRequests:   true,
						UnauthorizedTTL:           metav1.Duration{Duration: 30 * time.Second},
						CacheUnauthorizedRequests: true,
					},
				}},
			},
		},
		{
			name: "v1 - yaml",
			data: []byte(`
apiVersion: apiserver.config.k8s.io/v1
kind: AuthorizationConfiguration
authorizers:
- type: Webhook
`),
			expectConfig: &api.AuthorizationConfiguration{
				Authorizers: []api.AuthorizerConfiguration{{Type: "Webhook"}},
			},
		},
		{
			name:      "missing apiVersion",
			data:      []byte(`{"kind":"AuthorizationConfiguration"}`),
			expectErr: `'apiVersion' is missing`,
		},
		{
			name:      "missing kind",
			data:      []byte(`{"apiVersion":"apiserver.config.k8s.io/v1alpha1"}`),
			expectErr: `'Kind' is missing`,
		},
		{
			name:      "unknown group",
			data:      []byte(`{"apiVersion":"apps/v1alpha1","kind":"AuthorizationConfiguration"}`),
			expectErr: `apps/v1alpha1`,
		},
		{
			name:      "unknown version",
			data:      []byte(`{"apiVersion":"apiserver.config.k8s.io/v99","kind":"AuthorizationConfiguration"}`),
			expectErr: `apiserver.config.k8s.io/v99`,
		},
		{
			name:      "unknown kind",
			data:      []byte(`{"apiVersion":"apiserver.config.k8s.io/v1alpha1","kind":"SomeConfiguration"}`),
			expectErr: `SomeConfiguration`,
		},
		{
			// 严格解码：字段拼写错误必须报错，否则配置会被静默忽略。
			name: "unknown field",
			data: []byte(`{
"apiVersion":"apiserver.config.k8s.io/v1alpha1",
"kind":"AuthorizationConfiguration",
"authorzers":[{"type":"Webhook"}]}`),
			expectErr: `unknown field "authorzers"`,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := LoadFromData(tc.data)
			if err != nil {
				if len(tc.expectErr) == 0 {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tc.expectErr) {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if len(tc.expectErr) > 0 {
				t.Fatalf("expected err, got none")
			}

			if !reflect.DeepEqual(config, tc.expectConfig) {
				t.Fatalf("unexpected config:\n%s", cmp.Diff(tc.expectConfig, config))
			}
		})
	}
}
