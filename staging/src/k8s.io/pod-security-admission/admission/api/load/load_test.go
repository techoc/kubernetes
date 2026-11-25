/*
Copyright 2021 The Kubernetes Authors.

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

	utiltesting "k8s.io/client-go/util/testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/pod-security-admission/admission/api"
)

// 缺省配置即最宽松档，三个动作都放行到最新版本；
// 后续用例只覆盖个别字段，未覆盖的字段应保持这里的缺省值。
var defaultConfig = &api.PodSecurityConfiguration{
	Defaults: api.PodSecurityDefaults{
		Enforce: "privileged", EnforceVersion: "latest",
		Warn: "privileged", WarnVersion: "latest",
		Audit: "privileged", AuditVersion: "latest",
	},
}

// 临时文件交给 t.Cleanup 回收，避免用例失败时把配置残留在系统临时目录里。
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	file, err := os.CreateTemp("", "podsecurityconfig")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		utiltesting.CloseAndRemove(t, file)
	})
	if err := os.WriteFile(file.Name(), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return file.Name()
}

// TestLoadFromFile 覆盖空路径、空文件、各版本配置、缺失文件与未知版本。
// 空路径和空文件都返回缺省配置，避免未显式配置时启动失败。
func TestLoadFromFile(t *testing.T) {
	// 空路径表示不启用配置文件，不能当作读取失败。
	{
		config, err := LoadFromFile("")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, defaultConfig) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(defaultConfig, config))
		}
	}

	// 空文件与空路径等价：用户可能预置空文件，不应因此启动失败。
	{
		config, err := LoadFromFile(writeTempFile(t, ``))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, defaultConfig) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(defaultConfig, config))
		}
	}

	// v1alpha1 仍是兼容格式，加载器必须继续支持，避免升级时强制改写配置。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v1alpha1",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`
		expect := &api.PodSecurityConfiguration{
			Defaults: api.PodSecurityDefaults{
				Enforce: "baseline", EnforceVersion: "latest",
				Warn: "privileged", WarnVersion: "latest",
				Audit: "privileged", AuditVersion: "latest",
			},
		}

		config, err := LoadFromFile(writeTempFile(t, input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// 逐个版本运行同一套断言，防止回归时丢掉旧格式兼容路径。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v1beta1",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`
		expect := &api.PodSecurityConfiguration{
			Defaults: api.PodSecurityDefaults{
				Enforce: "baseline", EnforceVersion: "latest",
				Warn: "privileged", WarnVersion: "latest",
				Audit: "privileged", AuditVersion: "latest",
			},
		}

		config, err := LoadFromFile(writeTempFile(t, input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// v1 是稳定版本，解析结果应与其他版本一致。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v1",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`
		expect := &api.PodSecurityConfiguration{
			Defaults: api.PodSecurityDefaults{
				Enforce: "baseline", EnforceVersion: "latest",
				Warn: "privileged", WarnVersion: "latest",
				Audit: "privileged", AuditVersion: "latest",
			},
		}

		config, err := LoadFromFile(writeTempFile(t, input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// 文件缺失时错误必须带文件名，便于定位错误路径。
	{
		_, err := LoadFromFile(`bogus-missing-pod-security-policy-config-file`)
		if err == nil {
			t.Fatalf("expected err, got none")
		}
		if !strings.Contains(err.Error(), "bogus-missing-pod-security-policy-config-file") {
			t.Fatalf("expected missing file error, got %v", err)
		}
	}

	// 未知 apiVersion 必须报错，不能静默套用缺省配置。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v99",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`

		_, err := LoadFromFile(writeTempFile(t, input))
		if err == nil {
			t.Fatalf("expected err, got none")
		}
		if !strings.Contains(err.Error(), "pod-security.admission.config.k8s.io/v99") {
			t.Fatalf("expected apiVersion error, got %v", err)
		}
	}
}

// TestLoadFromReader 验证从流读取的语义与从文件读取一致。
// nil reader 代表没有配置源，必须回落到缺省配置而不是 panic。
func TestLoadFromReader(t *testing.T) {
	// nil reader 表示未指定配置源。
	{
		config, err := LoadFromReader(nil)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, defaultConfig) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(defaultConfig, config))
		}
	}

	// 空流与空文件等价，避免数据源类型导致行为差异。
	{
		config, err := LoadFromReader(&bytes.Buffer{})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, defaultConfig) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(defaultConfig, config))
		}
	}

	// v1alpha1 从流读取时同样需要补齐缺省字段。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v1alpha1",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`
		expect := &api.PodSecurityConfiguration{
			Defaults: api.PodSecurityDefaults{
				Enforce: "baseline", EnforceVersion: "latest",
				Warn: "privileged", WarnVersion: "latest",
				Audit: "privileged", AuditVersion: "latest",
			},
		}

		config, err := LoadFromReader(bytes.NewBufferString(input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// v1beta1 的解析结果必须与其他版本一致。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v1beta1",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`
		expect := &api.PodSecurityConfiguration{
			Defaults: api.PodSecurityDefaults{
				Enforce: "baseline", EnforceVersion: "latest",
				Warn: "privileged", WarnVersion: "latest",
				Audit: "privileged", AuditVersion: "latest",
			},
		}

		config, err := LoadFromReader(bytes.NewBufferString(input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// v1 从流读取后应得到规范化的内部配置。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v1",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`
		expect := &api.PodSecurityConfiguration{
			Defaults: api.PodSecurityDefaults{
				Enforce: "baseline", EnforceVersion: "latest",
				Warn: "privileged", WarnVersion: "latest",
				Audit: "privileged", AuditVersion: "latest",
			},
		}

		config, err := LoadFromReader(bytes.NewBufferString(input))
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !reflect.DeepEqual(config, expect) {
			t.Fatalf("unexpected config:\n%s", cmp.Diff(expect, config))
		}
	}

	// 流读取与文件读取采用相同的严格版本校验。
	{
		input := `{
			"apiVersion":"pod-security.admission.config.k8s.io/v99",
			"kind":"PodSecurityConfiguration",
			"defaults":{"enforce":"baseline"}}`

		_, err := LoadFromReader(bytes.NewBufferString(input))
		if err == nil {
			t.Fatalf("expected err, got none")
		}
		if !strings.Contains(err.Error(), "pod-security.admission.config.k8s.io/v99") {
			t.Fatalf("expected apiVersion error, got %v", err)
		}
	}
}

// TestLoadFromData 用表驱动方式覆盖字节数据加载：既测 JSON/YAML 两种编码，也测多个 apiVersion，
// 还覆盖缺字段、未知组/版本/类型以及拼错字段名等错误路径。
func TestLoadFromData(t *testing.T) {
	testcases := []struct {
		name         string
		data         []byte
		expectErr    string
		expectConfig *api.PodSecurityConfiguration
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
"apiVersion":"pod-security.admission.config.k8s.io/v1alpha1",
"kind":"PodSecurityConfiguration",
"defaults":{"enforce":"baseline"}}`),
			expectConfig: &api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce: "baseline", EnforceVersion: "latest",
					Warn: "privileged", WarnVersion: "latest",
					Audit: "privileged", AuditVersion: "latest",
				},
			},
		},
		{
			name: "v1alpha1 - yaml",
			data: []byte(`
apiVersion: pod-security.admission.config.k8s.io/v1alpha1
kind: PodSecurityConfiguration
defaults:
  enforce: baseline
  enforce-version: v1.7
exemptions:
  usernames: ["alice","bob"]
  namespaces: ["kube-system"]
  runtimeClasses: ["special"]
`),
			expectConfig: &api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce: "baseline", EnforceVersion: "v1.7",
					Warn: "privileged", WarnVersion: "latest",
					Audit: "privileged", AuditVersion: "latest",
				},
				Exemptions: api.PodSecurityExemptions{
					Usernames:      []string{"alice", "bob"},
					Namespaces:     []string{"kube-system"},
					RuntimeClasses: []string{"special"},
				},
			},
		},
		{
			name: "v1beta1 - json",
			data: []byte(`{
"apiVersion":"pod-security.admission.config.k8s.io/v1beta1",
"kind":"PodSecurityConfiguration",
"defaults":{"enforce":"baseline"}}`),
			expectConfig: &api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce: "baseline", EnforceVersion: "latest",
					Warn: "privileged", WarnVersion: "latest",
					Audit: "privileged", AuditVersion: "latest",
				},
			},
		},
		{
			name: "v1beta1 - yaml",
			data: []byte(`
apiVersion: pod-security.admission.config.k8s.io/v1beta1
kind: PodSecurityConfiguration
defaults:
  enforce: baseline
  enforce-version: v1.7
exemptions:
  usernames: ["alice","bob"]
  namespaces: ["kube-system"]
  runtimeClasses: ["special"]
`),
			expectConfig: &api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce: "baseline", EnforceVersion: "v1.7",
					Warn: "privileged", WarnVersion: "latest",
					Audit: "privileged", AuditVersion: "latest",
				},
				Exemptions: api.PodSecurityExemptions{
					Usernames:      []string{"alice", "bob"},
					Namespaces:     []string{"kube-system"},
					RuntimeClasses: []string{"special"},
				},
			},
		},
		{
			name: "v1 - json",
			data: []byte(`{
"apiVersion":"pod-security.admission.config.k8s.io/v1",
"kind":"PodSecurityConfiguration",
"defaults":{"enforce":"baseline"}}`),
			expectConfig: &api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce: "baseline", EnforceVersion: "latest",
					Warn: "privileged", WarnVersion: "latest",
					Audit: "privileged", AuditVersion: "latest",
				},
			},
		},
		{
			name: "v1 - yaml",
			data: []byte(`
apiVersion: pod-security.admission.config.k8s.io/v1
kind: PodSecurityConfiguration
defaults:
  enforce: baseline
  enforce-version: v1.7
exemptions:
  usernames: ["alice","bob"]
  namespaces: ["kube-system"]
  runtimeClasses: ["special"]
`),
			expectConfig: &api.PodSecurityConfiguration{
				Defaults: api.PodSecurityDefaults{
					Enforce: "baseline", EnforceVersion: "v1.7",
					Warn: "privileged", WarnVersion: "latest",
					Audit: "privileged", AuditVersion: "latest",
				},
				Exemptions: api.PodSecurityExemptions{
					Usernames:      []string{"alice", "bob"},
					Namespaces:     []string{"kube-system"},
					RuntimeClasses: []string{"special"},
				},
			},
		},
		// 以下错误用例要求错误信息带上出错的字段名或值，
		// 便于用户一眼看出是版本拼错、类型写错还是字段笔误。
		{
			name:      "missing apiVersion",
			data:      []byte(`{"kind":"PodSecurityConfiguration"}`),
			expectErr: `'apiVersion' is missing`,
		},
		{
			name:      "missing kind",
			data:      []byte(`{"apiVersion":"pod-security.admission.config.k8s.io/v1alpha1"}`),
			expectErr: `'Kind' is missing`,
		},
		{
			name:      "unknown group",
			data:      []byte(`{"apiVersion":"apps/v1alpha1","kind":"PodSecurityConfiguration"}`),
			expectErr: `apps/v1alpha1`,
		},
		{
			name:      "unknown version",
			data:      []byte(`{"apiVersion":"pod-security.admission.config.k8s.io/v99","kind":"PodSecurityConfiguration"}`),
			expectErr: `pod-security.admission.config.k8s.io/v99`,
		},
		{
			name:      "unknown kind",
			data:      []byte(`{"apiVersion":"pod-security.admission.config.k8s.io/v1alpha1","kind":"SomeConfiguration"}`),
			expectErr: `SomeConfiguration`,
		},
		// 严格解码未知字段才能发现 "deflaults" 这类拼写错误，
		// 否则缺省配置会静默失效，准入行为与用户预期不符。
		{
			name: "unknown field",
			data: []byte(`{
"apiVersion":"pod-security.admission.config.k8s.io/v1alpha1",
"kind":"PodSecurityConfiguration",
"deflaults":{"enforce":"baseline"}}`),
			expectErr: `unknown field "deflaults"`,
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
