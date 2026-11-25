/*
Copyright The Kubernetes Authors.

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

package loader

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadValidatingManifests 覆盖校验型 webhook 清单的加载、默认值和错误边界，
// 防止无效配置或资源类型混入时被静默接受。
func TestLoadValidatingManifests(t *testing.T) {
	tests := []struct {
		name                 string
		files                map[string]string
		wantErr              bool
		errContains          string
		wantConfigCount      int
		wantWebhookAccessors int
	}{
		{
			name:    "empty directory",
			files:   map[string]string{},
			wantErr: false,
		},
		{
			name: "single validating webhook yaml",
			files: map[string]string{
				"validating.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: test-validating.static.k8s.io
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/validate"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantConfigCount:      1,
			wantWebhookAccessors: 1,
		},
		{
			name: "multiple webhooks in one config",
			files: map[string]string{
				"multi.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: test-multi.static.k8s.io
webhooks:
- name: first.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/first"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
- name: second.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/second"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["UPDATE"]
    resources: ["pods"]
`,
			},
			wantConfigCount:      1,
			wantWebhookAccessors: 2,
		},
		{
			name: "json format",
			files: map[string]string{
				"webhook.json": `{
  "apiVersion": "admissionregistration.k8s.io/v1",
  "kind": "ValidatingWebhookConfiguration",
  "metadata": {"name": "test-json.static.k8s.io"},
  "webhooks": [{
    "name": "json.webhook.io",
    "admissionReviewVersions": ["v1"],
    "clientConfig": {"url": "https://example.com/json", "caBundle": "dGVzdA=="},
    "sideEffects": "None",
    "rules": [{"apiGroups": [""], "apiVersions": ["v1"], "operations": ["CREATE"], "resources": ["pods"]}]
  }]
}`,
			},
			wantConfigCount:      1,
			wantWebhookAccessors: 1,
		},
		{
			name: "mixed validating and mutating errors for validating plugin",
			files: map[string]string{
				"01-validating.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: test-validating.static.k8s.io
webhooks:
- name: validate.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/validate"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
				"02-mutating.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: test-mutating.static.k8s.io
webhooks:
- name: mutate.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/mutate"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantErr:     true,
			errContains: "unsupported resource type",
		},
		{
			name: "multi-document yaml",
			files: map[string]string{
				"multi-doc.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: validating-first.static.k8s.io
webhooks:
- name: validate1.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/validate1"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: validating-second.static.k8s.io
webhooks:
- name: validate2.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/validate2"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantConfigCount:      2,
			wantWebhookAccessors: 2,
		},
		{
			name: "yml extension",
			files: map[string]string{
				"webhook.yml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: test-yml.static.k8s.io
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantConfigCount:      1,
			wantWebhookAccessors: 1,
		},
		{
			name: "skip non-yaml files",
			files: map[string]string{
				"readme.txt": "This is not a webhook configuration",
				"webhook.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: test.static.k8s.io
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantConfigCount:      1,
			wantWebhookAccessors: 1,
		},
		{
			name: "invalid yaml",
			files: map[string]string{
				"bad.yaml": "this is not valid: yaml: content",
			},
			wantErr:     true,
			errContains: "error loading",
		},
		{
			name: "unsupported resource type",
			files: map[string]string{
				"pod.yaml": `
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
spec:
  containers:
  - name: test
    image: test
`,
			},
			wantErr:     true,
			errContains: "no kind",
		},
		{
			name: "missing name",
			files: map[string]string{
				"noname.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantErr:     true,
			errContains: "name",
		},
		{
			name: "name missing static suffix",
			files: map[string]string{
				"webhook.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: no-suffix
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantErr:     true,
			errContains: "must have a name ending with",
		},
		{
			name: "duplicate names across files",
			files: map[string]string{
				"01-first.yaml":  createValidatingWebhookYAML("dup.static.k8s.io"),
				"02-second.yaml": createValidatingWebhookYAML("dup.static.k8s.io"),
			},
			wantErr:     true,
			errContains: "duplicate",
		},
		{
			name: "webhook with service reference",
			files: map[string]string{
				"webhook.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: svc-webhook.static.k8s.io
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    service:
      name: my-webhook
      namespace: my-ns
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantErr:     true,
			errContains: "clientConfig.service is not supported",
		},
		{
			name: "webhook with no url",
			files: map[string]string{
				"webhook.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: no-url.static.k8s.io
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig: {}
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantErr:     true,
			errContains: "clientConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			for name, content := range tt.files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
					t.Fatalf("failed to write test file: %v", err)
				}
			}

			result, err := LoadValidatingManifests(dir)
			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadValidatingManifests() expected error containing %q, got nil", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("LoadValidatingManifests() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("LoadValidatingManifests() unexpected error: %v", err)
				return
			}

			if got := len(result.Configurations); got != tt.wantConfigCount {
				t.Errorf("Configurations count = %d, want %d", got, tt.wantConfigCount)
			}

			if got := len(result.GetWebhookAccessors()); got != tt.wantWebhookAccessors {
				t.Errorf("WebhookAccessors count = %d, want %d", got, tt.wantWebhookAccessors)
			}
		})
	}
}

// TestLoadMutatingManifests 覆盖变更型 webhook 清单的加载、默认值和错误边界，
// 确保两类 webhook 配置不会互相误判。
func TestLoadMutatingManifests(t *testing.T) {
	tests := []struct {
		name                 string
		files                map[string]string
		wantErr              bool
		errContains          string
		wantConfigCount      int
		wantWebhookAccessors int
	}{
		{
			name: "single mutating webhook yaml",
			files: map[string]string{
				"mutating.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: test-mutating.static.k8s.io
webhooks:
- name: test.mutate.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/mutate"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantConfigCount:      1,
			wantWebhookAccessors: 1,
		},
		{
			name: "missing static suffix",
			files: map[string]string{
				"mutating.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: no-suffix
webhooks:
- name: test.mutate.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com/mutate"
  sideEffects: None
`,
			},
			wantErr:     true,
			errContains: "must have a name ending with",
		},
		{
			name: "service reference not allowed",
			files: map[string]string{
				"mutating.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: svc-ref.static.k8s.io
webhooks:
- name: test.mutate.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    service:
      name: my-svc
      namespace: default
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`,
			},
			wantErr:     true,
			errContains: "clientConfig.service is not supported",
		},
		{
			name: "mixed validating and mutating errors for mutating plugin",
			files: map[string]string{
				"mixed.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: test-validating.static.k8s.io
webhooks:
- name: validate.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com"
  sideEffects: None
`,
			},
			wantErr:     true,
			errContains: "unsupported resource type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()

			for name, content := range tt.files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
					t.Fatalf("failed to write test file: %v", err)
				}
			}

			result, err := LoadMutatingManifests(dir)
			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadMutatingManifests() expected error containing %q, got nil", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("LoadMutatingManifests() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("LoadMutatingManifests() unexpected error: %v", err)
				return
			}

			if got := len(result.Configurations); got != tt.wantConfigCount {
				t.Errorf("Configurations count = %d, want %d", got, tt.wantConfigCount)
			}

			if got := len(result.GetWebhookAccessors()); got != tt.wantWebhookAccessors {
				t.Errorf("WebhookAccessors count = %d, want %d", got, tt.wantWebhookAccessors)
			}
		})
	}
}

// TestLoadManifestsFromDirectory_EmptyPath 验证空路径不会被当作当前目录加载。
func TestLoadManifestsFromDirectory_EmptyPath(t *testing.T) {
	_, err := LoadValidatingManifests("")
	if err == nil {
		t.Error("LoadValidatingManifests(\"\") expected error, got nil")
	}
}

// TestLoadManifestsFromDirectory_NonExistentDirectory 验证目录不存在时返回错误。
func TestLoadManifestsFromDirectory_NonExistentDirectory(t *testing.T) {
	_, err := LoadValidatingManifests("/nonexistent/directory/path")
	if err == nil {
		t.Error("LoadValidatingManifests() expected error for non-existent directory, got nil")
	}
}

// TestLoadManifestsFromDirectory_AlphabeticalOrder 验证加载顺序不依赖目录遍历顺序，
// 以保证配置摘要和访问器顺序可复现。
func TestLoadManifestsFromDirectory_AlphabeticalOrder(t *testing.T) {
	dir := t.TempDir()

	files := []string{"03-third.yaml", "01-first.yaml", "02-second.yaml"}
	for i, name := range files {
		content := createValidatingWebhookYAML(name[:2] + "-config.static.k8s.io")
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
			t.Fatalf("failed to write test file %d: %v", i, err)
		}
	}

	result, err := LoadValidatingManifests(dir)
	if err != nil {
		t.Fatalf("LoadValidatingManifests() unexpected error: %v", err)
	}

	if len(result.Configurations) != 3 {
		t.Fatalf("expected 3 configurations, got %d", len(result.Configurations))
	}

	// 验证顺序由配置名（与文件前缀一致）确定，避免目录遍历顺序影响结果。
	accessors := result.GetWebhookAccessors()
	if len(accessors) != 3 {
		t.Fatalf("expected 3 accessors, got %d", len(accessors))
	}

	// 配置应按名称排序（01-config.static.k8s.io、02-config.static.k8s.io、03-config.static.k8s.io）。
	expected := []string{"01-config.static.k8s.io", "02-config.static.k8s.io", "03-config.static.k8s.io"}
	for i, acc := range accessors {
		if acc.GetConfigurationName() != expected[i] {
			t.Errorf("accessor[%d].GetConfigurationName() = %s, want %s", i, acc.GetConfigurationName(), expected[i])
		}
	}
}

func createValidatingWebhookYAML(name string) string {
	return `apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: ` + name + `
webhooks:
- name: test.webhook.io
  admissionReviewVersions: ["v1"]
  clientConfig:
    url: "https://example.com"
    caBundle: dGVzdA==
  sideEffects: None
  rules:
  - apiGroups: [""]
    apiVersions: ["v1"]
    operations: ["CREATE"]
    resources: ["pods"]
`
}

// TestLoadManifestsFromDirectory_ListTypes 验证类型化 List 的展开及类型不匹配时的拒绝行为。
func TestLoadManifestsFromDirectory_ListTypes(t *testing.T) {
	t.Run("validating lists", func(t *testing.T) {
		tests := []struct {
			name            string
			files           map[string]string
			wantErr         bool
			errContains     string
			wantConfigCount int
		}{
			{
				name: "ValidatingWebhookConfigurationList",
				files: map[string]string{
					"list.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfigurationList
items:
- metadata:
    name: list-item-1.static.k8s.io
  webhooks:
  - name: w1.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com/1"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
- metadata:
    name: list-item-2.static.k8s.io
  webhooks:
  - name: w2.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com/2"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
`,
				},
				wantConfigCount: 2,
			},
			{
				name: "v1.List with valid validating webhooks",
				files: map[string]string{
					"list.yaml": `
apiVersion: v1
kind: List
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingWebhookConfiguration
  metadata:
    name: v1list-first.static.k8s.io
  webhooks:
  - name: first.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com/first"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingWebhookConfiguration
  metadata:
    name: v1list-second.static.k8s.io
  webhooks:
  - name: second.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com/second"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
`,
				},
				wantConfigCount: 2,
			},
			{
				name: "v1.List with mixed webhook types errors",
				files: map[string]string{
					"list.yaml": `
apiVersion: v1
kind: List
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingWebhookConfiguration
  metadata:
    name: v1list-validating.static.k8s.io
  webhooks:
  - name: v.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com/v"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
- apiVersion: admissionregistration.k8s.io/v1
  kind: MutatingWebhookConfiguration
  metadata:
    name: v1list-mutating.static.k8s.io
  webhooks:
  - name: m.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com/m"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
`,
				},
				wantErr:     true,
				errContains: "unsupported resource type",
			},
			{
				name: "v1.List with unsupported type",
				files: map[string]string{
					"list.yaml": `
apiVersion: v1
kind: List
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingWebhookConfiguration
  metadata:
    name: valid.static.k8s.io
  webhooks:
  - name: w.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
- apiVersion: v1
  kind: ConfigMap
  metadata:
    name: not-a-webhook
`,
				},
				wantErr:     true,
				errContains: "failed to decode list item",
			},
			{
				name: "ValidatingWebhookConfigurationList with invalid item",
				files: map[string]string{
					"list.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfigurationList
items:
- metadata:
    name: missing-suffix
  webhooks:
  - name: w1.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
`,
				},
				wantErr:     true,
				errContains: "must have a name ending with",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dir := t.TempDir()
				for name, content := range tt.files {
					if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
						t.Fatalf("Failed to write file: %v", err)
					}
				}
				result, err := LoadValidatingManifests(dir)
				if tt.wantErr {
					if err == nil {
						t.Fatal("expected error, got nil")
					}
					if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
						t.Errorf("expected error containing %q, got: %v", tt.errContains, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got := len(result.Configurations); got != tt.wantConfigCount {
					t.Errorf("Configurations count = %d, want %d", got, tt.wantConfigCount)
				}
			})
		}
	})

	t.Run("mutating lists", func(t *testing.T) {
		tests := []struct {
			name            string
			files           map[string]string
			wantErr         bool
			errContains     string
			wantConfigCount int
		}{
			{
				name: "MutatingWebhookConfigurationList",
				files: map[string]string{
					"list.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfigurationList
items:
- metadata:
    name: mut-list-1.static.k8s.io
  webhooks:
  - name: m1.webhook.io
    admissionReviewVersions: ["v1"]
    clientConfig:
      url: "https://example.com/1"
      caBundle: dGVzdA==
    sideEffects: None
    rules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
`,
				},
				wantConfigCount: 1,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dir := t.TempDir()
				for name, content := range tt.files {
					if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
						t.Fatalf("Failed to write file: %v", err)
					}
				}
				result, err := LoadMutatingManifests(dir)
				if tt.wantErr {
					if err == nil {
						t.Fatal("expected error, got nil")
					}
					if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
						t.Errorf("expected error containing %q, got: %v", tt.errContains, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got := len(result.Configurations); got != tt.wantConfigCount {
					t.Errorf("Configurations count = %d, want %d", got, tt.wantConfigCount)
				}
			})
		}
	})
}
