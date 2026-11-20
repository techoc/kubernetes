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

// TestLoadMutatingManifestsFromDirectory 用表驱动方式验证 MAP 清单加载的
// 正常路径和各类错误路径。每个用例把若干清单文件写进临时目录，
// 再调用 LoadMutatingManifestsFromDirectory 并检查返回的 policy/binding 数量或错误信息。
func TestLoadMutatingManifestsFromDirectory(t *testing.T) {
	tests := []struct {
		name             string            // 用例名，同时作为子测试名
		files            map[string]string // 文件名 -> 文件内容，模拟一个清单目录
		wantErr          bool              // 是否期望加载失败
		errContains      string            // 期望错误信息中包含的子串；为空则不检查内容
		wantPolicyCount  int               // 期望加载到的 MAP 数量
		wantBindingCount int               // 期望加载到的 MAPB 数量
	}{
		{
			// 空目录是合法场景：不应该报错，也不应该产生任何对象。
			name:  "empty directory",
			files: map[string]string{},
		},
		{
			// 正常路径：单个 YAML 里用 --- 分隔的 policy + binding 都要被识别。
			name: "load policy and binding",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicy
metadata:
  name: test-mutate.static.k8s.io
spec:
  reinvocationPolicy: Never
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  mutations:
  - patchType: ApplyConfiguration
    applyConfiguration:
      expression: "Object{spec: Object.spec{serviceAccountName: \"default\"}}"
---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicyBinding
metadata:
  name: test-mutate-binding.static.k8s.io
spec:
  policyName: test-mutate.static.k8s.io
`,
			},
			wantPolicyCount:  1,
			wantBindingCount: 1,
		},
		{
			// binding 引用了目录里不存在的 policy，应被引用检查拦截。
			name: "binding references non-existent policy",
			files: map[string]string{
				"binding.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicyBinding
metadata:
  name: orphan-binding.static.k8s.io
spec:
  policyName: does-not-exist.static.k8s.io
`,
			},
			wantErr:     true,
			errContains: "does not exist",
		},
		{
			// 没有 name：既过不了 scheme 校验，也过不了清单名称校验。
			name: "policy without name",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicy
metadata:
  name: ""
spec:
  reinvocationPolicy: Never
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  mutations:
  - patchType: ApplyConfiguration
    applyConfiguration:
      expression: "Object{}"
`,
			},
			wantErr:     true,
			errContains: "name",
		},
		{
			// 静态清单要求名称以 .static.k8s.io 结尾，否则无法与 API 存储的策略区分。
			name: "policy name missing static suffix",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicy
metadata:
  name: no-suffix
spec:
  reinvocationPolicy: Never
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  mutations:
  - patchType: ApplyConfiguration
    applyConfiguration:
      expression: "Object{}"
`,
			},
			wantErr:     true,
			errContains: "must have a name ending with",
		},
		{
			// 两个文件里出现同名 policy，名称唯一性检查应报错。
			name: "duplicate policy names",
			files: map[string]string{
				"01-policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicy
metadata:
  name: dup.static.k8s.io
spec:
  reinvocationPolicy: Never
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  mutations:
  - patchType: ApplyConfiguration
    applyConfiguration:
      expression: "Object{}"
`,
				"02-policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicy
metadata:
  name: dup.static.k8s.io
spec:
  reinvocationPolicy: Never
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  mutations:
  - patchType: ApplyConfiguration
    applyConfiguration:
      expression: "Object{}"
`,
			},
			wantErr:     true,
			errContains: "duplicate",
		},
		{
			// 静态清单无法解析 paramRef 指向的参数资源，因此直接禁用 paramKind。
			name: "policy with paramKind",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicy
metadata:
  name: with-param.static.k8s.io
spec:
  reinvocationPolicy: Never
  paramKind:
    apiVersion: v1
    kind: ConfigMap
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  mutations:
  - patchType: ApplyConfiguration
    applyConfiguration:
      expression: "Object{}"
`,
			},
			wantErr:     true,
			errContains: "paramKind is not supported",
		},
		{
			// 同理，binding 的 paramRef 也不被静态清单支持。
			name: "binding with paramRef",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicy
metadata:
  name: ref-policy.static.k8s.io
spec:
  reinvocationPolicy: Never
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  mutations:
  - patchType: ApplyConfiguration
    applyConfiguration:
      expression: "Object{}"
---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicyBinding
metadata:
  name: with-param.static.k8s.io
spec:
  policyName: ref-policy.static.k8s.io
  paramRef:
    name: some-configmap
`,
			},
			wantErr:     true,
			errContains: "paramRef",
		},
		{
			// binding 引用的 policyName 也必须以静态后缀结尾。
			name: "binding policyName missing static suffix",
			files: map[string]string{
				"binding.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicyBinding
metadata:
  name: bad-ref.static.k8s.io
spec:
  policyName: some-policy-without-suffix
`,
			},
			wantErr:     true,
			errContains: "must end with",
		},
		{
			// 目录中出现 ValidatingAdmissionPolicy：本加载器只接受 MAP/MAPB，
			// 应该以 "unsupported resource type" 报错，而不是忽略。
			name: "unsupported type in directory",
			files: map[string]string{
				"wrong.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: wrong-type.static.k8s.io
spec:
  validations:
  - expression: "true"
`,
			},
			wantErr:     true,
			errContains: "unsupported resource type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每个用例使用独立临时目录，避免文件互相干扰。
			dir := t.TempDir()
			for name, content := range tt.files {
				// 把用例中声明的清单内容落盘。
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
					t.Fatalf("Failed to write file: %v", err)
				}
			}
			result, err := LoadMutatingManifestsFromDirectory(dir)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				// 不仅要求报错，还要求错误包含预期关键词，确认命中的是正确的校验分支。
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got: %v", tt.errContains, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := len(result.Policies); got != tt.wantPolicyCount {
				t.Errorf("Policies count = %d, want %d", got, tt.wantPolicyCount)
			}
			if got := len(result.Bindings); got != tt.wantBindingCount {
				t.Errorf("Bindings count = %d, want %d", got, tt.wantBindingCount)
			}
		})
	}
}

// TestLoadMutatingManifestsFromDirectory_ListTypes 专门验证各种 List 包装形式：
// 强类型 MutatingAdmissionPolicyList 以及通用 v1.List（其内部 item 需要二次解码）。
func TestLoadMutatingManifestsFromDirectory_ListTypes(t *testing.T) {
	tests := []struct {
		name             string
		files            map[string]string
		wantErr          bool
		errContains      string
		wantPolicyCount  int
		wantBindingCount int
	}{
		{
			// 强类型 List：一个文件里放两个 policy，应该展开成两个对象。
			name: "MutatingAdmissionPolicyList",
			files: map[string]string{
				"list.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingAdmissionPolicyList
items:
- metadata:
    name: list-policy-1.static.k8s.io
  spec:
    reinvocationPolicy: Never
    failurePolicy: Fail
    matchConstraints:
      resourceRules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["pods"]
    mutations:
    - patchType: ApplyConfiguration
      applyConfiguration:
        expression: "Object{}"
- metadata:
    name: list-policy-2.static.k8s.io
  spec:
    reinvocationPolicy: Never
    failurePolicy: Fail
    matchConstraints:
      resourceRules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["pods"]
    mutations:
    - patchType: ApplyConfiguration
      applyConfiguration:
        expression: "Object{}"
`,
			},
			wantPolicyCount: 2,
		},
		{
			// 通用 v1.List：每个 item 自带 apiVersion/kind，需要递归解码和类型分发。
			name: "v1.List with policy and binding",
			files: map[string]string{
				"list.yaml": `
apiVersion: v1
kind: List
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: MutatingAdmissionPolicy
  metadata:
    name: v1list-policy.static.k8s.io
  spec:
    reinvocationPolicy: Never
    failurePolicy: Fail
    matchConstraints:
      resourceRules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["pods"]
    mutations:
    - patchType: ApplyConfiguration
      applyConfiguration:
        expression: "Object{}"
- apiVersion: admissionregistration.k8s.io/v1
  kind: MutatingAdmissionPolicyBinding
  metadata:
    name: v1list-binding.static.k8s.io
  spec:
    policyName: v1list-policy.static.k8s.io
`,
			},
			wantPolicyCount:  1,
			wantBindingCount: 1,
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
			result, err := LoadMutatingManifestsFromDirectory(dir)
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
			if got := len(result.Policies); got != tt.wantPolicyCount {
				t.Errorf("Policies count = %d, want %d", got, tt.wantPolicyCount)
			}
			if got := len(result.Bindings); got != tt.wantBindingCount {
				t.Errorf("Bindings count = %d, want %d", got, tt.wantBindingCount)
			}
		})
	}
}
