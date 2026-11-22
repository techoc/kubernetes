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

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

var (
	testScheme = runtime.NewScheme()
	testCodecs serializer.CodecFactory
)

func init() {
	utilruntime.Must(admissionregistrationv1.AddToScheme(testScheme))
	// v1.List 与 v1.Status 是无版本类型，需要显式注册才能解码 List 文件。
	testScheme.AddUnversionedTypes(metav1.SchemeGroupVersion, &metav1.List{}, &metav1.Status{})
	testCodecs = serializer.NewCodecFactory(testScheme, serializer.EnableStrict)
}

// acceptTestPolicy 模拟生产环境中的类型接受函数，同时接受单个对象与 List。
func acceptTestPolicy(obj runtime.Object) ([]*admissionregistrationv1.ValidatingAdmissionPolicy, error) {
	switch p := obj.(type) {
	case *admissionregistrationv1.ValidatingAdmissionPolicy:
		return []*admissionregistrationv1.ValidatingAdmissionPolicy{p}, nil
	case *admissionregistrationv1.ValidatingAdmissionPolicyList:
		items := make([]*admissionregistrationv1.ValidatingAdmissionPolicy, len(p.Items))
		for i := range p.Items {
			items[i] = &p.Items[i]
		}
		return items, nil
	default:
		return nil, ErrUnrecognizedType
	}
}

// acceptTestBinding 模拟生产环境中的类型接受函数，同时接受单个对象与 List。
func acceptTestBinding(obj runtime.Object) ([]*admissionregistrationv1.ValidatingAdmissionPolicyBinding, error) {
	switch b := obj.(type) {
	case *admissionregistrationv1.ValidatingAdmissionPolicyBinding:
		return []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{b}, nil
	case *admissionregistrationv1.ValidatingAdmissionPolicyBindingList:
		items := make([]*admissionregistrationv1.ValidatingAdmissionPolicyBinding, len(b.Items))
		for i := range b.Items {
			items[i] = &b.Items[i]
		}
		return items, nil
	default:
		return nil, ErrUnrecognizedType
	}
}

// TestLoadPolicyManifests 覆盖从目录加载静态策略清单的主要场景：正常加载、
// 空目录、悬空引用、重名、类型不支持、List 文件、以及参数化策略的拒绝。
func TestLoadPolicyManifests(t *testing.T) {
	tests := []struct {
		name           string
		files          map[string]string
		wantPolicies   int
		wantBindings   int
		wantErr        bool
		wantErrContain string
	}{
		{
			// 正常路径：一个策略 + 一个绑定，绑定引用已存在的策略。
			name: "load policy and binding",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: test-policy.static.k8s.io
spec:
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  validations:
  - expression: "true"
`,
				"binding.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: test-binding.static.k8s.io
spec:
  policyName: test-policy.static.k8s.io
  validationActions:
  - Deny
`,
			},
			wantPolicies: 1,
			wantBindings: 1,
		},
		{
			// 空目录是合法配置：静态清单可选。
			name:         "empty directory",
			files:        map[string]string{},
			wantPolicies: 0,
			wantBindings: 0,
		},
		{
			// 悬空引用必须在加载期拒绝，否则策略永远不会生效且难以排查。
			name: "binding references non-existent policy",
			files: map[string]string{
				"binding.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: orphan.static.k8s.io
spec:
  policyName: does-not-exist.static.k8s.io
  validationActions:
  - Deny
`,
			},
			wantErr:        true,
			wantErrContain: "does not exist",
		},
		{
			// 同一名称在两个文件中出现时，后加载的会静默覆盖前者，因此必须报错。
			name: "duplicate policy names",
			files: map[string]string{
				"01-policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: dup.static.k8s.io
spec:
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  validations:
  - expression: "true"
`,
				"02-policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: dup.static.k8s.io
spec:
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  validations:
  - expression: "true"
`,
			},
			wantErr:        true,
			wantErrContain: "duplicate",
		},
		{
			// 目录中出现非策略类型（如 ConfigMap）应导致加载失败。
			name: "unsupported resource type",
			files: map[string]string{
				"wrong.yaml": `
apiVersion: v1
kind: ConfigMap
metadata:
  name: not-a-policy
`,
			},
			wantErr:        true,
			wantErrContain: "error loading",
		},
		{
			// v1.List 允许把多个对象打包进一个文件，加载器需逐个展开。
			name: "v1.List with policy and binding",
			files: map[string]string{
				"list.yaml": `
apiVersion: v1
kind: List
items:
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingAdmissionPolicy
  metadata:
    name: v1list-policy.static.k8s.io
  spec:
    matchConstraints:
      resourceRules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["pods"]
    validations:
    - expression: "true"
- apiVersion: admissionregistration.k8s.io/v1
  kind: ValidatingAdmissionPolicyBinding
  metadata:
    name: v1list-binding.static.k8s.io
  spec:
    policyName: v1list-policy.static.k8s.io
    validationActions:
    - Deny
`,
			},
			wantPolicies: 1,
			wantBindings: 1,
		},
		{
			// 静态清单不支持 paramKind：静态文件无法引用集群内对象。
			name: "policy with paramKind rejected",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: param-policy.static.k8s.io
spec:
  paramKind:
    apiVersion: v1
    kind: ConfigMap
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  validations:
  - expression: "true"
`,
			},
			wantErr:        true,
			wantErrContain: "paramKind is not supported",
		},
		{
			// 同理，绑定上的 paramRef 也不被静态清单支持。
			name: "binding with paramRef rejected",
			files: map[string]string{
				"policy.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: ref-policy.static.k8s.io
spec:
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE"]
      resources: ["pods"]
  validations:
  - expression: "true"
`,
				"binding.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: ref-binding.static.k8s.io
spec:
  policyName: ref-policy.static.k8s.io
  paramRef:
    name: my-config
    namespace: default
  validationActions:
  - Deny
`,
			},
			wantErr:        true,
			wantErrContain: "paramRef",
		},
		{
			// 静态策略名称带 .static.k8s.io 后缀，绑定引用也必须带后缀，
			// 否则无法区分静态策略与 API 策略。
			name: "binding policyName missing static suffix",
			files: map[string]string{
				"binding.yaml": `
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: bad-ref.static.k8s.io
spec:
  policyName: some-policy-without-suffix
  validationActions:
  - Deny
`,
			},
			wantErr:        true,
			wantErrContain: "must end with",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每个用例使用独立临时目录，避免文件互相影响。
			dir := t.TempDir()
			for name, content := range tt.files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
					t.Fatalf("failed to write file %s: %v", name, err)
				}
			}

			policies, bindings, _, err := LoadPolicyManifests(
				dir,
				testCodecs.UniversalDeserializer(),
				acceptTestPolicy,
				acceptTestBinding,
				func(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) string {
					return b.Spec.PolicyName
				},
			)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.wantErrContain != "" && !strings.Contains(err.Error(), tt.wantErrContain) {
					t.Errorf("expected error containing %q, got %q", tt.wantErrContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(policies) != tt.wantPolicies {
				t.Errorf("expected %d policies, got %d", tt.wantPolicies, len(policies))
			}
			if len(bindings) != tt.wantBindings {
				t.Errorf("expected %d bindings, got %d", tt.wantBindings, len(bindings))
			}
		})
	}
}

// TestValidateBindingReferences 验证绑定引用校验：所有绑定都指向存在的策略时
// 通过，存在悬空引用时报错。
func TestValidateBindingReferences(t *testing.T) {
	policies := []*admissionregistrationv1.ValidatingAdmissionPolicy{
		{ObjectMeta: metav1.ObjectMeta{Name: "policy-a"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "policy-b"}},
	}
	goodBindings := []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		{ObjectMeta: metav1.ObjectMeta{Name: "b1"}, Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: "policy-a"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "b2"}, Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: "policy-b"}},
	}
	badBindings := []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		{ObjectMeta: metav1.ObjectMeta{Name: "b1"}, Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: "policy-missing"}},
	}

	getBindPolicy := func(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) string {
		return b.Spec.PolicyName
	}

	if err := validateBindingReferences(policies, goodBindings, getBindPolicy); err != nil {
		t.Errorf("unexpected error for valid bindings: %v", err)
	}

	if err := validateBindingReferences(policies, badBindings, getBindPolicy); err == nil {
		t.Error("expected error for binding referencing non-existent policy")
	}
}
