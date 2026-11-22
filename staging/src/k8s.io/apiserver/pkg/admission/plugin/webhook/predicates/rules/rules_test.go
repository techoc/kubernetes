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

package rules

import (
	"fmt"
	"testing"

	adreg "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
)

// ruleTest 描述一条 webhook 规则以及它应当匹配/不匹配的请求属性集合。
type ruleTest struct {
	rule    adreg.RuleWithOperations
	match   []admission.Attributes
	noMatch []admission.Attributes
}

// tests 以用例名索引规则测试，便于表驱动执行。
type tests map[string]ruleTest

// a 构造一个带命名空间的 admission.Attributes；Kind 由资源名推导，仅用于测试。
func a(group, version, resource, subresource, name string, operation admission.Operation, operationOptions runtime.Object) admission.Attributes {
	return admission.NewAttributesRecord(
		nil, nil,
		schema.GroupVersionKind{Group: group, Version: version, Kind: "k" + resource},
		"ns", name,
		schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, subresource,
		operation,
		operationOptions,
		false,
		nil,
	)
}

// namespacedAttributes 与 a 相同，仅用于在 scope 测试中显式表达“命名空间作用域”。
func namespacedAttributes(group, version, resource, subresource, name string, operation admission.Operation, operationOptions runtime.Object) admission.Attributes {
	return admission.NewAttributesRecord(
		nil, nil,
		schema.GroupVersionKind{Group: group, Version: version, Kind: "k" + resource},
		"ns", name,
		schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, subresource,
		operation,
		operationOptions,
		false,
		nil,
	)
}

// clusterScopedAttributes 构造命名空间为空的对象，模拟集群级资源。
func clusterScopedAttributes(group, version, resource, subresource, name string, operation admission.Operation, operationOptions runtime.Object) admission.Attributes {
	return admission.NewAttributesRecord(
		nil, nil,
		schema.GroupVersionKind{Group: group, Version: version, Kind: "k" + resource},
		"", name,
		schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, subresource,
		operation,
		operationOptions,
		false,
		nil,
	)
}

// attrList 提供可变参数到切片的语法糖。
func attrList(a ...admission.Attributes) []admission.Attributes {
	return a
}

// TestGroup 验证规则的 APIGroups 匹配：支持 "*" 通配与精确列表。
func TestGroup(t *testing.T) {
	table := tests{
		"wildcard": {
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					APIGroups: []string{"*"},
				},
			},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
		"exact": {
			// 组名必须完全一致；版本、资源差异不应影响组匹配。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					APIGroups: []string{"g1", "g2"},
				},
			},
			match: attrList(
				a("g1", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g2", "v2", "r3", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				a("g3", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g4", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
	}

	for name, tt := range table {
		for _, m := range tt.match {
			r := Matcher{tt.rule, m}
			if !r.group() {
				t.Errorf("%v: expected match %#v", name, m)
			}
		}
		for _, m := range tt.noMatch {
			r := Matcher{tt.rule, m}
			if r.group() {
				t.Errorf("%v: expected no match %#v", name, m)
			}
		}
	}
}

// TestVersion 验证 APIVersions 的匹配，包括通配与精确版本。
func TestVersion(t *testing.T) {
	table := tests{
		"wildcard": {
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					APIVersions: []string{"*"},
				},
			},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
		"exact": {
			// 版本与组、资源相互独立匹配。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					APIVersions: []string{"v1", "v2"},
				},
			},
			match: attrList(
				a("g1", "v1", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g2", "v2", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				a("g1", "v3", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g2", "v4", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
	}
	for name, tt := range table {
		for _, m := range tt.match {
			r := Matcher{tt.rule, m}
			if !r.version() {
				t.Errorf("%v: expected match %#v", name, m)
			}
		}
		for _, m := range tt.noMatch {
			r := Matcher{tt.rule, m}
			if r.version() {
				t.Errorf("%v: expected no match %#v", name, m)
			}
		}
	}
}

// TestOperation 验证 Operations 匹配：OperationAll 覆盖所有操作，
// 其余情况要求操作精确匹配列表中的某一项。
func TestOperation(t *testing.T) {
	table := tests{
		"wildcard": {
			rule: adreg.RuleWithOperations{Operations: []adreg.OperationType{adreg.OperationAll}},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g", "v", "r", "", "name", admission.Update, &metav1.UpdateOptions{}),
				a("g", "v", "r", "", "name", admission.Delete, &metav1.DeleteOptions{}),
				a("g", "v", "r", "", "name", admission.Connect, nil),
			),
		},
		"create": {
			rule: adreg.RuleWithOperations{Operations: []adreg.OperationType{adreg.Create}},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				a("g", "v", "r", "", "name", admission.Update, &metav1.UpdateOptions{}),
				a("g", "v", "r", "", "name", admission.Delete, &metav1.DeleteOptions{}),
				a("g", "v", "r", "", "name", admission.Connect, nil),
			),
		},
		"update": {
			rule: adreg.RuleWithOperations{Operations: []adreg.OperationType{adreg.Update}},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Update, &metav1.UpdateOptions{}),
			),
			noMatch: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g", "v", "r", "", "name", admission.Delete, &metav1.DeleteOptions{}),
				a("g", "v", "r", "", "name", admission.Connect, nil),
			),
		},
		"delete": {
			rule: adreg.RuleWithOperations{Operations: []adreg.OperationType{adreg.Delete}},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Delete, &metav1.DeleteOptions{}),
			),
			noMatch: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g", "v", "r", "", "name", admission.Update, &metav1.UpdateOptions{}),
				a("g", "v", "r", "", "name", admission.Connect, nil),
			),
		},
		"connect": {
			rule: adreg.RuleWithOperations{Operations: []adreg.OperationType{adreg.Connect}},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Connect, nil),
			),
			noMatch: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g", "v", "r", "", "name", admission.Update, &metav1.UpdateOptions{}),
				a("g", "v", "r", "", "name", admission.Delete, &metav1.DeleteOptions{}),
			),
		},
		"multiple": {
			rule: adreg.RuleWithOperations{Operations: []adreg.OperationType{adreg.Update, adreg.Delete}},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Update, &metav1.UpdateOptions{}),
				a("g", "v", "r", "", "name", admission.Delete, &metav1.DeleteOptions{}),
			),
			noMatch: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g", "v", "r", "", "name", admission.Connect, nil),
			),
		},
	}
	for name, tt := range table {
		for _, m := range tt.match {
			r := Matcher{tt.rule, m}
			if !r.operation() {
				t.Errorf("%v: expected match %#v", name, m)
			}
		}
		for _, m := range tt.noMatch {
			r := Matcher{tt.rule, m}
			if r.operation() {
				t.Errorf("%v: expected no match %#v", name, m)
			}
		}
	}
}

// TestResource 验证 Resources 匹配，重点是 "resource/subresource" 形式的通配语义：
// "r" 只匹配主资源，"r/*" 同时匹配主资源与其全部子资源。
func TestResource(t *testing.T) {
	table := tests{
		"no subresources": {
			// "*" 不含子资源，因此带 subresource 的请求不匹配。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					Resources: []string{"*"},
				},
			},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r2", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				a("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r2", "proxy", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
		"r & subresources": {
			// "r/*" 覆盖 r 自身及任意子资源，但不匹配其他资源。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					Resources: []string{"r/*"},
				},
			},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				a("2", "v", "r2", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r2", "proxy", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
		"r & subresources or r2": {
			// 多条规则取并集；r2 未带 "/*"，其子资源仍不匹配。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					Resources: []string{"r/*", "r2"},
				},
			},
			match: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r2", "", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				a("2", "v", "r2", "proxy", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
		"proxy or exec": {
			// 通配符可出现在资源段，按 subresource 名称筛选。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					Resources: []string{"*/proxy", "*/exec"},
				},
			},
			match: attrList(
				a("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r2", "proxy", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r3", "proxy", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				a("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r2", "", "name", admission.Create, &metav1.CreateOptions{}),
				a("2", "v", "r4", "scale", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
	}
	for name, tt := range table {
		for _, m := range tt.match {
			r := Matcher{tt.rule, m}
			if !r.resource() {
				t.Errorf("%v: expected match %#v", name, m)
			}
		}
		for _, m := range tt.noMatch {
			r := Matcher{tt.rule, m}
			if r.resource() {
				t.Errorf("%v: expected no match %#v", name, m)
			}
		}
	}
}

// TestScope 验证 Cluster/Namespaced/AllScopes 三种作用域的匹配行为。
// 注意 namespaces 资源本身属于集群作用域，即使请求携带命名空间也不例外。
func TestScope(t *testing.T) {
	cluster := adreg.ClusterScope
	namespace := adreg.NamespacedScope
	allscopes := adreg.AllScopes
	table := tests{
		"cluster scope": {
			// 集群作用域应同时匹配空命名空间的属性，以及 namespaces 资源的属性。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					Resources: []string{"*"},
					Scope:     &cluster,
				},
			},
			match: attrList(
				clusterScopedAttributes("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("", "v1", "namespaces", "", "ns", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("", "v1", "namespaces", "finalize", "ns", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("", "v1", "namespaces", "", "ns", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("", "v1", "namespaces", "finalize", "ns", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				namespacedAttributes("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
		"namespace scope": {
			// 命名空间作用域只匹配普通命名空间资源，不匹配集群级或 namespaces 资源。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					Resources: []string{"*"},
					Scope:     &namespace,
				},
			},
			match: attrList(
				namespacedAttributes("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(
				clusterScopedAttributes("", "v1", "namespaces", "", "ns", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("", "v1", "namespaces", "finalize", "ns", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("", "v1", "namespaces", "", "ns", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("", "v1", "namespaces", "finalize", "ns", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
			),
		},
		"all scopes": {
			// AllScopes 是默认值，任何请求都应匹配。
			rule: adreg.RuleWithOperations{
				Rule: adreg.Rule{
					Resources: []string{"*"},
					Scope:     &allscopes,
				},
			},
			match: attrList(
				namespacedAttributes("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("g", "v", "r", "", "name", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("g", "v", "r", "exec", "name", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("", "v1", "namespaces", "", "ns", admission.Create, &metav1.CreateOptions{}),
				clusterScopedAttributes("", "v1", "namespaces", "finalize", "ns", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("", "v1", "namespaces", "", "ns", admission.Create, &metav1.CreateOptions{}),
				namespacedAttributes("", "v1", "namespaces", "finalize", "ns", admission.Create, &metav1.CreateOptions{}),
			),
			noMatch: attrList(),
		},
	}
	keys := sets.New[string]()
	for name := range table {
		keys.Insert(name)
	}
	for _, name := range sets.List(keys) {
		tt := table[name]
		for i, m := range tt.match {
			t.Run(fmt.Sprintf("%s_match_%d", name, i), func(t *testing.T) {
				r := Matcher{tt.rule, m}
				if !r.scope() {
					t.Errorf("%v: expected match %#v", name, m)
				}
			})
		}
		for i, m := range tt.noMatch {
			t.Run(fmt.Sprintf("%s_nomatch_%d", name, i), func(t *testing.T) {
				r := Matcher{tt.rule, m}
				if r.scope() {
					t.Errorf("%v: expected no match %#v", name, m)
				}
			})
		}
	}
}
