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

// Package authorizer 提供带缓存的授权器包装，缓存键由授权属性（用户、
// 资源、选择器等）归一化后构造，避免对相同请求重复调用后端授权器。
package authorizer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// mustParseLabelSelector 解析标签选择器并只取 Requirements，测试中用于
// 构造缓存键参与比较的选择器部分。
func mustParseLabelSelector(str string) labels.Requirements {
	ret, err := labels.Parse(str)
	if err != nil {
		panic(err)
	}
	retRequirements, _ /*selectable*/ := ret.Requirements()
	return retRequirements
}

// TestCachingAuthorizer 用假的“未缓存”计数器验证缓存命中/未命中语义：
// 命中时后端调用次数不增加，且缓存键必须对用户组顺序、extra 顺序做归一化，
// 同时对字段/标签选择器保持区分（否则会错误复用更宽的授权结果）。
func TestCachingAuthorizer(t *testing.T) {
	type result struct {
		decision authorizer.Decision
		reason   string
		error    error
	}

	type invocation struct {
		attributes authorizer.Attributes
		expected   result
	}

	for _, tc := range []struct {
		name    string
		calls   []invocation
		backend []result
	}{
		{
			// 两次相同属性，第二次应命中缓存：后端只被调用一次。
			name: "hit",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{Name: "test name"},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{Name: "test name"},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
			},
		},
		{
			// 用户组是无序集合，顺序不同必须归一化为同一个缓存键。
			name: "hit with differently-ordered groups",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						User: &user.DefaultInfo{
							Groups: []string{"a", "b", "c"},
						},
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{
						User: &user.DefaultInfo{
							Groups: []string{"c", "b", "a"},
						},
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
			},
		},
		{
			// extra 的值列表同样按集合处理。
			name: "hit with differently-ordered extra",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						User: &user.DefaultInfo{
							Extra: map[string][]string{
								"k": {"a", "b", "c"},
							},
						},
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{
						User: &user.DefaultInfo{
							Extra: map[string][]string{
								"k": {"c", "b", "a"},
							},
						},
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
			},
		},
		{
			// 资源名不同不能共享缓存，否则会把 alpha 的授权串到 beta。
			name: "miss due to different name",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{Name: "alpha"},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason alpha",
						error:    fmt.Errorf("test error alpha"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{Name: "beta"},
					expected: result{
						decision: authorizer.DecisionDeny,
						reason:   "test reason beta",
						error:    fmt.Errorf("test error beta"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason alpha",
					error:    fmt.Errorf("test error alpha"),
				},
				{
					decision: authorizer.DecisionDeny,
					reason:   "test reason beta",
					error:    fmt.Errorf("test error beta"),
				},
			},
		},
		{
			name: "miss due to different user",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						User: &user.DefaultInfo{Name: "alpha"},
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason alpha",
						error:    fmt.Errorf("test error alpha"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{
						User: &user.DefaultInfo{Name: "beta"},
					},
					expected: result{
						decision: authorizer.DecisionDeny,
						reason:   "test reason beta",
						error:    fmt.Errorf("test error beta"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason alpha",
					error:    fmt.Errorf("test error alpha"),
				},
				{
					decision: authorizer.DecisionDeny,
					reason:   "test reason beta",
					error:    fmt.Errorf("test error beta"),
				},
			},
		},
		{
			// 合法字段选择器参与缓存键，因此带选择器与不带选择器是两次调用。
			name: "honor good field selector",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						Name:                      "test name",
						FieldSelectorRequirements: fields.ParseSelectorOrDie("foo=bar").Requirements(),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{
						Name: "test name",
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason 2",
						error:    fmt.Errorf("test error 2"),
					},
				},
				{
					// 此时结果应当已被缓存
					attributes: authorizer.AttributesRecord{
						Name:                      "test name",
						FieldSelectorRequirements: fields.ParseSelectorOrDie("foo=bar").Requirements(),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason 2",
					error:    fmt.Errorf("test error 2"),
				},
			},
		},
		{
			// 解析失败的选择器被忽略（退化为无选择器），可与更宽的缓存结果共享。
			name: "ignore malformed field selector first",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						Name:                    "test name",
						FieldSelectorParsingErr: errors.New("malformed"),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					// 注意这里没有那个格式错误的字段选择器，
					// 应当使用缓存的结果
					attributes: authorizer.AttributesRecord{
						Name: "test name",
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
			},
		},
		{
			name: "ignore malformed field selector second",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						Name: "test name",
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					// 由于选择器会被忽略，这里应使用更宽泛的缓存值
					attributes: authorizer.AttributesRecord{
						Name:                    "test name",
						FieldSelectorParsingErr: errors.New("malformed"),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
			},
		},

		{
			// 合法标签选择器参与缓存键，不同选择器必须分别计算。
			name: "honor good label selector",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						Name:                      "test name",
						LabelSelectorRequirements: mustParseLabelSelector("foo=bar"),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{
						Name: "test name",
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason 2",
						error:    fmt.Errorf("test error 2"),
					},
				},
				{
					// 此时结果应当已被缓存
					attributes: authorizer.AttributesRecord{
						Name:                      "test name",
						LabelSelectorRequirements: mustParseLabelSelector("foo=bar"),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					attributes: authorizer.AttributesRecord{
						Name:                      "test name",
						LabelSelectorRequirements: mustParseLabelSelector("diff=zero"),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason 3",
						error:    fmt.Errorf("test error 3"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason 2",
					error:    fmt.Errorf("test error 2"),
				},
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason 3",
					error:    fmt.Errorf("test error 3"),
				},
			},
		},
		{
			name: "ignore malformed label selector first",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						Name:                    "test name",
						LabelSelectorParsingErr: errors.New("malformed mess"),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					// 注意这里没有那个格式错误的字段选择器，
					// 应当使用缓存的结果
					attributes: authorizer.AttributesRecord{
						Name: "test name",
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
			},
		},
		{
			name: "ignore malformed label selector second",
			calls: []invocation{
				{
					attributes: authorizer.AttributesRecord{
						Name: "test name",
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
				{
					// 由于选择器会被忽略，这里应使用更宽泛的缓存值
					attributes: authorizer.AttributesRecord{
						Name:                    "test name",
						LabelSelectorParsingErr: errors.New("malformed mess"),
					},
					expected: result{
						decision: authorizer.DecisionAllow,
						reason:   "test reason",
						error:    fmt.Errorf("test error"),
					},
				},
			},
			backend: []result{
				{
					decision: authorizer.DecisionAllow,
					reason:   "test reason",
					error:    fmt.Errorf("test error"),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 后端按调用顺序返回预置结果；超出预置数量说明缓存未命中次数
			// 与预期不符，直接失败。
			var misses int
			frontend := NewCachingAuthorizer(func() authorizer.UnconditionalAuthorizer {
				return authorizer.AuthorizerFunc(func(_ context.Context, attributes authorizer.Attributes) (authorizer.Decision, string, error) {
					if misses >= len(tc.backend) {
						t.Fatalf("got more than expected %d backend invocations", len(tc.backend))
					}
					result := tc.backend[misses]
					misses++
					return result.decision, result.reason, result.error
				})
			}())

			for i, invocation := range tc.calls {
				decision, reason, err := frontend.Authorize(context.TODO(), invocation.attributes)
				if decision != invocation.expected.decision {
					t.Errorf("(call %d of %d) expected decision %v, got %v", i+1, len(tc.calls), invocation.expected.decision, decision)
				}
				if reason != invocation.expected.reason {
					t.Errorf("(call %d of %d) expected reason %q, got %q", i+1, len(tc.calls), invocation.expected.reason, reason)
				}
				if err.Error() != invocation.expected.error.Error() {
					t.Errorf("(call %d of %d) expected error %q, got %q", i+1, len(tc.calls), invocation.expected.error.Error(), err.Error())
				}
			}

			if len(tc.backend) > misses {
				t.Errorf("expected %d backend invocations, got %d", len(tc.backend), misses)
			}
		})
	}
}
