/*
Copyright 2018 The Kubernetes Authors.

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

package path

import (
	"context"
	"testing"

	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// TestNewAuthorizer 覆盖路径允许规则的构造与匹配语义：
// 仅末尾的 "*" 是通配符，出现在中间或连续出现都视为非法配置；
// 命中规则返回 Allow，未命中返回 NoOpinion（而非 Deny），把决定权留给后续授权器。
func TestNewAuthorizer(t *testing.T) {
	tests := []struct {
		name                       string
		excludedPaths              []string
		allowed, denied, noOpinion []string
		wantErr                    bool
	}{
		// 星号不在末尾属于非法通配，应在构造阶段报错。
		{"inner star", []string{"/foo*bar"}, nil, nil, nil, true},
		{"double star", []string{"/foo**"}, nil, nil, nil, true},
		// 空列表等价于排除 "/"。
		{"empty", nil, nil, nil, []string{"/"}, false},
		{"slash", []string{"/"}, []string{"/"}, nil, []string{"/foo", "//"}, false},
		{"foo", []string{"/foo"}, []string{"/foo", "foo"}, nil, []string{"/", "", "/bar", "/foo/", "/fooooo", "//foo"}, false},
		{"foo slash", []string{"/foo/"}, []string{"/foo/"}, nil, []string{"/", "", "/bar", "/foo", "/fooooo"}, false},
		// "/foo/*" 匹配 "/foo/" 及其下的任意路径。
		{"foo slash star", []string{"/foo/*"}, []string{"/foo/", "/foo/bar/bla"}, nil, []string{"/", "", "/foo", "/bar", "/fooooo"}, false},
		{"foo bar", []string{"/foo", "/bar"}, []string{"/foo", "/bar"}, nil, []string{"/", "", "/foo/", "/bar/", "/fooooo"}, false},
		// 末尾星号是前缀匹配。
		{"foo star", []string{"/foo*"}, []string{"/foo", "/foooo"}, nil, []string{"/", "", "/fo", "/bar"}, false},
		// "/*" 匹配所有路径。
		{"star", []string{"/*"}, []string{"/", "", "/foo", "/foooo"}, nil, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := NewAuthorizer(tt.excludedPaths)
			if err != nil && !tt.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}
			if err == nil && tt.wantErr {
				t.Fatalf("expected error, didn't get any")
			}
			if err != nil {
				return
			}

			// 按期望决策分组断言：allow/deny/no-opinion 三类路径。
			for _, cases := range []struct {
				paths []string
				want  authorizer.Decision
			}{
				{tt.allowed, authorizer.DecisionAllow},
				{tt.denied, authorizer.DecisionDeny},
				{tt.noOpinion, authorizer.DecisionNoOpinion},
			} {
				for _, pth := range cases.paths {
					info := authorizer.AttributesRecord{
						Path: pth,
					}
					if got, _, err := a.Authorize(context.Background(), info); err != nil {
						t.Errorf("NewAuthorizer(%v).Authorize(%q) return unexpected error: %v", tt.excludedPaths, pth, err)
					} else if got != cases.want {
						t.Errorf("NewAuthorizer(%v).Authorize(%q) = %v, want %v", tt.excludedPaths, pth, got, cases.want)
					}
				}
			}
		})
	}
}
