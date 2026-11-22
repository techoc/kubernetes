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

package authenticator

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apiserver/pkg/authentication/user"
)

// TestAuthenticate 验证 audiences 在显式请求 audience 与认证器隐式
// audience 不匹配时的处理。核心规则：认证器成功返回用户后，只有当
// 请求的 audience 集合与隐式 audience 有交集时才算认证通过；错误则
// 原样透传，未认证则返回 nil。测试通过三层循环穷举多组配置与请求
// 组合，确保交集计算没有边界遗漏。
func TestAuthenticate(t *testing.T) {
	// treq 描述一次认证器调用及其期望结果。
	type treq struct {
		resp          *Response
		authenticated bool
		err           error

		wantResp          *Response
		wantAuthenticated bool
		wantErr           bool
	}
	// taudcfg 描述一组"请求要求 audience"与"认证器隐式 audience"。
	type taudcfg struct {
		auds         Audiences
		implicitAuds Audiences
	}
	cs := []struct {
		name string

		taudcfgs []taudcfg
		treqs    []treq
	}{
		{
			name: "good audience",

			// 每个用例都保证请求 audience 与隐式 audience 至少有一个交集。
			taudcfgs: []taudcfg{
				{
					implicitAuds: Audiences{"api"},
					auds:         Audiences{"api"},
				},
				{
					implicitAuds: Audiences{"api", "other"},
					auds:         Audiences{"api"},
				},
				{
					implicitAuds: Audiences{"api"},
					auds:         Audiences{"api", "other"},
				},
				{
					implicitAuds: Audiences{"api", "other"},
					auds:         Audiences{"api", "other_other"},
				},
			},

			// 认证成功：期望响应附带请求的 audience；错误与未认证路径
			// 也应保持原语义。
			treqs: []treq{
				{
					resp: &Response{
						User: &user.DefaultInfo{
							Name: "test_user",
						},
					},
					authenticated: true,

					wantResp: &Response{
						User: &user.DefaultInfo{
							Name: "test_user",
						},
						Audiences: Audiences{"api"},
					},
					wantAuthenticated: true,
				},
				{
					err:     errors.New("uhoh"),
					wantErr: true,
				},
				{
					authenticated:     false,
					wantAuthenticated: false,
				},
			},
		},
		{
			name: "multiple good audiences",

			// 两边各有多个 audience，验证交集为多个元素时全部保留。
			taudcfgs: []taudcfg{
				{
					implicitAuds: Audiences{"api", "other_api"},
					auds:         Audiences{"api", "other_api"},
				},
				{
					implicitAuds: Audiences{"api", "other_api", "other"},
					auds:         Audiences{"api", "other_api"},
				},
				{
					implicitAuds: Audiences{"api", "other_api"},
					auds:         Audiences{"api", "other_api", "other"},
				},
				{
					implicitAuds: Audiences{"api", "other_api", "other"},
					auds:         Audiences{"api", "other_api", "other_other"},
				},
			},

			treqs: []treq{
				{
					resp: &Response{
						User: &user.DefaultInfo{
							Name: "test_user",
						},
					},
					authenticated: true,

					wantResp: &Response{
						User: &user.DefaultInfo{
							Name: "test_user",
						},
						Audiences: Audiences{"api", "other_api"},
					},
					wantAuthenticated: true,
				},
				{
					err: errors.New("uhoh"),

					wantErr: true,
				},
				{
					authenticated: false,

					wantAuthenticated: false,
				},
			},
		},
		{
			name: "bad audience(s)",

			// 请求 audience 与隐式 audience 无交集（含任一方为空），
			// 此时即使认证器返回了用户，也必须判定为未认证。
			taudcfgs: []taudcfg{
				{
					implicitAuds: Audiences{"api"},
					auds:         Audiences{"other_api"},
				},
				{
					implicitAuds: Audiences{},
					auds:         Audiences{"other_api"},
				},
				{
					implicitAuds: Audiences{"api"},
					auds:         Audiences{},
				},
				{
					implicitAuds: Audiences{"api", "other"},
					auds:         Audiences{},
				},
				{
					implicitAuds: Audiences{},
					auds:         Audiences{"api", "other"},
				},
			},

			treqs: []treq{
				{
					resp: &Response{
						User: &user.DefaultInfo{
							Name: "test_user",
						},
					},
					authenticated: true,

					wantAuthenticated: false,
				},
				{
					err: errors.New("uhoh"),

					wantAuthenticated: false,
				},
				{
					authenticated: false,

					wantAuthenticated: false,
				},
			},
		},
	}

	for _, c := range cs {
		t.Run(c.name, func(t *testing.T) {
			for _, taudcfg := range c.taudcfgs {
				for _, treq := range c.treqs {
					t.Run(fmt.Sprintf("auds=%q,implicit=%q", taudcfg.auds, taudcfg.implicitAuds), func(t *testing.T) {
						ctx := context.Background()
						ctx = WithAudiences(ctx, taudcfg.auds)
						// 把待测请求包装成认证器函数，模拟底层认证器
						// 返回的响应/状态/错误。
						resp, ok, err := authenticate(ctx, taudcfg.implicitAuds, func() (*Response, bool, error) {
							if treq.resp != nil {
								resp := *treq.resp
								return &resp, treq.authenticated, treq.err
							}
							return nil, treq.authenticated, treq.err
						})
						if got, want := (err != nil), treq.wantErr; got != want {
							t.Errorf("Unexpected error. got=%v, want=%v, err=%v", got, want, err)
						}
						if got, want := ok, treq.wantAuthenticated; got != want {
							t.Errorf("Unexpected authentication. got=%v, want=%v", got, want)
						}
						if got, want := resp, treq.wantResp; !reflect.DeepEqual(got, want) {
							t.Errorf("Unexpected response. diff:\n%v", cmp.Diff(got, want))
						}
					})
				}
			}
		})
	}
}
