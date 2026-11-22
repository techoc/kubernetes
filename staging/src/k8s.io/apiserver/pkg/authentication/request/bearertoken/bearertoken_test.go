/*
Copyright 2014 The Kubernetes Authors.

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

// Package bearertoken 提供基于 Authorization: Bearer 头的令牌认证。
package bearertoken

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/warning"
)

// TestAuthenticateRequest 验证合法 Bearer 头能解析出 token 并交给 TokenFunc。
func TestAuthenticateRequest(t *testing.T) {
	auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		if token != "token" {
			t.Errorf("unexpected token: %s", token)
		}
		return &authenticator.Response{User: &user.DefaultInfo{Name: "user"}}, true, nil
	}))
	resp, ok, err := auth.AuthenticateRequest(&http.Request{
		Header: http.Header{"Authorization": []string{"Bearer token"}},
	})
	if !ok || resp == nil || err != nil {
		t.Errorf("expected valid user")
	}
}

// TestAuthenticateRequestIncludingValueAfterToken 验证 token 后追加多余内容时仍只取第一个 token，
// 其余片段被忽略而不会拼接进 token。
func TestAuthenticateRequestIncludingValueAfterToken(t *testing.T) {
	testCases := []struct {
		Req *http.Request
	}{
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"Bearer token a"}}}},
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"Bearer token a b c"}}}},
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"Bearer token   a"}}}},
	}
	for i, testCase := range testCases {
		auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			if token != "token" {
				t.Errorf("unexpected token: %s", token)
			}
			return &authenticator.Response{User: &user.DefaultInfo{Name: "user"}}, true, nil
		}))
		resp, ok, err := auth.AuthenticateRequest(testCase.Req)
		if !ok || resp == nil || err != nil {
			t.Errorf("%d: expected valid user", i)
		}
	}
}

// TestAuthenticateRequestTokenInvalid 验证 TokenFunc 返回 (nil,false,nil) 时，
// 认证器会补上 invalidToken 错误，避免出现“未通过但无错误”的含糊结果。
func TestAuthenticateRequestTokenInvalid(t *testing.T) {
	auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		return nil, false, nil
	}))
	resp, ok, err := auth.AuthenticateRequest(&http.Request{
		Header: http.Header{"Authorization": []string{"Bearer token"}},
	})
	if ok || resp != nil {
		t.Errorf("expected not authenticated user")
	}
	if err != invalidToken {
		t.Errorf("expected invalidToken error, got %v", err)
	}
}

// TestAuthenticateRequestTokenInvalidCustomError 验证 TokenFunc 返回的自定义错误被原样透传，
// 不会被 invalidToken 覆盖。
func TestAuthenticateRequestTokenInvalidCustomError(t *testing.T) {
	customError := errors.New("custom")
	auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		return nil, false, customError
	}))
	resp, ok, err := auth.AuthenticateRequest(&http.Request{
		Header: http.Header{"Authorization": []string{"Bearer token"}},
	})
	if ok || resp != nil {
		t.Errorf("expected not authenticated user")
	}
	if err != customError {
		t.Errorf("expected custom error, got %v", err)
	}
}

// TestAuthenticateRequestTokenError 验证认证器返回的普通错误被透传，且不返回用户信息。
func TestAuthenticateRequestTokenError(t *testing.T) {
	auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
		return nil, false, errors.New("error")
	}))
	resp, ok, err := auth.AuthenticateRequest(&http.Request{
		Header: http.Header{"Authorization": []string{"Bearer token"}},
	})
	if ok || resp != nil || err == nil {
		t.Errorf("expected error")
	}
}

// TestAuthenticateRequestBadValue 验证缺少 token 或非 Bearer 格式的请求不会调用 TokenFunc，
// 而是以“未认证、无错误”返回，把请求交给后续认证器处理。
func TestAuthenticateRequestBadValue(t *testing.T) {
	testCases := []struct {
		Req *http.Request
	}{
		// 无 Authorization 头
		{Req: &http.Request{}},
		// 只有 scheme，没有 token
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"Bearer"}}}},
		// scheme 大小写不匹配
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"bear token"}}}},
		// 用了冒号而非空格
		{Req: &http.Request{Header: http.Header{"Authorization": []string{"Bearer: token"}}}},
	}
	for i, testCase := range testCases {
		auth := New(authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
			t.Errorf("authentication should not have been called")
			return nil, false, nil
		}))
		user, ok, err := auth.AuthenticateRequest(testCase.Req)
		if ok || user != nil || err != nil {
			t.Errorf("%d: expected not authenticated (no token)", i)
		}
	}
}

// dummyRecorder 是测试用 warning.Recorder，记录最近一条 warning 供断言。
type dummyRecorder struct {
	agent string
	text  string
}

// AddWarning 实现 warning.Recorder 接口。
func (r *dummyRecorder) AddWarning(agent, text string) {
	r.agent = agent
	r.text = text
	return
}

// getWarning 返回最近一次记录的 warning 文本。
func (r *dummyRecorder) getWarning() string {
	return r.text
}

// 编译期断言：dummyRecorder 实现 warning.Recorder。
var _ warning.Recorder = &dummyRecorder{}

// TestBearerToken 以表驱动方式覆盖认证、Authorization 头清理与 warning 的端到端行为：
// 仅认证成功时才移除头，其余情况保留原头以免掩盖问题或丢失下游所需信息。
func TestBearerToken(t *testing.T) {
	tests := map[string]struct {
		AuthorizationHeaders []string
		TokenAuth            authenticator.Token

		ExpectedUserName             string
		ExpectedOK                   bool
		ExpectedErr                  bool
		ExpectedAuthorizationHeaders []string
		ExpectedRecordedWarning      string
	}{
		// 以下情况应视为“未处理”：不认证、不返回错误，并保留原始 Authorization 头。
		"no header": {
			AuthorizationHeaders:         nil,
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: nil,
		},
		"empty header": {
			AuthorizationHeaders:         []string{""},
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: []string{""},
		},
		"non-bearer header": {
			AuthorizationHeaders:         []string{"Basic 123"},
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: []string{"Basic 123"},
		},
		"empty bearer token": {
			AuthorizationHeaders:         []string{"Bearer "},
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: []string{"Bearer "},
		},
		// 成功认证后必须删除头，防止凭据被上游或下游意外记录。
		"valid bearer token removing header": {
			AuthorizationHeaders: []string{"Bearer 123"},
			TokenAuth: authenticator.TokenFunc(func(ctx context.Context, t string) (*authenticator.Response, bool, error) {
				return &authenticator.Response{User: &user.DefaultInfo{Name: "myuser"}}, true, nil
			}),
			ExpectedUserName:             "myuser",
			ExpectedOK:                   true,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: nil,
		},
		// 认证器拒绝且未返回错误时，由 AuthenticateRequest 补上 invalidToken。
		"invalid bearer token": {
			AuthorizationHeaders:         []string{"Bearer 123"},
			TokenAuth:                    authenticator.TokenFunc(func(ctx context.Context, t string) (*authenticator.Response, bool, error) { return nil, false, nil }),
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  true,
			ExpectedAuthorizationHeaders: []string{"Bearer 123"},
		},
		// 双空格会被切成空 token；此时应记录 warning 提示调用方修正头格式。
		"valid bearer token with a space": {
			AuthorizationHeaders:         []string{"Bearer  token"},
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  false,
			ExpectedAuthorizationHeaders: []string{"Bearer  token"},
			ExpectedRecordedWarning:      invalidTokenWithSpaceWarning,
		},
		// 认证器错误必须原样返回，不能退化为 invalidToken。
		"error bearer token": {
			AuthorizationHeaders: []string{"Bearer 123"},
			TokenAuth: authenticator.TokenFunc(func(ctx context.Context, t string) (*authenticator.Response, bool, error) {
				return nil, false, errors.New("error")
			}),
			ExpectedUserName:             "",
			ExpectedOK:                   false,
			ExpectedErr:                  true,
			ExpectedAuthorizationHeaders: []string{"Bearer 123"},
		},
	}

	for k, tc := range tests {
		// 每个用例独立的 recorder，避免 warning 串扰。
		dc := dummyRecorder{agent: "", text: ""}
		ctx := genericapirequest.NewDefaultContext()
		// 通过 context 注入 recorder，warning.AddWarning 才能被记录。
		ctxWithRecorder := warning.WithWarningRecorder(ctx, &dc)
		req, _ := http.NewRequestWithContext(ctxWithRecorder, "GET", "/", nil)
		// 允许同一用例添加多个头；Get 只读取第一个。
		for _, h := range tc.AuthorizationHeaders {
			req.Header.Add("Authorization", h)
		}

		bearerAuth := New(tc.TokenAuth)
		resp, ok, err := bearerAuth.AuthenticateRequest(req)
		if tc.ExpectedErr != (err != nil) {
			t.Errorf("%s: Expected err=%v, got %v", k, tc.ExpectedErr, err)
			continue
		}
		if ok != tc.ExpectedOK {
			t.Errorf("%s: Expected ok=%v, got %v", k, tc.ExpectedOK, ok)
			continue
		}
		if ok && resp.User.GetName() != tc.ExpectedUserName {
			t.Errorf("%s: Expected username=%v, got %v", k, tc.ExpectedUserName, resp.User.GetName())
			continue
		}
		if len(tc.ExpectedRecordedWarning) > 0 && tc.ExpectedRecordedWarning != dc.getWarning() {
			t.Errorf("%s: Expected recorded warning=%v, got %v", k, tc.ExpectedRecordedWarning, dc.getWarning())
			continue
		}
		if !reflect.DeepEqual(req.Header["Authorization"], tc.ExpectedAuthorizationHeaders) {
			t.Errorf("%s: Expected headers=%#v, got %#v", k, tc.ExpectedAuthorizationHeaders, req.Header["Authorization"])
			continue
		}
	}
}
