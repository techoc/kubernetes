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

package union

import (
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// mockAuthRequestHandler 返回预设的用户、认证状态与错误，用于在不依赖
// 真实凭据的情况下驱动认证链。
type mockAuthRequestHandler struct {
	returnUser      user.Info
	isAuthenticated bool
	err             error
}

var (
	user1 = &user.DefaultInfo{Name: "fresh_ferret", UID: "alfa"}
	user2 = &user.DefaultInfo{Name: "elegant_sheep", UID: "bravo"}
)

// AuthenticateRequest 原样返回 mock 中配置的结果。
func (mock *mockAuthRequestHandler) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	return &authenticator.Response{User: mock.returnUser}, mock.isAuthenticated, mock.err
}

// TestAuthenticateRequestSecondPasses 第一个认证器未认证成功时，链应继续
// 尝试并采用第二个认证器的用户。
func TestAuthenticateRequestSecondPasses(t *testing.T) {
	handler1 := &mockAuthRequestHandler{returnUser: user1}
	handler2 := &mockAuthRequestHandler{returnUser: user2, isAuthenticated: true}
	authRequestHandler := New(handler1, handler2)
	req, _ := http.NewRequest("GET", "http://example.org", nil)

	resp, isAuthenticated, err := authRequestHandler.AuthenticateRequest(req)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !isAuthenticated {
		t.Errorf("Unexpectedly unauthenticated: %v", isAuthenticated)
	}
	if !reflect.DeepEqual(user2, resp.User) {
		t.Errorf("Expected %v, got %v", user2, resp.User)
	}
}

// TestAuthenticateRequestFirstPasses 第一个认证器成功时应立即返回，
// 后续认证器不得覆盖其结果。
func TestAuthenticateRequestFirstPasses(t *testing.T) {
	handler1 := &mockAuthRequestHandler{returnUser: user1, isAuthenticated: true}
	handler2 := &mockAuthRequestHandler{returnUser: user2}
	authRequestHandler := New(handler1, handler2)
	req, _ := http.NewRequest("GET", "http://example.org", nil)

	resp, isAuthenticated, err := authRequestHandler.AuthenticateRequest(req)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !isAuthenticated {
		t.Errorf("Unexpectedly unauthenticated: %v", isAuthenticated)
	}
	if !reflect.DeepEqual(user1, resp.User) {
		t.Errorf("Expected %v, got %v", user2, resp.User)
	}
}

// TestAuthenticateRequestSuppressUnnecessaryErrors 非 FailOnError 模式下，
// 前一个认证器的错误不应遮蔽后一个认证器的成功结果。
func TestAuthenticateRequestSuppressUnnecessaryErrors(t *testing.T) {
	handler1 := &mockAuthRequestHandler{err: errors.New("first")}
	handler2 := &mockAuthRequestHandler{isAuthenticated: true}
	authRequestHandler := New(handler1, handler2)
	req, _ := http.NewRequest("GET", "http://example.org", nil)

	_, isAuthenticated, err := authRequestHandler.AuthenticateRequest(req)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !isAuthenticated {
		t.Errorf("Unexpectedly unauthenticated: %v", isAuthenticated)
	}
}

// TestAuthenticateRequestNoAuthenticators 空链应返回"未认证且无错误"，
// 调用方据此返回 401 而不是 500。
func TestAuthenticateRequestNoAuthenticators(t *testing.T) {
	authRequestHandler := New()
	req, _ := http.NewRequest("GET", "http://example.org", nil)

	resp, isAuthenticated, err := authRequestHandler.AuthenticateRequest(req)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if isAuthenticated {
		t.Errorf("Unexpectedly authenticated: %v", isAuthenticated)
	}
	if resp != nil {
		t.Errorf("Unexpected authenticatedUser: %v", resp)
	}
}

// TestAuthenticateRequestNonePass 链中所有认证器都未认证时应报告未认证。
func TestAuthenticateRequestNonePass(t *testing.T) {
	handler1 := &mockAuthRequestHandler{}
	handler2 := &mockAuthRequestHandler{}
	authRequestHandler := New(handler1, handler2)
	req, _ := http.NewRequest("GET", "http://example.org", nil)

	_, isAuthenticated, err := authRequestHandler.AuthenticateRequest(req)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if isAuthenticated {
		t.Errorf("Unexpectedly authenticated: %v", isAuthenticated)
	}
}

// TestAuthenticateRequestAdditiveErrors 默认模式下应收集所有认证器的错误，
// 而不是只返回第一个，方便运维定位全部失败原因。
func TestAuthenticateRequestAdditiveErrors(t *testing.T) {
	handler1 := &mockAuthRequestHandler{err: errors.New("first")}
	handler2 := &mockAuthRequestHandler{err: errors.New("second")}
	authRequestHandler := New(handler1, handler2)
	req, _ := http.NewRequest("GET", "http://example.org", nil)

	_, isAuthenticated, err := authRequestHandler.AuthenticateRequest(req)
	if err == nil {
		t.Errorf("Expected an error")
	}
	if !strings.Contains(err.Error(), "first") {
		t.Errorf("Expected error containing %v, got %v", "first", err)
	}
	if !strings.Contains(err.Error(), "second") {
		t.Errorf("Expected error containing %v, got %v", "second", err)
	}
	if isAuthenticated {
		t.Errorf("Unexpectedly authenticated: %v", isAuthenticated)
	}
}

// TestAuthenticateRequestFailEarly FailOnError 模式下第一个错误即短路，
// 后续认证器不应被调用（因此错误信息中不会出现 "second"）。
func TestAuthenticateRequestFailEarly(t *testing.T) {
	handler1 := &mockAuthRequestHandler{err: errors.New("first")}
	handler2 := &mockAuthRequestHandler{err: errors.New("second")}
	authRequestHandler := NewFailOnError(handler1, handler2)
	req, _ := http.NewRequest("GET", "http://example.org", nil)

	_, isAuthenticated, err := authRequestHandler.AuthenticateRequest(req)
	if err == nil {
		t.Errorf("Expected an error")
	}
	if !strings.Contains(err.Error(), "first") {
		t.Errorf("Expected error containing %v, got %v", "first", err)
	}
	if strings.Contains(err.Error(), "second") {
		t.Errorf("Did not expect second error, got %v", err)
	}
	if isAuthenticated {
		t.Errorf("Unexpectedly authenticated: %v", isAuthenticated)
	}
}
