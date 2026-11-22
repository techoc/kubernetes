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

package union

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// mockAuthRequestHandler 返回预置结果的 token 认证器，用于验证联合认证器的合并逻辑。
type mockAuthRequestHandler struct {
	returnUser      user.Info
	isAuthenticated bool
	err             error
}

var (
	user1 = &user.DefaultInfo{Name: "fresh_ferret", UID: "alfa"}
	user2 = &user.DefaultInfo{Name: "elegant_sheep", UID: "bravo"}
)

func (mock *mockAuthRequestHandler) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	return &authenticator.Response{User: mock.returnUser}, mock.isAuthenticated, mock.err
}

// TestAuthenticateTokenSecondPasses 验证第一个认证器未通过时继续尝试第二个。
func TestAuthenticateTokenSecondPasses(t *testing.T) {
	handler1 := &mockAuthRequestHandler{returnUser: user1}
	handler2 := &mockAuthRequestHandler{returnUser: user2, isAuthenticated: true}
	authRequestHandler := New(handler1, handler2)

	resp, isAuthenticated, err := authRequestHandler.AuthenticateToken(context.Background(), "foo")
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

// TestAuthenticateTokenFirstPasses 验证第一个认证器通过后不再调用后续认证器。
func TestAuthenticateTokenFirstPasses(t *testing.T) {
	handler1 := &mockAuthRequestHandler{returnUser: user1, isAuthenticated: true}
	handler2 := &mockAuthRequestHandler{returnUser: user2}
	authRequestHandler := New(handler1, handler2)

	resp, isAuthenticated, err := authRequestHandler.AuthenticateToken(context.Background(), "foo")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !isAuthenticated {
		t.Errorf("Unexpectedly unauthenticated: %v", isAuthenticated)
	}
	if !reflect.DeepEqual(user1, resp.User) {
		t.Errorf("Expected %v, got %v", user1, resp.User)
	}
}

// TestAuthenticateTokenSuppressUnnecessaryErrors 验证有认证器成功时，
// 其他认证器的错误被抑制，避免把一次成功的认证变成请求失败。
func TestAuthenticateTokenSuppressUnnecessaryErrors(t *testing.T) {
	handler1 := &mockAuthRequestHandler{err: errors.New("first")}
	handler2 := &mockAuthRequestHandler{isAuthenticated: true}
	authRequestHandler := New(handler1, handler2)

	_, isAuthenticated, err := authRequestHandler.AuthenticateToken(context.Background(), "foo")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !isAuthenticated {
		t.Errorf("Unexpectedly unauthenticated: %v", isAuthenticated)
	}
}

// TestAuthenticateTokenNoAuthenticators 验证空链时返回未认证且无错误。
func TestAuthenticateTokenNoAuthenticators(t *testing.T) {
	authRequestHandler := New()

	resp, isAuthenticated, err := authRequestHandler.AuthenticateToken(context.Background(), "foo")
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

// TestAuthenticateTokenNonePass 验证所有认证器均未通过时返回未认证。
func TestAuthenticateTokenNonePass(t *testing.T) {
	handler1 := &mockAuthRequestHandler{}
	handler2 := &mockAuthRequestHandler{}
	authRequestHandler := New(handler1, handler2)

	_, isAuthenticated, err := authRequestHandler.AuthenticateToken(context.Background(), "foo")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if isAuthenticated {
		t.Errorf("Unexpectedly authenticated: %v", isAuthenticated)
	}
}

// TestAuthenticateTokenAdditiveErrors 验证全部失败时聚合所有错误，
// 便于排查是哪个认证器出了问题。
func TestAuthenticateTokenAdditiveErrors(t *testing.T) {
	handler1 := &mockAuthRequestHandler{err: errors.New("first")}
	handler2 := &mockAuthRequestHandler{err: errors.New("second")}
	authRequestHandler := New(handler1, handler2)

	_, isAuthenticated, err := authRequestHandler.AuthenticateToken(context.Background(), "foo")
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

// TestAuthenticateTokenFailEarly 验证 FailOnError 模式下遇到第一个错误即停止，
// 后续认证器不再执行，避免在已知失败时继续产生额外开销或副作用。
func TestAuthenticateTokenFailEarly(t *testing.T) {
	handler1 := &mockAuthRequestHandler{err: errors.New("first")}
	handler2 := &mockAuthRequestHandler{err: errors.New("second")}
	authRequestHandler := NewFailOnError(handler1, handler2)

	_, isAuthenticated, err := authRequestHandler.AuthenticateToken(context.Background(), "foo")
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
