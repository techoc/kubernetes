/*
Copyright 2016 The Kubernetes Authors.

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

package group

import (
	"net/http"
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// TestGroupAdder 验证追加组的两个性质：新组追加在末尾，且委托方的 Response
// 不被就地修改（追加必须产生新的 Groups 切片）。
//
// 第二点尤其重要：认证器返回的 Response 可能被多个请求共享，就地修改会导致
// 组信息跨请求泄漏。
func TestGroupAdder(t *testing.T) {
	capacity := make([]string, 0, 1024)
	response := &authenticator.Response{User: &user.DefaultInfo{Name: "user", Groups: append(capacity, "original")}}
	orig := toJson(response)

	adder := authenticator.Request(
		NewGroupAdder(
			authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
				return response, true, nil
			}),
			[]string{"added"},
		),
	)

	r, _, _ := adder.AuthenticateRequest(nil)
	if want := []string{"original", "added"}; !reflect.DeepEqual(r.User.GetGroups(), want) {
		t.Errorf("Unexpected groups\ngot:\t%#v\nwant:\t%#v", r.User.GetGroups(), want)
	}

	if got := toJson(response); got != orig {
		t.Errorf("Expected response from delegate to be unmodified: orig=%v got=%v", orig, got)
	}
}

// TestAuthenticatedGroupAdder 验证 system:authenticated 组的注入规则：
// 普通用户追加；已存在时不重复追加；匿名用户与 system:unauthenticated 用户不追加。
func TestAuthenticatedGroupAdder(t *testing.T) {
	tests := []struct {
		name         string
		inputUser    *user.DefaultInfo
		expectedUser user.Info
	}{
		{
			name: "add",
			inputUser: &user.DefaultInfo{
				Name:   "user",
				Groups: []string{"some-group"},
			},
			expectedUser: &user.DefaultInfo{
				Name:   "user",
				Groups: []string{"some-group", user.AllAuthenticated},
			},
		},
		{
			// 幂等：重复包装认证链时不能出现两个 system:authenticated。
			name: "don't double add",
			inputUser: &user.DefaultInfo{
				Name:   "user",
				Groups: []string{user.AllAuthenticated, "some-group"},
			},
			expectedUser: &user.DefaultInfo{
				Name:   "user",
				Groups: []string{user.AllAuthenticated, "some-group"},
			},
		},
		{
			// 匿名用户不应获得已认证身份，否则授权规则会被绕过。
			name: "don't add for anon",
			inputUser: &user.DefaultInfo{
				Name:   user.Anonymous,
				Groups: []string{"some-group"},
			},
			expectedUser: &user.DefaultInfo{
				Name:   user.Anonymous,
				Groups: []string{"some-group"},
			},
		},
		{
			name: "don't add for unauthenticated group",
			inputUser: &user.DefaultInfo{
				Name:   "user",
				Groups: []string{user.AllUnauthenticated, "some-group"},
			},
			expectedUser: &user.DefaultInfo{
				Name:   "user",
				Groups: []string{user.AllUnauthenticated, "some-group"},
			},
		},
	}

	for _, test := range tests {
		capacity := make([]string, 0, 1024)
		user := test.inputUser
		user.Groups = append(capacity, user.Groups...) // make sure there is capacity in the groups array to trigger potential mutation
		response := &authenticator.Response{User: user}
		orig := toJson(response)

		adder := authenticator.Request(
			NewAuthenticatedGroupAdder(
				authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
					return response, true, nil
				}),
			),
		)

		r, _, _ := adder.AuthenticateRequest(nil)
		if !reflect.DeepEqual(r.User, test.expectedUser) {
			t.Errorf("Unexpected user\ngot:\t%#v\nwant:\t%#v", r.User, test.expectedUser)
		}

		if got := toJson(response); got != orig {
			t.Errorf("Expected response from delegate to be unmodified: orig=%v got=%v", orig, got)
		}
	}

}
