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

package group

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// TestTokenGroupAdder 验证 token 组附加器把配置的组追加到认证结果中，
// 且不修改 delegate 返回的原始 Response（同一 Response 可能被缓存或复用）。
func TestTokenGroupAdder(t *testing.T) {
	// 预留容量 1024，模拟底层数组仍有空位的情形：若实现直接 append 到原切片，
	// 可能污染 delegate 持有的数据，测试借此暴露该问题。
	capacity := make([]string, 0, 1024)
	response := &authenticator.Response{User: &user.DefaultInfo{Name: "user", Groups: append(capacity, "original")}}
	orig := toJson(response)

	adder := authenticator.Token(
		NewTokenGroupAdder(
			authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
				return response, true, nil
			}),
			[]string{"added"},
		),
	)

	r, _, _ := adder.AuthenticateToken(context.Background(), "")
	// 附加组应排在原始组之后，保持原有顺序。
	if !reflect.DeepEqual(r.User.GetGroups(), []string{"original", "added"}) {
		t.Errorf("Expected original,added groups, got %#v", r.User.GetGroups())
	}

	// delegate 的响应必须保持原样，防止跨请求的组泄漏。
	if got := toJson(response); got != orig {
		t.Errorf("Expected response from delegate to be unmodified: orig=%v got=%v", orig, got)
	}
}

// toJson 把对象序列化为 JSON 字符串，用作"内容未变"的快速比较。
func toJson(x interface{}) string {
	b, _ := json.Marshal(x)
	return string(b)
}
