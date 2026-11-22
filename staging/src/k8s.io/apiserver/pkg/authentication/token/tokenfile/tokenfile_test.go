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

package tokenfile

import (
	"context"
	"os"
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
)

// TestTokenFile 验证 CSV 令牌文件的解析与认证：每行 token,user,uid[,groups...]，
// 多余的列被忽略，带引号的逗号分隔组名应被正确拆分。
func TestTokenFile(t *testing.T) {
	auth, err := newWithContents(t, `
token1,user1,uid1
token2,user2,uid2
token3,user3,uid3,"group1,group2"
token4,user4,uid4,"group2"
token5,user5,uid5,group5
token6,user6,uid6,group5,otherdata
token7,user7,uid7,"group1,group2",otherdata
`)
	if err != nil {
		t.Fatalf("unable to read tokenfile: %v", err)
	}

	testCases := []struct {
		Token string
		User  *user.DefaultInfo
		Ok    bool
		Err   bool
	}{
		{
			Token: "token1",
			User:  &user.DefaultInfo{Name: "user1", UID: "uid1"},
			Ok:    true,
		},
		{
			Token: "token2",
			User:  &user.DefaultInfo{Name: "user2", UID: "uid2"},
			Ok:    true,
		},
		{
			Token: "token3",
			User:  &user.DefaultInfo{Name: "user3", UID: "uid3", Groups: []string{"group1", "group2"}},
			Ok:    true,
		},
		{
			Token: "token4",
			User:  &user.DefaultInfo{Name: "user4", UID: "uid4", Groups: []string{"group2"}},
			Ok:    true,
		},
		{
			Token: "token5",
			User:  &user.DefaultInfo{Name: "user5", UID: "uid5", Groups: []string{"group5"}},
			Ok:    true,
		},
		{
			Token: "token6",
			User:  &user.DefaultInfo{Name: "user6", UID: "uid6", Groups: []string{"group5"}},
			Ok:    true,
		},
		{
			// token7 在组列之后还有 otherdata，应被忽略而非报错。
			Token: "token7",
			User:  &user.DefaultInfo{Name: "user7", UID: "uid7", Groups: []string{"group1", "group2"}},
			Ok:    true,
		},
		{
			// 未知 token 必须返回 ok=false 且不带用户，认证器不得猜测身份。
			Token: "token8",
		},
	}
	for i, testCase := range testCases {
		resp, ok, err := auth.AuthenticateToken(context.Background(), testCase.Token)
		// 未命中时 resp 必须为 nil，测试里后续只比较 resp.User，故先单独断言。
		if testCase.User == nil {
			if resp != nil {
				t.Errorf("%d: unexpected non-nil user %#v", i, resp.User)
			}
		} else if !reflect.DeepEqual(testCase.User, resp.User) {
			t.Errorf("%d: expected user %#v, got %#v", i, testCase.User, resp.User)
		}

		if testCase.Ok != ok {
			t.Errorf("%d: expected auth %v, got %v", i, testCase.Ok, ok)
		}
		switch {
		case err == nil && testCase.Err:
			t.Errorf("%d: unexpected nil error", i)
		case err != nil && !testCase.Err:
			t.Errorf("%d: unexpected error: %v", i, err)
		}
	}
}

// TestBadTokenFile 列数不足（如 token3,user3、token4）的文件必须整体加载失败，
// 不能跳过坏行继续使用，否则配置错误会被静默忽略。
func TestBadTokenFile(t *testing.T) {
	_, err := newWithContents(t, `
token1,user1,uid1
token2,user2,uid2
token3,user3
token4
`)
	if err == nil {
		t.Fatalf("unexpected non error")
	}
}

// TestInsufficientColumnsTokenFile 只有 token 一列的行同样应报错。
func TestInsufficientColumnsTokenFile(t *testing.T) {
	_, err := newWithContents(t, "token4\n")
	if err == nil {
		t.Fatalf("unexpected non error")
	}
}

// TestEmptyTokenTokenFile 空 token 行不报错但必须被丢弃：空 token 会让任何
// 不带凭据的请求都被认证，属于安全边界，认证器主动忽略而非依赖调用方。
func TestEmptyTokenTokenFile(t *testing.T) {
	auth, err := newWithContents(t, ",user5,uid5\n")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if len(auth.tokens) != 0 {
		t.Fatalf("empty token should not be recorded")
	}
}

// newWithContents 把内容写入临时文件再走 NewCSV 的公开路径，避免测试直接
// 依赖内部解析细节；临时文件用完即删。
func newWithContents(t *testing.T, contents string) (auth *TokenAuthenticator, err error) {
	f, err := os.CreateTemp("", "tokenfile_test")
	if err != nil {
		t.Fatalf("unexpected error creating tokenfile: %v", err)
	}
	f.Close()
	defer os.Remove(f.Name())

	if err := os.WriteFile(f.Name(), []byte(contents), 0700); err != nil {
		t.Fatalf("unexpected error writing tokenfile: %v", err)
	}

	return NewCSV(f.Name())
}
