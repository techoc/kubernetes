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

package oidc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// TestJSONTime 验证 jsonTime 既能解析整数 Unix 时间戳，也能解析浮点科学计数法形式；
// 两种写法在 JWT 的 exp 声明中都可能出现。
func TestJSONTime(t *testing.T) {
	data := `{
		"t1": 1493851263,
		"t2": 1.493851263e9
	}`

	var v struct {
		T1 jsonTime `json:"t1"`
		T2 jsonTime `json:"t2"`
	}
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		t.Fatal(err)
	}
	wantT1 := time.Unix(1493851263, 0)
	wantT2 := time.Unix(1493851263, 0)
	gotT1 := time.Time(v.T1)
	gotT2 := time.Time(v.T2)

	if !wantT1.Equal(gotT1) {
		t.Errorf("t1 value: wanted %s got %s", wantT1, gotT1)
	}
	if !wantT2.Equal(gotT2) {
		t.Errorf("t2 value: wanted %s got %s", wantT2, gotT2)
	}
}

// encodeJWT 用无填充的 base64url 拼出一个结构合法的 JWT，签名可以是任意字符串：
// 这里只测试载荷解析与过期判断，不需要真实验签。
func encodeJWT(header, payload, sig string) string {
	e := func(s string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(s))
	}
	return e(header) + "." + e(payload) + "." + e(sig)
}

// TestExpired 固定“当前时间”后校验 idTokenExpired 对有效期、过期、非法 exp 声明
// 以及根本不是 JWT 的输入的行为。
func TestExpired(t *testing.T) {
	now := time.Now()

	nowFunc := func() time.Time { return now }

	tests := []struct {
		name        string
		idToken     string
		wantErr     bool
		wantExpired bool
	}{
		{
			name: "valid",
			// exp 在一小时后，未过期。
			idToken: encodeJWT(
				"{}",
				fmt.Sprintf(`{"exp":%d}`, now.Add(time.Hour).Unix()),
				"blah", // signature isn't veified.
			),
		},
		{
			name: "expired",
			// exp 在一小时前，应判定为已过期。
			idToken: encodeJWT(
				"{}",
				fmt.Sprintf(`{"exp":%d}`, now.Add(-time.Hour).Unix()),
				"blah", // signature isn't veified.
			),
			wantExpired: true,
		},
		{
			name: "bad exp claim",
			// exp 是字符串而非数字，解析阶段就应报错。
			idToken: encodeJWT(
				"{}",
				`{"exp":"foobar"}`,
				"blah", // signature isn't veified.
			),
			wantErr: true,
		},
		{
			name: "not an id token",
			// 连三段式结构都不满足，必然报错。
			idToken: "notanidtoken",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			valid, err := idTokenExpired(nowFunc, test.idToken)
			if err != nil {
				if !test.wantErr {
					t.Errorf("parse error: %v", err)
				}
				return
			}
			if test.wantExpired == valid {
				t.Errorf("wanted expired %t, got %t", test.wantExpired, !valid)
			}
		})
	}
}

// TestClientCache 覆盖 clientCache 的“先到先得”语义：同一 (cluster, issuer, clientID)
// 重复 setClient 返回已有实例，不同 cluster 则分别缓存。
func TestClientCache(t *testing.T) {
	cache := newClientCache()

	// 空缓存不应命中。
	if _, ok := cache.getClient("cluster1", "issuer1", "id1"); ok {
		t.Fatalf("got client before putting one in the cache")
	}
	assertCacheLen(t, cache, 0)

	cli1 := new(oidcAuthProvider)
	cli2 := new(oidcAuthProvider)
	cli3 := new(oidcAuthProvider)

	gotcli := cache.setClient("cluster1", "issuer1", "id1", cli1)
	if cli1 != gotcli {
		t.Fatalf("set first client and got a different one")
	}
	assertCacheLen(t, cache, 1)

	// 相同 key 再次写入：应返回先前缓存的 cli1，而不是覆盖成 cli2。
	gotcli = cache.setClient("cluster1", "issuer1", "id1", cli2)
	if cli1 != gotcli {
		t.Fatalf("set a second client and didn't get the first")
	}
	assertCacheLen(t, cache, 1)

	// 不同 cluster 是不同 key，应各自缓存并返回新实例。
	gotcli = cache.setClient("cluster2", "issuer1", "id1", cli3)
	if cli1 == gotcli {
		t.Fatalf("set a third client and got the first")
	}
	if cli3 != gotcli {
		t.Fatalf("set third client and got a different one")
	}
	assertCacheLen(t, cache, 2)
}

// assertCacheLen 断言缓存条目数，用于确认 setClient 没有意外新增或覆盖。
func assertCacheLen(t *testing.T, cache *clientCache, length int) {
	t.Helper()
	if len(cache.cache) != length {
		t.Errorf("expected cache length %d got %d", length, len(cache.cache))
	}
}
