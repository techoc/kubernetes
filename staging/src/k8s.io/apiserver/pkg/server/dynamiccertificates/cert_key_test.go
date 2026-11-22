/*
Copyright 2019 The Kubernetes Authors.

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

package dynamiccertificates

import (
	"testing"
)

// TestCertKeyContentEquals 验证证书内容的相等判断，用于决定是否需要重新加载。
//
// nil 与空对象必须区分：两者都返回 false 才能触发一次“从无到有”的加载；
// 但两个 nil 视为相等，避免空快照被反复判定为变化。
func TestCertKeyContentEquals(t *testing.T) {
	tests := []struct {
		name     string
		lhs      *certKeyContent
		rhs      *certKeyContent
		expected bool
	}{
		{
			name:     "both nil",
			expected: true,
		},
		{
			name:     "lhs nil",
			rhs:      &certKeyContent{},
			expected: false,
		},
		{
			name:     "rhs nil",
			lhs:      &certKeyContent{},
			expected: false,
		},
		{
			name:     "same",
			lhs:      &certKeyContent{cert: []byte("foo"), key: []byte("baz")},
			rhs:      &certKeyContent{cert: []byte("foo"), key: []byte("baz")},
			expected: true,
		},
		{
			name:     "different cert",
			lhs:      &certKeyContent{cert: []byte("foo"), key: []byte("baz")},
			rhs:      &certKeyContent{cert: []byte("bar"), key: []byte("baz")},
			expected: false,
		},
		{
			name:     "different key",
			lhs:      &certKeyContent{cert: []byte("foo"), key: []byte("baz")},
			rhs:      &certKeyContent{cert: []byte("foo"), key: []byte("qux")},
			expected: false,
		},
		{
			name:     "different cert and key",
			lhs:      &certKeyContent{cert: []byte("foo"), key: []byte("baz")},
			rhs:      &certKeyContent{cert: []byte("bar"), key: []byte("qux")},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.lhs.Equal(test.rhs)
			if actual != test.expected {
				t.Error(actual)
			}
		})
	}
}

// TestSNICertKeyContentEquals 在证书/私钥比较之外，额外要求 SNI 名称列表完全一致
// （顺序敏感、长度敏感）：名称变化同样意味着需要更新 TLS 配置。
func TestSNICertKeyContentEquals(t *testing.T) {
	tests := []struct {
		name     string
		lhs      *sniCertKeyContent
		rhs      *sniCertKeyContent
		expected bool
	}{
		{
			name:     "both nil",
			expected: true,
		},
		{
			name:     "lhs nil",
			rhs:      &sniCertKeyContent{},
			expected: false,
		},
		{
			name:     "rhs nil",
			lhs:      &sniCertKeyContent{},
			expected: false,
		},
		{
			name:     "same",
			lhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a"}},
			rhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a"}},
			expected: true,
		},
		{
			name:     "different cert",
			lhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a"}},
			rhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("bar"), key: []byte("baz")}, sniNames: []string{"a"}},
			expected: false,
		},
		{
			name:     "different key",
			lhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a"}},
			rhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("qux")}, sniNames: []string{"a"}},
			expected: false,
		},
		{
			name:     "different cert and key",
			lhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a"}},
			rhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("bar"), key: []byte("qux")}, sniNames: []string{"a"}},
			expected: false,
		},
		{
			name:     "different names",
			lhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a"}},
			rhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"b"}},
			expected: false,
		},
		{
			name:     "extra names",
			lhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a"}},
			rhs:      &sniCertKeyContent{certKeyContent: certKeyContent{cert: []byte("foo"), key: []byte("baz")}, sniNames: []string{"a", "b"}},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.lhs.Equal(test.rhs)
			if actual != test.expected {
				t.Error(actual)
			}
		})
	}
}
