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

package secrets

import (
	"reflect"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
)

// TestGetSecretString 覆盖键存在、键缺失以及 Data 为 nil 三种情况，
// 确认调用方总能得到安全可用的空串。
func TestGetSecretString(t *testing.T) {
	var tests = []struct {
		name        string
		secret      *v1.Secret
		key         string
		expectedVal string
	}{
		{
			name: "existing key",
			secret: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Data: map[string][]byte{
					"foo": []byte("bar"),
				},
			},
			key:         "foo",
			expectedVal: "bar",
		},
		{
			name: "non-existing key",
			secret: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
				Data: map[string][]byte{
					"foo": []byte("bar"),
				},
			},
			key:         "baz",
			expectedVal: "",
		},
		{
			name: "no data",
			secret: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "foo"},
			},
			key:         "foo",
			expectedVal: "",
		},
	}
	for _, rt := range tests {
		t.Run(rt.name, func(t *testing.T) {
			actual := GetData(rt.secret, rt.key)
			if actual != rt.expectedVal {
				t.Errorf(
					"failed getSecretString:\n\texpected: %s\n\t  actual: %s",
					rt.expectedVal,
					actual,
				)
			}
		})
	}
}

// TestParseSecretName 逐项验证前缀、ID 长度、字符集都必须严格符合，
// 防止把任意 Secret 误认成 bootstrap token。
func TestParseSecretName(t *testing.T) {
	tokenID, ok := ParseName("bootstrap-token-abc123")
	if !ok {
		t.Error("ParseName should accept valid name")
	}
	if tokenID != "abc123" {
		t.Error("ParseName should return token ID")
	}

	_, ok = ParseName("")
	if ok {
		t.Error("ParseName should reject blank name")
	}

	_, ok = ParseName("abc123")
	if ok {
		t.Error("ParseName should reject with no prefix")
	}

	_, ok = ParseName("bootstrap-token-")
	if ok {
		t.Error("ParseName should reject no token ID")
	}

	_, ok = ParseName("bootstrap-token-abc")
	if ok {
		t.Error("ParseName should reject short token ID")
	}

	_, ok = ParseName("bootstrap-token-abc123ghi")
	if ok {
		t.Error("ParseName should reject long token ID")
	}

	_, ok = ParseName("bootstrap-token-ABC123")
	if ok {
		t.Error("ParseName should reject invalid token ID")
	}
}

// TestGetGroups 验证默认组始终存在、空值等同未设置、非法前缀报错，
// 并对合法附加组去重和排序。
func TestGetGroups(t *testing.T) {
	tests := []struct {
		name         string
		secret       *v1.Secret
		expectResult []string
		expectError  bool
	}{
		{
			name: "not set",
			secret: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Data:       map[string][]byte{},
			},
			expectResult: []string{"system:bootstrappers"},
		},
		{
			name: "set to empty value",
			secret: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Data: map[string][]byte{
					bootstrapapi.BootstrapTokenExtraGroupsKey: []byte(""),
				},
			},
			expectResult: []string{"system:bootstrappers"},
		},
		{
			name: "invalid prefix",
			secret: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Data: map[string][]byte{
					bootstrapapi.BootstrapTokenExtraGroupsKey: []byte("foo"),
				},
			},
			expectError: true,
		},
		{
			name: "valid",
			secret: &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Data: map[string][]byte{
					bootstrapapi.BootstrapTokenExtraGroupsKey: []byte("system:bootstrappers:foo,system:bootstrappers:bar,system:bootstrappers:bar"),
				},
			},
			// 期望结果已去重并按字典序排列。
			expectResult: []string{
				"system:bootstrappers",
				"system:bootstrappers:bar",
				"system:bootstrappers:foo",
			},
		},
	}
	for _, test := range tests {
		result, err := GetGroups(test.secret)
		if test.expectError {
			if err == nil {
				t.Errorf("test %q expected an error, but didn't get one (result: %#v)", test.name, result)
			}
			continue
		}
		if err != nil {
			t.Errorf("test %q return an unexpected error: %v", test.name, err)
			continue
		}
		if !reflect.DeepEqual(result, test.expectResult) {
			t.Errorf("test %q expected %#v, got %#v", test.name, test.expectResult, result)
		}
	}
}
