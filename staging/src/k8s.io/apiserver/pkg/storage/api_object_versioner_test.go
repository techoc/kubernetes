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

package storage

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage/testresource"
)

// TestObjectVersioner 验证 ResourceVersion 的读取、非法值处理与写入。
//
// 非法 RV 必须返回错误而不是 0 值，否则会把“数据损坏”误判为“版本 0”，
// 导致 watch 从错误的起点回放。
func TestObjectVersioner(t *testing.T) {
	v := APIObjectVersioner{}
	if ver, err := v.ObjectResourceVersion(&testresource.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"}}); err != nil || ver != 5 {
		t.Errorf("unexpected version: %d %v", ver, err)
	}
	if ver, err := v.ObjectResourceVersion(&testresource.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "a"}}); err == nil || ver != 0 {
		t.Errorf("unexpected version: %d %v", ver, err)
	}
	// UpdateObject 写入新的 RV，且不应顺带改动 DeletionTimestamp 等无关字段。
	obj := &testresource.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "a"}}
	if err := v.UpdateObject(obj, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if obj.ResourceVersion != "5" || obj.DeletionTimestamp != nil {
		t.Errorf("unexpected resource version: %#v", obj)
	}
}

// TestEtcdParseResourceVersion 覆盖解析边界：空串视为 0（list/watch 的“最新”语义），
// 非数字与纯空格属于无效输入，必须返回 Invalid 错误。
func TestEtcdParseResourceVersion(t *testing.T) {
	testCases := []struct {
		Version       string
		ExpectVersion uint64
		Err           bool
	}{
		{Version: "", ExpectVersion: 0},
		{Version: "a", Err: true},
		{Version: " ", Err: true},
		{Version: "1", ExpectVersion: 1},
		{Version: "10", ExpectVersion: 10},
	}

	v := APIObjectVersioner{}
	testFuncs := []func(string) (uint64, error){
		v.ParseResourceVersion,
	}

	for _, testCase := range testCases {
		for i, f := range testFuncs {
			version, err := f(testCase.Version)
			switch {
			case testCase.Err && err == nil:
				t.Errorf("%s[%v]: unexpected non-error", testCase.Version, i)
			// 错误类型必须是 Invalid：上层据此返回 400 而非 500。
			case testCase.Err && !IsInvalidError(err):
				t.Errorf("%s[%v]: unexpected error: %v", testCase.Version, i, err)
			case !testCase.Err && err != nil:
				t.Errorf("%s[%v]: unexpected error: %v", testCase.Version, i, err)
			}
			if version != testCase.ExpectVersion {
				t.Errorf("%s[%v]: expected version %d but was %d", testCase.Version, i, testCase.ExpectVersion, version)
			}
		}
	}
}

// TestCompareResourceVersion 验证比较函数返回 -1/0/1，且按数值而非字典序比较。
func TestCompareResourceVersion(t *testing.T) {
	five := &testresource.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"}}
	six := &testresource.TestResource{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "6"}}

	versioner := APIObjectVersioner{}

	if e, a := -1, versioner.CompareResourceVersion(five, six); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
	if e, a := 1, versioner.CompareResourceVersion(six, five); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
	if e, a := 0, versioner.CompareResourceVersion(six, six); e != a {
		t.Errorf("expected %v got %v", e, a)
	}
}
