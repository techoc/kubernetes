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

package conversion

import "testing"

// TestInvalidPtrValueKind 验证 EnforcePtr 对非指针值返回错误。
// 注意当前实现是在 default 分支中调用，等价于只覆盖 interface{} 的动态
// 非指针情形。
func TestInvalidPtrValueKind(t *testing.T) {
	var simple interface{}
	switch obj := simple.(type) {
	default:
		_, err := EnforcePtr(obj)
		if err == nil {
			t.Errorf("Expected error on invalid kind")
		}
	}
}

// TestEnforceNilPtr 验证类型正确但值为 nil 的指针也会被拒绝：调用方
// 不能把 nil 指针当作可写的目标。
func TestEnforceNilPtr(t *testing.T) {
	var nilPtr *struct{}
	_, err := EnforcePtr(nilPtr)
	if err == nil {
		t.Errorf("Expected error on nil pointer")
	}
}
