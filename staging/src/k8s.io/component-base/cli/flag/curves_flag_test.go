/*
Copyright The Kubernetes Authors.

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

package flag

import (
	"crypto/tls"
	"reflect"
	"testing"
)

// TestTLSCurvePreferences 覆盖曲线 ID 到 tls.CurveID 的转换，
// 重点在于：空输入保持 nil、重复/越界/未知 ID 必须报错，
// 以及现行 Go 支持的所有标准曲线都能正确映射。
func TestTLSCurvePreferences(t *testing.T) {
	tests := []struct {
		name        string
		input       []int32
		expected    []tls.CurveID
		expectError bool
	}{
		{
			name:     "multiple curves",
			input:    []int32{23, 24, 29},
			expected: []tls.CurveID{tls.CurveP256, tls.CurveP384, tls.X25519},
		},
		{
			name:     "single curve",
			input:    []int32{29},
			expected: []tls.CurveID{tls.X25519},
		},
		{
			// 空和非 nil 输入都返回 nil，语义上等同于“使用 Go 默认值”。
			name:     "empty input",
			input:    []int32{},
			expected: nil,
		},
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			// 包含后量子曲线 X25519MLKEM768，确保新增标准曲线同样被接受。
			name:     "all standard curves",
			input:    []int32{23, 24, 25, 29, 4588},
			expected: []tls.CurveID{tls.CurveP256, tls.CurveP384, tls.CurveP521, tls.X25519, tls.X25519MLKEM768},
		},
		{
			name:        "duplicate curve",
			input:       []int32{23, 29, 23},
			expectError: true,
		},
		{
			name:        "zero value",
			input:       []int32{0},
			expectError: true,
		},
		{
			name:        "negative value",
			input:       []int32{-1},
			expectError: true,
		},
		{
			name:        "exceeds uint16 range",
			input:       []int32{70000},
			expectError: true,
		},
		{
			name:        "unknown curve ID",
			input:       []int32{9999},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TLSCurvePreferences(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("TLSCurvePreferences(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("TLSCurvePreferences(%v) unexpected error: %v", tt.input, err)
				return
			}
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("TLSCurvePreferences(%v) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
