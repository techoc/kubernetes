/*
Copyright 2024 The Kubernetes Authors.

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
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPreconditionsCheckWithNilObject 验证前置条件检查在对象为 nil 时返回
// 内部错误而非 panic；错误须可被 errors.As 识别为 InternalError。
func TestPreconditionsCheckWithNilObject(t *testing.T) {
	p := &Preconditions{}
	err := p.Check("foo", nil)
	if err == nil {
		t.Fatalf("expected an error")
	}

	var internalErr InternalError
	if !errors.As(err, &internalErr) {
		t.Fatalf("expected error to be of type: %T, but got: %#v", InternalError{}, err)
	}
	if want := "can't enforce preconditions"; !strings.Contains(internalErr.Error(), want) {
		t.Errorf("expected error to contain %q", want)
	}
}

// TestValidateListOptions 覆盖 list 请求 revision 的归一化规则：
// continue token 与 resourceVersion 的互斥、非法值报错、不同 match 策略
// 以及“只有递归且带 limit 时才保留精确 revision”的兼容语义。
func TestValidateListOptions(t *testing.T) {
	testCases := []struct {
		name                string
		opts                ListOptions
		expectedError       string
		expectedRev         int64
		expectedContinueKey string
	}{
		{
			// continue token 自带 revision，再指定 resourceVersion 会产生歧义。
			name: "specifying resource version when using continue",
			opts: ListOptions{
				Recursive:       true,
				ResourceVersion: "200",
				Predicate: SelectionPredicate{
					Continue: encodeContinueOrDie("meta.k8s.io/v1", 100, "continue"),
				},
			},
			expectedError: "specifying resource version is not allowed when using continue",
		},
		{
			name: "invalid resource version",
			opts: ListOptions{
				ResourceVersion: "invalid",
			},
			expectedError: "invalid resource version",
		},
		{
			name: "unknown ResourceVersionMatch value",
			opts: ListOptions{
				ResourceVersion:      "200",
				ResourceVersionMatch: "unknown",
			},
			expectedError: "unknown ResourceVersionMatch value",
		},
		{
			name: "use continueRV",
			opts: ListOptions{
				ResourceVersion: "0",
				Recursive:       true,
				Predicate: SelectionPredicate{
					Continue: encodeContinueOrDie("meta.k8s.io/v1", 100, "continue"),
				},
			},
			expectedRev:         100,
			expectedContinueKey: "continue",
		},
		{
			name: "use continueRV with empty rv",
			opts: ListOptions{
				ResourceVersion: "",
				Recursive:       true,
				Predicate: SelectionPredicate{
					Continue: encodeContinueOrDie("meta.k8s.io/v1", 100, "continue"),
				},
			},
			expectedRev:         100,
			expectedContinueKey: "continue",
		},
		{
			// continue token 内 revision 为 0 视为非法：无法定位分页起点。
			name: "continueRV = 0",
			opts: ListOptions{
				ResourceVersion: "",
				Recursive:       true,
				Predicate: SelectionPredicate{
					Continue: encodeContinueOrDie("meta.k8s.io/v1", 0, "continue"),
				},
			},
			expectedError: "invalid continue token",
		},
		{
			name: "continueRV < 0",
			opts: ListOptions{
				ResourceVersion: "",
				Recursive:       true,
				Predicate: SelectionPredicate{
					Continue: encodeContinueOrDie("meta.k8s.io/v1", -1, "continue"),
				},
			},
			expectedRev:         0,
			expectedContinueKey: "continue",
		},
		{
			name:        "default",
			opts:        ListOptions{},
			expectedRev: 0,
		},
		{
			// NotOlderThan 在缓存层没有精确快照语义，归一化为 0（任意不旧于）。
			name: "rev resolve to 0 if ResourceVersionMatchNotOlderThan",
			opts: ListOptions{
				ResourceVersion:      "200",
				ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			},
			expectedRev: 0,
		},
		{
			name: "specified rev if ResourceVersionMatchExact",
			opts: ListOptions{
				ResourceVersion:      "200",
				ResourceVersionMatch: metav1.ResourceVersionMatchExact,
			},
			expectedRev: 200,
		},
		{
			name: "rev resolve to 0 if not recursive",
			opts: ListOptions{
				ResourceVersion: "200",
				Predicate: SelectionPredicate{
					Limit: 1,
				},
			},
			expectedRev: 0,
		},
		{
			name: "rev resolve to 0 if limit unspecified",
			opts: ListOptions{
				ResourceVersion: "200",
				Recursive:       true,
			},
			expectedRev: 0,
		},
		{
			name: "specified rev if recursive with limit",
			opts: ListOptions{
				ResourceVersion: "200",
				Recursive:       true,
				Predicate: SelectionPredicate{
					Limit: 1,
				},
			},
			expectedRev: 200,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			withRev, continueKey, err := ValidateListOptions("", APIObjectVersioner{}, tt.opts)
			if len(tt.expectedError) > 0 {
				if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
					t.Fatalf("expected error: %s, but got: %v", tt.expectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveRevForGetList failed: %v", err)
			}
			if withRev != tt.expectedRev {
				t.Errorf("%s: expecting rev = %d, but get %d", tt.name, tt.expectedRev, withRev)
			}
			if continueKey != tt.expectedContinueKey {
				t.Errorf("%s: expecting continueKey = %q, but get %q", tt.name, tt.expectedContinueKey, continueKey)
			}
		})
	}
}
