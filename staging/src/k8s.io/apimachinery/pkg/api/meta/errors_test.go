/*
Copyright 2022 The Kubernetes Authors.

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

package meta

import (
	"errors"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestErrorMatching 校验各类元数据错误通过 errors.Is 与 matcher 函数按类型匹配的行为。
func TestErrorMatching(t *testing.T) {
	testCases := []struct {
		name string
		// input 必须包含一个 _非空_ 的错误，否则 errors 库基于 reflectlite.DeepEqual 的比较会总是成功；
		// 这里想验证的是匹配基于类型而非值的相等。
		input       error
		new         func() error
		matcherFunc func(error) bool
	}{
		{
			name:        "AmbiguousResourceError",
			input:       &AmbiguousResourceError{MatchingResources: []schema.GroupVersionResource{{}}},
			new:         func() error { return &AmbiguousResourceError{} },
			matcherFunc: IsAmbiguousError,
		},
		{
			name:        "AmbiguousKindError",
			input:       &AmbiguousKindError{MatchingResources: []schema.GroupVersionResource{{}}},
			new:         func() error { return &AmbiguousKindError{} },
			matcherFunc: IsAmbiguousError,
		},
		{
			name:        "NoResourceMatchError",
			input:       &NoResourceMatchError{PartialResource: schema.GroupVersionResource{Group: "foo"}},
			new:         func() error { return &NoResourceMatchError{} },
			matcherFunc: IsNoMatchError,
		},
		{
			name:        "NoKindMatchError",
			input:       &NoKindMatchError{SearchedVersions: []string{"foo"}},
			new:         func() error { return &NoKindMatchError{} },
			matcherFunc: IsNoMatchError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 自身匹配自身
			if !errors.Is(tc.input, tc.new()) {
				t.Error("error doesn't match itself directly")
			}
			// 包装后仍能匹配
			if !errors.Is(fmt.Errorf("wrapepd: %w", tc.input), tc.new()) {
				t.Error("error doesn't match itself when wrapped")
			}
			// 对应的 matcher 函数应返回 true
			if !tc.matcherFunc(tc.input) {
				t.Errorf("error doesn't get matched by matcherfunc")
			}
			// 不应误匹配无关错误
			if errors.Is(tc.input, errors.New("foo")) {
				t.Error("error incorrectly matches other error")
			}
		})
	}
}
