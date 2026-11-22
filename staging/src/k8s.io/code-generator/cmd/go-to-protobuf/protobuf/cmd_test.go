/*
Copyright 2020 The Kubernetes Authors.

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

package protobuf

import (
	"reflect"
	"testing"
)

// TestImportOrder 验证 importOrder 的拓扑排序：被依赖的包先输出，
// 存在环时必须报错而不是死循环或产生不确定顺序。
func TestImportOrder(t *testing.T) {
	testcases := []struct {
		Name      string
		Input     map[string][]string
		Expect    []string
		ExpectErr bool
	}{
		{
			Name:   "empty",
			Input:  nil,
			Expect: []string{},
		},
		{
			// 依赖链 apps -> extensions -> core -> meta，输出应反向。
			Name:   "simple",
			Input:  map[string][]string{"apps": {"core", "extensions", "meta"}, "extensions": {"core", "meta"}, "core": {"meta"}},
			Expect: []string{"meta", "core", "extensions", "apps"},
		},
		{
			// core 依赖 apps，而 apps 间接依赖 core，构成环。
			Name:      "cycle",
			Input:     map[string][]string{"apps": {"core", "extensions", "meta"}, "extensions": {"core", "meta"}, "core": {"meta", "apps"}},
			ExpectErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			order, err := importOrder(tc.Input)
			if err != nil {
				if !tc.ExpectErr {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if tc.ExpectErr {
				t.Fatalf("expected error, got none")
			}
			if !reflect.DeepEqual(order, tc.Expect) {
				t.Fatalf("expected %v, got %v", tc.Expect, order)
			}
		})
	}
}
