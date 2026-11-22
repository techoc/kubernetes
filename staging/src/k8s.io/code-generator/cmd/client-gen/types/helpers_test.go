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

package types

import (
	"reflect"
	"sort"
	"testing"
)

// TestVersionSort 验证 sortableSliceOfVersions 的排序规则：
// 带 alpha/beta 后缀的预发布版本排在稳定版本之前，稳定版本再按数字升序
// （v1 < v3）。这决定了 client-gen 输出中各版本的排列顺序。
func TestVersionSort(t *testing.T) {
	unsortedVersions := []string{"v4beta1", "v2beta1", "v2alpha1", "v3", "v1"}
	expected := []string{"v2alpha1", "v2beta1", "v4beta1", "v1", "v3"}
	// 原地排序，断言结果与期望完全一致。
	sort.Sort(sortableSliceOfVersions(unsortedVersions))
	if !reflect.DeepEqual(unsortedVersions, expected) {
		t.Errorf("expected %#v\ngot %#v", expected, unsortedVersions)
	}
}
