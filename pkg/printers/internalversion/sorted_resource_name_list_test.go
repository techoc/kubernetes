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

package internalversion

import (
	"reflect"
	"sort"
	"testing"

	api "k8s.io/kubernetes/pkg/apis/core"
)

// TestSortableResourceNamesSorting 验证 SortableResourceNames 类型实现了 sort.Interface，
// 能正确按字典序排序资源名称。空字符串排在最前，数字排在字母前。
func TestSortableResourceNamesSorting(t *testing.T) {
	want := SortableResourceNames{
		api.ResourceName(""),
		api.ResourceName("42"),
		api.ResourceName("bar"),
		api.ResourceName("foo"),
		api.ResourceName("foo"),
		api.ResourceName("foobar"),
	}

	in := SortableResourceNames{
		api.ResourceName("foo"),
		api.ResourceName("42"),
		api.ResourceName("foobar"),
		api.ResourceName("foo"),
		api.ResourceName("bar"),
		api.ResourceName(""),
	}

	sort.Sort(in)
	if !reflect.DeepEqual(in, want) {
		t.Errorf("got %v, want %v", in, want)
	}
}
