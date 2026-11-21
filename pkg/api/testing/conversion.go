/*
Copyright 2015 The Kubernetes Authors.

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

package testing

import (
	"testing"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// TestSelectableFieldLabelConversionsOfKind 验证给定资源的每个可选字段（selectable field）
// 都定义了对应的 label 转换规则。
// fields 参数给出该资源的可选字段集合；
// labelMap 把已弃用的 label 名映射到其规范名称。
func TestSelectableFieldLabelConversionsOfKind(t *testing.T, apiVersion string, kind string, fields fields.Set, labelMap map[string]string) {
	// 这些 label 不是合法的字段标签，转换时必须报错。
	badFieldLabels := []string{
		"name",
		".name",
		"bad",
		"metadata",
		"foo.bar",
	}

	value := "value"

	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		t.Errorf("kind=%s: got unexpected error: %v", kind, err)
		return
	}
	gvk := gv.WithKind(kind)

	if len(fields) == 0 {
		t.Logf("no selectable fields for kind %q, skipping", kind)
	}
	for label := range fields {
		// "name" 已被 "metadata.name" 取代，不应再作为可选字段存在。
		if label == "name" {
			t.Logf("FIXME: \"name\" is deprecated by \"metadata.name\", it should be removed from selectable fields of kind=%s", kind)
			continue
		}
		newLabel, newValue, err := legacyscheme.Scheme.ConvertFieldLabel(gvk, label, value)
		if err != nil {
			t.Errorf("kind=%s label=%s: got unexpected error: %v", kind, label, err)
		} else {
			expectedLabel := label
			if l, exists := labelMap[label]; exists {
				expectedLabel = l
			}
			// 转换后的 label 名应与（可能映射后的）原名一致，值保持不变。
			if newLabel != expectedLabel {
				t.Errorf("kind=%s label=%s: got unexpected label name (%q != %q)", kind, label, newLabel, expectedLabel)
			}
			if newValue != value {
				t.Errorf("kind=%s label=%s: got unexpected new value (%q != %q)", kind, label, newValue, value)
			}
		}
	}

	// 非法 label 必须全部被拒绝，否则说明转换规则过于宽松。
	for _, label := range badFieldLabels {
		_, _, err := legacyscheme.Scheme.ConvertFieldLabel(gvk, label, "value")
		if err == nil {
			t.Errorf("kind=%s label=%s: got unexpected non-error", kind, label)
		}
	}
}
