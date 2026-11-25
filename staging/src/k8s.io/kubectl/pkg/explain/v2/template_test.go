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

package v2

import (
	"os"
	"path"
	"strings"
	"testing"
)

// 确保模板被正确嵌入。
func TestRegisterBuitinTemplates(t *testing.T) {
	myGenerator := NewGenerator().(*generator)
	err := registerBuiltinTemplates(myGenerator)
	if err != nil {
		t.Fatal(err)
	}
	// 验证 generator 现在为 `templates` 目录中的每个文件
	// 都拥有一个具名模板。
	files, err := os.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}

	for _, templateFile := range files {
		name := templateFile.Name()
		ext := path.Ext(name)
		if ext != "tmpl" {
			continue
		}

		name = strings.TrimSuffix(name, ext)
		if _, ok := myGenerator.templates[name]; !ok {
			t.Fatalf("missing template: %v", name)
		}
	}
}
