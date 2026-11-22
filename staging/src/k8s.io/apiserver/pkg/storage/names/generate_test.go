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

package names

import (
	"strings"
	"testing"
)

// TestSimpleNameGenerator 验证生成的名字保留前缀且不是裸前缀本身：
// 后者会导致同一 generateName 的多次创建产生冲突。
func TestSimpleNameGenerator(t *testing.T) {
	name := SimpleNameGenerator.GenerateName("foo")
	if !strings.HasPrefix(name, "foo") || name == "foo" {
		t.Errorf("unexpected name: %s", name)
	}
}
