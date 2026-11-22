/*
Copyright 2017 The Kubernetes Authors.

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

package clientcmd

import (
	"testing"

	"github.com/spf13/pflag"
)

// TestNamespacePrefixStrip 验证 --namespace 值中的 "namespace/..."、"ns/..."
// 前缀会被剥离，且前缀匹配不区分大小写；只有前缀而没有名字时保持原值。
func TestNamespacePrefixStrip(t *testing.T) {
	testData := map[string]string{
		"namespaces/foo": "foo",
		"NAMESPACES/foo": "foo",
		"NameSpaces/foo": "foo",
		"namespace/foo":  "foo",
		"NAMESPACE/foo":  "foo",
		"nameSpace/foo":  "foo",
		"ns/foo":         "foo",
		"NS/foo":         "foo",
		// 无具体命名空间时不剥离，避免得到空字符串后回退到默认命名空间。
		"namespaces/": "namespaces/",
		"namespace/":  "namespace/",
		"ns/":         "ns/",
	}

	for before, after := range testData {
		overrides := &ConfigOverrides{}
		fs := &pflag.FlagSet{}
		BindOverrideFlags(overrides, fs, RecommendedConfigOverrideFlags(""))
		fs.Parse([]string{"--namespace", before})

		if overrides.Context.Namespace != after {
			t.Fatalf("Expected %s, got %s", after, overrides.Context.Namespace)
		}
	}
}
