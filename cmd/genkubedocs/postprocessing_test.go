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

package main

import (
	"testing"
)

func TestCleanupForInclude(t *testing.T) {

	var tests = []struct {
		markdown, expectedMarkdown string
	}{
		{ // 删除第一行
			// 注意：第一行是文档的标题，删除它可获得
			//     更大的 include 灵活性，例如 include 到标签页中
			markdown: "line 1\n" +
				"line 2\n" +
				"line 3",
			expectedMarkdown: "line 2\n" +
				"line 3",
		},
		{ // 删除 ###SEE ALSO 之后的所有内容
			// 注意：see also 假定文件将用作主页面（不适用于 include）
			markdown: "line 1\n" +
				"line 2\n" +
				"### SEE ALSO\n" +
				"line 3",
			expectedMarkdown: "line 2\n",
		},
	}
	for _, rt := range tests {
		actual := cleanupForInclude(rt.markdown)
		if actual != rt.expectedMarkdown {
			t.Errorf(
				"failed cleanupForInclude:\n\texpected: %s\n\t  actual: %s",
				rt.expectedMarkdown,
				actual,
			)
		}
	}

}
