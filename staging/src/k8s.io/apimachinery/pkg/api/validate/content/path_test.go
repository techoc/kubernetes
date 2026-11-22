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

package content

import (
	"strings"
	"testing"
)

// TestIsPathSegmentPrefix 校验 "路径元素前缀" 语义：只要不含 '/' 或 '%' 即可接受，
// 允许 '.'/'..' 以及空串（空串在 JSONPath 场景中表示根）。
func TestIsPathSegmentPrefix(t *testing.T) {
	testcases := map[string]struct {
		Name        string
		ExpectedMsg string
	}{
		"empty": {
			Name:        "",
			ExpectedMsg: "", // NOTE: this probably should fail
		},
		// NOTE: no validation of max length
		"valid short": {
			Name:        "foo",
			ExpectedMsg: "",
		},
		"valid long": {
			Name:        "foo.bar.baz",
			ExpectedMsg: "",
		},
		// Make sure mixed case, non DNS subdomain characters are tolerated
		"valid complex": {
			Name:        "sha256:ABCDEF012345@ABCDEF012345",
			ExpectedMsg: "",
		},
		// Make sure non-ascii characters are tolerated
		"valid extended charset": {
			Name:        "Iñtërnâtiônàlizætiøn",
			ExpectedMsg: "",
		},
		// 前缀后面还会拼接实际字段名，因此单独的 '.' 和 '..' 合法。
		"dot": {
			Name:        ".",
			ExpectedMsg: "",
		},
		"dot leading": {
			Name:        ".test",
			ExpectedMsg: "",
		},
		"dot dot": {
			Name:        "..",
			ExpectedMsg: "",
		},
		"dot dot leading": {
			Name:        "..test",
			ExpectedMsg: "",
		},
		// '/' 会让后续解析把内容误当作新的路径层级。
		"slash": {
			Name:        "foo/bar",
			ExpectedMsg: "/",
		},
		// '%' 会触发 URL 解码，导致前缀与实际路径不一致。
		"percent": {
			Name:        "foo%bar",
			ExpectedMsg: "%",
		},
	}

	for k, tc := range testcases {
		msgs := IsPathSegmentPrefix(tc.Name)
		if len(tc.ExpectedMsg) == 0 && len(msgs) > 0 {
			t.Errorf("%s: expected no error, got %v", k, msgs)
		}
		if len(tc.ExpectedMsg) > 0 && len(msgs) == 0 {
			t.Errorf("%s: expected error, got none", k)
		}
		if len(tc.ExpectedMsg) > 0 && !strings.Contains(msgs[0], tc.ExpectedMsg) {
			t.Errorf("%s: expected error containing %q, got %v", k, tc.ExpectedMsg, msgs[0])
		}
	}
}

// TestIsPathSegmentName 与 Prefix 版本的差异在于完整名称不允许等于 "." 或 ".."，
// 因为它们会被路径规范化逻辑当作当前/上级目录。
func TestIsPathSegmentName(t *testing.T) {
	testcases := map[string]struct {
		Name        string
		ExpectedMsg string
	}{
		"empty": {
			Name:        "",
			ExpectedMsg: "", // NOTE: this probably should fail
		},
		// NOTE: no validation of max length
		"valid short": {
			Name:        "foo",
			ExpectedMsg: "",
		},
		"valid long": {
			Name:        "foo.bar.baz",
			ExpectedMsg: "",
		},
		// Make sure mixed case, non DNS subdomain characters are tolerated
		"valid complex": {
			Name:        "sha256:ABCDEF012345@ABCDEF012345",
			ExpectedMsg: "",
		},
		// Make sure non-ascii characters are tolerated
		"valid extended charset": {
			Name:        "Iñtërnâtiônàlizætiøn",
			ExpectedMsg: "",
		},
		// 单独 "." 是路径遍历风险。
		"dot": {
			Name:        ".",
			ExpectedMsg: ".",
		},
		"dot leading": {
			Name:        ".test",
			ExpectedMsg: "",
		},
		// 单独 ".." 同样是路径遍历风险。
		"dot dot": {
			Name:        "..",
			ExpectedMsg: "..",
		},
		"dot dot leading": {
			Name:        "..test",
			ExpectedMsg: "",
		},
		"slash": {
			Name:        "foo/bar",
			ExpectedMsg: "/",
		},
		"percent": {
			Name:        "foo%bar",
			ExpectedMsg: "%",
		},
	}

	for k, tc := range testcases {
		msgs := IsPathSegmentName(tc.Name)
		if len(tc.ExpectedMsg) == 0 && len(msgs) > 0 {
			t.Errorf("%s: expected no error, got %v", k, msgs)
		}
		if len(tc.ExpectedMsg) > 0 && len(msgs) == 0 {
			t.Errorf("%s: expected error, got none", k)
		}
		if len(tc.ExpectedMsg) > 0 && !strings.Contains(msgs[0], tc.ExpectedMsg) {
			t.Errorf("%s: expected error containing %q, got %v", k, tc.ExpectedMsg, msgs[0])
		}
	}
}
