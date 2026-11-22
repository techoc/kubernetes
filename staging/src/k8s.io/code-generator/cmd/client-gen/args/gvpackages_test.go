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

package args

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"k8s.io/code-generator/cmd/client-gen/types"
)

// TestGVPackageFlag 验证 --input/--input-base 的解析行为：
// group/version 如何被归并到 GroupVersions，以及包路径如何拼接。
func TestGVPackageFlag(t *testing.T) {
	tests := []struct {
		args           []string
		def            []string
		importBasePath string
		expected       map[types.GroupVersion]string
		expectedGroups []types.GroupVersions
		parseError     string
	}{
		{
			// 没有任何输入时应得到空结果，而不是 nil。
			args:           []string{},
			expected:       map[types.GroupVersion]string{},
			expectedGroups: []types.GroupVersions{},
		},
		{
			// 同一 group 的多个 version 归并到一个 GroupVersions；末尾的 "foo/bar/"
			// 表示不带版本号的 internal 包，Version 为空串。
			args: []string{"foo/bar/v1", "foo/bar/v2", "foo/bar/", "foo/v1"},
			expectedGroups: []types.GroupVersions{
				{PackageName: "bar", Group: types.Group("bar"), Versions: []types.PackageVersion{
					{Version: "v1", Package: "foo/bar/v1"},
					{Version: "v2", Package: "foo/bar/v2"},
					{Version: "", Package: "foo/bar"},
				}},
				{PackageName: "foo", Group: types.Group("foo"), Versions: []types.PackageVersion{
					{Version: "v1", Package: "foo/v1"},
				}},
			},
		},
		{
			// 默认值（def）不应引入额外的 group/version：def 只用于补全路径信息。
			args: []string{"foo/bar/v1", "foo/bar/v2", "foo/bar/", "foo/v1"},
			def:  []string{"foo/bar/v1alpha1", "foo/v1"},
			expectedGroups: []types.GroupVersions{
				{PackageName: "bar", Group: types.Group("bar"), Versions: []types.PackageVersion{
					{Version: "v1", Package: "foo/bar/v1"},
					{Version: "v2", Package: "foo/bar/v2"},
					{Version: "", Package: "foo/bar"},
				}},
				{PackageName: "foo", Group: types.Group("foo"), Versions: []types.PackageVersion{
					{Version: "v1", Package: "foo/v1"},
				}},
			},
		},
		{
			// 单层 group（group 名与包名相同）也应正确归并。
			args: []string{"api/v1", "api"},
			expectedGroups: []types.GroupVersions{
				{PackageName: "api", Group: types.Group("api"), Versions: []types.PackageVersion{
					{Version: "v1", Package: "api/v1"},
					{Version: "", Package: "api"},
				}},
			},
		},
		{
			// 指定 importBasePath 时，源码包路径应拼接为 <base>/<group>/<version>。
			args:           []string{"foo/v1"},
			importBasePath: "k8s.io/api",
			expectedGroups: []types.GroupVersions{
				{PackageName: "foo", Group: types.Group("foo"), Versions: []types.PackageVersion{
					{Version: "v1", Package: "k8s.io/api/foo/v1"},
				}},
			},
		},
	}
	for i, test := range tests {
		fs := pflag.NewFlagSet("testGVPackage", pflag.ContinueOnError)
		groups := []types.GroupVersions{}
		builder := NewGroupVersionsBuilder(&groups)
		fs.Var(NewGVPackagesValue(builder, test.def), "input", "usage")
		fs.Var(NewInputBasePathValue(builder, test.importBasePath), "input-base-path", "usage")

		// 把用例中的每个输入展开成独立的 --input 参数。
		args := []string{}
		for _, a := range test.args {
			args = append(args, fmt.Sprintf("--input=%s", a))
		}

		err := fs.Parse(args)
		if test.parseError != "" {
			if err == nil {
				t.Errorf("%d: expected error %q, got nil", i, test.parseError)
			} else if !strings.Contains(err.Error(), test.parseError) {
				t.Errorf("%d: expected error %q, got %q", i, test.parseError, err)
			}
		} else if err != nil {
			t.Errorf("%d: expected nil error, got %v", i, err)
		}
		// 解析结束后，builder 应已把结果写回 groups。
		if !reflect.DeepEqual(groups, test.expectedGroups) {
			t.Errorf("%d: expected groups %+v, got groups %+v", i, test.expectedGroups, groups)
		}
	}
}
