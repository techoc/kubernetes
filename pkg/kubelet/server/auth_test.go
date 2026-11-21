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

package server

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestIsSubPath 验证子路径判断：相等、根路径下、子路径与不匹配场景。
func TestIsSubPath(t *testing.T) {
	// 用表驱动覆盖相等、根路径、前缀相似但不匹配等情况。
	testcases := map[string]struct {
		subpath  string
		path     string
		expected bool
	}{
		"empty": {subpath: "", path: "", expected: true},

		"match 1": {subpath: "foo", path: "foo", expected: true},
		"match 2": {subpath: "/foo", path: "/foo", expected: true},
		"match 3": {subpath: "/foo/", path: "/foo/", expected: true},
		"match 4": {subpath: "/foo/bar", path: "/foo/bar", expected: true},

		"subpath of root 1": {subpath: "/foo", path: "/", expected: true},
		"subpath of root 2": {subpath: "/foo/", path: "/", expected: true},
		"subpath of root 3": {subpath: "/foo/bar", path: "/", expected: true},

		"subpath of path 1": {subpath: "/foo", path: "/foo", expected: true},
		"subpath of path 2": {subpath: "/foo/", path: "/foo", expected: true},
		"subpath of path 3": {subpath: "/foo/bar", path: "/foo", expected: true},

		"mismatch 1": {subpath: "/foo", path: "/bar", expected: false},
		"mismatch 2": {subpath: "/foo", path: "/foobar", expected: false},
		"mismatch 3": {subpath: "/foobar", path: "/foo", expected: false},
	}

	// 逐用例比较 isSubpath 的结果。
	for k, tc := range testcases {
		result := isSubpath(tc.subpath, tc.path)
		if result != tc.expected {
			t.Errorf("%s: expected %v, got %v", k, tc.expected, result)
		}
	}
}

// TestGetRequestAttributes 验证节点请求属性的构建，分别在精细授权开关下运行。
func TestGetRequestAttributes(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 分别以关闭/开启精细授权运行同一组用例。
	for _, fineGrained := range []bool{false, true} {
		if !fineGrained {
			// 关闭精细授权时模拟 1.35 版本，避免新版本默认值影响结果。
			featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
		}
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletFineGrainedAuthz, fineGrained)
		// 用例来自 AuthzTestCases，期望值随精细授权开关变化。
		for _, test := range AuthzTestCases(fineGrained) {
			t.Run(test.Method+":"+test.Path, func(t *testing.T) {
				// 构造属性获取器并生成请求。
				getter := NewNodeAuthorizerAttributesGetter(authzTestNodeName)

				req, err := http.NewRequest(test.Method, "https://localhost:1234"+test.Path, nil)
				require.NoError(t, err)
				attrs := getter.GetRequestAttributes(tCtx, AuthzTestUser(), req)

				test.AssertAttributes(t, attrs)
			})
		}
	}
}

// 测试用节点名与用户名。
const (
	authzTestNodeName = "test"
	authzTestUserName = "phibby"
)

// AuthzTestCase 描述一个授权测试用例：HTTP 方法、路径与期望的子资源。
type AuthzTestCase struct {
	Method, Path string

	ExpectedVerb         string
	ExpectedSubresources []string
}

// AssertAttributes 按期望子资源逐条比对属性。
func (a *AuthzTestCase) AssertAttributes(t *testing.T, attrs []authorizer.Attributes) {
	var expectedAttributes []authorizer.AttributesRecord
	for _, subresource := range a.ExpectedSubresources {
		expectedAttributes = append(expectedAttributes, authorizer.AttributesRecord{
			User:            AuthzTestUser(),
			APIGroup:        "",
			APIVersion:      "v1",
			Verb:            a.ExpectedVerb,
			Resource:        "nodes",
			Name:            authzTestNodeName,
			Subresource:     subresource,
			ResourceRequest: true,
			Path:            a.Path,
		})
	}

	assert.Equal(t, len(attrs), len(expectedAttributes))
	for i := range attrs {
		assert.Equal(t, attrs[i], expectedAttributes[i])
	}
}

// AuthzTestUser 返回测试用户。
func AuthzTestUser() user.Info {
	return &user.DefaultInfo{Name: authzTestUserName}
}

// AuthzTestCases 返回所有路径在各 HTTP 方法下的测试用例。
func AuthzTestCases(fineGrained bool) []AuthzTestCase {
	// 路径 -> 期望子资源。
	testPaths := map[string][]string{
		"/attach/{podNamespace}/{podID}/{containerName}":       {"proxy"},
		"/attach/{podNamespace}/{podID}/{uid}/{containerName}": {"proxy"},
		"/checkpoint/{podNamespace}/{podID}/{containerName}":   {"checkpoint"},
		"/configz": {"proxy"},
		"/flagz":   {"configz"},
		"/statusz": {"statusz"},
		"/containerLogs/{podNamespace}/{podID}/{containerName}": {"proxy"},
		"/debug/flags/v":                                     {"proxy"},
		"/debug/pprof/{subpath:*}":                           {"proxy"},
		"/exec/{podNamespace}/{podID}/{containerName}":       {"proxy"},
		"/exec/{podNamespace}/{podID}/{uid}/{containerName}": {"proxy"},
		"/healthz":                            {"proxy"},
		"/healthz/log":                        {"proxy"},
		"/healthz/ping":                       {"proxy"},
		"/healthz/syncloop":                   {"proxy"},
		"/logs/":                              {"log"},
		"/logs/{logpath:*}":                   {"log"},
		"/metrics":                            {"metrics"},
		"/metrics/slis":                       {"metrics"},
		"/metrics/cadvisor":                   {"metrics"},
		"/metrics/probes":                     {"metrics"},
		"/metrics/resource":                   {"metrics"},
		"/pods/":                              {"proxy"},
		"/portForward/{podNamespace}/{podID}": {"proxy"},
		"/portForward/{podNamespace}/{podID}/{uid}":         {"proxy"},
		"/run/{podNamespace}/{podID}/{containerName}":       {"proxy"},
		"/run/{podNamespace}/{podID}/{uid}/{containerName}": {"proxy"},
		"/runningpods/":  {"proxy"},
		"/stats/":        {"stats"},
		"/stats/summary": {"stats"},
	}

	// 精细授权开启时，部分路径额外要求 healthz/pods/configz 子资源。
	if fineGrained {
		testPaths["/healthz"] = append([]string{"healthz"}, testPaths["/healthz"]...)
		testPaths["/healthz/log"] = append([]string{"healthz"}, testPaths["/healthz/log"]...)
		testPaths["/healthz/ping"] = append([]string{"healthz"}, testPaths["/healthz/ping"]...)
		testPaths["/healthz/syncloop"] = append([]string{"healthz"}, testPaths["/healthz/syncloop"]...)
		testPaths["/pods/"] = append([]string{"pods"}, testPaths["/pods/"]...)
		testPaths["/runningpods/"] = append([]string{"pods"}, testPaths["/runningpods/"]...)
		testPaths["/configz"] = append([]string{"configz"}, testPaths["/configz"]...)
	}

	// 每个路径与每种 HTTP 方法组合成一个用例。
	testCases := []AuthzTestCase{}
	for path, subresource := range testPaths {
		testCases = append(testCases,
			AuthzTestCase{"POST", path, "create", subresource},
			AuthzTestCase{"GET", path, "get", subresource},
			AuthzTestCase{"PUT", path, "update", subresource},
			AuthzTestCase{"PATCH", path, "patch", subresource},
			AuthzTestCase{"DELETE", path, "delete", subresource})
	}
	return testCases
}
