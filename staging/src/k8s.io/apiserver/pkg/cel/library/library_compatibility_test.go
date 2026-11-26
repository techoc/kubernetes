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

package library

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/decls"
	"github.com/google/cel-go/common/types"

	"k8s.io/apimachinery/pkg/util/sets"
)

// TestLibraryCompatibility 锁定所有 CEL 库函数名及其引入版本。
// 该测试是兼容性闸门：新增函数必须登记到对应 Kubernetes 版本，
// 且库名必须以 "kubernetes." 前缀命名，避免与 cel-go 内建函数冲突。
func TestLibraryCompatibility(t *testing.T) {
	functionNames := sets.New[string]()
	for _, lib := range KnownLibraries() {
		// 前缀约定保证函数不会与上游 cel-go 声明重名。
		if !strings.HasPrefix(lib.LibraryName(), "kubernetes.") {
			t.Errorf("Expected all kubernetes CEL libraries to have a name package with a 'kubernetes.' prefix but got %v", lib.LibraryName())
		}
		for name := range lib.declarations() {
			functionNames[name] = struct{}{}
		}
	}

	// WARN: 所有库改动必须遵循
	// https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/2876-crd-validation-expression-language#function-library-updates
	// 并在此记录函数及其引入的 Kubernetes 版本。
	knownFunctions := sets.New(
		// Kubernetes 1.24:
		"isSorted", "sum", "max", "min", "indexOf", "lastIndexOf", "find", "findAll", "url", "getScheme", "getHost", "getHostname",
		"getPort", "getEscapedPath", "getQuery", "isURL",
		// Kubernetes <1.27>:
		"path", "group", "serviceAccount", "resource", "subresource", "namespace", "name", "check", "allowed", "reason",
		// Kubernetes <1.28>:
		"errored", "error",
		// Kubernetes <1.29>:
		"add", "asApproximateFloat", "asInteger", "compareTo", "isGreaterThan", "isInteger", "isLessThan", "isQuantity", "quantity", "sign", "sub",
		// Kubernetes <1.30>:
		"ip", "family", "isUnspecified", "isLoopback", "isLinkLocalMulticast", "isLinkLocalUnicast", "isGlobalUnicast", "ip.isCanonical", "isIP", "cidr", "containsIP", "containsCIDR", "masked", "prefixLength", "isCIDR", "string",
		// Kubernetes <1.31>:
		"fieldSelector", "labelSelector", "validate", "format.named", "isSemver", "major", "minor", "patch", "semver",
		// Kubernetes <1.32>:
		"jsonpatch.escapeKey",
		// Kubernetes <1.33>:
		"semver", "isSemver", "major", "minor", "patch",
		// Kubernetes <1.37>:
		"includes",
		// Kubernetes <1.??>:
	)

	// TODO: 测试 celgo 函数列表

	// 双向比对：多出的函数说明未登记版本，缺失的函数说明清单过期。
	unexpected := functionNames.Difference(knownFunctions)
	missing := knownFunctions.Difference(functionNames)

	if len(unexpected) != 0 {
		t.Errorf("Expected all functions in the libraries to be assigned to a kubernetes release, but found the unexpected function names: %v", unexpected)
	}
	if len(missing) != 0 {
		t.Errorf("Expected all functions in the libraries to be assigned to a kubernetes release, but found the missing function names: %v", missing)
	}
}

// TestTypeRegistration 确保 Kubernetes CEL 库定义并使用的所有自定义类型
// 都由 library.Types() 返回。其他测试依赖 Types() 提供最新的
// 库内声明类型列表。
func TestTypeRegistration(t *testing.T) {
	for _, lib := range KnownLibraries() {
		registeredTypes := sets.New[*cel.Type]()
		usedTypes := sets.New[*cel.Type]()
		// 扫描该库所有已注册的函数声明
		for _, fn := range lib.declarations() {
			// 构造临时函数声明只为拿到重载签名；名字本身不参与校验。
			fn, err := decls.NewFunction("placeholder-not-used", fn...)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range fn.OverloadDecls() {
				// ArgTypes 同时包含接收者类型（若有）与
				// 所有函数参数类型。
				for _, at := range o.ArgTypes() {
					switch at.Kind() {
					// 自定义类型为 Opaque 或 Struct。
					case types.OpaqueKind, types.StructKind:
						usedTypes.Insert(at)
					default:
						// 跳过
					}
				}
			}
		}
		for _, lb := range lib.Types() {
			registeredTypes.Insert(lb)
			// legacyTypeNames 是历史遗留的未加前缀类型，迁移完成后可删除白名单。
			if !strings.HasPrefix(lb.TypeName(), "kubernetes.") && !legacyTypeNames.Has(lb.TypeName()) {
				t.Errorf("Expected all types in kubernetes CEL libraries to have a type name packaged with a 'kubernetes.' prefix but got %v", lb.TypeName())
			}
		}
		// 函数签名中出现的自定义类型必须通过 Types() 暴露，否则编译期类型解析会失败。
		unregistered := usedTypes.Difference(registeredTypes)
		if len(unregistered) != 0 {
			t.Errorf("Expected types to be registered with the %s library Type() functions, but they were not: %v", lib.LibraryName(), unregistered)
		}
	}
}

// TODO: 若不打算把它们提升到 cel-go，考虑改名为 "kubernetes.net.IP" 与 "kubernetes.net.CIDR"
var legacyTypeNames = sets.New[string]("net.IP", "net.CIDR")
