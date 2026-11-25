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

package jsonmergepatch

import (
	"fmt"
	"reflect"
	"testing"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/utils/dump"
	"sigs.k8s.io/yaml"
)

// FilterNullTestCases 是空值处理测试夹具的 YAML 结构。
type FilterNullTestCases struct {
	TestCases []FilterNullTestCase
}

// FilterNullTestCase 描述一个保留或删除空值的场景，包含输入以及
// 保留、删除空值后各自的预期输出。
type FilterNullTestCase struct {
	Description         string
	OriginalObj         map[string]interface{}
	ExpectedWithNull    map[string]interface{}
	ExpectedWithoutNull map[string]interface{}
}

var filterNullTestCaseData = []byte(`
testCases:
  - description: nil original
    originalObj: {}
    expectedWithNull: {}
    expectedWithoutNull: {}
  - description: simple map
    originalObj:
      nilKey: null
      nonNilKey: foo
    expectedWithNull:
      nilKey: null
    expectedWithoutNull:
      nonNilKey: foo
  - description: simple map with all nil values
    originalObj:
      nilKey1: null
      nilKey2: null
    expectedWithNull:
      nilKey1: null
      nilKey2: null
    expectedWithoutNull: {}
  - description: simple map with all non-nil values
    originalObj:
      nonNilKey1: foo
      nonNilKey2: bar
    expectedWithNull: {}
    expectedWithoutNull:
      nonNilKey1: foo
      nonNilKey2: bar
  - description: nested map
    originalObj:
      mapKey:
        nilKey: null
        nonNilKey: foo
    expectedWithNull:
      mapKey:
        nilKey: null
    expectedWithoutNull:
      mapKey:
        nonNilKey: foo
  - description: nested map that all subkeys are nil
    originalObj:
      mapKey:
        nilKey1: null
        nilKey2: null
    expectedWithNull:
      mapKey:
        nilKey1: null
        nilKey2: null
    expectedWithoutNull: {}
  - description: nested map that all subkeys are non-nil
    originalObj:
      mapKey:
        nonNilKey1: foo
        nonNilKey2: bar
    expectedWithNull: {}
    expectedWithoutNull:
      mapKey:
        nonNilKey1: foo
        nonNilKey2: bar
  - description: explicitly empty map as value
    originalObj:
      mapKey: {}
    expectedWithNull: {}
    expectedWithoutNull:
      mapKey: {}
  - description: explicitly empty nested map
    originalObj:
      mapKey:
        nonNilKey: {}
    expectedWithNull: {}
    expectedWithoutNull:
      mapKey:
        nonNilKey: {}
  - description: multiple expliclty empty nested maps
    originalObj:
      mapKey:
        nonNilKey1: {}
        nonNilKey2: {}
    expectedWithNull: {}
    expectedWithoutNull:
      mapKey:
        nonNilKey1: {}
        nonNilKey2: {}
  - description: nested map with non-null value as empty map
    originalObj:
      mapKey:
        nonNilKey: {}
        nilKey: null
    expectedWithNull:
      mapKey:
        nilKey: null
    expectedWithoutNull:
      mapKey:
        nonNilKey: {}
  - description: empty list
    originalObj:
      listKey: []
    expectedWithNull: {}
    expectedWithoutNull:
      listKey: []
  - description: list of primitives
    originalObj:
      listKey:
      - 1
      - 2
    expectedWithNull: {}
    expectedWithoutNull:
      listKey:
      - 1
      - 2
  - description: list of maps
    originalObj:
      listKey:
      - k1: v1
      - k2: null
      - k3: v3
        k4: null
    expectedWithNull: {}
    expectedWithoutNull:
      listKey:
      - k1: v1
      - k2: null
      - k3: v3
        k4: null
  - description: list of different types
    originalObj:
      listKey:
      - k1: v1
      - k2: null
      - v3
    expectedWithNull: {}
    expectedWithoutNull:
      listKey:
      - k1: v1
      - k2: null
      - v3
`)

// TestKeepOrDeleteNullInObj 验证递归空值过滤："keep" 模式保留显式空值，
// "delete" 模式删除空值；空映射和空切片不会被误判为空值，
// 而会原样保留。
func TestKeepOrDeleteNullInObj(t *testing.T) {
	tc := FilterNullTestCases{}
	err := yaml.Unmarshal(filterNullTestCaseData, &tc)
	if err != nil {
		t.Fatalf("can't unmarshal test cases: %s\n", err)
	}

	for _, test := range tc.TestCases {
		resultWithNull, err := keepOrDeleteNullInObj(test.OriginalObj, true)
		if err != nil {
			t.Errorf("Failed in test case %q when trying to keep null values: %s", test.Description, err)
		}
		if !reflect.DeepEqual(test.ExpectedWithNull, resultWithNull) {
			t.Errorf("Failed in test case %q when trying to keep null values:\nexpected expectedWithNull:\n%+v\nbut got:\n%+v\n", test.Description, test.ExpectedWithNull, resultWithNull)
		}

		resultWithoutNull, err := keepOrDeleteNullInObj(test.OriginalObj, false)
		if err != nil {
			t.Errorf("Failed in test case %q when trying to keep non-null values: %s", test.Description, err)
		}
		if !reflect.DeepEqual(test.ExpectedWithoutNull, resultWithoutNull) {
			t.Errorf("Failed in test case %q when trying to keep non-null values:\n expected expectedWithoutNull:\n%+v\nbut got:\n%+v\n", test.Description, test.ExpectedWithoutNull, resultWithoutNull)
		}
	}
}

// JSONMergePatchTestCases 是三方合并补丁测试夹具的 YAML 结构。
type JSONMergePatchTestCases struct {
	TestCases []JSONMergePatchTestCase
}

// JSONMergePatchTestCase 描述一个具名的三方合并场景。
type JSONMergePatchTestCase struct {
	Description string
	JSONMergePatchTestCaseData
}

// JSONMergePatchTestCaseData 保存三方合并的四个输入或输出：
// 原始配置、修改后的配置、当前线上对象、
// 预期生成的补丁，以及应用补丁后的结果。
type JSONMergePatchTestCaseData struct {
	//Original 是原始对象（注解中的 last-applied 配置）。
	Original map[string]interface{}
	//Modified 是修改后的对象（目标新配置）。
	Modified map[string]interface{}
	//Current 是当前对象（服务器上的实际配置）。
	Current map[string]interface{}
	//ThreeWay 是预期的三方合并补丁。
	ThreeWay map[string]interface{}
	//Result 是在当前对象上应用三方合并补丁后的预期对象。
	Result map[string]interface{}
}

var createJSONMergePatchTestCaseData = []byte(`
testCases:
  - description: nil original
    modified:
      name: 1
      value: 1
    current:
      name: 1
      other: a
    threeWay:
      value: 1
    result:
      name: 1
      value: 1
      other: a
  - description: nil patch
    original:
      name: 1
    modified:
      name: 1
    current:
      name: 1
    threeWay:
      {}
    result:
      name: 1
  - description: add field to map
    original:
      name: 1
    modified:
      name: 1
      value: 1
    current:
      name: 1
      other: a
    threeWay:
      value: 1
    result:
      name: 1
      value: 1
      other: a
  - description: add field to map with conflict
    original:
      name: 1
    modified:
      name: 1
      value: 1
    current:
      name: a
      other: a
    threeWay:
      name: 1
      value: 1
    result:
      name: 1
      value: 1
      other: a
  - description: add field and delete field from map
    original:
      name: 1
    modified:
      value: 1
    current:
      name: 1
      other: a
    threeWay:
      name: null
      value: 1
    result:
      value: 1
      other: a
  - description: add field and delete field from map with conflict
    original:
      name: 1
    modified:
      value: 1
    current:
      name: a
      other: a
    threeWay:
      name: null
      value: 1
    result:
      value: 1
      other: a
  - description: delete field from nested map
    original:
      simpleMap:
        key1: 1
        key2: 1
    modified:
      simpleMap:
        key1: 1
    current:
      simpleMap:
        key1: 1
        key2: 1
        other: a
    threeWay:
      simpleMap:
        key2: null
    result:
      simpleMap:
        key1: 1
        other: a
  - description: delete field from nested map with conflict
    original:
      simpleMap:
        key1: 1
        key2: 1
    modified:
      simpleMap:
        key1: 1
    current:
      simpleMap:
        key1: a
        key2: 1
        other: a
    threeWay:
      simpleMap:
        key1: 1
        key2: null
    result:
      simpleMap:
        key1: 1
        other: a
  - description: delete all fields from map
    original:
      name: 1
      value: 1
    modified: {}
    current:
      name: 1
      value: 1
      other: a
    threeWay:
      name: null
      value: null
    result:
      other: a
  - description: delete all fields from map with conflict
    original:
      name: 1
      value: 1
    modified: {}
    current:
      name: 1
      value: a
      other: a
    threeWay:
      name: null
      value: null
    result:
      other: a
  - description: add field and delete all fields from map
    original:
      name: 1
      value: 1
    modified:
      other: a
    current:
      name: 1
      value: 1
      other: a
    threeWay:
      name: null
      value: null
    result:
      other: a
  - description: add field and delete all fields from map with conflict
    original:
      name: 1
      value: 1
    modified:
      other: a
    current:
      name: 1
      value: 1
      other: b
    threeWay:
      name: null
      value: null
      other: a
    result:
      other: a
  - description: replace list of scalars
    original:
      intList:
        - 1
        - 2
    modified:
      intList:
        - 2
        - 3
    current:
      intList:
        - 1
        - 2
    threeWay:
      intList:
        - 2
        - 3
    result:
      intList:
        - 2
        - 3
  - description: replace list of scalars with conflict
    original:
      intList:
        - 1
        - 2
    modified:
      intList:
        - 2
        - 3
    current:
      intList:
        - 1
        - 4
    threeWay:
      intList:
        - 2
        - 3
    result:
      intList:
        - 2
        - 3
  - description: patch with different scalar type
    original:
      foo: 1
    modified:
      foo: true
    current:
      foo: 1
      bar: 2
    threeWay:
      foo: true
    result:
      foo: true
      bar: 2
  - description: patch from scalar to list
    original:
      foo: 0
    modified:
      foo:
      - 1
      - 2
    current:
      foo: 0
      bar: 2
    threeWay:
      foo:
      - 1
      - 2
    result:
      foo:
      - 1
      - 2
      bar: 2
  - description: patch from list to scalar
    original:
      foo:
      - 1
      - 2
    modified:
      foo: 0
    current:
      foo:
      - 1
      - 2
      bar: 2
    threeWay:
      foo: 0
    result:
      foo: 0
      bar: 2
  - description: patch from scalar to map
    original:
      foo: 0
    modified:
      foo:
        baz: 1
    current:
      foo: 0
      bar: 2
    threeWay:
      foo:
        baz: 1
    result:
      foo:
        baz: 1
      bar: 2
  - description: patch from map to scalar
    original:
      foo:
        baz: 1
    modified:
      foo: 0
    current:
      foo:
        baz: 1
      bar: 2
    threeWay:
      foo: 0
    result:
      foo: 0
      bar: 2
  - description: patch from map to list
    original:
      foo:
        baz: 1
    modified:
      foo:
      - 1
      - 2
    current:
      foo:
        baz: 1
      bar: 2
    threeWay:
      foo:
      - 1
      - 2
    result:
      foo:
      - 1
      - 2
      bar: 2
  - description: patch from list to map
    original:
      foo:
      - 1
      - 2
    modified:
      foo:
        baz: 0
    current:
      foo:
      - 1
      - 2
      bar: 2
    threeWay:
      foo:
        baz: 0
    result:
      foo:
        baz: 0
      bar: 2
  - description: patch with different nested types
    original:
      foo:
      - a: true
      - 2
      - false
    modified:
      foo:
      - 1
      - false
      - b: 1
    current:
      foo:
      - a: true
      - 2
      - false
      bar: 0
    threeWay:
      foo:
      - 1
      - false
      - b: 1
    result:
      foo:
      - 1
      - false
      - b: 1
      bar: 0
  - description: patch array with nil
    original:
      foo:
      - a: true
      - null
      - false
      bar: []
      drop:
      - 1
    modified:
      foo:
      - 1
      - false
      - b: 1
      bar:
      - c
      - null
      - null
      - a
      drop:
      - null
    current:
      foo:
      - a: true
      - 2
      - false
      bar:
      - c
      - null
      - null
      - a
      drop:
    threeWay:
      foo:
      - 1
      - false
      - b: 1
      drop:
      - null
    result:
      foo:
      - 1
      - false
      - b: 1
      drop:
      - null
      bar:
      - c
      - null
      - null
      - a
`)

// TestCreateThreeWayJSONMergePatch 对每个夹具分别执行补丁生成和应用检查。
func TestCreateThreeWayJSONMergePatch(t *testing.T) {
	tc := JSONMergePatchTestCases{}
	err := yaml.Unmarshal(createJSONMergePatchTestCaseData, &tc)
	if err != nil {
		t.Errorf("can't unmarshal test cases: %s\n", err)
		return
	}

	for _, c := range tc.TestCases {
		testThreeWayPatch(t, c)
	}
}

// testThreeWayPatch 检查生成的补丁与夹具一致，
// 并检查应用到当前对象后得到预期结果。
func testThreeWayPatch(t *testing.T, c JSONMergePatchTestCase) {
	original, modified, current, expected, result := threeWayTestCaseToJSONOrFail(t, c)
	actual, err := CreateThreeWayJSONMergePatch(original, modified, current)
	if err != nil {
		t.Fatalf("error: %s", err)
	}
	testPatchCreation(t, expected, actual, c.Description)
	testPatchApplication(t, current, actual, result, c.Description)
}

// testPatchCreation 将生成的补丁与夹具比较，
// 并以 YAML 输出两者以便阅读差异。
func testPatchCreation(t *testing.T, expected, actual []byte, description string) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("error in test case: %s\nexpected patch:\n%s\ngot:\n%s\n",
			description, jsonToYAMLOrError(expected), jsonToYAMLOrError(actual))
		return
	}
}

// testPatchApplication 使用独立的 RFC 7386 实现应用补丁，
// 避免测试只是重复被测代码的行为。
func testPatchApplication(t *testing.T, original, patch, expected []byte, description string) {
	result, err := jsonpatch.MergePatch(original, patch)
	if err != nil {
		t.Errorf("error: %s\nin test case: %s\ncannot apply patch:\n%s\nto original:\n%s\n",
			err, description, jsonToYAMLOrError(patch), jsonToYAMLOrError(original))
		return
	}

	if !reflect.DeepEqual(result, expected) {
		format := "error in test case: %s\npatch application failed:\noriginal:\n%s\npatch:\n%s\nexpected:\n%s\ngot:\n%s\n"
		t.Errorf(format, description,
			jsonToYAMLOrError(original), jsonToYAMLOrError(patch),
			jsonToYAMLOrError(expected), jsonToYAMLOrError(result))
		return
	}
}

// threeWayTestCaseToJSONOrFail 序列化五个夹具映射，并将 nil 保留为 nil 字节切片，
// 从而区分“原始对象不存在”和 "{}"。
func threeWayTestCaseToJSONOrFail(t *testing.T, c JSONMergePatchTestCase) ([]byte, []byte, []byte, []byte, []byte) {
	return testObjectToJSONOrFail(t, c.Original),
		testObjectToJSONOrFail(t, c.Modified),
		testObjectToJSONOrFail(t, c.Current),
		testObjectToJSONOrFail(t, c.ThreeWay),
		testObjectToJSONOrFail(t, c.Result)
}

// testObjectToJSONOrFail 序列化夹具映射；原始对象不存在时返回 nil。
func testObjectToJSONOrFail(t *testing.T, o map[string]interface{}) []byte {
	if o == nil {
		return nil
	}
	j, err := toJSON(o)
	if err != nil {
		t.Error(err)
	}
	return j
}

// jsonToYAMLOrError 将 JSON 渲染为 YAML 用于失败信息；转换失败时内联返回错误，
// 使测试仍能提供有用的诊断。
func jsonToYAMLOrError(j []byte) string {
	y, err := jsonToYAML(j)
	if err != nil {
		return err.Error()
	}
	return string(y)
}

// toJSON 序列化夹具，并在错误中包含格式化输出，
// 便于诊断意外输入。
func toJSON(v interface{}) ([]byte, error) {
	j, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal failed: %v\n%v\n", err, dump.Pretty(v))
	}
	return j, nil
}

// jsonToYAML 将 JSON 转换为 YAML，失败时在错误中附带原始字节。
func jsonToYAML(j []byte) ([]byte, error) {
	y, err := yaml.JSONToYAML(j)
	if err != nil {
		return nil, fmt.Errorf("json to yaml failed: %v\n%v\n", err, j)
	}
	return y, nil
}
