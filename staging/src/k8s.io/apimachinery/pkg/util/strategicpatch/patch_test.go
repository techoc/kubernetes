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

// 本文件是 strategicpatch 包的测试。大部分用例以 YAML 或原始 JSON 形式内联在文件中，
// 并针对三种 schema 来源（Go 结构体标签、OpenAPI v2、OpenAPI v3）各跑一遍，
// 以保证不同 schema 提供者产出的 patch 语义完全一致。

package strategicpatch

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/mergepatch"
	"k8s.io/apimachinery/pkg/util/sets"
	sptest "k8s.io/apimachinery/pkg/util/strategicpatch/testing"
	"k8s.io/utils/dump"
)

// 三份测试用 schema：mergeItem 用于列表合并、retainKeys 等主要语义，
// precisionItem 专门用于数值精度相关的转换测试。v2 与 v3 各一份以覆盖两代 OpenAPI 实现。
var (
	fakeMergeItemSchema     = sptest.Fake{Path: filepath.Join("testdata", "swagger-merge-item.json")}
	fakePrecisionItemSchema = sptest.Fake{Path: filepath.Join("testdata", "swagger-precision-item.json")}

	fakeMergeItemV3Schema     = sptest.OpenAPIV3Getter{Path: filepath.Join("testdata", "swagger-merge-item-v3.json")}
	fakePrecisionItemV3Schema = sptest.OpenAPIV3Getter{Path: filepath.Join("testdata", "swagger-precision-item-v3.json")}
)

// SortMergeListTestCases 是 SortMergeList 测试用例的集合，对应 YAML 中的 testCases 字段。
type SortMergeListTestCases struct {
	TestCases []SortMergeListTestCase
}

// SortMergeListTestCase 描述一个排序用例：把 Original 中的合并列表按 schema 规定的键排序后
// 应与 Sorted 一致。
type SortMergeListTestCase struct {
	Description string
	Original    map[string]interface{}
	Sorted      map[string]interface{}
}

// StrategicMergePatchTestCases 是 StrategicMergePatch 测试用例的集合。
type StrategicMergePatchTestCases struct {
	TestCases []StrategicMergePatchTestCase
}

// StrategicMergePatchTestCase 是一条以结构化对象描述的 patch 用例。
type StrategicMergePatchTestCase struct {
	Description string
	StrategicMergePatchTestCaseData
}

// StrategicMergePatchRawTestCase 是一条以原始 YAML/JSON 字节描述的 patch 用例，
// 用于表达结构化对象无法精确表达的格式（如字段顺序、重复键、旧客户端格式）。
type StrategicMergePatchRawTestCase struct {
	Description string
	StrategicMergePatchRawTestCaseData
}

// StrategicMergePatchTestCaseData 保存一条 patch 用例的输入与期望输出。
// 三方合并场景下 Original/Modified/Current 分别对应 last-applied 注解、用户期望配置与服务端实时配置。
type StrategicMergePatchTestCaseData struct {
	// Original 是原始对象（注解中的 last-applied 配置）。
	Original map[string]interface{}
	// Modified 是修改后的对象（用户期望的新配置）。
	Modified map[string]interface{}
	// Current 是当前对象（服务端的实时配置）。
	Current map[string]interface{}
	// TwoWay 是 Original 与 Modified 之间期望得到的双向合并补丁。
	TwoWay map[string]interface{}
	// ThreeWay 是期望得到的三方合并补丁。
	ThreeWay map[string]interface{}
	// Result 是在 Current 上应用三方补丁后期望得到的对象。
	Result map[string]interface{}
	// TwoWayResult 是在 Current 上应用双向补丁后期望得到的对象；为 nil 时使用 Modified。
	TwoWayResult map[string]interface{}
}

// 各字段含义与 StrategicMergePatchTestCaseData 相同，区别在于全部字段都是 JSON 编码后的数据。
// 之所以需要原始形式，是因为某些用例依赖 YAML 的重复键、null 与字段顺序等结构化对象无法保留的信息。
type StrategicMergePatchRawTestCaseData struct {
	Original      []byte
	Modified      []byte
	Current       []byte
	TwoWay        []byte
	ThreeWay      []byte
	Result        []byte
	TwoWayResult  []byte
	ExpectedError string
}

// MergeItem 是测试 schema 的主体类型，覆盖了 strategic merge patch 支持的各种字段形态：
// 合并列表、非合并列表、标量列表、指针、map、替换型字段、retainKeys 以及嵌套合并列表。
type MergeItem struct {
	Name                  string               `json:"name,omitempty"`
	Value                 string               `json:"value,omitempty"`
	Other                 string               `json:"other,omitempty"`
	MergingList           []MergeItem          `json:"mergingList,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	NonMergingList        []MergeItem          `json:"nonMergingList,omitempty"`
	MergingIntList        []int                `json:"mergingIntList,omitempty" patchStrategy:"merge"`
	NonMergingIntList     []int                `json:"nonMergingIntList,omitempty"`
	MergeItemPtr          *MergeItem           `json:"mergeItemPtr,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	SimpleMap             map[string]string    `json:"simpleMap,omitempty"`
	ReplacingItem         runtime.RawExtension `json:"replacingItem,omitempty" patchStrategy:"replace"`
	JSONItem              struct{ Raw []byte } `json:"jsonItem,omitempty"`
	RetainKeysMap         RetainKeysMergeItem  `json:"retainKeysMap,omitempty" patchStrategy:"retainKeys"`
	RetainKeysMergingList []MergeItem          `json:"retainKeysMergingList,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name"`
}

// RetainKeysMergeItem 用于测试 retainKeys 策略：应用补丁时未出现在补丁中的字段会被清除。
type RetainKeysMergeItem struct {
	Name           string            `json:"name,omitempty"`
	Value          string            `json:"value,omitempty"`
	Other          string            `json:"other,omitempty"`
	SimpleMap      map[string]string `json:"simpleMap,omitempty"`
	MergingIntList []int             `json:"mergingIntList,omitempty" patchStrategy:"merge"`
	MergingList    []MergeItem       `json:"mergingList,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	NonMergingList []MergeItem       `json:"nonMergingList,omitempty"`
}

// mergeItem 是取类型用的零值；mergeItemStructSchema 是基于该类型标签构建的 schema，
// 与 OpenAPI schema 并列作为被测实现之一。
var (
	mergeItem             MergeItem
	mergeItemStructSchema = PatchMetaFromStruct{T: GetTagStructTypeOrDie(mergeItem)}
)

// 这些是 SortMergeList 的用例，用于断言它能（递归地）正确排序合并列表与非合并列表。
var sortMergeListTestCaseData = []byte(`
testCases:
  - description: sort one list of maps
    original:
      mergingList:
        - name: 1
        - name: 3
        - name: 2
    sorted:
      mergingList:
        - name: 1
        - name: 2
        - name: 3
  - description: sort lists of maps but not nested lists of maps
    original:
      mergingList:
        - name: 2
          nonMergingList:
            - name: 1
            - name: 3
            - name: 2
        - name: 1
          nonMergingList:
            - name: 2
            - name: 1
    sorted:
      mergingList:
        - name: 1
          nonMergingList:
            - name: 2
            - name: 1
        - name: 2
          nonMergingList:
            - name: 1
            - name: 3
            - name: 2
  - description: sort lists of maps and nested lists of maps
    original:
      mergingList:
        - name: 2
          mergingList:
            - name: 1
            - name: 3
            - name: 2
        - name: 1
          mergingList:
            - name: 2
            - name: 1
    sorted:
      mergingList:
        - name: 1
          mergingList:
            - name: 1
            - name: 2
        - name: 2
          mergingList:
            - name: 1
            - name: 2
            - name: 3
  - description: merging list should NOT sort when nested in non merging list
    original:
      nonMergingList:
        - name: 2
          mergingList:
            - name: 1
            - name: 3
            - name: 2
        - name: 1
          mergingList:
            - name: 2
            - name: 1
    sorted:
      nonMergingList:
        - name: 2
          mergingList:
            - name: 1
            - name: 3
            - name: 2
        - name: 1
          mergingList:
            - name: 2
            - name: 1
  - description: sort very nested list of maps
    fieldTypes:
    original:
      mergingList:
        - mergingList:
            - mergingList:
                - name: 2
                - name: 1
    sorted:
      mergingList:
        - mergingList:
            - mergingList:
                - name: 1
                - name: 2
  - description: sort nested lists of ints
    original:
      mergingList:
        - name: 2
          mergingIntList:
            - 1
            - 3
            - 2
        - name: 1
          mergingIntList:
            - 2
            - 1
    sorted:
      mergingList:
        - name: 1
          mergingIntList:
            - 1
            - 2
        - name: 2
          mergingIntList:
            - 1
            - 2
            - 3
  - description: sort nested pointers of ints
    original:
      mergeItemPtr:
        - name: 2
          mergingIntList:
            - 1
            - 3
            - 2
        - name: 1
          mergingIntList:
            - 2
            - 1
    sorted:
      mergeItemPtr:
        - name: 1
          mergingIntList:
            - 1
            - 2
        - name: 2
          mergingIntList:
            - 1
            - 2
            - 3
  - description: sort merging list by pointer
    original:
      mergeItemPtr:
        - name: 1
        - name: 3
        - name: 2
    sorted:
      mergeItemPtr:
        - name: 1
        - name: 2
        - name: 3
`)

// TestSortMergeLists 验证 sortJsonOrFail 能把对象中的合并列表按合并键排序。
//
// 同一组用例分别在三种 schema 上执行：结构体标签、OpenAPI v2 与 OpenAPI v3，
// 任何实现出现排序差异都会在此暴露。
func TestSortMergeLists(t *testing.T) {
	mergeItemOpenapiSchema := PatchMetaFromOpenAPI{
		Schema: sptest.GetSchemaOrDie(&fakeMergeItemSchema, "mergeItem"),
	}
	mergeItemOpenapiV3Schema := PatchMetaFromOpenAPIV3{
		SchemaList: fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas,
		Schema:     fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas["mergeItem"],
	}
	schemas := []LookupPatchMeta{
		mergeItemStructSchema,
		mergeItemOpenapiSchema,
		mergeItemOpenapiV3Schema,
	}

	tc := SortMergeListTestCases{}
	err := yaml.Unmarshal(sortMergeListTestCaseData, &tc)
	if err != nil {
		t.Errorf("can't unmarshal test cases: %s\n", err)
		return
	}

	for _, schema := range schemas {
		for _, c := range tc.TestCases {
			// 先把两侧都转成 JSON 并排序，再按字节比较，避免 map 遍历顺序带来的假阳性。
			temp := testObjectToJSONOrFail(t, c.Original)
			got := sortJsonOrFail(t, temp, c.Description, schema)
			expected := testObjectToJSONOrFail(t, c.Sorted)
			if !reflect.DeepEqual(got, expected) {
				t.Errorf("using %s error in test case: %s\ncannot sort object:\n%s\nexpected:\n%s\ngot:\n%s\n",
					getSchemaType(schema), c.Description, mergepatch.ToYAMLOrError(c.Original), mergepatch.ToYAMLOrError(c.Sorted), jsonToYAMLOrError(got))
			}
		}
	}
}

// 这些 StrategicMergePatch 用例无法由 CreateTwoWayMergePatch 生成，可能涉及：
// - 不使用 replace 指令。
// - 为合并列表补丁生成重复整数。
// - 生成空合并列表。
// - 使用旧客户端的补丁格式。
//
// 这些用例用于测试 CreateTwoWayMergePatch 无法生成的补丁，原因可能包括：
// 未使用 replace 指令、为合并列表生成重复整数、生成空的合并列表，
// 或使用旧客户端产生的补丁格式。它们只验证"应用补丁"这一侧的行为。
var customStrategicMergePatchTestCaseData = []byte(`
testCases:
  - description: unique scalars when merging lists
    original:
      mergingIntList:
        - 1
        - 2
    twoWay:
      mergingIntList:
        - 2
        - 3
    modified:
      mergingIntList:
        - 1
        - 2
        - 3
  - description: delete map from nested map
    original:
      simpleMap:
        key1: 1
        key2: 1
    twoWay:
      simpleMap:
        $patch: delete
    modified:
      simpleMap:
        {}
  - description: delete all items from merging list
    original:
      mergingList:
        - name: 1
        - name: 2
    twoWay:
      mergingList:
        - $patch: replace
    modified:
      mergingList: []
  - description: merge empty merging lists
    original:
      mergingList: []
    twoWay:
      mergingList: []
    modified:
      mergingList: []
  - description: delete all keys from map
    original:
      name: 1
      value: 1
    twoWay:
      $patch: replace
    modified: {}
  - description: add key and delete all keys from map
    original:
      name: 1
      value: 1
    twoWay:
      other: a
      $patch: replace
    modified:
      other: a
  - description: delete all duplicate entries in a merging list
    original:
      mergingList:
        - name: 1
        - name: 1
        - name: 2
          value: a
        - name: 3
        - name: 3
    twoWay:
      mergingList:
        - name: 1
          $patch: delete
        - name: 3
          $patch: delete
    modified:
      mergingList:
        - name: 2
          value: a
  - description: retainKeys map can add a field when no retainKeys directive present
    original:
      retainKeysMap:
        name: foo
    twoWay:
      retainKeysMap:
        value: bar
    modified:
      retainKeysMap:
        name: foo
        value: bar
  - description: retainKeys map can change a field when no retainKeys directive present
    original:
      retainKeysMap:
        name: foo
        value: a
    twoWay:
      retainKeysMap:
        value: b
    modified:
      retainKeysMap:
        name: foo
        value: b
  - description: retainKeys map can delete a field when no retainKeys directive present
    original:
      retainKeysMap:
        name: foo
        value: a
    twoWay:
      retainKeysMap:
        value: null
    modified:
      retainKeysMap:
        name: foo
  - description: retainKeys map merge an empty map
    original:
      retainKeysMap:
        name: foo
        value: a
    twoWay:
      retainKeysMap: {}
    modified:
      retainKeysMap:
        name: foo
        value: a
  - description: retainKeys list can add a field when no retainKeys directive present
    original:
      retainKeysMergingList:
      - name: bar
      - name: foo
    twoWay:
      retainKeysMergingList:
      - name: foo
        value: a
    modified:
      retainKeysMergingList:
      - name: bar
      - name: foo
        value: a
  - description: retainKeys list can change a field when no retainKeys directive present
    original:
      retainKeysMergingList:
      - name: bar
      - name: foo
        value: a
    twoWay:
      retainKeysMergingList:
      - name: foo
        value: b
    modified:
      retainKeysMergingList:
      - name: bar
      - name: foo
        value: b
  - description: retainKeys list can delete a field when no retainKeys directive present
    original:
      retainKeysMergingList:
      - name: bar
      - name: foo
        value: a
    twoWay:
      retainKeysMergingList:
      - name: foo
        value: null
    modified:
      retainKeysMergingList:
      - name: bar
      - name: foo
  - description: preserve the order from the patch in a merging list
    original:
      mergingList:
        - name: 1
        - name: 2
          value: b
        - name: 3
    twoWay:
      mergingList:
        - name: 3
          value: c
        - name: 1
          value: a
        - name: 2
          other: x
    modified:
      mergingList:
        - name: 3
          value: c
        - name: 1
          value: a
        - name: 2
          value: b
          other: x
  - description: preserve the order from the patch in a merging list 2
    original:
      mergingList:
        - name: 1
        - name: 2
          value: b
        - name: 3
    twoWay:
      mergingList:
        - name: 3
          value: c
        - name: 1
          value: a
    modified:
      mergingList:
        - name: 2
          value: b
        - name: 3
          value: c
        - name: 1
          value: a
  - description: preserve the order from the patch in a merging int list
    original:
      mergingIntList:
        - 1
        - 2
        - 3
    twoWay:
      mergingIntList:
        - 3
        - 1
        - 2
    modified:
      mergingIntList:
        - 3
        - 1
        - 2
  - description: preserve the order from the patch in a merging int list
    original:
      mergingIntList:
        - 1
        - 2
        - 3
    twoWay:
      mergingIntList:
        - 3
        - 1
    modified:
      mergingIntList:
        - 2
        - 3
        - 1
`)

// customStrategicMergePatchRawTestCases 是手工构造的原始补丁用例，
// 用于覆盖 $setElementOrder 等旧版或非标准格式的输入。
var customStrategicMergePatchRawTestCases = []StrategicMergePatchRawTestCase{
	{
		Description: "$setElementOrder contains item that is not present in the list to be merged",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 3
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 3
  - name: 2
  - name: 1
mergingList:
  - name: 3
    value: 3
  - name: 1
    value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 3
    value: 3
  - name: 1
    value: 1
`),
		},
	},
	{
		Description: "$setElementOrder contains item that is not present in the int list to be merged",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 3
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 3
  - 2
  - 1
`),
			Modified: []byte(`
mergingIntList:
  - 3
  - 1
`),
		},
	},
	{
		Description: "should check if order in $setElementOrder and patch list match",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 3
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
  - name: 3
mergingList:
  - name: 3
    value: 3
  - name: 1
    value: 1
`),
			ExpectedError: "doesn't match",
		},
	},
	{
		Description: "$setElementOrder contains item that is not present in the int list to be merged",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 3
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
  - 3
mergingIntList:
  - 3
  - 1
`),
			ExpectedError: "doesn't match",
		},
	},
	{
		Description: "missing merge key should error out",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: a
`),
			TwoWay: []byte(`
mergingList:
  - value: b
`),
			ExpectedError: "does not contain declared merge key",
		},
	},
	{
		Description: "$deleteFromPrimitiveList of nonexistent item in primitive list should not add the item to the list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
`),
			TwoWay: []byte(`
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 1
  - 2
`),
		},
	},
	{
		Description: "$deleteFromPrimitiveList on empty primitive list should not add the item to the list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
`),
			TwoWay: []byte(`
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
`),
		},
	},
	{
		Description: "$deleteFromPrimitiveList on nonexistent primitive list should not add the primitive list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
foo:
  - bar
`),
			TwoWay: []byte(`
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Modified: []byte(`
foo:
  - bar
`),
		},
	},
	{
		Description: "$deleteFromPrimitiveList should delete item from a list with merge patch strategy",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
			TwoWay: []byte(`
$deleteFromPrimitiveList/mergingIntList:
  - 2
`),
			Modified: []byte(`
mergingIntList:
  - 1
  - 3
`),
		},
	},
	{
		Description: "$deleteFromPrimitiveList should delete item from a list without merge patch strategy",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
nonMergingIntList:
  - 1
  - 2
  - 3
`),
			TwoWay: []byte(`
$deleteFromPrimitiveList/nonMergingIntList:
  - 2
`),
			Modified: []byte(`
nonMergingIntList:
  - 1
  - 3
`),
		},
	},
}

// TestCustomStrategicMergePatch 验证手工构造的补丁能被正确应用。
//
// 与 TestStrategicMergePatch 不同，这里不检查补丁的"生成"过程，
// 只断言给定的补丁在给定原始对象上应用后的结果，因此可以覆盖生成器不会产出的格式。
func TestCustomStrategicMergePatch(t *testing.T) {
	mergeItemOpenapiSchema := PatchMetaFromOpenAPI{
		Schema: sptest.GetSchemaOrDie(&fakeMergeItemSchema, "mergeItem"),
	}
	mergeItemOpenapiV3Schema := PatchMetaFromOpenAPIV3{
		SchemaList: fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas,
		Schema:     fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas["mergeItem"],
	}
	schemas := []LookupPatchMeta{
		mergeItemStructSchema,
		mergeItemOpenapiSchema,
		mergeItemOpenapiV3Schema,
	}

	tc := StrategicMergePatchTestCases{}
	err := yaml.Unmarshal(customStrategicMergePatchTestCaseData, &tc)
	if err != nil {
		t.Errorf("can't unmarshal test cases: %v\n", err)
		return
	}

	for _, c := range tc.TestCases {
		t.Run(c.Description, func(t *testing.T) {
			for _, schema := range schemas {
				t.Run(schema.Name(), func(t *testing.T) {
					original, expectedTwoWayPatch, _, expectedResult := twoWayTestCaseToJSONOrFail(t, c, schema)
					testPatchApplication(t, original, expectedTwoWayPatch, expectedResult, c.Description, "", schema)
				})

				// 原始 YAML 用例也放在这里执行，覆盖结构化用例无法表达的格式细节。
				for _, c := range customStrategicMergePatchRawTestCases {
					original, expectedTwoWayPatch, _, expectedResult := twoWayRawTestCaseToJSONOrFail(t, c)
					testPatchApplication(t, original, expectedTwoWayPatch, expectedResult, c.Description, c.ExpectedError, schema)
				}
			}
		})
	}
}

// 这些是 StrategicMergePatch 的用例，断言应用补丁能得到正确结果；
// 同时也是 CreateTwoWayMergePatch 与 CreateThreeWayMergePatch 的用例，
// 断言二者能为给定输入生成正确的补丁。
var createStrategicMergePatchTestCaseData = []byte(`
testCases:
  - description: nil original
    twoWay:
      name: 1
      value: 1
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
    twoWay:
      {}
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
    twoWay:
      value: 1
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
    twoWay:
      value: 1
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
    twoWay:
      name: null
      value: 1
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
    twoWay:
      name: null
      value: 1
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
    twoWay:
      simpleMap:
        key2: null
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
    twoWay:
      simpleMap:
        key2: null
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
    twoWay:
      name: null
      value: null
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
    twoWay:
      name: null
      value: null
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
    twoWay:
      name: null
      value: null
      other: a
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
    twoWay:
      name: null
      value: null
      other: a
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
      nonMergingIntList:
        - 1
        - 2
    twoWay:
      nonMergingIntList:
        - 2
        - 3
    modified:
      nonMergingIntList:
        - 2
        - 3
    current:
      nonMergingIntList:
        - 1
        - 2
    threeWay:
      nonMergingIntList:
        - 2
        - 3
    result:
      nonMergingIntList:
        - 2
        - 3
  - description: replace list of scalars with conflict
    original:
      nonMergingIntList:
        - 1
        - 2
    twoWay:
      nonMergingIntList:
        - 2
        - 3
    modified:
      nonMergingIntList:
        - 2
        - 3
    current:
      nonMergingIntList:
        - 1
        - 4
    threeWay:
      nonMergingIntList:
        - 2
        - 3
    result:
      nonMergingIntList:
        - 2
        - 3
  - description: delete all maps from merging list
    original:
      mergingList:
        - name: 1
        - name: 2
    twoWay:
      mergingList:
        - name: 1
          $patch: delete
        - name: 2
          $patch: delete
    modified:
      mergingList: []
    current:
      mergingList:
        - name: 1
        - name: 2
    threeWay:
      mergingList:
        - name: 1
          $patch: delete
        - name: 2
          $patch: delete
    result:
      mergingList: []
  - description: delete all maps from merging list with conflict
    original:
      mergingList:
        - name: 1
        - name: 2
    twoWay:
      mergingList:
        - name: 1
          $patch: delete
        - name: 2
          $patch: delete
    modified:
      mergingList: []
    current:
      mergingList:
        - name: 1
          other: a
        - name: 2
          other: b
    threeWay:
      mergingList:
        - name: 1
          $patch: delete
        - name: 2
          $patch: delete
    result:
      mergingList: []
  - description: delete all maps from empty merging list
    original:
      mergingList:
        - name: 1
        - name: 2
    twoWay:
      mergingList:
        - name: 1
          $patch: delete
        - name: 2
          $patch: delete
    modified:
      mergingList: []
    current:
      mergingList: []
    threeWay:
      mergingList:
        - name: 1
          $patch: delete
        - name: 2
          $patch: delete
    result:
      mergingList: []
  - description: merge empty merging lists
    original:
      mergingList: []
    twoWay:
      {}
    modified:
      mergingList: []
    current:
      mergingList: []
    threeWay:
      {}
    result:
      mergingList: []
  - description: defined null values should propagate overwrite current fields (with conflict)
    original:
      name: 2
    twoWay:
      name: 1
      value: 1
      other: null
    twoWayResult:
      name: 1
      value: 1
    modified:
      name: 1
      value: 1
      other: null
    current:
      name: a
      other: a
    threeWay:
      name: 1
      value: 1
      other: null
    result:
      name: 1
      value: 1
  - description: defined null values should propagate removing original fields
    original:
      name: original-name
      value: original-value
    current:
      name: original-name
      value: original-value
      other: current-other
    modified:
      name: modified-name
      value: null
    twoWay:
      name: modified-name
      value: null
    twoWayResult:
      name: modified-name
    threeWay:
      name: modified-name
      value: null
    result:
      name: modified-name
      other: current-other
  - description: nil patch with retainKeys map
    original:
      name: a
      retainKeysMap:
        name: foo
    current:
      name: a
      value: b
      retainKeysMap:
        name: foo
    modified:
      name: a
      retainKeysMap:
        name: foo
    twoWay: {}
    threeWay: {}
    result:
      name: a
      value: b
      retainKeysMap:
        name: foo
  - description: retainKeys map with no change should not be present
    original:
      name: a
      retainKeysMap:
        name: foo
    current:
      name: a
      other: c
      retainKeysMap:
        name: foo
    modified:
      name: a
      value: b
      retainKeysMap:
        name: foo
    twoWay:
      value: b
    threeWay:
      value: b
    result:
      name: a
      value: b
      other: c
      retainKeysMap:
        name: foo
`)

// strategicMergePatchRawTestCases 是主测试使用的大批量原始用例，
// 覆盖列表删除、$patch 指令、标量列表合并、retainKeys 等场景。
var strategicMergePatchRawTestCases = []StrategicMergePatchRawTestCase{
	{
		Description: "nested patch merge with empty list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
name: hi
`),
			Current: []byte(`
name: hi
mergingList:
- name: hello2
`),
			Modified: []byte(`
name: hi
mergingList:
- name: hello
- $patch: delete
  name: doesntexist
`),
			TwoWay: []byte(`
mergingList:
- name: hello
- $patch: delete
  name: doesntexist
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
- name: hello
- name: doesntexist
mergingList:
- name: hello
`),
			TwoWayResult: []byte(`
name: hi
mergingList:
- name: hello
`),
			Result: []byte(`
name: hi
mergingList:
- name: hello
- name: hello2
`),
		},
	},
	{
		Description: "delete items in lists of scalars",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 1
  - 2
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
  - 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 1
  - 2
  - 4
`),
		},
	},
	{
		Description: "delete all duplicate items in lists of scalars",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
  - 3
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 1
  - 2
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
  - 3
  - 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 1
  - 2
  - 4
`),
		},
	},
	{
		Description: "add and delete items in lists of scalars",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
  - 4
$deleteFromPrimitiveList/mergingIntList:
  - 3
mergingIntList:
  - 4
`),
			Modified: []byte(`
mergingIntList:
  - 1
  - 2
  - 4
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
  - 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
  - 4
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 1
  - 2
  - 4
`),
		},
	},
	{
		Description: "merge lists of maps",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 1
  - name: 2
  - name: 3
mergingList:
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
			Modified: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 1
  - name: 2
    value: 2
  - name: 3
    value: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 1
  - name: 2
  - name: 3
mergingList:
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
			Result: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
  - name: 3
    value: 3
`),
		},
	},
	{
		Description: "merge lists of maps with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
  - name: 3
mergingList:
  - name: 3
    value: 3
`),
			Modified: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
  - name: 3
    value: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 3
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
  - name: 3
mergingList:
  - name: 2
    value: 2
  - name: 3
    value: 3
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
  - name: 3
    value: 3
`),
		},
	},
	{
		Description: "add field to map in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "add field to map in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "add field to map in merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 3
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			Result: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
  - name: 3
    value: 2
    other: b
`),
		},
	},
	{
		Description: "add duplicate field to map in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`{}`),
			Result: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "add an item that already exists in current object in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: a
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
  - name: 3
mergingList:
  - name: 3
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: a
  - name: 2
  - name: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
    value: a
    other: x
  - name: 2
  - name: 3
`),
			ThreeWay: []byte(`{}`),
			Result: []byte(`
mergingList:
  - name: 1
    value: a
    other: x
  - name: 2
  - name: 3
`),
		},
	},
	{
		Description: "add duplicate field to map in merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 3
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 2
    value: 2
`),
			Result: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "replace map field value in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: a
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: a
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: a
`),
			Result: []byte(`
mergingList:
  - name: 1
    value: a
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "replace map field value in merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: a
`),
			Modified: []byte(`
mergingList:
  - name: 1
    value: a
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    value: 3
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: a
`),
			Result: []byte(`
mergingList:
  - name: 1
    value: a
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "delete map from merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 1
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 1
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "delete map from merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 1
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 1
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "delete missing map from merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 1
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 1
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "delete missing map from merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 1
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 3
    other: a
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
mergingList:
  - name: 2
  - name: 1
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 2
  - name: 3
    other: a
`),
		},
	},
	{
		Description: "add map and delete map from merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 3
mergingList:
  - name: 3
  - name: 1
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 2
  - name: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
  - name: 2
    other: b
  - name: 4
    other: c
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 3
mergingList:
  - name: 3
  - name: 1
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 2
    other: b
  - name: 4
    other: c
  - name: 3
`),
		},
	},
	{
		Description: "add map and delete map from merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 3
mergingList:
  - name: 3
  - name: 1
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 2
  - name: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 4
    other: c
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 3
mergingList:
  - name: 2
  - name: 3
  - name: 1
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 4
    other: c
  - name: 2
  - name: 3
`),
		},
	},
	{
		Description: "delete field from map in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
`),
			Modified: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    value: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "delete field from map in merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
`),
			Modified: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    value: a
    other: a
  - name: 2
    value: 2
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
`),
		},
	},
	{
		Description: "delete missing field from map in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
`),
			Modified: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "delete missing field from map in merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    value: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
`),
			Modified: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    value: null
  - name: 2
    value: 2
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "replace non merging list nested in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
    nonMergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
    nonMergingList:
      - name: 1
        value: 1
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "replace non merging list nested in merging list with value conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
    nonMergingList:
      - name: 1
        value: c
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
    nonMergingList:
      - name: 1
        value: 1
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "replace non merging list nested in merging list with deletion conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
    nonMergingList:
      - name: 2
        value: 2
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 1
    nonMergingList:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
    nonMergingList:
      - name: 1
        value: 1
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "add field to map in merging list nested in merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 1
      - name: 2
    name: 1
    mergingList:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 1
        value: 1
      - name: 2
        value: 2
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 1
      - name: 2
    name: 1
    mergingList:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 1
        value: 1
      - name: 2
        value: 2
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "add field to map in merging list nested in merging list with value conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 1
      - name: 2
    name: 1
    mergingList:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 1
        value: 1
      - name: 2
        value: 2
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 1
        value: a
        other: c
      - name: 2
        value: b
        other: d
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 1
      - name: 2
    name: 1
    mergingList:
      - name: 1
        value: 1
      - name: 2
        value: 2
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 1
        value: 1
        other: c
      - name: 2
        value: 2
        other: d
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "add field to map in merging list nested in merging list with deletion conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 1
      - name: 2
    name: 1
    mergingList:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 1
        value: 1
      - name: 2
        value: 2
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 2
        value: 2
        other: d
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 1
      - name: 2
    name: 1
    mergingList:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 1
        value: 1
      - name: 2
        value: 2
        other: d
  - name: 2
    other: b
`),
		},
	},

	{
		Description: "add field to map in merging list nested in merging list with deletion conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 2
      - name: 1
    name: 1
    mergingList:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergingList:
  - name: 1
    mergingList:
      - name: 2
        value: 2
      - name: 1
        value: 1
  - name: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 2
        value: 2
        other: d
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - $setElementOrder/mergingList:
      - name: 2
      - name: 1
    name: 1
    mergingList:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
    mergingList:
      - name: 2
        value: 2
        other: d
      - name: 1
        value: 1
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "add map to merging list by pointer",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergeItemPtr:
  - name: 1
`),
			TwoWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - name: 2
`),
			Modified: []byte(`
mergeItemPtr:
  - name: 1
  - name: 2
`),
			Current: []byte(`
mergeItemPtr:
  - name: 1
    other: a
  - name: 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - name: 2
`),
			Result: []byte(`
mergeItemPtr:
  - name: 1
    other: a
  - name: 2
  - name: 3
`),
		},
	},
	{
		Description: "add map to merging list by pointer with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergeItemPtr:
  - name: 1
`),
			TwoWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - name: 2
`),
			Modified: []byte(`
mergeItemPtr:
  - name: 1
  - name: 2
`),
			Current: []byte(`
mergeItemPtr:
  - name: 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - name: 1
  - name: 2
`),
			Result: []byte(`
mergeItemPtr:
  - name: 1
  - name: 2
  - name: 3
`),
		},
	},
	{
		Description: "add field to map in merging list by pointer",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergeItemPtr:
  - name: 1
    mergeItemPtr:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - $setElementOrder/mergeItemPtr:
      - name: 1
      - name: 2
    name: 1
    mergeItemPtr:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergeItemPtr:
  - name: 1
    mergeItemPtr:
      - name: 1
        value: 1
      - name: 2
        value: 2
  - name: 2
`),
			Current: []byte(`
mergeItemPtr:
  - name: 1
    other: a
    mergeItemPtr:
      - name: 1
        other: a
      - name: 2
        value: 2
        other: b
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - $setElementOrder/mergeItemPtr:
      - name: 1
      - name: 2
    name: 1
    mergeItemPtr:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergeItemPtr:
  - name: 1
    other: a
    mergeItemPtr:
      - name: 1
        value: 1
        other: a
      - name: 2
        value: 2
        other: b
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "add field to map in merging list by pointer with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergeItemPtr:
  - name: 1
    mergeItemPtr:
      - name: 1
      - name: 2
        value: 2
  - name: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - $setElementOrder/mergeItemPtr:
      - name: 1
      - name: 2
    name: 1
    mergeItemPtr:
      - name: 1
        value: 1
`),
			Modified: []byte(`
mergeItemPtr:
  - name: 1
    mergeItemPtr:
      - name: 1
        value: 1
      - name: 2
        value: 2
  - name: 2
`),
			Current: []byte(`
mergeItemPtr:
  - name: 1
    other: a
    mergeItemPtr:
      - name: 1
        value: a
      - name: 2
        value: 2
        other: b
  - name: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergeItemPtr:
  - name: 1
  - name: 2
mergeItemPtr:
  - $setElementOrder/mergeItemPtr:
      - name: 1
      - name: 2
    name: 1
    mergeItemPtr:
      - name: 1
        value: 1
`),
			Result: []byte(`
mergeItemPtr:
  - name: 1
    other: a
    mergeItemPtr:
      - name: 1
        value: 1
      - name: 2
        value: 2
        other: b
  - name: 2
    other: b
`),
		},
	},
	{
		Description: "merge lists of scalars",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
- 1
- 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
- 1
- 2
- 3
mergingIntList:
- 3
`),
			Modified: []byte(`
mergingIntList:
- 1
- 2
- 3
`),
			Current: []byte(`
mergingIntList:
- 1
- 2
- 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
- 1
- 2
- 3
mergingIntList:
- 3
`),
			Result: []byte(`
mergingIntList:
- 1
- 2
- 3
- 4
`),
		},
	},
	{
		Description: "add duplicate field to map in merging int list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 2
  - 3
mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
			ThreeWay: []byte(`{}`),
			Result: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
		},
	},
	//setElementOrder 测试用例
	{
		Description: "add an item in a list of primitives and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
- 1
- 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
- 3
- 1
- 2
mergingIntList:
- 3
`),
			Modified: []byte(`
mergingIntList:
- 3
- 1
- 2
`),
			Current: []byte(`
mergingIntList:
- 1
- 4
- 2
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
- 3
- 1
- 2
mergingIntList:
- 3
`),
			Result: []byte(`
mergingIntList:
- 3
- 1
- 4
- 2
`),
		},
	},
	{
		Description: "delete an item in a list of primitives and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
- 1
- 2
- 3
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
- 2
- 1
$deleteFromPrimitiveList/mergingIntList:
- 3
`),
			Modified: []byte(`
mergingIntList:
- 2
- 1
`),
			Current: []byte(`
mergingIntList:
- 1
- 2
- 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
- 2
- 1
$deleteFromPrimitiveList/mergingIntList:
- 3
`),
			Result: []byte(`
mergingIntList:
- 2
- 1
`),
		},
	},
	{
		Description: "add an item in a list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 3
  - name: 1
  - name: 2
mergingList:
  - name: 3
    value: 3
`),
			Modified: []byte(`
mergingList:
  - name: 3
    value: 3
  - name: 1
  - name: 2
    value: 2
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 3
  - name: 1
  - name: 2
mergingList:
  - name: 3
    value: 3
`),
			Result: []byte(`
mergingList:
  - name: 3
    value: 3
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
		},
	},
	{
		Description: "add multiple items in a list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 4
  - name: 2
  - name: 3
mergingList:
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
			Modified: []byte(`
mergingList:
  - name: 1
  - name: 4
    value: 4
  - name: 2
    value: 2
  - name: 3
    value: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 4
  - name: 2
  - name: 3
mergingList:
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
			Result: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 4
    value: 4
  - name: 2
    value: 2
    other: b
  - name: 3
    value: 3
`),
		},
	},
	{
		Description: "delete an item in a list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 3
    value: 3
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 1
mergingList:
  - name: 3
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 2
    value: 2
  - name: 1
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
  - name: 3
    value: 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 1
mergingList:
  - name: 3
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 2
    value: 2
    other: b
  - name: 1
    other: a
`),
		},
	},
	{
		Description: "change an item in a list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 3
    value: 3
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 3
  - name: 1
mergingList:
  - name: 3
    value: x
`),
			Modified: []byte(`
mergingList:
  - name: 2
    value: 2
  - name: 3
    value: x
  - name: 1
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
  - name: 3
    value: 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 3
  - name: 1
mergingList:
  - name: 3
    value: x
`),
			Result: []byte(`
mergingList:
  - name: 2
    value: 2
    other: b
  - name: 3
    value: x
  - name: 1
    other: a
`),
		},
	},
	{
		Description: "add and delete an item in a list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 3
    value: 3
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 2
  - name: 1
mergingList:
  - name: 4
    value: 4
  - name: 3
    $patch: delete
`),
			Modified: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 2
    value: 2
  - name: 1
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
    other: b
  - name: 3
    value: 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 2
  - name: 1
mergingList:
  - name: 4
    value: 4
  - name: 3
    $patch: delete
`),
			Result: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 2
    value: 2
    other: b
  - name: 1
    other: a
`),
		},
	},
	{
		Description: "set elements order in a list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 3
    value: 3
  - name: 4
    value: 4
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 2
  - name: 3
  - name: 1
`),
			Modified: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 2
    value: 2
  - name: 3
    value: 3
  - name: 1
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 3
    value: 3
  - name: 4
    value: 4
  - name: 2
    value: 2
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 2
  - name: 3
  - name: 1
`),
			Result: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 2
    value: 2
  - name: 3
    value: 3
  - name: 1
    other: a
`),
		},
	},
	{
		Description: "set elements order in a list with server-only items",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 3
    value: 3
  - name: 4
    value: 4
  - name: 2
    value: 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 2
  - name: 3
  - name: 1
`),
			Modified: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 2
    value: 2
  - name: 3
    value: 3
  - name: 1
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 3
    value: 3
  - name: 4
    value: 4
  - name: 2
    value: 2
  - name: 9
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 4
  - name: 2
  - name: 3
  - name: 1
`),
			Result: []byte(`
mergingList:
  - name: 4
    value: 4
  - name: 2
    value: 2
  - name: 3
    value: 3
  - name: 1
    other: a
  - name: 9
`),
		},
	},
	{
		Description: "set elements order in a list with server-only items 2",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
  - name: 3
    value: 3
  - name: 4
    value: 4
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 1
  - name: 4
  - name: 3
`),
			Modified: []byte(`
mergingList:
  - name: 2
    value: 2
  - name: 1
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
  - name: 9
  - name: 3
    value: 3
  - name: 4
    value: 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 1
  - name: 4
  - name: 3
`),
			Result: []byte(`
mergingList:
  - name: 2
    value: 2
  - name: 1
    other: a
  - name: 9
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
		},
	},
	{
		Description: "set elements order in a list with server-only items 3",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
  - name: 1
  - name: 2
    value: 2
  - name: 3
    value: 3
  - name: 4
    value: 4
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 1
  - name: 4
  - name: 3
`),
			Modified: []byte(`
mergingList:
  - name: 2
    value: 2
  - name: 1
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
			Current: []byte(`
mergingList:
  - name: 1
    other: a
  - name: 2
    value: 2
  - name: 7
  - name: 9
  - name: 8
  - name: 3
    value: 3
  - name: 4
    value: 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 2
  - name: 1
  - name: 4
  - name: 3
`),
			Result: []byte(`
mergingList:
  - name: 2
    value: 2
  - name: 1
    other: a
  - name: 7
  - name: 9
  - name: 8
  - name: 4
    value: 4
  - name: 3
    value: 3
`),
		},
	},
	{
		Description: "add an item in a int list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 3
  - 1
  - 2
mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 3
  - 1
  - 2
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 3
  - 1
  - 2
mergingIntList:
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 3
  - 1
  - 2
`),
		},
	},
	{
		Description: "add multiple items in a int list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 4
  - 2
  - 3
mergingIntList:
  - 4
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 1
  - 4
  - 2
  - 3
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 1
  - 4
  - 2
  - 3
mergingIntList:
  - 4
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 1
  - 4
  - 2
  - 3
`),
		},
	},
	{
		Description: "delete an item in a int list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 3
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 2
  - 1
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 2
  - 1
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 2
  - 1
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 2
  - 1
`),
		},
	},
	{
		Description: "add and delete an item in a int list and preserve order",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 3
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 4
  - 2
  - 1
mergingIntList:
  - 4
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 4
  - 2
  - 1
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 4
  - 2
  - 1
mergingIntList:
  - 4
$deleteFromPrimitiveList/mergingIntList:
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 4
  - 2
  - 1
`),
		},
	},
	{
		Description: "set elements order in a int list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 3
  - 4
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 4
  - 2
  - 3
  - 1
`),
			Modified: []byte(`
mergingIntList:
  - 4
  - 2
  - 3
  - 1
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 3
  - 4
  - 2
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 4
  - 2
  - 3
  - 1
`),
			Result: []byte(`
mergingIntList:
  - 4
  - 2
  - 3
  - 1
`),
		},
	},
	{
		Description: "set elements order in a int list with server-only items",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 3
  - 4
  - 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 4
  - 2
  - 3
  - 1
`),
			Modified: []byte(`
mergingIntList:
  - 4
  - 2
  - 3
  - 1
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 3
  - 4
  - 2
  - 9
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 4
  - 2
  - 3
  - 1
`),
			Result: []byte(`
mergingIntList:
  - 4
  - 2
  - 3
  - 1
  - 9
`),
		},
	},
	{
		Description: "set elements order in a int list with server-only items 2",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
  - 4
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 2
  - 1
  - 4
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 2
  - 1
  - 4
  - 3
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 9
  - 3
  - 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 2
  - 1
  - 4
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 2
  - 1
  - 9
  - 4
  - 3
`),
		},
	},
	{
		Description: "set elements order in a int list with server-only items 3",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
  - 1
  - 2
  - 3
  - 4
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
  - 2
  - 1
  - 4
  - 3
`),
			Modified: []byte(`
mergingIntList:
  - 2
  - 1
  - 4
  - 3
`),
			Current: []byte(`
mergingIntList:
  - 1
  - 2
  - 7
  - 9
  - 8
  - 3
  - 4
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
  - 2
  - 1
  - 4
  - 3
`),
			Result: []byte(`
mergingIntList:
  - 2
  - 1
  - 7
  - 9
  - 8
  - 4
  - 3
`),
		},
	},
	{
		//此用例仅演示列表含重复
		Description: "behavior of set element order for a merging list with duplicate",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
- name: 1
- name: 2
  value: dup1
- name: 3
- name: 2
  value: dup2
- name: 4
`),
			Current: []byte(`
mergingList:
- name: 1
- name: 2
  value: dup1
- name: 3
- name: 2
  value: dup2
- name: 4
`),
			Modified: []byte(`
mergingList:
- name: 2
  value: dup1
- name: 1
- name: 4
- name: 3
- name: 2
  value: dup2
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
- name: 2
- name: 1
- name: 4
- name: 3
- name: 2
`),
			TwoWayResult: []byte(`
mergingList:
- name: 2
  value: dup1
- name: 2
  value: dup2
- name: 1
- name: 4
- name: 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
- name: 2
- name: 1
- name: 4
- name: 3
- name: 2
`),
			Result: []byte(`
mergingList:
- name: 2
  value: dup1
- name: 2
  value: dup2
- name: 1
- name: 4
- name: 3
`),
		},
	},
	{
		//此用例仅演示列表含重复
		Description: "behavior of set element order for a merging int list with duplicate",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingIntList:
- 1
- 2
- 3
- 2
- 4
`),
			Current: []byte(`
mergingIntList:
- 1
- 2
- 3
- 2
- 4
`),
			Modified: []byte(`
mergingIntList:
- 2
- 1
- 4
- 3
- 2
`),
			TwoWay: []byte(`
$setElementOrder/mergingIntList:
- 2
- 1
- 4
- 3
- 2
`),
			TwoWayResult: []byte(`
mergingIntList:
- 2
- 2
- 1
- 4
- 3
`),
			ThreeWay: []byte(`
$setElementOrder/mergingIntList:
- 2
- 1
- 4
- 3
- 2
`),
			Result: []byte(`
mergingIntList:
- 2
- 2
- 1
- 4
- 3
`),
		},
	},
	{
		Description: "retainKeys map should clear defaulted field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`{}`),
			Current: []byte(`
retainKeysMap:
  value: foo
`),
			Modified: []byte(`
retainKeysMap:
  other: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  other: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - other
  other: bar
`),
			Result: []byte(`
retainKeysMap:
  other: bar
`),
		},
	},
	{
		Description: "retainKeys map should clear defaulted field with conflict (discriminated union)",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`{}`),
			Current: []byte(`
retainKeysMap:
  name: type1
  value: foo
`),
			Modified: []byte(`
retainKeysMap:
  name: type2
  other: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  name: type2
  other: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - other
  name: type2
  other: bar
`),
			Result: []byte(`
retainKeysMap:
  name: type2
  other: bar
`),
		},
	},
	{
		Description: "retainKeys map adds a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
`),
			Current: []byte(`
retainKeysMap:
  name: foo
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
		},
	},
	{
		Description: "retainKeys map adds a field and clear a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  other: a
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
		},
	},
	{
		Description: "retainKeys map deletes a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
  value: null
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
  value: null
`),
			Result: []byte(`
retainKeysMap:
  name: foo
`),
		},
	},
	{
		Description: "retainKeys map deletes a field and clears a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
  other: a
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
  value: null
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
  value: null
`),
			Result: []byte(`
retainKeysMap:
  name: foo
`),
		},
	},
	{
		Description: "retainKeys map clears a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
  other: a
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			TwoWay: []byte(`{}`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
		},
	},
	{
		Description: "retainKeys map nested map with no change",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  simpleMap:
    key1: a
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  simpleMap:
    key1: a
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  value: bar
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
`),
		},
	},
	{
		Description: "retainKeys map adds a field in a nested map",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key3: c
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key2: b
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key2: b
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key2: b
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key2: b
    key3: c
`),
		},
	},
	{
		Description: "retainKeys map deletes a field in a nested map",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key2: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key2: b
    key3: c
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key2: null
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key2: null
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key3: c
`),
		},
	},
	{
		Description: "retainKeys map changes a field in a nested map",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key2: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: a
    key2: b
    key3: c
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: x
    key2: b
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key1: x
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key1: x
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: x
    key2: b
    key3: c
`),
		},
	},
	{
		Description: "retainKeys map changes a field in a nested map with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: old
    key2: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: new
    key2: b
    key3: c
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: modified
    key2: b
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key1: modified
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - simpleMap
    - value
  simpleMap:
    key1: modified
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  simpleMap:
    key1: modified
    key2: b
    key3: c
`),
		},
	},
	{
		Description: "retainKeys map replaces non-merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: b
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: c
  - name: b
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - nonMergingList
    - value
  nonMergingList:
  - name: a
  - name: c
  - name: b
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - nonMergingList
    - value
  nonMergingList:
  - name: a
  - name: c
  - name: b
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: c
  - name: b
`),
		},
	},
	{
		Description: "retainKeys map nested non-merging list with no change",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: b
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - nonMergingList
    - value
  value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - nonMergingList
    - value
  value: bar
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: b
`),
		},
	},
	{
		Description: "retainKeys map nested non-merging list with no change with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
  - name: c
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: b
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - nonMergingList
    - value
  value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - nonMergingList
    - value
  value: bar
  nonMergingList:
  - name: a
  - name: b
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  nonMergingList:
  - name: a
  - name: b
`),
		},
	},
	{
		Description: "retainKeys map deletes nested non-merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
  nonMergingList: null
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
  nonMergingList: null
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
		},
	},
	{
		Description: "retainKeys map delete nested non-merging list with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  nonMergingList:
  - name: a
  - name: b
  - name: c
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
  nonMergingList: null
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
  nonMergingList: null
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
		},
	},
	{
		Description: "retainKeys map nested merging int list with no change",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  mergingIntList:
  - 1
  - 2
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingIntList
    - name
    - value
  value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingIntList
    - name
    - value
  $setElementOrder/mergingIntList:
    - 1
    - 2
  value: bar
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  mergingIntList:
  - 1
  - 2
  - 3
`),
		},
	},
	{
		Description: "retainKeys map adds an item in nested merging int list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 4
`),
			TwoWay: []byte(`
retainKeysMap:
  $setElementOrder/mergingIntList:
    - 1
    - 2
    - 4
  $retainKeys:
    - mergingIntList
    - name
  mergingIntList:
  - 4
`),
			ThreeWay: []byte(`
retainKeysMap:
  $setElementOrder/mergingIntList:
    - 1
    - 2
    - 4
  $retainKeys:
    - mergingIntList
    - name
  mergingIntList:
  - 4
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 4
  - 3
`),
		},
	},
	{
		Description: "retainKeys map deletes an item in nested merging int list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
  - 4
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 3
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingIntList
    - name
  $deleteFromPrimitiveList/mergingIntList:
  - 2
  $setElementOrder/mergingIntList:
    - 1
    - 3
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingIntList
    - name
  $deleteFromPrimitiveList/mergingIntList:
  - 2
  $setElementOrder/mergingIntList:
    - 1
    - 3
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 3
  - 4
`),
		},
	},
	{
		Description: "retainKeys map adds an item and deletes an item in nested merging int list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
  - 4
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 3
  - 5
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingIntList
    - name
  mergingIntList:
  - 5
  $deleteFromPrimitiveList/mergingIntList:
  - 2
  $setElementOrder/mergingIntList:
    - 1
    - 3
    - 5
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingIntList
    - name
  mergingIntList:
  - 5
  $deleteFromPrimitiveList/mergingIntList:
  - 2
  $setElementOrder/mergingIntList:
    - 1
    - 3
    - 5
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 3
  - 5
  - 4
`),
		},
	},
	{
		Description: "retainKeys map deletes nested merging int list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingIntList:
  - 1
  - 2
  - 3
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
  mergingIntList: null
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
  mergingIntList: null
`),
			Result: []byte(`
retainKeysMap:
  name: foo
`),
		},
	},
	{
		Description: "retainKeys map nested merging list with no change",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
  - name: c
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
  mergingList:
  - name: a
  - name: b
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
    - value
  value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
    - value
  $setElementOrder/mergingList:
    - name: a
    - name: b
  value: bar
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
  mergingList:
  - name: a
  - name: b
  - name: c
`),
		},
	},
	{
		Description: "retainKeys map adds an item in nested merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
  - name: x
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
  - name: c
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
  $setElementOrder/mergingList:
    - name: a
    - name: b
    - name: c
  mergingList:
  - name: c
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
  $setElementOrder/mergingList:
    - name: a
    - name: b
    - name: c
  mergingList:
  - name: c
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
  - name: c
  - name: x
`),
		},
	},
	{
		Description: "retainKeys map changes an item in nested merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
    value: foo
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
    value: foo
  - name: x
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
    value: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
  $setElementOrder/mergingList:
    - name: a
    - name: b
  mergingList:
  - name: b
    value: bar
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
  $setElementOrder/mergingList:
    - name: a
    - name: b
  mergingList:
  - name: b
    value: bar
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
    value: bar
  - name: x
`),
		},
	},
	{
		Description: "retainKeys map deletes nested merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
  mergingList: null
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - name
    - value
  value: bar
  mergingList: null
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  value: bar
`),
		},
	},
	{
		Description: "retainKeys map deletes an item in nested merging list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
`),
			Current: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: b
  - name: x
`),
			Modified: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
`),
			TwoWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
  $setElementOrder/mergingList:
    - name: a
  mergingList:
  - name: b
    $patch: delete
`),
			ThreeWay: []byte(`
retainKeysMap:
  $retainKeys:
    - mergingList
    - name
  $setElementOrder/mergingList:
    - name: a
  mergingList:
  - name: b
    $patch: delete
`),
			Result: []byte(`
retainKeysMap:
  name: foo
  mergingList:
  - name: a
  - name: x
`),
		},
	},
	{
		Description: "retainKeys list of maps clears a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
  other: x
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
			TwoWay: []byte(`{}`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
		},
	},
	{
		Description: "retainKeys list of maps clears a field with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: old
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: new
  other: x
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: modified
`),
			TwoWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: modified
`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: modified
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: modified
`),
		},
	},
	{
		Description: "retainKeys list of maps changes a field and clear a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: old
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: old
  other: x
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: new
`),
			TwoWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: new
`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: new
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: new
`),
		},
	},
	{
		Description: "retainKeys list of maps changes a field and clear a field with conflict",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: old
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: modified
  other: x
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: new
`),
			TwoWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: new
`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: new
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: new
`),
		},
	},
	{
		Description: "retainKeys list of maps adds a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
			TwoWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: a
`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: a
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
		},
	},
	{
		Description: "retainKeys list of maps adds a field and clear a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  other: x
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
			TwoWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: a
`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
    - value
  name: foo
  value: a
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
		},
	},
	{
		Description: "retainKeys list of maps deletes a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
`),
			TwoWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
  name: foo
  value: null
`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
  name: foo
  value: null
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
`),
		},
	},
	{
		Description: "retainKeys list of maps deletes a field and clear a field",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
`),
			Current: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
  value: a
  other: x
`),
			Modified: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
`),
			TwoWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
  name: foo
  value: null
`),
			ThreeWay: []byte(`
$setElementOrder/retainKeysMergingList:
  - name: bar
  - name: foo
retainKeysMergingList:
- $retainKeys:
    - name
  name: foo
  value: null
`),
			Result: []byte(`
retainKeysMergingList:
- name: bar
- name: foo
`),
		},
	},
	{
		Description: "delete and reorder in one list, reorder in another",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
mergingList:
- name: a
  value: a
- name: b
  value: b
mergeItemPtr:
- name: c
  value: c
- name: d
  value: d
`),
			Current: []byte(`
mergingList:
- name: a
  value: a
- name: b
  value: b
mergeItemPtr:
- name: c
  value: c
- name: d
  value: d
`),
			Modified: []byte(`
mergingList:
- name: b
  value: b
mergeItemPtr:
- name: d
  value: d
- name: c
  value: c
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
- name: b
$setElementOrder/mergeItemPtr:
- name: d
- name: c
mergingList:
- $patch: delete
  name: a
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
- name: b
$setElementOrder/mergeItemPtr:
- name: d
- name: c
mergingList:
- $patch: delete
  name: a
`),
			Result: []byte(`
mergingList:
- name: b
  value: b
mergeItemPtr:
- name: d
  value: d
- name: c
  value: c
`),
		},
	},
}

// TestStrategicMergePatch 是主测试入口，覆盖双向与三方补丁的生成与应用。
//
// 先校验若干非法输入的报错，再对结构化用例与原始用例各跑一遍；
// 每个用例都在三种 schema 上执行，确保不同 schema 来源的行为一致。
func TestStrategicMergePatch(t *testing.T) {
	// 结构体本身非法时应在构建 schema 阶段就报错，而不是等到应用补丁时。
	testStrategicMergePatchWithCustomArgumentsUsingStruct(t, "bad struct",
		"{}", "{}", []byte("<THIS IS NOT A STRUCT>"), mergepatch.ErrBadArgKind(struct{}{}, []byte{}))

	mergeItemOpenapiSchema := PatchMetaFromOpenAPI{
		Schema: sptest.GetSchemaOrDie(&fakeMergeItemSchema, "mergeItem"),
	}
	mergeItemOpenapiV3Schema := PatchMetaFromOpenAPIV3{
		SchemaList: fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas,
		Schema:     fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas["mergeItem"],
	}
	schemas := []LookupPatchMeta{
		mergeItemStructSchema,
		mergeItemOpenapiSchema,
		mergeItemOpenapiV3Schema,
	}

	tc := StrategicMergePatchTestCases{}
	err := yaml.Unmarshal(createStrategicMergePatchTestCaseData, &tc)
	if err != nil {
		t.Errorf("can't unmarshal test cases: %s\n", err)
		return
	}

	for _, schema := range schemas {
		t.Run(schema.Name(), func(t *testing.T) {

			// 非法 JSON 与 nil schema 都应返回稳定的哨兵错误，便于上层识别。
			testStrategicMergePatchWithCustomArguments(t, "bad original",
				"<THIS IS NOT JSON>", "{}", schema, mergepatch.ErrBadJSONDoc)
			testStrategicMergePatchWithCustomArguments(t, "bad patch",
				"{}", "<THIS IS NOT JSON>", schema, mergepatch.ErrBadJSONDoc)
			testStrategicMergePatchWithCustomArguments(t, "nil struct",
				"{}", "{}", nil, mergepatch.ErrBadArgKind(struct{}{}, nil))

			for _, c := range tc.TestCases {
				t.Run(c.Description+"/TwoWay", func(t *testing.T) {
					testTwoWayPatch(t, c, schema)
				})
				t.Run(c.Description+"/ThreeWay", func(t *testing.T) {
					testThreeWayPatch(t, c, schema)
				})
			}
		})

		//多次运行以覆盖不同的映射遍历顺序。
		// 多次重复运行，以覆盖 map 遍历顺序不同的情况；补丁算法必须与遍历顺序无关。
		for i := 0; i < 10; i++ {
			for _, c := range strategicMergePatchRawTestCases {
				t.Run(c.Description+"/TwoWay", func(t *testing.T) {
					testTwoWayPatchForRawTestCase(t, c, schema)
				})
				t.Run(c.Description+"/ThreeWay", func(t *testing.T) {
					testThreeWayPatchForRawTestCase(t, c, schema)
				})
			}
		}
	}
}

// testStrategicMergePatchWithCustomArgumentsUsingStruct 先由结构体构建 schema；
// 若构建失败则直接比对错误，否则再用得到的 schema 执行补丁测试。
func testStrategicMergePatchWithCustomArgumentsUsingStruct(t *testing.T, description, original, patch string, dataStruct interface{}, expected error) {
	schema, actual := NewPatchMetaFromStruct(dataStruct)
	//actual 非 nil 时检查错误；错误匹配则返回。
	// 若实际返回了错误，检查其是否与预期一致后返回。
	if actual != nil {
		checkErrorsEqual(t, description, expected, actual, schema)
		return
	}
	testStrategicMergePatchWithCustomArguments(t, description, original, patch, schema, expected)
}

// testStrategicMergePatchWithCustomArguments 以给定 schema 应用补丁，并校验返回的错误。
func testStrategicMergePatchWithCustomArguments(t *testing.T, description, original, patch string, schema LookupPatchMeta, expected error) {
	_, actual := StrategicMergePatch([]byte(original), []byte(patch), schema)
	checkErrorsEqual(t, description, expected, actual, schema)
}

// checkErrorsEqual 比较预期错误与实际错误。
//
// 优先比较错误指针；若不同，再退化为比较错误文本。这样既能兼容哨兵错误的直接比较，
// 也能覆盖带上下文信息、无法预先生成同指针的错误。
func checkErrorsEqual(t *testing.T, description string, expected, actual error, schema LookupPatchMeta) {
	if actual != expected {
		if actual == nil {
			t.Errorf("using %s expected error: %s\ndid not occur in test case: %s", getSchemaType(schema), expected, description)
			return
		}

		if expected == nil || actual.Error() != expected.Error() {
			t.Errorf("using %s unexpected error: %s\noccurred in test case: %s", getSchemaType(schema), actual, description)
			return
		}
	}
}

// testTwoWayPatch 生成双向补丁，并分别校验补丁内容与在原始对象上的应用结果。
func testTwoWayPatch(t *testing.T, c StrategicMergePatchTestCase, schema LookupPatchMeta) {
	original, expectedPatch, modified, expectedResult := twoWayTestCaseToJSONOrFail(t, c, schema)

	actualPatch, err := CreateTwoWayMergePatchUsingLookupPatchMeta(original, modified, schema)
	if err != nil {
		t.Errorf("using %s error: %s\nin test case: %s\ncannot create two way patch: %s:\n%s\n",
			getSchemaType(schema), err, c.Description, original, mergepatch.ToYAMLOrError(c.StrategicMergePatchTestCaseData))
		return
	}

	testPatchCreation(t, expectedPatch, actualPatch, c.Description)
	testPatchApplication(t, original, actualPatch, expectedResult, c.Description, "", schema)
}

// testTwoWayPatchForRawTestCase 与 testTwoWayPatch 相同，但输入是原始 YAML 字节，
// 并额外校验用例声明的预期错误。
func testTwoWayPatchForRawTestCase(t *testing.T, c StrategicMergePatchRawTestCase, schema LookupPatchMeta) {
	original, expectedPatch, modified, expectedResult := twoWayRawTestCaseToJSONOrFail(t, c)

	actualPatch, err := CreateTwoWayMergePatchUsingLookupPatchMeta(original, modified, schema)
	if err != nil {
		t.Errorf("error: %s\nin test case: %s\ncannot create two way patch:\noriginal:%s\ntwoWay:%s\nmodified:%s\ncurrent:%s\nthreeWay:%s\nresult:%s\n",
			err, c.Description, c.Original, c.TwoWay, c.Modified, c.Current, c.ThreeWay, c.Result)
		return
	}

	testPatchCreation(t, expectedPatch, actualPatch, c.Description)
	testPatchApplication(t, original, actualPatch, expectedResult, c.Description, c.ExpectedError, schema)
}

// twoWayTestCaseToJSONOrFail 把用例各字段转为已排序的 JSON 字节。
//
// TwoWayResult 未设置时回退为 Modified：双向补丁的目标就是让对象变成 Modified。
func twoWayTestCaseToJSONOrFail(t *testing.T, c StrategicMergePatchTestCase, schema LookupPatchMeta) ([]byte, []byte, []byte, []byte) {
	expectedResult := c.TwoWayResult
	if expectedResult == nil {
		expectedResult = c.Modified
	}
	return sortJsonOrFail(t, testObjectToJSONOrFail(t, c.Original), c.Description, schema),
		sortJsonOrFail(t, testObjectToJSONOrFail(t, c.TwoWay), c.Description, schema),
		sortJsonOrFail(t, testObjectToJSONOrFail(t, c.Modified), c.Description, schema),
		sortJsonOrFail(t, testObjectToJSONOrFail(t, expectedResult), c.Description, schema)
}

// twoWayRawTestCaseToJSONOrFail 把原始 YAML 用例转为 JSON 字节；TwoWayResult 同样回退为 Modified。
func twoWayRawTestCaseToJSONOrFail(t *testing.T, c StrategicMergePatchRawTestCase) ([]byte, []byte, []byte, []byte) {
	expectedResult := c.TwoWayResult
	if expectedResult == nil {
		expectedResult = c.Modified
	}
	return yamlToJSONOrError(t, c.Original),
		yamlToJSONOrError(t, c.TwoWay),
		yamlToJSONOrError(t, c.Modified),
		yamlToJSONOrError(t, expectedResult)
}

// testThreeWayPatch 生成三方补丁并校验结果。
//
// 冲突是三方合并的正常分支：若用例描述中包含 "conflict"，则期望生成冲突错误；
// 冲突后若用例仍给出了 Result，则改用 force 模式重新生成，验证强制覆盖能成功。
func testThreeWayPatch(t *testing.T, c StrategicMergePatchTestCase, schema LookupPatchMeta) {
	original, modified, current, expected, result := threeWayTestCaseToJSONOrFail(t, c, schema)
	actual, err := CreateThreeWayMergePatch(original, modified, current, schema, false)
	if err != nil {
		// 非冲突类错误一律视为用例失败，避免把实现缺陷误当成预期冲突。
		if !mergepatch.IsConflict(err) {
			t.Errorf("using %s error: %s\nin test case: %s\ncannot create three way patch:\n%s\n",
				getSchemaType(schema), err, c.Description, mergepatch.ToYAMLOrError(c.StrategicMergePatchTestCaseData))
			return
		}

		// 用例描述未声明冲突，却出现冲突，说明合并逻辑判断有误。
		if !strings.Contains(c.Description, "conflict") {
			t.Errorf("using %s unexpected conflict: %s\nin test case: %s\ncannot create three way patch:\n%s\n",
				getSchemaType(schema), err, c.Description, mergepatch.ToYAMLOrError(c.StrategicMergePatchTestCaseData))
			return
		}

		if len(c.Result) > 0 {
			// force=true 时冲突应被调用方的意图覆盖，从而得到可用补丁。
			actual, err := CreateThreeWayMergePatch(original, modified, current, schema, true)
			if err != nil {
				t.Errorf("using %s error: %s\nin test case: %s\ncannot force three way patch application:\n%s\n",
					getSchemaType(schema), err, c.Description, mergepatch.ToYAMLOrError(c.StrategicMergePatchTestCaseData))
				return
			}

			testPatchCreation(t, expected, actual, c.Description)
			testPatchApplication(t, current, actual, result, c.Description, "", schema)
		}

		return
	}

	// 未报冲突，但用例期望冲突或没有给出结果，说明冲突检测漏报。
	if strings.Contains(c.Description, "conflict") || len(c.Result) < 1 {
		t.Errorf("using %s error in test case: %s\nexpected conflict did not occur:\n%s\n",
			getSchemaType(schema), c.Description, mergepatch.ToYAMLOrError(c.StrategicMergePatchTestCaseData))
		return
	}

	testPatchCreation(t, expected, actual, c.Description)
	testPatchApplication(t, current, actual, result, c.Description, "", schema)
}

// testThreeWayPatchForRawTestCase 与 testThreeWayPatch 相同，输入为原始 YAML 字节。
func testThreeWayPatchForRawTestCase(t *testing.T, c StrategicMergePatchRawTestCase, schema LookupPatchMeta) {
	original, modified, current, expected, result := threeWayRawTestCaseToJSONOrFail(t, c)
	actual, err := CreateThreeWayMergePatch(original, modified, current, schema, false)
	if err != nil {
		if !mergepatch.IsConflict(err) {
			t.Errorf("using %s error: %s\nin test case: %s\ncannot create three way patch:\noriginal:%s\ntwoWay:%s\nmodified:%s\ncurrent:%s\nthreeWay:%s\nresult:%s\n",
				getSchemaType(schema), err, c.Description, c.Original, c.TwoWay, c.Modified, c.Current, c.ThreeWay, c.Result)
			return
		}

		if !strings.Contains(c.Description, "conflict") {
			t.Errorf("using %s unexpected conflict: %s\nin test case: %s\ncannot create three way patch:\noriginal:%s\ntwoWay:%s\nmodified:%s\ncurrent:%s\nthreeWay:%s\nresult:%s\n",
				getSchemaType(schema), err, c.Description, c.Original, c.TwoWay, c.Modified, c.Current, c.ThreeWay, c.Result)
			return
		}

		if len(c.Result) > 0 {
			actual, err := CreateThreeWayMergePatch(original, modified, current, schema, true)
			if err != nil {
				t.Errorf("using %s error: %s\nin test case: %s\ncannot force three way patch application:\noriginal:%s\ntwoWay:%s\nmodified:%s\ncurrent:%s\nthreeWay:%s\nresult:%s\n",
					getSchemaType(schema), err, c.Description, c.Original, c.TwoWay, c.Modified, c.Current, c.ThreeWay, c.Result)
				return
			}

			testPatchCreation(t, expected, actual, c.Description)
			testPatchApplication(t, current, actual, result, c.Description, c.ExpectedError, schema)
		}

		return
	}

	if strings.Contains(c.Description, "conflict") || len(c.Result) < 1 {
		t.Errorf("using %s error: %s\nin test case: %s\nexpected conflict did not occur:\noriginal:%s\ntwoWay:%s\nmodified:%s\ncurrent:%s\nthreeWay:%s\nresult:%s\n",
			getSchemaType(schema), err, c.Description, c.Original, c.TwoWay, c.Modified, c.Current, c.ThreeWay, c.Result)
		return
	}

	testPatchCreation(t, expected, actual, c.Description)
	testPatchApplication(t, current, actual, result, c.Description, c.ExpectedError, schema)
}

// threeWayTestCaseToJSONOrFail 把三方用例的各字段转为已排序的 JSON 字节。
func threeWayTestCaseToJSONOrFail(t *testing.T, c StrategicMergePatchTestCase, schema LookupPatchMeta) ([]byte, []byte, []byte, []byte, []byte) {
	return sortJsonOrFail(t, testObjectToJSONOrFail(t, c.Original), c.Description, schema),
		sortJsonOrFail(t, testObjectToJSONOrFail(t, c.Modified), c.Description, schema),
		sortJsonOrFail(t, testObjectToJSONOrFail(t, c.Current), c.Description, schema),
		sortJsonOrFail(t, testObjectToJSONOrFail(t, c.ThreeWay), c.Description, schema),
		sortJsonOrFail(t, testObjectToJSONOrFail(t, c.Result), c.Description, schema)
}

// threeWayRawTestCaseToJSONOrFail 把原始 YAML 三方用例的各字段转为 JSON 字节。
func threeWayRawTestCaseToJSONOrFail(t *testing.T, c StrategicMergePatchRawTestCase) ([]byte, []byte, []byte, []byte, []byte) {
	return yamlToJSONOrError(t, c.Original),
		yamlToJSONOrError(t, c.Modified),
		yamlToJSONOrError(t, c.Current),
		yamlToJSONOrError(t, c.ThreeWay),
		yamlToJSONOrError(t, c.Result)
}

// testPatchCreation 断言生成的补丁与期望补丁一致；不一致时以 YAML 形式输出便于人工比对。
func testPatchCreation(t *testing.T, expected, actual []byte, description string) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("error in test case: %s\nexpected patch:\n%s\ngot:\n%s\n",
			description, jsonToYAMLOrError(expected), jsonToYAMLOrError(actual))
		return
	}
}

// testPatchApplication 在 original 上应用 patch，并按需校验错误或结果。
//
// expectedError 非空时只关心错误文本是否包含该片段，这允许错误消息附带额外上下文；
// expectedError 为空时则要求应用成功且结果与 expected 完全一致。
func testPatchApplication(t *testing.T, original, patch, expected []byte, description, expectedError string, schema LookupPatchMeta) {
	result, err := StrategicMergePatchUsingLookupPatchMeta(original, patch, schema)
	if len(expectedError) != 0 {
		if err != nil && strings.Contains(err.Error(), expectedError) {
			return
		}
		t.Errorf("using %s expected error should contain:\n%s\nin test case: %s\nbut got:\n%s\n", getSchemaType(schema), expectedError, description, err)
	}
	if err != nil {
		t.Errorf("using %s error: %s\nin test case: %s\ncannot apply patch:\n%s\nto original:\n%s\n",
			getSchemaType(schema), err, description, jsonToYAMLOrError(patch), jsonToYAMLOrError(original))
		return
	}

	if !reflect.DeepEqual(result, expected) {
		format := "using error in test case: %s\npatch application failed:\noriginal:\n%s\npatch:\n%s\nexpected:\n%s\ngot:\n%s\n"
		t.Errorf(format, description,
			jsonToYAMLOrError(original), jsonToYAMLOrError(patch),
			jsonToYAMLOrError(expected), jsonToYAMLOrError(result))
		return
	}
}

// testObjectToJSONOrFail 把对象转为 JSON；nil 对象返回 nil，以区分"空对象"与"缺失"。
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

// sortJsonOrFail 按 schema 中的合并键对 JSON 中的列表排序。
//
// 排序使比较与 map 遍历顺序无关，因此测试可以安全地多次运行以覆盖不同遍历顺序。
func sortJsonOrFail(t *testing.T, j []byte, description string, schema LookupPatchMeta) []byte {
	if j == nil {
		return nil
	}
	r, err := sortMergeListsByName(j, schema)
	if err != nil {
		t.Errorf("using %s error: %s\n in test case: %s\ncannot sort object:\n%s\n", getSchemaType(schema), err, description, j)
		return nil
	}

	return r
}

// getSchemaType 返回 schema 的具体类型名，用于错误消息中区分是哪种实现失败。
func getSchemaType(schema LookupPatchMeta) string {
	return reflect.TypeOf(schema).String()
}

// jsonToYAMLOrError 把 JSON 转为 YAML 用于展示；失败时返回错误文本，避免二次出错。
func jsonToYAMLOrError(j []byte) string {
	y, err := jsonToYAML(j)
	if err != nil {
		return err.Error()
	}

	return string(y)
}

// toJSON 序列化为 JSON，失败时附带格式化转储，便于定位不可序列化的值。
func toJSON(v interface{}) ([]byte, error) {
	j, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal failed: %v\n%v\n", err, dump.Pretty(v))
	}

	return j, nil
}

// jsonToYAML 把 JSON 字节转换为 YAML 字节。
func jsonToYAML(j []byte) ([]byte, error) {
	y, err := yaml.JSONToYAML(j)
	if err != nil {
		return nil, fmt.Errorf("json to yaml failed: %v\n%v\n", err, j)
	}

	return y, nil
}

// yamlToJSON 把 YAML 字节转换为 JSON 字节。
func yamlToJSON(y []byte) ([]byte, error) {
	j, err := yaml.YAMLToJSON(y)
	if err != nil {
		return nil, fmt.Errorf("yaml to json failed: %v\n%v\n", err, y)
	}

	return j, nil
}

// yamlToJSONOrError 是 yamlToJSON 的测试便捷版本：失败时记录错误并返回 nil。
func yamlToJSONOrError(t *testing.T, y []byte) []byte {
	j, err := yamlToJSON(y)
	if err != nil {
		t.Errorf("%v", err)
	}

	return j
}

// PrecisionItem 用于测试各类数值在 JSON 往返与补丁生成中的精度保持情况。
type PrecisionItem struct {
	Name    string  `json:"name,omitempty"`
	Int32   int32   `json:"int32,omitempty"`
	Int64   int64   `json:"int64,omitempty"`
	Float32 float32 `json:"float32,omitempty"`
	Float64 float64 `json:"float64,omitempty"`
}

var (
	precisionItem             PrecisionItem
	precisionItemStructSchema = PatchMetaFromStruct{T: GetTagStructTypeOrDie(precisionItem)}
)

// TestNumberConversion 验证数值在补丁生成与应用过程中不发生精度损失。
//
// 覆盖 int32/int64 的最大值以及 float32/float64 的最大值：这些值在 JSON 中
// 以浮点或科学计数法表示，若实现中把数字统一当作 float64 处理，就会丢失低位精度，
// 进而把"未变化"误判为"有变化"而生成多余补丁。
func TestNumberConversion(t *testing.T) {
	testcases := map[string]struct {
		Old            string
		New            string
		ExpectedPatch  string
		ExpectedResult string
	}{
		"empty": {
			Old:            `{}`,
			New:            `{}`,
			ExpectedPatch:  `{}`,
			ExpectedResult: `{}`,
		},
		"int32 medium": {
			Old:            `{"int32":1000000}`,
			New:            `{"int32":1000000,"name":"newname"}`,
			ExpectedPatch:  `{"name":"newname"}`,
			ExpectedResult: `{"int32":1000000,"name":"newname"}`,
		},
		"int32 max": {
			Old:            `{"int32":2147483647}`,
			New:            `{"int32":2147483647,"name":"newname"}`,
			ExpectedPatch:  `{"name":"newname"}`,
			ExpectedResult: `{"int32":2147483647,"name":"newname"}`,
		},
		"int64 medium": {
			Old:            `{"int64":1000000}`,
			New:            `{"int64":1000000,"name":"newname"}`,
			ExpectedPatch:  `{"name":"newname"}`,
			ExpectedResult: `{"int64":1000000,"name":"newname"}`,
		},
		"int64 max": {
			Old:            `{"int64":9223372036854775807}`,
			New:            `{"int64":9223372036854775807,"name":"newname"}`,
			ExpectedPatch:  `{"name":"newname"}`,
			ExpectedResult: `{"int64":9223372036854775807,"name":"newname"}`,
		},
		"float32 max": {
			Old:            `{"float32":3.4028234663852886e+38}`,
			New:            `{"float32":3.4028234663852886e+38,"name":"newname"}`,
			ExpectedPatch:  `{"name":"newname"}`,
			ExpectedResult: `{"float32":3.4028234663852886e+38,"name":"newname"}`,
		},
		"float64 max": {
			Old:            `{"float64":1.7976931348623157e+308}`,
			New:            `{"float64":1.7976931348623157e+308,"name":"newname"}`,
			ExpectedPatch:  `{"name":"newname"}`,
			ExpectedResult: `{"float64":1.7976931348623157e+308,"name":"newname"}`,
		},
	}

	precisionItemOpenapiSchema := PatchMetaFromOpenAPI{
		Schema: sptest.GetSchemaOrDie(&fakePrecisionItemSchema, "precisionItem"),
	}
	precisionItemOpenapiV3Schema := PatchMetaFromOpenAPIV3{
		SchemaList: fakePrecisionItemV3Schema.SchemaOrDie().Components.Schemas,
		Schema:     fakePrecisionItemV3Schema.SchemaOrDie().Components.Schemas["precisionItem"],
	}
	precisionItemSchemas := []LookupPatchMeta{
		precisionItemStructSchema,
		precisionItemOpenapiSchema,
		precisionItemOpenapiV3Schema,
	}

	for _, schema := range precisionItemSchemas {
		for k, tc := range testcases {
			patch, err := CreateTwoWayMergePatchUsingLookupPatchMeta([]byte(tc.Old), []byte(tc.New), schema)
			if err != nil {
				t.Errorf("using %s in testcase %s: unexpected error %v", getSchemaType(schema), k, err)
				continue
			}
			// 只有 name 变化时，补丁中不应出现任何数值字段，否则说明精度比较失效。
			if tc.ExpectedPatch != string(patch) {
				t.Errorf("using %s in testcase %s: expected %s, got %s", getSchemaType(schema), k, tc.ExpectedPatch, string(patch))
				continue
			}

			// 应用补丁后原数值必须原样保留。
			result, err := StrategicMergePatchUsingLookupPatchMeta([]byte(tc.Old), patch, schema)
			if err != nil {
				t.Errorf("using %s in testcase %s: unexpected error %v", getSchemaType(schema), k, err)
				continue
			}
			if tc.ExpectedResult != string(result) {
				t.Errorf("using %s in testcase %s: expected %s, got %s", getSchemaType(schema), k, tc.ExpectedResult, string(result))
				continue
			}
		}
	}
}

// replaceRawExtensionPatchTestCases 验证 patchStrategy:"replace" 字段的整体替换语义：
// 补丁中出现的 replace 字段会整体覆盖原值，其内部的 map 不做逐键合并。
var replaceRawExtensionPatchTestCases = []StrategicMergePatchRawTestCase{
	{
		Description: "replace RawExtension field, rest unchanched",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
name: my-object
value: some-value
other: current-other
replacingItem:
  Some: Generic
  Yaml: Inside
  The: RawExtension
  Field: Period
`),
			Current: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 2
  - name: 3
replacingItem:
  Some: Generic
  Yaml: Inside
  The: RawExtension
  Field: Period
`),
			Modified: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 2
  - name: 3
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			TwoWay: []byte(`
mergingList:
  - name: 1
  - name: 2
  - name: 3
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			TwoWayResult: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 2
  - name: 3
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			ThreeWay: []byte(`
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			Result: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 2
  - name: 3
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
		},
	},
	{
		Description: "replace RawExtension field and merge list",
		StrategicMergePatchRawTestCaseData: StrategicMergePatchRawTestCaseData{
			Original: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
replacingItem:
  Some: Generic
  Yaml: Inside
  The: RawExtension
  Field: Period
`),
			Current: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 3
replacingItem:
  Some: Generic
  Yaml: Inside
  The: RawExtension
  Field: Period
`),
			Modified: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 2
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			TwoWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 2
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			TwoWayResult: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 2
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			ThreeWay: []byte(`
$setElementOrder/mergingList:
  - name: 1
  - name: 2
mergingList:
  - name: 2
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
			Result: []byte(`
name: my-object
value: some-value
other: current-other
mergingList:
  - name: 1
  - name: 2
  - name: 3
replacingItem:
  Newly: Modified
  Yaml: Inside
  The: RawExtension
`),
		},
	},
}

// TestReplaceWithRawExtension 在三种 schema 上同时验证 replace 字段的双向与三方补丁。
func TestReplaceWithRawExtension(t *testing.T) {
	mergeItemOpenapiSchema := PatchMetaFromOpenAPI{
		Schema: sptest.GetSchemaOrDie(&fakeMergeItemSchema, "mergeItem"),
	}
	mergeItemOpenapiV3Schema := PatchMetaFromOpenAPIV3{
		SchemaList: fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas,
		Schema:     fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas["mergeItem"],
	}
	schemas := []LookupPatchMeta{
		mergeItemStructSchema,
		mergeItemOpenapiSchema,
		mergeItemOpenapiV3Schema,
	}

	for _, schema := range schemas {
		for _, c := range replaceRawExtensionPatchTestCases {
			testTwoWayPatchForRawTestCase(t, c, schema)
			testThreeWayPatchForRawTestCase(t, c, schema)
		}
	}
}

// TestUnknownField 覆盖 schema 中未声明字段（未知字段）的处理规则。
//
// 分为两类：
//   - 可以成功合并的用例：未知字段作为不透明 JSON 处理，其中的 null 会被过滤，
//     但已存在于原对象中的 null 会被保留；
//   - 无法成功合并的用例：未知字段的值发生变化时无法定位到 schema 字段，应返回错误。
//
// 这些规则关系到旧客户端写入新字段时的兼容性，因此单独成组测试。
func TestUnknownField(t *testing.T) {
	testcases := map[string]struct {
		Original string
		Current  string
		Modified string

		ExpectedTwoWay         string
		ExpectedTwoWayErr      string
		ExpectedTwoWayResult   string
		ExpectedThreeWay       string
		ExpectedThreeWayErr    string
		ExpectedThreeWayResult string
	}{
		//可以成功执行战略合并的用例
		"no diff": {
			Original: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Current:  `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Modified: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,

			ExpectedTwoWay:         `{}`,
			ExpectedTwoWayResult:   `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			ExpectedThreeWay:       `{}`,
			ExpectedThreeWayResult: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
		},
		"no diff even if modified null": {
			Original: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Current:  `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Modified: `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":{"key":null},"name":"foo","scalar":true}`,

			ExpectedTwoWay:         `{"complex_nullable":{"key":null}}`,
			ExpectedTwoWayResult:   `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":{},"name":"foo","scalar":true}`,
			ExpectedThreeWay:       `{"complex_nullable":{"key":null}}`,
			ExpectedThreeWayResult: `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":{},"name":"foo","scalar":true}`,
		},
		"discard nulls in nested and adds not nulls": {
			Original: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Current:  `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Modified: `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":{"key":{"keynotnull":"value","keynull":null}},"name":"foo","scalar":true}`,

			ExpectedTwoWay:         `{"complex_nullable":{"key":{"keynotnull":"value","keynull":null}}}`,
			ExpectedTwoWayResult:   `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":{"key":{"keynotnull":"value"}},"name":"foo","scalar":true}`,
			ExpectedThreeWay:       `{"complex_nullable":{"key":{"keynotnull":"value","keynull":null}}}`,
			ExpectedThreeWayResult: `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":{"key":{"keynotnull":"value"}},"name":"foo","scalar":true}`,
		},
		"discard if modified all nulls": {
			Original: `{}`,
			Current:  `{}`,
			Modified: `{"complex":{"nested":null}}`,

			ExpectedTwoWay:         `{"complex":{"nested":null}}`,
			ExpectedTwoWayResult:   `{"complex":{}}`,
			ExpectedThreeWay:       `{"complex":{"nested":null}}`,
			ExpectedThreeWayResult: `{"complex":{}}`,
		},
		"add only not nulls": {
			Original: `{}`,
			Current:  `{}`,
			Modified: `{"complex":{"nested":null,"nested2":"foo"}}`,

			ExpectedTwoWay:         `{"complex":{"nested":null,"nested2":"foo"}}`,
			ExpectedTwoWayResult:   `{"complex":{"nested2":"foo"}}`,
			ExpectedThreeWay:       `{"complex":{"nested":null,"nested2":"foo"}}`,
			ExpectedThreeWayResult: `{"complex":{"nested2":"foo"}}`,
		},
		"null values in original are preserved": {
			Original: `{"thing":null}`,
			Current:  `{"thing":null}`,
			Modified: `{"nested":{"value":5},"thing":null}`,

			ExpectedTwoWay:         `{"nested":{"value":5}}`,
			ExpectedTwoWayResult:   `{"nested":{"value":5},"thing":null}`,
			ExpectedThreeWay:       `{"nested":{"value":5}}`,
			ExpectedThreeWayResult: `{"nested":{"value":5},"thing":null}`,
		},
		"nested null values in original are preserved": {
			Original: `{"complex":{"key":null},"thing":null}`,
			Current:  `{"complex":{"key":null},"thing":null}`,
			Modified: `{"complex":{"key":null},"nested":{"value":5},"thing":null}`,

			ExpectedTwoWay:         `{"nested":{"value":5}}`,
			ExpectedTwoWayResult:   `{"complex":{"key":null},"nested":{"value":5},"thing":null}`,
			ExpectedThreeWay:       `{"nested":{"value":5}}`,
			ExpectedThreeWayResult: `{"complex":{"key":null},"nested":{"value":5},"thing":null}`,
		},
		"add empty slices": {
			Original: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Current:  `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Modified: `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":[],"name":"foo","scalar":true}`,

			ExpectedTwoWay:         `{"complex_nullable":[]}`,
			ExpectedTwoWayResult:   `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":[],"name":"foo","scalar":true}`,
			ExpectedThreeWay:       `{"complex_nullable":[]}`,
			ExpectedThreeWayResult: `{"array":[1,2,3],"complex":{"nested":true},"complex_nullable":[],"name":"foo","scalar":true}`,
		},
		"filter nulls from nested slices": {
			Original: `{}`,
			Current:  `{}`,
			Modified: `{"complex_nullable":[{"inner_one":{"key_one":"foo","key_two":null}}]}`,

			ExpectedTwoWay:         `{"complex_nullable":[{"inner_one":{"key_one":"foo","key_two":null}}]}`,
			ExpectedTwoWayResult:   `{"complex_nullable":[{"inner_one":{"key_one":"foo"}}]}`,
			ExpectedThreeWay:       `{"complex_nullable":[{"inner_one":{"key_one":"foo","key_two":null}}]}`,
			ExpectedThreeWayResult: `{"complex_nullable":[{"inner_one":{"key_one":"foo"}}]}`,
		},
		"filter if slice is all empty": {
			Original: `{}`,
			Current:  `{}`,
			Modified: `{"complex_nullable":[{"inner_one":{"key_one":null,"key_two":null}}]}`,

			ExpectedTwoWay:         `{"complex_nullable":[{"inner_one":{"key_one":null,"key_two":null}}]}`,
			ExpectedTwoWayResult:   `{"complex_nullable":[{"inner_one":{}}]}`,
			ExpectedThreeWay:       `{"complex_nullable":[{"inner_one":{"key_one":null,"key_two":null}}]}`,
			ExpectedThreeWayResult: `{"complex_nullable":[{"inner_one":{}}]}`,
		},
		"not filter nulls from non-associative slice": {
			Original: `{}`,
			Current:  `{}`,
			Modified: `{"complex_nullable":["key1",null,"key2"]}`,

			ExpectedTwoWay:         `{"complex_nullable":["key1",null,"key2"]}`,
			ExpectedTwoWayResult:   `{"complex_nullable":["key1",null,"key2"]}`,
			ExpectedThreeWay:       `{"complex_nullable":["key1",null,"key2"]}`,
			ExpectedThreeWayResult: `{"complex_nullable":["key1",null,"key2"]}`,
		},
		"added only": {
			Original: `{"name":"foo"}`,
			Current:  `{"name":"foo"}`,
			Modified: `{"name":"foo","scalar":true,"complex":{"nested":true},"array":[1,2,3]}`,

			ExpectedTwoWay:         `{"array":[1,2,3],"complex":{"nested":true},"scalar":true}`,
			ExpectedTwoWayResult:   `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			ExpectedThreeWay:       `{"array":[1,2,3],"complex":{"nested":true},"scalar":true}`,
			ExpectedThreeWayResult: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
		},
		"removed only": {
			Original: `{"name":"foo","scalar":true,"complex":{"nested":true}}`,
			Current:  `{"name":"foo","scalar":true,"complex":{"nested":true},"array":[1,2,3]}`,
			Modified: `{"name":"foo"}`,

			ExpectedTwoWay:         `{"complex":null,"scalar":null}`,
			ExpectedTwoWayResult:   `{"name":"foo"}`,
			ExpectedThreeWay:       `{"complex":null,"scalar":null}`,
			ExpectedThreeWayResult: `{"array":[1,2,3],"name":"foo"}`,
		},

		//无法成功执行战略合并的用例（预期报错）
		"diff": {
			Original: `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Current:  `{"array":[1,2,3],"complex":{"nested":true},"name":"foo","scalar":true}`,
			Modified: `{"array":[1,2,3],"complex":{"nested":false},"name":"foo","scalar":true}`,

			ExpectedTwoWayErr:   `unable to find api field`,
			ExpectedThreeWayErr: `unable to find api field`,
		},

		"json": {
			Original: `{"name":"foo","jsonItem":{"nested":{"nested2":{"nested3":{}}}}}`,
			Current:  `{"name":"foo","jsonItem":{"nested":{"nested2":{"nested3":{}}}}}`,
			Modified: `{"name":"foo","jsonItem":{"nested":{"nested2":{"nested3":{"a":"b"}}}}}`,

			ExpectedTwoWay:         `{"jsonItem":{"nested":{"nested2":{"nested3":{"a":"b"}}}}}`,
			ExpectedTwoWayResult:   `{"jsonItem":{"nested":{"nested2":{"nested3":{"a":"b"}}}},"name":"foo"}`,
			ExpectedThreeWay:       `{"jsonItem":{"nested":{"nested2":{"nested3":{"a":"b"}}}}}`,
			ExpectedThreeWayResult: `{"jsonItem":{"nested":{"nested2":{"nested3":{"a":"b"}}}},"name":"foo"}`,
			ExpectedTwoWayErr:      `unable to find api field`,
			ExpectedThreeWayErr:    `unable to find api field`,
		},
	}

	mergeItemOpenapiSchema := PatchMetaFromOpenAPI{
		Schema: sptest.GetSchemaOrDie(&fakeMergeItemSchema, "mergeItem"),
	}
	mergeItemOpenapiV3Schema := PatchMetaFromOpenAPIV3{
		SchemaList: fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas,
		Schema:     fakeMergeItemV3Schema.SchemaOrDie().Components.Schemas["mergeItem"],
	}
	schemas := []LookupPatchMeta{
		mergeItemStructSchema,
		mergeItemOpenapiSchema,
		mergeItemOpenapiV3Schema,
	}

	for _, k := range sets.StringKeySet(testcases).List() {
		t.Run(k, func(t *testing.T) {
			tc := testcases[k]
			for _, schema := range schemas {
				t.Run(schema.Name()+"/TwoWay", func(t *testing.T) {
					twoWay, err := CreateTwoWayMergePatchUsingLookupPatchMeta([]byte(tc.Original), []byte(tc.Modified), schema)
					if err != nil {
						if len(tc.ExpectedTwoWayErr) == 0 {
							t.Errorf("using %s in testcase %s: error making two-way patch: %v", getSchemaType(schema), k, err)
						}
						if !strings.Contains(err.Error(), tc.ExpectedTwoWayErr) {
							t.Errorf("using %s in testcase %s: expected error making two-way patch to contain '%s', got %s", getSchemaType(schema), k, tc.ExpectedTwoWayErr, err)
						}
						return
					}

					if string(twoWay) != tc.ExpectedTwoWay {
						t.Errorf("using %s in testcase %s: expected two-way patch:\n\t%s\ngot\n\t%s", getSchemaType(schema), k, string(tc.ExpectedTwoWay), string(twoWay))
						return
					}

					twoWayResult, err := StrategicMergePatchUsingLookupPatchMeta([]byte(tc.Original), twoWay, schema)
					if err != nil {
						t.Errorf("using %s in testcase %s: error applying two-way patch: %v", getSchemaType(schema), k, err)
						return
					}
					if string(twoWayResult) != tc.ExpectedTwoWayResult {
						t.Errorf("using %s in testcase %s: expected two-way result:\n\t%s\ngot\n\t%s", getSchemaType(schema), k, string(tc.ExpectedTwoWayResult), string(twoWayResult))
						return
					}
				})

				t.Run(schema.Name()+"/ThreeWay", func(t *testing.T) {
					threeWay, err := CreateThreeWayMergePatch([]byte(tc.Original), []byte(tc.Modified), []byte(tc.Current), schema, false)
					if err != nil {
						if len(tc.ExpectedThreeWayErr) == 0 {
							t.Errorf("using %s in testcase %s: error making three-way patch: %v", getSchemaType(schema), k, err)
						} else if !strings.Contains(err.Error(), tc.ExpectedThreeWayErr) {
							t.Errorf("using %s in testcase %s: expected error making three-way patch to contain '%s', got %s", getSchemaType(schema), k, tc.ExpectedThreeWayErr, err)
						}
						return
					}

					if string(threeWay) != tc.ExpectedThreeWay {
						t.Errorf("using %s in testcase %s: expected three-way patch:\n\t%s\ngot\n\t%s", getSchemaType(schema), k, string(tc.ExpectedThreeWay), string(threeWay))
						return
					}

					threeWayResult, err := StrategicMergePatchUsingLookupPatchMeta([]byte(tc.Current), threeWay, schema)
					if err != nil {
						t.Errorf("using %s in testcase %s: error applying three-way patch: %v", getSchemaType(schema), k, err)
						return
					} else if string(threeWayResult) != tc.ExpectedThreeWayResult {
						t.Errorf("using %s in testcase %s: expected three-way result:\n\t%s\ngot\n\t%s", getSchemaType(schema), k, string(tc.ExpectedThreeWayResult), string(threeWayResult))
						return
					}
				})
			}
		})
	}
}
