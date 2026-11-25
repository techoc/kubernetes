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

package unstructured

import (
	"io/ioutil"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	runtimetesting "k8s.io/apimachinery/pkg/runtime/testing"

	"github.com/stretchr/testify/assert"
)

// 测试方式：多个 goroutine 并发编码同一个 UnstructuredList。Encode 若在
// 编码过程中修改对象内部 map，-race 检测会报数据竞争；正常实现应只读。
func TestCodecOfUnstructuredList(t *testing.T) {
	var wg sync.WaitGroup
	concurrency := 10
	list := UnstructuredList{
		Object: map[string]interface{}{},
	}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			// 输出丢弃即可，测试关注的是并发安全而非编码结果。
			assert.NoError(t, UnstructuredJSONScheme.Encode(&list, ioutil.Discard))
		}()
	}
	wg.Wait()
}

// TestRemoveNestedField 覆盖逐级删除：先删深层字段保留兄弟字段，再删空
// 父字段，最后验证删除不存在的路径是幂等的。
func TestRemoveNestedField(t *testing.T) {
	obj := map[string]interface{}{
		"x": map[string]interface{}{
			"y": 1,
			"a": "foo",
		},
	}
	RemoveNestedField(obj, "x", "a")
	assert.Len(t, obj["x"], 1)
	RemoveNestedField(obj, "x", "y")
	assert.Empty(t, obj["x"])
	RemoveNestedField(obj, "x")
	assert.Empty(t, obj)
	RemoveNestedField(obj, "x") // 删除不存在的字段。
	assert.Empty(t, obj)
}

// TestNestedFieldNoCopy 验证 NestedFieldNoCopy 的语义：找到的值是原对象的
// 引用而非拷贝；并覆盖字段缺失、中间字段为 null、路径经过非 map 等边界。
func TestNestedFieldNoCopy(t *testing.T) {
	target := map[string]interface{}{"foo": "bar"}

	obj := map[string]interface{}{
		"a": map[string]interface{}{
			"b": target,
			"c": nil,
			"d": []interface{}{"foo"},
			"e": []interface{}{
				map[string]interface{}{
					"f": "bar",
				},
			},
		},
	}

	// 情况 1：字段存在且非 nil。
	res, exists, err := NestedFieldNoCopy(obj, "a", "b")
	assert.True(t, exists)
	assert.NoError(t, err)
	assert.Equal(t, target, res)
	target["foo"] = "baz"
	assert.Equal(t, target["foo"], res.(map[string]interface{})["foo"], "result should be a reference to the expected item")

	// 情况 2：字段存在且为 nil。
	res, exists, err = NestedFieldNoCopy(obj, "a", "c")
	assert.True(t, exists)
	assert.NoError(t, err)
	assert.Nil(t, res)

	// 情况 3：遍历对象时出错。
	res, exists, err = NestedFieldNoCopy(obj, "a", "d", "foo")
	assert.False(t, exists)
	assert.Error(t, err)
	assert.Nil(t, res)

	// 情况 4：字段不存在。
	res, exists, err = NestedFieldNoCopy(obj, "a", "g")
	assert.False(t, exists)
	assert.NoError(t, err)
	assert.Nil(t, res)

	// 情况 5：中间字段不存在。
	res, exists, err = NestedFieldNoCopy(obj, "a", "g", "f")
	assert.False(t, exists)
	assert.NoError(t, err)
	assert.Nil(t, res)

	// 情况 6：中间字段为 null。
	//         （背景：在 YAML 中很容易出现）
	res, exists, err = NestedFieldNoCopy(obj, "a", "c", "f")
	assert.False(t, exists)
	assert.NoError(t, err)
	assert.Nil(t, res)

	// 情况 7：不支持数组/切片语法。
	//         （背景：用户可能期望支持此行为。）
	res, exists, err = NestedFieldNoCopy(obj, "a", "e[0]")
	assert.False(t, exists)
	assert.NoError(t, err)
	assert.Nil(t, res)
}

// TestNestedFieldCopy 验证 NestedFieldCopy 返回深拷贝：修改原对象不影响
// 结果。其余边界与 NestedFieldNoCopy 基本一致。
func TestNestedFieldCopy(t *testing.T) {
	target := map[string]interface{}{"foo": "bar"}

	obj := map[string]interface{}{
		"a": map[string]interface{}{
			"b": target,
			"c": nil,
			"d": []interface{}{"foo"},
		},
	}

	// 情况 1：字段存在且非 nil。
	res, exists, err := NestedFieldCopy(obj, "a", "b")
	assert.True(t, exists)
	assert.NoError(t, err)
	assert.Equal(t, target, res)
	target["foo"] = "baz"
	assert.NotEqual(t, target["foo"], res.(map[string]interface{})["foo"], "result should be a copy of the expected item")

	// 情况 2：字段存在且为 nil。
	res, exists, err = NestedFieldCopy(obj, "a", "c")
	assert.True(t, exists)
	assert.NoError(t, err)
	assert.Nil(t, res)

	// 情况 3：遍历对象时出错。
	res, exists, err = NestedFieldCopy(obj, "a", "d", "foo")
	assert.False(t, exists)
	assert.Error(t, err)
	assert.Nil(t, res)

	// 情况 4：字段不存在。
	res, exists, err = NestedFieldCopy(obj, "a", "e")
	assert.False(t, exists)
	assert.NoError(t, err)
	assert.Nil(t, res)
}

// TestCacheableObject 复用 runtime/testing 的通用契约测试，确认
// UnstructuredJSONScheme 产生的对象是可安全缓存的。
func TestCacheableObject(t *testing.T) {
	runtimetesting.CacheableObjectTest(t, UnstructuredJSONScheme)
}

// TestSetNestedStringSlice 验证 SetNestedStringSlice 会把 []string 转换
// 为 []interface{} 并写入指定路径。
func TestSetNestedStringSlice(t *testing.T) {
	obj := map[string]interface{}{
		"x": map[string]interface{}{
			"y": 1,
			"a": "foo",
		},
	}

	err := SetNestedStringSlice(obj, []string{"bar"}, "x", "z")
	assert.NoError(t, err)
	assert.Len(t, obj["x"], 3)
	assert.Len(t, obj["x"].(map[string]interface{})["z"], 1)
	assert.Equal(t, "bar", obj["x"].(map[string]interface{})["z"].([]interface{})[0])
}

// TestSetNestedSlice 验证直接写入 []interface{} 到嵌套路径。
func TestSetNestedSlice(t *testing.T) {
	obj := map[string]interface{}{
		"x": map[string]interface{}{
			"y": 1,
			"a": "foo",
		},
	}

	err := SetNestedSlice(obj, []interface{}{"bar"}, "x", "z")
	assert.NoError(t, err)
	assert.Len(t, obj["x"], 3)
	assert.Len(t, obj["x"].(map[string]interface{})["z"], 1)
	assert.Equal(t, "bar", obj["x"].(map[string]interface{})["z"].([]interface{})[0])
}

// TestSetNestedStringMap 验证 map[string]string 会被转换为
// map[string]interface{} 后写入。
func TestSetNestedStringMap(t *testing.T) {
	obj := map[string]interface{}{
		"x": map[string]interface{}{
			"y": 1,
			"a": "foo",
		},
	}

	err := SetNestedStringMap(obj, map[string]string{"b": "bar"}, "x", "z")
	assert.NoError(t, err)
	assert.Len(t, obj["x"], 3)
	assert.Len(t, obj["x"].(map[string]interface{})["z"], 1)
	assert.Equal(t, "bar", obj["x"].(map[string]interface{})["z"].(map[string]interface{})["b"])
}

// TestSetNestedMap 验证已符合 unstructured 表示形式的 map 直接写入。
func TestSetNestedMap(t *testing.T) {
	obj := map[string]interface{}{
		"x": map[string]interface{}{
			"y": 1,
			"a": "foo",
		},
	}

	err := SetNestedMap(obj, map[string]interface{}{"b": "bar"}, "x", "z")
	assert.NoError(t, err)
	assert.Len(t, obj["x"], 3)
	assert.Len(t, obj["x"].(map[string]interface{})["z"], 1)
	assert.Equal(t, "bar", obj["x"].(map[string]interface{})["z"].(map[string]interface{})["b"])
}

// TestNestedNumberAsFloat64 覆盖数值读取路径：接受 float64 与 int64，
// 拒绝其他类型；int64 超出 float64 可精确表示范围时必须报错而不是静默
// 舍入（否则下游会拿到错误的数值）。
func TestNestedNumberAsFloat64(t *testing.T) {
	for _, tc := range []struct {
		name           string
		obj            map[string]interface{}
		path           []string
		wantFloat64    float64
		wantBool       bool
		wantErrMessage string
	}{
		{
			name:           "not found",
			obj:            nil,
			path:           []string{"missing"},
			wantFloat64:    0,
			wantBool:       false,
			wantErrMessage: "",
		},
		{
			name:           "found float64",
			obj:            map[string]interface{}{"value": float64(42)},
			path:           []string{"value"},
			wantFloat64:    42,
			wantBool:       true,
			wantErrMessage: "",
		},
		{
			name:           "found unexpected type bool",
			obj:            map[string]interface{}{"value": true},
			path:           []string{"value"},
			wantFloat64:    0,
			wantBool:       false,
			wantErrMessage: ".value accessor error: true is of the type bool, expected float64 or int64",
		},
		{
			name:           "found int64",
			obj:            map[string]interface{}{"value": int64(42)},
			path:           []string{"value"},
			wantFloat64:    42,
			wantBool:       true,
			wantErrMessage: "",
		},
		{
			name:           "found int64 not representable as float64",
			obj:            map[string]interface{}{"value": int64(math.MaxInt64)},
			path:           []string{"value"},
			wantFloat64:    0,
			wantBool:       false,
			wantErrMessage: ".value accessor error: int64 value 9223372036854775807 cannot be losslessly converted to float64",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotFloat64, gotBool, gotErr := NestedNumberAsFloat64(tc.obj, tc.path...)
			if gotFloat64 != tc.wantFloat64 {
				t.Errorf("got %v, wanted %v", gotFloat64, tc.wantFloat64)
			}
			if gotBool != tc.wantBool {
				t.Errorf("got %t, wanted %t", gotBool, tc.wantBool)
			}
			if tc.wantErrMessage != "" {
				if gotErr == nil {
					t.Errorf("got nil error, wanted %s", tc.wantErrMessage)
				} else if gotErrMessage := gotErr.Error(); gotErrMessage != tc.wantErrMessage {
					t.Errorf("wanted error %q, got: %v", gotErrMessage, tc.wantErrMessage)
				}
			} else if gotErr != nil {
				t.Errorf("wanted nil error, got %v", gotErr)
			}
		})
	}
}

// TestNestedNullCoercingStringMap 覆盖“宽松”的字符串 map 读取：null 值
// 被强制转换为空字符串；但非字符串、非 null 的值会报错并返回 found=false，
// 避免把类型错误悄悄吞掉。
func TestNestedNullCoercingStringMap(t *testing.T) {
	for _, tc := range []struct {
		name           string
		obj            map[string]interface{}
		path           []string
		wantObj        map[string]string
		wantFound      bool
		wantErrMessage string
	}{
		{
			name:           "missing map",
			obj:            nil,
			path:           []string{"path"},
			wantObj:        nil,
			wantFound:      false,
			wantErrMessage: "",
		},
		{
			name:           "null map",
			obj:            map[string]interface{}{"path": nil},
			path:           []string{"path"},
			wantObj:        nil,
			wantFound:      true,
			wantErrMessage: "",
		},
		{
			name:           "non map",
			obj:            map[string]interface{}{"path": 0},
			path:           []string{"path"},
			wantObj:        nil,
			wantFound:      false,
			wantErrMessage: "type int",
		},
		{
			name:           "empty map",
			obj:            map[string]interface{}{"path": map[string]interface{}{}},
			path:           []string{"path"},
			wantObj:        map[string]string{},
			wantFound:      true,
			wantErrMessage: "",
		},
		{
			name:           "string value",
			obj:            map[string]interface{}{"path": map[string]interface{}{"a": "1", "b": "2"}},
			path:           []string{"path"},
			wantObj:        map[string]string{"a": "1", "b": "2"},
			wantFound:      true,
			wantErrMessage: "",
		},
		{
			name:           "null value",
			obj:            map[string]interface{}{"path": map[string]interface{}{"a": "1", "b": nil}},
			path:           []string{"path"},
			wantObj:        map[string]string{"a": "1", "b": ""},
			wantFound:      true,
			wantErrMessage: "",
		},
		{
			name:           "invalid value",
			obj:            map[string]interface{}{"path": map[string]interface{}{"a": "1", "b": nil, "c": 0}},
			path:           []string{"path"},
			wantObj:        nil,
			wantFound:      false,
			wantErrMessage: `key "c": 0`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotObj, gotFound, gotErr := NestedNullCoercingStringMap(tc.obj, tc.path...)
			if !reflect.DeepEqual(gotObj, tc.wantObj) {
				t.Errorf("got %#v, wanted %#v", gotObj, tc.wantObj)
			}
			if gotFound != tc.wantFound {
				t.Errorf("got %v, wanted %v", gotFound, tc.wantFound)
			}
			if tc.wantErrMessage != "" {
				if gotErr == nil {
					t.Errorf("got nil error, wanted %s", tc.wantErrMessage)
				} else if gotErrMessage := gotErr.Error(); !strings.Contains(gotErrMessage, tc.wantErrMessage) {
					t.Errorf("wanted error %q, got: %v", gotErrMessage, tc.wantErrMessage)
				}
			} else if gotErr != nil {
				t.Errorf("wanted nil error, got %v", gotErr)
			}
		})
	}
}
