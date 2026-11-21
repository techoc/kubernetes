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

package compat

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// TestCompatibility 用指定版本的 codec 把输入重新编码，
// 然后检查生成的 JSON 中期望的键存在、不该有的键不存在。
// 参考：https://github.com/openshift/origin/blob/master/pkg/api/compatibility_test.go
func TestCompatibility(
	t *testing.T,
	version schema.GroupVersion,
	input []byte,
	validator func(obj runtime.Object) field.ErrorList,
	expectedKeys map[string]string,
	absentKeys []string,
) {

	// 用目标版本的 legacy codec 解码输入。
	codec := legacyscheme.Codecs.LegacyCodec(version)
	obj, err := runtime.Decode(codec, input)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 解码后的对象必须能通过校验，否则测试前提不成立。
	errs := validator(obj)
	if len(errs) != 0 {
		t.Fatalf("Unexpected validation errors: %v", errs)
	}

	// 再编码回 JSON：兼容性关注的就是这一轮往返后字段是否保留/消失。
	output, err := runtime.Encode(codec, obj)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 反序列化成通用 map，便于按路径检查任意层级的键。
	generic := map[string]interface{}{}
	if err := json.Unmarshal(output, &generic); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 检查必须存在的键及其值。
	for k, expectedValue := range expectedKeys {
		keys := strings.Split(k, ".")
		if actualValue, ok, err := getJSONValue(generic, keys...); err != nil || !ok {
			t.Errorf("Unexpected error for %s: %v", k, err)
		} else if !reflect.DeepEqual(expectedValue, fmt.Sprintf("%v", actualValue)) {
			t.Errorf("Unexpected value for %v: expected %v, got %v", k, expectedValue, actualValue)
		}
	}

	// 检查必须不存在的键：能取到值（或没报错）都算失败。
	for _, absentKey := range absentKeys {
		keys := strings.Split(absentKey, ".")
		actualValue, ok, err := getJSONValue(generic, keys...)
		if err == nil || ok {
			t.Errorf("Unexpected value found for key %s: %v", absentKey, actualValue)
		}
	}

	// 失败时打印解码对象和编码结果，方便定位字段差异。
	if t.Failed() {
		data, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			t.Log(err)
		} else {
			t.Log(string(data))
		}
		t.Logf("2: Encoded value: %v", string(output))
	}
}

// getJSONValue 按点分隔的路径从嵌套 JSON（map/slice）中取值。
// 路径中的 key 可以带 [n] 后缀表示取切片的第 n 个元素，例如 "items[0].name"。
func getJSONValue(data map[string]interface{}, keys ...string) (interface{}, bool, error) {
	// 没有剩余 key，当前数据就是结果。
	if len(keys) == 0 {
		return data, true, nil
	}

	// 取当前级的 key，并解析可选的下标。
	key := keys[0]
	index := -1
	if matches := regexp.MustCompile(`^(.*)\[(\d+)\]$`).FindStringSubmatch(key); len(matches) > 0 {
		key = matches[1]
		index, _ = strconv.Atoi(matches[2])
	}

	// 查找 key。
	value, ok := data[key]
	if !ok {
		return nil, false, fmt.Errorf("no key %s found", key)
	}

	// 如果带了下标，要求 value 是切片且下标有效。
	if index >= 0 {
		valueSlice, ok := value.([]interface{})
		if !ok {
			return nil, false, fmt.Errorf("key %s did not hold a slice", key)
		}
		if index >= len(valueSlice) {
			return nil, false, fmt.Errorf("index %d out of bounds for slice at key: %v", index, key)
		}
		value = valueSlice[index]
	}

	// 路径走完了，返回当前值。
	if len(keys) == 1 {
		return value, true, nil
	}

	// 继续深入下一级，要求当前值是 map。
	childData, ok := value.(map[string]interface{})
	if !ok {
		return nil, false, fmt.Errorf("key %s did not hold a map", keys[0])
	}
	return getJSONValue(childData, keys[1:]...)
}
