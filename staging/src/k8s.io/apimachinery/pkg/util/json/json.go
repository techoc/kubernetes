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

package json

import (
	"encoding/json"
	"fmt"
	"io"

	kjson "sigs.k8s.io/json"
)

// NewEncoder 转发到标准库 json.NewEncoder。保留该包装函数是为了让本包可直接替代
// 常见的 encoding/json 用法，调用方无需修改编码器创建逻辑。
// 仅做转发，使调用方可以整体切换到本包而无需改动编码代码。
func NewEncoder(w io.Writer) *json.Encoder {
	return json.NewEncoder(w)
}

// Marshal 转发到标准库 json.Marshal。保留该包装函数是为了让本包可直接替代
// 常见的 encoding/json 用法。
func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// 限制递归深度，防止深度异常的嵌套 JSON 导致栈溢出。
// maxDepth 限制递归遍历深度，防止恶意嵌套 JSON 导致栈溢出。
const maxDepth = 10000

// Unmarshal 将给定 JSON 数据解码到 v。对象键区分大小写；解码到 interface{} 的数字
// 会转换为 int64 或 float64，而不是默认统一转换为 float64。
// 与标准库不同，这里键名大小写敏感且尽量保留整数精度，这是 API 兼容性要求。
func Unmarshal(data []byte, v interface{}) error {
	return kjson.UnmarshalCaseSensitivePreserveInts(data, v)
}

// ConvertInterfaceNumbers 将 v 中的 json.Number 转换为 int64 或 float64。
// 对 map[string]interface{} 和 []interface{} 会递归遍历其内容。
// 对传入值的顶层做一次转换；depth 由调用方传入，用于限制递归深度。
func ConvertInterfaceNumbers(v *interface{}, depth int) error {
	var err error
	switch v2 := (*v).(type) {
	case json.Number:
		*v, err = convertNumber(v2)
	case map[string]interface{}:
		err = ConvertMapNumbers(v2, depth+1)
	case []interface{}:
		err = ConvertSliceNumbers(v2, depth+1)
	}
	return err
}

// ConvertMapNumbers 遍历 map，将其中的 json.Number 转换为 int64 或 float64。
// 嵌套 map 和 slice 会递归处理；转换结果直接写回原 map。
// 遍历 map 并就地替换 json.Number；遇到嵌套容器时递归，深度超限即报错。
func ConvertMapNumbers(m map[string]interface{}, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("exceeded max depth of %d", maxDepth)
	}

	var err error
	for k, v := range m {
		switch v := v.(type) {
		case json.Number:
			m[k], err = convertNumber(v)
		case map[string]interface{}:
			err = ConvertMapNumbers(v, depth+1)
		case []interface{}:
			err = ConvertSliceNumbers(v, depth+1)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// ConvertSliceNumbers 遍历 slice，将其中的 json.Number 转换为 int64 或 float64。
// 嵌套 map 和 slice 会递归处理；转换结果直接写回原 slice。
// 遍历切片并就地替换 json.Number；深度限制与 ConvertMapNumbers 一致。
func ConvertSliceNumbers(s []interface{}, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("exceeded max depth of %d", maxDepth)
	}

	var err error
	for i, v := range s {
		switch v := v.(type) {
		case json.Number:
			s[i], err = convertNumber(v)
		case map[string]interface{}:
			err = ConvertMapNumbers(v, depth+1)
		case []interface{}:
			err = ConvertSliceNumbers(v, depth+1)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// convertNumber 优先将 json.Number 转换为 int64，以保留整数精度；若不适合 int64，
// 则尝试转换为 float64，转换失败时返回错误。
// 优先转 int64 以保留整数精度，失败（含溢出）时退回 float64。
func convertNumber(n json.Number) (interface{}, error) {
	// 优先尝试 int64，避免整数被转成 float64 后丢失精度。
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	// 回退为 float64，与标准 JSON 解码器的默认数字行为一致；超出 float64 范围时返回错误。
	return n.Float64()
}
