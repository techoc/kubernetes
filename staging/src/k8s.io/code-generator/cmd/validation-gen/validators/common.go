/*
Copyright 2024 The Kubernetes Authors.

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

package validators

import (
	"fmt"
	"strconv"

	"k8s.io/gengo/v2/types"
)

const (
	// libValidationPkg 是校验函数“标准库”的 import path。
	// 生成的校验代码调用该包中的泛型校验函数，而不是内联逻辑，
	// 以便校验语义集中维护并被单元测试覆盖。
	libValidationPkg = "k8s.io/apimachinery/pkg/api/validate"
)

// rootTypeString 返回 src/dst 类型关系的字符串表示，用于错误信息。
// 当源/目标类型相同时只输出一个名字，避免 "T -> T" 这类噪声。
func rootTypeString(src, dst *types.Type) string {
	if src == dst {
		return src.String()
	}
	return src.String() + " -> " + dst.String()
}

// isUnsignedInt 报告 t 是否为无符号整数类型。
func isUnsignedInt(t *types.Type) bool {
	switch t {
	case types.Uint, types.Uint64, types.Uint32, types.Uint16, types.Byte:
		return true
	}
	return false
}

// intBitSize 返回 gengo 整数类型的位宽。平台相关类型（int、uint）使用
// strconv.IntSize，Byte 视为 8。非受支持整数类型直接报错，以便在生成阶段
// 发现问题，而不是静默降级。
func intBitSize(t *types.Type) (int, error) {
	switch t {
	case types.Byte: // int8 becomes byte in gengo
		return 8, nil
	case types.Int16, types.Uint16:
		return 16, nil
	case types.Int32, types.Uint32:
		return 32, nil
	case types.Int64, types.Uint64:
		return 64, nil
	case types.Int, types.Uint:
		return strconv.IntSize, nil
	default:
		return 0, fmt.Errorf("unsupported integer type: %v", t)
	}
}
