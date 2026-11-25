/*
Copyright 2023 The Kubernetes Authors.

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

package cel

import (
	"fmt"
	"math"
	"net/netip"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// CIDR 提供网络地址的 CEL 表示。
// 内嵌 netip.Prefix 而非字符串，使 CEL 表达式中的 net.CIDR 在求值时
// 保持结构化语义（可做相等比较、可转字符串），同时避免每次操作重新解析。
// 该类型不可变：所有方法都以值接收者工作，可安全地在并发求值间共享。
type CIDR struct {
	netip.Prefix
}

var (
	// CIDRType 是注册到 CEL 环境的不透明类型标识；声明为包级变量是为了让
	// 类型转换与类型检查复用同一个标识，避免重复构造导致的比较失败。
	CIDRType = cel.OpaqueType("net.CIDR")
)

// ConvertToNative 实现 ref.Val.ConvertToNative。
// CEL 与 Go 宿主互操作时，把 CIDR 转为 netip.Prefix 或字符串。
// 前者保留结构，后者用于需要文本的场景（如 JSON 序列化）。
func (d CIDR) ConvertToNative(typeDesc reflect.Type) (any, error) {
	if reflect.TypeOf(d.Prefix).AssignableTo(typeDesc) {
		return d.Prefix, nil
	}
	if reflect.TypeOf("").AssignableTo(typeDesc) {
		return d.Prefix.String(), nil
	}
	return nil, fmt.Errorf("type conversion error from 'CIDR' to '%v'", typeDesc)
}

// ConvertToType 实现 ref.Val.ConvertToType。
// 支持三种目标类型——自身、TypeType（用于 typeof）与 String。
// 其他类型返回 CEL 错误值而非 Go error，以便在表达式求值中作为可捕获的错误传播。
func (d CIDR) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case CIDRType:
		return d
	case types.TypeType:
		return CIDRType
	case types.StringType:
		return types.String(d.Prefix.String())
	}
	return types.NewErr("type conversion error from '%s' to '%s'", CIDRType, typeVal)
}

// Equal 实现 ref.Val.Equal。
// netip.Prefix 是可直接比较的值类型，因此用 == 即可同时比较地址与掩码长度；
// 非 CIDR 的右操作数返回 no such overload，符合 CEL 对不匹配类型的处理约定。
func (d CIDR) Equal(other ref.Val) ref.Val {
	otherD, ok := other.(CIDR)
	if !ok {
		return types.ValOrErr(other, "no such overload")
	}

	return types.Bool(d.Prefix == otherD.Prefix)
}

// Type implements ref.Val.Type.
func (d CIDR) Type() ref.Type {
	return CIDRType
}

// Value 实现 ref.Val.Value。
// 返回原始 netip.Prefix 而非字符串，让宿主侧拿回无损数据。
func (d CIDR) Value() any {
	return d.Prefix
}

// Size 返回 CIDR 前缀地址的字节大小，用于运行时成本（runtime cost）的
// 规模估算。
// CEL 运行时用 Size 估算对象占用的「成本」，从而对表达式求值设置资源上限，
// 防止恶意或失误的表达式耗尽 CPU/内存。这里按前缀位数向上取整到字节数，
// 是地址宽度的保守近似（未计入结构体开销），但足以让成本模型对 IPv6 与 IPv4 区别计价。
func (d CIDR) Size() ref.Val {
	return types.Int(int(math.Ceil(float64(d.Prefix.Bits()) / 8)))
}
