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

// IP provides a CEL representation of an IP address.
//
// 包装 netip.Addr 而非字符串，使 CEL 中的比较是地址语义（IPv4/IPv6 归一化）而不是字节语义。
type IP struct {
	netip.Addr
}

var (
	// IPType 是对外暴露的不透明类型名，CEL 表达式中通过它做类型断言。
	IPType = cel.OpaqueType("net.IP")
)

// ConvertToNative implements ref.Val.ConvertToNative.
//
// 支持转为 netip.Addr 或字符串，后者便于输出与日志。
func (d IP) ConvertToNative(typeDesc reflect.Type) (any, error) {
	if reflect.TypeOf(d.Addr).AssignableTo(typeDesc) {
		return d.Addr, nil
	}
	if reflect.TypeOf("").AssignableTo(typeDesc) {
		return d.Addr.String(), nil
	}
	return nil, fmt.Errorf("type conversion error from 'IP' to '%v'", typeDesc)
}

// ConvertToType implements ref.Val.ConvertToType.
func (d IP) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case IPType:
		return d
	case types.TypeType:
		return IPType
	case types.StringType:
		return types.String(d.Addr.String())
	}
	return types.NewErr("type conversion error from '%s' to '%s'", IPType, typeVal)
}

// Equal implements ref.Val.Equal.
//
// 与不同类型比较时返回 "no such overload" 而不是 false，遵循 CEL 的类型严格性。
func (d IP) Equal(other ref.Val) ref.Val {
	otherD, ok := other.(IP)
	if !ok {
		return types.ValOrErr(other, "no such overload")
	}
	return types.Bool(d.Addr == otherD.Addr)
}

// Type implements ref.Val.Type.
func (d IP) Type() ref.Type {
	return IPType
}

// Value implements ref.Val.Value.
func (d IP) Value() any {
	return d.Addr
}

// Size returns the size of the IP address in bytes.
// Used in the size estimation of the runtime cost.
//
// CEL 运行时成本估算依赖 Size 计算内存占用，因此必须返回字节数而非字符串长度。
func (d IP) Size() ref.Val {
	return types.Int(int(math.Ceil(float64(d.Addr.BitLen()) / 8)))
}
