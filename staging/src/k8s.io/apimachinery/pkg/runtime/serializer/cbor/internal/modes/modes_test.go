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

// Package modes_test 从外部验证 modes 包暴露的编码/解码模式；这些模式被
// 逐项收集到列表中，便于后续对每种模式统一跑同一组断言。
package modes_test

import (
	"errors"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor/internal/modes"
)

// encModeNames/allEncModes 是编码模式及其可读名称，测试失败时用于标识
// 具体是哪个模式出问题。
var encModeNames = map[modes.EncMode]string{
	modes.Encode:                 "Encode",
	modes.EncodeNondeterministic: "EncodeNondeterministic",
}

var allEncModes = []modes.EncMode{
	modes.Encode,
	modes.EncodeNondeterministic,
}

// decModeNames/allDecModes 同上，对应解码模式；DecodeLax 允许非首选编码
// （例如非最短长度的整数），用于兼容性场景。
var decModeNames = map[cbor.DecMode]string{
	modes.Decode:    "Decode",
	modes.DecodeLax: "DecodeLax",
}

var allDecModes = []cbor.DecMode{
	modes.Decode,
	modes.DecodeLax,
}

// assertNilError 断言无错误。
func assertNilError(t *testing.T, e error) {
	if e != nil {
		t.Errorf("expected nil error, got: %v", e)
	}
}

// assertOnConcreteError 把 error 断言为具体类型 E 后再交给 fn 进一步检查，
// 避免各用例重复写 errors.As。
func assertOnConcreteError[E error](fn func(*testing.T, E)) func(t *testing.T, e error) {
	return func(t *testing.T, ei error) {
		var ec E
		if !errors.As(ei, &ec) {
			t.Errorf("expected concrete error type %T, got %T: %v", ec, ei, ei)
			return
		}
		fn(t, ec)
	}
}

// assertErrorMessage 精确比较错误文案；CBOR 编解码错误的可读信息属于预期
// 行为的一部分，因此这里不做模糊匹配。
func assertErrorMessage(want string) func(*testing.T, error) {
	return func(t *testing.T, got error) {
		if got == nil {
			t.Error("expected non-nil error")
			return
		}
		if got.Error() != want {
			t.Errorf("got error %q, want %q", got.Error(), want)
		}
	}
}

// assertIdenticalError 断言 error 与期望值在结构上完全一致（用 cmp.Diff
// 展示差异），适合含字段值的具体错误类型。
func assertIdenticalError[E error](expected E) func(*testing.T, error) {
	return assertOnConcreteError(func(t *testing.T, actual E) {
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Errorf("diff between actual error and expected error:\n%s", diff)
		}
	})
}
