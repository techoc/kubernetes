//go:build !notest

/*
Copyright 2020 The Kubernetes Authors.

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

package v1

import (
	"math/rand"
	"time"

	"sigs.k8s.io/randfill"
)

// Fuzz satisfies randfill.SimpleSelfFiller.
//
// RandFill 为 MicroTime 提供随机填充实现，供 fuzz 测试构造输入。
// 构建标签 !notest 保证只在测试构建中包含该文件，生产二进制不会引入 rand 依赖。
func (t *MicroTime) RandFill(r *rand.Rand) {
	if t == nil {
		return
	}
	// Allow for about 1000 years of randomness. Accurate to a tenth of
	// micro second. Leave off nanoseconds because JSON doesn't
	// represent them so they can't round-trip properly.
	//
	// 微秒部分乘以 1000 并落在 [0, 1e9) 内，因此精度为 0.1 微秒；
	// 纳秒恒为 0，避免 JSON 往返时被截断导致测试误报。
	t.Time = time.Unix(r.Int63n(1000*365*24*60*60), 1000*r.Int63n(1000000))
}

// ensure MicroTime implements randfill.Interface
var _ randfill.SimpleSelfFiller = &MicroTime{}
