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
// RandFill 用 1000 年范围内的随机秒数填充 Time。
func (t *Time) RandFill(r *rand.Rand) {
	if t == nil {
		return
	}
	// Allow for about 1000 years of randomness.  Leave off nanoseconds
	// because JSON doesn't represent them so they can't round-trip
	// properly.
	// 保留纳秒会导致 JSON 往返丢失精度，因此随机值固定为整秒。
	t.Time = time.Unix(r.Int63n(1000*365*24*60*60), 0)
}

// ensure Time implements randfill.SimpleSelfFiller
// 编译期断言，防止 randfill 接口变更后这里悄悄失效。
var _ randfill.SimpleSelfFiller = &Time{}
