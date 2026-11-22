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

package v1

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"sigs.k8s.io/randfill"
)

// TestPatchOptionsIsSuperSetOfUpdateOptions 验证 PatchOptions 在 JSON 字段上
// 是 UpdateOptions 的超集：任意随机填充的 UpdateOptions 序列化后能被
// PatchOptions 解码、再编码回来不丢字段。
//
// 这是兼容性约束：客户端可能把 UpdateOptions 的 JSON 直接用于 patch 路径，
// 两个类型的共享字段必须保持相同的 json tag。
func TestPatchOptionsIsSuperSetOfUpdateOptions(t *testing.T) {
	f := randfill.New()
	for i := 0; i < 1000; i++ {
		t.Run(fmt.Sprintf("Run %d/1000", i), func(t *testing.T) {
			update := UpdateOptions{}
			f.Fill(&update)

			// UpdateOptions -> JSON
			b, err := json.Marshal(update)
			if err != nil {
				t.Fatalf("failed to marshal UpdateOptions (%v): %v", err, update)
			}
			// JSON -> PatchOptions：共享字段必须被识别，未知字段被忽略。
			patch := PatchOptions{}
			err = json.Unmarshal(b, &patch)
			if err != nil {
				t.Fatalf("failed to unmarshal UpdateOptions into PatchOptions: %v", err)
			}

			// PatchOptions -> JSON -> UpdateOptions：共享字段不得丢失。
			b, err = json.Marshal(patch)
			if err != nil {
				t.Fatalf("failed to marshal PatchOptions (%v): %v", err, patch)
			}
			got := UpdateOptions{}
			err = json.Unmarshal(b, &got)
			if err != nil {
				t.Fatalf("failed to unmarshal UpdateOptions into UpdateOptions: %v", err)
			}

			if !reflect.DeepEqual(update, got) {
				t.Fatalf(`UpdateOptions -> PatchOptions -> UpdateOptions round-trip failed:
got: %v
want: %v`, got, update)
			}
		})
	}
}
