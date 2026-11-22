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

package meta

import (
	"math/rand"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"
)

// TestAsPartialObjectMetadata 用随机数据填充对象，验证
// AsPartialObjectMetadata 生成的 PartialObjectMetadata 完整复制了 ObjectMeta，
// 不丢失任何字段（包括 nil 字段）。两种输入类型分别验证。
func TestAsPartialObjectMetadata(t *testing.T) {
	// NilChance(.5) 让一半的指针/切片为 nil，NumElements(0,1) 限制集合大小，
	// 固定随机种子以保证可复现。
	f := randfill.New().NilChance(.5).NumElements(0, 1).RandSource(rand.NewSource(1))

	for i := 0; i < 100; i++ {
		m := &metav1.ObjectMeta{}
		f.Fill(m)
		partial := AsPartialObjectMetadata(m)
		if !reflect.DeepEqual(&partial.ObjectMeta, m) {
			t.Fatalf("incomplete partial object metadata: %s", cmp.Diff(&partial.ObjectMeta, m))
		}
	}

	// v1beta1.PartialObjectMetadata 走相同实现，确认其对象元数据也能无损拷贝。
	for i := 0; i < 100; i++ {
		m := &metav1beta1.PartialObjectMetadata{}
		f.Fill(&m.ObjectMeta)
		partial := AsPartialObjectMetadata(m)
		if !reflect.DeepEqual(&partial.ObjectMeta, &m.ObjectMeta) {
			t.Fatalf("incomplete partial object metadata: %s", cmp.Diff(&partial.ObjectMeta, &m.ObjectMeta))
		}
	}
}
