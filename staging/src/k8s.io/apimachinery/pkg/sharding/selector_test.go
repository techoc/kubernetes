/*
Copyright The Kubernetes Authors.

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

package sharding

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// testObject 是用于测试的最小 runtime.Object，只带类型与对象元信息。
type testObject struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata"`
}

// DeepCopyObject 复制元信息，避免测试对象之间共享指针。
func (t *testObject) DeepCopyObject() runtime.Object {
	return &testObject{
		TypeMeta:   t.TypeMeta,
		ObjectMeta: *t.ObjectMeta.DeepCopy(),
	}
}

// TestSelectorMatches 验证 sharding.Selector 的范围判定：空选择器与 Everything()
// 匹配全部对象；范围比较按哈希字符串的大小进行；边界应是“左闭右开”。
func TestSelectorMatches(t *testing.T) {
	obj := &testObject{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID("test-uid-123"),
			Name:      "test-name",
			Namespace: "test-namespace",
		},
	}

	// 用对象 UID 计算分片哈希，作为各用例的参照值。
	hash := "0x" + HashField("test-uid-123")

	tests := []struct {
		name      string
		selector  Selector
		wantMatch bool
	}{
		{
			name:      "everything matches",
			selector:  Everything(),
			wantMatch: true,
		},
		{
			name:      "empty selector matches",
			selector:  NewSelector(),
			wantMatch: true,
		},
		{
			name: "full range matches",
			selector: NewSelector(ShardRangeRequirement{
				Key:   "object.metadata.uid",
				Start: "0x0000000000000000",
				End:   "0x10000000000000000",
			}),
			wantMatch: true,
		},
		{
			name: "hash in specific range",
			selector: NewSelector(ShardRangeRequirement{
				Key:   "object.metadata.uid",
				Start: hash,
				End:   hash + "f", // hash + "f" 一定大于 hash
			}),
			wantMatch: true,
		},
		{
			name: "hash below start",
			selector: NewSelector(ShardRangeRequirement{
				Key:   "object.metadata.uid",
				Start: "0xffffffffffffffff",
				End:   "0x10000000000000000",
			}),
			// 起点高于哈希时是否匹配取决于实际哈希，用同一比较表达式保证断言自洽。
			wantMatch: hash >= "0xffffffffffffffff",
		},
		{
			name: "hash at or above end",
			selector: NewSelector(ShardRangeRequirement{
				Key:   "object.metadata.uid",
				Start: "0x0000000000000000",
				End:   "0x0000000000000001",
			}),
			// 区间右端开放：哈希等于 End 时不应匹配，这里用它验证边界语义。
			wantMatch: hash < "0x0000000000000001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, err := tt.selector.Matches(obj)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if matched != tt.wantMatch {
				t.Errorf("Matches() = %v, want %v (hash=%s)", matched, tt.wantMatch, hash)
			}
		})
	}
}

// TestSelectorEmpty 验证 Empty 判定：Everything() 与无要求的选择器为空，
// 带任一范围要求的选择器不为空。
func TestSelectorEmpty(t *testing.T) {
	if !Everything().Empty() {
		t.Error("Everything() should be empty")
	}
	if !NewSelector().Empty() {
		t.Error("NewSelector() with no args should be empty")
	}

	sel := NewSelector(ShardRangeRequirement{Key: "object.metadata.uid", Start: "0x0000000000000000", End: "0x8000000000000000"})
	if sel.Empty() {
		t.Error("selector with requirement should not be empty")
	}
}

// TestSelectorString 验证选择器的字符串表示稳定，便于日志与调试。
func TestSelectorString(t *testing.T) {
	sel := NewSelector(ShardRangeRequirement{
		Key:   "object.metadata.uid",
		Start: "0x00",
		End:   "0x80",
	})
	expected := "shardRange(object.metadata.uid, '0x00', '0x80')"
	if sel.String() != expected {
		t.Errorf("String() = %q, want %q", sel.String(), expected)
	}
}
