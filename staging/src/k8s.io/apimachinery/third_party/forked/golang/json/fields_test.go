package json

import (
	"reflect"
	"testing"
)

// TestLookupPtrToStruct 验证对结构体指针按 JSON 字段名查找时，
// 能返回字段类型、patchStrategy 列表与 patchMergeKey。
func TestLookupPtrToStruct(t *testing.T) {
	type Elem struct {
		Key   string
		Value string
	}
	type Outer struct {
		Inner []Elem `json:"inner" patchStrategy:"merge" patchMergeKey:"key"`
	}
	outer := &Outer{}
	elemType, patchStrategies, patchMergeKey, err := LookupPatchMetadataForStruct(reflect.TypeOf(outer), "inner")
	if err != nil {
		t.Fatal(err)
	}
	if elemType != reflect.TypeOf([]Elem{}) {
		t.Errorf("elemType = %v, want: %v", elemType, reflect.TypeOf([]Elem{}))
	}
	if !reflect.DeepEqual(patchStrategies, []string{"merge"}) {
		t.Errorf("patchStrategies = %v, want: %v", patchStrategies, []string{"merge"})
	}
	if patchMergeKey != "key" {
		t.Errorf("patchMergeKey = %v, want: %v", patchMergeKey, "key")
	}
}
