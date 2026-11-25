/*
Copyright 2025 The Kubernetes Authors.

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

package diff

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// TestDiffWithRealGoCmp 将我们的 Diff 与实际的 go-cmp Diff 在多种数据类型和结构上对比，
// 以验证兼容性。
//
// 由于 Diff 可能在不同 json v1/v2 实现下产生不同措辞，这里不比对完整
// 文本，只断言关键字段与取值都出现在输出中。
func TestDiffWithRealGoCmp(t *testing.T) {
	//测试简单类型
	t.Run("SimpleTypes", func(t *testing.T) {
		testCases := []struct {
			name string
			a    interface{}
			b    interface{}
		}{
			{name: "Integers", a: 42, b: 43},
			{name: "Strings", a: "hello", b: "world"},
			{name: "Booleans", a: true, b: false},
			{name: "Floats", a: 3.14, b: 2.71},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ourDiff := Diff(tc.a, tc.b)
				goCmpDiff := cmp.Diff(tc.a, tc.b)

				t.Logf("Our diff:\n%s\n\nGo-cmp diff:\n%s", ourDiff, goCmpDiff)

				//验证两个差异结果均非空
				if ourDiff == "" || goCmpDiff == "" {
					t.Errorf("Expected non-empty diffs, got ourDiff: %v, goCmpDiff: %v",
						ourDiff == "", goCmpDiff == "")
				}
			})
		}
	})

	//测试简单结构体
	// 验证字段名、旧值与新值都出现在 diff 中。
	t.Run("SimpleStruct", func(t *testing.T) {
		type TestStruct struct {
			Name string
			Age  int
			Tags []string
			Map  map[string]int
		}

		a := TestStruct{
			Name: "Alice",
			Age:  30,
			Tags: []string{"tag1", "tag2"},
			Map:  map[string]int{"a": 1, "b": 2},
		}

		b := TestStruct{
			Name: "Bob",
			Age:  25,
			Tags: []string{"tag1", "tag3"},
			Map:  map[string]int{"a": 1, "c": 3},
		}

		ourDiff := Diff(a, b)
		goCmpDiff := cmp.Diff(a, b)

		t.Logf("Our diff:\n%s\n\nGo-cmp diff:\n%s", ourDiff, goCmpDiff)

		//检查我们的差异结果包含关键差异
		keyDifferences := []string{
			"Name", "Alice", "Bob",
			"Age", "30", "25",
			"Tags", "tag2", "tag3",
			"Map", "b", "c",
		}

		for _, key := range keyDifferences {
			if !strings.Contains(ourDiff, key) {
				t.Errorf("Our diff doesn't contain expected key difference: %q", key)
			}
		}
	})

	//测试复杂的嵌套结构体
	// 覆盖 map 中缺失键、切片元素增删与 interface 嵌套的差异表达。
	t.Run("ComplexNestedStruct", func(t *testing.T) {
		type Address struct {
			Street     string
			City       string
			State      string
			PostalCode string
			Country    string
		}

		type Contact struct {
			Type  string
			Value string
		}

		type Person struct {
			ID        int
			FirstName string
			LastName  string
			Age       int
			Addresses map[string]Address
			Contacts  []Contact
			Metadata  map[string]interface{}
			CreatedAt time.Time
		}

		now := time.Now()
		later := now.Add(24 * time.Hour)

		person1 := Person{
			ID:        1,
			FirstName: "John",
			LastName:  "Doe",
			Age:       30,
			Addresses: map[string]Address{
				"home": {
					Street:     "123 Main St",
					City:       "Anytown",
					State:      "CA",
					PostalCode: "12345",
					Country:    "USA",
				},
				"work": {
					Street:     "456 Market St",
					City:       "Worktown",
					State:      "CA",
					PostalCode: "54321",
					Country:    "USA",
				},
			},
			Contacts: []Contact{
				{Type: "email", Value: "john.doe@example.com"},
				{Type: "phone", Value: "555-1234"},
			},
			Metadata: map[string]interface{}{
				"created":    "2023-01-01",
				"loginCount": 42,
				"settings": map[string]bool{
					"notifications": true,
					"darkMode":      false,
				},
			},
			CreatedAt: now,
		}

		person2 := Person{
			ID:        1,
			FirstName: "John",
			LastName:  "Smith", //不同
			Age:       31,      //不同
			Addresses: map[string]Address{
				"home": {
					Street:     "123 Main St",
					City:       "Anytown",
					State:      "CA",
					PostalCode: "12345",
					Country:    "USA",
				},
				//"work" 地址缺失
			},
			Contacts: []Contact{
				{Type: "email", Value: "john.smith@example.com"}, //不同
				{Type: "phone", Value: "555-1234"},
				{Type: "fax", Value: "555-5678"}, //额外内容
			},
			Metadata: map[string]interface{}{
				"created":    "2023-01-01",
				"loginCount": 43, //不同
				"settings": map[string]bool{
					"notifications": false, //不同
					"darkMode":      true,  //不同
				},
				"newField": "new value", //新增字段
			},
			CreatedAt: later, //不同
		}

		ourDiff := Diff(person1, person2)
		goCmpDiff := cmp.Diff(person1, person2)

		t.Logf("Our diff:\n%s\n\nGo-cmp diff:\n%s", ourDiff, goCmpDiff)

		//检查我们的差异结果包含关键差异
		keyDifferences := []string{
			"LastName", "Smith",
			"Age",
			"Addresses", "work",
			"Contacts", "john.smith@example.com", "fax",
			"Metadata", "loginCount", "settings", "notifications", "darkMode", "newField",
		}

		for _, key := range keyDifferences {
			if !strings.Contains(ourDiff, key) {
				t.Errorf("Our diff doesn't contain expected key difference: %q", key)
			}
		}

		//检查两个差异结果均非空
		if ourDiff == "" || goCmpDiff == "" {
			t.Errorf("Expected non-empty diffs, got ourDiff: %v, goCmpDiff: %v",
				ourDiff == "", goCmpDiff == "")
		}
	})

	//测试切片和映射
	// 切片按下标显示变化，map 按 key 显示增删改。
	t.Run("SlicesAndMaps", func(t *testing.T) {
		//测试切片
		a1 := []int{1, 2, 3, 4, 5}
		b1 := []int{1, 2, 6, 4, 7}

		ourDiff1 := Diff(a1, b1)
		goCmpDiff1 := cmp.Diff(a1, b1)

		t.Logf("Our diff (slices):\n%s\n\nGo-cmp diff (slices):\n%s", ourDiff1, goCmpDiff1)

		//检查我们的差异结果包含预期差异
		if !strings.Contains(ourDiff1, "3") || !strings.Contains(ourDiff1, "6") ||
			!strings.Contains(ourDiff1, "5") || !strings.Contains(ourDiff1, "7") {
			t.Errorf("Our diff doesn't contain all expected differences for slices")
		}

		//测试映射
		a2 := map[string]int{"a": 1, "b": 2, "c": 3}
		b2 := map[string]int{"a": 1, "b": 5, "d": 4}

		ourDiff2 := Diff(a2, b2)
		goCmpDiff2 := cmp.Diff(a2, b2)

		t.Logf("Our diff (maps):\n%s\n\nGo-cmp diff (maps):\n%s", ourDiff2, goCmpDiff2)

		//检查我们的差异结果包含预期差异
		if !strings.Contains(ourDiff2, "b") || !strings.Contains(ourDiff2, "5") ||
			!strings.Contains(ourDiff2, "c") || !strings.Contains(ourDiff2, "d") {
			t.Errorf("Our diff doesn't contain all expected differences for maps")
		}
	})

	//测试未导出字段
	// Diff 基于 JSON 序列化，未导出字段本就不会出现；与 go-cmp 配合
	// IgnoreUnexported 的结果保持一致。
	t.Run("UnexportedFields", func(t *testing.T) {
		type WithUnexported struct {
			Exported   int
			unexported int
		}

		a := WithUnexported{Exported: 1, unexported: 2}
		b := WithUnexported{Exported: 3, unexported: 4}

		ourDiff := Diff(a, b)
		//在 go-cmp 中使用 cmpopts.IgnoreUnexported 忽略未导出字段
		goCmpDiff := cmp.Diff(a, b, cmpopts.IgnoreUnexported(WithUnexported{}))

		t.Logf("Our diff (unexported):\n%s\n\nGo-cmp diff (unexported):\n%s", ourDiff, goCmpDiff)

		//检查我们的差异结果仅包含导出字段的差异
		if !strings.Contains(ourDiff, "Exported") || !strings.Contains(ourDiff, "1") || !strings.Contains(ourDiff, "3") {
			t.Errorf("Our diff doesn't contain the exported field difference")
		}
	})

	//测试嵌入结构体
	// 内嵌字段在 JSON 中会被提升，diff 输出应表现为普通字段。
	t.Run("EmbeddedStructs", func(t *testing.T) {
		type Embedded struct {
			Value int
		}

		type Container struct {
			Embedded
			Extra string
		}

		a := Container{Embedded: Embedded{Value: 1}, Extra: "a"}
		b := Container{Embedded: Embedded{Value: 2}, Extra: "b"}

		ourDiff := Diff(a, b)
		goCmpDiff := cmp.Diff(a, b)

		t.Logf("Our diff (embedded):\n%s\n\nGo-cmp diff (embedded):\n%s", ourDiff, goCmpDiff)

		//检查我们的差异结果包含容器字段的差异
		if !strings.Contains(ourDiff, "Extra") || !strings.Contains(ourDiff, "a") || !strings.Contains(ourDiff, "b") {
			t.Errorf("Our diff doesn't contain the container field difference")
		}
	})

	//测试同类型接口值
	// 同为 int 的 interface 值应直接显示数值差异。
	t.Run("InterfaceValues", func(t *testing.T) {
		type Container struct {
			Value interface{}
		}

		//测试接口中的同类型值
		c := Container{Value: 42}
		d := Container{Value: 43}

		ourDiff := Diff(c, d)
		goCmpDiff := cmp.Diff(c, d)

		t.Logf("Our diff (interface same type):\n%s\n\nGo-cmp diff (interface same type):\n%s", ourDiff, goCmpDiff)

		//检查我们的差异结果包含值差异
		if !strings.Contains(ourDiff, "42") || !strings.Contains(ourDiff, "43") {
			t.Errorf("Our diff doesn't contain the value difference for interface values of same type")
		}
	})

	//测试无法序列化为 JSON 的对象
	// 循环引用会让 json.Marshal 失败，Diff 必须退回 dump.Pretty 而不是
	// 返回空字符串或 panic。
	t.Run("UnmarshalableObjects", func(t *testing.T) {
		//测试无法序列化为 JSON 的循环引用
		type Node struct {
			Value int
			Next  *Node
		}

		//创建循环引用
		nodeA := &Node{Value: 1}
		nodeA.Next = nodeA //指向自身

		nodeB := &Node{Value: 2}
		nodeB.Next = nodeB //指向自身

		//此时应回退到 dump.Pretty
		circularDiff := Diff(nodeA, nodeB)

		t.Logf("Diff for circular references:\n%s", circularDiff)

		//验证差异结果包含这些值
		if !strings.Contains(circularDiff, "1") || !strings.Contains(circularDiff, "2") {
			t.Errorf("Diff doesn't contain expected value differences for circular references")
		}
	})
}
