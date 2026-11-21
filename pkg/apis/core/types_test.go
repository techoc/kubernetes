/*
Copyright 2021 The Kubernetes Authors.

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

package core

import (
	"reflect"
	"testing"
)

// TestEphemeralContainer 通过反射确保 Container 与 EphemeralContainerCommon 的字段标签保持一致。
// 两者的字段必须同步，否则临时容器在序列化/反序列化时会出现字段不匹配的问题。
func TestEphemeralContainer(t *testing.T) {
	ephemeralType := reflect.TypeOf(EphemeralContainerCommon{})
	containerType := reflect.TypeOf(Container{})

	ephemeralFields := ephemeralType.NumField()
	containerFields := containerType.NumField()
	if containerFields != ephemeralFields {
		t.Fatalf("%v has %d fields, %v has %d fields", ephemeralType, ephemeralFields, containerType, containerFields)
	}
	for i := 0; i < ephemeralFields; i++ {
		ephemeralField := ephemeralType.Field(i)
		containerField := containerType.Field(i)
		if !reflect.DeepEqual(ephemeralField, containerField) {
			t.Errorf("field %v differs:\n\t%#v\n\t%#v", ephemeralField.Name, ephemeralField, containerField)
		}
	}
}
