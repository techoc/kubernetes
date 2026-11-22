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

package v1

import (
	"reflect"
	"strings"
	"testing"
)

// Test_NetworkPolicyRemovedFieldProtobufNumberReservation 校验已删除字段占用的 protobuf 字段号未被复用。
// 任何情况下都不要删除此测试：它防止已废弃的字段号被其他字段意外重用，破坏兼容性。
func Test_NetworkPolicyRemovedFieldProtobufNumberReservation(t *testing.T) {
	obj := reflect.ValueOf(NetworkPolicy{}).Type()
	for i := 0; i < obj.NumField(); i++ {
		f := obj.Field(i)
		protobufSpec := f.Tag.Get("protobuf")
		if protobufSpec == "" {
			continue
		}

		protobufNum := strings.Split(protobufSpec, ",")[1]
		if protobufNum == "3" {
			t.Errorf("protobuf 3 in NetworkPolicy is reserved for removed status field")
		}
	}
}
