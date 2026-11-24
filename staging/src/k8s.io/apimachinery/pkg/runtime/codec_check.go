/*
Copyright 2016 The Kubernetes Authors.

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

package runtime

import (
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
)

// CheckCodec 验证编解码器能否编码 internalType、识别并解码 externalTypes 中的每种外部类型，
// 以及能否将外部数据解码到 internalObject 中。此检查假定使用 JSON 序列化，
// DecodeInto 会修改传入的 internalObject；当前尚未验证编码时是否选择了预期的外部版本。
func CheckCodec(c Codec, internalType Object, externalTypes ...schema.GroupVersionKind) error {
	// 编码内部对象会同时经过版本选择和类型解析；一旦失败立即返回，便于定位首个问题。
	if _, err := Encode(c, internalType); err != nil {
		return fmt.Errorf("internal type not encodable: %v", err)
	}
	for _, et := range externalTypes {
		// 构造只包含 TypeMeta 的最小文档，重点验证 Decode 能否根据外部类型信息找到内部类型。
		typeMeta := TypeMeta{
			Kind:       et.Kind,
			APIVersion: et.GroupVersion().String(),
		}
		exBytes, err := json.Marshal(&typeMeta)
		if err != nil {
			return err
		}
		obj, err := Decode(c, exBytes)
		if err != nil {
			return fmt.Errorf("external type %s not interpretable: %v", et, err)
		}
		// 解码结果必须与预期内部类型具有完全相同的 Go 反射类型，不能只是字段相似。
		if reflect.TypeOf(obj) != reflect.TypeOf(internalType) {
			return fmt.Errorf("decode of external type %s produced: %#v", et, obj)
		}
		// DecodeInto additionally verifies an explicit conversion path exists.
		if err = DecodeInto(c, exBytes, internalType); err != nil {
			return fmt.Errorf("external type %s not convertible to internal type: %v", et, err)
		}
	}
	return nil
}
