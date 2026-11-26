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

package printers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// JSONPrinter 把对象输出为 JSON。
type JSONPrinter struct{}

// PrintObj 直接把对象写入 Writer。
func (p *JSONPrinter) PrintObj(obj runtime.Object, w io.Writer) error {
	// 用 reflect.Indirect 取出指针背后的实际值，才能拿到对象的包路径。
	// 这里可以无条件使用，因为 runtime.Object 按约定都是指针。
	// 中文：拒绝打印 internal 版本对象，它们缺少 apiVersion/kind 且不该出现在 CLI 输出中。
	if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj)).Type().PkgPath()) {
		return errors.New(InternalObjectPrinterErr)
	}

	switch obj := obj.(type) {
	case *metav1.WatchEvent:
		// WatchEvent 包了一层 Object，必须校验内层对象，否则可绕过上面的 internal 检查。
		if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj.Object.Object)).Type().PkgPath()) {
			return errors.New(InternalObjectPrinterErr)
		}
		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		if err != nil {
			return err
		}
		// 每个对象后补换行，保证多文档流在终端里可读。
		_, err = w.Write([]byte{'\n'})
		return err
	case *runtime.Unknown:
		// 原始字节已经是 JSON，只做缩进，避免先反序列化再序列化的损耗与精度问题。
		var buf bytes.Buffer
		err := json.Indent(&buf, obj.Raw, "", "    ")
		if err != nil {
			return err
		}
		buf.WriteRune('\n')
		_, err = buf.WriteTo(w)
		return err
	}

	if obj.GetObjectKind().GroupVersionKind().Empty() {
		// 缺少类型元信息会输出不可再解析的 JSON，提前报错并给出修复建议。
		return fmt.Errorf("missing apiVersion or kind; try GetObjectKind().SetGroupVersionKind() if you know the type")
	}

	// 固定 4 空格缩进，与 kubectl 历史输出保持一致。
	data, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}
