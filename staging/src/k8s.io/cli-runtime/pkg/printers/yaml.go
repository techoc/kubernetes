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

package printers

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"

	"sigs.k8s.io/yaml"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// YAMLPrinter is an implementation of ResourcePrinter which outputs an object as YAML.
// The input object is assumed to be in the internal version of an API and is converted
// to the given version first.
// If PrintObj() is called multiple times, objects are separated with a '---' separator.
//
// printCount 用于在多次调用之间维持分隔符状态；使用 atomic 是为了让同一 printer
// 在并发打印多个对象时也不会产生错乱的分隔符。
type YAMLPrinter struct {
	printCount int64
}

// PrintObj prints the data as YAML.
func (p *YAMLPrinter) PrintObj(obj runtime.Object, w io.Writer) error {
	// we use reflect.Indirect here in order to obtain the actual value from a pointer.
	// we need an actual value in order to retrieve the package path for an object.
	// using reflect.Indirect indiscriminately is valid here, as all runtime.Objects are supposed to be pointers.
	//
	// 禁止直接打印 internal 类型，否则会泄露内部 API 结构到用户输出。
	if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj)).Type().PkgPath()) {
		return errors.New(InternalObjectPrinterErr)
	}

	// 仅对第二个及之后的文档写分隔符，保证首个对象前不带多余的 "---"。
	count := atomic.AddInt64(&p.printCount, 1)
	if count > 1 {
		if _, err := w.Write([]byte("---\n")); err != nil {
			return err
		}
	}

	switch obj := obj.(type) {
	case *metav1.WatchEvent:
		// WatchEvent 的内嵌 Object 仍需单独做 internal 校验（见上）。
		if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj.Object.Object)).Type().PkgPath()) {
			return errors.New(InternalObjectPrinterErr)
		}
		data, err := yaml.Marshal(obj)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	case *runtime.Unknown:
		// Unknown 只持有原始 JSON 字节，直接转 YAML 可避免一次编解码往返。
		data, err := yaml.JSONToYAML(obj.Raw)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}

	// 缺少 apiVersion/kind 的 YAML 无法再次被 kubectl 读取，因此显式报错并提供修复提示。
	if obj.GetObjectKind().GroupVersionKind().Empty() {
		return fmt.Errorf("missing apiVersion or kind; try GetObjectKind().SetGroupVersionKind() if you know the type")
	}

	output, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(w, string(output))
	return err
}
