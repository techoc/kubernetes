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

// package kyaml 提供把 Kubernetes 对象格式化为 KYAML 的 printer。
// KYAML 是 YAML 的严格子集，力求表达明确、无歧义；它本身仍是 YAML。
package printers

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml/kyaml"
)

// KYAMLPrinter 把数据格式化为 KYAML 这一特定 YAML 方言。
// KYAML 介于 YAML 与 JSON 之间，但仍是 YAML 的严格子集，任何 YAML 解析器都能读取。
// 它刻意追求明确、无歧义，避开依赖空白的表达方式。
type KYAMLPrinter struct {
	// 复用 kyaml 库的编码器，保证输出与生态内其他 KYAML 工具一致。
	encoder kyaml.Encoder
}

// PrintObj 把数据以 KYAML 格式写入指定 writer。
func (p *KYAMLPrinter) PrintObj(obj runtime.Object, w io.Writer) error {
	// 用 reflect.Indirect 取出指针背后的实际值，才能拿到对象的包路径。
	// 这里可以无条件使用，因为 runtime.Object 按约定都是指针。
	// 中文：拦截 internal 版本对象，防止其未经转换直接输出。
	if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj)).Type().PkgPath()) {
		return errors.New(InternalObjectPrinterErr)
	}

	switch obj := obj.(type) {
	case *metav1.WatchEvent:
		// WatchEvent 需校验内层对象，否则可绕过 internal 检查。
		if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj.Object.Object)).Type().PkgPath()) {
			return errors.New(InternalObjectPrinterErr)
		}
		return p.encoder.FromObject(obj, w)

	case *runtime.Unknown:
		// 原始字节视为 YAML，直接转码，避免反序列化再编码。
		return p.encoder.FromYAML(bytes.NewReader(obj.Raw), w)
	}

	if obj.GetObjectKind().GroupVersionKind().Empty() {
		// 没有类型信息就无法生成可读、可再解析的文档。
		return fmt.Errorf("missing apiVersion or kind; try GetObjectKind().SetGroupVersionKind() if you know the type")
	}

	return p.encoder.FromObject(obj, w)
}
