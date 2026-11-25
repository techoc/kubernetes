/*
Copyright 2019 The Kubernetes Authors.

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

package get

import (
	"fmt"
	"io"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/klog/v2"
)

// TablePrinter 先将 table 对象解码为类型化对象，再委托给另一个打印器。
// 非 table 类型则直接透传
type TablePrinter struct {
	Delegate printers.ResourcePrinter
}

func (t *TablePrinter) PrintObj(obj runtime.Object, writer io.Writer) error {
	table, err := decodeIntoTable(obj)
	if err == nil {
		return t.Delegate.PrintObj(table, writer)
	}
	// 如果无法将服务端的响应解码为 v1beta1.Table，
	// 则退回到客户端打印，使用服务端返回的现有信息。
	klog.V(2).Infof("Unable to decode server response into a Table. Falling back to hardcoded types: %v", err)
	return t.Delegate.PrintObj(obj, writer)
}

var recognizedTableVersions = map[schema.GroupVersionKind]bool{
	metav1beta1.SchemeGroupVersion.WithKind("Table"): true,
	metav1.SchemeGroupVersion.WithKind("Table"):      true,
}

// 断言类型相同，因为这两种类型都会被解码为 metav1.Table
var _ metav1.Table = metav1beta1.Table{}
var _ metav1beta1.Table = metav1.Table{}

func decodeIntoTable(obj runtime.Object) (runtime.Object, error) {
	event, isEvent := obj.(*metav1.WatchEvent)
	if isEvent {
		obj = event.Object.Object
	}

	if !recognizedTableVersions[obj.GetObjectKind().GroupVersionKind()] {
		return nil, fmt.Errorf("attempt to decode non-Table object")
	}

	unstr, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("attempt to decode non-Unstructured object")
	}
	table := &metav1.Table{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstr.Object, table); err != nil {
		return nil, err
	}

	for i := range table.Rows {
		row := &table.Rows[i]
		if row.Object.Raw == nil || row.Object.Object != nil {
			continue
		}
		converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, row.Object.Raw)
		if err != nil {
			return nil, err
		}
		row.Object.Object = converted
	}

	if isEvent {
		event.Object.Object = table
		return event, nil
	}
	return table, nil
}
