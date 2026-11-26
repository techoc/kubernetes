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

package printers

import (
	"bytes"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestPrinters 验证 JSONPathPrinter 对两类输入的行为：
// 单对象（Pod）可正常取值；List 与 Endpoints 因为顶层是列表/嵌套结构，
// 使用 {.metadata.name} 取不到值，必须报错。
// 后半段在启用 JSONOutput 后再跑一遍，确保输出模式切换不影响错误语义。
func TestPrinters(t *testing.T) {
	om := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name} }

	jsonpathPrinter, err := NewJSONPathPrinter("{.metadata.name}")
	if err != nil {
		t.Fatal(err)
	}

	objects := map[string]runtime.Object{
		"pod":             &v1.Pod{ObjectMeta: om("pod")},
		"emptyPodList":    &v1.PodList{},
		"nonEmptyPodList": &v1.PodList{Items: []v1.Pod{{}}},
		"endpoints": &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Addresses: []v1.EndpointAddress{{IP: "127.0.0.1"}, {IP: "localhost"}},
				Ports:     []v1.EndpointPort{{Port: 8080}},
			}}},
	}

	// 以下字符串表示应产生错误的对象。
	expectedErrors := sets.New[string]("emptyPodList", "nonEmptyPodList", "endpoints")

	for oName, obj := range objects {
		b := &bytes.Buffer{}
		if err := jsonpathPrinter.PrintObj(obj, b); err != nil {
			if expectedErrors.Has(oName) {
				// 期望的错误。
				continue
			}
			t.Errorf("JSONPathPrinter error object '%v'; error: '%v'", oName, err)
		}
	}

	// 开启 JSONOutput 后重跑测试。
	jsonpathPrinter.EnableJSONOutput(true)

	for oName, obj := range objects {
		b := &bytes.Buffer{}
		if err := jsonpathPrinter.PrintObj(obj, b); err != nil {
			if expectedErrors.Has(oName) {
				// 期望的错误。
				continue
			}
			t.Errorf("JSONPathPrinter jsonOutput error object '%v'; error: '%v'", oName, err)
		}
	}
}
