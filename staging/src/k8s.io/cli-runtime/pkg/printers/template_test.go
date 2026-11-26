/*
Copyright 2018 The Kubernetes Authors.

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
	"strings"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TestTemplate 覆盖 GoTemplatePrinter 的模板函数与错误路径：
// base64decode 能解出 Secret 数据，遇到非法 base64 时给出可读错误，
// 并且 eq 对数字类型做比较时不应把 int 当成不可比较类型而报错。
func TestTemplate(t *testing.T) {
	testCase := []struct {
		name      string
		template  string
		obj       runtime.Object
		expectOut string
		expectErr func(error) (string, bool)
	}{
		{
			name:     "support base64 decoding of secret data",
			template: "{{ .data.username | base64decode }}",
			obj: &v1.Secret{
				Data: map[string][]byte{
					"username": []byte("hunter"),
				},
			},
			expectOut: "hunter",
		},
		{
			name:     "test error path for base64 decoding",
			template: "{{ .data.username | base64decode }}",
			obj:      &badlyMarshaledSecret{},
			expectErr: func(err error) (string, bool) {
				matched := strings.Contains(err.Error(), "base64 decode")
				return "a base64 decode error", matched
			},
		},
		{
			name:     "template 'eq' should not throw error for numbers",
			template: "{{ eq .count 1}}",
			obj: &v1.Event{
				Count: 1,
			},
			expectOut: "true",
		},
	}
	for _, test := range testCase {
		t.Run(test.name, func(t *testing.T) {
			buffer := &bytes.Buffer{}

			// 模板既可能在构建阶段失败（语法/函数错误），也可能在打印阶段失败
			// （数据与函数组合出错），两条路径都要按 expectErr 校验。
			p, err := NewGoTemplatePrinter([]byte(test.template))
			if err != nil {
				if test.expectErr == nil {
					t.Errorf("[%s]expected success but got:\n %v\n", test.name, err)
					return
				}
				if expected, ok := test.expectErr(err); !ok {
					t.Errorf("[%s]expect:\n %v\n but got:\n %v\n", test.name, expected, err)
				}
				return
			}

			err = p.PrintObj(test.obj, buffer)
			if err != nil {
				if test.expectErr == nil {
					t.Errorf("[%s]expected success but got:\n %v\n", test.name, err)
					return
				}
				if expected, ok := test.expectErr(err); !ok {
					t.Errorf("[%s]expect:\n %v\n but got:\n %v\n", test.name, expected, err)
				}
				return
			}

			if test.expectErr != nil {
				t.Errorf("[%s]expect:\n error\n but got:\n no error\n", test.name)
				return
			}

			if test.expectOut != buffer.String() {
				t.Errorf("[%s]expect:\n %v\n but got:\n %v\n", test.name, test.expectOut, buffer.String())
			}
		})
	}
}

// badlyMarshaledSecret 故意输出非法 base64，用于验证模板层能捕获解码错误。
type badlyMarshaledSecret struct {
	v1.Secret
}

// MarshalJSON 返回 data.username 不是合法 base64 的 Secret JSON。
func (a badlyMarshaledSecret) MarshalJSON() ([]byte, error) {
	return []byte(`{"apiVersion":"v1","data":{"username":"--THIS IS NOT BASE64--"},"kind":"Secret"}`), nil
}

// TestTemplateStrings 验证 exists 函数在嵌套字段上的语义，模板取自 update.sh：
// 只有当容器名为 foo 且其 state.running 存在时才输出 true。
// 注意 "foo" 没有 state.running 的用例（fooExists、barValid）结果仍为 false，
// 说明 exists 检查的是完整路径而不仅是容器名。
func TestTemplateStrings(t *testing.T) {
	// 同时测试 "exists" 函数和 update.sh 中的模板。
	table := map[string]struct {
		pod    v1.Pod
		expect string
	}{
		"nilInfo":   {v1.Pod{}, "false"},
		"emptyInfo": {v1.Pod{Status: v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{}}}, "false"},
		"fooExists": {
			v1.Pod{
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "foo",
						},
					},
				},
			},
			"false",
		},
		"barExists": {
			v1.Pod{
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "bar",
						},
					},
				},
			},
			"false",
		},
		"bothExist": {
			v1.Pod{
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "foo",
						},
						{
							Name: "bar",
						},
					},
				},
			},
			"false",
		},
		"barValid": {
			v1.Pod{
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "foo",
						},
						{
							Name: "bar",
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{
									StartedAt: metav1.Time{},
								},
							},
						},
					},
				},
			},
			"false",
		},
		"bothValid": {
			v1.Pod{
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name: "foo",
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{
									StartedAt: metav1.Time{},
								},
							},
						},
						{
							Name: "bar",
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{
									StartedAt: metav1.Time{},
								},
							},
						},
					},
				},
			},
			"true",
		},
	}
	// 本测试用于验证下面的模板可用。
	tmpl := `{{if (exists . "status" "containerStatuses")}}{{range .status.containerStatuses}}{{if (and (eq .name "foo") (exists . "state" "running"))}}true{{end}}{{end}}{{end}}`
	printer, err := NewGoTemplatePrinter([]byte(tmpl))
	if err != nil {
		t.Fatalf("tmpl fail: %v", err)
	}

	for name, item := range table {
		buffer := &bytes.Buffer{}
		err = printer.PrintObj(&item.pod, buffer)
		if err != nil {
			t.Errorf("%v: unexpected err: %v", name, err)
			continue
		}
		// 模板未输出任何内容时语义等同于 false，这里统一归类以便表驱动比较。
		actual := buffer.String()
		if len(actual) == 0 {
			actual = "false"
		}
		if e := item.expect; e != actual {
			t.Errorf("%v: expected %v, got %v", name, e, actual)
		}
	}
}

// TestTemplatePanic 验证模板在访问 nil 指针链时会返回错误而不是让进程崩溃，
// 并且仍会向输出写入调试信息（帮助用户定位模板中出错的位置）。
func TestTemplatePanic(t *testing.T) {
	tmpl := `{{and ((index .currentState.info "foo").state.running.startedAt) .currentState.info.net.state.running.startedAt}}`
	printer, err := NewGoTemplatePrinter([]byte(tmpl))
	if err != nil {
		t.Fatalf("tmpl fail: %v", err)
	}
	buffer := &bytes.Buffer{}
	err = printer.PrintObj(&v1.Pod{}, buffer)
	if err == nil {
		t.Fatalf("expected that template to crash")
	}
	// 报错之外还应有调试输出，否则用户只知道失败却看不到出错路径。
	if buffer.String() == "" {
		t.Errorf("no debugging info was printed")
	}
}

// TestTemplateSuccess 验证 {.name} 这类简单模板对所有对象都能成功执行；
// PodList/Endpoints 的 .name 为空，模板把它当缺失字段处理，不应转型为错误。
func TestTemplateSuccess(t *testing.T) {

	templatePrinter, err := NewGoTemplatePrinter([]byte("{{.name}}"))
	if err != nil {
		t.Fatal(err)
	}

	// 这些资源应能被模板 printer 成功处理。
	om := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name} }
	objects := []runtime.Object{
		&v1.Pod{ObjectMeta: om("pod")},
		&v1.PodList{},
		&v1.PodList{Items: []v1.Pod{{}}},
		&v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Addresses: []v1.EndpointAddress{{IP: "127.0.0.1"}, {IP: "localhost"}},
				Ports:     []v1.EndpointPort{{Port: 8080}},
			}}},
	}

	for _, obj := range objects {
		b := &bytes.Buffer{}
		if err := templatePrinter.PrintObj(obj, b); err != nil {
			t.Errorf("Unexpected template error: %v", err)
		}
	}
}

// TestTemplateErrors 使用 {len .items} 验证模板字段不适用时会返回错误，
// 且不会 panic；这是 CLI 把模板错误反馈给用户而非直接崩溃的保障。
func TestTemplateErrors(t *testing.T) {

	templatePrinter, err := NewGoTemplatePrinter([]byte("{{len .items}}"))
	if err != nil {
		t.Fatal(err)
	}

	// 这些资源应让模板 printer 失败。
	om := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name} }
	objects := []runtime.Object{
		&v1.Pod{ObjectMeta: om("pod")},
		&v1.PodList{},
		&v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Addresses: []v1.EndpointAddress{{IP: "127.0.0.1"}, {IP: "localhost"}},
				Ports:     []v1.EndpointPort{{Port: 8080}},
			}}},
	}

	for _, obj := range objects {
		b := &bytes.Buffer{}
		if err := templatePrinter.PrintObj(obj, b); err == nil {
			t.Errorf("Expected template printer error; received none")
		}
	}
}
