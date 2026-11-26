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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"reflect"
	"text/template"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
)

// GoTemplatePrinter 用 Go template 格式化数据。
type GoTemplatePrinter struct {
	// 原始模板文本，出错时回显给用户便于排查。
	rawTemplate string
	template    *template.Template
}

// NewGoTemplatePrinter 解析 Go 模板并注册 CLI 特有函数：
// exists 用于安全探测字段，base64decode 便于直接查看 Secret 等 base64 内容。
func NewGoTemplatePrinter(tmpl []byte) (*GoTemplatePrinter, error) {
	t, err := template.New("output").
		Funcs(template.FuncMap{
			"exists":       exists,
			"base64decode": base64decode,
		}).
		Parse(string(tmpl))
	if err != nil {
		return nil, err
	}
	return &GoTemplatePrinter{
		rawTemplate: string(tmpl),
		template:    t,
	}, nil
}

// AllowMissingKeys 告诉模板引擎是否允许缺失的键。
func (p *GoTemplatePrinter) AllowMissingKeys(allow bool) {
	// default 模式把缺失键渲染为空值，error 模式直接失败，便于发现字段拼写错误。
	if allow {
		p.template.Option("missingkey=default")
	} else {
		p.template.Option("missingkey=error")
	}
}

// PrintObj 用 Go template 格式化对象。
func (p *GoTemplatePrinter) PrintObj(obj runtime.Object, w io.Writer) error {
	// 拒绝 internal 版本对象，避免输出缺少类型信息的内容。
	if InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj)).Type().PkgPath()) {
		return errors.New(InternalObjectPrinterErr)
	}

	var data []byte
	var err error
	// 统一转为通用 JSON map 再执行模板，模板里可用字段名而非结构化类型。
	data, err = json.Marshal(obj)
	if err != nil {
		return err
	}

	out := map[string]interface{}{}
	if err := json.Unmarshal(data, &out); err != nil {
		return err
	}
	if err = p.safeExecute(w, out); err != nil {
		// 这类问题输出到 stdout 比只写 stdin 更容易排查，
		// 因此除了返回错误，也把有用信息写到 writer。
		fmt.Fprintf(w, "Error executing template: %v. Printing more information for debugging the template:\n", err)
		fmt.Fprintf(w, "\ttemplate was:\n\t\t%v\n", p.rawTemplate)
		fmt.Fprintf(w, "\traw data was:\n\t\t%v\n", string(data))
		fmt.Fprintf(w, "\tobject given to template engine was:\n\t\t%+v\n\n", out)
		return fmt.Errorf("error executing template %q: %v", p.rawTemplate, err)
	}
	return nil
}

// safeExecute 执行模板，并在模板引擎 panic 时转成错误返回。
// 中文：模板可能触发引擎内部 panic，包装一层 recover 转成错误，避免整个进程崩溃。
func (p *GoTemplatePrinter) safeExecute(w io.Writer, obj interface{}) error {
	var panicErr error
	// 这里用了两层匿名函数，见谅；defer 修改返回值的写法虽然可行，但更晦涩。
	retErr := func() error {
		defer func() {
			if x := recover(); x != nil {
				panicErr = fmt.Errorf("caught panic: %+v", x)
			}
		}()
		return p.template.Execute(w, obj)
	}()
	if panicErr != nil {
		return panicErr
	}
	return retErr
}

// base64decode 是暴露给模板的辅助函数，用于把 Secret 的 data 字段解成明文，省去管道外处理。
func base64decode(v string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return "", fmt.Errorf("base64 decode failed: %v", err)
	}
	return string(data), nil
}
