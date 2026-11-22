/*
Copyright 2024 The Kubernetes Authors.

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

package main

import (
	"fmt"
	"sort"

	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// linter 持有 lint 过程的状态。
// 它包含已 lint 过的类型集合、lint 规则列表，以及过程中产生的错误。
type linter struct {
	linted map[*types.Type]bool
	rules  []lintRule
	// lintErrors 是按类型分组的全部 lint 错误。
	lintErrors map[*types.Type][]error
}

// lintRule 校验一组注释。
// container 是包含被检查元素的类型（例如检查字段时即所属结构体）；
// 若元素是顶层（如类型定义）则可能为 nil。
// t 是被检查元素的类型（如字段类型或类型本身）。
// 注释非法时返回错误消息字符串；lint 过程本身出错时返回 error。
type lintRule func(container *types.Type, t *types.Type, tags []codetags.Tag) (string, error)

// AddError 记录一条 lint 错误；field 非空时前缀字段名，便于定位。
func (l *linter) AddError(t *types.Type, field, msg string) {
	var err error
	if field == "" {
		err = fmt.Errorf("%s", msg)
	} else {
		err = fmt.Errorf("field %s: %s", field, msg)
	}
	l.lintErrors[t] = append(l.lintErrors[t], err)
}

// newLinter 构造 linter。未传规则通常意味着配置错误，因此告警。
func newLinter(rules ...lintRule) *linter {
	if len(rules) == 0 {
		klog.Errorf("rules are not passed to the linter")
	}
	return &linter{
		linted:     make(map[*types.Type]bool),
		rules:      rules,
		lintErrors: map[*types.Type][]error{},
	}
}

// lintType 递归检查类型及其所有组成部分（字段、别名底层类型、元素、map 键值）。
// 递归是必要的：标记可能出现在任意嵌套层级，而生成器会为这些层级生成代码。
func (l *linter) lintType(t *types.Type) error {
	// 同一类型可能被多个父类型引用，去重避免重复报告。
	if _, ok := l.linted[t]; ok {
		return nil
	}
	l.linted[t] = true

	if t.CommentLines != nil {
		extracted := codetags.Extract("+", t.CommentLines)
		// 显式的 nolint 标记用于无法用规则表达的特殊场景，直接跳过整个类型。
		if _, ok := extracted["k8s:validation-gen-nolint"]; ok {
			return nil
		}
		klog.V(5).Infof("linting type %s", t.Name.String())
		lintErrs, err := l.lintComments(t, t, t.CommentLines)
		if err != nil {
			return err
		}
		for _, lintErr := range lintErrs {
			l.AddError(t, "", lintErr)
		}
	}
	switch t.Kind {
	case types.Alias:
		// 递归检查别名的底层类型。
		if err := l.lintType(t.Underlying); err != nil {
			return err
		}
	case types.Struct:
		// 递归检查结构体的每个成员。
		for _, member := range t.Members {
			klog.V(5).Infof("linting comments for field %s of type %s", member.String(), t.Name.String())
			lintErrs, err := l.lintComments(t, member.Type, member.CommentLines)
			if err != nil {
				return err
			}
			for _, lintErr := range lintErrs {
				l.AddError(t, member.Name, lintErr)
			}
			if err := l.lintType(member.Type); err != nil {
				return err
			}
		}
	case types.Slice, types.Array, types.Pointer:
		// 递归检查切片/数组/指针的元素类型。
		if err := l.lintType(t.Elem); err != nil {
			return err
		}
	case types.Map:
		// 递归检查 map 的键类型与值类型。
		if err := l.lintType(t.Key); err != nil {
			return err
		}
		if err := l.lintType(t.Elem); err != nil {
			return err
		}
	}
	return nil
}

// lintComments 对一组注释运行全部已注册规则。
func (l *linter) lintComments(container *types.Type, t *types.Type, comments []string) ([]string, error) {
	var lintErrs []string
	var tags []codetags.Tag

	extracted := codetags.Extract("+", comments)
	// 按键名排序后遍历，使同一条注释的规则执行与报错顺序稳定。
	keys := make([]string, 0, len(extracted))
	for k := range extracted {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, tagName := range keys {
		lines := extracted[tagName]
		t, err := codetags.ParseAll(lines)
		if err != nil {
			// 解析失败说明不是合法标记，交由其他检查（或忽略），此处不报错。
			continue
		}
		tags = append(tags, t...)
	}

	for _, rule := range l.rules {
		if msg, err := rule(container, t, tags); err != nil {
			return nil, err
		} else if msg != "" {
			lintErrs = append(lintErrs, msg)
		}
	}

	return lintErrs, nil
}
