/*
Copyright 2015 The Kubernetes Authors.

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

package errors

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
)

// MessageCountMap 记录每种错误消息出现的次数。
// Deprecated: k8s.io 代码库已不再使用，请改用 `errors.Join`。
type MessageCountMap map[string]int

// Aggregate 表示包含多个错误的对象，但这些错误不一定具有单一的语义。
// 聚合结果可配合 `errors.Is()` 检查是否包含特定错误类型。
// 不支持 Errors.As()，因为调用方通常只关心可能匹配多个错误中的某一个。
type Aggregate interface {
	error
	Errors() []error
	Is(error) bool
}

// NewAggregate 把错误切片转换为 Aggregate（其本身也实现 error 接口）。
// 切片为空时返回 nil；同时会过滤掉 nil 元素，避免调用 Error() 时空指针 panic。
func NewAggregate(errlist []error) Aggregate {
	if len(errlist) == 0 {
		return nil
	}
	// 输入列表中可能包含 nil，先过滤
	var errs []error
	for _, e := range errlist {
		if e != nil {
			errs = append(errs, e)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return aggregate(errs)
}

// aggregate 实现 error 和 Errors 接口。将其保持为非导出类型，可以避免调用方构造
// 空聚合：空聚合本身不代表错误，却仍然满足 error 接口。
type aggregate []error

// Error 实现 error 接口：单条错误直接返回其文本；
// 多条错误去重后用 ", " 连接，多于一条时外层加方括号。
func (agg aggregate) Error() string {
	if len(agg) == 0 {
		// 实际上不应发生
		return ""
	}
	if len(agg) == 1 {
		return agg[0].Error()
	}
	seenerrs := sets.NewString()
	result := ""
	agg.visit(func(err error) bool {
		msg := err.Error()
		if seenerrs.Has(msg) {
			return false
		}
		seenerrs.Insert(msg)
		if len(seenerrs) > 1 {
			result += ", "
		}
		result += msg
		return false
	})
	if len(seenerrs) == 1 {
		return result
	}
	return "[" + result + "]"
}

// Is 报告聚合中是否存在与 target 匹配的错误（含嵌套聚合）。
func (agg aggregate) Is(target error) bool {
	return agg.visit(func(err error) bool {
		return errors.Is(err, target)
	})
}

// visit 递归遍历聚合中的错误，遇到第一个 f 返回 true 的错误即返回 true。
// 对嵌套的 aggregate 与实现 Aggregate 接口的错误都会展开。
func (agg aggregate) visit(f func(err error) bool) bool {
	for _, err := range agg {
		switch err := err.(type) {
		case aggregate:
			if match := err.visit(f); match {
				return match
			}
		case Aggregate:
			for _, nestedErr := range err.Errors() {
				if match := f(nestedErr); match {
					return match
				}
			}
		default:
			if match := f(err); match {
				return match
			}
		}
	}

	return false
}

// Errors 返回聚合中的所有错误（Aggregate 接口的一部分）。
func (agg aggregate) Errors() []error {
	return []error(agg)
}

// Matcher 用于匹配错误：匹配时返回 true。
type Matcher func(error) bool

// FilterOut 从输入错误中移除所有被任一 matcher 匹配的错误。
// 输入是单个错误时只测试它；实现 Aggregate 接口时递归处理其中的错误列表。
// 例如可用于从错误列表中剔除已知可接受的错误（如 io.EOF 或 os.PathNotFound）。
func FilterOut(err error, fns ...Matcher) error {
	if err == nil {
		return nil
	}
	if agg, ok := err.(Aggregate); ok {
		return NewAggregate(filterErrors(agg.Errors(), fns...))
	}
	if !matchesError(err, fns...) {
		return err
	}
	return nil
}

// matchesError 在任一 Matcher 返回 true 时返回 true。
func matchesError(err error, fns ...Matcher) bool {
	for _, fn := range fns {
		if fn(err) {
			return true
		}
	}
	return false
}

// filterErrors 返回所有不被任何 fn 匹配的错误（若列表含嵌套 Aggregate 则递归展开）。
// 没有剩余错误时返回 nil；结果切片会顺带把嵌套切片展平。
func filterErrors(list []error, fns ...Matcher) []error {
	result := []error{}
	for _, err := range list {
		r := FilterOut(err, fns...)
		if r != nil {
			result = append(result, r)
		}
	}
	return result
}

// Flatten 递归地把任意嵌套的 Aggregate 展平为单个 Aggregate。
func Flatten(agg Aggregate) Aggregate {
	result := []error{}
	if agg == nil {
		return nil
	}
	for _, err := range agg.Errors() {
		if a, ok := err.(Aggregate); ok {
			r := Flatten(a)
			if r != nil {
				result = append(result, r.Errors()...)
			}
		} else {
			if err != nil {
				result = append(result, err)
			}
		}
	}
	return NewAggregate(result)
}

// CreateAggregateFromMessageCountMap 把 MessageCountMap 转换为 Aggregate；
// 出现次数大于 1 的错误信息会附加 "(repeated N times)" 后缀。
// Deprecated: k8s.io 代码库已不再使用，请改用 `errors.Join`。
func CreateAggregateFromMessageCountMap(m MessageCountMap) Aggregate {
	if m == nil {
		return nil
	}
	result := make([]error, 0, len(m))
	for errStr, count := range m {
		var countStr string
		if count > 1 {
			countStr = fmt.Sprintf(" (repeated %v times)", count)
		}
		result = append(result, fmt.Errorf("%v%v", errStr, countStr))
	}
	return NewAggregate(result)
}

// Reduce 返回 err；若 err 是只含一项的 Aggregate，则返回该项；
// 若是空 Aggregate，则返回 nil。
func Reduce(err error) error {
	if agg, ok := err.(Aggregate); ok && err != nil {
		switch len(agg.Errors()) {
		case 1:
			return agg.Errors()[0]
		case 0:
			return nil
		}
	}
	return err
}

// AggregateGoroutines 并行运行给定函数，把所有非 nil 错误收集到返回的 Aggregate 中；
// 所有函数都成功时返回 nil。
func AggregateGoroutines(funcs ...func() error) Aggregate {
	// 带缓冲通道，确保 goroutine 不会因无人接收而泄漏。
	errChan := make(chan error, len(funcs))
	for _, f := range funcs {
		go func(f func() error) { errChan <- f() }(f)
	}
	errs := make([]error, 0)
	for i := 0; i < cap(errChan); i++ {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}
	return NewAggregate(errs)
}

// ErrPreconditionViolated 在前置条件被违反时返回。
var ErrPreconditionViolated = errors.New("precondition is violated")
