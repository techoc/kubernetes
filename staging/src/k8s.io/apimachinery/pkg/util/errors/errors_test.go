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
	"reflect"
	"sort"
	"testing"
)

// TestEmptyAggregate 验证空切片（以及直接构造的空 aggregate）不产生错误对象，
// 其 Error()/Errors() 分别返回空串与空切片。
func TestEmptyAggregate(t *testing.T) {
	var slice []error
	var agg Aggregate
	var err error

	agg = NewAggregate(slice)
	if agg != nil {
		t.Errorf("expected nil, got %#v", agg)
	}
	err = NewAggregate(slice)
	if err != nil {
		t.Errorf("expected nil, got %#v", err)
	}

	//这种情况通常不会发生，但仍需测试以防边界遗漏。
	agg = aggregate(slice) //空聚合
	if s := agg.Error(); s != "" {
		t.Errorf("expected empty string, got %q", s)
	}
	if s := agg.Errors(); len(s) != 0 {
		t.Errorf("expected empty slice, got %#v", s)
	}
	err = agg.(error)
	if s := err.Error(); s != "" {
		t.Errorf("expected empty string, got %q", s)
	}
}

// TestAggregateWithNil 验证 nil 元素会被过滤：全 nil 返回 nil，混合时只保留非 nil 错误。
func TestAggregateWithNil(t *testing.T) {
	var slice []error
	slice = []error{nil}
	var agg Aggregate
	var err error

	agg = NewAggregate(slice)
	if agg != nil {
		t.Errorf("expected nil, got %#v", agg)
	}
	err = NewAggregate(slice)
	if err != nil {
		t.Errorf("expected nil, got %#v", err)
	}

	//追加一个非 nil 错误
	slice = append(slice, fmt.Errorf("err"))
	agg = NewAggregate(slice)
	if agg == nil {
		t.Errorf("expected non-nil")
	}
	if s := agg.Error(); s != "err" {
		t.Errorf("expected 'err', got %q", s)
	}
	if s := agg.Errors(); len(s) != 1 {
		t.Errorf("expected one-element slice, got %#v", s)
	}
	if s := agg.Errors()[0].Error(); s != "err" {
		t.Errorf("expected 'err', got %q", s)
	}

	err = agg.(error)
	if err == nil {
		t.Errorf("expected non-nil")
	}
	if s := err.Error(); s != "err" {
		t.Errorf("expected 'err', got %q", s)
	}
}

// TestSingularAggregate 验证只有一个错误时 Error() 直接返回该错误的文本，不加方括号。
func TestSingularAggregate(t *testing.T) {
	var slice = []error{fmt.Errorf("err")}
	var agg Aggregate
	var err error

	agg = NewAggregate(slice)
	if agg == nil {
		t.Errorf("expected non-nil")
	}
	if s := agg.Error(); s != "err" {
		t.Errorf("expected 'err', got %q", s)
	}
	if s := agg.Errors(); len(s) != 1 {
		t.Errorf("expected one-element slice, got %#v", s)
	}
	if s := agg.Errors()[0].Error(); s != "err" {
		t.Errorf("expected 'err', got %q", s)
	}

	err = agg.(error)
	if err == nil {
		t.Errorf("expected non-nil")
	}
	if s := err.Error(); s != "err" {
		t.Errorf("expected 'err', got %q", s)
	}
}

// TestPluralAggregate 验证多个错误时 Error() 输出 "[a, b]" 形式，Errors() 保留全部条目。
func TestPluralAggregate(t *testing.T) {
	var slice = []error{fmt.Errorf("abc"), fmt.Errorf("123")}
	var agg Aggregate
	var err error

	agg = NewAggregate(slice)
	if agg == nil {
		t.Errorf("expected non-nil")
	}
	if s := agg.Error(); s != "[abc, 123]" {
		t.Errorf("expected '[abc, 123]', got %q", s)
	}
	if s := agg.Errors(); len(s) != 2 {
		t.Errorf("expected two-elements slice, got %#v", s)
	}
	if s := agg.Errors()[0].Error(); s != "abc" {
		t.Errorf("expected '[abc, 123]', got %q", s)
	}

	err = agg.(error)
	if err == nil {
		t.Errorf("expected non-nil")
	}
	if s := err.Error(); s != "[abc, 123]" {
		t.Errorf("expected '[abc, 123]', got %q", s)
	}
}

// TestDedupeAggregate 验证 Error() 输出会去重，但 Errors() 仍返回原始条目（长度不变）。
func TestDedupeAggregate(t *testing.T) {
	var slice = []error{fmt.Errorf("abc"), fmt.Errorf("abc")}
	var agg Aggregate

	agg = NewAggregate(slice)
	if agg == nil {
		t.Errorf("expected non-nil")
	}
	if s := agg.Error(); s != "abc" {
		t.Errorf("expected 'abc', got %q", s)
	}
	if s := agg.Errors(); len(s) != 2 {
		t.Errorf("expected two-elements slice, got %#v", s)
	}
}

// TestDedupePluralAggregate 验证多个不同错误时去重后的输出顺序与首次出现顺序一致。
func TestDedupePluralAggregate(t *testing.T) {
	var slice = []error{fmt.Errorf("abc"), fmt.Errorf("abc"), fmt.Errorf("123")}
	var agg Aggregate

	agg = NewAggregate(slice)
	if agg == nil {
		t.Errorf("expected non-nil")
	}
	if s := agg.Error(); s != "[abc, 123]" {
		t.Errorf("expected '[abc, 123]', got %q", s)
	}
	if s := agg.Errors(); len(s) != 3 {
		t.Errorf("expected three-elements slice, got %#v", s)
	}
}

// TestFlattenAndDedupeAggregate 验证嵌套 Aggregate 会先展平再去重，
// 但 Errors() 返回的是展平后的原始条目而非去重结果。
func TestFlattenAndDedupeAggregate(t *testing.T) {
	var slice = []error{fmt.Errorf("abc"), fmt.Errorf("abc"), NewAggregate([]error{fmt.Errorf("abc")})}
	var agg Aggregate

	agg = NewAggregate(slice)
	if agg == nil {
		t.Errorf("expected non-nil")
	}
	if s := agg.Error(); s != "abc" {
		t.Errorf("expected 'abc', got %q", s)
	}
	if s := agg.Errors(); len(s) != 3 {
		t.Errorf("expected three-elements slice, got %#v", s)
	}
}

// TestFlattenAggregate 验证多层嵌套的 Aggregate 被递归展平，且重复消息在输出中去重。
func TestFlattenAggregate(t *testing.T) {
	var slice = []error{fmt.Errorf("abc"), fmt.Errorf("abc"), NewAggregate([]error{fmt.Errorf("abc"), fmt.Errorf("def"), NewAggregate([]error{fmt.Errorf("def"), fmt.Errorf("ghi")})})}
	var agg Aggregate

	agg = NewAggregate(slice)
	if agg == nil {
		t.Errorf("expected non-nil")
	}
	if s := agg.Error(); s != "[abc, def, ghi]" {
		t.Errorf("expected '[abc, def, ghi]', got %q", s)
	}
	if s := agg.Errors(); len(s) != 3 {
		t.Errorf("expected three-elements slice, got %#v", s)
	}
}

// TestFilterOut 验证 FilterOut 的匹配语义：任一 Matcher 返回 true 即剔除该错误；
// 嵌套的 Aggregate 会被逐层过滤，过滤后为空的子聚合也会被丢弃。
func TestFilterOut(t *testing.T) {
	testCases := []struct {
		err      error
		filter   []Matcher
		expected error
	}{
		{
			nil,
			[]Matcher{},
			nil,
		},
		{
			aggregate{},
			[]Matcher{},
			nil,
		},
		{
			aggregate{fmt.Errorf("abc")},
			[]Matcher{},
			aggregate{fmt.Errorf("abc")},
		},
		{
			aggregate{fmt.Errorf("abc")},
			[]Matcher{func(err error) bool { return false }},
			aggregate{fmt.Errorf("abc")},
		},
		{
			aggregate{fmt.Errorf("abc")},
			[]Matcher{func(err error) bool { return true }},
			nil,
		},
		{
			aggregate{fmt.Errorf("abc")},
			[]Matcher{func(err error) bool { return false }, func(err error) bool { return false }},
			aggregate{fmt.Errorf("abc")},
		},
		{
			aggregate{fmt.Errorf("abc")},
			[]Matcher{func(err error) bool { return false }, func(err error) bool { return true }},
			nil,
		},
		{
			aggregate{fmt.Errorf("abc"), fmt.Errorf("def"), fmt.Errorf("ghi")},
			[]Matcher{func(err error) bool { return err.Error() == "def" }},
			aggregate{fmt.Errorf("abc"), fmt.Errorf("ghi")},
		},
		{
			aggregate{aggregate{fmt.Errorf("abc")}},
			[]Matcher{},
			aggregate{aggregate{fmt.Errorf("abc")}},
		},
		{
			aggregate{aggregate{fmt.Errorf("abc"), aggregate{fmt.Errorf("def")}}},
			[]Matcher{},
			aggregate{aggregate{fmt.Errorf("abc"), aggregate{fmt.Errorf("def")}}},
		},
		{
			aggregate{aggregate{fmt.Errorf("abc"), aggregate{fmt.Errorf("def")}}},
			[]Matcher{func(err error) bool { return err.Error() == "def" }},
			aggregate{aggregate{fmt.Errorf("abc")}},
		},
	}
	for i, testCase := range testCases {
		err := FilterOut(testCase.err, testCase.filter...)
		if !reflect.DeepEqual(testCase.expected, err) {
			t.Errorf("%d: expected %v, got %v", i, testCase.expected, err)
		}
	}
}

// TestFlatten 验证 Flatten 把任意深度的 Aggregate 拉平为一层；
// 输入 nil 或空聚合时返回 nil。
func TestFlatten(t *testing.T) {
	testCases := []struct {
		agg      Aggregate
		expected Aggregate
	}{
		{
			nil,
			nil,
		},
		{
			aggregate{},
			nil,
		},
		{
			aggregate{fmt.Errorf("abc")},
			aggregate{fmt.Errorf("abc")},
		},
		{
			aggregate{fmt.Errorf("abc"), fmt.Errorf("def"), fmt.Errorf("ghi")},
			aggregate{fmt.Errorf("abc"), fmt.Errorf("def"), fmt.Errorf("ghi")},
		},
		{
			aggregate{aggregate{fmt.Errorf("abc")}},
			aggregate{fmt.Errorf("abc")},
		},
		{
			aggregate{aggregate{aggregate{fmt.Errorf("abc")}}},
			aggregate{fmt.Errorf("abc")},
		},
		{
			aggregate{aggregate{fmt.Errorf("abc"), aggregate{fmt.Errorf("def")}}},
			aggregate{fmt.Errorf("abc"), fmt.Errorf("def")},
		},
		{
			aggregate{aggregate{aggregate{fmt.Errorf("abc")}, fmt.Errorf("def"), aggregate{fmt.Errorf("ghi")}}},
			aggregate{fmt.Errorf("abc"), fmt.Errorf("def"), fmt.Errorf("ghi")},
		},
	}
	for i, testCase := range testCases {
		agg := Flatten(testCase.agg)
		if !reflect.DeepEqual(testCase.expected, agg) {
			t.Errorf("%d: expected %v, got %v", i, testCase.expected, agg)
		}
	}
}

// TestCreateAggregateFromMessageCountMap 验证从“消息->次数”映射重建聚合错误：
// 次数大于 1 时追加 "(repeated N times)"，且输出按消息排序以保证确定性。
func TestCreateAggregateFromMessageCountMap(t *testing.T) {
	testCases := []struct {
		name     string
		mcm      MessageCountMap
		expected Aggregate
	}{
		{
			"input has single instance of one message",
			MessageCountMap{"abc": 1},
			aggregate{fmt.Errorf("abc")},
		},
		{
			"input has multiple messages",
			MessageCountMap{"abc": 2, "ghi": 1},
			aggregate{fmt.Errorf("abc (repeated 2 times)"), fmt.Errorf("ghi")},
		},
		{
			"input has multiple messages",
			MessageCountMap{"ghi": 1, "abc": 2},
			aggregate{fmt.Errorf("abc (repeated 2 times)"), fmt.Errorf("ghi")},
		},
	}

	var expected, agg []error
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.expected != nil {
				expected = testCase.expected.Errors()
				sort.Slice(expected, func(i, j int) bool { return expected[i].Error() < expected[j].Error() })
			}
			if testCase.mcm != nil {
				agg = CreateAggregateFromMessageCountMap(testCase.mcm).Errors()
				sort.Slice(agg, func(i, j int) bool { return agg[i].Error() < agg[j].Error() })
			}
			if !reflect.DeepEqual(expected, agg) {
				t.Errorf("expected %v, got %v", expected, agg)
			}
		})
	}
}

// TestAggregateGoroutines 验证 AggregateGoroutines 并发收集错误：nil 被忽略，
// 结果顺序不确定，因此只比较错误集合。
func TestAggregateGoroutines(t *testing.T) {
	testCases := []struct {
		errs     []error
		expected map[string]bool //由于顺序不确定，不能直接与 Aggregate 比较
	}{
		{
			[]error{},
			nil,
		},
		{
			[]error{nil},
			nil,
		},
		{
			[]error{nil, nil},
			nil,
		},
		{
			[]error{fmt.Errorf("1")},
			map[string]bool{"1": true},
		},
		{
			[]error{fmt.Errorf("1"), nil},
			map[string]bool{"1": true},
		},
		{
			[]error{fmt.Errorf("1"), fmt.Errorf("267")},
			map[string]bool{"1": true, "267": true},
		},
		{
			[]error{fmt.Errorf("1"), nil, fmt.Errorf("1234")},
			map[string]bool{"1": true, "1234": true},
		},
		{
			[]error{nil, fmt.Errorf("1"), nil, fmt.Errorf("1234"), fmt.Errorf("22")},
			map[string]bool{"1": true, "1234": true, "22": true},
		},
	}
	for i, testCase := range testCases {
		funcs := make([]func() error, len(testCase.errs))
		for i := range testCase.errs {
			err := testCase.errs[i]
			funcs[i] = func() error { return err }
		}
		agg := AggregateGoroutines(funcs...)
		if agg == nil {
			if len(testCase.expected) > 0 {
				t.Errorf("%d: expected %v, got nil", i, testCase.expected)
			}
			continue
		}
		if len(agg.Errors()) != len(testCase.expected) {
			t.Errorf("%d: expected %d errors in aggregate, got %v", i, len(testCase.expected), agg)
			continue
		}
		for _, err := range agg.Errors() {
			if !testCase.expected[err.Error()] {
				t.Errorf("%d: expected %v, got aggregate containing %v", i, testCase.expected, err)
			}
		}
	}
}

// alwaysMatchingError 的 Is 恒为 true，用于验证 Aggregate 的 errors.Is 会遍历内部错误。
type alwaysMatchingError struct{}

// Error 返回固定文本，内容不影响匹配测试。
func (_ alwaysMatchingError) Error() string {
	return "error"
}

// Is 忽略目标错误，始终报告匹配。
func (_ alwaysMatchingError) Is(_ error) bool {
	return true
}

// someError 是可比较的值类型错误，用于测试 errors.Is 的“相等匹配”分支。
type someError struct{ msg string }

// Error 在未设置消息时回落到 "err"，让空值也能参与比较。
func (se someError) Error() string {
	if se.msg != "" {
		return se.msg
	}
	return "err"
}

// TestAggregateWithErrorsIs 验证 Aggregate 实现了 errors.Is 语义：
// 支持内部错误的 Is 方法、值相等、嵌套聚合以及外层 %w 包装。
func TestAggregateWithErrorsIs(t *testing.T) {
	testCases := []struct {
		name         string
		err          error
		matchAgainst error
		expectMatch  bool
	}{
		{
			name:         "no match",
			err:          aggregate{errors.New("my-error"), errors.New("my-other-error")},
			matchAgainst: fmt.Errorf("no entry %s", "here"),
		},
		{
			name:         "match via .Is()",
			err:          aggregate{errors.New("forbidden"), alwaysMatchingError{}},
			matchAgainst: errors.New("unauthorized"),
			expectMatch:  true,
		},
		{
			name:         "match via equality",
			err:          aggregate{errors.New("err"), someError{}},
			matchAgainst: someError{},
			expectMatch:  true,
		},
		{
			name:         "match via nested aggregate",
			err:          aggregate{errors.New("closed today"), aggregate{aggregate{someError{}}}},
			matchAgainst: someError{},
			expectMatch:  true,
		},
		{
			name:         "match via wrapped aggregate",
			err:          fmt.Errorf("wrap: %w", aggregate{errors.New("err"), someError{}}),
			matchAgainst: someError{},
			expectMatch:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := errors.Is(tc.err, tc.matchAgainst)
			if result != tc.expectMatch {
				t.Errorf("expected match: %t, got match: %t", tc.expectMatch, result)
			}
		})
	}
}

// accessTrackingError 记录其 Is 是否被调用，用来验证匹配的短路行为。
type accessTrackingError struct {
	wasAccessed bool
}

// Error 返回固定文本。
func (accessTrackingError) Error() string {
	return "err"
}

// Is 记录被访问并返回 true。
func (ate *accessTrackingError) Is(_ error) bool {
	ate.wasAccessed = true
	return true
}

// 编译期确认 *accessTrackingError 实现了 error。
var _ error = &accessTrackingError{}

// TestErrConfigurationInvalidWithErrorsIsShortCircuitsOnFirstMatch 验证 errors.Is
// 在第一个匹配后立即返回，不会继续访问后续错误（避免无谓的副作用与开销）。
func TestErrConfigurationInvalidWithErrorsIsShortCircuitsOnFirstMatch(t *testing.T) {
	errC := aggregate{&accessTrackingError{}, &accessTrackingError{}}
	_ = errors.Is(errC, &accessTrackingError{})

	var numAccessed int
	for _, err := range errC {
		if ate := err.(*accessTrackingError); ate.wasAccessed {
			numAccessed++
		}
	}
	if numAccessed != 1 {
		t.Errorf("expected exactly one error to get accessed, got %d", numAccessed)
	}
}
