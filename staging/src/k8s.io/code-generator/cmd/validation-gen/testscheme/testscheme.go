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

package testscheme

import (
	"bytes"
	stdcmp "cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"         // nolint:depguard // this package provides test utilities
	"github.com/google/go-cmp/cmp/cmpopts" // nolint:depguard // this package provides test utilities

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/test/coverage"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/randfill"
)

// Scheme 与 runtime.Scheme 类似，但只用于校验测试：只做校验、可注册任意类型
// （不限于 runtime.Object），并直接实现 Register，因此也能当作 scheme builder 使用。
// 只能用于在调用 validate 之前完成全部注册的测试。
// 由于测试是单线程的，这里用普通 map 保存注册函数，无需加锁。
type Scheme struct {
	validationFuncs    map[reflect.Type]func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList
	registrationErrors field.ErrorList
}

// New 创建新的 Scheme。
func New() *Scheme {
	return &Scheme{validationFuncs: map[reflect.Type]func(ctx context.Context, op operation.Operation, object interface{}, oldObject interface{}) field.ErrorList{}}
}

// AddValidationFunc 以 reflect.Type 为键注册校验函数，重复注册时后写者胜出。
func (s *Scheme) AddValidationFunc(srcType any, fn func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList) {
	s.validationFuncs[reflect.TypeOf(srcType)] = fn
}

// Validate 使用已注册的校验函数校验对象。
// 注册阶段出现错误时短路返回，避免测试因后续 panic 而丢失根因。
func (s *Scheme) Validate(ctx context.Context, options map[string]bool, object any, subresources ...string) field.ErrorList {
	if len(s.registrationErrors) > 0 {
		return s.registrationErrors // short circuit with registration errors if any are present
	}
	if fn, ok := s.validationFuncs[reflect.TypeOf(object)]; ok {
		return fn(ctx, operation.Operation{Type: operation.Create, Request: operation.Request{Subresources: subresources}, Options: options}, object, nil)
	}
	return nil
}

// ValidateUpdate 用已注册的校验函数校验对象更新。
func (s *Scheme) ValidateUpdate(ctx context.Context, options map[string]bool, object, oldObject any, subresources ...string) field.ErrorList {
	if len(s.registrationErrors) > 0 {
		return s.registrationErrors // short circuit with registration errors if any are present
	}
	if fn, ok := s.validationFuncs[reflect.TypeOf(object)]; ok {
		return fn(ctx, operation.Operation{Type: operation.Update, Request: operation.Request{Subresources: subresources}, Options: options}, object, oldObject)
	}
	return nil
}

// Register 把一个 scheme 初始化函数加入列表。
// 与 runtime.Scheme 不同，注册错误被累积而不是立即 panic，
// 便于在 Validate* 中统一呈现。
func (s *Scheme) Register(funcs ...func(*Scheme) error) {
	for _, f := range funcs {
		err := f(s)
		if err != nil {
			s.registrationErrors = append(s.registrationErrors, toRegistrationError(err))
		}
	}
}

// toRegistrationError 将注册错误包装成字段内部错误，保持错误类型统一。
func toRegistrationError(err error) *field.Error {
	return field.InternalError(nil, fmt.Errorf("registration error: %w", err))
}

// Test 为该 scheme 返回 ValidationTestBuilder。
func (s *Scheme) Test(t *testing.T) *ValidationTestBuilder {
	return &ValidationTestBuilder{t, s}
}

// ValidationTestBuilder 提供构建校验测试的便捷函数。
// 它包装 *testing.T 与 Scheme，所有 Expect* 方法都会创建子测试，
// 便于定位失败用例。
type ValidationTestBuilder struct {
	*testing.T
	s *Scheme
}

// fixtureEnvVar 控制是否重写 fixture 文件；仅用于本地更新期望值。
const fixtureEnvVar = "UPDATE_VALIDATION_GEN_FIXTURE_DATA"

// ValidateFixtures 确认所有注册类型的校验错误与测试 fixture 文件的预期一致。
// 对每个注册类型，先创建值并随机填充，再执行校验。
// 细节见 ValueFuzzed。
//
// 设置 UPDATE_VALIDATION_GEN_FIXTURE_DATA=true 时会创建或覆盖测试 fixture 文件。
//
// fixture：
// - validate-false.json：定义“注册类型 -> 字段路径 -> +validateFalse 参数”的映射，
// 即校验该类型时预期报出的错误。
//
// 该测试会遍历所有注册类型，因此新增类型若未更新 fixture 会失败；
// 这是刻意的，防止生成器悄悄漏掉校验。
func (s *ValidationTestBuilder) ValidateFixtures() {
	s.T.Helper()

	flag := os.Getenv(fixtureEnvVar)
	// 执行校验
	got := map[string]map[string][]string{}
	for t := range s.s.validationFuncs {
		var v any
		// TODO: 这里应支持 map 和 slice
		// 为类型构造指针值，使可空类型也能被 fuzz 填充。
		if t.Kind() == reflect.Pointer {
			v = reflect.New(t.Elem()).Interface()
		} else {
			v = reflect.Indirect(reflect.New(t)).Interface()
		}
		if reflect.TypeOf(v).Kind() != reflect.Pointer {
			v = &v
		}
		s.ValueFuzzed(v)
		vt := &ValidationTester{ValidationTestBuilder: s, value: v}
		byPath := vt.validateFalseArgsByPath()
		got[t.String()] = byPath
	}

	testdataFilename := "testdata/validate-false.json"
	if flag == "true" {
		// 生成 fixture 文件
		if err := os.MkdirAll(path.Dir(testdataFilename), os.FileMode(0755)); err != nil {
			s.Fatal("error making directory", err)
		}
		data, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			s.Fatal(err)
		}
		err = os.WriteFile(testdataFilename, data, os.FileMode(0644))
		if err != nil {
			s.Fatal(err)
		}
	} else {
		// 加载 fixture 文件
		testdataFile, err := os.Open(testdataFilename)
		if errors.Is(err, os.ErrNotExist) {
			s.Fatalf("%s test fixture data not found. Run go test with the environment variable %s=true to create test fixture data.",
				testdataFilename, fixtureEnvVar)
		} else if err != nil {
			s.Fatal(err)
		}
		defer func() {
			err := testdataFile.Close()
			if err != nil {
				s.Fatal(err)
			}
		}()

		byteValue, err := io.ReadAll(testdataFile)
		if err != nil {
			s.Fatal(err)
		}
		testdata := map[string]map[string][]string{}
		err = json.Unmarshal(byteValue, &testdata)
		if err != nil {
			s.Fatal(err)
		}
		// 将 fixture 与校验结果比较
		expectedKeys := sets.New[string]()
		gotKeys := sets.New[string]()
		for k := range got {
			gotKeys.Insert(k)
		}
		hasErrors := false
		for k, expectedForType := range testdata {
			expectedKeys.Insert(k)
			gotForType, ok := got[k]
			s.T.Run(k, func(t *testing.T) {
				t.Helper()

				if !ok {
					t.Errorf("%q has expected validateFalse args in %s but got no validation errors.", k, testdataFilename)
					hasErrors = true
				} else if !cmp.Equal(gotForType, expectedForType) {
					t.Errorf("validateFalse args, grouped by field path, differed from %s:\n%s\n",
						testdataFilename, cmp.Diff(gotForType, expectedForType, cmpopts.SortMaps(stdcmp.Less[string])))
					hasErrors = true
				}
			})
		}
		for unexpectedType := range gotKeys.Difference(expectedKeys) {
			s.T.Run(unexpectedType, func(t *testing.T) {
				t.Helper()

				t.Errorf("%q got unexpected validateFalse args, grouped by field path:\n%s\n",
					unexpectedType, cmp.Diff(nil, got[unexpectedType], cmpopts.SortMaps(stdcmp.Less[string])))
				hasErrors = true
			})
		}
		if hasErrors {
			s.T.Logf("If the test expectations have changed, run go test with the environment variable %s=true", fixtureEnvVar)
		}
	}
}

// randfiller 返回固定种子的 randfill 填充器：保证测试可重复，
// 并强制产生非空集合，以覆盖迭代分支。
func randfiller() *randfill.Filler {
	// 确保列表与 map 非空，并使用确定性随机种子。
	// 但也不要无限递归。
	return randfill.New().NilChance(0.0).NumElements(2, 2).MaxDepth(8).RandSource(rand.NewSource(0))
}

// ValueFuzzed 用确定性填充器自动填充给定值。
// 填充器把指针设为具体值，并总是放入两个 map 键和 slice 元素。
func (s *ValidationTestBuilder) ValueFuzzed(value any) *ValidationTester {
	randfiller().Fill(value)
	return &ValidationTester{ValidationTestBuilder: s, value: value}
}

// Value 为给定值返回 ValidationTester。该值
// 必须已在 scheme 中注册，才能执行校验。
func (s *ValidationTestBuilder) Value(value any) *ValidationTester {
	return &ValidationTester{ValidationTestBuilder: s, value: value}
}

// ValidationTester 提供便捷函数，为某个可校验值定义校验测试。
// oldValue 非 nil 时自动切换为 update 校验，因此链式调用顺序有意义。
type ValidationTester struct {
	*ValidationTestBuilder
	value        any
	oldValue     any
	isUpdate     bool
	options      map[string]bool
	subresources []string
}

// OldValue 为 ValidationTester 设置 oldValue；设置后
// 将使用更新校验来测试。
// oldValue 必须与 value 类型相同。
// 返回 ValidationTester 以支持链式调用。
func (v *ValidationTester) OldValue(oldValue any) *ValidationTester {
	v.oldValue = oldValue
	v.isUpdate = true
	return v
}

// OldValueFuzzed 用确定性填充器自动填充给定值。
// 填充器把指针设为具体值，并总是放入两个 map 键和 slice 元素。
func (v *ValidationTester) OldValueFuzzed(oldValue any) *ValidationTester {
	randfiller().Fill(oldValue)
	v.oldValue = oldValue
	v.isUpdate = true
	return v
}

// Opts 设置要使用的 ValidationOpts。
func (v *ValidationTester) Opts(options map[string]bool) *ValidationTester {
	v.options = options
	return v
}

// Subresource 设置要使用的 ValidationOpts。
func (v *ValidationTester) Subresources(subresources []string) *ValidationTester {
	v.subresources = subresources
	return v
}

// multiline 将错误列表格式化为多行，便于测试失败时阅读。
func multiline(errs field.ErrorList) string {
	if len(errs) == 0 {
		return "<no errors>"
	}
	if len(errs) == 1 {
		return errs[0].Error()
	}

	var buf bytes.Buffer
	for _, err := range errs {
		buf.WriteString("\n")
		buf.WriteString(err.Error())
	}
	return buf.String()
}

// ExpectValid 校验值，若有错误则调用 t.Errorf。
// 返回 ValidationTester 以支持链式调用。
func (v *ValidationTester) ExpectValid() *ValidationTester {
	v.T.Helper()

	v.T.Run(fmt.Sprintf("%T", v.value), func(t *testing.T) {
		t.Helper()

		errs := v.validate()
		if len(errs) > 0 {
			t.Errorf("want no errors, got: %v", multiline(errs))
		}
	})
	return v
}

// ExpectValidateFalseByPath 校验值并查找
// 由 `+k8s:validateFalse` 标签产生的错误。每个字段（map 键）
// 可对应多个错误字符串（map 值）。若要证明
// 校验逻辑本身（如 validation-gen）产生了预期错误，
// 应使用此方法。
func (v *ValidationTester) ExpectValidateFalseByPath(expectedByPath map[string][]string) *ValidationTester {
	v.T.Helper()

	v.T.Run(fmt.Sprintf("%T", v.value), func(t *testing.T) {
		t.Helper()

		actualByPath := v.validateFalseArgsByPath()
		// 确保参数已排序
		for _, args := range expectedByPath {
			sort.Strings(args)
		}
		if !cmp.Equal(expectedByPath, actualByPath) {
			t.Errorf("validateFalse args, grouped by field path, differed from expected:\n%s\n", cmp.Diff(expectedByPath, actualByPath, cmpopts.SortMaps(stdcmp.Less[string])))
		}

	})
	return v
}

// validateFalseArgsByPath 提取由 +k8s:validateFalse 产生的错误，
// 按字段路径聚合其参数。字段路径为 "<nil>" 时归一化为空串，
// 与 fixture 中类型级校验的键保持一致。
func (v *ValidationTester) validateFalseArgsByPath() map[string][]string {
	byPath := map[string][]string{}
	errs := v.validate()
	for _, e := range errs {
		if strings.HasPrefix(e.Detail, "forced failure: ") {
			arg := strings.TrimPrefix(e.Detail, "forced failure: ")
			f := e.Field
			if f == "<nil>" {
				f = ""
			}
			byPath[f] = append(byPath[f], arg)
		}
	}
	// 确保参数已排序
	for _, args := range byPath {
		sort.Strings(args)
	}
	return byPath
}

// ExpectMatches 用给定的 ErrorMatcher 比较预期错误
// 与校验实际返回的错误。若要证明
// 某个校验用例（如测试 pod 校验）
// 产生预期错误，应使用此方法。
func (v *ValidationTester) ExpectMatches(matcher field.ErrorMatcher, expected field.ErrorList) *ValidationTester {
	v.Helper()

	v.Run(fmt.Sprintf("%T", v.value), func(t *testing.T) {
		t.Helper()
		actual := v.validate()
		matcher.Test(t, expected, actual)
	})
	return v
}

// validate 根据是否设置 oldValue 选择 create 或 update 校验，
// 并把观测到的规则上报给覆盖率统计。
func (v *ValidationTester) validate() field.ErrorList {
	var errs field.ErrorList
	if v.isUpdate {
		errs = v.s.ValidateUpdate(context.Background(), v.options, v.value, v.oldValue, v.subresources...)
	} else {
		errs = v.s.Validate(context.Background(), v.options, v.value, v.subresources...)
	}

	rt := reflect.TypeOf(v.value)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	pkgName := strings.Split(rt.String(), ".")[0]
	gvk := schema.GroupVersionKind{Group: rt.PkgPath(), Version: pkgName, Kind: rt.Name()}
	coverage.RecordObservedRules(gvk, errs)

	return errs
}
