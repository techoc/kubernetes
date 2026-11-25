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

package testing

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	apinamingtest "k8s.io/apimachinery/pkg/api/apitesting/naming"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
)

// APIVersionRegexp 是匹配合法 API 版本的正则表达式。
// 只接受 vN、vNalphaM、vNbetaM 三种形式，用于拒绝内部版本和随意命名的版本。
var APIVersionRegexp = regexp.MustCompile(`^v\d+((alpha|beta){1}\d+)?$`)

// ComponentConfigPackage 用于 API 组测试，描述待校验的组件配置包。
// AllowedTags 与 AllowedNonstandardJSONNames 用于白名单那些因历史原因无法满足命名约定的字段。
type ComponentConfigPackage struct {
	ComponentName      string
	GroupName          string
	SchemeGroupVersion schema.GroupVersion
	AddToScheme        func(*runtime.Scheme) error
	// SkipTests 按校验函数名跳过特定检查，供个别包规避无法修复的历史问题。
	SkipTests                   sets.String
	AllowedTags                 map[reflect.Type]bool
	AllowedNonstandardJSONNames map[reflect.Type]string
}

// testingFunc 是所有校验函数的统一签名，便于按名称注册与跳过。
type testingFunc func(*runtime.Scheme, *ComponentConfigPackage) error

const (
	verifyTagNaming                 = "verifyTagNaming"
	verifyGroupNameSuffix           = "verifyGroupNameSuffix"
	verifyGroupNameMatch            = "verifyGroupNameMatch"
	verifyCorrectGroupName          = "verifyCorrectGroupName"
	verifyComponentConfigKindExists = "verifyComponentConfigKindExists"
	verifyExternalAPIVersion        = "verifyExternalAPIVersion"
	verifyInternalAPIVersion        = "verifyInternalAPIVersion"
)

// testingFuncs 是外部与内部包共用的基础校验集合；名称同时作为 SkipTests 的键。
var testingFuncs = map[string]testingFunc{
	verifyTagNaming:        verifyTagNamingFunc,
	verifyGroupNameSuffix:  verifyGroupNameSuffixFunc,
	verifyGroupNameMatch:   verifyGroupNameMatchFunc,
	verifyCorrectGroupName: verifyCorrectGroupNameFunc,
}

// VerifyExternalTypePackage 校验外部组件配置包的定义是否正确：
// 检查标签命名（json 名称应与 Go 名称一致）、GroupName 的 k8s.io 后缀与匹配关系、
// API 版本模式且非内部版本、addKnownTypes 与 AddToScheme 至少注册一个类型且不报错，
// 以及 GroupName 命名正确且 scheme 中存在 {Component}Configuration 类型。
// 外部版本额外要求版本号符合 vN(alpha|beta)M 模式。
func VerifyExternalTypePackage(pkginfo *ComponentConfigPackage) error {
	scheme, err := setup(pkginfo)
	if err != nil {
		return fmt.Errorf("test setup error: %v", err)
	}
	extraFns := map[string]testingFunc{
		verifyExternalAPIVersion: verifyExternalAPIVersionFunc,
	}
	return runFuncs(scheme, pkginfo, extraFns)
}

// VerifyInternalTypePackage 校验内部组件配置包的定义是否正确：
// 检查标签命名（不允许标签）、GroupName 的 k8s.io 后缀与匹配关系、API 版本为内部版本、
// addKnownTypes 与 AddToScheme 至少注册一个类型且不报错，以及 GroupName 命名正确且 scheme 中
// 存在 {Component}Configuration 类型。内部版本不允许 json 标签，版本号必须为 __internal。
func VerifyInternalTypePackage(pkginfo *ComponentConfigPackage) error {
	scheme, err := setup(pkginfo)
	if err != nil {
		return fmt.Errorf("test setup error: %v", err)
	}
	extraFns := map[string]testingFunc{
		verifyInternalAPIVersion:        verifyInternalAPIVersionFunc,
		verifyComponentConfigKindExists: verifyComponentConfigKindExistsFunc,
	}
	return runFuncs(scheme, pkginfo, extraFns)
}

// setup 校验参数完整性并构建 scheme；任何一步失败都视为测试自身配置错误。
func setup(pkginfo *ComponentConfigPackage) (*runtime.Scheme, error) {
	// 缺少任一必填项都说明调用方没有正确构造 pkginfo，先于后续校验报错。
	if len(pkginfo.ComponentName) == 0 ||
		len(pkginfo.GroupName) == 0 ||
		pkginfo.SchemeGroupVersion.Empty() ||
		pkginfo.AddToScheme == nil {
		return nil, fmt.Errorf("invalid argument: not all parameters were passed correctly to the function")
	}

	scheme := runtime.NewScheme()
	if err := pkginfo.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("AddToScheme must not return an error: %v", err)
	}
	// 空 scheme 说明 AddToScheme 什么也没注册，后续检查会全部失去意义。
	if len(scheme.AllKnownTypes()) == 0 {
		return nil, fmt.Errorf("AddToScheme doesn't register any type")
	}
	return scheme, nil
}

// runFuncs 执行基础校验与额外校验，汇总所有错误一次性返回，
// 避免修复一个问题后又暴露下一个。
func runFuncs(scheme *runtime.Scheme, pkginfo *ComponentConfigPackage, extraFns map[string]testingFunc) error {
	verifyFns := []testingFunc{}
	for name, fn := range testingFuncs {
		if pkginfo.SkipTests.Has(name) {
			continue
		}
		verifyFns = append(verifyFns, fn)
	}
	for name, fn := range extraFns {
		if pkginfo.SkipTests.Has(name) {
			continue
		}
		verifyFns = append(verifyFns, fn)
	}
	errs := []error{}
	for _, fn := range verifyFns {
		if err := fn(scheme, pkginfo); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewAggregate(errs)
}

// verifyTagNamingFunc 检查结构体字段的 json 标签名与 Go 字段名一致（除白名单例外）。
func verifyTagNamingFunc(scheme *runtime.Scheme, pkginfo *ComponentConfigPackage) error {
	return apinamingtest.VerifyTagNaming(scheme, pkginfo.AllowedTags, pkginfo.AllowedNonstandardJSONNames)
}

// verifyGroupNameSuffixFunc 检查所有组的名称以 k8s.io 结尾。
func verifyGroupNameSuffixFunc(scheme *runtime.Scheme, _ *ComponentConfigPackage) error {
	return apinamingtest.VerifyGroupNames(scheme, sets.NewString())
}

// verifyGroupNameMatchFunc 检查声明的 GroupName 与 SchemeGroupVersion 中的组一致，
// 两者不一致通常意味着 register.go 里的常量写错。
func verifyGroupNameMatchFunc(_ *runtime.Scheme, pkginfo *ComponentConfigPackage) error {
	if pkginfo.GroupName != pkginfo.SchemeGroupVersion.Group {
		return fmt.Errorf("GroupName must be equal to SchemeGroupVersion.Group, GroupName: %v,SchemeGroupVersion.Group: %v",
			pkginfo.GroupName, pkginfo.SchemeGroupVersion.Group)
	}
	return nil
}

// verifyCorrectGroupNameFunc 要求组名严格等于 "<component>.config.k8s.io"（组件名去连字符、小写）。
func verifyCorrectGroupNameFunc(_ *runtime.Scheme, pkginfo *ComponentConfigPackage) error {
	desiredGroupName := fmt.Sprintf("%s.config.k8s.io", lowercaseWithoutDashes(pkginfo.ComponentName))
	if pkginfo.SchemeGroupVersion.Group != desiredGroupName {
		return fmt.Errorf("got GroupName %q, want %q", pkginfo.SchemeGroupVersion.Group, desiredGroupName)

	}
	return nil
}

// verifyComponentConfigKindExistsFunc 要求内部 scheme 中存在 {Component}Configuration 类型。
func verifyComponentConfigKindExistsFunc(scheme *runtime.Scheme, pkginfo *ComponentConfigPackage) error {
	expectedKind := fmt.Sprintf("%sConfiguration", dashesToCapitalCase(pkginfo.ComponentName))
	expectedGVK := pkginfo.SchemeGroupVersion.WithKind(expectedKind)
	if !scheme.Recognizes(expectedGVK) {
		registeredKinds := sets.NewString()
		for gvk := range scheme.AllKnownTypes() {
			registeredKinds.Insert(gvk.Kind)
		}
		return fmt.Errorf("Kind %s not registered in the scheme, registered kinds are %v", expectedKind, registeredKinds.List())
	}
	return nil
}

// verifyExternalAPIVersionFunc 要求外部版本号匹配 APIVersionRegexp。
func verifyExternalAPIVersionFunc(_ *runtime.Scheme, pkginfo *ComponentConfigPackage) error {
	if !APIVersionRegexp.MatchString(pkginfo.SchemeGroupVersion.Version) {
		return fmt.Errorf("invalid API version %q, must match %q", pkginfo.SchemeGroupVersion.Version, APIVersionRegexp.String())
	}
	return nil
}

// verifyInternalAPIVersionFunc 要求版本号恰为 runtime.APIVersionInternal（"__internal"）。
func verifyInternalAPIVersionFunc(_ *runtime.Scheme, pkginfo *ComponentConfigPackage) error {
	if pkginfo.SchemeGroupVersion.Version != runtime.APIVersionInternal {
		return fmt.Errorf("internal API version must be %q, got %q",
			runtime.APIVersionInternal, pkginfo.SchemeGroupVersion.Version)
	}
	return nil
}

// lowercaseWithoutDashes 把组件名转为组名片段：全小写且去掉连字符。
func lowercaseWithoutDashes(str string) string {
	return strings.Replace(strings.ToLower(str), "-", "", -1)
}

// dashesToCapitalCase 把 kebab-case 组件名转成 Kind 用的 PascalCase。
func dashesToCapitalCase(str string) string {
	segments := strings.Split(str, "-")
	result := ""
	for _, segment := range segments {
		result += cases.Title(language.English).String(segment)
	}
	return result
}
