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

package library

import (
	"fmt"
	"net/url"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/validation"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
)

var (
	// base64_length estimate for base64 regex size from github.com/asaskevich/govalidator
	base64Length = 84
	// url_length estimate for url regex size from github.com/asaskevich/govalidator
	urlLength = 1103
)

// Format provides a CEL library exposing common named Kubernetes string
// validations. Can be used in CRD ValidationRules messageExpression.
//
// Example:
//
//	rule:              format.dns1123label.validate(object.metadata.name).hasValue()
//	messageExpression: format.dns1123label.validate(object.metadata.name).value().join("\n")
//
// format.named(name: string) -> ?Format
//
// Returns the Format with the given name, if it exists. Otherwise, optional.none
// Allowed names are:
//   - `dns1123Label`
//   - `dns1123Subdomain`
//   - `dns1035Label`
//   - `qualifiedName`
//   - `dns1123LabelPrefix`
//   - `dns1123SubdomainPrefix`
//   - `dns1035LabelPrefix`
//   - `labelValue`
//   - `uri`
//   - `uuid`
//   - `byte`
//   - `date`
//   - `datetime`
//
// format.<formatName>() -> Format
//
// Convenience functions for all the named formats are also available.
//
// Examples:
//
//	format.dns1123Label().validate("my-label-name")
//	format.dns1123Subdomain().validate("apiextensions.k8s.io")
//	format.dns1035Label().validate("my-label-name")
//	format.qualifiedName().validate("apiextensions.k8s.io/v1beta1")
//	format.dns1123LabelPrefix().validate("my-label-prefix-")
//	format.dns1123SubdomainPrefix().validate("mysubdomain.prefix.-")
//	format.dns1035LabelPrefix().validate("my-label-prefix-")
//	format.uri().validate("http://example.com")
//	    Uses same pattern as isURL, but returns an error
//	format.uuid().validate("123e4567-e89b-12d3-a456-426614174000")
//	format.byte().validate("aGVsbG8=")
//	format.date().validate("2021-01-01")
//	format.datetime().validate("2021-01-01T00:00:00Z")
//
// <Format>.validate(str: string) -> ?list<string>
//
// Validates the given string against the given format. Returns optional.none
// if the string is valid, otherwise a list of validation error strings.
func Format() cel.EnvOption {
	return cel.Lib(formatLib)
}

var formatLib = &format{}

// format 是 "kubernetes.format" CEL 库的实现。
// 无状态，因此用单例即可被所有编译环境共享。
type format struct{}

// LibraryName 返回库名，必须以 "kubernetes." 前缀避免与 cel-go 冲突。
func (*format) LibraryName() string {
	return "kubernetes.format"
}

// Types 声明库引入的自定义类型（Format），供编译期类型解析使用。
func (*format) Types() []*cel.Type {
	return []*cel.Type{apiservercel.FormatType}
}

// declarations 暴露函数声明，供兼容性测试核对函数集合。
func (*format) declarations() map[string][]cel.FunctionOpt {
	return formatLibraryDecls
}

// ZeroArgumentFunctionBinding 把无参 Go 函数包装成 CEL 重载绑定。
// 这里显式校验参数个数为 0，防止误用导致运行期 panic。
func ZeroArgumentFunctionBinding(binding func() ref.Val) decls.OverloadOpt {
	return func(o *decls.OverloadDecl) (*decls.OverloadDecl, error) {
		wrapped, err := decls.FunctionBinding(func(values ...ref.Val) ref.Val { return binding() })(o)
		if err != nil {
			return nil, err
		}
		if len(wrapped.ArgTypes()) != 0 {
			return nil, fmt.Errorf("function binding must have 0 arguments")
		}
		return o, nil
	}
}

// CompileOptions 注册 validate、format.named 以及每个具名格式的零参构造函数。
// 具名格式被编译为常量返回，避免每次求值都查找 map。
func (*format) CompileOptions() []cel.EnvOption {
	options := make([]cel.EnvOption, 0, len(formatLibraryDecls))
	for name, overloads := range formatLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	for name, constantValue := range ConstantFormats {
		prefixedName := "format." + name
		options = append(options, cel.Function(prefixedName, cel.Overload(prefixedName, []*cel.Type{}, apiservercel.FormatType, ZeroArgumentFunctionBinding(func() ref.Val {
			return constantValue
		}))))
	}
	return options
}

// ProgramOptions 无需运行期配置，格式库完全由编译期声明驱动。
func (*format) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// ConstantFormats 把 CEL 中可用的格式名映射到校验实现。
// MaxRegexSize 用于 CEL 的成本估算（避免正则类校验被滥用来消耗 CPU），
// 因此必须按最坏情况保守取值。
var ConstantFormats = map[string]apiservercel.Format{
	"dns1123Label": {
		Name:         "DNS1123Label",
		ValidateFunc: func(s string) []string { return apimachineryvalidation.NameIsDNSLabel(s, false) },
		MaxRegexSize: 30,
	},
	"dns1123Subdomain": {
		Name:         "DNS1123Subdomain",
		ValidateFunc: func(s string) []string { return apimachineryvalidation.NameIsDNSSubdomain(s, false) },
		MaxRegexSize: 60,
	},
	"dns1035Label": {
		Name:         "DNS1035Label",
		ValidateFunc: func(s string) []string { return apimachineryvalidation.NameIsDNS1035Label(s, false) },
		MaxRegexSize: 30,
	},
	"qualifiedName": {
		Name:         "QualifiedName",
		ValidateFunc: validation.IsQualifiedName,
		MaxRegexSize: 60, // uses subdomain regex
	},

	"dns1123LabelPrefix": {
		Name:         "DNS1123LabelPrefix",
		ValidateFunc: func(s string) []string { return apimachineryvalidation.NameIsDNSLabel(s, true) },
		MaxRegexSize: 30,
	},
	"dns1123SubdomainPrefix": {
		Name:         "DNS1123SubdomainPrefix",
		ValidateFunc: func(s string) []string { return apimachineryvalidation.NameIsDNSSubdomain(s, true) },
		MaxRegexSize: 60,
	},
	"dns1035LabelPrefix": {
		Name:         "DNS1035LabelPrefix",
		ValidateFunc: func(s string) []string { return apimachineryvalidation.NameIsDNS1035Label(s, true) },
		MaxRegexSize: 30,
	},
	"labelValue": {
		Name:         "LabelValue",
		ValidateFunc: validation.IsValidLabelValue,
		MaxRegexSize: 40,
	},

	// CRD formats
	// Implementations sourced from strfmt, which kube-openapi uses as its
	// format library. There are other CRD formats supported, but they are
	// covered by other portions of the CEL library (like IP/CIDR), or their
	// use is discouraged (like bsonobjectid, email, etc)
	"uri": {
		Name: "URI",
		ValidateFunc: func(s string) []string {
			// Directly call ParseRequestURI since we can get a better error message
			_, err := url.ParseRequestURI(s)
			if err != nil {
				return []string{err.Error()}
			}
			return nil
		},
		// Use govalidator url regex to estimate, since ParseRequestURI
		// doesnt use regex
		MaxRegexSize: urlLength,
	},
	"uuid": {
		Name: "uuid",
		ValidateFunc: func(s string) []string {
			if !strfmt.Default.Validates("uuid", s) {
				return []string{"does not match the UUID format"}
			}
			return nil
		},
		MaxRegexSize: len(strfmt.UUIDPattern),
	},
	"byte": {
		Name: "byte",
		ValidateFunc: func(s string) []string {
			if !strfmt.Default.Validates("byte", s) {
				return []string{"invalid base64"}
			}
			return nil
		},
		MaxRegexSize: base64Length,
	},
	"date": {
		Name: "date",
		ValidateFunc: func(s string) []string {
			if !strfmt.Default.Validates("date", s) {
				return []string{"invalid date"}
			}
			return nil
		},
		// Estimated regex size for RFC3339FullDate which is
		// a date format. Assume a date-time pattern is longer
		// so use that to conservatively estimate this
		MaxRegexSize: len(strfmt.DateTimePattern),
	},
	"datetime": {
		Name: "datetime",
		ValidateFunc: func(s string) []string {
			if !strfmt.Default.Validates("datetime", s) {
				return []string{"invalid datetime"}
			}
			return nil
		},
		MaxRegexSize: len(strfmt.DateTimePattern),
	},
}

var formatLibraryDecls = map[string][]cel.FunctionOpt{
	// validate 是成员函数：<Format>.validate(str) 返回 ?list<string>。
	// 用 Optional 表达"无错误"，调用方无需区分空列表与 nil。
	"validate": {
		cel.MemberOverload("format-validate", []*cel.Type{apiservercel.FormatType, cel.StringType}, cel.OptionalType(cel.ListType(cel.StringType)), cel.BinaryBinding(formatValidate)),
	},
	"format.named": {
		cel.Overload("format-named", []*cel.Type{cel.StringType}, cel.OptionalType(apiservercel.FormatType), cel.UnaryBinding(func(name ref.Val) ref.Val {
			nameString, ok := name.Value().(string)
			if !ok {
				return types.MaybeNoSuchOverloadErr(name)
			}

			f, ok := ConstantFormats[nameString]
			if !ok {
				return types.OptionalNone
			}
			return types.OptionalOf(f)
		})),
	},
}

// formatValidate 实现 validate 重载：类型不匹配时返回重载错误，
// 校验通过返回 optional.none，失败返回错误消息列表。
func formatValidate(arg1, arg2 ref.Val) ref.Val {
	f, ok := arg1.Value().(apiservercel.Format)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg1)
	}

	str, ok := arg2.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg2)
	}

	res := f.ValidateFunc(str)
	if len(res) == 0 {
		return types.OptionalNone
	}
	return types.OptionalOf(types.NewStringList(types.DefaultTypeAdapter, res))
}
