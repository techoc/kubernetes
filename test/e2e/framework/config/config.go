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

// Package config 简化配置选项的声明。
// 目前实现会把它们直接映射成命令行 flag。在测试套件中与
// test/e2e/framework/viperconfig 结合使用时，这些 flag 也可以从配置文件读取。
//
// 所有命令行 flag 都存储在私有的 flag set 中，由 E2E 测试套件的开发者
// 决定如何暴露它们。可选方式包括：
//   - 作为普通 flag 暴露在实际命令行中：
//     CopyFlags(Flags, flag.CommandLine)
//   - 通过 test/e2e/framework/viperconfig 填充：
//     viperconfig.ViperizeFlags("my-config.yaml", "", Flags)
//   - 两者结合：
//     CopyFlags(Flags, flag.CommandLine)
//     viperconfig.ViperizeFlags("my-config.yaml", "", flag.CommandLine)
//
// 测试开发者无需逐个定义 flag，只需给结构体加 tag 然后调用一个函数即可。
// 思路与 https://godoc.org/github.com/jessevdk/go-flags 相同，
// 但实现上让测试套件可以继续使用标准的 "flag" 包。
//
// 例如，storage/csi.go 文件可以这样定义：
//
//	var scaling struct {
//	        NumNodes int  `default:"1" description:"number of nodes to run on"`
//	        Master string
//	}
//	_ = config.AddOptions(&scaling, "storage.csi.scaling")
//
// 这会定义如下命令行 flag：
//
//	-storage.csi.scaling.numNodes=<int>  - 要运行的节点数（默认：1）
//	-storage.csi.scaling.master=<string>
//
// 结构体中的所有字段都必须导出，且类型必须是下列之一（与 `flag` 包一致）：
// - bool
// - time.Duration
// - float64
// - string
// - int
// - int64
// - uint
// - uint64
// - 以及/或者包含上述基本类型的嵌套或嵌入结构体。
//
// 每个基本字段可以带 tag，支持以下可选键：
//
//	usage:   选项的补充说明
//	default: 默认值，格式与命令行传入的一致，布尔值用 true/false
//
// 最终配置选项的名称由结构体所有选项共用的可选前缀和字段名以点号拼接而成。
// 为与 `ginkgo` 定义的命令行 flag 保持一致，每个字段名的首字符会转成小写。
//
// 目前不支持别名，因此重命名字段或公共前缀对测试套件用户是可见的，
// 并可能破坏依赖旧名称的脚本。
//
// 测试套件会在运行测试前把实际值填进该变量。注意，注册 Ginkgo 测试的代码
// 不能使用这些配置选项，因为测试注册和选项注册都发生在 E2E 测试套件
// 处理参数之前。
package config

import (
	"flag"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"
)

// Flags 是 AddOptions 添加 flag 的目标 flag set。测试作者也应使用它，
// 而不是直接往全局命令行添加 flag。
var Flags = flag.NewFlagSet("", flag.ContinueOnError)

// CopyFlags 确保 source flag set 中定义的所有 flag 都出现在 target 中，
// 就像直接定义在 target 里一样。沿用 flag 包的行为：如果 target 已经包含
// source 中的同名 flag，会 panic。
func CopyFlags(source *flag.FlagSet, target *flag.FlagSet) {
	source.VisitAll(func(flag *flag.Flag) {
		// 无需复制 flag.DefValue：原始默认值（例如 flag.String 设置的）
		// 已保存在 value 中，Var 会提取它用于帮助信息。
		target.Var(flag.Value, flag.Name, flag.Usage)
	})
}

// AddOptions 分析 options 值并创建填充它所需的 flag。
//
// prefix 可以把选项放到整体选项集的更深层，不同层级用点号分隔。
//
// 该函数总是返回 true，以便简化选项注册：
// _ = AddOptions(...)
//
// 遇到不支持的类型或选项名冲突等错误时会 panic。
func AddOptions(options interface{}, prefix string) bool {
	return AddOptionsToSet(Flags, options, prefix)
}

// AddOptionsToSet 与 AddOptions 相同，只是可以指定 flag set。
func AddOptionsToSet(flags *flag.FlagSet, options interface{}, prefix string) bool {
	optionsType := reflect.TypeOf(options)
	if optionsType == nil {
		panic("options parameter without a type - nil?!")
	}
	if optionsType.Kind() != reflect.Pointer || optionsType.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("need a pointer to a struct, got instead: %T", options))
	}
	addStructFields(flags, optionsType.Elem(), reflect.Indirect(reflect.ValueOf(options)), prefix)
	return true
}

func addStructFields(flags *flag.FlagSet, structType reflect.Type, structValue reflect.Value, prefix string) {
	for i := 0; i < structValue.NumField(); i++ {
		entry := structValue.Field(i)
		addr := entry.Addr()
		structField := structType.Field(i)
		name := structField.Name
		r, n := utf8.DecodeRuneInString(name)
		name = string(unicode.ToLower(r)) + name[n:]
		usage := structField.Tag.Get("usage")
		def := structField.Tag.Get("default")
		if prefix != "" {
			name = prefix + "." + name
		}
		if structField.PkgPath != "" {
			panic(fmt.Sprintf("struct entry %q not exported", name))
		}
		ptr := addr.Interface()
		if structField.Anonymous {
			// 嵌入字段中的成员视为结构体自身的成员，
			// 即使用相同的前缀添加。
			addStructFields(flags, structField.Type, entry, prefix)
			continue
		}
		if structField.Type.Kind() == reflect.Struct {
			// 添加嵌套选项。
			addStructFields(flags, structField.Type, entry, name)
			continue
		}
		// 可以直接按 structField.Type 做 switch，但拿到持有字段指针的
		// interface 后再 switch 的好处是，可以直接把该指针作为 flag 变量添加。
		//
		// 也许将来泛型会让整个 switch 变得多余……
		switch ptr := ptr.(type) {
		case *bool:
			var defValue bool
			parseDefault(&defValue, name, def)
			flags.BoolVar(ptr, name, defValue, usage)
		case *time.Duration:
			var defValue time.Duration
			parseDefault(&defValue, name, def)
			flags.DurationVar(ptr, name, defValue, usage)
		case *float64:
			var defValue float64
			parseDefault(&defValue, name, def)
			flags.Float64Var(ptr, name, defValue, usage)
		case *string:
			flags.StringVar(ptr, name, def, usage)
		case *int:
			var defValue int
			parseDefault(&defValue, name, def)
			flags.IntVar(ptr, name, defValue, usage)
		case *int64:
			var defValue int64
			parseDefault(&defValue, name, def)
			flags.Int64Var(ptr, name, defValue, usage)
		case *uint:
			var defValue uint
			parseDefault(&defValue, name, def)
			flags.UintVar(ptr, name, defValue, usage)
		case *uint64:
			var defValue uint64
			parseDefault(&defValue, name, def)
			flags.Uint64Var(ptr, name, defValue, usage)
		default:
			panic(fmt.Sprintf("unsupported struct entry type %q: %T", name, entry.Interface()))
		}
	}
}

// parseDefault 是必需的，因为 "flag" 要求默认值是实际类型，不能传字符串。
// 理想情况是复用 "flag" 包中已有的解析代码，但那些代码没有导出。
func parseDefault(value interface{}, name, def string) {
	if def == "" {
		return
	}
	checkErr := func(err error, value interface{}) {
		if err != nil {
			panic(fmt.Sprintf("invalid default %q for %T entry %s: %s", def, value, name, err))
		}
	}
	switch value := value.(type) {
	case *bool:
		v, err := strconv.ParseBool(def)
		checkErr(err, *value)
		*value = v
	case *time.Duration:
		v, err := time.ParseDuration(def)
		checkErr(err, *value)
		*value = v
	case *float64:
		v, err := strconv.ParseFloat(def, 64)
		checkErr(err, *value)
		*value = v
	case *int:
		v, err := strconv.Atoi(def)
		checkErr(err, *value)
		*value = v
	case *int64:
		v, err := strconv.ParseInt(def, 0, 64)
		checkErr(err, *value)
		*value = v
	case *uint:
		v, err := strconv.ParseUint(def, 0, strconv.IntSize)
		checkErr(err, *value)
		*value = uint(v)
	case *uint64:
		v, err := strconv.ParseUint(def, 0, 64)
		checkErr(err, *value)
		*value = v
	default:
		panic(fmt.Sprintf("%q: setting defaults not supported for type %T", name, value))
	}
}
