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

package genericclioptions

import (
	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/utils/ptr"
)

// ResourceBuilderFlags 聚合了“定位资源”所需的一组可选 flag。
// 字段为指针是关键设计：只有非 nil 的字段才会被注册为 flag，
// 从而允许不同命令按需裁剪（例如 apply 需要 -f，而 get 需要 selector）。
type ResourceBuilderFlags struct {
	// FileNameFlags 控制 -f/-k/-R 等文件来源相关的 flag。
	FileNameFlags *FileNameFlags

	// 以下指针字段为 nil 时表示该 flag 不注册。
	LabelSelector *string // -l
	FieldSelector *string // --field-selector
	AllNamespaces *bool   // -A
	All           *bool   // --all
	Local         *bool   // --local

	// Scheme 非空时按类型化对象构建 builder，否则退化为 unstructured 解析。
	Scheme           *runtime.Scheme
	Latest           bool // 强制从服务端取最新版本
	StopOnFirstError bool // true 时遇错即停止，默认继续处理后续资源
}

// NewResourceBuilderFlags 返回默认配置：只启用文件来源 flag，且 -R 默认开启；其余 flag 需通过 With* 方法显式开启。
func NewResourceBuilderFlags() *ResourceBuilderFlags {
	filenames := []string{}

	return &ResourceBuilderFlags{
		FileNameFlags: &FileNameFlags{
			Usage:     "identifying the resource.",
			Filenames: &filenames,
			Recursive: ptr.To(true),
		},
	}
}

// WithFile 覆盖默认的文件 flag 配置（默认 -R 为 true，这里以传入值为准）。
func (o *ResourceBuilderFlags) WithFile(recurse bool, kustomize *string, files ...string) *ResourceBuilderFlags {
	o.FileNameFlags = &FileNameFlags{
		Usage:     "identifying the resource.",
		Filenames: &files,
		Kustomize: kustomize,
		Recursive: ptr.To(recurse),
	}

	return o
}

// WithLabelSelector 启用标签选择器 flag，并设置默认值。
func (o *ResourceBuilderFlags) WithLabelSelector(selector string) *ResourceBuilderFlags {
	o.LabelSelector = &selector
	return o
}

// WithFieldSelector 启用字段选择器 flag，并设置默认值。
func (o *ResourceBuilderFlags) WithFieldSelector(selector string) *ResourceBuilderFlags {
	o.FieldSelector = &selector
	return o
}

// WithAllNamespaces 启用跨命名空间 flag，并设置默认值。
func (o *ResourceBuilderFlags) WithAllNamespaces(defaultVal bool) *ResourceBuilderFlags {
	o.AllNamespaces = &defaultVal
	return o
}

// WithAll 启用选择全部资源的 flag，并设置默认值。
func (o *ResourceBuilderFlags) WithAll(defaultVal bool) *ResourceBuilderFlags {
	o.All = &defaultVal
	return o
}

// WithLocal 启用本地模式 flag，并设置默认值。
func (o *ResourceBuilderFlags) WithLocal(defaultVal bool) *ResourceBuilderFlags {
	o.Local = &defaultVal
	return o
}

// WithScheme 设置用于类型化解析的 Scheme。
func (o *ResourceBuilderFlags) WithScheme(scheme *runtime.Scheme) *ResourceBuilderFlags {
	o.Scheme = scheme
	return o
}

// WithLatest 启用从服务端获取最新版本的语义。
func (o *ResourceBuilderFlags) WithLatest() *ResourceBuilderFlags {
	o.Latest = true
	return o
}

// StopOnError 启用遇到首个错误即停止的行为。
func (o *ResourceBuilderFlags) StopOnError() *ResourceBuilderFlags {
	o.StopOnFirstError = true
	return o
}

// AddFlags 注册用于定位资源的 flag。
//
// 这里按字段是否为 nil 决定注册哪些 flag；帮助文本直接面向终端用户，
// 因此措辞需与 kubectl 保持一致，避免不同命令出现不一致的 flag 语义。
func (o *ResourceBuilderFlags) AddFlags(flagset *pflag.FlagSet) {
	o.FileNameFlags.AddFlags(flagset)

	// selector 以短选项 -l 暴露，与 kubectl 的既有交互习惯保持兼容。
	if o.LabelSelector != nil {
		flagset.StringVarP(o.LabelSelector, "selector", "l", *o.LabelSelector, "Selector (label query) to filter on, supports '=', '==', and '!='.(e.g. -l key1=value1,key2=value2)")
	}
	if o.FieldSelector != nil {
		flagset.StringVar(o.FieldSelector, "field-selector", *o.FieldSelector, "Selector (field query) to filter on, supports '=', '==', and '!='.(e.g. --field-selector key1=value1,key2=value2). The server only supports a limited number of field queries per type.")
	}
	if o.AllNamespaces != nil {
		flagset.BoolVarP(o.AllNamespaces, "all-namespaces", "A", *o.AllNamespaces, "If present, list the requested object(s) across all namespaces. Namespace in current context is ignored even if specified with --namespace.")
	}
	if o.All != nil {
		flagset.BoolVar(o.All, "all", *o.All, "Select all resources in the namespace of the specified resource types")
	}
	if o.Local != nil {
		flagset.BoolVar(o.Local, "local", *o.Local, "If true, annotation will NOT contact api-server but run locally.")
	}
}

// ToBuilder 依据当前 flag 取值返回一个 ResourceFinder，用于访问所定位到的资源。
//
// 该方法是整个 flag 集合的落地点：把各 flag 的当前值翻译成 resource.Builder
// 上对应的配置。注意 local 与非 local 的互斥关系——本地模式下没有 API server，
// 因而类型/名称参数、label/field selector 和 latest 语义都无法成立。
func (o *ResourceBuilderFlags) ToBuilder(restClientGetter RESTClientGetter, resources []string) ResourceFinder {
	// 先确定命名空间及是否由 kubeconfig 强制指定；错误延迟到最后通过 AddError 返回，
	// 这样即便命名空间解析失败也能继续构建其余部分并给出更完整的诊断。
	namespace, enforceNamespace, namespaceErr := restClientGetter.ToRawKubeConfigLoader().Namespace()

	builder := resource.NewBuilder(restClientGetter).
		NamespaceParam(namespace).DefaultNamespace()

	if o.AllNamespaces != nil {
		builder.AllNamespaces(*o.AllNamespaces)
	}

	// 指定 Scheme 时使用类型化对象（保留原始字段与默认值处理）；
	// 否则用 unstructured，适合任意 CRD 而无需预先注册类型。
	if o.Scheme != nil {
		builder.WithScheme(o.Scheme, o.Scheme.PrioritizedVersionsAllGroups()...)
	} else {
		builder.Unstructured()
	}

	if o.FileNameFlags != nil {
		opts := o.FileNameFlags.ToOptions()
		builder.FilenameParam(enforceNamespace, &opts)
	}

	if o.Local == nil || !*o.Local {
		// 仅当服务端可用时才接受 type/name 参数与各类选择器。
		if o.All != nil {
			builder.ResourceTypeOrNameArgs(*o.All, resources...)
		} else {
			builder.ResourceTypeOrNameArgs(false, resources...)
		}
		// 标签选择器由服务端过滤，字段选择器仅支持每种类型的少量字段。
		if o.LabelSelector != nil {
			builder.LabelSelectorParam(*o.LabelSelector)
		}
		// 字段选择器需要在服务端索引中存在，否则查询会失败。
		if o.FieldSelector != nil {
			builder.FieldSelectorParam(*o.FieldSelector)
		}
		// latest 表示忽略文件中记录的 resourceVersion，强制读取服务端最新对象。
		if o.Latest {
			builder.Latest()
		}

	} else {
		// 本地模式只允许纯文件输入；一旦同时给出资源参数就显式报错，
		// 避免用户误以为本地解析也会按 type/name 去查询。
		builder.Local()

		if len(resources) > 0 {
			builder.AddError(resource.LocalResourceError)
		}
	}

	// 默认继续处理错误以便一次报告多个问题；StopOnFirstError 适合需要尽快失败的脚本场景。
	if !o.StopOnFirstError {
		builder.ContinueOnError()
	}

	return &ResourceFindBuilderWrapper{
		builder: builder.
			Flatten().
			AddError(namespaceErr),
	}
}

// ResourceFindBuilderWrapper 把 *resource.Builder 包装成 ResourceFinder 接口。
//
// 之所以需要包装，是因为 ResourceFinder 只需要 Do() 一个方法，
// 而 *resource.Builder 还暴露了大量链式配置方法。
type ResourceFindBuilderWrapper struct {
	builder *resource.Builder
}

// Do 返回 builder 构建出的资源访问器，此时才真正开始遍历资源。
func (b *ResourceFindBuilderWrapper) Do() resource.Visitor {
	return b.builder.Do()
}

// ResourceFinder 允许在测试中用假实现替换 resource builder。
//
// ResourceFinder 是本包对外暴露的最小抽象，便于测试时替换为假实现
// （参见 FakeResourceFinder）。
type ResourceFinder interface {
	Do() resource.Visitor
}

// ResourceFinderFunc 把普通函数适配成 ResourceFinder。
type ResourceFinderFunc func() resource.Visitor

// Do 调用被适配的函数并返回其 Visitor。
func (fn ResourceFinderFunc) Do() resource.Visitor {
	return fn()
}

// ResourceFinderForResult 把一个 Visitor 复用为 ResourceFinder。
//
// 把已经构建好的 Visitor 直接当找源使用：每次 Do() 返回同一个 Visitor。
// 仅用于结果已确定、无需重新构建的场景。
func ResourceFinderForResult(result resource.Visitor) ResourceFinder {
	return ResourceFinderFunc(func() resource.Visitor {
		return result
	})
}
