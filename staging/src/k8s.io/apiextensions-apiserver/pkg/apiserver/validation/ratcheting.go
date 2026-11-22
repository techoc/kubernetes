/*
Copyright 2023 The Kubernetes Authors.

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

package validation

import (
	"reflect"

	"k8s.io/apiserver/pkg/cel/common"
	celopenapi "k8s.io/apiserver/pkg/cel/openapi"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/kube-openapi/pkg/validation/validate"
)

// schemaArgs 是构造 OpenAPI schema 校验器（NewSchemaValidator）所需的参数。
type schemaArgs struct {
	schema       *spec.Schema
	root         interface{}
	path         string
	knownFormats strfmt.Registry
	options      []validate.Option
}

// RatchetingSchemaValidator 包装 kube-openapi 的 SchemaValidator，
// 提供支持“棘轮”（只允许收紧、不允许放宽）策略的 ValidateUpdate 方法。
type RatchetingSchemaValidator struct {
	schemaArgs
}

// NewRatchetingSchemaValidator 基于给定 schema 创建棘轮校验器。
func NewRatchetingSchemaValidator(schema *spec.Schema, rootSchema interface{}, root string, formats strfmt.Registry, options ...validate.Option) *RatchetingSchemaValidator {
	return &RatchetingSchemaValidator{
		schemaArgs: schemaArgs{
			schema:       schema,
			root:         rootSchema,
			path:         root,
			knownFormats: formats,
			options:      options,
		},
	}
}

// Validate 校验新值，等价于直接使用底层 SchemaValidator。
func (r *RatchetingSchemaValidator) Validate(new interface{}, options ...ValidationOption) *validate.Result {
	sv := validate.NewSchemaValidator(r.schema, r.root, r.path, r.knownFormats, r.options...)
	return sv.Validate(new)
}

// ValidateUpdate 校验从 old 到 new 的更新。未开启棘轮时退化为普通校验；
// 开启时关联 old/new，对子节点递归使用棘轮校验器。
func (r *RatchetingSchemaValidator) ValidateUpdate(new, old interface{}, options ...ValidationOption) *validate.Result {
	opts := NewValidationOptions(options...)

	if !opts.Ratcheting {
		sv := validate.NewSchemaValidator(r.schema, r.root, r.path, r.knownFormats, r.options...)
		return sv.Validate(new)
	}

	correlation := opts.CorrelatedObject
	if correlation == nil {
		correlation = common.NewCorrelatedObject(new, old, &celopenapi.Schema{Schema: r.schema})
	}

	return newRatchetingValueValidator(
		correlation,
		r.schemaArgs,
	).Validate(new)
}

// ratchetingValueValidator 表示针对特定 `old` 和 `new` 参数的一次
// SchemaValidator.ValidateUpdate 调用。
//
// 它通过在每次递归调用中注入 validate.Option，跟随 openapi SchemaValidator 对新值的遍历过程。
//
// 每探索一个子索引或子属性，就会构造一个 ratchetingValueValidator 并加入校验树。
//
// 它的主要职责是在遍历过程中保持 old/new 的关联，并按棘轮策略后处理错误。
//
// ratchetingValueValidator 不是线程安全的。
type ratchetingValueValidator struct {
	// schemaArgs 提供在 Validate 调用期间创建的临时 SchemaValidator 所用的参数。
	schemaArgs
	correlation *common.CorrelatedObject
}

// newRatchetingValueValidator 创建一个新的棘轮子校验器。
func newRatchetingValueValidator(correlation *common.CorrelatedObject, args schemaArgs) *ratchetingValueValidator {
	return &ratchetingValueValidator{
		schemaArgs:  args,
		correlation: correlation,
	}
}

// getValidateOption 返回一个 kube-openapi validate.Option，用于 SchemaValidator，
// 把 ratchetingValueValidator 注入所有子键和子索引的校验。
func (r *ratchetingValueValidator) getValidateOption() validate.Option {
	return func(svo *validate.SchemaValidatorOptions) {
		svo.NewValidatorForField = r.SubPropertyValidator
		svo.NewValidatorForIndex = r.SubIndexValidator
	}
}

// Validate 校验从 r.oldValue 到新值 r.value 的更新。
//
// 校验过程中会为所有遍历到的字段路径构建一棵临时的 ratchetingValueValidator 树。
// 构建这棵树是必要的：可以在不大幅改动 kube-openapi 实现的前提下，复用底层对象在
// 校验时执行的 DeepEqual 检查。
//
// 该树以及单次 Validate 调用所用的全部缓存/临时空间，会在最外层 Validate 调用结束时被丢弃。
//
// Validate 会为每个被探索的子节点创建一个树节点。节点的主要用途是保存 oldValue 与当前
// 传入值之间惰性计算的 DeepEqual 结果；一旦计算过，就会保存在节点中，供父节点在需要
// DeepEqual 比较时复用。
//
// 本次调用会顺带填充其 children 字段，把探索到的对象树节点写入其中。
func (r *ratchetingValueValidator) Validate(new interface{}) *validate.Result {
	opts := append([]validate.Option{
		r.getValidateOption(),
	}, r.options...)

	s := validate.NewSchemaValidator(r.schema, r.root, r.path, r.knownFormats, opts...)

	res := s.Validate(r.correlation.Value)

	if res.IsValid() {
		return res
	}

	// 当前棘轮规则是：当 DeepEqual(old, new) 为 true 时，把错误降级为警告。
	// 这样旧版本对象已接受的字段不会被新版本的更严格校验拒绝。
	if r.correlation.CachedDeepEqual() {
		newRes := &validate.Result{}
		newRes.MergeAsWarnings(res)
		return newRes
	}

	return res
}

// SubPropertyValidator 重写子属性的标准校验器构造函数，返回我们的棘轮变体。
//
// 若能关联到旧值，就返回用于该子节点的棘轮校验器。
//
// 若无法关联到旧值，则使用默认校验。
func (r *ratchetingValueValidator) SubPropertyValidator(field string, schema *spec.Schema, rootSchema interface{}, root string, formats strfmt.Registry, options ...validate.Option) validate.ValueValidator {
	childNode := r.correlation.Key(field)
	if childNode == nil || (r.path == "" && isTypeMetaField(field)) {
		// 若无法关联旧值，或正在校验根对象且字段属于 metadata，
		// 则回退到默认校验。
		//
		// apiVersion 的变更不可见，无法应用棘轮
		// （旧值和新值都会转换到同一类型）。
		return validate.NewSchemaValidator(schema, rootSchema, root, formats, options...)
	}

	return newRatchetingValueValidator(childNode, schemaArgs{
		schema:       schema,
		root:         rootSchema,
		path:         root,
		knownFormats: formats,
		options:      options,
	})
}

// SubIndexValidator 重写子索引的标准校验器构造函数，返回我们的棘轮变体。
//
// 若能关联到旧值，就返回用于该子节点的棘轮校验器。
//
// 若无法关联到旧值，则使用默认校验。
func (r *ratchetingValueValidator) SubIndexValidator(index int, schema *spec.Schema, rootSchema interface{}, root string, formats strfmt.Registry, options ...validate.Option) validate.ValueValidator {
	childNode := r.correlation.Index(index)
	if childNode == nil {
		return validate.NewSchemaValidator(schema, rootSchema, root, formats, options...)
	}

	return newRatchetingValueValidator(childNode, schemaArgs{
		schema:       schema,
		root:         rootSchema,
		path:         root,
		knownFormats: formats,
		options:      options,
	})
}

// 编译期断言：ratchetingValueValidator 满足 kube-openapi 的 ValueValidator 接口。
var _ validate.ValueValidator = (&ratchetingValueValidator{})

// SetPath 是 ValueValidator 接口的占位实现，kube-openapi 不会调用。
func (r ratchetingValueValidator) SetPath(path string) {
	// 不做处理
	// kube-openapi 不会使用
}

// Applies 表示该校验器适用于任意值和任意 Kind。
func (r ratchetingValueValidator) Applies(source interface{}, valueKind reflect.Kind) bool {
	return true
}

// isTypeMetaField 判断给定路径是否为 kind/apiVersion 这类类型元数据字段。
func isTypeMetaField(path string) bool {
	return path == "kind" || path == "apiVersion"
}
