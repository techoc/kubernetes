/*
Copyright 2014 The Kubernetes Authors.

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

package runtime

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/naming"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kube-openapi/pkg/util"
)

// Scheme 是 Kubernetes API 对象的核心注册表，负责对象序列化/反序列化所需的类型识别，
// 并维护 Go 类型与 Group、Version、Kind（GVK）之间的映射，以及不同版本 Go 结构之间的转换关系。
//
// Type 表示一个 Go 结构体；Version 表示该类型的一种 API 表示版本；Kind 是该版本内的类型名称；
// Group 将一组随时间演进的 Version、Kind 和 Type 组织在一起。Unversioned 类型没有绑定到
// 常规版本演进路径，承诺长期保持向后兼容。
//
// Scheme 应在程序启动阶段完成注册，注册完成后再并发读取。它的内部 map 不提供并发写保护，
// 因此运行期间继续注册类型、转换函数或默认值函数可能造成数据竞争。
// Scheme 同时保存默认值函数与声明式校验函数，是 API 对象进入编码、解码和转换流程的重要基础。
type Scheme struct {
	// gvkToType 根据 GVK 查找对应的 Go 结构体类型，是解码时按 apiVersion/kind 构造对象的索引。
	gvkToType map[schema.GroupVersionKind]reflect.Type

	// typeToGVK 根据 Go 结构体类型反查其注册过的全部 GVK。键必须是非指针 reflect.Type，
	// 因为注册时会先对传入对象类型执行 Elem()。
	typeToGVK map[reflect.Type][]schema.GroupVersionKind

	// unversioned 类型在 ConvertToVersion 中不做转换直接处理：只改 GVK、不做结构转换，因此永远向后兼容。
	unversionedTypes map[reflect.Type]schema.GroupVersionKind

	// unversionedKinds 按 Kind 名称索引 unversioned 类型，使其可以在任意 Group/Version 上构造。
	// TODO: resolve the status of unversioned types.
	unversionedKinds map[string]reflect.Type

	// fieldLabelConversionFuncs 保存 GVK 对应的字段选择器转换函数，将外部版本字段映射到内部版本。
	fieldLabelConversionFuncs map[schema.GroupVersionKind]FieldLabelConversionFunc

	// defaulterFuncs 按 Go 类型保存默认值函数；执行时传入对象指针，以便函数原地填充字段。
	defaulterFuncs map[reflect.Type]func(interface{})

	// validationFuncs 按 Go 类型保存声明式校验函数。创建校验时 oldObject 为 nil；
	// 更新校验时会同时传入旧对象，以执行状态转换规则和不可变字段检查。
	validationFuncs map[reflect.Type]func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList

	// converter 保存手写与生成的类型转换函数，并提供未显式注册时的默认反射转换行为。
	converter *conversion.Converter

	// versionPriority 保存每个 API Group 的版本优先级列表，决定默认选择哪个版本进行转换或展示。
	versionPriority map[string][]string

	// observedVersions 按首次注册顺序记录 GroupVersion，用于未显式配置优先级时生成稳定顺序。
	// 用于在没有显式 SetVersionPriority 时，按注册顺序补全版本列表。
	observedVersions []schema.GroupVersion

	// schemeName 是该 Scheme 的名称。未显式指定时从 NewScheme 调用栈推断，
	// 主要用于错误信息定位 Scheme 的创建来源。
	schemeName string
}

// FieldLabelConversionFunc converts a field selector to internal representation.
type FieldLabelConversionFunc func(label, value string) (internalLabel, internalValue string, err error)

// 创建一个新的 Scheme，并初始化类型注册表、转换器、默认值函数和校验函数映射。
// 默认注册 Object 与 RawExtension、字符串之间的基础转换。调用方应在启动阶段完成注册，运行期间只读使用。
func NewScheme() *Scheme {
	s := &Scheme{
		gvkToType:                 map[schema.GroupVersionKind]reflect.Type{},
		typeToGVK:                 map[reflect.Type][]schema.GroupVersionKind{},
		unversionedTypes:          map[reflect.Type]schema.GroupVersionKind{},
		unversionedKinds:          map[string]reflect.Type{},
		fieldLabelConversionFuncs: map[schema.GroupVersionKind]FieldLabelConversionFunc{},
		defaulterFuncs:            map[reflect.Type]func(interface{}){},
		validationFuncs:           map[reflect.Type]func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList{},
		versionPriority:           map[string][]string{},
		// 未显式命名时从调用栈推断，便于错误信息定位是哪个 scheme 出的问题。
		schemeName: naming.GetNameFromCallsite(internalPackages...),
	}
	s.converter = conversion.NewConverter(nil)

	// Enable couple default conversions by default.
	// Object<->RawExtension 与 string 相关转换是几乎所有 scheme 都需要的基础能力。
	utilruntime.Must(RegisterEmbeddedConversions(s))
	utilruntime.Must(RegisterStringConversions(s))
	return s
}

// 返回当前 Scheme 使用的转换器。调用方可通过它注册或查询更细粒度的类型转换规则。
func (s *Scheme) Converter() *conversion.Converter {
	return s.converter
}

// 将给定对象类型注册为 unversioned 类型。序列化时使用传入的 GroupVersion，但不执行版本结构转换；这类对象必须长期保持向后兼容。
// 注册过程先记录版本并登记普通类型，再建立类型到 GVK 的 unversioned 映射。同一 Kind 名称不能映射到不同 Go 类型。
func (s *Scheme) AddUnversionedTypes(version schema.GroupVersion, types ...Object) {
	s.addObservedVersion(version)
	s.AddKnownTypes(version, types...)
	for _, obj := range types {
		t := reflect.TypeOf(obj).Elem()
		gvk := version.WithKind(t.Name())
		s.unversionedTypes[t] = gvk
		if old, ok := s.unversionedKinds[gvk.Kind]; ok && t != old {
			panic(fmt.Sprintf("%v.%v has already been registered as unversioned kind %q - kind name must be unique in scheme %q", old.PkgPath(), old.Name(), gvk, s.schemeName))
		}
		s.unversionedKinds[gvk.Kind] = t
	}
}

// 将多个 Go 对象类型注册到指定 GroupVersion。每个参数必须是指向结构体的指针，结构体名称默认作为 Kind。
// 如果需要让 Go 类型使用不同的 Kind 名称，请使用 AddKnownTypeWithName。
func (s *Scheme) AddKnownTypes(gv schema.GroupVersion, types ...Object) {
	s.addObservedVersion(gv)
	for _, obj := range types {
		t := reflect.TypeOf(obj)
		if t.Kind() != reflect.Pointer {
			panic("All types must be pointers to structs.")
		}
		t = t.Elem()
		// 默认以 Go 类型名作为 kind；需要别名时用 AddKnownTypeWithName。
		s.AddKnownTypeWithName(gv.WithKind(t.Name()), obj)
	}
}

// 将一个 Go 对象类型注册到指定 GVK，允许显式指定 Kind 名称。
// 方法会校验版本非空、对象为结构体指针，并维护 GVK 到 Go 类型及 Go 类型到 GVK 的双向索引；重复注册同一映射是幂等的，不同类型占用同一 GVK 会触发 panic。
func (s *Scheme) AddKnownTypeWithName(gvk schema.GroupVersionKind, obj Object) {
	s.addObservedVersion(gvk.GroupVersion())
	t := reflect.TypeOf(obj)
	if len(gvk.Version) == 0 {
		panic(fmt.Sprintf("version is required on all types: %s %v", gvk, t))
	}
	if t.Kind() != reflect.Pointer {
		panic("All types must be pointers to structs.")
	}
	t = t.Elem()
	if t.Kind() != reflect.Struct {
		panic("All types must be pointers to structs.")
	}

	// 同一个 GVK 绑定到两个不同 Go 类型属于编程错误，必须尽早暴露。
	if oldT, found := s.gvkToType[gvk]; found && oldT != t {
		panic(fmt.Sprintf("Double registration of different types for %v: old=%v.%v, new=%v.%v in scheme %q", gvk, oldT.PkgPath(), oldT.Name(), t.PkgPath(), t.Name(), s.schemeName))
	}

	s.gvkToType[gvk] = t

	for _, existingGvk := range s.typeToGVK[t] {
		if existingGvk == gvk {
			return
		}
	}
	s.typeToGVK[t] = append(s.typeToGVK[t], gvk)

	// 若类型实现了 DeepCopyInto(<obj>)，则注册同类型转换：直接复用 DeepCopyInto，
	// 并清空 TypeMeta，与反射转换的旧行为保持一致。
	if m := reflect.ValueOf(obj).MethodByName("DeepCopyInto"); m.IsValid() && m.Type().NumIn() == 1 && m.Type().NumOut() == 0 && m.Type().In(0) == reflect.TypeOf(obj) {
		if err := s.AddGeneratedConversionFunc(obj, obj, func(a, b interface{}, scope conversion.Scope) error {
			// copy a to b
			reflect.ValueOf(a).MethodByName("DeepCopyInto").Call([]reflect.Value{reflect.ValueOf(b)})
			// clear TypeMeta to match legacy reflective conversion
			b.(Object).GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
			return nil
		}); err != nil {
			panic(err)
		}
	}
}

// KnownTypes 返回指定版本已知的 kind -> Go 类型映射，用于按版本构造对象或反射检查。
func (s *Scheme) KnownTypes(gv schema.GroupVersion) map[string]reflect.Type {
	types := make(map[string]reflect.Type)
	for gvk, t := range s.gvkToType {
		if gv != gvk.GroupVersion() {
			continue
		}

		types[gvk.Kind] = t
	}
	return types
}

// VersionsForGroupKind 返回某个 GroupKind 在给定组内可以转换到的版本列表。
// GroupKind 也可能被转换到其他组，该信息可从 EquivalentResourceMapper 获取。
// 实现上先按优先级顺序遍历已知版本，再筛选该 GroupKind 实际注册过的
// 版本，从而保证输出稳定且符合优先级。
func (s *Scheme) VersionsForGroupKind(gk schema.GroupKind) []schema.GroupVersion {
	availableVersions := []schema.GroupVersion{}
	for gvk := range s.gvkToType {
		if gk != gvk.GroupKind() {
			continue
		}

		availableVersions = append(availableVersions, gvk.GroupVersion())
	}

	// order the return for stability
	ret := []schema.GroupVersion{}
	for _, version := range s.PrioritizedVersionsForGroup(gk.Group) {
		for _, availableVersion := range availableVersions {
			if version != availableVersion {
				continue
			}
			ret = append(ret, availableVersion)
		}
	}

	return ret
}

// 返回 Scheme 当前维护的完整 GVK 到 Go reflect.Type 映射。返回的是内部映射本身，调用方不应修改它。
func (s *Scheme) AllKnownTypes() map[schema.GroupVersionKind]reflect.Type {
	return s.gvkToType
}

// 查询 Go 对象可能对应的全部 GVK，并返回它是否为 unversioned 类型。
// 普通对象必须是已注册类型的指针；Unstructured 对象不查注册表，而是直接读取对象自身声明的 GVK，并要求 Kind 与 Version 均非空。
func (s *Scheme) ObjectKinds(obj Object) ([]schema.GroupVersionKind, bool, error) {
	// Unstructured 对象始终被认为携带其自身声明的 GVK：它不在注册表中，只能相信其内部声明的 GVK。
	// 识别对象要求 GVK 已填充完整，因此 kind/version 缺失时报错而不是猜测。
	if _, ok := obj.(Unstructured); ok {
		gvk := obj.GetObjectKind().GroupVersionKind()
		if len(gvk.Kind) == 0 {
			return nil, false, NewMissingKindErr("unstructured object has no kind")
		}
		if len(gvk.Version) == 0 {
			return nil, false, NewMissingVersionErr("unstructured object has no version")
		}
		return []schema.GroupVersionKind{gvk}, false, nil
	}

	v, err := conversion.EnforcePtr(obj)
	if err != nil {
		return nil, false, err
	}
	t := v.Type()

	gvks, ok := s.typeToGVK[t]
	if !ok {
		return nil, false, NewNotRegisteredErrForType(s.schemeName, t)
	}
	_, unversionedType := s.unversionedTypes[t]

	return gvks, unversionedType, nil
}

// 判断指定 GVK 是否已经登记在当前 Scheme 中。该方法只检查注册表，不执行对象构造或版本转换。
func (s *Scheme) Recognizes(gvk schema.GroupVersionKind) bool {
	_, exists := s.gvkToType[gvk]
	return exists
}

// IsUnversioned 判断 obj 是否为已注册的 unversioned 类型。
// 第一个返回值表示是否为 unversioned；第二个表示该类型是否已在 scheme 中
// 注册——两者要分开，因为“未注册”并不等同于“versioned”。
func (s *Scheme) IsUnversioned(obj Object) (bool, bool) {
	v, err := conversion.EnforcePtr(obj)
	if err != nil {
		return false, false
	}
	t := v.Type()

	if _, ok := s.typeToGVK[t]; !ok {
		return false, false
	}
	_, ok := s.unversionedTypes[t]
	return ok, true
}

// 根据 GVK 创建一个新的 API 对象。先精确查询 GVK 注册表；若未命中，再按 Kind 查询 unversioned 类型；两条路径都未命中时返回未注册错误。
func (s *Scheme) New(kind schema.GroupVersionKind) (Object, error) {
	if t, exists := s.gvkToType[kind]; exists {
		return reflect.New(t).Interface().(Object), nil
	}

	// unversioned kind 可以在任意 group/version 下构造，因此只按 kind 名查找。
	if t, exists := s.unversionedKinds[kind.Kind]; exists {
		return reflect.New(t).Interface().(Object), nil
	}
	return nil, NewNotRegisteredErrForKind(s.schemeName, kind)
}

// 登记一对转换时应忽略的类型。转换器遇到该类型对时会跳过其内部数据转换，适用于转换过程中明确丢弃的数据。
func (s *Scheme) AddIgnoredConversionType(from, to interface{}) error {
	return s.converter.RegisterIgnoredConversion(from, to)
}

// 注册由调用方提供的类型转换函数。函数必须能正确处理指定的源类型和目标类型；Scheme 不会替调用方验证函数内部的类型断言。
func (s *Scheme) AddConversionFunc(a, b interface{}, fn conversion.ConversionFunc) error {
	return s.converter.RegisterUntypedConversionFunc(a, b, fn)
}

// 注册由代码生成器生成的类型转换函数。它与手写转换函数分别登记，便于区分自动生成逻辑和人工编写逻辑。
func (s *Scheme) AddGeneratedConversionFunc(a, b interface{}, fn conversion.ConversionFunc) error {
	return s.converter.RegisterGeneratedUntypedConversionFunc(a, b, fn)
}

// 为指定 GVK 注册字段选择器转换函数。该函数负责把外部版本中的字段名和值转换为内部版本使用的表示。
func (s *Scheme) AddFieldLabelConversionFunc(gvk schema.GroupVersionKind, conversionFunc FieldLabelConversionFunc) error {
	s.fieldLabelConversionFuncs[gvk] = conversionFunc
	return nil
}

// 为指定 Go 对象类型注册默认值函数。调用 Default 时，只有对象的具体 Go 类型与注册类型完全匹配才会执行；同一类型重复注册时以后一次注册为准。
func (s *Scheme) AddTypeDefaultingFunc(srcType Object, fn func(interface{})) {
	s.defaulterFuncs[reflect.TypeOf(srcType)] = fn
}

// 为对象应用已注册的默认值函数。根据对象的具体 Go 类型查找函数，并将对象指针传入以便原地填充默认值；没有匹配函数时不执行任何操作。
func (s *Scheme) Default(src Object) {
	// 只用指针类型做键，保证默认值函数能原地修改对象。
	if fn, ok := s.defaulterFuncs[reflect.TypeOf(src)]; ok {
		fn(src)
	}
}

// 为指定 Go 对象类型注册声明式校验函数。调用 Validate 或 ValidateUpdate 时按对象类型分派；同一类型重复注册时以后一次注册为准。
func (s *Scheme) AddValidationFunc(srcType Object, fn func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList) {
	s.validationFuncs[reflect.TypeOf(srcType)] = fn
}

// 执行该对象类型已注册的生成式声明校验函数，并以创建操作构造校验上下文。
// 注意：此入口不会运行手写 validation.go 中的校验逻辑，只运行生成的 zz_generated.validations.go 校验代码。
func (s *Scheme) Validate(ctx context.Context, options map[string]bool, object Object, subresources ...string) field.ErrorList {
	if fn, ok := s.validationFuncs[reflect.TypeOf(object)]; ok {
		return fn(ctx, operation.Operation{Type: operation.Create, Request: operation.Request{Subresources: subresources}, Options: options}, object, nil)
	}
	return nil
}

// 执行对象更新场景的生成式声明校验，同时传入新对象与旧对象，以支持状态转换规则和不可变字段检查。
// 注意：此入口同样只运行生成的声明式校验代码，不会执行手写 validation.go 中的校验逻辑。
func (s *Scheme) ValidateUpdate(ctx context.Context, options map[string]bool, object, oldObject Object, subresources ...string) field.ErrorList {
	if fn, ok := s.validationFuncs[reflect.TypeOf(object)]; ok {
		return fn(ctx, operation.Operation{Type: operation.Update, Request: operation.Request{Subresources: subresources}, Options: options}, object, oldObject)
	}
	return nil
}

// 判断对象的 Go 类型是否注册了校验函数。它可以区分“没有注册校验函数”和“校验函数执行后没有错误”这两种都会返回空错误列表的情况。
func (s *Scheme) HasValidationFunc(obj Object) bool {
	_, ok := s.validationFuncs[reflect.TypeOf(obj)]
	return ok
}

// 尝试把输入对象转换到输出对象。输入和输出都必须是指针；转换器按已注册规则寻找转换路径，并将 context 透传给转换函数。此方法也允许测试尚未单独注册、但嵌套在已注册类型中的结构。
func (s *Scheme) Convert(in, out interface{}, context interface{}) error {
	unstructuredIn, okIn := in.(Unstructured)
	unstructuredOut, okOut := out.(Unstructured)
	switch {
	case okIn && okOut:
		// converting unstructured input to an unstructured output is a straight copy - unstructured
		// is a "smart holder" and the contents are passed by reference between the two objects
		unstructuredOut.SetUnstructuredContent(unstructuredIn.UnstructuredContent())
		return nil

	case okOut:
		// if the output is an unstructured object, use the standard Go type to unstructured
		// conversion. The object must not be internal.
		obj, ok := in.(Object)
		if !ok {
			return fmt.Errorf("unable to convert object type %T to Unstructured, must be a runtime.Object", in)
		}
		gvks, unversioned, err := s.ObjectKinds(obj)
		if err != nil {
			return err
		}
		gvk := gvks[0]

		// 若无需转换（unversioned 或版本不是 internal），立即转换。
		// internal 对象不能直接暴露给用户（可能含未对外开放的字段），
		// 必须先转到外部版本。
		if unversioned || gvk.Version != APIVersionInternal {
			content, err := DefaultUnstructuredConverter.ToUnstructured(in)
			if err != nil {
				return err
			}
			unstructuredOut.SetUnstructuredContent(content)
			unstructuredOut.GetObjectKind().SetGroupVersionKind(gvk)
			return nil
		}

		// attempt to convert the object to an external version first.
		target, ok := context.(GroupVersioner)
		if !ok {
			return fmt.Errorf("unable to convert the internal object type %T to Unstructured without providing a preferred version to convert to", in)
		}
		// Convert is implicitly unsafe, so we don't need to perform a safe conversion
		versioned, err := s.UnsafeConvertToVersion(obj, target)
		if err != nil {
			return err
		}
		content, err := DefaultUnstructuredConverter.ToUnstructured(versioned)
		if err != nil {
			return err
		}
		unstructuredOut.SetUnstructuredContent(content)
		return nil

	case okIn:
		// converting an unstructured object to any type is modeled by first converting
		// the input to a versioned type, then running standard conversions
		typed, err := s.unstructuredToTyped(unstructuredIn)
		if err != nil {
			return err
		}
		in = typed
	}

	meta := s.generateConvertMeta(in)
	meta.Context = context
	return s.converter.Convert(in, out, meta)
}

// 将指定 GVK 下的字段选择器名称和值转换为内部版本表示。若该 GVK 没有注册转换函数，则返回错误，避免静默改变字段语义。
func (s *Scheme) ConvertFieldLabel(gvk schema.GroupVersionKind, label, value string) (string, string, error) {
	// 未注册专用转换时退回 metav1 的通用字段选择器规则。
	conversionFunc, ok := s.fieldLabelConversionFuncs[gvk]
	if !ok {
		return DefaultMetaV1FieldSelectorConversion(label, value)
	}
	return conversionFunc(label, value)
}

// 将输入对象转换为目标 GroupVersioner 所选择的版本。流程包括识别输入对象的 GVK、确定目标 GVK、创建目标对象并执行转换；若目标版本不包含对应 Kind、转换失败或结果不是有效 Object，则返回错误。
func (s *Scheme) ConvertToVersion(in Object, target GroupVersioner) (Object, error) {
	return s.convertToVersion(true, in, target)
}

// 执行尽可能少分配的版本转换，但不保证输出对象与输入对象完全隔离，二者可能共享底层字段。只有调用方确认不会因修改输出而影响输入时才应使用。
func (s *Scheme) UnsafeConvertToVersion(in Object, target GroupVersioner) (Object, error) {
	return s.convertToVersion(false, in, target)
}

// convertToVersion handles conversion with an optional copy.
func (s *Scheme) convertToVersion(copy bool, in Object, target GroupVersioner) (Object, error) {
	var t reflect.Type

	if u, ok := in.(Unstructured); ok {
		typed, err := s.unstructuredToTyped(u)
		if err != nil {
			return nil, err
		}

		in = typed
		// unstructuredToTyped returns an Object, which must be a pointer to a struct.
		t = reflect.TypeOf(in).Elem()

	} else {
		// determine the incoming kinds with as few allocations as possible.
		t = reflect.TypeOf(in)
		if t.Kind() != reflect.Pointer {
			return nil, fmt.Errorf("only pointer types may be converted: %v", t)
		}
		t = t.Elem()
		if t.Kind() != reflect.Struct {
			return nil, fmt.Errorf("only pointers to struct types may be converted: %v", t)
		}
	}

	kinds, ok := s.typeToGVK[t]
	if !ok || len(kinds) == 0 {
		return nil, NewNotRegisteredErrForType(s.schemeName, t)
	}

	gvk, ok := target.KindForGroupVersionKinds(kinds)
	if !ok {
		// try to see if this type is listed as unversioned (for legacy support)
		// TODO: when we move to server API versions, we should completely remove the unversioned concept
		if unversionedKind, ok := s.unversionedTypes[t]; ok {
			if gvk, ok := target.KindForGroupVersionKinds([]schema.GroupVersionKind{unversionedKind}); ok {
				return copyAndSetTargetKind(copy, in, gvk)
			}
			return copyAndSetTargetKind(copy, in, unversionedKind)
		}
		return nil, NewNotRegisteredErrForTarget(s.schemeName, t, target)
	}

	// target wants to use the existing type, set kind and return (no conversion necessary)
	for _, kind := range kinds {
		if gvk == kind {
			return copyAndSetTargetKind(copy, in, gvk)
		}
	}

	// type is unversioned, no conversion necessary
	if unversionedKind, ok := s.unversionedTypes[t]; ok {
		if gvk, ok := target.KindForGroupVersionKinds([]schema.GroupVersionKind{unversionedKind}); ok {
			return copyAndSetTargetKind(copy, in, gvk)
		}
		return copyAndSetTargetKind(copy, in, unversionedKind)
	}

	out, err := s.New(gvk)
	if err != nil {
		return nil, err
	}

	// unsafe 路径不复制输入；其余路径必须先深拷贝，避免转换过程污染调用方对象。
	if copy {
		in = in.DeepCopyObject()
	}

	meta := s.generateConvertMeta(in)
	meta.Context = target
	if err := s.converter.Convert(in, out, meta); err != nil {
		return nil, err
	}

	setTargetKind(out, gvk)
	return out, nil
}

// unstructuredToTyped attempts to transform an unstructured object to a typed
// object if possible. It will return an error if conversion is not possible, or the versioned
// Go form of the object. Note that this conversion will lose fields.
func (s *Scheme) unstructuredToTyped(in Unstructured) (Object, error) {
	// the type must be something we recognize
	gvks, _, err := s.ObjectKinds(in)
	if err != nil {
		return nil, err
	}
	typed, err := s.New(gvks[0])
	if err != nil {
		return nil, err
	}
	// FromUnstructured 只填充 Struct 中存在的字段，其余数据会被丢弃。
	if err := DefaultUnstructuredConverter.FromUnstructured(in.UnstructuredContent(), typed); err != nil {
		return nil, fmt.Errorf("unable to convert unstructured object to %v: %v", gvks[0], err)
	}
	return typed, nil
}

// generateConvertMeta constructs the meta value we pass to Convert.
func (s *Scheme) generateConvertMeta(in interface{}) *conversion.Meta {
	return s.converter.DefaultMeta(reflect.TypeOf(in))
}

// copyAndSetTargetKind performs a conditional copy before returning the object, or an error if copy was not successful.
func copyAndSetTargetKind(copy bool, obj Object, kind schema.GroupVersionKind) (Object, error) {
	if copy {
		obj = obj.DeepCopyObject()
	}
	setTargetKind(obj, kind)
	return obj, nil
}

// setTargetKind sets the kind on an object, taking into account whether the target kind is the internal version.
func setTargetKind(obj Object, kind schema.GroupVersionKind) {
	if kind.Version == APIVersionInternal {
		// internal 版本是一种特殊情况。
		// TODO: look at removing the need to special case this
		// internal 对象不应暴露 GVK，清空以避免序列化时写入伪版本。
		obj.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
		return
	}
	obj.GetObjectKind().SetGroupVersionKind(kind)
}

// 为同一个 API Group 设置明确的版本优先级顺序。所有参数必须属于同一 Group；本次传入顺序会覆盖该 Group 之前设置的优先级。
func (s *Scheme) SetVersionPriority(versions ...schema.GroupVersion) error {
	groups := sets.String{}
	order := []string{}
	for _, version := range versions {
		// internal 不是真实的外部版本，参与优先级排序没有意义。
		if len(version.Version) == 0 || version.Version == APIVersionInternal {
			return fmt.Errorf("internal versions cannot be prioritized: %v", version)
		}

		groups.Insert(version.Group)
		order = append(order, version.Version)
	}
	if len(groups) != 1 {
		return fmt.Errorf("must register versions for exactly one group: %v", strings.Join(groups.List(), ", "))
	}

	s.versionPriority[groups.List()[0]] = order
	return nil
}

// 返回指定 Group 的版本优先级列表。优先返回显式设置的版本，再按观察到的注册顺序补充尚未列出的版本，避免遗漏已注册版本。
func (s *Scheme) PrioritizedVersionsForGroup(group string) []schema.GroupVersion {
	ret := []schema.GroupVersion{}
	for _, version := range s.versionPriority[group] {
		ret = append(ret, schema.GroupVersion{Group: group, Version: version})
	}
	for _, observedVersion := range s.observedVersions {
		if observedVersion.Group != group {
			continue
		}
		found := false
		for _, existing := range ret {
			if existing == observedVersion {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, observedVersion)
		}
	}

	return ret
}

// PrioritizedVersionsAllGroups 按优先级顺序返回所有已知版本。组是随机的，但
// 单个组的版本按优先级排序
func (s *Scheme) PrioritizedVersionsAllGroups() []schema.GroupVersion {
	ret := []schema.GroupVersion{}
	for group, versions := range s.versionPriority {
		for _, version := range versions {
			ret = append(ret, schema.GroupVersion{Group: group, Version: version})
		}
	}
	for _, observedVersion := range s.observedVersions {
		found := false
		for _, existing := range ret {
			if existing == observedVersion {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, observedVersion)
		}
	}
	return ret
}

// 为每个已注册的 Group 返回优先级最高的版本。不同 Group 之间的排列顺序不保证稳定。
func (s *Scheme) PreferredVersionAllGroups() []schema.GroupVersion {
	ret := []schema.GroupVersion{}
	for group, versions := range s.versionPriority {
		for _, version := range versions {
			// 只取每组优先级最高的一个版本，随后 break。
			ret = append(ret, schema.GroupVersion{Group: group, Version: version})
			break
		}
	}
	// 未显式设置优先级的组，用注册顺序中最早出现的版本作为首选。
	for _, observedVersion := range s.observedVersions {
		found := false
		for _, existing := range ret {
			if existing.Group == observedVersion.Group {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, observedVersion)
		}
	}

	return ret
}

// 判断 Scheme 是否至少注册过指定 API Group 的类型。
func (s *Scheme) IsGroupRegistered(group string) bool {
	for _, observedVersion := range s.observedVersions {
		if observedVersion.Group == group {
			return true
		}
	}
	return false
}

// 判断 Scheme 是否注册过指定 GroupVersion 下的类型。
func (s *Scheme) IsVersionRegistered(version schema.GroupVersion) bool {
	for _, observedVersion := range s.observedVersions {
		if observedVersion == version {
			return true
		}
	}

	return false
}

// addObservedVersion 记录首次出现的非 internal 版本，用于后续补全优先级
// 列表；internal 版本不是真实的外部版本，不参与。
func (s *Scheme) addObservedVersion(version schema.GroupVersion) {
	if len(version.Version) == 0 || version.Version == APIVersionInternal {
		return
	}
	for _, observedVersion := range s.observedVersions {
		if observedVersion == version {
			return
		}
	}

	s.observedVersions = append(s.observedVersions, version)
}

// Name 返回构造时推断出的 scheme 名称，用于错误信息。
func (s *Scheme) Name() string {
	return s.schemeName
}

// internalPackages are packages that ignored when creating a default reflector name. These packages are in the common
// call chains to NewReflector, so they'd be low entropy names for reflectors
var internalPackages = []string{"k8s.io/apimachinery/pkg/runtime/scheme.go"}

// 根据 GVK 查找对应类型的 OpenAPI 定义名称。对象必须已注册；Version 必须提供，Unstructured 类型还必须提供 Group 与 Kind。
func (s *Scheme) ToOpenAPIDefinitionName(groupVersionKind schema.GroupVersionKind) (string, error) {
	if groupVersionKind.Version == "" { // Empty version is not allowed by New() so check it first to avoid a panic.
		return "", fmt.Errorf("version is required on all types: %v", groupVersionKind)
	}
	example, err := s.New(groupVersionKind)
	if err != nil {
		return "", err
	}

	// Use a namer if provided
	if namer, ok := example.(util.OpenAPIModelNamer); ok {
		return namer.OpenAPIModelName(), nil
	}

	if _, ok := example.(Unstructured); ok {
		if groupVersionKind.Group == "" || groupVersionKind.Kind == "" {
			return "", fmt.Errorf("unable to convert GroupVersionKind with empty fields to unstructured type to an OpenAPI definition name: %v", groupVersionKind)
		}
		return reverseParts(groupVersionKind.Group) + "." + groupVersionKind.Version + "." + groupVersionKind.Kind, nil
	}
	rtype := reflect.TypeOf(example).Elem()
	name := toOpenAPIDefinitionName(rtype.PkgPath() + "." + rtype.Name())
	return name, nil
}

// toOpenAPIDefinitionName converts Golang package/type canonical name into REST friendly OpenAPI name.
// Input is expected to be `PkgPath + "." TypeName.
//
// Examples of REST friendly OpenAPI name:
//
//	Input:  k8s.io/api/core/v1.Pod
//	Output: io.k8s.api.core.v1.Pod
//
//	Input:  k8s.io/api/core/v1
//	Output: io.k8s.api.core.v1
//
//	Input:  csi.storage.k8s.io/v1alpha1.CSINodeInfo
//	Output: io.k8s.storage.csi.v1alpha1.CSINodeInfo
//
// Note that this is a copy of ToRESTFriendlyName from k8s.io/kube-openapi/pkg/util. It is duplicated here to avoid
// a dependency on kube-openapi.
func toOpenAPIDefinitionName(name string) string {
	nameParts := strings.Split(name, "/")
	// 反转第一段域名成 REST 风格（例如 io.k8s... instead of k8s.io...），其余路径段只做 "." 连接。
	if len(nameParts) > 0 && strings.Contains(nameParts[0], ".") {
		nameParts[0] = reverseParts(nameParts[0])
	}
	return strings.Join(nameParts, ".")
}

// reverseParts 反转点分隔名称的各段，例如 "k8s.io" -> "io.k8s"。
func reverseParts(dotSeparatedName string) string {
	parts := strings.Split(dotSeparatedName, ".")
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, ".")
}
