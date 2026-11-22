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

// Scheme defines methods for serializing and deserializing API objects, a type
// registry for converting group, version, and kind information to and from Go
// schemas, and mappings between Go schemas of different versions. A scheme is the
// foundation for a versioned API and versioned configuration over time.
//
// In a Scheme, a Type is a particular Go struct, a Version is a point-in-time
// identifier for a particular representation of that Type (typically backwards
// compatible), a Kind is the unique name for that Type within the Version, and a
// Group identifies a set of Versions, Kinds, and Types that evolve over time. An
// Unversioned Type is one that is not yet formally bound to a type and is promised
// to be backwards compatible (effectively a "v1" of a Type that does not expect
// to break in the future).
//
// Schemes are not expected to change at runtime and are only threadsafe after
// registration is complete.
//
// 中文说明：Scheme 是版本化 API 的核心注册表——把 Go 类型、GVK、转换函数、
// 默认值函数与校验函数绑定在一起。注册阶段完成后只读，因此并发读取安全；
// 运行期再注册会引入数据竞争。
type Scheme struct {
	// gvkToType allows one to figure out the go type of an object with
	// the given version and name.
	gvkToType map[schema.GroupVersionKind]reflect.Type

	// typeToGVK allows one to find metadata for a given go object.
	// The reflect.Type we index by should *not* be a pointer.
	typeToGVK map[reflect.Type][]schema.GroupVersionKind

	// unversionedTypes are transformed without conversion in ConvertToVersion.
	// 中文说明：unversioned 类型只改 GVK、不做结构转换，因此永远向后兼容。
	unversionedTypes map[reflect.Type]schema.GroupVersionKind

	// unversionedKinds are the names of kinds that can be created in the context of any group
	// or version
	// TODO: resolve the status of unversioned types.
	unversionedKinds map[string]reflect.Type

	// Map from version and resource to the corresponding func to convert
	// resource field labels in that version to internal version.
	fieldLabelConversionFuncs map[schema.GroupVersionKind]FieldLabelConversionFunc

	// defaulterFuncs is a map to funcs to be called with an object to provide defaulting
	// the provided object must be a pointer.
	defaulterFuncs map[reflect.Type]func(interface{})

	// validationFuncs is a map to funcs to be called with an object to perform validation.
	// The provided object must be a pointer.
	// If oldObject is non-nil, update validation is performed and may perform additional
	// validation such as transition rules and immutability checks.
	validationFuncs map[reflect.Type]func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList

	// converter stores all registered conversion functions. It also has
	// default converting behavior.
	converter *conversion.Converter

	// versionPriority is a map of groups to ordered lists of versions for those groups indicating the
	// default priorities of these versions as registered in the scheme
	versionPriority map[string][]string

	// observedVersions keeps track of the order we've seen versions during type registration
	// 中文说明：用于在没有显式 SetVersionPriority 时，按注册顺序补全版本列表。
	observedVersions []schema.GroupVersion

	// schemeName is the name of this scheme.  If you don't specify a name, the stack of the NewScheme caller will be used.
	// This is useful for error reporting to indicate the origin of the scheme.
	schemeName string
}

// FieldLabelConversionFunc converts a field selector to internal representation.
type FieldLabelConversionFunc func(label, value string) (internalLabel, internalValue string, err error)

// NewScheme creates a new Scheme. This scheme is pluggable by default.
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

// Converter allows access to the converter for the scheme
func (s *Scheme) Converter() *conversion.Converter {
	return s.converter
}

// AddUnversionedTypes registers the provided types as "unversioned", which means that they follow special rules.
// Whenever an object of this type is serialized, it is serialized with the provided group version and is not
// converted. Thus unversioned objects are expected to remain backwards compatible forever, as if they were in an
// API group and version that would never be updated.
//
// TODO: there is discussion about removing unversioned and replacing it with objects that are manifest into
// every version with particular schemas. Resolve this method at that point.
//
// 中文说明：同一 kind 名称只能对应一个 unversioned 类型，冲突时 panic——
// 否则解码时无法确定该构造哪个 Go 类型。
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

// AddKnownTypes registers all types passed in 'types' as being members of version 'version'.
// All objects passed to types should be pointers to structs. The name that go reports for
// the struct becomes the "kind" field when encoding. Version may not be empty - use the
// APIVersionInternal constant if you have a type that does not have a formal version.
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

// AddKnownTypeWithName is like AddKnownTypes, but it lets you specify what this type should
// be encoded as. Useful for testing when you don't want to make multiple packages to define
// your structs. Version may not be empty - use the APIVersionInternal constant if you have a
// type that does not have a formal version.
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

	// if the type implements DeepCopyInto(<obj>), register a self-conversion
	// 中文说明：同类型转换直接复用 DeepCopyInto，并清空 TypeMeta，与反射
	// 转换的旧行为保持一致。
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

// KnownTypes returns the types known for the given version.
// 中文说明：返回 kind -> Go 类型的映射，用于按版本构造对象或反射检查。
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

// VersionsForGroupKind returns the versions that a particular GroupKind can be converted to within the given group.
// A GroupKind might be converted to a different group. That information is available in EquivalentResourceMapper.
//
// 中文说明：先按优先级顺序遍历已知版本，再筛选该 GroupKind 实际注册过的
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

// AllKnownTypes returns the all known types.
func (s *Scheme) AllKnownTypes() map[schema.GroupVersionKind]reflect.Type {
	return s.gvkToType
}

// ObjectKinds returns all possible group,version,kind of the go object, true if the
// object is considered unversioned, or an error if it's not a pointer or is unregistered.
func (s *Scheme) ObjectKinds(obj Object) ([]schema.GroupVersionKind, bool, error) {
	// Unstructured objects are always considered to have their declared GVK
	// 中文说明：unstructured 对象不在注册表中，只能相信其内部声明的 GVK；
	// 因此 kind/version 缺失时报错而不是猜测。
	if _, ok := obj.(Unstructured); ok {
		// we require that the GVK be populated in order to recognize the object
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

// Recognizes returns true if the scheme is able to handle the provided group,version,kind
// of an object.
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

// New returns a new API object of the given version and name, or an error if it hasn't
// been registered. The version and kind fields must be specified.
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

// AddIgnoredConversionType identifies a pair of types that should be skipped by
// conversion (because the data inside them is explicitly dropped during
// conversion).
func (s *Scheme) AddIgnoredConversionType(from, to interface{}) error {
	return s.converter.RegisterIgnoredConversion(from, to)
}

// AddConversionFunc registers a function that converts between a and b by passing objects of those
// types to the provided function. The function *must* accept objects of a and b - this machinery will not enforce
// any other guarantee.
func (s *Scheme) AddConversionFunc(a, b interface{}, fn conversion.ConversionFunc) error {
	return s.converter.RegisterUntypedConversionFunc(a, b, fn)
}

// AddGeneratedConversionFunc registers a function that converts between a and b by passing objects of those
// types to the provided function. The function *must* accept objects of a and b - this machinery will not enforce
// any other guarantee.
//
// 中文说明：generated 转换函数由 conversion-gen 产出，与手写转换分开登记，
// 以便审计哪些转换是自动生成的。
func (s *Scheme) AddGeneratedConversionFunc(a, b interface{}, fn conversion.ConversionFunc) error {
	return s.converter.RegisterGeneratedUntypedConversionFunc(a, b, fn)
}

// AddFieldLabelConversionFunc adds a conversion function to convert field selectors
// of the given kind from the given version to internal version representation.
func (s *Scheme) AddFieldLabelConversionFunc(gvk schema.GroupVersionKind, conversionFunc FieldLabelConversionFunc) error {
	s.fieldLabelConversionFuncs[gvk] = conversionFunc
	return nil
}

// AddTypeDefaultingFunc registers a function that is passed a pointer to an
// object and can default fields on the object. These functions will be invoked
// when Default() is called. The function will never be called unless the
// defaulted object matches srcType. If this function is invoked twice with the
// same srcType, the fn passed to the later call will be used instead.
func (s *Scheme) AddTypeDefaultingFunc(srcType Object, fn func(interface{})) {
	s.defaulterFuncs[reflect.TypeOf(srcType)] = fn
}

// Default sets defaults on the provided Object.
func (s *Scheme) Default(src Object) {
	// 只用指针类型做键，保证默认值函数能原地修改对象。
	if fn, ok := s.defaulterFuncs[reflect.TypeOf(src)]; ok {
		fn(src)
	}
}

// AddValidationFunc registered a function that can validate the object, and
// oldObject. These functions will be invoked when Validate() or ValidateUpdate()
// is called. The function will never be called unless the validated object
// matches srcType. If this function is invoked twice with the same srcType, the
// fn passed to the later call will be used instead.
func (s *Scheme) AddValidationFunc(srcType Object, fn func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList) {
	s.validationFuncs[reflect.TypeOf(srcType)] = fn
}

// Validate validates the provided Object according to the generated declarative validation code.
// WARNING: This does not validate all objects!  The handwritten validation code in validation.go
// is not run when this is called.  Only the generated zz_generated.validations.go validation code is run.
func (s *Scheme) Validate(ctx context.Context, options map[string]bool, object Object, subresources ...string) field.ErrorList {
	if fn, ok := s.validationFuncs[reflect.TypeOf(object)]; ok {
		return fn(ctx, operation.Operation{Type: operation.Create, Request: operation.Request{Subresources: subresources}, Options: options}, object, nil)
	}
	return nil
}

// ValidateUpdate validates the provided object and oldObject according to the generated declarative validation code.
// WARNING: This does not validate all objects!  The handwritten validation code in validation.go
// is not run when this is called.  Only the generated zz_generated.validations.go validation code is run.
func (s *Scheme) ValidateUpdate(ctx context.Context, options map[string]bool, object, oldObject Object, subresources ...string) field.ErrorList {
	if fn, ok := s.validationFuncs[reflect.TypeOf(object)]; ok {
		return fn(ctx, operation.Operation{Type: operation.Update, Request: operation.Request{Subresources: subresources}, Options: options}, object, oldObject)
	}
	return nil
}

// HasValidationFunc reports whether a validation function is registered for the
// object's type. Unlike Validate, it distinguishes "no function registered" from
// "function ran and found no errors", which both yield a nil error list.
func (s *Scheme) HasValidationFunc(obj Object) bool {
	_, ok := s.validationFuncs[reflect.TypeOf(obj)]
	return ok
}

// Convert will attempt to convert in into out. Both must be pointers. For easy
// testing of conversion functions. Returns an error if the conversion isn't
// possible. You can call this with types that haven't been registered (for example,
// a to test conversion of types that are nested within registered types). The
// context interface is passed to the convertor. Convert also supports Unstructured
// types and will convert them intelligently.
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

		// if no conversion is necessary, convert immediately
		// 中文说明：internal 对象不能直接暴露给用户（可能含未对外开放的字段），
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

// ConvertFieldLabel alters the given field label and value for an kind field selector from
// versioned representation to an unversioned one or returns an error.
func (s *Scheme) ConvertFieldLabel(gvk schema.GroupVersionKind, label, value string) (string, string, error) {
	// 未注册专用转换时退回 metav1 的通用字段选择器规则。
	conversionFunc, ok := s.fieldLabelConversionFuncs[gvk]
	if !ok {
		return DefaultMetaV1FieldSelectorConversion(label, value)
	}
	return conversionFunc(label, value)
}

// ConvertToVersion attempts to convert an input object to its matching Kind in another
// version within this scheme. Will return an error if the provided version does not
// contain the inKind (or a mapping by name defined with AddKnownTypeWithName). Will also
// return an error if the conversion does not result in a valid Object being
// returned. Passes target down to the conversion methods as the Context on the scope.
func (s *Scheme) ConvertToVersion(in Object, target GroupVersioner) (Object, error) {
	return s.convertToVersion(true, in, target)
}

// UnsafeConvertToVersion will convert in to the provided target if such a conversion is possible,
// but does not guarantee the output object does not share fields with the input object. It attempts to be as
// efficient as possible when doing conversion.
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
		// internal is a special case
		// TODO: look at removing the need to special case this
		// 中文说明：internal 对象不应暴露 GVK，清空以避免序列化时写入伪版本。
		obj.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
		return
	}
	obj.GetObjectKind().SetGroupVersionKind(kind)
}

// SetVersionPriority allows specifying a precise order of priority. All specified versions must be in the same group,
// and the specified order overwrites any previously specified order for this group
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

// PrioritizedVersionsForGroup returns versions for a single group in priority order
// 中文说明：先返回显式设置的优先级顺序，再按注册顺序追加未列出的版本，
// 保证不遗漏且结果稳定。
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

// PreferredVersionAllGroups returns the most preferred version for every group.
// group ordering is random.
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

// IsGroupRegistered returns true if types for the group have been registered with the scheme
func (s *Scheme) IsGroupRegistered(group string) bool {
	for _, observedVersion := range s.observedVersions {
		if observedVersion.Group == group {
			return true
		}
	}
	return false
}

// IsVersionRegistered returns true if types for the version have been registered with the scheme
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

// ToOpenAPIDefinitionName returns the REST-friendly OpenAPI definition name known type identified by groupVersionKind.
// If the groupVersionKind does not identify a known type, an error is returned.
// The Version field of groupVersionKind is required, and the Group and Kind fields are required for unstructured.Unstructured
// types. If a required field is empty, an error is returned.
//
// The OpenAPI definition name is the canonical name of the type, with the group and version removed.
// For example, the OpenAPI definition name of Pod is `io.k8s.api.core.v1.Pod`.
//
// This respects the util.OpenAPIModelNamer interface and will return the name returned by
// OpenAPIModelName() if it is defined on the type.
//
// A known type that is registered as an unstructured.Unstructured type is treated as a custom resource and
// which has an OpenAPI definition name of the form `<reversed-group>.<version.<kind>`.
// For example, the OpenAPI definition name of `group: stable.example.com, version: v1, kind: Pod` is
// `com.example.stable.v1.Pod`.
//
// 中文说明：优先使用类型自定义的 OpenAPIModelName；其次对 unstructured
// （自定义资源）按反转 group 的约定拼名；最后从 PkgPath 推导内置类型名。
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
	// Reverse first part. e.g., io.k8s... instead of k8s.io...
	// 中文说明：域名部分需要反转成 REST 风格，其余路径段只做 "." 连接。
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
