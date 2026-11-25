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

package protobuf

import (
	"fmt"
	"io"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"

	genutil "k8s.io/code-generator/pkg/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// genProtoIDL produces a .proto IDL.
//
// 输出的是 proto2 语法的消息定义；每个 Go 输入包对应一个 .proto 文件。
// omitGogo 用于生成不依赖 gogoproto 扩展的“干净” IDL。
type genProtoIDL struct {
	// This base type is close enough to what we need, if we redefine some
	// methods.
	generator.GoGenerator
	localPackage   types.Name
	localGoPackage types.Name
	imports        namer.ImportTracker

	generateAll    bool
	omitGogo       bool
	omitFieldTypes map[types.Name]struct{}
}

// PackageVars 输出 .proto 的 option 段。未 omitGogo 时打开 gogoproto 的
// marshaler/sizer/unmarshaler 生成，关闭其自带的 stringer/getter/unrecognized，
// 与 k8s 现有的 proto 代码约定一致。
func (g *genProtoIDL) PackageVars(c *generator.Context) []string {
	if g.omitGogo {
		return []string{
			fmt.Sprintf("option go_package = %q;", g.localGoPackage.Package),
		}
	}
	return []string{
		"option (gogoproto.marshaler_all) = true;",
		"option (gogoproto.stable_marshaler_all) = true;",
		"option (gogoproto.sizer_all) = true;",
		"option (gogoproto.goproto_stringer_all) = false;",
		"option (gogoproto.stringer_all) = true;",
		"option (gogoproto.unmarshaler_all) = true;",
		"option (gogoproto.goproto_unrecognized_all) = false;",
		"option (gogoproto.goproto_enum_prefix_all) = false;",
		"option (gogoproto.goproto_getters_all) = false;",
		fmt.Sprintf("option go_package = %q;", g.localGoPackage.Package),
	}
}

// Filename 在同一输出文件名后加 .proto 后缀，避免与 Go 输出冲突。
func (g *genProtoIDL) Filename() string { return g.OutputFilename + ".proto" }

// FileType 告诉 gengo 使用 proto IDL 的组装逻辑（见 NewProtoFile）。
func (g *genProtoIDL) FileType() string { return "protoidl" }

// Namers 使用 local namer：proto 类型名需按 package 规则渲染，而非 Go 的限定名。
func (g *genProtoIDL) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		// The local namer returns the correct protobuf name for a proto type
		// in the context of a package
		"local": localNamer{g.localPackage},
	}
}

// Filter 忽略被判定为不可导出的类型。
// 类型级 +protobuf=true/false 优先于包级 generateAll；未显式标注时，
// 只有可被 proto 表达的类型才会输出。
func (g *genProtoIDL) Filter(c *generator.Context, t *types.Type) bool {
	tags, err := genutil.ExtractCommentTagsWithoutArguments("+", []string{"protobuf"}, t.CommentLines)
	if err != nil {
		klog.Fatalf(`Error extracting tag "protobuf": %v`, err)
	}
	if tags["protobuf"] != nil {
		if tags["protobuf"][0] == "false" {
			// Type specified "false".
			return false
		}
		if tags["protobuf"][0] == "true" {
			// Type specified "true".
			return true
		}
		klog.Fatalf(`Comment tag "protobuf" must be true or false, found: %q`, tags["protobuf"][0])
	}
	if !g.generateAll {
		// We're not generating everything.
		return false
	}
	seen := map[*types.Type]bool{}
	ok := isProtoable(seen, t)
	return ok
}

// isProtoable 递归判断类型能否用 proto 表示。
// seen 用于在类型环上乐观地返回 true：真正不可表示的类型会在其他分支被拒；
// struct 只要有一个成员可表示即可（其余成员可能被 protobuf:"-" 排除）。
func isProtoable(seen map[*types.Type]bool, t *types.Type) bool {
	if seen[t] {
		// be optimistic in the case of type cycles.
		return true
	}
	seen[t] = true
	switch t.Kind {
	case types.Builtin:
		return true
	case types.Alias:
		return isProtoable(seen, t.Underlying)
	case types.Slice, types.Pointer:
		return isProtoable(seen, t.Elem)
	case types.Map:
		return isProtoable(seen, t.Key) && isProtoable(seen, t.Elem)
	case types.Struct:
		if len(t.Members) == 0 {
			return true
		}
		for _, m := range t.Members {
			if isProtoable(seen, m.Type) {
				return true
			}
		}
		return false
	case types.Func, types.Chan:
		return false
	case types.DeclarationOf, types.Unknown, types.Unsupported:
		return false
	case types.Interface:
		return false
	default:
		log.Printf("WARNING: type %q is not portable: %s", t.Kind, t.Name)
		return false
	}
}

// isOptionalAlias should return true if the specified type has an underlying type
// (is an alias) of a map or slice and has the comment tag protobuf.nullable=true,
// indicating that the type should be nullable in protobuf.
func isOptionalAlias(t *types.Type) bool {
	if t.Underlying == nil || (t.Underlying.Kind != types.Map && t.Underlying.Kind != types.Slice) {
		return false
	}
	return extractBoolTagOrDie("protobuf.nullable", t.CommentLines)
}

// Imports 在 omitGogo 模式下剔除 gogoproto 的 import；否则残留的
// (gogoproto.*) 选项在 option 段已被移除，import 也必须一起移除。
func (g *genProtoIDL) Imports(c *generator.Context) (imports []string) {
	lines := []string{}
	// TODO: this could be expressed more cleanly
	for _, line := range g.imports.ImportLines() {
		if g.omitGogo && line == "github.com/gogo/protobuf/gogoproto/gogo.proto" {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

// GenerateType 生成实现类型 t 对应集合的文件主体。
// 别名与 struct 分别走 doAlias/doStruct；其他 Kind 说明生成器不应处理该类型。
func (g *genProtoIDL) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "$", "$")
	b := bodyGen{
		locator: &protobufLocator{
			namer:    c.Namers["proto"].(ProtobufFromGoNamer),
			tracker:  g.imports,
			universe: c.Universe,

			localGoPackage: g.localGoPackage.Package,
		},
		localPackage: g.localPackage,

		omitGogo:       g.omitGogo,
		omitFieldTypes: g.omitFieldTypes,

		t: t,
	}
	switch t.Kind {
	case types.Alias:
		return b.doAlias(sw)
	case types.Struct:
		return b.doStruct(sw)
	default:
		return b.unknown()
	}
}

// ProtobufFromGoNamer finds the protobuf name of a type (and its package, and
// the package path) from its Go name.
type ProtobufFromGoNamer interface {
	GoNameToProtoName(name types.Name) types.Name
}

// ProtobufLocator 提供 Go 类型 ↔ proto 类型之间的映射与命名策略。
type ProtobufLocator interface {
	ProtoTypeFor(t *types.Type) (*types.Type, error)
	GoTypeForName(name types.Name) *types.Type
	CastTypeName(name types.Name) string
}

// protobufLocator 是 ProtobufLocator 的实现，依赖 proto namer 与 import 追踪器。
type protobufLocator struct {
	namer    ProtobufFromGoNamer
	tracker  namer.ImportTracker
	universe types.Universe

	localGoPackage string
}

// CastTypeName 返回某个 Go 类型的 cast 类型名。
// TODO: 是否该交给新的 localgo namer 处理？
// 本包内类型只写短名；跨包类型写全限定名，供 gogoproto.casttype 使用。
func (p protobufLocator) CastTypeName(name types.Name) string {
	if name.Package == p.localGoPackage {
		return name.Name
	}
	return name.String()
}

// GoTypeForName 在未指定包时默认落到本包；用于 protobuf.as 的别名解析。
func (p protobufLocator) GoTypeForName(name types.Name) *types.Type {
	if len(name.Package) == 0 {
		name.Package = p.localGoPackage
	}
	return p.universe.Type(name)
}

// ProtoTypeFor locates a Protobuf type for the provided Go type (if possible).
func (p protobufLocator) ProtoTypeFor(t *types.Type) (*types.Type, error) {
	// we've already converted the type, or it's a map
	if t.Kind == types.Protobuf || t.Kind == types.Map {
		p.tracker.AddType(t)
		return t, nil
	}
	// it's a fundamental type
	if t, ok := isFundamentalProtoType(t); ok {
		p.tracker.AddType(t)
		return t, nil
	}
	// it's a message
	if t.Kind == types.Struct || isOptionalAlias(t) {
		t := &types.Type{
			Name: p.namer.GoNameToProtoName(t.Name),
			Kind: types.Protobuf,

			CommentLines: t.CommentLines,
		}
		p.tracker.AddType(t)
		return t, nil
	}
	return nil, errUnrecognizedType
}

// bodyGen 负责单个类型/消息的文本输出；locator 决定字段类型如何映射。
type bodyGen struct {
	locator        ProtobufLocator
	localPackage   types.Name
	omitGogo       bool
	omitFieldTypes map[types.Name]struct{}

	t *types.Type
}

func (b bodyGen) unknown() error {
	return fmt.Errorf("not sure how to generate: %#v", b.t)
}

// doAlias 只为标记了 protobuf.nullable=true 的 map/slice 别名生成消息：
// 把它们包装成含单个 Items 字段的 message，从而在 proto 中表达“可为空”。
func (b bodyGen) doAlias(sw *generator.SnippetWriter) error {
	if !isOptionalAlias(b.t) {
		return nil
	}

	var kind string
	switch b.t.Underlying.Kind {
	case types.Map:
		kind = "map"
	default:
		kind = "slice"
	}
	optional := &types.Type{
		Name: b.t.Name,
		Kind: types.Struct,

		CommentLines:              b.t.CommentLines,
		SecondClosestCommentLines: b.t.SecondClosestCommentLines,
		Members: []types.Member{
			{
				Name:         "Items",
				CommentLines: []string{fmt.Sprintf("items, if empty, will result in an empty %s\n", kind)},
				Type:         b.t.Underlying,
			},
		},
	}
	nested := b
	nested.t = optional
	return nested.doStruct(sw)
}

// doStruct 输出一个 message：先收集类型级 option 与 protobuf.as/embed 指令，
// 再按 Go struct 字段顺序逐行输出字段与字段级 option。
func (b bodyGen) doStruct(sw *generator.SnippetWriter) error {
	// 匿名类型与私有类型不产生可引用的消息名，直接跳过。
	if len(b.t.Name.Name) == 0 {
		return nil
	}
	if namer.IsPrivateGoName(b.t.Name.Name) {
		return nil
	}

	var alias *types.Type
	var fields []protoField
	options := []string{}
	allOptions := gengo.ExtractCommentTags("+", b.t.CommentLines)
	for k, v := range allOptions {
		switch {
		case strings.HasPrefix(k, "protobuf.options."):
			key := strings.TrimPrefix(k, "protobuf.options.")
			switch key {
			case "marshal":
				// marshal=false 需要同时关掉 unmarshaler/sizer，否则 gogo 生成代码不完整。
				if v[0] == "false" {
					if !b.omitGogo {
						options = append(options,
							"(gogoproto.marshaler) = false",
							"(gogoproto.unmarshaler) = false",
							"(gogoproto.sizer) = false",
						)
					}
				}
			default:
				if !b.omitGogo || !strings.HasPrefix(key, "(gogoproto.") {
					if key == "(gogoproto.goproto_stringer)" && v[0] == "false" {
						options = append(options, "(gogoproto.stringer) = false")
					}
					options = append(options, fmt.Sprintf("%s = %s", key, v[0]))
				}
			}
		// protobuf.as allows a type to have the same message contents as another Go type
		case k == "protobuf.as":
			fields = nil
			if alias = b.locator.GoTypeForName(types.Name{Name: v[0]}); alias == nil {
				return fmt.Errorf("type %v references alias %q which does not exist", b.t, v[0])
			}
		// protobuf.embed instructs the generator to use the named type in this package
		// as an embedded message.
		case k == "protobuf.embed":
			fields = []protoField{
				{
					Tag:  1,
					Name: v[0],
					Type: &types.Type{
						Name: types.Name{
							Name:    v[0],
							Package: b.localPackage.Package,
							Path:    b.localPackage.Path,
						},
					},
				},
			}
		}
	}
	if alias == nil {
		alias = b.t
	}

	// If we don't explicitly embed anything, generate fields by traversing fields.
	if fields == nil {
		memberFields, err := membersToFields(b.locator, alias, b.localPackage, b.omitFieldTypes)
		if err != nil {
			return fmt.Errorf("type %v cannot be converted to protobuf: %v", b.t, err)
		}
		fields = memberFields
	}

	out := sw.Out()
	genComment(out, b.t.CommentLines, "")
	sw.Do(`message $.Name.Name$ {
`, b.t)

	// option 排序输出，否则 map 遍历顺序会让生成结果在每次运行间抖动。
	if len(options) > 0 {
		sort.Strings(options)
		for _, s := range options {
			fmt.Fprintf(out, "  option %s;\n", s)
		}
		fmt.Fprintln(out)
	}

	for i, field := range fields {
		genComment(out, field.CommentLines, "  ")
		fmt.Fprintf(out, "  ")
		switch {
		case field.Map:
		case field.Repeated:
			fmt.Fprintf(out, "repeated ")
		case field.Required:
			fmt.Fprintf(out, "required ")
		default:
			fmt.Fprintf(out, "optional ")
		}
		sw.Do(`$.Type|local$ $.Name$ = $.Tag$`, field)
		if len(field.Extras) > 0 {
			// 同样排序，保持同一份输入产生逐字节可比的输出。
			extras := []string{}
			for k, v := range field.Extras {
				if b.omitGogo && strings.HasPrefix(k, "(gogoproto.") {
					continue
				}
				extras = append(extras, fmt.Sprintf("%s = %s", k, v))
			}
			sort.Strings(extras)
			if len(extras) > 0 {
				fmt.Fprintf(out, " [")
				fmt.Fprint(out, strings.Join(extras, ", "))
				fmt.Fprintf(out, "]")
			}
		}
		fmt.Fprintf(out, ";\n")
		if i != len(fields)-1 {
			fmt.Fprintf(out, "\n")
		}
	}
	fmt.Fprintf(out, "}\n\n")
	return nil
}

// protoField 描述最终写入 .proto 的一个字段；Tag 为 -1 表示尚未分配。
type protoField struct {
	LocalPackage types.Name

	Tag      int
	Name     string
	Type     *types.Type
	Map      bool
	Repeated bool
	Optional bool
	Required bool
	Nullable bool
	Extras   map[string]string

	CommentLines []string
}

var (
	// errUnrecognizedType 表示某成员类型无法用 proto 表达；调用方据此中止生成。
	errUnrecognizedType = fmt.Errorf("did not recognize the provided type")
)

// isFundamentalProtoType 把 Go 内建类型映射为 proto2 标量。
// 注意 int/uint 统一映射到 64 位，float64/float 映射 double、float32 映射 float，
// 这是 k8s proto 序列化格式的既定兼容性要求。
func isFundamentalProtoType(t *types.Type) (*types.Type, bool) {
	// TODO: when we enable proto3, also include other fundamental types in the google.protobuf package
	// switch {
	// case t.Kind == types.Struct && t.Name == types.Name{Package: "time", Name: "Time"}:
	// 	return &types.Type{
	// 		Kind: types.Protobuf,
	// 		Name: types.Name{Path: "google/protobuf/timestamp.proto", Package: "google.protobuf", Name: "Timestamp"},
	// 	}, true
	// }
	switch t.Kind {
	case types.Slice:
		if t.Elem.Name.Name == "byte" && len(t.Elem.Name.Package) == 0 {
			return &types.Type{Name: types.Name{Name: "bytes"}, Kind: types.Protobuf}, true
		}
	case types.Builtin:
		switch t.Name.Name {
		case "string", "uint32", "int32", "uint64", "int64", "bool":
			return &types.Type{Name: types.Name{Name: t.Name.Name}, Kind: types.Protobuf}, true
		case "int":
			return &types.Type{Name: types.Name{Name: "int64"}, Kind: types.Protobuf}, true
		case "uint":
			return &types.Type{Name: types.Name{Name: "uint64"}, Kind: types.Protobuf}, true
		case "float64", "float":
			return &types.Type{Name: types.Name{Name: "double"}, Kind: types.Protobuf}, true
		case "float32":
			return &types.Type{Name: types.Name{Name: "float"}, Kind: types.Protobuf}, true
		case "uintptr":
			return &types.Type{Name: types.Name{Name: "uint64"}, Kind: types.Protobuf}, true
		}
		// TODO: complex?
	}
	return t, false
}

// memberTypeToProtobufField 递归把 Go 成员类型映射为 protoField。
// 关键不变式：map 的 casttype 需要拆成 castkey/castvalue 分别挂在键和值上，
// 指针/可选别名设置 Nullable，否则 gogo 生成代码会把指针字段当作值类型。
func memberTypeToProtobufField(locator ProtobufLocator, field *protoField, t *types.Type) error {
	var err error
	switch t.Kind {
	case types.Protobuf:
		field.Type, err = locator.ProtoTypeFor(t)
	case types.Builtin:
		field.Type, err = locator.ProtoTypeFor(t)
	case types.Map:
		valueField := &protoField{}
		if err := memberTypeToProtobufField(locator, valueField, t.Elem); err != nil {
			return err
		}
		keyField := &protoField{}
		if err := memberTypeToProtobufField(locator, keyField, t.Key); err != nil {
			return err
		}
		// All other protobuf types have kind types.Protobuf, so setting types.Map
		// here would be very misleading.
		field.Type = &types.Type{
			Kind: types.Protobuf,
			Key:  keyField.Type,
			Elem: valueField.Type,
		}
		if !strings.HasPrefix(t.Name.Name, "map[") {
			field.Extras["(gogoproto.casttype)"] = strconv.Quote(locator.CastTypeName(t.Name))
		}
		if k, ok := keyField.Extras["(gogoproto.casttype)"]; ok {
			field.Extras["(gogoproto.castkey)"] = k
		}
		if v, ok := valueField.Extras["(gogoproto.casttype)"]; ok {
			field.Extras["(gogoproto.castvalue)"] = v
		}
		field.Map = true
	case types.Pointer:
		if err := memberTypeToProtobufField(locator, field, t.Elem); err != nil {
			return err
		}
		field.Nullable = true
	case types.Alias:
		if isOptionalAlias(t) {
			field.Type, err = locator.ProtoTypeFor(t)
			field.Nullable = true
		} else {
			if err := memberTypeToProtobufField(locator, field, t.Underlying); err != nil {
				log.Printf("failed to alias: %s %s: err %v", t.Name, t.Underlying.Name, err)
				return err
			}
			// If this is not an alias to a slice, cast to the alias
			if !field.Repeated {
				if field.Extras == nil {
					field.Extras = make(map[string]string)
				}
				field.Extras["(gogoproto.casttype)"] = strconv.Quote(locator.CastTypeName(t.Name))
			}
		}
	case types.Slice:
		if t.Elem.Name.Name == "byte" && len(t.Elem.Name.Package) == 0 {
			field.Type = &types.Type{Name: types.Name{Name: "bytes"}, Kind: types.Protobuf}
			return nil
		}
		if err := memberTypeToProtobufField(locator, field, t.Elem); err != nil {
			return err
		}
		field.Repeated = true
	case types.Struct:
		if len(t.Name.Name) == 0 {
			return errUnrecognizedType
		}
		field.Type, err = locator.ProtoTypeFor(t)
		field.Nullable = false
	default:
		return errUnrecognizedType
	}
	return err
}

// protobufTagToField 从已有的 protobuf 标签中提取信息。
// 手写 protobuf:"..." 标签可覆盖自动推导的 wire type、字段号与名称；
// 显式 wire type（varint/bytes 等）表示字段保持自动映射，只是编号/名称被固定。
func protobufTagToField(tag string, field *protoField, m types.Member, t *types.Type, localPackage types.Name) error {
	if len(tag) == 0 || tag == "-" {
		return nil
	}

	// protobuf:"bytes,3,opt,name=Id,customtype=github.com/gogo/protobuf/test.Uuid"
	parts := strings.Split(tag, ",")
	if len(parts) < 3 {
		return fmt.Errorf("member %q of %q malformed 'protobuf' tag, not enough segments", m.Name, t.Name)
	}
	protoTag, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("member %q of %q malformed 'protobuf' tag, field ID is %q which is not an integer: %w", m.Name, t.Name, parts[1], err)
	}
	field.Tag = protoTag

	// In general there is doesn't make sense to parse the protobuf tags to get the type,
	// as all auto-generated once will have wire type "bytes", "varint" or "fixed64".
	// However, sometimes we explicitly set them to have a custom serialization, e.g.:
	//   type Time struct {
	//     time.Time `protobuf:"Timestamp,1,req,name=time"`
	//   }
	// to force the generator to use a given type (that we manually wrote serialization &
	// deserialization methods for).
	switch parts[0] {
	case "varint", "fixed32", "fixed64", "bytes", "group":
	default:
		var name types.Name
		if last := strings.LastIndex(parts[0], "."); last != -1 {
			prefix := parts[0][:last]
			name = types.Name{
				Name:    parts[0][last+1:],
				Package: prefix,
				Path:    strings.ReplaceAll(prefix, ".", "/"),
			}
		} else {
			name = types.Name{
				Name:    parts[0],
				Package: localPackage.Package,
				Path:    localPackage.Path,
			}
		}
		field.Type = &types.Type{
			Name: name,
			Kind: types.Protobuf,
		}
	}

	protoExtra := make(map[string]string)
	for i, extra := range parts[3:] {
		parts := strings.SplitN(extra, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("member %q of %q malformed 'protobuf' tag, tag %d should be key=value, got %q", m.Name, t.Name, i+4, extra)
		}
		switch parts[0] {
		case "name":
			protoExtra[parts[0]] = parts[1]
		case "casttype", "castkey", "castvalue":
			parts[0] = fmt.Sprintf("(gogoproto.%s)", parts[0])
			protoExtra[parts[0]] = strconv.Quote(parts[1])
		}
	}

	field.Extras = protoExtra
	if name, ok := protoExtra["name"]; ok {
		field.Name = name
		delete(protoExtra, "name")
	}

	return nil
}

// membersToFields 把结构体成员转换为 proto 字段并分配字段号。
// 显式编号优先且必须唯一；未编号字段从当前最大值继续递增，
// 从而保证已有编号不变、新增字段不冲突（proto 兼容性的关键）。
func membersToFields(locator ProtobufLocator, t *types.Type, localPackage types.Name, omitFieldTypes map[types.Name]struct{}) ([]protoField, error) {
	fields := []protoField{}

	for _, m := range t.Members {
		if namer.IsPrivateGoName(m.Name) {
			// skip private fields
			continue
		}
		if _, ok := omitFieldTypes[types.Name{Name: m.Type.Name.Name, Package: m.Type.Name.Package}]; ok {
			continue
		}
		tags := reflect.StructTag(m.Tags)
		field := protoField{
			LocalPackage: localPackage,

			Tag:    -1,
			Extras: make(map[string]string),
		}

		protobufTag := tags.Get("protobuf")
		if protobufTag == "-" {
			continue
		}

		if err := protobufTagToField(protobufTag, &field, m, t, localPackage); err != nil {
			return nil, err
		}

		// extract information from JSON field tag
		if tag, _ := tags.Lookup("json"); len(tag) > 0 {
			parts := strings.Split(tag, ",")
			if len(field.Name) == 0 && len(parts[0]) != 0 {
				field.Name = parts[0]
			}
			if field.Tag == -1 && field.Name == "-" {
				continue
			}
		}

		if field.Type == nil {
			if err := memberTypeToProtobufField(locator, &field, m.Type); err != nil {
				return nil, fmt.Errorf("unable to embed type %q as field %q in %q: %v", m.Type, field.Name, t.Name, err)
			}
		}
		if len(field.Name) == 0 {
			field.Name = namer.IL(m.Name)
		}

		if field.Map && field.Repeated {
			// maps cannot be repeated
			field.Repeated = false
			field.Nullable = true
		}

		if !field.Nullable {
			field.Extras["(gogoproto.nullable)"] = "false"
		}
		if (field.Type.Name.Name == "bytes" && field.Type.Name.Package == "") || (field.Repeated && field.Type.Name.Package == "" && namer.IsPrivateGoName(field.Type.Name.Name)) {
			delete(field.Extras, "(gogoproto.nullable)")
		}
		if field.Name != m.Name {
			field.Extras["(gogoproto.customname)"] = strconv.Quote(m.Name)
		}
		field.CommentLines = m.CommentLines
		fields = append(fields, field)
	}

	// assign tags
	highest := 0
	byTag := make(map[int]*protoField)
	// fields are in Go struct order, which we preserve
	for i := range fields {
		field := &fields[i]
		tag := field.Tag
		if tag != -1 {
			if existing, ok := byTag[tag]; ok {
				return nil, fmt.Errorf("field %q and %q both have tag %d", field.Name, existing.Name, tag)
			}
			byTag[tag] = field
		}
		if tag > highest {
			highest = tag
		}
	}
	// starting from the highest observed tag, assign new field tags
	for i := range fields {
		field := &fields[i]
		if field.Tag != -1 {
			continue
		}
		highest++
		field.Tag = highest
		byTag[field.Tag] = field
	}
	return fields, nil
}

// genComment 输出缩进后的 // 注释，并去掉末尾空行——.proto 中空注释行会产生
// 无意义的输出，且部分格式化工具会报错。
func genComment(out io.Writer, lines []string, indent string) {
	for {
		l := len(lines)
		if l == 0 || len(lines[l-1]) != 0 {
			break
		}
		lines = lines[:l-1]
	}
	for _, c := range lines {
		if len(c) == 0 {
			fmt.Fprintf(out, "%s//\n", indent) // avoid trailing whitespace
			continue
		}
		fmt.Fprintf(out, "%s// %s\n", indent, c)
	}
}

// formatProtoFile 目前原样返回；protoc 的格式化器不可用时仍需保持接口一致。
func formatProtoFile(source []byte) ([]byte, error) {
	// TODO; Is there any protobuf formatter?
	return source, nil
}

// assembleProtoFile 按 proto2 语法组装最终文件：header、syntax、package、
// 排序后的 import、文件级 option，最后是消息体。
func assembleProtoFile(w io.Writer, f *generator.File) {
	w.Write(f.Header)

	fmt.Fprint(w, "syntax = \"proto2\";\n\n")

	if len(f.PackageName) > 0 {
		fmt.Fprintf(w, "package %s;\n\n", f.PackageName)
	}

	if len(f.Imports) > 0 {
		imports := []string{}
		for i := range f.Imports {
			imports = append(imports, i)
		}
		sort.Strings(imports)
		for _, s := range imports {
			fmt.Fprintf(w, "import %q;\n", s)
		}
		fmt.Fprint(w, "\n")
	}

	if f.Vars.Len() > 0 {
		fmt.Fprintf(w, "%s\n", f.Vars.String())
	}

	w.Write(f.Body.Bytes())
}

// NewProtoFile 返回 .proto 的文件类型，交给 gengo 按 FileType 分发。
func NewProtoFile() *generator.DefaultFileType {
	return &generator.DefaultFileType{
		Format:   formatProtoFile,
		Assemble: assembleProtoFile,
	}
}
