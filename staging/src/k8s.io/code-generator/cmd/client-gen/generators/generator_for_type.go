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

package generators

import (
	"io"
	"path"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
)

// genClientForType 为每个顶层类型生成一个文件。
// 一个实例只服务一个类型（见 Filter），因此 typed client 的接口、实现和组级
// 访问器能放在同一个文件里，且彼此引用无需跨文件解析。
type genClientForType struct {
	generator.GoGenerator
	outputPackage             string // must be a Go import-path；生成代码所在包
	inputPackage              string // API 类型所在包，用于推导 group/version
	clientsetPackage          string // must be a Go import-path；scheme.ParameterCodec 的来源
	applyConfigurationPackage string // must be a Go import-path；为空表示不生成 apply verb
	group                     string // group 名，用于生成客户端类型前缀
	version                   string
	groupGoName               string // group 的 Go 标识符形式
	prefersProtobuf           bool   // 该类型是否默认使用 protobuf 传输
	typeToMatch               *types.Type
	imports                   namer.ImportTracker
}

var _ generator.Generator = &genClientForType{}

// titler 用于把 verb 名转成导出形式（首字母大写）。
var titler = cases.Title(language.Und)

// Filter ignores all but one type because we're making a single file per type.
func (g *genClientForType) Filter(c *generator.Context, t *types.Type) bool {
	return t == g.typeToMatch
}

// Namers 注册 raw 命名系统，输出带导入限定的 Go 类型名。
func (g *genClientForType) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// Imports 返回生成文件所需的导入行。
func (g *genClientForType) Imports(c *generator.Context) (imports []string) {
	return g.imports.ImportLines()
}

// 理想情况是 API Server 为 "status" 注册了子资源路径时 genStatus 才返回 true，
// 但我们拿不到那个信息，所以只要类型有 status 字段就返回 true。
// 这是保守启发式：有 Status 字段且未声明 +genclient:noStatus 就生成 Status 相关 verb。
func genStatus(t *types.Type) bool {
	// Default to true if we have a Status member
	hasStatus := false
	for _, m := range t.Members {
		if m.Name == "Status" {
			hasStatus = true
			break
		}
	}
	return hasStatus && !util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...)).NoStatus
}

// GenerateType 生成实现类型 t 的单个 typed client 的文件主体。
// 生成流程分三段：Getter 接口 → 接口方法集合 → 结构体/构造器；扩展 verb
// 在接口与实现两处分别展开，必须使用相同的模板替换逻辑以保证签名一致。
func (g *genClientForType) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	generateApply := len(g.applyConfigurationPackage) > 0
	defaultVerbTemplates := buildDefaultVerbTemplates(generateApply)
	subresourceDefaultVerbTemplates := buildSubresourceDefaultVerbTemplates(generateApply)
	sw := generator.NewSnippetWriter(w, c, "$", "$")
	pkg := path.Base(t.Name.Package)
	tags, err := util.ParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
	if err != nil {
		return err
	}
	type extendedInterfaceMethod struct {
		template string
		args     map[string]interface{}
	}
	_, typeGVString := util.ParsePathGroupVersion(g.inputPackage)
	extendedMethods := []extendedInterfaceMethod{}
	for _, e := range tags.Extensions {
		// 没有 apply configuration 包时跳过 apply 扩展，否则会引用不存在的类型。
		if e.HasVerb("apply") && !generateApply {
			continue
		}
		inputType := *t
		resultType := *t
		inputGVString := typeGVString
		// TODO: Extract this to some helper method as this code is copied into
		// 2 other places.
		// input/result 覆盖允许扩展方法引用其它 GV 的类型（如 Scale）。
		if len(e.InputTypeOverride) > 0 {
			if name, pkg := e.Input(); len(pkg) > 0 {
				_, inputGVString = util.ParsePathGroupVersion(pkg)
				newType := c.Universe.Type(types.Name{Package: pkg, Name: name})
				inputType = *newType
			} else {
				inputType.Name.Name = e.InputTypeOverride
			}
		}
		if len(e.ResultTypeOverride) > 0 {
			if name, pkg := e.Result(); len(pkg) > 0 {
				newType := c.Universe.Type(types.Name{Package: pkg, Name: name})
				resultType = *newType
			} else {
				resultType.Name.Name = e.ResultTypeOverride
			}
		}
		// 复用默认 verb 模板的签名，只把方法名替换成扩展名；子资源用不同的
		// 参数列表（多一个父资源名）。
		var updatedVerbtemplate string
		if _, exists := subresourceDefaultVerbTemplates[e.VerbType]; e.IsSubresource() && exists {
			updatedVerbtemplate = e.VerbName + "(" + strings.TrimPrefix(subresourceDefaultVerbTemplates[e.VerbType], titler.String(e.VerbType)+"(")
		} else {
			updatedVerbtemplate = e.VerbName + "(" + strings.TrimPrefix(defaultVerbTemplates[e.VerbType], titler.String(e.VerbType)+"(")
		}
		extendedMethod := extendedInterfaceMethod{
			template: updatedVerbtemplate,
			args: map[string]interface{}{
				"type":          t,
				"inputType":     &inputType,
				"resultType":    &resultType,
				"CreateOptions": c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "CreateOptions"}),
				"GetOptions":    c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "GetOptions"}),
				"ListOptions":   c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ListOptions"}),
				"UpdateOptions": c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "UpdateOptions"}),
				"ApplyOptions":  c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ApplyOptions"}),
				"PatchType":     c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/types", Name: "PatchType"}),
				"PatchOptions":  c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "PatchOptions"}),
				"context":       c.Universe.Type(types.Name{Package: "context", Name: "Context"}),
			},
		}
		// apply 扩展的入参是 apply configuration 指针，而不是 API 类型。
		if e.HasVerb("apply") {
			extendedMethod.args["inputApplyConfig"] = types.Ref(path.Join(g.applyConfigurationPackage, inputGVString), inputType.Name.Name+"ApplyConfiguration")
		}
		extendedMethods = append(extendedMethods, extendedMethod)
	}
	// m 是主模板参数表；后续扩展 verb 会就地覆写 inputType/resultType 等键。
	m := map[string]interface{}{
		"type":                      t,
		"inputType":                 t,
		"resultType":                t,
		"package":                   pkg,
		"Package":                   namer.IC(pkg),
		"namespaced":                !tags.NonNamespaced,
		"Group":                     namer.IC(g.group),
		"subresource":               false,
		"subresourcePath":           "",
		"GroupGoName":               g.groupGoName,
		"prefersProtobuf":           g.prefersProtobuf,
		"Version":                   namer.IC(g.version),
		"CreateOptions":             c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "CreateOptions"}),
		"DeleteOptions":             c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "DeleteOptions"}),
		"GetOptions":                c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "GetOptions"}),
		"ListOptions":               c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ListOptions"}),
		"PatchOptions":              c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "PatchOptions"}),
		"ApplyOptions":              c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "ApplyOptions"}),
		"UpdateOptions":             c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/apis/meta/v1", Name: "UpdateOptions"}),
		"PatchType":                 c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/types", Name: "PatchType"}),
		"watchInterface":            c.Universe.Type(types.Name{Package: "k8s.io/apimachinery/pkg/watch", Name: "Interface"}),
		"RESTClientInterface":       c.Universe.Type(types.Name{Package: "k8s.io/client-go/rest", Name: "Interface"}),
		"schemeParameterCodec":      c.Universe.Variable(types.Name{Package: path.Join(g.clientsetPackage, "scheme"), Name: "ParameterCodec"}),
		"fmtErrorf":                 c.Universe.Function(types.Name{Package: "fmt", Name: "Errorf"}),
		"klogWarningf":              c.Universe.Function(types.Name{Package: "k8s.io/klog/v2", Name: "Warningf"}),
		"context":                   c.Universe.Type(types.Name{Package: "context", Name: "Context"}),
		"timeDuration":              c.Universe.Type(types.Name{Package: "time", Name: "Duration"}),
		"timeSecond":                c.Universe.Type(types.Name{Package: "time", Name: "Second"}),
		"applyNewRequest":           c.Universe.Function(types.Name{Package: "k8s.io/client-go/util/apply", Name: "NewRequest"}),
		"Client":                    c.Universe.Type(types.Name{Package: "k8s.io/client-go/gentype", Name: "Client"}),
		"ClientWithList":            c.Universe.Type(types.Name{Package: "k8s.io/client-go/gentype", Name: "ClientWithList"}),
		"ClientWithApply":           c.Universe.Type(types.Name{Package: "k8s.io/client-go/gentype", Name: "ClientWithApply"}),
		"ClientWithListAndApply":    c.Universe.Type(types.Name{Package: "k8s.io/client-go/gentype", Name: "ClientWithListAndApply"}),
		"NewClient":                 c.Universe.Function(types.Name{Package: "k8s.io/client-go/gentype", Name: "NewClient"}),
		"NewClientWithApply":        c.Universe.Function(types.Name{Package: "k8s.io/client-go/gentype", Name: "NewClientWithApply"}),
		"NewClientWithList":         c.Universe.Function(types.Name{Package: "k8s.io/client-go/gentype", Name: "NewClientWithList"}),
		"NewClientWithListAndApply": c.Universe.Function(types.Name{Package: "k8s.io/client-go/gentype", Name: "NewClientWithListAndApply"}),
	}

	if generateApply {
		// 生成 Apply 函数所需的 apply configuration 类型引用：
		// apply configuration 与被生成代码位于同一 GV 子包下，因此需要重新解析链接串。
		_, gvString := util.ParsePathGroupVersion(g.inputPackage)
		m["inputApplyConfig"] = types.Ref(path.Join(g.applyConfigurationPackage, gvString), t.Name.Name+"ApplyConfiguration")
	}

	sw.Do(getterComment, m)
	if tags.NonNamespaced {
		sw.Do(getterNonNamespaced, m)
	} else {
		sw.Do(getterNamespaced, m)
	}

	sw.Do(interfaceTemplate1, m)
	if !tags.NoVerbs {
		// 没有 Status 字段时，updateStatus/applyStatus 只能跳过，否则实现会引用不存在的子资源。
		if !genStatus(t) {
			tags.SkipVerbs = append(tags.SkipVerbs, "updateStatus")
			tags.SkipVerbs = append(tags.SkipVerbs, "applyStatus")
		}
		interfaceSuffix := ""
		if len(extendedMethods) > 0 {
			interfaceSuffix = "\n"
		}
		sw.Do("\n"+generateInterface(defaultVerbTemplates, tags)+interfaceSuffix, m)
		// add extended verbs into interface
		for _, v := range extendedMethods {
			sw.Do(v.template+interfaceSuffix, v.args)
		}

	}
	sw.Do(interfaceTemplate4, m)

	// 结构体与构造器按“命名空间 × list × apply”三维组合选择模板；
	// 位掩码索引配合下面的常量声明实现编译期枚举。
	structNamespaced := namespaced
	if tags.NonNamespaced {
		structNamespaced = nonNamespaced
	}

	if tags.NoVerbs {
		sw.Do(structType[noList|noApply], m)
		sw.Do(newStruct[structNamespaced|noList|noApply], m)

		return sw.Error()
	}

	listableOrAppliable := noList | noApply

	if tags.HasVerb("list") {
		listableOrAppliable |= withList
	}

	// 只有存在 apply configuration 包且支持 apply verb 时才选择带 apply 的构造器，
	// 否则会引用未生成的类型。
	if tags.HasVerb("apply") && generateApply {
		listableOrAppliable |= withApply
	}

	sw.Do(structType[listableOrAppliable], m)
	sw.Do(newStruct[structNamespaced|listableOrAppliable], m)

	// 逐个生成扩展方法（expansion methods）；m 是共享 map，每次循环覆写相关键，因此必须在同一轮内
	// 生成所有 verb，避免不同扩展之间串值。
	for _, e := range tags.Extensions {
		if e.HasVerb("apply") && !generateApply {
			continue
		}
		inputType := *t
		resultType := *t
		inputGVString := typeGVString
		if len(e.InputTypeOverride) > 0 {
			if name, pkg := e.Input(); len(pkg) > 0 {
				_, inputGVString = util.ParsePathGroupVersion(pkg)
				newType := c.Universe.Type(types.Name{Package: pkg, Name: name})
				inputType = *newType
			} else {
				inputType.Name.Name = e.InputTypeOverride
			}
		}
		if len(e.ResultTypeOverride) > 0 {
			if name, pkg := e.Result(); len(pkg) > 0 {
				newType := c.Universe.Type(types.Name{Package: pkg, Name: name})
				resultType = *newType
			} else {
				resultType.Name.Name = e.ResultTypeOverride
			}
		}
		m["inputType"] = &inputType
		m["resultType"] = &resultType
		m["subresourcePath"] = e.SubResourcePath
		m["verb"] = e.VerbName
		if e.HasVerb("apply") {
			m["inputApplyConfig"] = types.Ref(path.Join(g.applyConfigurationPackage, inputGVString), inputType.Name.Name+"ApplyConfiguration")
		}

		// 每个 verb 有主资源/子资源两套模板；watch/delete 的子资源语义尚未定义，
		// 因此 TODO 标注后仍统一使用主资源模板。
		if e.HasVerb("get") {
			if e.IsSubresource() {
				sw.Do(getSubresourceTemplate, m)
			} else {
				sw.Do(getTemplate, m)
			}
		}

		if e.HasVerb("list") {
			if e.IsSubresource() {
				sw.Do(listSubresourceTemplate, m)
			} else {
				sw.Do(listTemplate, m)
			}
		}

		// TODO: Figure out schemantic for watching a sub-resource.
		if e.HasVerb("watch") {
			sw.Do(watchTemplate, m)
		}

		if e.HasVerb("create") {
			if e.IsSubresource() {
				sw.Do(createSubresourceTemplate, m)
			} else {
				sw.Do(createTemplate, m)
			}
		}

		if e.HasVerb("update") {
			if e.IsSubresource() {
				sw.Do(updateSubresourceTemplate, m)
			} else {
				sw.Do(updateTemplate, m)
			}
		}

		// TODO: Figure out schemantic for deleting a sub-resource (what arguments
		// are passed, does it need two names? etc.
		if e.HasVerb("delete") {
			sw.Do(deleteTemplate, m)
		}

		if e.HasVerb("patch") {
			sw.Do(patchTemplate, m)
		}

		if e.HasVerb("apply") {
			if e.IsSubresource() {
				sw.Do(applySubresourceTemplate, m)
			} else {
				sw.Do(applyTemplate, m)
			}
		}
	}

	return sw.Error()
}

// generateInterface 按 util.SupportedVerbs 的固定顺序拼接接口方法。
// 顺序固定是为了让生成结果可复现、diff 稳定，不能改成 map 迭代。
func generateInterface(defaultVerbTemplates map[string]string, tags util.Tags) string {
	// need an ordered list here to guarantee order of generated methods.
	out := []string{}
	for _, m := range util.SupportedVerbs {
		if tags.HasVerb(m) && len(defaultVerbTemplates[m]) > 0 {
			out = append(out, defaultVerbTemplates[m])
		}
	}
	return strings.Join(out, "\n")
}

// buildSubresourceDefaultVerbTemplates 返回子资源 verb 的方法签名。
// 子资源方法多一个父资源名字参数，用于定位 <name>/<subresource>。
func buildSubresourceDefaultVerbTemplates(generateApply bool) map[string]string {
	m := map[string]string{
		"create": `Create(ctx $.context|raw$, $.type|private$Name string, $.inputType|private$ *$.inputType|raw$, opts $.CreateOptions|raw$) (*$.resultType|raw$, error)`,
		"list":   `List(ctx $.context|raw$, $.type|private$Name string, opts $.ListOptions|raw$) (*$.resultType|raw$List, error)`,
		"update": `Update(ctx $.context|raw$, $.type|private$Name string, $.inputType|private$ *$.inputType|raw$, opts $.UpdateOptions|raw$) (*$.resultType|raw$, error)`,
		"get":    `Get(ctx $.context|raw$, $.type|private$Name string, options $.GetOptions|raw$) (*$.resultType|raw$, error)`,
	}
	if generateApply {
		m["apply"] = `Apply(ctx $.context|raw$, $.type|private$Name string, $.inputType|private$ *$.inputApplyConfig|raw$, opts $.ApplyOptions|raw$) (*$.resultType|raw$, error)`
	}
	return m
}

// buildDefaultVerbTemplates 返回主资源 verb 的方法签名。
// apply/applyStatus 仅在 generateApply 为真时加入，且 apply* 使用 ApplyConfiguration 入参。
func buildDefaultVerbTemplates(generateApply bool) map[string]string {
	m := map[string]string{
		"create": `Create(ctx $.context|raw$, $.inputType|private$ *$.inputType|raw$, opts $.CreateOptions|raw$) (*$.resultType|raw$, error)`,
		"update": `Update(ctx $.context|raw$, $.inputType|private$ *$.inputType|raw$, opts $.UpdateOptions|raw$) (*$.resultType|raw$, error)`,
		"updateStatus": `// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
UpdateStatus(ctx $.context|raw$, $.inputType|private$ *$.type|raw$, opts $.UpdateOptions|raw$) (*$.type|raw$, error)`,
		"delete":           `Delete(ctx $.context|raw$, name string, opts $.DeleteOptions|raw$) error`,
		"deleteCollection": `DeleteCollection(ctx $.context|raw$, opts $.DeleteOptions|raw$, listOpts $.ListOptions|raw$) error`,
		"get":              `Get(ctx $.context|raw$, name string, opts $.GetOptions|raw$) (*$.resultType|raw$, error)`,
		"list":             `List(ctx $.context|raw$, opts $.ListOptions|raw$) (*$.resultType|raw$List, error)`,
		"watch":            `Watch(ctx $.context|raw$, opts $.ListOptions|raw$) ($.watchInterface|raw$, error)`,
		"patch":            `Patch(ctx $.context|raw$, name string, pt $.PatchType|raw$, data []byte, opts $.PatchOptions|raw$, subresources ...string) (result *$.resultType|raw$, err error)`,
	}
	if generateApply {
		m["apply"] = `Apply(ctx $.context|raw$, $.inputType|private$ *$.inputApplyConfig|raw$, opts $.ApplyOptions|raw$) (result *$.resultType|raw$, err error)`
		m["applyStatus"] = `// Add a +genclient:noStatus comment above the type to avoid generating ApplyStatus().
ApplyStatus(ctx $.context|raw$, $.inputType|private$ *$.inputApplyConfig|raw$, opts $.ApplyOptions|raw$) (result *$.resultType|raw$, err error)`
	}
	return m
}

// getterComment 是该组级 client 会实现的接口声明。
// 组级 Getter 是 typed client 与具体类型 client 之间的桥梁：
// 调用方通过 client.<Plural>(namespace) 拿到类型客户端。
var getterComment = `
// $.type|publicPlural$Getter has a method to return a $.type|public$Interface.
// A group's client should implement this interface.`

var getterNamespaced = `
type $.type|publicPlural$Getter interface {
	$.type|publicPlural$(namespace string) $.type|public$Interface
}
`

var getterNonNamespaced = `
type $.type|publicPlural$Getter interface {
	$.type|publicPlural$() $.type|public$Interface
}
`

// this type's interface, typed client will implement this interface.
var interfaceTemplate1 = `
// $.type|public$Interface has methods to work with $.type|public$ resources.
type $.type|public$Interface interface {`

var interfaceTemplate4 = `
	$.type|public$Expansion
}
`

// struct and constructor variants
const (
	// The following values are bits in a bitmask.
	// The values which can be set indicate namespace support, list support, and apply support;
	// to make the declarations easier to read (like a truth table), corresponding zero-values
	// are also declared.
	namespaced    = 0
	noList        = 0
	noApply       = 0
	nonNamespaced = 1 << iota
	withList
	withApply
)

// The following string slices are similar to maps, but with combinable keys used as indices.
// Each entry defines whether it supports lists and/or apply, and if namespacedness matters,
// namespaces; each bit is then toggled:
// * noList, noApply: index 0;
// * withList, noApply: index 2;
// * noList, withApply: index 4;
// * withList, withApply: index 6.
// When namespacedness matters, the namespaced variants are the same as the above, and
// the non-namespaced variants are offset by 1.
// Go enforces index unicity in these kinds of declarations.

// struct 声明。
// structType 与是否命名空间无关，用位掩码索引选择实现：Client / ClientWithList / ClientWithApply /
// ClientWithListAndApply 分别对应不同的能力组合，避免生成大量重复样板。
var structType = []string{
	noList | noApply: `
	// $.type|privatePlural$ implements $.type|public$Interface
	type $.type|privatePlural$ struct {
		*$.Client|raw$[*$.resultType|raw$]
	}
	`,
	withList | noApply: `
	// $.type|privatePlural$ implements $.type|public$Interface
	type $.type|privatePlural$ struct {
		*$.ClientWithList|raw$[*$.resultType|raw$, *$.resultType|raw$List]
	}
	`,
	noList | withApply: `
	// $.type|privatePlural$ implements $.type|public$Interface
	type $.type|privatePlural$ struct {
		*$.ClientWithApply|raw$[*$.resultType|raw$, *$.inputApplyConfig|raw$]
	}
	`,
	withList | withApply: `
	// $.type|privatePlural$ implements $.type|public$Interface
	type $.type|privatePlural$ struct {
		*$.ClientWithListAndApply|raw$[*$.resultType|raw$, *$.resultType|raw$List, *$.inputApplyConfig|raw$]
	}
	`,
}

// struct 构造器的全部变体。
// newStruct 的索引与是否命名空间有关：构造器按“是否命名空间 + 能力位”索引；
// 非命名空间变体把 namespace 参数替换为 ""。
var newStruct = []string{
	namespaced | noList | noApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client, namespace string) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClient|raw$[*$.resultType|raw$](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				namespace,
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
	namespaced | noList | withApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client, namespace string) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClientWithApply|raw$[*$.resultType|raw$, *$.inputApplyConfig|raw$](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				namespace,
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
	namespaced | withList | noApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client, namespace string) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClientWithList|raw$[*$.resultType|raw$, *$.resultType|raw$List](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				namespace,
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				func() *$.resultType|raw$List { return &$.resultType|raw$List{} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
	namespaced | withList | withApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client, namespace string) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClientWithListAndApply|raw$[*$.resultType|raw$, *$.resultType|raw$List, *$.inputApplyConfig|raw$](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				namespace,
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				func() *$.resultType|raw$List { return &$.resultType|raw$List{} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
	nonNamespaced | noList | noApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClient|raw$[*$.resultType|raw$](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				"",
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
	nonNamespaced | noList | withApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClientWithApply|raw$[*$.resultType|raw$, *$.inputApplyConfig|raw$](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				"",
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
	nonNamespaced | withList | noApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClientWithList|raw$[*$.resultType|raw$, *$.resultType|raw$List](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				"",
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				func() *$.resultType|raw$List { return &$.resultType|raw$List{} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
	nonNamespaced | withList | withApply: `
	// new$.type|publicPlural$ returns a $.type|publicPlural$
	func new$.type|publicPlural$(c *$.GroupGoName$$.Version$Client) *$.type|privatePlural$ {
		return &$.type|privatePlural${
			$.NewClientWithListAndApply|raw$[*$.resultType|raw$, *$.resultType|raw$List, *$.inputApplyConfig|raw$](
				"$.type|resource$",
				c.RESTClient(),
				$.schemeParameterCodec|raw$,
				"",
				func() *$.resultType|raw$ { return &$.resultType|raw${} },
				func() *$.resultType|raw$List { return &$.resultType|raw$List{} },
				$if .prefersProtobuf$gentype.PrefersProtobuf[*$.resultType|raw$](),$end$
			),
		}
	}
	`,
}

var listTemplate = `
// $.verb$ takes label and field selectors, and returns the list of $.resultType|publicPlural$ that match those selectors.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, opts $.ListOptions|raw$) (result *$.resultType|raw$List, err error) {
	var timeout $.timeDuration|raw$
	if opts.TimeoutSeconds != nil{
		timeout = $.timeDuration|raw$(*opts.TimeoutSeconds) * $.timeSecond|raw$
	}
	result = &$.resultType|raw$List{}
	err = c.GetClient().Get().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}
`

var listSubresourceTemplate = `
// $.verb$ takes $.type|raw$ name, label and field selectors, and returns the list of $.resultType|publicPlural$ that match those selectors.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.type|private$Name string, opts $.ListOptions|raw$) (result *$.resultType|raw$List, err error) {
	var timeout $.timeDuration|raw$
	if opts.TimeoutSeconds != nil{
		timeout = $.timeDuration|raw$(*opts.TimeoutSeconds) * $.timeSecond|raw$
	}
	result = &$.resultType|raw$List{}
	err = c.GetClient().Get().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name($.type|private$Name).
		SubResource("$.subresourcePath$").
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}
`

var getTemplate = `
// $.verb$ takes name of the $.type|private$, and returns the corresponding $.resultType|private$ object, and an error if there is any.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, name string, options $.GetOptions|raw$) (result *$.resultType|raw$, err error) {
	result = &$.resultType|raw${}
	err = c.GetClient().Get().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name(name).
		VersionedParams(&options, $.schemeParameterCodec|raw$).
		Do(ctx).
		Into(result)
	return
}
`

var getSubresourceTemplate = `
// $.verb$ takes name of the $.type|private$, and returns the corresponding $.resultType|raw$ object, and an error if there is any.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.type|private$Name string, options $.GetOptions|raw$) (result *$.resultType|raw$, err error) {
	result = &$.resultType|raw${}
	err = c.GetClient().Get().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name($.type|private$Name).
		SubResource("$.subresourcePath$").
		VersionedParams(&options, $.schemeParameterCodec|raw$).
		Do(ctx).
		Into(result)
	return
}
`

var deleteTemplate = `
// $.verb$ takes name of the $.type|private$ and deletes it. Returns an error if one occurs.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, name string, opts $.DeleteOptions|raw$) error {
	return c.GetClient().Delete().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}
`

var createSubresourceTemplate = `
// $.verb$ takes the representation of a $.inputType|private$ and creates it.  Returns the server's representation of the $.resultType|private$, and an error, if there is any.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.type|private$Name string, $.inputType|private$ *$.inputType|raw$, opts $.CreateOptions|raw$) (result *$.resultType|raw$, err error) {
	result = &$.resultType|raw${}
	err = c.GetClient().Post().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name($.type|private$Name).
		SubResource("$.subresourcePath$").
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Body($.inputType|private$).
		Do(ctx).
		Into(result)
	return
}
`

var createTemplate = `
// $.verb$ takes the representation of a $.inputType|private$ and creates it.  Returns the server's representation of the $.resultType|private$, and an error, if there is any.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.inputType|private$ *$.inputType|raw$, opts $.CreateOptions|raw$) (result *$.resultType|raw$, err error) {
	result = &$.resultType|raw${}
	err = c.GetClient().Post().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Body($.inputType|private$).
		Do(ctx).
		Into(result)
	return
}
`

var updateSubresourceTemplate = `
// $.verb$ takes the top resource name and the representation of a $.inputType|private$ and updates it. Returns the server's representation of the $.resultType|private$, and an error, if there is any.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.type|private$Name string, $.inputType|private$ *$.inputType|raw$, opts $.UpdateOptions|raw$) (result *$.resultType|raw$, err error) {
	result = &$.resultType|raw${}
	err = c.GetClient().Put().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name($.type|private$Name).
		SubResource("$.subresourcePath$").
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Body($.inputType|private$).
		Do(ctx).
		Into(result)
	return
}
`

var updateTemplate = `
// $.verb$ takes the representation of a $.inputType|private$ and updates it. Returns the server's representation of the $.resultType|private$, and an error, if there is any.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.inputType|private$ *$.inputType|raw$, opts $.UpdateOptions|raw$) (result *$.resultType|raw$, err error) {
	result = &$.resultType|raw${}
	err = c.GetClient().Put().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name($.inputType|private$.Name).
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Body($.inputType|private$).
		Do(ctx).
		Into(result)
	return
}
`

var watchTemplate = `
// $.verb$ returns a $.watchInterface|raw$ that watches the requested $.type|privatePlural$.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, opts $.ListOptions|raw$) ($.watchInterface|raw$, error) {
	var timeout $.timeDuration|raw$
	if opts.TimeoutSeconds != nil{
		timeout = $.timeDuration|raw$(*opts.TimeoutSeconds) * $.timeSecond|raw$
	}
	opts.Watch = true
	return c.GetClient().Get().
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Timeout(timeout).
		Watch(ctx)
}
`
var patchTemplate = `
// $.verb$ applies the patch and returns the patched $.resultType|private$.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, name string, pt $.PatchType|raw$, data []byte, opts $.PatchOptions|raw$, subresources ...string) (result *$.resultType|raw$, err error) {
	result = &$.resultType|raw${}
	err = c.GetClient().Patch(pt).
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, $.schemeParameterCodec|raw$).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
`

// applyTemplate 生成 server-side apply 请求。必须在客户端校验非空配置与 Name，
// 因为 apply 请求以 Name 构造 URL，缺失时服务端错误不够直观。
var applyTemplate = `
// $.verb$ takes the given apply declarative configuration, applies it and returns the applied $.resultType|private$.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.inputType|private$ *$.inputApplyConfig|raw$, opts $.ApplyOptions|raw$) (result *$.resultType|raw$, err error) {
	if $.inputType|private$ == nil {
		return nil, $.fmtErrorf|raw$("$.inputType|private$ provided to $.verb$ must not be nil")
	}
	patchOpts := opts.ToPatchOptions()
	name := $.inputType|private$.Name
	if name == nil {
		return nil, $.fmtErrorf|raw$("$.inputType|private$.Name must be provided to $.verb$")
	}
	request, err := $.applyNewRequest|raw$(c.GetClient(), $.inputType|private$)
	if err != nil {
		return nil, err
	}
	result = &$.resultType|raw${}
	err = request.
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name(*name).
		VersionedParams(&patchOpts, $.schemeParameterCodec|raw$).
		Do(ctx).
		Into(result)
	return
}
`

// applySubresourceTemplate 与 applyTemplate 类似，但父资源名来自方法参数。
var applySubresourceTemplate = `
// $.verb$ takes top resource name and the apply declarative configuration for $.subresourcePath$,
// applies it and returns the applied $.resultType|private$, and an error, if there is any.
func (c *$.type|privatePlural$) $.verb$(ctx $.context|raw$, $.type|private$Name string, $.inputType|private$ *$.inputApplyConfig|raw$, opts $.ApplyOptions|raw$) (result *$.resultType|raw$, err error) {
	if $.inputType|private$ == nil {
		return nil, $.fmtErrorf|raw$("$.inputType|private$ provided to $.verb$ must not be nil")
	}
	patchOpts := opts.ToPatchOptions()
	request, err := $.applyNewRequest|raw$(c.GetClient(), $.inputType|private$)
	if err != nil {
		return nil, err
	}

	result = &$.resultType|raw${}
	err = request.
		$if .prefersProtobuf$UseProtobufAsDefault().$end$
		$if .namespaced$Namespace(c.GetNamespace()).$end$
		Resource("$.type|resource$").
		Name($.type|private$Name).
		SubResource("$.subresourcePath$").
		VersionedParams(&patchOpts, $.schemeParameterCodec|raw$).
		Do(ctx).
		Into(result)
	return
}
`
