/*
Copyright 2016 The Kubernetes Authors.

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

package util

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/gengo/v2"
)

// supportedTags 列出 client-gen 认识的全部 genclient 标签。
// validateClientGenTags 用该列表反向剔除已识别标签，剩下的 genclient: 前缀标签
// 即视为拼写错误，避免静默忽略用户的错误注解。
var supportedTags = []string{
	"genclient",
	"genclient:nonNamespaced",
	"genclient:noVerbs",
	"genclient:onlyVerbs",
	"genclient:skipVerbs",
	"genclient:noStatus",
	"genclient:readonly",
	"genclient:method",
}

// SupportedVerbs 是 +onlyVerbs 与 +skipVerbs 可指定的全部动词。
var SupportedVerbs = []string{
	"create",
	"update",
	"updateStatus",
	"delete",
	"deleteCollection",
	"get",
	"list",
	"watch",
	"patch",
	"apply",
	"applyStatus",
}

// ReadonlyVerbs 是只读动词列表，供 +readonly 使用。
var ReadonlyVerbs = []string{
	"get",
	"list",
	"watch",
}

// genClientPrefix 是带子选项的 genclient 标签的统一前缀。
const genClientPrefix = "genclient:"

// unsupportedExtensionVerbs 是扩展方法不支持的动词：
// 它们无法通过通用模板生成，强行生成会得到语义错误的方法。
var unsupportedExtensionVerbs = []string{
	"updateStatus",
	"deleteCollection",
	"watch",
	"delete",
}

// inputTypeSupportedVerbs 是允许覆盖输入参数类型的动词。
var inputTypeSupportedVerbs = []string{
	"create",
	"update",
	"apply",
}

// resultTypeSupportedVerbs 是允许覆盖返回类型的动词。
var resultTypeSupportedVerbs = []string{
	"create",
	"update",
	"get",
	"list",
	"patch",
	"apply",
}

// extension 对应单条 +genclient:method 标签的解析结果。
//
// 例如以下标签会生成一个 UpdateScale 方法：
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=Scale,result=Scale
// 其中 method 是生成的 client 方法名，verb=update 决定使用 PUT 动作，
// subresource 指定用子资源模板生成，input/result 分别覆盖入参与返回类型。
//
// 字段语义由 verb 决定：只有 create/update/apply 能覆盖输入类型，
// 只有部分 verb 能覆盖返回类型。
type extension struct {
	// VerbName 是自定义方法名，如 Scale、Instantiate。
	VerbName string
	// VerbType 是动词类型，只能取 SupportedVerbs 中的值。
	VerbType string
	// SubResourcePath 是请求使用的子资源路径，可选。
	SubResourcePath string
	// InputTypeOverride 覆盖动词的入参类型，默认使用原类型。
	// 仅 create/update 支持覆盖，且新类型必须与原类型位于同一包。可选。
	InputTypeOverride string
	// ResultTypeOverride 覆盖动词的返回类型，默认使用原类型。可选。
	ResultTypeOverride string
}

// IsSubresource 报告该扩展是否针对子资源。
func (e *extension) IsSubresource() bool {
	return len(e.SubResourcePath) > 0
}

// HasVerb 报告该扩展是否匹配给定动词。
func (e *extension) HasVerb(verb string) bool {
	return e.VerbType == verb
}

// Input 返回输入类型覆盖项的类型名与包路径，顺序为 (类型名, 包路径)。
// 分隔符用 "."，因此输入值形如 "v1.Scale"。
func (e *extension) Input() (string, string) {
	parts := strings.Split(e.InputTypeOverride, ".")
	return parts[len(parts)-1], strings.Join(parts[0:len(parts)-1], ".")
}

// Result 同 Input，返回 (类型名, 包路径)。
func (e *extension) Result() (string, string) {
	parts := strings.Split(e.ResultTypeOverride, ".")
	return parts[len(parts)-1], strings.Join(parts[0:len(parts)-1], ".")
}

// Tags 是单个类型的 genclient 配置。
//
// 每个被 +genclient 标注的类型都会解析出一个 Tags，生成器据此决定
// 是否产出 client、是否 namespace 化、包含哪些 verb 以及扩展方法。
type Tags struct {
	// +genclient
	GenerateClient bool
	// +genclient:nonNamespaced
	NonNamespaced bool
	// +genclient:noStatus
	NoStatus bool
	// +genclient:noVerbs
	NoVerbs bool
	// +genclient:skipVerbs=get,update
	// +genclient:onlyVerbs=create,delete
	SkipVerbs []string
	// +genclient:method=UpdateScale,verb=update,subresource=scale,input=Scale,result=Scale
	Extensions []extension
}

// HasVerb 报告给定动词是否应进入最终 client 接口并生成对应方法。
func (t Tags) HasVerb(verb string) bool {
	if len(t.SkipVerbs) == 0 {
		return true
	}
	for _, s := range t.SkipVerbs {
		if verb == s {
			return false
		}
	}
	return true
}

// MustParseClientGenTags 调用 ParseClientGenTags，出错时直接 panic。
func MustParseClientGenTags(lines []string) Tags {
	tags, err := ParseClientGenTags(lines)
	if err != nil {
		panic(err.Error())
	}
	return tags
}

// ParseClientGenTags 解析给定的 genclient 标签，并拒绝未知标签。
//
// onlyVerbs 与 readonly 的实现方式是反转为 SkipVerbs，这样后续生成器只需
// 判断 HasVerb，无需区分白名单/黑名单两种模式。
func ParseClientGenTags(lines []string) (Tags, error) {
	ret := Tags{}
	values := gengo.ExtractCommentTags("+", lines)
	var value []string
	value, ret.GenerateClient = values["genclient"]
	if len(value) > 0 && len(value[0]) > 0 {
		return ret, fmt.Errorf("+genclient=%s is invalid, use //+genclient if you want to generate client or omit it when you want to disable generation", value)
	}
	_, ret.NonNamespaced = values[genClientPrefix+"nonNamespaced"]
	if value := values["nonNamespaced"]; len(value) > 0 && len(value[0]) > 0 {
		return ret, fmt.Errorf("+nonNamespaced=%s is invalid, use //+genclient:nonNamespaced instead", value[0])
	}
	_, ret.NoVerbs = values[genClientPrefix+"noVerbs"]
	_, ret.NoStatus = values[genClientPrefix+"noStatus"]
	onlyVerbs := []string{}
	if _, isReadonly := values[genClientPrefix+"readonly"]; isReadonly {
		onlyVerbs = ReadonlyVerbs
	}
	if value := values["readonly"]; len(value) > 0 && len(value[0]) > 0 {
		return ret, fmt.Errorf("+readonly=%s is invalid, use //+genclient:readonly instead", value[0])
	}
	if v, exists := values[genClientPrefix+"skipVerbs"]; exists {
		ret.SkipVerbs = strings.Split(v[0], ",")
	}
	if v, exists := values[genClientPrefix+"onlyVerbs"]; exists || len(onlyVerbs) > 0 {
		if len(v) > 0 {
			onlyVerbs = append(onlyVerbs, strings.Split(v[0], ",")...)
		}
		// 把 onlyVerbs 白名单翻译成 SkipVerbs 黑名单。
		skipVerbs := []string{}
		for _, m := range SupportedVerbs {
			skip := true
			for _, o := range onlyVerbs {
				if o == m {
					skip = false
					break
				}
			}
			// 同一动词不能同时出现在 skipVerbs 与 onlyVerbs 中。
			for _, v := range skipVerbs {
				if v == m {
					return ret, fmt.Errorf("verb %q used both in genclient:skipVerbs and genclient:onlyVerbs", v)
				}
			}
			if skip {
				skipVerbs = append(skipVerbs, m)
			}
		}
		ret.SkipVerbs = skipVerbs
	}
	var err error
	if ret.Extensions, err = parseClientExtensions(values); err != nil {
		return ret, err
	}
	return ret, validateClientGenTags(values)
}

// parseClientExtensions 解析所有 +genclient:method 标签。
// 标签名以 "method" 为前缀（因为 gengo 会把 method 后面带不带值统一成键名），
// 值形如 "UpdateScale,verb=update,subresource=scale"。
func parseClientExtensions(tags map[string][]string) ([]extension, error) {
	var ret []extension
	for name, values := range tags {
		if !strings.HasPrefix(name, genClientPrefix+"method") {
			continue
		}
		for _, value := range values {
			// 标签值形如 "Foo,verb=create"。
			ext := extension{}
			parts := strings.Split(value, ",")
			if len(parts) == 0 {
				return nil, fmt.Errorf("invalid of empty extension verb name: %q", value)
			}
			// 第一段是扩展方法名。
			ext.VerbName = parts[0]
			if len(ext.VerbName) == 0 {
				return nil, fmt.Errorf("must specify a verb name (// +genclient:method=Foo,verb=create)")
			}
			// 其余片段是 key=value 参数。
			params := parts[1:]
			for _, p := range params {
				parts := strings.Split(p, "=")
				if len(parts) != 2 {
					return nil, fmt.Errorf("invalid extension tag specification %q", p)
				}
				key, val := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
				if len(val) == 0 {
					return nil, fmt.Errorf("empty value of %q for %q extension", key, ext.VerbName)
				}
				switch key {
				case "verb":
					ext.VerbType = val
				case "subresource":
					ext.SubResourcePath = val
				case "input":
					ext.InputTypeOverride = val
				case "result":
					ext.ResultTypeOverride = val
				default:
					return nil, fmt.Errorf("unknown extension configuration key %q", key)
				}
			}
			// 解析完成后统一校验扩展配置。
			if len(ext.VerbType) == 0 {
				return nil, fmt.Errorf("verb type must be specified (use '// +genclient:method=%s,verb=create')", ext.VerbName)
			}
			// 类型覆盖只在部分 verb 上有意义，提前拒绝可避免生成出
			// 签名与实际 HTTP 动作不匹配的 client 方法。
			if len(ext.ResultTypeOverride) > 0 {
				supported := false
				for _, v := range resultTypeSupportedVerbs {
					if ext.VerbType == v {
						supported = true
						break
					}
				}
				if !supported {
					return nil, fmt.Errorf("%s: result type is not supported for %q verbs (supported verbs: %#v)", ext.VerbName, ext.VerbType, resultTypeSupportedVerbs)
				}
			}
			if len(ext.InputTypeOverride) > 0 {
				supported := false
				for _, v := range inputTypeSupportedVerbs {
					if ext.VerbType == v {
						supported = true
						break
					}
				}
				if !supported {
					return nil, fmt.Errorf("%s: input type is not supported for %q verbs (supported verbs: %#v)", ext.VerbName, ext.VerbType, inputTypeSupportedVerbs)
				}
			}
			for _, t := range unsupportedExtensionVerbs {
				if ext.VerbType == t {
					return nil, fmt.Errorf("verb %q is not supported by extension generator", ext.VerbType)
				}
			}
			ret = append(ret, ext)
		}
	}
	return ret, nil
}

// validateClientGenTags 校验只出现了受支持的 genclient 标签。
//
// 直接删除 map 中的已识别键，剩余的 genclient: 前缀键即为未知标签。
// 注意这里只是校验，调用方不应复用该 map。
func validateClientGenTags(values map[string][]string) error {
	for _, k := range supportedTags {
		delete(values, k)
	}
	for key := range values {
		if strings.HasPrefix(key, strings.TrimSuffix(genClientPrefix, ":")) {
			return errors.New("unknown tag detected: " + key)
		}
	}
	return nil
}
