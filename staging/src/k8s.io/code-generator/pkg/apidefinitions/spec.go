/*
Copyright The Kubernetes Authors.

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

// Package apidefinitions 统一描述各代码生成器的标签协议（激活标签、
// 输入标签、取值模式与附加标签），并提供包识别与 lint 能力。
// 将协议集中在此处，避免各生成器对同一标签做出不一致的解释。
package apidefinitions

import "k8s.io/apimachinery/pkg/util/sets"

// Spec 描述单个生成器的标签协议。为兼容既有标签行为，此结构是必需的。
// with existing tag behavior.
type Spec struct {
	// ActivationTag 声明本生成器的主启用标签
	ActivationTag string

	// ValueMode 声明如何解释 ActivationTag 的值
	ValueMode ValueMode

	// GroupActivationTag 是包级启用标签，
	// 出现即激活本生成器。
	// ActivationTag 设为 "false" 时始终覆盖此设置。
	GroupActivationTag string

	// ScanTypes 表示当 ActivationTag 缺失时，
	// 生成器会扫描包内类型上的
	// +<ActivationTag>=true tags. ActivationTag set to "false"
	// 会跳过扫描并忽略该包。
	ScanTypes bool

	// DefaultEnabled 表示生成器默认处理所有输入包，
	// 除非 ActivationTag 设为 "false"。
	DefaultEnabled bool

	// InputTag 声明为本生成器提供输入包列表的标签
	InputTag string

	// AuxTags 声明本生成器拥有的其他标签
	AuxTags []string
}

// ValueMode 描述如何解释 ActivationTag 的值。
type ValueMode int

const (
	// Boolean 表示显式开启/关闭生成器的 "true"/"false" 值。
	// off a generator.
	Boolean ValueMode = iota

	// Package 表示显式开启/关闭整包生成的 "package"/"false" 标签。
	// 这类标签通常还可
	// 按类型单独开启，除非取值为
	// "false"。
	Package

	// TypeFilterList：取值为类型名模式（如 defaulter-gen
	// 和 validation-gen 的 "TypeMeta"），经 Activation.TypeFilters 读取。
	TypeFilterList

	// ConversionPeerList 是 conversion-gen 专用的标签协议。
	// 取值要么是 peer 包，要么是 "false"，表示
	// 生成器进入仅显式模式。
	ConversionPeerList
)

// allKnownSpecs 是所有已知生成器协议的清单，供 lint 与标签集合构建使用。
// 新增生成器时必须同步登记，否则其标签会被 known-tags lint 判为未知。
var allKnownSpecs = []Spec{
	Conversion, Defaulter, Validation, PrereleaseLifecycle,
	Deepcopy, Register, OpenAPI, Protobuf,
	Client, ApplyConfiguration, Informer, Lister,
}

// 固化 API 定义生成器中激活标签的行为。
var (
	Conversion = Spec{
		ActivationTag: "k8s:conversion-gen",
		InputTag:      "k8s:conversion-gen-external-types",
		ValueMode:     ConversionPeerList,
	}
	Defaulter = Spec{
		ActivationTag: "k8s:defaulter-gen",
		InputTag:      "k8s:defaulter-gen-input",
		ValueMode:     TypeFilterList,
	}
	Validation = Spec{
		ActivationTag: "k8s:validation-gen",
		InputTag:      "k8s:validation-gen-input",
		ValueMode:     TypeFilterList,
		AuxTags: []string{
			"k8s:validation-gen-nolint",
			"k8s:validation-gen-scheme-registry",
			"k8s:validation-gen-test-fixture",
			"k8s:validation-gen-test-targets",
		},
	}
	PrereleaseLifecycle = Spec{
		ActivationTag: "k8s:prerelease-lifecycle-gen",
		ValueMode:     Boolean,
	}
	Deepcopy = Spec{
		ActivationTag: "k8s:deepcopy-gen",
		ValueMode:     Package,
		ScanTypes:     true,
	}
	Register = Spec{
		ActivationTag:      "k8s:register-gen",
		ValueMode:          Boolean,
		GroupActivationTag: "groupName",
	}
	OpenAPI = Spec{
		ActivationTag: "k8s:openapi-gen",
		InputTag:      "k8s:openapi-model-package",
		ValueMode:     Boolean,
	}
	Protobuf = Spec{
		ActivationTag: "k8s:protobuf-gen",
		ValueMode:     Package,
	}
	Client = Spec{
		ActivationTag:  "k8s:client-gen",
		ValueMode:      Boolean,
		DefaultEnabled: true,
		AuxTags: []string{
			"genclient",
			"genclient:nonNamespaced",
			"genclient:noVerbs",
			"genclient:onlyVerbs",
			"genclient:skipVerbs",
			"genclient:noStatus",
			"genclient:readonly",
			"genclient:method",
			"groupGoName",
		},
	}
	ApplyConfiguration = Spec{
		ActivationTag:  "k8s:applyconfiguration-gen",
		ValueMode:      Boolean,
		DefaultEnabled: true,
	}
	Informer = Spec{
		ActivationTag:  "k8s:informer-gen",
		ValueMode:      Boolean,
		DefaultEnabled: true,
	}
	Lister = Spec{
		ActivationTag:  "k8s:lister-gen",
		ValueMode:      Boolean,
		DefaultEnabled: true,
	}
)

// allKnownTags 汇总所有生成器拥有的标签（激活、输入、附加），
// 用于判断某个 +k8s:*-gen 标签是否属于已知协议。
var allKnownTags = func() sets.Set[string] {
	out := sets.New[string]()
	for _, s := range allKnownSpecs {
		out.Insert(s.ActivationTag)
		if s.InputTag != "" {
			out.Insert(s.InputTag)
		}
		out.Insert(s.AuxTags...)
	}
	return out
}()
