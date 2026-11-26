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

// Package fake 生成 client-gen 的 fake clientset 及其各 group/version 的 fake 实现。
// 这些产物只用于单元测试：它们用 object tracker 在内存里模拟 API 行为，
// 不做校验和默认值处理。
package fake

import (
	"fmt"
	"io"
	"path"
	"strings"

	clientgentypes "k8s.io/code-generator/cmd/client-gen/types"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// genClientset 生成 fake clientset 的整包文件。
// 它不依赖任何具体类型，只需要 group/version 列表和导入路径。
type genClientset struct {
	generator.GoGenerator
	groups               []clientgentypes.GroupVersions
	groupGoNames         map[clientgentypes.GroupVersion]string
	fakeClientsetPackage string // 必须是 Go import-path
	imports              namer.ImportTracker
	clientsetGenerated   bool
	// 真实 clientset 的 import path，fake 包需要导入它以断言接口实现。
	realClientsetPackage      string // must be a Go import-path
	applyConfigurationPackage string
}

var _ generator.Generator = &genClientset{}

// Namers 只注册 raw namer，并让它把用到的类型登记进 imports。
func (g *genClientset) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.fakeClientsetPackage, g.imports),
	}
}

// Filter 让 GenerateType 只被调用一次：fake clientset 是包级产物，
// 与具体类型无关，因此第一个类型触发后即返回 false。
func (g *genClientset) Filter(c *generator.Context, t *types.Type) bool {
	ret := !g.clientsetGenerated
	g.clientsetGenerated = true
	return ret
}

// Imports 计算 fake clientset 需要的导入集合。
// 每个 group/version 需要两处导入：真实 typed client 包与对应的 fake 包，
// 并为它们生成基于 group 名和版本号的别名以避免冲突。
func (g *genClientset) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.imports.ImportLines()...)
	for _, group := range g.groups {
		for _, version := range group.Versions {
			// 约定目录布局：<fakeClientsetPackage>/typed/<group>/<version>，
			// fake 实现位于其下的 fake 子目录。
			groupClientPackage := path.Join(g.fakeClientsetPackage, "typed", strings.ToLower(group.PackageName), strings.ToLower(version.NonEmpty()))
			fakeGroupClientPackage := path.Join(groupClientPackage, "fake")

			// 别名统一小写，并以版本号结尾，保证不同 group/version 之间不冲突。
			groupAlias := strings.ToLower(g.groupGoNames[clientgentypes.GroupVersion{Group: group.Group, Version: version.Version}])
			imports = append(imports, fmt.Sprintf("%s%s \"%s\"", groupAlias, strings.ToLower(version.NonEmpty()), groupClientPackage))
			imports = append(imports, fmt.Sprintf("fake%s%s \"%s\"", groupAlias, strings.ToLower(version.NonEmpty()), fakeGroupClientPackage))
		}
	}
	// 真实 clientset 包，提供要实现的 clientset.Interface。
	imports = append(imports, fmt.Sprintf("clientset \"%s\"", g.realClientsetPackage))
	// commonTemplate 与 managedFieldsClientset 中固定用到的依赖。
	imports = append(imports,
		"k8s.io/client-go/testing",
		"k8s.io/client-go/discovery",
		"fakediscovery \"k8s.io/client-go/discovery/fake\"",
		"k8s.io/apimachinery/pkg/runtime",
		"k8s.io/apimachinery/pkg/watch",
		"metav1 \"k8s.io/apimachinery/pkg/apis/meta/v1\"",
	)

	return
}

// GenerateType 输出 fake clientset 文件：公共部分、可选的 managed-fields
// 构造函数、接口实现断言，以及每个 group/version 的访问方法。
func (g *genClientset) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	// 只有显式提供 apply configuration 包时才生成支持服务端 apply 的构造函数。
	generateApply := len(g.applyConfigurationPackage) > 0

	// TODO: 生成 clientset 其实不需要类型信息，可以考虑让 go2ild 框架支持这种用法。
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	// ToGroupVersionInfo 把内部表示转换为模板所需的扁平结构（含 PackageAlias 等）。
	allGroups := clientgentypes.ToGroupVersionInfo(g.groups, g.groupGoNames)

	sw.Do(common, nil)

	if generateApply {
		// 注入 applyconfiguration-gen 的 NewTypeConverter，供字段管理 tracker 使用。
		sw.Do(managedFieldsClientset, map[string]any{
			"newTypeConverter": types.Ref(g.applyConfigurationPackage, "NewTypeConverter"),
		})
	}

	// 编译期断言：fake clientset 必须实现真实接口，避免两边漂移。
	sw.Do(checkImpl, nil)

	for _, group := range allGroups {
		// Version 用 namer.IC 做首字母大写，拼出 Getter 方法名（如 V1()）。
		m := map[string]interface{}{
			"group":        group.Group,
			"version":      group.Version,
			"PackageAlias": group.PackageAlias,
			"GroupGoName":  group.GroupGoName,
			"Version":      namer.IC(group.Version.String()),
		}

		sw.Do(clientsetInterfaceImplTemplate, m)
	}

	return sw.Error()
}

// managedFieldsClientset 是支持字段管理（服务端 apply）的构造函数模板，
// 仅当指定了 apply configuration 包时才会输出。
// 注意模板内容属于生成产物，注释只加在模板外部。
var managedFieldsClientset = `
// NewClientset returns a clientset that will respond with the provided objects.
// It's backed by a very simple object tracker that processes creates, updates and deletions as-is,
// without applying any validations and/or defaults. It shouldn't be considered a replacement
// for a real clientset and is mostly useful in simple unit tests.
//
// Compared to NewSimpleClientset, the Clientset returned here supports field tracking and thus
// server-side apply. Beware though that support in that for CRDs is missing
// (https://github.com/kubernetes/kubernetes/issues/126850).
func NewClientset(objects ...runtime.Object) *Clientset {
	o := testing.NewFieldManagedObjectTracker(
		scheme,
		codecs.UniversalDecoder(),
		$.newTypeConverter|raw$(scheme),
	)
	for _, obj := range objects {
		if err := o.Add(obj); err != nil {
			panic(err)
		}
	}

	cs := &Clientset{tracker: o}
	cs.discovery = &fakediscovery.FakeDiscovery{Fake: &cs.Fake}
	cs.AddReactor("*", "*", testing.ObjectReaction(o))
	cs.AddWatchReactor("*", func(action testing.Action) (handled bool, ret watch.Interface, err error) {
        var opts metav1.ListOptions
        if watchAction, ok := action.(testing.WatchActionImpl); ok {
            opts = watchAction.ListOptions
        }
		gvr := action.GetResource()
		ns := action.GetNamespace()
		watch, err := o.Watch(gvr, ns, opts)
		if err != nil {
			return false, nil, err
		}
		return true, watch, nil
	})

	return cs
}
`

// common 是 NewSimpleClientset 与 Clientset 结构体等公共代码的模板，
// 这部分代码与具体 group/version 无关，因此对所有 clientset 输出都相同。
var common = `
// NewSimpleClientset returns a clientset that will respond with the provided objects.
// It's backed by a very simple object tracker that processes creates, updates and deletions as-is,
// without applying any field management, validations and/or defaults. It shouldn't be considered a replacement
// for a real clientset and is mostly useful in simple unit tests.
func NewSimpleClientset(objects ...runtime.Object) *Clientset {
	o := testing.NewObjectTracker(scheme, codecs.UniversalDecoder())
	for _, obj := range objects {
		if err := o.Add(obj); err != nil {
			panic(err)
		}
	}

	cs := &Clientset{tracker: o}
	cs.discovery = &fakediscovery.FakeDiscovery{Fake: &cs.Fake}
	cs.AddReactor("*", "*", testing.ObjectReaction(o))
	cs.AddWatchReactor("*", func(action testing.Action) (handled bool, ret watch.Interface, err error) {
        var opts metav1.ListOptions
        if watchAction, ok := action.(testing.WatchActionImpl); ok {
            opts = watchAction.ListOptions
        }
		gvr := action.GetResource()
		ns := action.GetNamespace()
		watch, err := o.Watch(gvr, ns, opts)
		if err != nil {
			return false, nil, err
		}
		return true, watch, nil
	})

	return cs
}

// Clientset implements clientset.Interface. Meant to be embedded into a
// struct to get a default implementation. This makes faking out just the method
// you want to test easier.
type Clientset struct {
	testing.Fake
	discovery *fakediscovery.FakeDiscovery
	tracker testing.ObjectTracker
}

func (c *Clientset) Discovery() discovery.DiscoveryInterfaces {
	return c.discovery
}

func (c *Clientset) Tracker() testing.ObjectTracker {
	return c.tracker
}

// IsWatchListSemanticsUnSupported informs the reflector that this client
// doesn't support WatchList semantics.
//
// This is a synthetic method whose sole purpose is to satisfy the optional
// interface check performed by the reflector.
// Returning true signals that WatchList can NOT be used.
// No additional logic is implemented here.
func (c *Clientset) IsWatchListSemanticsUnSupported() bool {
	return true
}
`

// checkImpl 输出编译期接口断言，确保 fake clientset 与真实 clientset
// 的接口保持同步；接口一旦变化，生成代码会直接编译失败。
var checkImpl = `
var (
	_ clientset.Interface = &Clientset{}
	_ testing.FakeClient  = &Clientset{}
)
`

// clientsetInterfaceImplTemplate 为每个 group/version 生成访问方法，
// 返回对应 fake typed client，并共享同一个 testing.Fake 以复用 reactors。
var clientsetInterfaceImplTemplate = `
// $.GroupGoName$$.Version$ retrieves the $.GroupGoName$$.Version$Client
func (c *Clientset) $.GroupGoName$$.Version$() $.PackageAlias$.$.GroupGoName$$.Version$Interface {
	return &fake$.PackageAlias$.Fake$.GroupGoName$$.Version${Fake: &c.Fake}
}
`
