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

package generators

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

// genClientset 为 clientset 生成一个包（即聚合所有 group client 的 Clientset）。
type genClientset struct {
	generator.GoGenerator
	groups             []clientgentypes.GroupVersions
	groupGoNames       map[clientgentypes.GroupVersion]string
	clientsetPackage   string // 必须是 Go import path
	imports            namer.ImportTracker
	clientsetGenerated bool
}

var _ generator.Generator = &genClientset{}

// Namers 为 clientset 生成器注册 "raw" 命名系统，用于输出完整的限定类型名。
func (g *genClientset) Namers(c *generator.Context) namer.NameSystems {
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.clientsetPackage, g.imports),
	}
}

// Filter 让 GenerateType() 只被调用一次：clientset 是单文件产物，
// 与输入类型数量无关，因此首类型后即置位并返回 false。
func (g *genClientset) Filter(c *generator.Context, t *types.Type) bool {
	ret := !g.clientsetGenerated
	g.clientsetGenerated = true
	return ret
}

// Imports 汇总各 group-version 的 typed client 包，并为它们生成唯一的别名，
// 避免不同 group 的同名版本包（如 v1）发生导入冲突。
func (g *genClientset) Imports(c *generator.Context) (imports []string) {
	imports = append(imports, g.imports.ImportLines()...)
	for _, group := range g.groups {
		for _, version := range group.Versions {
			typedClientPath := path.Join(g.clientsetPackage, "typed", strings.ToLower(group.PackageName), strings.ToLower(version.NonEmpty()))
			groupAlias := strings.ToLower(g.groupGoNames[clientgentypes.GroupVersion{Group: group.Group, Version: version.Version}])
			imports = append(imports, fmt.Sprintf("%s%s \"%s\"", groupAlias, strings.ToLower(version.NonEmpty()), typedClientPath))
		}
	}
	return
}

// GenerateType 渲染 clientset 的全部模板片段。
// 模板按“接口 -> 结构体 -> 各 group 实现 -> 构造函数”的顺序输出，
// 该顺序决定生成文件的内容布局，必须保持稳定以便 diff 审查。
func (g *genClientset) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	// TODO: 生成 clientset 其实不需要类型信息，
	// 或许可以让 gengo 支持这种用法。
	sw := generator.NewSnippetWriter(w, c, "$", "$")

	allGroups := clientgentypes.ToGroupVersionInfo(g.groups, g.groupGoNames)
	// 模板中引用的符号统一从 universe 中解析，保证生成代码的类型名与实际包一致。
	m := map[string]interface{}{
		"allGroups":                            allGroups,
		"fmtErrorf":                            c.Universe.Type(types.Name{Package: "fmt", Name: "Errorf"}),
		"Config":                               c.Universe.Type(types.Name{Package: "k8s.io/client-go/rest", Name: "Config"}),
		"DefaultKubernetesUserAgent":           c.Universe.Function(types.Name{Package: "k8s.io/client-go/rest", Name: "DefaultKubernetesUserAgent"}),
		"RESTClientInterface":                  c.Universe.Type(types.Name{Package: "k8s.io/client-go/rest", Name: "Interface"}),
		"RESTHTTPClientFor":                    c.Universe.Function(types.Name{Package: "k8s.io/client-go/rest", Name: "HTTPClientFor"}),
		"DiscoveryInterfaces":                  c.Universe.Type(types.Name{Package: "k8s.io/client-go/discovery", Name: "DiscoveryInterfaces"}),
		"DiscoveryClient":                      c.Universe.Type(types.Name{Package: "k8s.io/client-go/discovery", Name: "DiscoveryClient"}),
		"httpClient":                           c.Universe.Type(types.Name{Package: "net/http", Name: "Client"}),
		"NewDiscoveryClientForConfigAndClient": c.Universe.Function(types.Name{Package: "k8s.io/client-go/discovery", Name: "NewDiscoveryClientForConfigAndClient"}),
		"NewDiscoveryClientForConfigOrDie":     c.Universe.Function(types.Name{Package: "k8s.io/client-go/discovery", Name: "NewDiscoveryClientForConfigOrDie"}),
		"NewDiscoveryClient":                   c.Universe.Function(types.Name{Package: "k8s.io/client-go/discovery", Name: "NewDiscoveryClient"}),
		"flowcontrolNewTokenBucketRateLimiter": c.Universe.Function(types.Name{Package: "k8s.io/client-go/util/flowcontrol", Name: "NewTokenBucketRateLimiter"}),
	}
	sw.Do(clientsetInterface, m)
	sw.Do(clientsetTemplate, m)
	for _, g := range allGroups {
		sw.Do(clientsetInterfaceImplTemplate, g)
	}
	sw.Do(getDiscoveryTemplate, m)
	sw.Do(newClientsetForConfigTemplate, m)
	sw.Do(newClientsetForConfigAndClientTemplate, m)
	sw.Do(newClientsetForConfigOrDieTemplate, m)
	sw.Do(newClientsetForRESTClientTemplate, m)

	return sw.Error()
}

// clientsetInterface 定义生成的 Clientset 接口：包含 Discovery 以及各 group-version 的访问方法。
// 这些方法的命名与 typed client 包中的接口名严格对应。
var clientsetInterface = `
type Interface interface {
	Discovery() $.DiscoveryInterfaces|raw$
    $range .allGroups$$.GroupGoName$$.Version$() $.PackageAlias$.$.GroupGoName$$.Version$Interface
	$end$
}
`

// clientsetTemplate 定义 Clientset 结构体，内嵌 DiscoveryClient 并为每个 group-version
// 持有一个具体 client 字段（字段名小写，与接口方法名区分）。
var clientsetTemplate = `
// Clientset contains the clients for groups.
type Clientset struct {
	*$.DiscoveryClient|raw$
    $range .allGroups$$.LowerCaseGroupGoName$$.Version$ *$.PackageAlias$.$.GroupGoName$$.Version$Client
    $end$
}
`

// clientsetInterfaceImplTemplate 为每个 group-version 生成接口实现方法。
var clientsetInterfaceImplTemplate = `
// $.GroupGoName$$.Version$ retrieves the $.GroupGoName$$.Version$Client
func (c *Clientset) $.GroupGoName$$.Version$() $.PackageAlias$.$.GroupGoName$$.Version$Interface {
	return c.$.LowerCaseGroupGoName$$.Version$
}
`

// getDiscoveryTemplate 生成 Discovery() 方法；nil 检查让零值 Clientset 也能安全调用。
var getDiscoveryTemplate = `
// Discovery retrieves the DiscoveryClient
func (c *Clientset) Discovery() $.DiscoveryInterfaces|raw$ {
	if c == nil {
		return nil
	}
	return c.DiscoveryClient
}
`

// newClientsetForConfigTemplate 生成 NewForConfig；在浅拷贝上补全 UserAgent，
// 并把构造出的 httpClient 交给 NewForConfigAndClient，从而在所有 client 间共享 transport。
var newClientsetForConfigTemplate = `
// NewForConfig creates a new Clientset for the given config.
// If config's RateLimiter is not set and QPS and Burst are acceptable, 
// NewForConfig will generate a rate-limiter in configShallowCopy.
// NewForConfig is equivalent to NewForConfigAndClient(c, httpClient),
// where httpClient was generated with rest.HTTPClientFor(c).
func NewForConfig(c *$.Config|raw$) (*Clientset, error) {
	configShallowCopy := *c

	if configShallowCopy.UserAgent == "" {
		configShallowCopy.UserAgent = $.DefaultKubernetesUserAgent|raw$()
	}

	// share the transport between all clients
	httpClient, err := $.RESTHTTPClientFor|raw$(&configShallowCopy)
	if err != nil {
		return nil, err
	}

	return NewForConfigAndClient(&configShallowCopy, httpClient)
}
`

// newClientsetForConfigAndClientTemplate 生成 NewForConfigAndClient；若未设置限流器，
// 则按 QPS/Burst 构造 token bucket，随后逐个构造各 group-version client 与 discovery client。
var newClientsetForConfigAndClientTemplate = `
// NewForConfigAndClient creates a new Clientset for the given config and http client.
// Note the http client provided takes precedence over the configured transport values.
// If config's RateLimiter is not set and QPS and Burst are acceptable,
// NewForConfigAndClient will generate a rate-limiter in configShallowCopy.
func NewForConfigAndClient(c *$.Config|raw$, httpClient *$.httpClient|raw$) (*Clientset, error) {
	configShallowCopy := *c
	if configShallowCopy.RateLimiter == nil && configShallowCopy.QPS > 0 {
		if configShallowCopy.Burst <= 0 {
			return nil, $.fmtErrorf|raw$("burst is required to be greater than 0 when RateLimiter is not set and QPS is set to greater than 0")
		}
		configShallowCopy.RateLimiter = $.flowcontrolNewTokenBucketRateLimiter|raw$(configShallowCopy.QPS, configShallowCopy.Burst)
	}

	var cs Clientset
	var err error
$range .allGroups$    cs.$.LowerCaseGroupGoName$$.Version$, err =$.PackageAlias$.NewForConfigAndClient(&configShallowCopy, httpClient)
	if err!=nil {
		return nil, err
	}
$end$
	cs.DiscoveryClient, err = $.NewDiscoveryClientForConfigAndClient|raw$(&configShallowCopy, httpClient)
	if err!=nil {
		return nil, err
	}
	return &cs, nil
}
`

// newClientsetForConfigOrDieTemplate 生成 NewForConfigOrDie：出错即 panic，供启动期使用。
var newClientsetForConfigOrDieTemplate = `
// NewForConfigOrDie creates a new Clientset for the given config and
// panics if there is an error in the config.
func NewForConfigOrDie(c *$.Config|raw$) *Clientset {
	cs, err := NewForConfig(c)
	if err!=nil {
		panic(err)
	}
	return cs
}
`

// newClientsetForRESTClientTemplate 生成 New：用已有的 RESTClient 组装 Clientset，
// 不做任何配置推导，便于调用方完全掌控底层连接。
var newClientsetForRESTClientTemplate = `
// New creates a new Clientset for the given RESTClient.
func New(c $.RESTClientInterface|raw$) *Clientset {
	var cs Clientset
$range .allGroups$    cs.$.LowerCaseGroupGoName$$.Version$ =$.PackageAlias$.New(c)
$end$
	cs.DiscoveryClient = $.NewDiscoveryClient|raw$(c)
	return &cs
}
`
