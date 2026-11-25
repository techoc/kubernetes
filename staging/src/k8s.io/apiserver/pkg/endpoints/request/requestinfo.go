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

package request

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/klog/v2"
)

// LongRunningRequestCheck 是一个谓词，对长时运行的 HTTP 请求返回 true。
// 长时请求（watch、exec、portforward 等）不参与请求超时与优雅终止的等待，
// 因此过滤器需要一个谓词来识别它们。
type LongRunningRequestCheck func(r *http.Request, requestInfo *RequestInfo) bool

// RequestInfoResolver 从原始 HTTP 请求提取 RequestInfo；实现必须对同名
// 请求保持一致结果，因为下游鉴权与审计都依赖该解析。
type RequestInfoResolver interface {
	NewRequestInfo(req *http.Request) (*RequestInfo, error)
}

// RequestInfo 保存从 http.Request 解析出来的信息。
// RequestInfo 是鉴权、审计、限流与指标共用的请求元数据视图。
type RequestInfo struct {
	// IsResourceRequest 表示请求是否针对某个 API 资源或子资源。
	IsResourceRequest bool
	// Path 是请求的 URL 路径。
	Path string
	// Verb 是 API 请求对应的 kube verb（不是 HTTP 方法），包括 list、watch 等。
	// 对非资源请求，这里是小写的 HTTP 方法。
	Verb string

	APIPrefix  string
	APIGroup   string
	APIVersion string
	Namespace  string
	// Resource 是被请求的资源名，注意不是 kind，例如 pods。
	Resource string
	// Subresource 是被请求的子资源名。它是隶属于父资源作用域的另一个资源，kind 可能不同。
	// 例如 /pods 的资源是 "pods"、kind 是 "Pod"；/pods/foo/status 的资源仍是 "pods"、子资源是 "status"、kind 是 "Pod"
	// （因为 status 操作的对象是 pod）；而 /pods/foo/binding 的资源是 "pods"、子资源是 "binding"、kind 是 "Binding"。
	Subresource string
	// Name 对某些 verb 为空；若请求直接给出了名称（而非放在请求体中），该字段会被填充。
	Name string
	// Parts 是请求的路径分段，始终以 /{resource}/{name} 开头。
	Parts []string

	// FieldSelector 是请求中尚未解析的字段选择器，仅当 apiserver 对该 verb 支持字段选择器时才有值。
	FieldSelector string
	// LabelSelector 是请求中尚未解析的标签选择器，仅当 apiserver 对该 verb 支持标签选择器时才有值。
	LabelSelector string
}

// specialVerbs 只包含那些用于 REST 路径、表示特殊动作的字符串，这些动作不属于常规的
// GET/POST/PUT/DELETE 增删改查。
// TODO: 想办法让这个列表自动保持最新，例如在 handler 注册到 master 的 Mux 时动态填充。
// proxy 与 watch 出现在 URL 路径段中而非 HTTP 方法中，解析时需优先识别。
var specialVerbs = sets.NewString("proxy", "watch")

// specialVerbsNoSubresources 是不允许带子资源的根级 verb。
// proxy 的路径段布局与普通资源不同，不能按“第三段即子资源”解析。
var specialVerbsNoSubresources = sets.NewString("proxy")

// namespaceSubresources 列出 Namespace 的子资源，使解析器能区分“命名空间的子资源”
// 与“某命名空间下的资源”。
// 例如 /namespaces/default/status 属于 Namespace 的子资源，而
// /namespaces/default/pods 是命名空间内的资源。
var namespaceSubresources = sets.NewString("status", "finalize")

// verbsWithSelectors 是支持 fieldSelector 与 labelSelector 参数的 verb 列表。
// 只有多对象操作需要按选择器鉴权；其他 verb 上的选择器不应影响鉴权结果。
var verbsWithSelectors = sets.NewString("list", "watch", "deletecollection")

// NamespaceSubResourcesForTest 导出 namespaceSubresources 供 pkg/controlplane/master_test.go 测试使用，避免两边行为漂移。
// 导出副本而非原集合，防止测试意外修改解析器行为。
var NamespaceSubResourcesForTest = sets.NewString(namespaceSubresources.List()...)

// RequestInfoFactory 根据 API 前缀区分带组与无组路径：
// /apis/{group}/{version}/... 与 /api/{version}/...。
type RequestInfoFactory struct {
	APIPrefixes          sets.String // 不含首尾斜杠。
	GrouplessAPIPrefixes sets.String // 不含首尾斜杠。
}

// TODO: 针对 swagger 文档写一个集成测试，验证 RequestInfo 的行为与响应是否一致。
// NewRequestInfo 返回从 HTTP 请求中解析出的信息。若返回错误，RequestInfo 仍会尽量保留失败前已解析出的内容。
// 它同时处理资源请求与非资源请求，并填满各自的必要信息。
// 合法输入形态：
// 资源路径
// /apis/{api-group}/{version}/namespaces
// /api/{version}/namespaces
// /api/{version}/namespaces/{namespace}
// /api/{version}/namespaces/{namespace}/{resource}
// /api/{version}/namespaces/{namespace}/{resource}/{resourceName}
// /api/{version}/{resource}
// /api/{version}/{resource}/{resourceName}
//
// 不带子资源的特殊 verb：
// /api/{version}/proxy/{resource}/{resourceName}
// /api/{version}/proxy/namespaces/{namespace}/{resource}/{resourceName}
//
// 带子资源的特殊 verb：
// /api/{version}/watch/{resource}
// /api/{version}/watch/namespaces/{namespace}/{resource}
//
// 非资源路径
// /apis/{api-group}/{version}
// /apis/{api-group}
// /apis
// /api/{version}
// /api
// /healthz
// /
// 解析按 URL 片段顺序推进，无法识别的形态一律降级为非资源请求（而不是报错），
// 这样 /healthz 等内置端点无需特殊注册即可被正确归类。
func (r *RequestInfoFactory) NewRequestInfo(req *http.Request) (*RequestInfo, error) {
	// 在证明之前先按非资源请求处理。
	requestInfo := RequestInfo{
		IsResourceRequest: false,
		Path:              req.URL.Path,
		Verb:              strings.ToLower(req.Method),
	}

	currentParts := splitPath(req.URL.Path)
	// 少于 3 段不可能是 /api/{version}/{resource}，直接视为非资源请求。
	if len(currentParts) < 3 {
		// 返回非资源请求。
		return &requestInfo, nil
	}

	// 首段必须是已注册的 API 前缀（如 api、apis），否则是 /healthz 之类。
	if !r.APIPrefixes.Has(currentParts[0]) {
		// 返回非资源请求。
		return &requestInfo, nil
	}
	requestInfo.APIPrefix = currentParts[0]
	currentParts = currentParts[1:]

	// /apis 系路径需要组名，因此消耗一段后仍需至少 3 段；/api 无组路径则不需要。
	if !r.GrouplessAPIPrefixes.Has(requestInfo.APIPrefix) {
		// 其中一段（APIPrefix）已被消耗，所以这里实际是在问“还剩四段吗？”
		if len(currentParts) < 3 {
			// 返回非资源请求。
			return &requestInfo, nil
		}

		requestInfo.APIGroup = currentParts[0]
		currentParts = currentParts[1:]
	}

	requestInfo.IsResourceRequest = true
	requestInfo.APIVersion = currentParts[0]
	currentParts = currentParts[1:]

	// 处理 /{specialVerb}/* 形态的输入。
	// 路径前缀形式（/watch/...）优先于 HTTP 方法映射，二者语义不同：
	// 老式 watch 端点通过路径而非查询参数表达 watch。
	verbViaPathPrefix := false
	if specialVerbs.Has(currentParts[0]) {
		if len(currentParts) < 2 {
			return &requestInfo, fmt.Errorf("unable to determine kind and namespace from url, %v", req.URL)
		}

		requestInfo.Verb = currentParts[0]
		currentParts = currentParts[1:]
		verbViaPathPrefix = true

	} else {
		// 常规 REST 映射：HTTP 方法到 kube verb。HEAD 与 GET 等价。
		switch req.Method {
		case MethodPost:
			requestInfo.Verb = "create"
		case MethodGet, MethodHead:
			requestInfo.Verb = "get"
		case MethodPut:
			requestInfo.Verb = "update"
		case MethodPatch:
			requestInfo.Verb = "patch"
		case MethodDelete:
			requestInfo.Verb = "delete"
		default:
			requestInfo.Verb = ""
		}
	}

	// URL 形态：/namespaces/{namespace}/{kind}/*，其中 Parts 会调整为相对 kind 的部分。
	// 命名空间是作用域而非资源：解析后要把它从路径中剥离，使 Parts 始终
	// 以资源名开头。
	if currentParts[0] == "namespaces" {
		if len(currentParts) > 1 {
			requestInfo.Namespace = currentParts[1]

			// 若命名空间名之后还有一段且它不是已知的命名空间子资源，
			// 就把 currentParts 前移，把它自身当作资源处理。
			// namespaces/status 等子资源保留 Parts，以便后续正确识别。
			if len(currentParts) > 2 && !namespaceSubresources.Has(currentParts[2]) {
				currentParts = currentParts[2:]
			}
		}
	} else {
		// 非命名空间前缀即集群级资源；显式填 NamespaceNone 避免空串与
		// default 命名空间混淆。
		requestInfo.Namespace = metav1.NamespaceNone
	}

	// 解析成功，此时可以确定 .Parts 的最终取值。
	requestInfo.Parts = currentParts

	// Parts 的形态为：resource/resourceName/subresource/其他我们不解释的内容。
	// 用 fallthrough 从深到浅赋值：子资源存在时才设置 Subresource，名称与
	// 资源名同理，缺省保持空值。
	switch {
	case len(requestInfo.Parts) >= 3 && !specialVerbsNoSubresources.Has(requestInfo.Verb):
		requestInfo.Subresource = requestInfo.Parts[2]
		fallthrough
	case len(requestInfo.Parts) >= 2:
		requestInfo.Name = requestInfo.Parts[1]
		fallthrough
	case len(requestInfo.Parts) >= 1:
		requestInfo.Resource = requestInfo.Parts[0]
	}

	// 请求没有名称且此前判定为 get 时，真实 verb 应是 list 或 watch。
	// 无名称的 GET 实际是 list/watch，需要解析查询参数才能区分。
	if len(requestInfo.Name) == 0 && requestInfo.Verb == "get" {
		opts := metainternalversion.ListOptions{}
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), metav1.SchemeGroupVersion, &opts); err != nil {
			// 解析请求出错时默认按 "list" 处理，且不设置 "name" 字段。
			klog.ErrorS(err, "Couldn't parse request", "request", req.URL.Query())
			// 重置 opts，不依赖解析得到的部分结果；
			// 但如果 watch 已设置，仍然如实上报。
			// 解析失败时丢弃部分结果，但仍单独识别 watch 参数，防止把 watch
			// 误当成普通 list 而立即返回。
			opts = metainternalversion.ListOptions{}
			if values := req.URL.Query()["watch"]; len(values) > 0 {
				switch strings.ToLower(values[0]) {
				case "false", "0":
				default:
					opts.Watch = true
				}
			}
		}

		if opts.Watch {
			requestInfo.Verb = "watch"
		} else {
			requestInfo.Verb = "list"
		}

		// 单对象 GET 可通过 metadata.name 字段选择器表达，这里提取名称以
		// 便鉴权按对象粒度判断（同时避免非法名称进入 Name）。
		if opts.FieldSelector != nil {
			if name, ok := opts.FieldSelector.RequiresExactMatch("metadata.name"); ok {
				if len(content.IsPathSegmentName(name)) == 0 {
					requestInfo.Name = name
				}
			}
		}
	}

	// 请求没有名称且此前判定为 delete 时，真实 verb 是 deletecollection。
	if len(requestInfo.Name) == 0 && requestInfo.Verb == "delete" {
		requestInfo.Verb = "deletecollection"
	}

	// 使用已废弃的“verb 写在路径里”机制的请求不支持按选择器鉴权，因为它们对选择器的支持并不一致。
	// watch 端点分为多对象与单对象两种，只有多对象那一种支持选择器。
	// 选择器单独从原始查询串读取：ListOptions 解码失败时选择器会被丢弃，
	// 但鉴权仍需要看到它们（宁可多鉴权也不放过按选择器批量操作）。
	if !verbViaPathPrefix && verbsWithSelectors.Has(requestInfo.Verb) {
		// 这些参数上面其实已经解析过，但那里的结构导致：只要 ListOptions 中某一项解码失败，
		// 字段与标签选择器就会丢失。
		// 因此这里直接读取原始查询参数。
		if vals := req.URL.Query()["fieldSelector"]; len(vals) > 0 {
			requestInfo.FieldSelector = vals[0]
		}
		if vals := req.URL.Query()["labelSelector"]; len(vals) > 0 {
			requestInfo.LabelSelector = vals[0]
		}
	}

	return &requestInfo, nil
}

// requestInfoKeyType 使用未导出类型作为 context key，避免与其他包的字符串
// 键冲突（类型不同即不相等）。
type requestInfoKeyType int

// requestInfoKey 是 context 中存放 RequestInfo 的键，这里是私有类型。
// 由于 key 是接口，而接口仅在类型与值都相等时才相等，
// 因此不会与 pkg/api 中定义的 key 冲突。
const requestInfoKey requestInfoKeyType = iota

// WithRequestInfo 返回 parent 的副本，其中已设置请求信息。
func WithRequestInfo(parent context.Context, info *RequestInfo) context.Context {
	return WithValue(parent, requestInfoKey, info)
}

// RequestInfoFrom 返回 ctx 上 RequestInfo key 的值。
// 第二个返回值区分“未设置”与“设置为 nil”，调用方不应把缺失当成空结构。
func RequestInfoFrom(ctx context.Context) (*RequestInfo, bool) {
	info, ok := ctx.Value(requestInfoKey).(*RequestInfo)
	return info, ok
}

// splitPath 返回 URL 路径的分段列表。
// 去掉首尾斜杠后再切分，因此 "/api/v1/" 与 "api/v1" 得到相同片段。
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}
