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

// LongRunningRequestCheck is a predicate which is true for long-running http requests.
// 长时请求（watch、exec、portforward 等）不参与请求超时与优雅终止的等待，
// 因此过滤器需要一个谓词来识别它们。
type LongRunningRequestCheck func(r *http.Request, requestInfo *RequestInfo) bool

// RequestInfoResolver 从原始 HTTP 请求提取 RequestInfo；实现必须对同名
// 请求保持一致结果，因为下游鉴权与审计都依赖该解析。
type RequestInfoResolver interface {
	NewRequestInfo(req *http.Request) (*RequestInfo, error)
}

// RequestInfo holds information parsed from the http.Request
// RequestInfo 是鉴权、审计、限流与指标共用的请求元数据视图。
type RequestInfo struct {
	// IsResourceRequest indicates whether or not the request is for an API resource or subresource
	IsResourceRequest bool
	// Path is the URL path of the request
	Path string
	// Verb is the kube verb associated with the request for API requests, not the http verb.  This includes things like list and watch.
	// for non-resource requests, this is the lowercase http verb
	Verb string

	APIPrefix  string
	APIGroup   string
	APIVersion string
	Namespace  string
	// Resource is the name of the resource being requested.  This is not the kind.  For example: pods
	Resource string
	// Subresource is the name of the subresource being requested.  This is a different resource, scoped to the parent resource, but it may have a different kind.
	// For instance, /pods has the resource "pods" and the kind "Pod", while /pods/foo/status has the resource "pods", the sub resource "status", and the kind "Pod"
	// (because status operates on pods). The binding resource for a pod though may be /pods/foo/binding, which has resource "pods", subresource "binding", and kind "Binding".
	Subresource string
	// Name is empty for some verbs, but if the request directly indicates a name (not in body content) then this field is filled in.
	Name string
	// Parts are the path parts for the request, always starting with /{resource}/{name}
	Parts []string

	// FieldSelector contains the unparsed field selector from a request.  It is only present if the apiserver
	// honors field selectors for the verb this request is associated with.
	FieldSelector string
	// LabelSelector contains the unparsed field selector from a request.  It is only present if the apiserver
	// honors field selectors for the verb this request is associated with.
	LabelSelector string
}

// specialVerbs contains just strings which are used in REST paths for special actions that don't fall under the normal
// CRUDdy GET/POST/PUT/DELETE actions on REST objects.
// TODO: find a way to keep this up to date automatically.  Maybe dynamically populate list as handlers added to
// master's Mux.
// proxy 与 watch 出现在 URL 路径段中而非 HTTP 方法中，解析时需优先识别。
var specialVerbs = sets.NewString("proxy", "watch")

// specialVerbsNoSubresources contains root verbs which do not allow subresources
// proxy 的路径段布局与普通资源不同，不能按“第三段即子资源”解析。
var specialVerbsNoSubresources = sets.NewString("proxy")

// namespaceSubresources contains subresources of namespace
// this list allows the parser to distinguish between a namespace subresource, and a namespaced resource
// 例如 /namespaces/default/status 属于 Namespace 的子资源，而
// /namespaces/default/pods 是命名空间内的资源。
var namespaceSubresources = sets.NewString("status", "finalize")

// verbsWithSelectors is the list of verbs which support fieldSelector and labelSelector parameters
// 只有多对象操作需要按选择器鉴权；其他 verb 上的选择器不应影响鉴权结果。
var verbsWithSelectors = sets.NewString("list", "watch", "deletecollection")

// NamespaceSubResourcesForTest exports namespaceSubresources for testing in pkg/controlplane/master_test.go, so we never drift
// 导出副本而非原集合，防止测试意外修改解析器行为。
var NamespaceSubResourcesForTest = sets.NewString(namespaceSubresources.List()...)

// RequestInfoFactory 根据 API 前缀区分带组与无组路径：
// /apis/{group}/{version}/... 与 /api/{version}/...。
type RequestInfoFactory struct {
	APIPrefixes          sets.String // without leading and trailing slashes
	GrouplessAPIPrefixes sets.String // without leading and trailing slashes
}

// TODO write an integration test against the swagger doc to test the RequestInfo and match up behavior to responses
// NewRequestInfo returns the information from the http request.  If error is not nil, RequestInfo holds the information as best it is known before the failure
// It handles both resource and non-resource requests and fills in all the pertinent information for each.
// Valid Inputs:
// Resource paths
// /apis/{api-group}/{version}/namespaces
// /api/{version}/namespaces
// /api/{version}/namespaces/{namespace}
// /api/{version}/namespaces/{namespace}/{resource}
// /api/{version}/namespaces/{namespace}/{resource}/{resourceName}
// /api/{version}/{resource}
// /api/{version}/{resource}/{resourceName}
//
// Special verbs without subresources:
// /api/{version}/proxy/{resource}/{resourceName}
// /api/{version}/proxy/namespaces/{namespace}/{resource}/{resourceName}
//
// Special verbs with subresources:
// /api/{version}/watch/{resource}
// /api/{version}/watch/namespaces/{namespace}/{resource}
//
// NonResource paths
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
	// start with a non-resource request until proven otherwise
	requestInfo := RequestInfo{
		IsResourceRequest: false,
		Path:              req.URL.Path,
		Verb:              strings.ToLower(req.Method),
	}

	currentParts := splitPath(req.URL.Path)
	// 少于 3 段不可能是 /api/{version}/{resource}，直接视为非资源请求。
	if len(currentParts) < 3 {
		// return a non-resource request
		return &requestInfo, nil
	}

	// 首段必须是已注册的 API 前缀（如 api、apis），否则是 /healthz 之类。
	if !r.APIPrefixes.Has(currentParts[0]) {
		// return a non-resource request
		return &requestInfo, nil
	}
	requestInfo.APIPrefix = currentParts[0]
	currentParts = currentParts[1:]

	// /apis 系路径需要组名，因此消耗一段后仍需至少 3 段；/api 无组路径则不需要。
	if !r.GrouplessAPIPrefixes.Has(requestInfo.APIPrefix) {
		// one part (APIPrefix) has already been consumed, so this is actually "do we have four parts?"
		if len(currentParts) < 3 {
			// return a non-resource request
			return &requestInfo, nil
		}

		requestInfo.APIGroup = currentParts[0]
		currentParts = currentParts[1:]
	}

	requestInfo.IsResourceRequest = true
	requestInfo.APIVersion = currentParts[0]
	currentParts = currentParts[1:]

	// handle input of form /{specialVerb}/*
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

	// URL forms: /namespaces/{namespace}/{kind}/*, where parts are adjusted to be relative to kind
	// 命名空间是作用域而非资源：解析后要把它从路径中剥离，使 Parts 始终
	// 以资源名开头。
	if currentParts[0] == "namespaces" {
		if len(currentParts) > 1 {
			requestInfo.Namespace = currentParts[1]

			// if there is another step after the namespace name and it is not a known namespace subresource
			// move currentParts to include it as a resource in its own right
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

	// parsing successful, so we now know the proper value for .Parts
	requestInfo.Parts = currentParts

	// parts look like: resource/resourceName/subresource/other/stuff/we/don't/interpret
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

	// if there's no name on the request and we thought it was a get before, then the actual verb is a list or a watch
	// 无名称的 GET 实际是 list/watch，需要解析查询参数才能区分。
	if len(requestInfo.Name) == 0 && requestInfo.Verb == "get" {
		opts := metainternalversion.ListOptions{}
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), metav1.SchemeGroupVersion, &opts); err != nil {
			// An error in parsing request will result in default to "list" and not setting "name" field.
			klog.ErrorS(err, "Couldn't parse request", "request", req.URL.Query())
			// Reset opts to not rely on partial results from parsing.
			// However, if watch is set, let's report it.
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

	// if there's no name on the request and we thought it was a delete before, then the actual verb is deletecollection
	if len(requestInfo.Name) == 0 && requestInfo.Verb == "delete" {
		requestInfo.Verb = "deletecollection"
	}

	// Don't support selector authorization on requests that used the deprecated verb-via-path mechanism, since they don't support selectors consistently.
	// There are multi-object and single-object watch endpoints, and only the multi-object one supports selectors.
	// 选择器单独从原始查询串读取：ListOptions 解码失败时选择器会被丢弃，
	// 但鉴权仍需要看到它们（宁可多鉴权也不放过按选择器批量操作）。
	if !verbViaPathPrefix && verbsWithSelectors.Has(requestInfo.Verb) {
		// interestingly these are parsed above, but the current structure there means that if one (or anything) in the
		// listOptions fails to decode, the field and label selectors are lost.
		// therefore, do the straight query param read here.
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

// requestInfoKey is the RequestInfo key for the context. It's of private type here. Because
// keys are interfaces and interfaces are equal when the type and the value is equal, this
// does not conflict with the keys defined in pkg/api.
const requestInfoKey requestInfoKeyType = iota

// WithRequestInfo returns a copy of parent in which the request info value is set
func WithRequestInfo(parent context.Context, info *RequestInfo) context.Context {
	return WithValue(parent, requestInfoKey, info)
}

// RequestInfoFrom returns the value of the RequestInfo key on the ctx
// 第二个返回值区分“未设置”与“设置为 nil”，调用方不应把缺失当成空结构。
func RequestInfoFrom(ctx context.Context) (*RequestInfo, bool) {
	info, ok := ctx.Value(requestInfoKey).(*RequestInfo)
	return info, ok
}

// splitPath returns the segments for a URL path.
// 去掉首尾斜杠后再切分，因此 "/api/v1/" 与 "api/v1" 得到相同片段。
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}
