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

package negotiation

import (
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/munnerz/goautoneg"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// MediaTypesForSerializer 返回服务器可用的媒体类型与流媒体类型列表。
// 流式类型用于 watch；开启 CBOR 特性门控时额外提供 CBOR Sequence，
// 因为 CBOR 的流式格式并非简单的 “;stream=watch” 参数化形式。
func MediaTypesForSerializer(ns runtime.NegotiatedSerializer) (mediaTypes, streamMediaTypes []string) {
	for _, info := range ns.SupportedMediaTypes() {
		mediaTypes = append(mediaTypes, info.MediaType)
		if info.StreamSerializer != nil {
			if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) && info.MediaType == runtime.ContentTypeCBOR {
				streamMediaTypes = append(streamMediaTypes, runtime.ContentTypeCBORSequence)
				continue
			}
			// stream=watch 是 watch 沿用的既有 mime 参数
			streamMediaTypes = append(streamMediaTypes, info.MediaType+";stream=watch")
		}
	}
	return mediaTypes, streamMediaTypes
}

// NegotiateOutputMediaType 协商输出的结构化媒体类型与序列化器，失败时返回错误。
// 客户端可通过 Accept 参数请求 pretty 输出；未显式请求时，已知的人类客户端
// （curl/Wget/浏览器）会自动获得 pretty 序列化。
func NegotiateOutputMediaType(req *http.Request, ns runtime.NegotiatedSerializer, restrictions EndpointRestrictions) (MediaTypeOptions, runtime.SerializerInfo, error) {
	mediaType, ok := NegotiateMediaTypeOptions(req.Header.Get("Accept"), ns.SupportedMediaTypes(), restrictions)
	if !ok {
		supported, _ := MediaTypesForSerializer(ns)
		return mediaType, runtime.SerializerInfo{}, NewNotAcceptableError(supported)
	}
	// TODO: 移到 resthandler 中
	info := mediaType.Accepted
	if (mediaType.Pretty || isPrettyPrint(req)) && info.PrettySerializer != nil {
		info.Serializer = info.PrettySerializer
	}
	return mediaType, info, nil
}

// NegotiateOutputMediaTypeStream 返回给定请求的流式序列化器。
// 只有声明了 StreamSerializer 的类型才能用于 watch，否则返回 NotAcceptable。
func NegotiateOutputMediaTypeStream(req *http.Request, ns runtime.NegotiatedSerializer, restrictions EndpointRestrictions) (runtime.SerializerInfo, error) {
	mediaType, ok := NegotiateMediaTypeOptions(req.Header.Get("Accept"), ns.SupportedMediaTypes(), restrictions)
	if !ok || mediaType.Accepted.StreamSerializer == nil {
		_, supported := MediaTypesForSerializer(ns)
		return runtime.SerializerInfo{}, NewNotAcceptableError(supported)
	}
	return mediaType.Accepted, nil
}

// NegotiateInputSerializer 返回该请求对应的输入序列化器。
func NegotiateInputSerializer(req *http.Request, streaming bool, ns runtime.NegotiatedSerializer) (runtime.SerializerInfo, error) {
	mediaType := req.Header.Get("Content-Type")
	return NegotiateInputSerializerForMediaType(mediaType, streaming, ns)
}

// NegotiateInputSerializerForMediaType 为给定媒体类型返回合适的序列化器。
// 空 Content-Type 时回退到首个受支持类型，保持与历史客户端（如 kubectl）的兼容。
func NegotiateInputSerializerForMediaType(mediaType string, streaming bool, ns runtime.NegotiatedSerializer) (runtime.SerializerInfo, error) {
	mediaTypes := ns.SupportedMediaTypes()
	if len(mediaType) == 0 {
		mediaType = mediaTypes[0].MediaType
	}
	if mediaType, _, err := mime.ParseMediaType(mediaType); err == nil {
		if info, ok := runtime.SerializerInfoForMediaType(mediaTypes, mediaType); ok {
			return info, nil
		}
	}

	supported, streamingSupported := MediaTypesForSerializer(ns)
	if streaming {
		return runtime.SerializerInfo{}, NewUnsupportedMediaTypeError(streamingSupported)
	}
	return runtime.SerializerInfo{}, NewUnsupportedMediaTypeError(supported)
}

// isPrettyPrint 在 "pretty" 查询参数为真，或 User-Agent 匹配已知的
// "人类"客户端时返回 true。
func isPrettyPrint(req *http.Request) bool {
	// DEPRECATED: 它本应是内容类型的一部分。
	if req.URL != nil {
		// 避免解析 URL query 带来的内存分配。
		// 先做廉价的子串检查，只有确实包含 pretty 时才解析 query。
		if strings.Contains(req.URL.RawQuery, "pretty") {
			pp := req.URL.Query().Get("pretty")
			if len(pp) > 0 {
				pretty, _ := strconv.ParseBool(pp)
				return pretty
			}
		}
	}
	userAgent := req.UserAgent()
	// 该正则覆盖常见的浏览器与命令行 HTTP 工具。
	if strings.HasPrefix(userAgent, "curl") || strings.HasPrefix(userAgent, "Wget") || strings.HasPrefix(userAgent, "Mozilla/5.0") {
		return true
	}
	return false
}

// EndpointRestrictions 接口让内容协商能够校验服务器对特定选项的支持情况。
type EndpointRestrictions interface {
	// AllowsMediaTypeTransform 在端点允许请求的媒体类型或请求的转换时返回 true。
	// 返回 false 时调用方应忽略该媒体类型；若 target 为 nil，说明客户端并未请求转换。
	AllowsMediaTypeTransform(mimeType, mimeSubType string, target *schema.GroupVersionKind) bool
	// AllowsServerVersion 在给定版本对 server group 合法时应返回 true。
	AllowsServerVersion(version string) bool
	// AllowsStreamSchema 在给定流 schema 对 server group 合法时应返回 true。
	AllowsStreamSchema(schema string) bool
}

// DefaultEndpointRestrictions 是默认的 EndpointRestrictions，
// 允许内容协商校验服务器对特定选项的支持情况。
var DefaultEndpointRestrictions = emptyEndpointRestrictions{}

// emptyEndpointRestrictions 只允许不请求类型转换、仅 watch 流的保守策略。
type emptyEndpointRestrictions struct{}

// AllowsMediaTypeTransform 仅当客户端没有请求 GVK 转换时放行。
func (emptyEndpointRestrictions) AllowsMediaTypeTransform(mimeType string, mimeSubType string, gvk *schema.GroupVersionKind) bool {
	return gvk == nil
}

// AllowsServerVersion 默认不允许指定服务端版本。
func (emptyEndpointRestrictions) AllowsServerVersion(string) bool { return false }

// AllowsStreamSchema 仅允许 watch。
func (emptyEndpointRestrictions) AllowsStreamSchema(s string) bool { return s == "watch" }

// MediaTypeOptions 描述某个媒体类型上可能改变服务器响应的信息。
type MediaTypeOptions struct {
	// pretty 为 true 时，请求的表示形式应为便于人阅读的排版。
	Pretty bool

	// stream 若设置，表示希望使用该编码的流式协议变体。
	// 目前唯一支持的取值是 watch，返回带版本的事件；将来可能引入其他流协议。
	Stream string

	// convert 表示希望改变服务器返回对象的类型，而非通常的响应形式。
	Convert *schema.GroupVersionKind
	// useServerVersion 是 server group 的可选版本。
	UseServerVersion string

	// export 为 true 时，请求的表示形式应排除服务器设置的字段。
	Export bool

	// profile 控制 discovery profile（例如 "local" 表示本地、非对等聚合的发现）。
	Profile string

	// unrecognized 列出所有无法识别的键。
	// 记录未知参数而非直接报错，为未来扩展参数留出前向兼容空间。
	Unrecognized []string

	// 客户端接受的媒体类型
	Accepted runtime.SerializerInfo
}

// acceptMediaTypeOptions 返回与给定媒体类型参数相匹配的选项对象。
// 若返回 false，说明这些选项不被允许，必须跳过该媒体类型。参数是无版本的，不可更改。
func acceptMediaTypeOptions(params map[string]string, accepts *runtime.SerializerInfo, endpoint EndpointRestrictions) (MediaTypeOptions, bool) {
	var options MediaTypeOptions

	// 提取所有已知参数。
	// 未知参数只记录不拒绝；但若 endpoint 限制不满足，则整个媒体类型被跳过。
	for k, v := range params {
		switch k {

		// 控制对象返回时的转换方式。
		case "as":
			if options.Convert == nil {
				options.Convert = &schema.GroupVersionKind{}
			}
			options.Convert.Kind = v
		case "g":
			if options.Convert == nil {
				options.Convert = &schema.GroupVersionKind{}
			}
			options.Convert.Group = v
		case "v":
			if options.Convert == nil {
				options.Convert = &schema.GroupVersionKind{}
			}
			options.Convert.Version = v

			// 控制流式 schema。
		case "stream":
			if len(v) > 0 && (accepts.StreamSerializer == nil || !endpoint.AllowsStreamSchema(v)) {
				return MediaTypeOptions{}, false
			}
			options.Stream = v

			// 控制用于通用输出的 server API group 版本。
		case "sv":
			if len(v) > 0 && !endpoint.AllowsServerVersion(v) {
				return MediaTypeOptions{}, false
			}
			options.UseServerVersion = v

			// 若指定，服务器应转换返回的输出，移除那些始终由服务器设置
			// 或符合默认行为的字段。
		case "export":
			options.Export = v == "1"

			// 若指定，则使用 pretty 序列化器。
		case "pretty":
			options.Pretty = v == "1"

			// 控制 discovery profile（例如 local 与 peer-aggregated）。
		case "profile":
			options.Profile = v

		default:
			options.Unrecognized = append(options.Unrecognized, k)
		}
	}

	// 最终由 endpoint 决定是否接受该媒体类型及其转换请求。
	if !endpoint.AllowsMediaTypeTransform(accepts.MediaTypeType, accepts.MediaTypeSubType, options.Convert) {
		return MediaTypeOptions{}, false
	}

	options.Accepted = *accepts
	return options, true
}

// NegotiateMediaTypeOptions 根据 Accept 头、可选媒体类型列表以及请求 scope，
// 返回最合适的内容类型及被接受的媒体类型参数。
// 按客户端偏好顺序遍历 Accept 子句，返回第一个被服务端与 endpoint 同时接受的选项；
// 空 Accept 时回退到服务端首个类型。
func NegotiateMediaTypeOptions(header string, accepted []runtime.SerializerInfo, endpoint EndpointRestrictions) (MediaTypeOptions, bool) {
	if len(header) == 0 && len(accepted) > 0 {
		return MediaTypeOptions{
			Accepted: accepted[0],
		}, true
	}

	clauses := goautoneg.ParseAccept(header)
	for i := range clauses {
		clause := &clauses[i]
		for i := range accepted {
			accepts := &accepted[i]
			// 支持精确匹配以及 type/*、*/* 通配。
			switch {
			case clause.Type == accepts.MediaTypeType && clause.SubType == accepts.MediaTypeSubType,
				clause.Type == accepts.MediaTypeType && clause.SubType == "*",
				clause.Type == "*" && clause.SubType == "*":
				if retVal, ret := acceptMediaTypeOptions(clause.Params, accepts, endpoint); ret {
					return retVal, true
				}
			}
		}
	}

	return MediaTypeOptions{}, false
}
