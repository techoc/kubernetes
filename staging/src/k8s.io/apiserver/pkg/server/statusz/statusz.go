/*
Copyright 2024 The Kubernetes Authors.

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

package statusz

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	"k8s.io/component-base/compatibility"

	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/statusz/api/v1alpha1"
	"k8s.io/apiserver/pkg/server/statusz/api/v1beta1"
	"k8s.io/apiserver/pkg/server/statusz/negotiate"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

var (
	// delimiters 用于把路径或 flag 形式的启动参数切分为可读片段。
	delimiters = []string{":", ": ", "=", " "}
	// nonDebuggingEndpoints 列出不对应调试能力的顶层路径，聚合展示时跳过，
	// 避免 statusz 把所有 API 与文档端点当作调试端点列出。
	nonDebuggingEndpoints = map[string]bool{
		"/apis":        true,
		"/api":         true,
		"/openid":      true,
		"/openapi":     true,
		"/.well-known": true,
	}
	v1alpha1StatuszKind = v1alpha1.SchemeGroupVersion.WithKind("Statusz")
	v1beta1StatuszKind  = v1beta1.SchemeGroupVersion.WithKind("Statusz")
	// recognizedStructuredKinds 限定协商时可选的 kind，防止请求任意类型。
	recognizedStructuredKinds = map[schema.GroupVersionKind]bool{
		v1alpha1StatuszKind: true,
		v1beta1StatuszKind:  true,
	}
)

// DefaultStatuszPath 是 statusz 端点的标准挂载路径。
const DefaultStatuszPath = "/statusz"

// headerFmt 是纯文本响应的头部；明确声明不保证机器可解析，
// 因为 statusz 面向调试而非自动化客户端。
const headerFmt = `
%s statusz
Warning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.
`

// statuszCodecFactory 包装 CodecFactory，从支持的媒体类型列表中过滤掉不支持的类型（如 protobuf），
// 使错误消息只显示实际可用的媒体类型。
type statuszCodecFactory struct {
	serializer.CodecFactory
	supportedMediaTypes []runtime.SerializerInfo
}

// mux 是 Install 所需的最小注册接口，避免依赖具体路由实现。
type mux interface {
	Handle(path string, handler http.Handler)
}

// ListedPathsOption 是 registry 的选项之一，用于注入可展示的路径列表。
type ListedPathsOption []string

// NewRegistry 创建 statusz registry。
// v1alpha1 被标记为已弃用：对外推荐使用 v1beta1。
func NewRegistry(effectiveVersion compatibility.EffectiveVersion, opts ...Option) statuszRegistry {
	r := &registry{
		effectiveVersion:      effectiveVersion,
		deprecatedVersionsMap: map[string]bool{"v1alpha1": true},
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Install 在 mux 上挂载 /statusz 端点。
// 注册失败时仅记录错误并继续：statusz 是调试辅助，不应阻止组件启动。
func Install(m mux, componentName string, reg statuszRegistry) {
	scheme := runtime.NewScheme()
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	filteredCodecFactory, err := newStatuszCodecFactory(scheme, componentName, reg)
	if err != nil {
		utilruntime.HandleError(err)
	}
	restrictions := negotiate.StatuszEndpointRestrictions{
		RecognizedStructuredKinds: recognizedStructuredKinds,
	}
	m.Handle(DefaultStatuszPath, handleStatusz(componentName, reg, filteredCodecFactory, restrictions))
}

// newStatuszCodecFactory 用 statusz 的标准序列化器创建编解码工厂，
// 过滤掉不支持的媒体类型（如 protobuf）。
// 过滤掉不支持的媒体类型（如 protobuf）。
// 出现实际不可用的媒体类型。
func newStatuszCodecFactory(scheme *runtime.Scheme, componentName string, reg statuszRegistry) (*statuszCodecFactory, error) {
	codecFactoryOpts := []serializer.CodecFactoryOptionsMutator{
		serializer.WithSerializer(func(_ runtime.ObjectCreater, _ runtime.ObjectTyper) runtime.SerializerInfo {
			textSerializer := statuszTextSerializer{componentName, reg}
			return runtime.SerializerInfo{
				MediaType:        "text/plain",
				MediaTypeType:    "text",
				MediaTypeSubType: "plain",
				EncodesAsText:    true,
				Serializer:       textSerializer,
				PrettySerializer: textSerializer,
			}
		}),
	}
	// TODO: remove this explicit check when https://github.com/kubernetes/enhancements/pull/5740 is implemented.
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		codecFactoryOpts = append(codecFactoryOpts, serializer.WithSerializer(cbor.NewSerializerInfo))
	}

	codecFactory := serializer.NewCodecFactory(scheme, codecFactoryOpts...)
	allTypes := codecFactory.SupportedMediaTypes()
	filtered := make([]runtime.SerializerInfo, 0, len(allTypes))

	var unknownTypes []string
	for _, info := range allTypes {
		switch info.MediaType {
		// 支持的媒体类型
		case "text/plain", runtime.ContentTypeJSON, runtime.ContentTypeYAML, runtime.ContentTypeCBOR:
			filtered = append(filtered, info)
		// 不支持的媒体类型
		case runtime.ContentTypeProtobuf:
			continue
		default:
			unknownTypes = append(unknownTypes, info.MediaType)
		}
	}

	var err error
	if len(unknownTypes) > 0 {
		err = fmt.Errorf("statusz: unknown media type(s) %v, excluding from supported types", unknownTypes)
	}

	return &statuszCodecFactory{
		CodecFactory:        codecFactory,
		supportedMediaTypes: filtered,
	}, err
}

// SupportedMediaTypes 返回过滤后的媒体类型列表，供内容协商与错误消息使用。
func (f *statuszCodecFactory) SupportedMediaTypes() []runtime.SerializerInfo {
	return f.supportedMediaTypes
}

// handleStatusz 构造 statusz 的 HTTP handler。
// 没有 Accept 头时返回 v1beta1 纯文本；否则按 statusz 专属的协商规则
// 选择结构化版本。指标中的 group/version 必须反映实际协商结果，
// 因此在这里手动调用 MonitorRequest 而不是使用通用中间件。
func handleStatusz(componentName string, reg statuszRegistry, serializer runtime.NegotiatedSerializer, restrictions negotiate.StatuszEndpointRestrictions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestReceivedTimestamp, ok := request.ReceivedTimestampFrom(r.Context())
		if !ok {
			requestReceivedTimestamp = time.Now()
		}
		// 用 delegator 捕获实际写出的状态码与长度，供监控使用。
		delegate := &metrics.ResponseWriterDelegator{ResponseWriter: w}
		w = responsewriter.WrapForHTTP1Or2(delegate)

		// 使用 MonitorRequest 而非 InstrumentHandlerFunc，因为 group、version 和弃用状态
		// 取决于每次请求的内容协商。
		// text/plain 请求的 group 和 version 为空；结构化响应（JSON/YAML/CBOR）
		// 则填入协商出的 API group 和 version
		// （如 config.k8s.io/v1alpha1）。
		var group, version string
		var deprecated bool
		defer func() {
			metrics.MonitorRequest(r, "GET", group, version,
				"statusz",     // resource
				"",            // subresource
				"",            // scope
				componentName, // component
				deprecated,
				"", // removedRelease
				delegate.Status(), delegate.ContentLength(), time.Since(requestReceivedTimestamp))
		}()

		acceptHeader := r.Header.Get("Accept")
		if strings.TrimSpace(acceptHeader) == "" {
			writePlainTextResponse(v1beta1Statusz(componentName, reg), serializer, w)
			return
		}

		mediaType, serializerInfo, err := negotiation.NegotiateOutputMediaType(r, serializer, restrictions)
		if err != nil {
			utilruntime.HandleError(err)
			responsewriters.ErrorNegotiated(
				err,
				serializer,
				schema.GroupVersion{},
				w,
				r,
			)
			return
		}

		switch serializerInfo.MediaType {
		case "application/json", "application/yaml", "application/cbor":
			if mediaType.Convert == nil {
				err := fmt.Errorf("content negotiation failed: mediaType.Convert is nil for %s", serializerInfo.MediaType)
				utilruntime.HandleError(err)
				responsewriters.ErrorNegotiated(
					err,
					serializer,
					schema.GroupVersion{},
					w,
					r,
				)
				return
			}
			// 从协商出的目标设置 group、version 和 deprecated，
			// 让延迟执行的 MonitorRequest 记录实际请求的 API 版本。
			group = mediaType.Convert.Group
			version = mediaType.Convert.Version
			deprecated = reg.deprecatedVersions()[version]
			// 弃用版本通过 Warning 头提示客户端迁移，但保持可用以维持兼容。
			if deprecated {
				w.Header().Set("Warning", `299 - "This version of the statusz endpoint is deprecated. Please use a newer version."`)
			}
			handleStructuredResponse(w, r, componentName, reg, serializer, restrictions, mediaType)
		case "text/plain":
			writePlainTextResponse(v1beta1Statusz(componentName, reg), serializer, w)
		default:
			err := fmt.Errorf("unsupported media type: %s/%s", serializerInfo.MediaType, serializerInfo.MediaTypeSubType)
			utilruntime.HandleError(err)
			responsewriters.ErrorNegotiated(
				err,
				serializer,
				schema.GroupVersion{},
				w,
				r,
			)
		}
	}
}

// handleStructuredResponse 按协商出的 kind 选择对应 API 版本的结构体。
func handleStructuredResponse(w http.ResponseWriter, r *http.Request, componentName string, reg statuszRegistry, serializer runtime.NegotiatedSerializer, restrictions negotiate.StatuszEndpointRestrictions, mediaType negotiation.MediaTypeOptions) {
	switch *mediaType.Convert {
	case v1alpha1StatuszKind:
		writeStructuredResponse(v1alpha1Statusz(componentName, reg), serializer, mediaType.Convert.GroupVersion(), restrictions, w, r)
	case v1beta1StatuszKind:
		writeStructuredResponse(v1beta1Statusz(componentName, reg), serializer, mediaType.Convert.GroupVersion(), restrictions, w, r)
	default:
		err := fmt.Errorf("unsupported media type: %s", mediaType.Convert.String())
		utilruntime.HandleError(err)
		responsewriters.ErrorNegotiated(
			err,
			serializer,
			schema.GroupVersion{},
			w,
			r,
		)
	}
}

// writePlainTextResponse 用 text/plain 序列化器输出；找不到序列化器或编码失败
// 时返回 500，因为此时内容已不可信。
func writePlainTextResponse(obj runtime.Object, serializer runtime.NegotiatedSerializer, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// 查找 text/plain 序列化器。
	var textSerializer runtime.Serializer
	for _, info := range serializer.SupportedMediaTypes() {
		if info.MediaType == "text/plain" {
			textSerializer = info.Serializer
			break
		}
	}
	if textSerializer == nil {
		utilruntime.HandleError(fmt.Errorf("text/plain serializer not available"))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if err := textSerializer.Encode(obj, w); err != nil {
		utilruntime.HandleError(fmt.Errorf("error encoding statusz as text/plain: %w", err))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// writeStructuredResponse 走标准对象协商写出路径，保证与其它 API 端点一致的
// 内容类型与编码行为。
func writeStructuredResponse(obj runtime.Object, serializer runtime.NegotiatedSerializer, targetGV schema.GroupVersion, restrictions negotiate.StatuszEndpointRestrictions, w http.ResponseWriter, r *http.Request) {
	responsewriters.WriteObjectNegotiated(
		serializer,
		restrictions,
		targetGV,
		w,
		r,
		http.StatusOK,
		obj,
		true,
	)
}

// v1alpha1Statusz 组装 v1alpha1 版本的 statusz 对象。
// 运行时长按秒向下取整并钳制为非负，避免时钟回拨导致负值。
func v1alpha1Statusz(componentName string, reg statuszRegistry) *v1alpha1.Statusz {
	startTime := reg.processStartTime()
	upTimeSeconds := max(0, int64(time.Since(startTime).Seconds()))
	goVersion := reg.goVersion()
	binaryVersion := reg.binaryVersion().String()
	var emulationVersion string
	// 未启用版本模拟时留空，而不是填二进制版本，以区分两种情况。
	if reg.emulationVersion() != nil {
		emulationVersion = reg.emulationVersion().String()
	}

	paths := aggregatePaths(reg.paths())
	data := &v1alpha1.Statusz{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1StatuszKind.Kind,
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: componentName,
		},
		StartTime:        metav1.Time{Time: startTime},
		UptimeSeconds:    upTimeSeconds,
		GoVersion:        goVersion,
		BinaryVersion:    binaryVersion,
		EmulationVersion: emulationVersion,
		Paths:            paths,
	}

	return data
}

// v1beta1Statusz 组装 v1beta1 版本的 statusz 对象，内容与 v1alpha1 相同，
// 仅在 API 版本与字段契约上不同。
func v1beta1Statusz(componentName string, reg statuszRegistry) *v1beta1.Statusz {
	startTime := reg.processStartTime()
	upTimeSeconds := max(0, int64(time.Since(startTime).Seconds()))
	goVersion := reg.goVersion()
	binaryVersion := reg.binaryVersion().String()
	var emulationVersion string
	if reg.emulationVersion() != nil {
		emulationVersion = reg.emulationVersion().String()
	}

	paths := aggregatePaths(reg.paths())
	data := &v1beta1.Statusz{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1beta1StatuszKind.Kind,
			APIVersion: v1beta1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: componentName,
		},
		StartTime:        metav1.Time{Time: startTime},
		UptimeSeconds:    upTimeSeconds,
		GoVersion:        goVersion,
		BinaryVersion:    binaryVersion,
		EmulationVersion: emulationVersion,
		Paths:            paths,
	}

	return data
}

// uptime 把启动时间格式化为 "N hr MM min SS sec" 的人类可读形式。
func uptime(t time.Time) string {
	upSince := int64(time.Since(t).Seconds())
	return fmt.Sprintf("%d hr %02d min %02d sec",
		upSince/3600, (upSince/60)%60, upSince%60)
}

// aggregatePaths 把具体路径折叠为顶层目录并去重排序，
// 例如 /metrics/foo 折叠为 /metrics；跳过非调试端点与根路径。
func aggregatePaths(listedPaths []string) []string {
	paths := make(map[string]bool)
	for _, listedPath := range listedPaths {
		parts := strings.Split(listedPath, "/")
		if len(parts) < 2 || parts[1] == "" {
			continue
		}
		folder := "/" + parts[1]
		if !paths[folder] && !nonDebuggingEndpoints[folder] {
			paths[folder] = true
		}
	}

	var sortedPaths []string
	for p := range paths {
		sortedPaths = append(sortedPaths, p)
	}
	sort.Strings(sortedPaths)

	return sortedPaths
}
