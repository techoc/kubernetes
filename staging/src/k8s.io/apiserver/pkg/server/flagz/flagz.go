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

package flagz

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/flagz/api/v1alpha1"
	"k8s.io/apiserver/pkg/server/flagz/api/v1beta1"
	"k8s.io/apiserver/pkg/server/flagz/negotiate"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

var (
	// 预先计算两个版本的 Kind，避免在每次请求的协商路径上重复拼接。
	v1alpha1FlagzKind         = v1alpha1.SchemeGroupVersion.WithKind("Flagz")
	v1beta1FlagzKind          = v1beta1.SchemeGroupVersion.WithKind("Flagz")
	recognizedStructuredKinds = map[schema.GroupVersionKind]bool{
		v1alpha1FlagzKind: true,
		v1beta1FlagzKind:  true,
	}
)

// DefaultFlagzPath 是 flagz 端点的固定挂载路径。
const DefaultFlagzPath = "/flagz"

// flagzCodecFactory wraps a CodecFactory to filter out unsupported media types (like protobuf)
// from the supported media types list, so error messages only show actually supported types.
type flagzCodecFactory struct {
	serializer.CodecFactory
	supportedMediaTypes []runtime.SerializerInfo
}

// mux 抽象出挂载端点所需的最小接口，便于在测试中替换。
type mux interface {
	Handle(path string, handler http.Handler)
}

// Install installs the flagz endpoint to the given mux.
// 内部总是注册 v1alpha1 与 v1beta1 两个版本，v1alpha1 标记为已弃用，
// 结构化响应会按内容协商选择版本并附带 Warning 头。
func Install(m mux, componentName string, flagReader Reader, opts ...Option) {
	reg := &registry{
		reader:                flagReader,
		deprecatedVersionsMap: map[string]bool{"v1alpha1": true},
	}
	for _, opt := range opts {
		opt(reg)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	filteredCodecFactory, err := newFlagzCodecFactory(scheme, componentName, reg.reader)
	if err != nil {
		utilruntime.HandleError(err)
	}
	restrictions := negotiate.FlagzEndpointRestrictions{
		RecognizedStructuredKinds: recognizedStructuredKinds,
	}
	m.Handle(DefaultFlagzPath, handleFlagz(componentName, reg, filteredCodecFactory, restrictions))
}

// newFlagzCodecFactory creates a codec factory with the standard serializers for flagz,
// filtering out unsupported media types (e.g., protobuf).
// 过滤是必要的：flagz 是文本型调试端点，protobuf 既无意义又会让协商错误信息
// 列出实际不可用的类型。遇到未知媒体类型时返回错误但仍返回可用的工厂，
// 由调用方决定是否仅记录日志。
func newFlagzCodecFactory(scheme *runtime.Scheme, componentName string, flagReader Reader) (*flagzCodecFactory, error) {
	codecFactoryOpts := []serializer.CodecFactoryOptionsMutator{
		serializer.WithSerializer(func(_ runtime.ObjectCreater, _ runtime.ObjectTyper) runtime.SerializerInfo {
			textSerializer := flagzTextSerializer{componentName, flagReader}
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
		// Supported media types
		case "text/plain", runtime.ContentTypeJSON, runtime.ContentTypeYAML, runtime.ContentTypeCBOR:
			filtered = append(filtered, info)
		// Unsupported media types
		case runtime.ContentTypeProtobuf:
			continue
		default:
			unknownTypes = append(unknownTypes, info.MediaType)
		}
	}

	var err error
	if len(unknownTypes) > 0 {
		err = fmt.Errorf("flagz: unknown media type(s) %v, excluding from supported types", unknownTypes)
	}

	return &flagzCodecFactory{
		CodecFactory:        codecFactory,
		supportedMediaTypes: filtered,
	}, err
}

// SupportedMediaTypes 返回过滤后的媒体类型列表，覆盖内嵌 CodecFactory 的同名方法，
// 使内容协商只看到真正可用的类型。
func (f *flagzCodecFactory) SupportedMediaTypes() []runtime.SerializerInfo {
	return f.supportedMediaTypes
}

// handleFlagz 返回端点的 HTTP 处理器：无 Accept 头时默认输出 text/plain，
// 否则按内容协商返回 JSON/YAML/CBOR 结构化响应。
// 指标上报放在 defer 中，确保错误路径也能记录 group/version/deprecated。
func handleFlagz(componentName string, reg *registry, serializer runtime.NegotiatedSerializer, restrictions negotiate.FlagzEndpointRestrictions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestReceivedTimestamp, ok := request.ReceivedTimestampFrom(r.Context())
		if !ok {
			requestReceivedTimestamp = time.Now()
		}
		delegate := &metrics.ResponseWriterDelegator{ResponseWriter: w}
		w = responsewriter.WrapForHTTP1Or2(delegate)

		// Use MonitorRequest instead of InstrumentHandlerFunc because the group,
		// version, and deprecated status depend on per-request content negotiation.
		// For text/plain requests, group and version remain empty. For structured
		// responses (JSON/YAML/CBOR), they are set to the negotiated API group and
		// version (e.g., config.k8s.io/v1alpha1).
		var group, version string
		var deprecated bool
		defer func() {
			metrics.MonitorRequest(r, "GET", group, version,
				"flagz",       // resource
				"",            // subresource
				"",            // scope
				componentName, // component
				deprecated,
				"", // removedRelease
				delegate.Status(), delegate.ContentLength(), time.Since(requestReceivedTimestamp))
		}()

		acceptHeader := r.Header.Get("Accept")
		// 没有 Accept 头时按纯文本处理，方便 curl/kubectl 直接读取。
		if strings.TrimSpace(acceptHeader) == "" {
			writePlainTextResponse(v1beta1Flagz(componentName, reg.reader), serializer, w, reg)
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
			group = mediaType.Convert.Group
			version = mediaType.Convert.Version
			deprecated = reg.deprecatedVersions()[version]
			// 弃用版本通过 Warning 头提示调用方升级，而不是直接拒绝请求，
			// 以免破坏仍在使用旧版本的客户端。
			if deprecated {
				w.Header().Set("Warning", `299 - "This version of the flagz endpoint is deprecated. Please use a newer version."`)
			}
			handleStructuredResponse(w, r, componentName, reg, serializer, restrictions, mediaType)
		case "text/plain":
			writePlainTextResponse(v1beta1Flagz(componentName, reg.reader), serializer, w, reg)
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

// writePlainTextResponse 输出 text/plain；成功编码后的字节被缓存，
// 因为标志在进程生命周期内基本不变，这样可以省去每个请求的重复编码。
// 缓存由 registry 的锁保护，写入失败只记录错误，因为响应头已发出。
func writePlainTextResponse(obj runtime.Object, serializer runtime.NegotiatedSerializer, w http.ResponseWriter, reg *registry) {
	reg.cachedPlainTextResponseLock.Lock()
	defer reg.cachedPlainTextResponseLock.Unlock()
	if reg.cachedPlainTextResponse != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err := w.Write(reg.cachedPlainTextResponse); err != nil {
			utilruntime.HandleError(fmt.Errorf("error writing cached flagz as text/plain: %w", err))
		}
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
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

	var buf strings.Builder
	if err := textSerializer.Encode(obj, &buf); err != nil {
		utilruntime.HandleError(fmt.Errorf("error encoding flagz as text/plain: %w", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	reg.cachedPlainTextResponse = []byte(buf.String())
	if _, err := w.Write(reg.cachedPlainTextResponse); err != nil {
		utilruntime.HandleError(fmt.Errorf("error writing flagz as text/plain: %w", err))
	}
}

// writeStructuredResponse 按协商结果写出结构化对象；
// 最后一个参数 true 表示该响应可以被缓存（GET 且无副作用）。
func writeStructuredResponse(obj runtime.Object, serializer runtime.NegotiatedSerializer, targetGV schema.GroupVersion, restrictions negotiate.FlagzEndpointRestrictions, w http.ResponseWriter, r *http.Request) {
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

// handleStructuredResponse 根据协商出的 GVK 选择对应版本的 Flagz 对象写出；
// 两个版本的数据内容相同，只是 API 形状不同。
func handleStructuredResponse(w http.ResponseWriter, r *http.Request, componentName string, reg *registry, serializer runtime.NegotiatedSerializer, restrictions negotiate.FlagzEndpointRestrictions, mediaType negotiation.MediaTypeOptions) {
	switch *mediaType.Convert {
	case v1alpha1FlagzKind:
		writeStructuredResponse(v1alpha1Flagz(componentName, reg.reader), serializer, v1alpha1FlagzKind.GroupVersion(), restrictions, w, r)
	case v1beta1FlagzKind:
		writeStructuredResponse(v1beta1Flagz(componentName, reg.reader), serializer, v1beta1FlagzKind.GroupVersion(), restrictions, w, r)
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

// v1alpha1Flagz 构造 v1alpha1 版本的 Flagz 对象；对象名使用组件名，
// 与 /flagz 的单一资源语义一致。
func v1alpha1Flagz(componentName string, flagReader Reader) *v1alpha1.Flagz {
	flags := flagReader.GetFlagz()
	return &v1alpha1.Flagz{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1alpha1FlagzKind.Kind,
			APIVersion: v1alpha1FlagzKind.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: componentName,
		},
		Flags: flags,
	}
}

// v1beta1Flagz 构造 v1beta1 版本的 Flagz 对象，是默认的结构化输出版本。
func v1beta1Flagz(componentName string, flagReader Reader) *v1beta1.Flagz {
	flags := flagReader.GetFlagz()
	return &v1beta1.Flagz{
		TypeMeta: metav1.TypeMeta{
			Kind:       v1beta1FlagzKind.Kind,
			APIVersion: v1beta1FlagzKind.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: componentName,
		},
		Flags: flags,
	}
}
