/*
Copyright 2018 The Kubernetes Authors.

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

package resource

import (
	"encoding/json"
	"io"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// dynamicCodec 包装标准 unstructured 编解码器，专门处理 Status 对象。
// 已废弃：仅测试代码使用，且实现有误。
//
// 已弃用：仅被测试代码使用，且现有实现并不完全正确；新代码应使用标准
// unstructured 或带 scheme 的 codec。
type dynamicCodec struct{}

// Decode 处理 v1 Status 的特殊情况：把它解码成强类型的 metav1.Status，
// 以便调用方能读取 Code/Reason 等字段，而不是一个裸 map。
func (dynamicCodec) Decode(data []byte, gvk *schema.GroupVersionKind, obj runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	obj, gvk, err := unstructured.UnstructuredJSONScheme.Decode(data, gvk, obj)
	if err != nil {
		return nil, nil, err
	}

	// status 的 group 可能为空（core）或 meta.k8s.io，两种都要识别。
	if strings.EqualFold(gvk.Kind, "status") && gvk.Version == "v1" && (gvk.Group == "" || gvk.Group == "meta.k8s.io") {
		if _, ok := obj.(*metav1.Status); !ok {
			obj = &metav1.Status{}
			err := json.Unmarshal(data, obj)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return obj, gvk, nil
}

// Encode 始终使用 unstructured JSON 编码，与 Decode 的非 Status 路径保持一致。
func (dynamicCodec) Encode(obj runtime.Object, w io.Writer) error {
	// 此处只是回退到其他编码器，无需处理 runtime.CacheableObject。
	return unstructured.UnstructuredJSONScheme.Encode(obj, w)
}

// Identifier 实现 runtime.Encoder 接口。
// 委托给底层 scheme，确保序列化缓存按相同标识区分编码器。
func (dynamicCodec) Identifier() runtime.Identifier {
	return unstructured.UnstructuredJSONScheme.Identifier()
}

// 为动态类型返回 rest.ContentConfig，其中包含足够编解码器，
// 可像普通序列化器一样处理选项、Status 等对象。
//
// 为动态客户端提供一套“够用”的 JSON codec：既能解析普通对象，也能把
// Status 解析成强类型，方便 REST 层统一处理错误。
func UnstructuredPlusDefaultContentConfig() rest.ContentConfig {
	// TODO: 这里的 scheme.Codecs 应换成 "pkg/apis/server/scheme"，那是与 Kubernetes 服务端通信所需的最小集合。
	jsonInfo, _ := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)

	// 用 dynamicCodec 替换默认 serializer，并禁用 pretty 版本以保持输出一致。
	jsonInfo.Serializer = dynamicCodec{}
	jsonInfo.PrettySerializer = nil
	return rest.ContentConfig{
		AcceptContentTypes:   runtime.ContentTypeJSON,
		ContentType:          runtime.ContentTypeJSON,
		NegotiatedSerializer: serializer.NegotiatedSerializerWrapper(jsonInfo),
	}
}
