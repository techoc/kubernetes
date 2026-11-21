/*
Copyright 2017 The Kubernetes Authors.

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

package codec

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/klog/v2"

	// 确保核心 API 已注册。
	_ "k8s.io/kubernetes/pkg/apis/core/install"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/component-base/codec"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
	kubeletconfigv1beta1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1beta1"
)

// EncodeKubeletConfig 把内部 KubeletConfiguration 编码为外部版本的 YAML。
func EncodeKubeletConfig(internal *kubeletconfig.KubeletConfiguration, targetVersion schema.GroupVersion) ([]byte, error) {
	encoder, err := NewKubeletconfigYAMLEncoder(targetVersion)
	if err != nil {
		return nil, err
	}
	// 编码器会把内部类型转换为外部版本。
	data, err := runtime.Encode(encoder, internal)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// NewKubeletconfigYAMLEncoder 返回把 kubeletconfig API 对象编码为 YAML 的编码器。
func NewKubeletconfigYAMLEncoder(targetVersion schema.GroupVersion) (runtime.Encoder, error) {
	_, codecs, err := scheme.NewSchemeAndCodecs()
	if err != nil {
		return nil, err
	}
	// 仅支持 YAML 序列化。
	mediaType := "application/yaml"
	// 从 scheme 支持的序列化器中选出 YAML。
	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return nil, fmt.Errorf("unsupported media type %q", mediaType)
	}
	return codecs.EncoderForVersion(info.Serializer, targetVersion), nil
}

// DecodeKubeletConfiguration 把序列化的 KubeletConfiguration 解码为内部类型。
func DecodeKubeletConfiguration(ctx context.Context, kubeletCodecs *serializer.CodecFactory, data []byte) (*kubeletconfig.KubeletConfiguration, error) {
	var (
		obj runtime.Object
		gvk *schema.GroupVersionKind
	)

	// UniversalDecoder 会执行默认值填充，并默认返回内部类型。
	obj, gvk, err := kubeletCodecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		logger := klog.FromContext(ctx)
		// 先尝试严格解码；失败时改用仅注册了 v1beta1 的宽松解码器，
		// 并记录警告。v1beta1 支持移除后，该宽松路径也应删除。
		if !runtime.IsStrictDecodingError(err) {
			return nil, fmt.Errorf("failed to decode: %w", err)
		}

		var lenientErr error
		_, lenientCodecs, lenientErr := codec.NewLenientSchemeAndCodecs(
			kubeletconfig.AddToScheme,
			kubeletconfigv1beta1.AddToScheme,
		)

		if lenientErr != nil {
			return nil, lenientErr
		}

		obj, gvk, lenientErr = lenientCodecs.UniversalDecoder().Decode(data, nil, nil)
		if lenientErr != nil {
			// 宽松解码也失败，返回最初的严格解码错误。
			return nil, fmt.Errorf("failed lenient decoding: %v", err)
		}
		// 继续使用宽松解码得到的 v1beta1 对象，但输出警告。
		logger.Info("Using lenient decoding as strict decoding failed", "err", err)
	}

	// 解码结果必须是内部 KubeletConfiguration 类型。
	internalKC, ok := obj.(*kubeletconfig.KubeletConfiguration)
	if !ok {
		return nil, fmt.Errorf("failed to cast object to KubeletConfiguration, unexpected type: %v", gvk)
	}

	return internalKC, nil
}

// DecodeKubeletConfigurationIntoJSON 把序列化的 KubeletConfiguration 解码并转换为 JSON。
func DecodeKubeletConfigurationIntoJSON(kubeletCodecs *serializer.CodecFactory, data []byte) ([]byte, *schema.GroupVersionKind, error) {
	// UniversalDecoder 会执行默认值填充，并默认返回内部类型。
	obj, gvk, err := kubeletCodecs.UniversalDecoder().Decode(data, nil, &unstructured.Unstructured{})
	if err != nil {
		return nil, nil, err
	}

	objT := obj.(*unstructured.Unstructured)

	// 把非结构化对象序列化为 JSON 返回。
	jsonData, err := json.Marshal(objT.Object)
	return jsonData, gvk, err
}
