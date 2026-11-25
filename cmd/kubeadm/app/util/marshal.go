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

package util

import (
	"bufio"
	"bytes"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	yamlserializer "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// MarshalToYaml 将对象序列化为 YAML。
func MarshalToYaml(obj runtime.Object, gv schema.GroupVersion) ([]byte, error) {
	return MarshalToYamlForCodecs(obj, gv, clientsetscheme.Codecs)
}

// MarshalToYamlForCodecs 使用指定 codec 将对象序列化为 YAML。
// TODO: 此处真的需要指定 GV 吗？
// TODO: 能否方便地直接支持 JSON？
func MarshalToYamlForCodecs(obj runtime.Object, gv schema.GroupVersion, codecs serializer.CodecFactory) ([]byte, error) {
	const mediaType = runtime.ContentTypeYAML
	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return []byte{}, errors.Errorf("unsupported media type %q", mediaType)
	}

	encoder := codecs.EncoderForVersion(info.Serializer, gv)
	return runtime.Encode(encoder, obj)
}

// UniversalUnmarshal 使用通用反序列化器将 YAML 或 JSON 反序列化到 runtime.Object。
func UniversalUnmarshal(buffer []byte) (runtime.Object, error) {
	codecs := clientsetscheme.Codecs
	decoder := codecs.UniversalDeserializer()
	obj, _, err := decoder.Decode(buffer, nil, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode %s into runtime.Object", buffer)
	}
	return obj, nil
}

// SplitConfigDocuments 逐文档读取 YAML/JSON 字节，解析各文档的 TypeMeta，
// 并返回文档 GVK 到文档字节的映射。
func SplitConfigDocuments(documentBytes []byte) (kubeadmapi.DocumentMap, error) {
	gvkmap := kubeadmapi.DocumentMap{}
	knownKinds := map[string]bool{}
	errs := []error{}
	buf := bytes.NewBuffer(documentBytes)
	reader := utilyaml.NewYAMLReader(bufio.NewReader(buf))
	for {
		// 每次读取一个 YAML 文档，直到返回 io.EOF。
		b, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if len(b) == 0 {
			break
		}
		// 反序列化此字节切片的 TypeMeta。
		gvk, err := yamlserializer.DefaultMetaFactory.Interpret(b)
		if err != nil {
			return nil, err
		}
		if len(gvk.Group) == 0 || len(gvk.Version) == 0 || len(gvk.Kind) == 0 {
			return nil, errors.Errorf("invalid configuration for GroupVersionKind %+v: kind and apiVersion is mandatory information that must be specified", gvk)
		}

		// 检查 kind 是否已注册；已注册则返回错误。
		if known := knownKinds[gvk.Kind]; known {
			errs = append(errs, errors.Errorf("invalid configuration: kind %q is specified twice in YAML file", gvk.Kind))
			continue
		}
		knownKinds[gvk.Kind] = true

		// 保存 GVK 到对象字节的映射。
		gvkmap[*gvk] = b
	}
	if err := errorsutil.NewAggregate(errs); err != nil {
		return nil, err
	}
	return gvkmap, nil
}

// GroupVersionKindsFromBytes 解析字节并返回 GVK 切片。
func GroupVersionKindsFromBytes(b []byte) ([]schema.GroupVersionKind, error) {
	gvkmap, err := SplitConfigDocuments(b)
	if err != nil {
		return nil, err
	}
	gvks := []schema.GroupVersionKind{}
	for gvk := range gvkmap {
		gvks = append(gvks, gvk)
	}
	return gvks, nil
}

// GroupVersionKindsHasKind 判断给定 GVK 切片是否包含指定 kind。
func GroupVersionKindsHasKind(gvks []schema.GroupVersionKind, kind string) bool {
	for _, gvk := range gvks {
		if gvk.Kind == kind {
			return true
		}
	}
	return false
}

// GroupVersionKindsHasClusterConfiguration 判断给定 GVK 切片是否包含 ClusterConfiguration。
func GroupVersionKindsHasClusterConfiguration(gvks ...schema.GroupVersionKind) bool {
	return GroupVersionKindsHasKind(gvks, constants.ClusterConfigurationKind)
}

// GroupVersionKindsHasInitConfiguration 判断给定 GVK 切片是否包含 InitConfiguration。
func GroupVersionKindsHasInitConfiguration(gvks ...schema.GroupVersionKind) bool {
	return GroupVersionKindsHasKind(gvks, constants.InitConfigurationKind)
}

// GroupVersionKindsHasJoinConfiguration 判断给定 GVK 切片是否包含 JoinConfiguration。
func GroupVersionKindsHasJoinConfiguration(gvks ...schema.GroupVersionKind) bool {
	return GroupVersionKindsHasKind(gvks, constants.JoinConfigurationKind)
}

// GroupVersionKindsHasResetConfiguration 判断给定 GVK 切片是否包含 ResetConfiguration。
func GroupVersionKindsHasResetConfiguration(gvks ...schema.GroupVersionKind) bool {
	return GroupVersionKindsHasKind(gvks, constants.ResetConfigurationKind)
}

// GroupVersionKindsHasUpgradeConfiguration 判断给定 GVK 切片是否包含 UpgradeConfiguration。
func GroupVersionKindsHasUpgradeConfiguration(gvks ...schema.GroupVersionKind) bool {
	return GroupVersionKindsHasKind(gvks, constants.UpgradeConfigurationKind)
}
