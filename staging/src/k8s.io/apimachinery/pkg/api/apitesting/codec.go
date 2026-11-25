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

package apitesting

import (
	"fmt"
	"mime"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/recognizer"
)

var (
	// 经 init 从环境变量解析出的 media type；为空表示未指定、沿用 LegacyCodec。
	testCodecMediaType        string
	testStorageCodecMediaType string
)

// TestCodec 返回用于测试目标 API 版本的 codec，格式由 KUBE_TEST_API_TYPE 环境变量指定。
func TestCodec(codecs runtimeserializer.CodecFactory, gvs ...schema.GroupVersion) runtime.Codec {
	if len(testCodecMediaType) != 0 {
		serializerInfo, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), testCodecMediaType)
		if !ok {
			// 环境变量要求的格式必须受支持，否则测试配置有误，直接 panic 更快暴露问题。
			panic(fmt.Sprintf("no serializer for %s", testCodecMediaType))
		}
		return codecs.CodecForVersions(serializerInfo.Serializer, codecs.UniversalDeserializer(), schema.GroupVersions(gvs), nil)
	}
	return codecs.LegacyCodec(gvs...)
}

// TestStorageCodec 返回用于测试存储层序列化的 codec，格式由 KUBE_TEST_API_STORAGE_TYPE 环境变量指定。
func TestStorageCodec(codecs runtimeserializer.CodecFactory, gvs ...schema.GroupVersion) runtime.Codec {
	if len(testStorageCodecMediaType) != 0 {
		serializerInfo, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), testStorageCodecMediaType)
		if !ok {
			// 同 TestCodec：未知格式属于测试配置错误。
			panic(fmt.Sprintf("no serializer for %s", testStorageCodecMediaType))
		}

		// etcd2 只支持字符串数据，因此返回前需对非文本编码做一层 base64 包装。
		// TODO: 淘汰 etcd3 兼容逻辑，或改为可配置。
		serializer := serializerInfo.Serializer
		if !serializerInfo.EncodesAsText {
			serializer = runtime.NewBase64Serializer(serializer, serializer)
		}

		// 解码时用 recognizer 同时接受包装后的存储格式与通用格式，便于读取历史数据。
		decoder := recognizer.NewDecoder(serializer, codecs.UniversalDeserializer())
		return codecs.CodecForVersions(serializer, decoder, schema.GroupVersions(gvs), nil)

	}
	return codecs.LegacyCodec(gvs...)
}

// init 在启动时解析测试格式环境变量。格式非法时 panic，因为这是测试环境配置错误。
func init() {
	var err error
	if apiMediaType := os.Getenv("KUBE_TEST_API_TYPE"); len(apiMediaType) > 0 {
		testCodecMediaType, _, err = mime.ParseMediaType(apiMediaType)
		if err != nil {
			panic(err)
		}
	}

	if storageMediaType := os.Getenv("KUBE_TEST_API_STORAGE_TYPE"); len(storageMediaType) > 0 {
		testStorageCodecMediaType, _, err = mime.ParseMediaType(storageMediaType)
		if err != nil {
			panic(err)
		}
	}
}

// InstallOrDieFunc 对应不返回错误的 install 函数，调用方假定其必定成功。
type InstallOrDieFunc func(scheme *runtime.Scheme)

// SchemeForInstallOrDie 构造测试用的 Scheme 与 CodecFactory，
// 便于直接复用上层不返回错误的 install 方法。
func SchemeForInstallOrDie(installFns ...InstallOrDieFunc) (*runtime.Scheme, runtimeserializer.CodecFactory) {
	scheme := runtime.NewScheme()
	codecFactory := runtimeserializer.NewCodecFactory(scheme)
	for _, installFn := range installFns {
		installFn(scheme)
	}

	return scheme, codecFactory
}

// InstallFunc 对应可能返回错误的 install 函数。
type InstallFunc func(scheme *runtime.Scheme) error

// SchemeForOrDie 构造测试用的 Scheme 与 CodecFactory；
// 任一注册函数返回错误时 panic，适合在测试初始化阶段使用。
func SchemeForOrDie(installFns ...InstallFunc) (*runtime.Scheme, runtimeserializer.CodecFactory) {
	scheme := runtime.NewScheme()
	codecFactory := runtimeserializer.NewCodecFactory(scheme)
	for _, installFn := range installFns {
		if err := installFn(scheme); err != nil {
			panic(err)
		}
	}

	return scheme, codecFactory
}
