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

package serializer

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// TODO: 应当把能改变版本的协商序列化器与能改变
// 序列化格式的协商序列化器拆分开
// negotiatedSerializerWrapper 将单个固定的 SerializerInfo 作为
// NegotiatedSerializer 暴露出来。版本选择是空操作，因为被封装的
// 编码器/解码器已固定指向某个版本。
type negotiatedSerializerWrapper struct {
	info runtime.SerializerInfo
}

// NegotiatedSerializerWrapper 返回一个 NegotiatedSerializer，它始终
// 呈现给定的 SerializerInfo，并忽略版本请求。
func NegotiatedSerializerWrapper(info runtime.SerializerInfo) runtime.NegotiatedSerializer {
	return &negotiatedSerializerWrapper{info}
}

func (n *negotiatedSerializerWrapper) SupportedMediaTypes() []runtime.SerializerInfo {
	return []runtime.SerializerInfo{n.info}
}

// EncoderForVersion 原样返回 e：该封装器无法将任意编码器
// 重新解释为另一个版本的编码器。
func (n *negotiatedSerializerWrapper) EncoderForVersion(e runtime.Encoder, _ runtime.GroupVersioner) runtime.Encoder {
	return e
}

// DecoderToVersion 出于与 EncoderForVersion 相同的原因原样返回 d。
func (n *negotiatedSerializerWrapper) DecoderToVersion(d runtime.Decoder, _gv runtime.GroupVersioner) runtime.Decoder {
	return d
}
