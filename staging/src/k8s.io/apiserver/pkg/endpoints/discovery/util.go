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

package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// APIGroupPrefix 是非核心（带组）资源在 URL 中的公共前缀，如 /apis/apps/v1。
const APIGroupPrefix = "/apis"

// keepUnversioned 判断某组是否应在发现文档中保持“无版本”形式。
// 空组代表核心组，extensions 是历史遗留组，二者都必须维持旧客户端期望的格式。
func keepUnversioned(group string) bool {
	return group == "" || group == "extensions"
}

// stripVersionEncoder 会从编码输出中去掉 APIVersion 字段，用于让发现端点的响应
// 与 release-1.1（当时响应的 APIVersion 为空）保持向后兼容。
//
// 实现方式是“编码→解码→清空 GVK→再编码”，代价较高，因此仅用于兼容路径。
type stripVersionEncoder struct {
	encoder    runtime.Encoder
	serializer runtime.Serializer
	identifier runtime.Identifier
}

// newStripVersionEncoder 构造去版本编码器，并预先算好 Identifier。
func newStripVersionEncoder(e runtime.Encoder, s runtime.Serializer) runtime.Encoder {
	return stripVersionEncoder{
		encoder:    e,
		serializer: s,
		identifier: identifier(e),
	}
}

// identifier 生成编码器的稳定标识，供缓存编码路径区分不同编码器。
// 名称固定为 stripVersion，内部编码器可能为 nil（测试场景），需防 nil。
func identifier(e runtime.Encoder) runtime.Identifier {
	result := map[string]string{
		"name": "stripVersion",
	}
	if e != nil {
		result["encoder"] = string(e.Identifier())
	}
	identifier, err := json.Marshal(result)
	if err != nil {
		klog.Fatalf("Failed marshaling identifier for stripVersionEncoder: %v", err)
	}
	return runtime.Identifier(identifier)
}

// Encode 对支持缓存的对象走 CacheEncode，避免每次请求重复做两次编解码。
func (c stripVersionEncoder) Encode(obj runtime.Object, w io.Writer) error {
	if co, ok := obj.(runtime.CacheableObject); ok {
		return co.CacheEncode(c.Identifier(), c.doEncode, w)
	}
	return c.doEncode(obj, w)
}

// doEncode 执行实际的去版本编码：先编码到缓冲区，再解码回来清空 Group/Version，
// 最后重新编码写出。中间对象会被修改，因此不能与缓存共享同一实例。
func (c stripVersionEncoder) doEncode(obj runtime.Object, w io.Writer) error {
	buf := bytes.NewBuffer([]byte{})
	err := c.encoder.Encode(obj, buf)
	if err != nil {
		return err
	}
	roundTrippedObj, gvk, err := c.serializer.Decode(buf.Bytes(), nil, nil)
	if err != nil {
		return err
	}
	gvk.Group = ""
	gvk.Version = ""
	roundTrippedObj.GetObjectKind().SetGroupVersionKind(*gvk)
	return c.serializer.Encode(roundTrippedObj, w)
}

// Identifier 实现 runtime.Encoder 接口。
func (c stripVersionEncoder) Identifier() runtime.Identifier {
	return c.identifier
}

// stripVersionNegotiatedSerializer 在 EncoderForVersion 被调用时返回
// stripVersionEncoder。参见 stripVersionEncoder 的注释。
type stripVersionNegotiatedSerializer struct {
	runtime.NegotiatedSerializer
}

// EncoderForVersion 把版本化编码器再包一层去版本逻辑。
// 需要同时拿到 encoder 与 decoder，但调用上下文只给 encoder，因此做尽力而为的类型断言；
// 失败即 panic：这只影响已废弃的兼容路径，静默降级会输出错误格式反而更难排查。
func (n stripVersionNegotiatedSerializer) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	serializer, ok := encoder.(runtime.Serializer)
	if !ok {
		// stripVersionEncoder 同时需要 encoder 和 decoder，但调用它的上下文拿不到
		// decoder。由于该代码路径仅用于向后兼容，这里做尽力而为的类型断言，
		// 以获取调用方的 decoder。
		panic(fmt.Sprintf("Unable to extract serializer from %#v", encoder))
	}
	versioned := n.NegotiatedSerializer.EncoderForVersion(encoder, gv)
	return newStripVersionEncoder(versioned, serializer)
}
