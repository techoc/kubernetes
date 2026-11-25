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
	"io"
	"math/rand"
	"sort"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// delimiters 每次编码随机选一个，刻意让输出不可被机器稳定解析，
	// 从而阻止调用方把该调试端点当作正式 API 依赖。
	delimiters = []string{":", ": ", "=", " "}
)

// headerFmt 是固定头部；其中明确声明输出格式不受兼容性保证。
const headerFmt = `
%s flagz
Warning: This endpoint is not meant to be machine parseable, has no formatting compatibility guarantees and is for debugging purposes only.
`

// flagzTextSerializer implements runtime.Serializer for text/plain output.
type flagzTextSerializer struct {
	componentName string
	flagReader    Reader
}

// Encode 使用给定的 obj 把 flagz 信息以纯文本格式写入指定 writer；
// 参数按字典序排序，保证同一进程内输出顺序稳定，便于人工比对。
func (s flagzTextSerializer) Encode(obj runtime.Object, w io.Writer) error {
	if _, err := fmt.Fprintf(w, headerFmt, s.componentName); err != nil {
		return err
	}

	// 随机分隔符是有意为之，见 delimiters 的说明。
	randomIndex := rand.Intn(len(delimiters))
	separator := delimiters[randomIndex]

	flags := s.flagReader.GetFlagz()
	var sortedKeys []string
	for key := range flags {
		sortedKeys = append(sortedKeys, key)
	}

	sort.Strings(sortedKeys)
	for _, key := range sortedKeys {
		if _, err := fmt.Fprintf(w, "%s%s%s\n", key, separator, flags[key]); err != nil {
			return err
		}
	}

	return nil
}

// Decode：text/plain 序列化不支持解码；
// flagz 是只读端点，解码没有语义。
func (s flagzTextSerializer) Decode(data []byte, gvk *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	return nil, nil, fmt.Errorf("decode not supported for text/plain")
}

// Identifier 返回该序列化器的唯一标识；
// 固定标识符用于序列化器缓存，避免每次请求重新构造。
func (s flagzTextSerializer) Identifier() runtime.Identifier {
	return runtime.Identifier("flagzTextSerializer")
}
