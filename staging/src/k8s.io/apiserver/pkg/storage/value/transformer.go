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

// Package value 包含辅助转换存储中值的工具方法。
//
// 值转换用于静态加密（encryption at rest）等场景：写入时把明文转换为带前缀的
// 密文，读取时根据前缀选择对应的转换器解密。前缀是区分不同 provider/key 的
// 唯一标识，因此轮换密钥时新旧数据可以共存，读取路径按前缀逐条尝试。
package value

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/errors"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
)

func init() {
	RegisterMetrics()
}

// Context 是存储转换校验静态数据所需的附加信息。
type Context interface {
	// AuthenticatedData 应返回描述当前值的字节数组。值变化时，
	// transformer 可报告该值不可读或被篡改。若不存在或不需要此类描述，可返回 nil。
	//
	AuthenticatedData() []byte
}

// Read 把存储中的字节转换回明文。
type Read interface {
	// TransformFromStorage 把存储表示的数据转换回来，或返回错误。
	// 即使对象内容没变，只要磁盘上的对象已陈旧、需要写回 etcd，Stale 就为 true。
	//
	TransformFromStorage(ctx context.Context, data []byte, dataCtx Context) (out []byte, stale bool, err error)
}

// Write 把明文转换为存储表示。
type Write interface {
	// TransformToStorage 把数据转换为适合存储的形式，或返回错误。
	TransformToStorage(ctx context.Context, data []byte, dataCtx Context) (out []byte, err error)
}

// Transformer 在值读写底层存储前后做转换，两个方法必须能撤销对方的转换。
type Transformer interface {
	Read
	Write
}

// ResourceTransformers 返回给定资源对应的 transformer。
type ResourceTransformers interface {
	TransformerForResource(resource schema.GroupResource) Transformer
}

// DefaultContext 是 Context 的简单字节切片实现。
type DefaultContext []byte

// AuthenticatedData 返回自身。
func (c DefaultContext) AuthenticatedData() []byte { return c }

// PrefixTransformer 持有 transformer 及其转换所对应的前缀。
type PrefixTransformer struct {
	Prefix      []byte
	Transformer Transformer
}

type prefixTransformers struct {
	transformers []PrefixTransformer
	err          error
}

var _ Transformer = &prefixTransformers{}

// NewPrefixTransformers 按顺序用给定前缀检查输入数据，实现 Transformer 接口。
func NewPrefixTransformers(err error, transformers ...PrefixTransformer) Transformer {
	if err == nil {
		err = fmt.Errorf("the provided value does not match any of the supported transformers")
	}
	return &prefixTransformers{
		transformers: transformers,
		err:          err,
	}
}

// TransformFromStorage 找到第一个前缀匹配的 transformer 并返回转换结果。
func (t *prefixTransformers) TransformFromStorage(ctx context.Context, data []byte, dataCtx Context) ([]byte, bool, error) {
	start := time.Now()
	var errs []error
	resource := getResourceFromContext(ctx)
	for i, transformer := range t.transformers {
		if bytes.HasPrefix(data, transformer.Prefix) {
			result, stale, err := transformer.Transformer.TransformFromStorage(ctx, data[len(transformer.Prefix):], dataCtx)
			// 为从加密迁移出去，用户可在配置文件中把 identity transformer 放在
			//
			// 空前缀代表 identity 转换器：它匹配一切数据，因此出错时必须继续尝试
			// 后面的转换器，否则加密数据将永远无法被解密读取。
			if len(transformer.Prefix) == 0 && err != nil {
				continue
			}
			if len(transformer.Prefix) == 0 {
				RecordTransformation(resource, "from_storage", "identity", time.Since(start), err)
			} else {
				RecordTransformation(resource, "from_storage", string(transformer.Prefix), time.Since(start), err)
			}

			// 同一加密 provider 以不同密钥配置多次时允许前缀重叠：
			//
			// Example:
			//
			//  {
			//    "aescbc": {
			//      "keys": [
			//        {
			//          "name": "2",
			//          "secret": "some key 2"
			//        }
			//      ]
			//    }
			//  },
			//  {
			//    "aescbc": {
			//      "keys": [
			//        {
			//          "name": "1",
			//          "secret": "some key 1"
			//        }
			//      ]
			//    }
			//  },
			//
			// 两个 aescbc 配置的 transformer 共享前缀 k8s:enc:aescbc:v1:，
			// 第一个失败不应阻止后续匹配继续尝试。
			// 因此前缀匹配出错时不短路。
			if err != nil {
				errs = append(errs, err)
				continue
			}

			// 非首个转换器成功说明数据是用旧配置写入的，标记 stale 促使上层回写
			// 为当前配置，从而完成密钥/配置轮换。
			return result, stale || i != 0, err
		}
	}
	if err := errors.Reduce(errors.NewAggregate(errs)); err != nil {
		logTransformErr(ctx, err, "failed to decrypt data")
		return nil, false, err
	}
	RecordTransformation(resource, "from_storage", "unknown", time.Since(start), t.err)
	return nil, false, t.err
}

// TransformToStorage 使用第一个 transformer 并加上其前缀。
//
// 写入永远使用第一个转换器，这是轮换的写入端：新数据用新密钥加密，旧数据在
// 读取时被识别并逐步重写。
func (t *prefixTransformers) TransformToStorage(ctx context.Context, data []byte, dataCtx Context) ([]byte, error) {
	start := time.Now()
	transformer := t.transformers[0]
	resource := getResourceFromContext(ctx)
	result, err := transformer.Transformer.TransformToStorage(ctx, data, dataCtx)
	RecordTransformation(resource, "to_storage", string(transformer.Prefix), time.Since(start), err)
	if err != nil {
		logTransformErr(ctx, err, "failed to encrypt data")
		return nil, err
	}
	prefixedData := make([]byte, len(transformer.Prefix), len(result)+len(transformer.Prefix))
	copy(prefixedData, transformer.Prefix)
	prefixedData = append(prefixedData, result...)
	return prefixedData, nil
}

// logTransformErr 在 V(6) 下附带完整的请求信息，否则只记录错误本身，避免在
// 常规日志级别泄露资源与用户细节。
func logTransformErr(ctx context.Context, err error, message string) {
	requestInfo := getRequestInfoFromContext(ctx)
	if klogLevel6 := klog.V(6); klogLevel6.Enabled() {
		klogLevel6.InfoSDepth(
			1,
			message,
			"err", err,
			"group", requestInfo.APIGroup,
			"version", requestInfo.APIVersion,
			"resource", requestInfo.Resource,
			"subresource", requestInfo.Subresource,
			"verb", requestInfo.Verb,
			"namespace", requestInfo.Namespace,
			"name", requestInfo.Name,
		)

		return
	}

	klog.ErrorSDepth(1, err, message)
}

// getRequestInfoFromContext 在上下文缺失 RequestInfo 时返回空结构而不是报错：
// 转换发生在存储层，可能来自后台任务（如 compaction），不应因此中断。
func getRequestInfoFromContext(ctx context.Context) *genericapirequest.RequestInfo {
	if reqInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		return reqInfo
	}
	klog.V(4).InfoSDepth(1, "no request info on context")
	return &genericapirequest.RequestInfo{}
}

func getResourceFromContext(ctx context.Context) string {
	reqInfo := getRequestInfoFromContext(ctx)
	return schema.GroupResource{Group: reqInfo.APIGroup, Resource: reqInfo.Resource}.String()
}
