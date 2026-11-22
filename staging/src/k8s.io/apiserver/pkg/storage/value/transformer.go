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

// Package value contains methods for assisting with transformation of values in storage.
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

// Context is additional information that a storage transformation may need to verify the data at rest.
type Context interface {
	// AuthenticatedData should return an array of bytes that describes the current value. If the value changes,
	// the transformer may report the value as unreadable or tampered. This may be nil if no such description exists
	// or is needed. For additional verification, set this to data that strongly identifies the value, such as
	// the key and creation version of the stored data.
	AuthenticatedData() []byte
}

// Read 把存储中的字节转换回明文。
type Read interface {
	// TransformFromStorage may transform the provided data from its underlying storage representation or return an error.
	// Stale is true if the object on disk is stale and a write to etcd should be issued, even if the contents of the object
	// have not changed.
	TransformFromStorage(ctx context.Context, data []byte, dataCtx Context) (out []byte, stale bool, err error)
}

// Write 把明文转换为存储表示。
type Write interface {
	// TransformToStorage may transform the provided data into the appropriate form in storage or return an error.
	TransformToStorage(ctx context.Context, data []byte, dataCtx Context) (out []byte, err error)
}

// Transformer allows a value to be transformed before being read from or written to the underlying store. The methods
// must be able to undo the transformation caused by the other.
type Transformer interface {
	Read
	Write
}

// ResourceTransformers returns a transformer for the provided resource.
type ResourceTransformers interface {
	TransformerForResource(resource schema.GroupResource) Transformer
}

// DefaultContext is a simple implementation of Context for a slice of bytes.
type DefaultContext []byte

// AuthenticatedData returns itself.
func (c DefaultContext) AuthenticatedData() []byte { return c }

// PrefixTransformer holds a transformer interface and the prefix that the transformation is located under.
type PrefixTransformer struct {
	Prefix      []byte
	Transformer Transformer
}

type prefixTransformers struct {
	transformers []PrefixTransformer
	err          error
}

var _ Transformer = &prefixTransformers{}

// NewPrefixTransformers supports the Transformer interface by checking the incoming data against the provided
// prefixes in order. The first matching prefix will be used to transform the value (the prefix is stripped
// before the Transformer interface is invoked). The first provided transformer will be used when writing to
// the store.
func NewPrefixTransformers(err error, transformers ...PrefixTransformer) Transformer {
	if err == nil {
		err = fmt.Errorf("the provided value does not match any of the supported transformers")
	}
	return &prefixTransformers{
		transformers: transformers,
		err:          err,
	}
}

// TransformFromStorage finds the first transformer with a prefix matching the provided data and returns
// the result of transforming the value. It will always mark any transformation as stale that is not using
// the first transformer.
func (t *prefixTransformers) TransformFromStorage(ctx context.Context, data []byte, dataCtx Context) ([]byte, bool, error) {
	start := time.Now()
	var errs []error
	resource := getResourceFromContext(ctx)
	for i, transformer := range t.transformers {
		if bytes.HasPrefix(data, transformer.Prefix) {
			result, stale, err := transformer.Transformer.TransformFromStorage(ctx, data[len(transformer.Prefix):], dataCtx)
			// To migrate away from encryption, user can specify an identity transformer higher up
			// (in the config file) than the encryption transformer. In that scenario, the identity transformer needs to
			// identify (during reads from disk) whether the data being read is encrypted or not. If the data is encrypted,
			// it shall throw an error, but that error should not prevent the next subsequent transformer from being tried.
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

			// It is valid to have overlapping prefixes when the same encryption provider
			// is specified multiple times but with different keys (the first provider is
			// being rotated to and some later provider is being rotated away from).
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
			// The transformers for both aescbc configs share the prefix k8s:enc:aescbc:v1:
			// but a failure in the first one should not prevent a later match from being attempted.
			// Thus we never short-circuit on a prefix match that results in an error.
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

// TransformToStorage uses the first transformer and adds its prefix to the data.
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
