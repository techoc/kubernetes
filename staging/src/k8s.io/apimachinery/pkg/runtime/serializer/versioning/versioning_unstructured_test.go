/*
Copyright 2015 The Kubernetes Authors.

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

package versioning

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// buildUnstructuredDecodable 构造指定 GVK 的 Unstructured 对象。
func buildUnstructuredDecodable(gvk schema.GroupVersionKind) runtime.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	return obj
}

// buildUnstructuredListDecodable 构造指定 GVK 的 UnstructuredList 对象。
func buildUnstructuredListDecodable(gvk schema.GroupVersionKind) runtime.Object {
	obj := &unstructured.UnstructuredList{}
	obj.SetGroupVersionKind(gvk)
	return obj
}

// TestEncodeUnstructured 验证 Codec 编码 Unstructured 时如何确定目标版本：
// GVK 与编码版本一致时跳过 typer 与转换；不一致时需经 convertor 转换；
// 无编码版本时回退到 typer 建议的版本；类型无法识别或版本不匹配时报错。
func TestEncodeUnstructured(t *testing.T) {
	v1GVK := schema.GroupVersionKind{
		Group:   "crispy",
		Version: "v1",
		Kind:    "Noxu",
	}
	v2GVK := schema.GroupVersionKind{
		Group:   "crispy",
		Version: "v2",
		Kind:    "Noxu",
	}
	elseGVK := schema.GroupVersionKind{
		Group:   "crispy2",
		Version: "else",
		Kind:    "Noxu",
	}
	elseUnstructuredDecodable := buildUnstructuredDecodable(elseGVK)
	elseUnstructuredDecodableList := buildUnstructuredListDecodable(elseGVK)
	v1UnstructuredDecodable := buildUnstructuredDecodable(v1GVK)
	v1UnstructuredDecodableList := buildUnstructuredListDecodable(v1GVK)
	v2UnstructuredDecodable := buildUnstructuredDecodable(v2GVK)

	testCases := []struct {
		name          string
		convertor     runtime.ObjectConvertor
		targetVersion runtime.GroupVersioner
		outObj        runtime.Object
		typer         runtime.ObjectTyper

		errFunc     func(error) bool
		expectedObj runtime.Object
	}{
		{
			name: "encode v1 unstructured with v2 encode version",
			typer: &mockTyper{
				gvks: []schema.GroupVersionKind{v1GVK},
			},
			outObj:        v1UnstructuredDecodable,
			targetVersion: v2GVK.GroupVersion(),
			convertor: &checkConvertor{
				obj:          v2UnstructuredDecodable,
				groupVersion: v2GVK.GroupVersion(),
			},
			expectedObj: v2UnstructuredDecodable,
		},
		{
			name: "both typer and conversion are bypassed when unstructured gvk matches encode gvk",
			typer: &mockTyper{
				err: fmt.Errorf("unexpected typer call"),
			},
			outObj:        v1UnstructuredDecodable,
			targetVersion: v1GVK.GroupVersion(),
			convertor: &checkConvertor{
				err: fmt.Errorf("unexpected conversion happened"),
			},
			expectedObj: v1UnstructuredDecodable,
		},
		{
			name:          "encode will fail when unstructured object's gvk and encode gvk mismatches",
			outObj:        elseUnstructuredDecodable,
			targetVersion: v1GVK.GroupVersion(),
			errFunc: func(err error) bool {
				return assert.Equal(t, runtime.NewNotRegisteredGVKErrForTarget("noxu-scheme", elseGVK, v1GVK.GroupVersion()), err)
			},
		},
		{
			name:          "encode with unstructured list's gvk regardless of its elements' gvk",
			outObj:        elseUnstructuredDecodableList,
			targetVersion: elseGVK.GroupVersion(),
		},
		{
			name:          "typer fail to recognize unstructured object gvk will fail the encoding",
			outObj:        elseUnstructuredDecodable,
			targetVersion: v1GVK.GroupVersion(),
			typer: &mockTyper{
				err: fmt.Errorf("invalid obj gvk"),
			},
		},
		{
			name:          "encoding unstructured object without encode version will fallback to typer suggested version",
			targetVersion: v1GVK.GroupVersion(),
			convertor: &checkConvertor{
				obj:          v1UnstructuredDecodableList,
				groupVersion: v1GVK.GroupVersion(),
			},
			outObj: elseUnstructuredDecodable,
			typer: &mockTyper{
				gvks: []schema.GroupVersionKind{v1GVK},
			},
		},
	}
	for _, testCase := range testCases {
		serializer := &mockSerializer{}
		codec := NewCodec(serializer, serializer, testCase.convertor, nil, testCase.typer, nil, testCase.targetVersion, nil, "noxu-scheme")
		err := codec.Encode(testCase.outObj, ioutil.Discard)
		if testCase.errFunc != nil {
			// 期望报错的用例：由 errFunc 校验具体错误。
			if !testCase.errFunc(err) {
				t.Errorf("%v: failed: %v", testCase.name, err)
			}
			return
		}
		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedObj, serializer.obj)
	}
}

// errNotRecognizedGVK 表示 convertor 不认识目标 GVK，并列出它声称支持的 GVK。
type errNotRecognizedGVK struct {
	failedGVK    schema.GroupVersionKind
	claimingGVKs []schema.GroupVersionKind
}

func (e errNotRecognizedGVK) Error() string {
	return fmt.Sprintf("unrecognized gvk %v, should be one of %v", e.failedGVK, e.claimingGVKs)
}

// mockUnstructuredNopConvertor 是只做 GVK 校验与深拷贝的 Unstructured 转换器，
// 用于验证 Codec 的版本转换流程而不引入真实转换逻辑。
type mockUnstructuredNopConvertor struct {
	claimingGVKs []schema.GroupVersionKind
}

// recognizeGVK 检查目标 GVK 是否在声明支持的列表中。
func (c *mockUnstructuredNopConvertor) recognizeGVK(gvkToCheck schema.GroupVersionKind) error {
	matched := false
	for _, gvk := range c.claimingGVKs {
		if gvk == gvkToCheck {
			matched = true
		}
	}
	if !matched {
		return errNotRecognizedGVK{
			failedGVK:    gvkToCheck,
			claimingGVKs: c.claimingGVKs,
		}
	}
	return nil
}

// Convert 把 in 深拷贝到 out，并保留 out 预先设置的 GVK；
// 用于模拟 “目标对象已带期望 GVK” 的转换行为。
func (c *mockUnstructuredNopConvertor) Convert(in, out, context interface{}) error {
	inObj := in.(*unstructured.Unstructured)
	outObj := out.(*unstructured.Unstructured)
	if err := c.recognizeGVK(outObj.GroupVersionKind()); err != nil {
		return err
	}
	outGVK := outObj.GetObjectKind().GroupVersionKind()
	*outObj = *inObj.DeepCopy()
	outObj.GetObjectKind().SetGroupVersionKind(outGVK)
	return nil
}

// ConvertToVersion 深拷贝对象并把 GVK 改为目标版本；目标版本无法确定时报错。
func (c *mockUnstructuredNopConvertor) ConvertToVersion(in runtime.Object, outVersion runtime.GroupVersioner) (runtime.Object, error) {
	out := in.DeepCopyObject()
	targetGVK, matched := outVersion.KindForGroupVersionKinds([]schema.GroupVersionKind{in.GetObjectKind().GroupVersionKind()})
	if !matched {
		return nil, fmt.Errorf("attempt to convert to mismatched gv %v", outVersion)
	}
	if err := c.recognizeGVK(out.GetObjectKind().GroupVersionKind()); err != nil {
		return nil, err
	}
	out.GetObjectKind().SetGroupVersionKind(targetGVK)
	return out, nil
}

// ConvertFieldLabel 在此 mock 中不应被调用，调用即返回错误。
func (c *mockUnstructuredNopConvertor) ConvertFieldLabel(gvk schema.GroupVersionKind, label, value string) (string, string, error) {
	return "", "", fmt.Errorf("unexpected call to ConvertFieldLabel")
}

// TestDecodeUnstructured 验证 Codec 解码 Unstructured 的目标版本选择：
// 按建议版本转换、into 对象为 nil 或已有对象时的行为、
// serializer 返回同一对象时跳过转换，以及转换失败时传播错误。
func TestDecodeUnstructured(t *testing.T) {
	internalGVK := schema.GroupVersionKind{
		Group:   "crispy",
		Version: runtime.APIVersionInternal,
		Kind:    "Noxu",
	}
	v1GVK := schema.GroupVersionKind{
		Group:   "crispy",
		Version: "v1",
		Kind:    "Noxu",
	}
	v2GVK := schema.GroupVersionKind{
		Group:   "crispy",
		Version: "v2",
		Kind:    "Noxu",
	}
	internalUnstructuredDecodable := buildUnstructuredDecodable(internalGVK)
	v1UnstructuredDecodable := buildUnstructuredDecodable(v1GVK)
	v2UnstructuredDecodable := buildUnstructuredDecodable(v2GVK)

	testCases := []struct {
		name                    string
		serializer              runtime.Serializer
		convertor               runtime.ObjectConvertor
		suggestedConvertVersion runtime.GroupVersioner
		defaultGVK              *schema.GroupVersionKind
		intoObj                 runtime.Object

		errFunc                     func(error) bool
		expectedGVKOfSerializedData *schema.GroupVersionKind
		expectedOut                 runtime.Object
	}{
		{
			name:       "decode v1 unstructured into non-nil v2 unstructured",
			serializer: &mockSerializer{actual: &v1GVK, obj: v1UnstructuredDecodable},
			convertor: &mockUnstructuredNopConvertor{
				claimingGVKs: []schema.GroupVersionKind{
					v1GVK, v2GVK,
				},
			},
			suggestedConvertVersion:     v2GVK.GroupVersion(),
			intoObj:                     v2UnstructuredDecodable,
			expectedGVKOfSerializedData: &v1GVK,
			expectedOut:                 v2UnstructuredDecodable,
		},
		{
			name:       "decode v1 unstructured into nil object with v2 version",
			serializer: &mockSerializer{actual: &v1GVK, obj: v1UnstructuredDecodable},
			convertor: &mockUnstructuredNopConvertor{
				claimingGVKs: []schema.GroupVersionKind{
					v1GVK, v2GVK,
				},
			},
			suggestedConvertVersion:     v2GVK.GroupVersion(),
			intoObj:                     nil,
			expectedGVKOfSerializedData: &v1GVK,
			expectedOut:                 v2UnstructuredDecodable,
		},
		{
			name:       "decode v1 unstructured into non-nil internal unstructured",
			serializer: &mockSerializer{actual: &v1GVK, obj: v1UnstructuredDecodable},
			convertor: &mockUnstructuredNopConvertor{
				claimingGVKs: []schema.GroupVersionKind{
					v1GVK, v2GVK,
				},
			},
			suggestedConvertVersion: internalGVK.GroupVersion(),
			intoObj:                 internalUnstructuredDecodable,
			errFunc: func(err error) bool {
				notRecognized, ok := err.(errNotRecognizedGVK)
				if !ok {
					return false
				}
				return assert.Equal(t, notRecognized.failedGVK, internalGVK)
			},
		},
		{
			name:       "decode v1 unstructured into nil object with internal version",
			serializer: &mockSerializer{actual: &v1GVK, obj: v1UnstructuredDecodable},
			convertor: &mockUnstructuredNopConvertor{
				claimingGVKs: []schema.GroupVersionKind{
					v1GVK, v2GVK,
				},
			},
			suggestedConvertVersion: internalGVK.GroupVersion(),
			intoObj:                 nil,
			errFunc: func(err error) bool {
				notRecognized, ok := err.(errNotRecognizedGVK)
				if !ok {
					return false
				}
				return assert.Equal(t, notRecognized.failedGVK, internalGVK)
			},
		},
		{
			name:       "skip conversion if serializer returns the same unstructured as into",
			serializer: &mockSerializer{actual: &v1GVK, obj: v1UnstructuredDecodable},
			convertor: &checkConvertor{
				err: fmt.Errorf("unexpected conversion happened"),
			},
			suggestedConvertVersion:     internalGVK.GroupVersion(),
			intoObj:                     v1UnstructuredDecodable,
			expectedGVKOfSerializedData: &v1GVK,
			expectedOut:                 v1UnstructuredDecodable,
		},
		{
			name:       "invalid convert version makes decoding unstructured fail",
			serializer: &mockSerializer{actual: &v1GVK, obj: v1UnstructuredDecodable},
			convertor: &checkConvertor{
				in:           v1UnstructuredDecodable,
				groupVersion: internalGVK.GroupVersion(),
				err:          fmt.Errorf("no matching decode version"),
			},
			suggestedConvertVersion: internalGVK.GroupVersion(),
			errFunc: func(err error) bool {
				return assert.Equal(t, err, fmt.Errorf("no matching decode version"))
			},
		},
	}
	for _, testCase := range testCases {
		codec := NewCodec(testCase.serializer, testCase.serializer, testCase.convertor, nil, nil, nil, nil, testCase.suggestedConvertVersion, "noxu-scheme")
		actualObj, actualSerializedGVK, err := codec.Decode([]byte(`{}`), testCase.defaultGVK, testCase.intoObj)
		if testCase.errFunc != nil {
			// 期望报错的用例：由 errFunc 校验具体错误。
			if !testCase.errFunc(err) {
				t.Errorf("%v: failed: %v", testCase.name, err)
			}
			return
		}
		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedOut, actualObj, "%v failed", testCase.name)
		assert.Equal(t, testCase.expectedGVKOfSerializedData, actualSerializedGVK, "%v failed", testCase.name)
	}
}
