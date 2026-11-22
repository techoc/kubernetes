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

package conversion

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/tracing"
)

// webhookConverterFactory 创建调用外部 webhook 的 CR 转换器。
type webhookConverterFactory struct {
	clientManager webhook.ClientManager
}

func newWebhookConverterFactory(serviceResolver webhook.ServiceResolver, authResolverWrapper webhook.AuthenticationInfoResolverWrapper) (*webhookConverterFactory, error) {
	clientManager, err := webhook.NewClientManager(
		[]schema.GroupVersion{v1.SchemeGroupVersion, v1beta1.SchemeGroupVersion},
		v1beta1.AddToScheme,
		v1.AddToScheme,
	)
	if err != nil {
		return nil, err
	}
	authInfoResolver, err := webhook.NewDefaultAuthenticationInfoResolver("")
	if err != nil {
		return nil, err
	}
	// 设置默认值，调用方之后可以覆盖。
	clientManager.SetAuthenticationInfoResolver(authInfoResolver)
	clientManager.SetAuthenticationInfoResolverWrapper(authResolverWrapper)
	clientManager.SetServiceResolver(serviceResolver)
	return &webhookConverterFactory{clientManager}, nil
}

// webhookConverter 是调用外部 webhook 完成 CR 转换的转换器。
type webhookConverter struct {
	clientManager webhook.ClientManager
	restClient    *rest.RESTClient
	name          string
	nopConverter  nopConverter

	conversionReviewVersions []string
}

// webhookClientConfigForCRD 由 CRD 的 webhook 配置构造通用 webhook 客户端配置。
func webhookClientConfigForCRD(crd *v1.CustomResourceDefinition) *webhook.ClientConfig {
	apiConfig := crd.Spec.Conversion.Webhook.ClientConfig
	caBundle := apiConfig.CABundle
	if len(caBundle) > 0 && len(bytes.TrimSpace(caBundle)) == 0 {
		// 将仅含空白的 caBundle 视为空
		caBundle = nil
	}
	ret := webhook.ClientConfig{
		Name:     fmt.Sprintf("conversion_webhook_for_%s", crd.Name),
		CABundle: caBundle,
	}
	if apiConfig.URL != nil {
		ret.URL = *apiConfig.URL
	}
	if apiConfig.Service != nil {
		ret.Service = &webhook.ClientConfigService{
			Name:      apiConfig.Service.Name,
			Namespace: apiConfig.Service.Namespace,
			Port:      *apiConfig.Service.Port,
		}
		if apiConfig.Service.Path != nil {
			ret.Service.Path = *apiConfig.Service.Path
		}
	}
	return &ret
}

var _ crConverterInterface = &webhookConverter{}

// NewWebhookConverter 为指定 CRD 创建 webhook 转换器。
func (f *webhookConverterFactory) NewWebhookConverter(crd *v1.CustomResourceDefinition) (*webhookConverter, error) {
	restClient, err := f.clientManager.HookClient(*webhookClientConfigForCRD(crd))
	if err != nil {
		return nil, err
	}
	return &webhookConverter{
		clientManager: f.clientManager,
		restClient:    restClient,
		name:          crd.Name,
		nopConverter:  nopConverter{},

		conversionReviewVersions: crd.Spec.Conversion.Webhook.ConversionReviewVersions,
	}, nil
}

// getObjectsToConvert 返回需要转换的对象列表。
// 若 obj 是列表，返回尚未处于目标版本的对象组成的列表（可能为空）。
// 若 obj 不是列表且已处于目标版本，返回空列表。
// 若 obj 不是列表且尚未处于目标版本，返回仅包含 obj 的列表。
func getObjectsToConvert(obj runtime.Object, apiVersion string) []runtime.RawExtension {
	listObj, isList := obj.(*unstructured.UnstructuredList)
	var objects []runtime.RawExtension
	if isList {
		for i := range listObj.Items {
			// 仅当 apiVersion 不同时才发送该项进行转换
			if listObj.Items[i].GetAPIVersion() != apiVersion {
				objects = append(objects, runtime.RawExtension{Object: &listObj.Items[i]})
			}
		}
	} else {
		if obj.GetObjectKind().GroupVersionKind().GroupVersion().String() != apiVersion {
			objects = []runtime.RawExtension{{Object: obj}}
		}
	}
	return objects
}

// createConversionReviewObjects 使用 conversionReviewVersions 中第一个受支持的版本，
// 构造 ConversionReview 的请求和响应对象。
func createConversionReviewObjects(conversionReviewVersions []string, objects []runtime.RawExtension, apiVersion string, requestUID types.UID) (request, response runtime.Object, err error) {
	for _, version := range conversionReviewVersions {
		switch version {
		case v1beta1.SchemeGroupVersion.Version:
			return &v1beta1.ConversionReview{
				Request: &v1beta1.ConversionRequest{
					Objects:           objects,
					DesiredAPIVersion: apiVersion,
					UID:               requestUID,
				},
				Response: &v1beta1.ConversionResponse{},
			}, &v1beta1.ConversionReview{}, nil
		case v1.SchemeGroupVersion.Version:
			return &v1.ConversionReview{
				Request: &v1.ConversionRequest{
					Objects:           objects,
					DesiredAPIVersion: apiVersion,
					UID:               requestUID,
				},
				Response: &v1.ConversionResponse{},
			}, &v1.ConversionReview{}, nil
		}
	}
	return nil, nil, fmt.Errorf("no supported conversion review versions")
}

// getRawExtensionObject 从 RawExtension 中取出对象，Raw 字段则按 unstructured 解析。
func getRawExtensionObject(rx runtime.RawExtension) (runtime.Object, error) {
	if rx.Object != nil {
		return rx.Object, nil
	}
	u := unstructured.Unstructured{}
	err := u.UnmarshalJSON(rx.Raw)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// getConvertedObjectsFromResponse 校验响应并返回转换后的对象。
// 若响应格式错误，则返回错误。
// 若响应未表示成功，则返回其中的错误信息。
func getConvertedObjectsFromResponse(expectedUID types.UID, response runtime.Object) (convertedObjects []runtime.RawExtension, err error) {
	switch response := response.(type) {
	case *v1.ConversionReview:
		// 校验 GVK，确认解码出的确实是我们期望的类型
		v1GVK := v1.SchemeGroupVersion.WithKind("ConversionReview")
		if response.GroupVersionKind() != v1GVK {
			return nil, fmt.Errorf("expected webhook response of %v, got %v", v1GVK.String(), response.GroupVersionKind().String())
		}

		if response.Response == nil {
			return nil, fmt.Errorf("no response provided")
		}

		// 校验 UID，确认该响应确实是针对我们发送的请求
		if response.Response.UID != expectedUID {
			return nil, fmt.Errorf("expected response.uid=%q, got %q", expectedUID, response.Response.UID)
		}

		if response.Response.Result.Status != metav1.StatusSuccess {
			// TODO: 返回 webhook 专用错误，以便转换为 meta.Status
			if len(response.Response.Result.Message) > 0 {
				return nil, errors.New(response.Response.Result.Message)
			}
			return nil, fmt.Errorf("response.result.status was '%s', not 'Success'", response.Response.Result.Status)
		}

		return response.Response.ConvertedObjects, nil

	case *v1beta1.ConversionReview:
		// v1beta1 的处理不校验 GVK 和 UID，为保持兼容性此处跳过

		if response.Response == nil {
			return nil, fmt.Errorf("no response provided")
		}

		if response.Response.Result.Status != metav1.StatusSuccess {
			// TODO: 返回 webhook 专用错误，以便转换为 meta.Status
			if len(response.Response.Result.Message) > 0 {
				return nil, errors.New(response.Response.Result.Message)
			}
			return nil, fmt.Errorf("response.result.status was '%s', not 'Success'", response.Response.Result.Status)
		}

		return response.Response.ConvertedObjects, nil

	default:
		return nil, fmt.Errorf("unrecognized response type: %T", response)
	}
}

// Convert 调用外部 webhook 将对象转换到目标版本。
func (c *webhookConverter) Convert(in runtime.Object, toGV schema.GroupVersion) (runtime.Object, error) {
	ctx := context.TODO()
	// 一般而言，webhook 不应做任何默认化或校验。一个特例是空对象转换，
	// 它必须返回空对象，实际上等同 nopConverter。
	// API machinery 中的冒烟测试会用空对象调用转换器。由于该情形总是出现，
	// 这里特判为不调用 webhook 转换器。该测试见：
	// https://github.com/kubernetes/kubernetes/blob/dbb448bbdcb9e440eee57024ffa5f1698956a054/staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go#L201
	if isEmptyUnstructuredObject(in) {
		return c.nopConverter.Convert(in, toGV)
	}
	t := time.Now()
	listObj, isList := in.(*unstructured.UnstructuredList)

	requestUID := uuid.NewUUID()
	desiredAPIVersion := toGV.String()
	objectsToConvert := getObjectsToConvert(in, desiredAPIVersion)
	request, response, err := createConversionReviewObjects(c.conversionReviewVersions, objectsToConvert, desiredAPIVersion, requestUID)
	if err != nil {
		return nil, err
	}

	objCount := len(objectsToConvert)
	if objCount == 0 {
		Metrics.ObserveConversionWebhookSuccess(ctx, time.Since(t))
		// 没有需要转换的对象
		if !isList {
			// 单个对象直接原样返回
			return in, nil
		}
		// 列表则只需设置顶层列表对象的版本（各个对象已处于正确版本）
		out := listObj.DeepCopy()
		out.SetAPIVersion(toGV.String())
		return out, nil
	}

	ctx, span := tracing.Start(ctx, "Call conversion webhook",
		attribute.String("custom-resource-definition", c.name),
		attribute.String("desired-api-version", desiredAPIVersion),
		attribute.Int("object-count", objCount),
		attribute.String("UID", string(requestUID)))
	// 只记录超过每对象 8ms 限制加 50ms 请求开销允许值的转换 webhook trace。
	// 每对象限制采用转换 webhook 的 SLO（约 4ms/对象），加上 apiserver 侧
	// 序列化/反序列化转换请求的时间（约 4ms/对象）。
	defer span.End(time.Duration(50+8*objCount) * time.Millisecond)

	// TODO: 考虑在此处加一秒超时是否合理。
	r := c.restClient.Post().Body(request).Do(ctx)
	if err := r.Into(response); err != nil {
		// TODO: 返回 webhook 专用错误，以便转换为 meta.Status
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookCallFailure)
		return nil, fmt.Errorf("conversion webhook for %v failed: %v", in.GetObjectKind().GroupVersionKind(), err)
	}
	span.AddEvent("Request completed")

	convertedObjects, err := getConvertedObjectsFromResponse(requestUID, response)
	if err != nil {
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookMalformedResponseFailure)
		return nil, fmt.Errorf("conversion webhook for %v failed: %v", in.GetObjectKind().GroupVersionKind(), err)
	}

	if len(convertedObjects) != len(objectsToConvert) {
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookPartialResponseFailure)
		return nil, fmt.Errorf("conversion webhook for %v returned %d objects, expected %d", in.GetObjectKind().GroupVersionKind(), len(convertedObjects), len(objectsToConvert))
	}

	if isList {
		// 对输入做深拷贝，并把响应中的转换结果填到对应位置。
		// 响应列表可能是不完整的，因为部分对象本来就已是目标版本。
		convertedList := listObj.DeepCopy()
		convertedIndex := 0
		for i := range convertedList.Items {
			original := &convertedList.Items[i]
			if original.GetAPIVersion() == toGV.String() {
				// 该项未发送去转换，因此不会出现在响应中。
				// convertedList 中已经是正确的对象。
				continue
			}
			converted, err := getRawExtensionObject(convertedObjects[convertedIndex])
			if err != nil {
				Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
				return nil, fmt.Errorf("conversion webhook for %v returned invalid converted object at index %v: %v", in.GetObjectKind().GroupVersionKind(), convertedIndex, err)
			}
			if expected, got := toGV, converted.GetObjectKind().GroupVersionKind().GroupVersion(); expected != got {
				Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
				return nil, fmt.Errorf("conversion webhook for %v returned invalid converted object at index %v: invalid groupVersion (expected %v, received %v)", in.GetObjectKind().GroupVersionKind(), convertedIndex, expected, got)
			}
			if expected, got := original.GetObjectKind().GroupVersionKind().Kind, converted.GetObjectKind().GroupVersionKind().Kind; expected != got {
				Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
				return nil, fmt.Errorf("conversion webhook for %v returned invalid converted object at index %v: invalid kind (expected %v, received %v)", in.GetObjectKind().GroupVersionKind(), convertedIndex, expected, got)
			}
			unstructConverted, ok := converted.(*unstructured.Unstructured)
			if !ok {
				// 这不应该发生
				Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
				return nil, fmt.Errorf("conversion webhook for %v returned invalid converted object at index %v: invalid type, expected=Unstructured, got=%T", in.GetObjectKind().GroupVersionKind(), convertedIndex, converted)
			}
			if err := validateConvertedObject(original, unstructConverted); err != nil {
				Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
				return nil, fmt.Errorf("conversion webhook for %v returned invalid converted object at index %v: %v", in.GetObjectKind().GroupVersionKind(), convertedIndex, err)
			}
			if err := restoreObjectMeta(original, unstructConverted); err != nil {
				Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
				return nil, fmt.Errorf("conversion webhook for %v returned invalid metadata in object at index %v: %v", in.GetObjectKind().GroupVersionKind(), convertedIndex, err)
			}
			convertedIndex++
			convertedList.Items[i] = *unstructConverted
		}
		convertedList.SetAPIVersion(toGV.String())
		Metrics.ObserveConversionWebhookSuccess(ctx, time.Since(t))
		return convertedList, nil
	}

	if len(convertedObjects) != 1 {
		// 这不应该发生
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookNoObjectsReturnedFailure)
		return nil, fmt.Errorf("conversion webhook for %v failed, no objects returned", in.GetObjectKind())
	}
	converted, err := getRawExtensionObject(convertedObjects[0])
	if err != nil {
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
		return nil, err
	}
	if e, a := toGV, converted.GetObjectKind().GroupVersionKind().GroupVersion(); e != a {
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
		return nil, fmt.Errorf("conversion webhook for %v returned invalid object at index 0: invalid groupVersion (expected %v, received %v)", in.GetObjectKind().GroupVersionKind(), e, a)
	}
	if e, a := in.GetObjectKind().GroupVersionKind().Kind, converted.GetObjectKind().GroupVersionKind().Kind; e != a {
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
		return nil, fmt.Errorf("conversion webhook for %v returned invalid object at index 0: invalid kind (expected %v, received %v)", in.GetObjectKind().GroupVersionKind(), e, a)
	}
	unstructConverted, ok := converted.(*unstructured.Unstructured)
	if !ok {
		// 这不应该发生
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
		return nil, fmt.Errorf("conversion webhook for %v failed, unexpected type %T at index 0", in.GetObjectKind().GroupVersionKind(), converted)
	}
	unstructIn, ok := in.(*unstructured.Unstructured)
	if !ok {
		// 这不应该发生
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
		return nil, fmt.Errorf("conversion webhook for %v failed unexpected input type %T", in.GetObjectKind().GroupVersionKind(), in)
	}
	if err := validateConvertedObject(unstructIn, unstructConverted); err != nil {
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
		return nil, fmt.Errorf("conversion webhook for %v returned invalid object: %v", in.GetObjectKind().GroupVersionKind(), err)
	}
	if err := restoreObjectMeta(unstructIn, unstructConverted); err != nil {
		Metrics.ObserveConversionWebhookFailure(ctx, time.Since(t), ConversionWebhookInvalidConvertedObjectFailure)
		return nil, fmt.Errorf("conversion webhook for %v returned invalid metadata: %v", in.GetObjectKind().GroupVersionKind(), err)
	}
	Metrics.ObserveConversionWebhookSuccess(ctx, time.Since(t))
	return converted, nil
}

// validateConvertedObject 校验 ObjectMeta 字段一致，但 labels 和 annotations 除外。
func validateConvertedObject(in, out *unstructured.Unstructured) error {
	if e, a := in.GetKind(), out.GetKind(); e != a {
		return fmt.Errorf("must have the same kind: %v != %v", e, a)
	}
	if e, a := in.GetName(), out.GetName(); e != a {
		return fmt.Errorf("must have the same name: %v != %v", e, a)
	}
	if e, a := in.GetNamespace(), out.GetNamespace(); e != a {
		return fmt.Errorf("must have the same namespace: %v != %v", e, a)
	}
	if e, a := in.GetUID(), out.GetUID(); e != a {
		return fmt.Errorf("must have the same UID: %v != %v", e, a)
	}
	return nil
}

// restoreObjectMeta 将原始对象的 metadata 深拷贝到转换结果中，同时保留转换结果中的 labels 和 annotations。
func restoreObjectMeta(original, converted *unstructured.Unstructured) error {
	obj, found := converted.Object["metadata"]
	if !found {
		return fmt.Errorf("missing metadata in converted object")
	}
	responseMetaData, ok := obj.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid metadata of type %T in converted object", obj)
	}

	if _, ok := original.Object["metadata"]; !ok {
		// 原始对象总会有 metadata。但为稳妥起见，这里用空对象而非 nil 清空转换结果中的 metadata，
		// 以便下面添加 labels 和 annotations。
		converted.Object["metadata"] = map[string]interface{}{}
	} else {
		converted.Object["metadata"] = runtime.DeepCopyJSONValue(original.Object["metadata"])
	}

	obj = converted.Object["metadata"]
	convertedMetaData, ok := obj.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid metadata of type %T in input object", obj)
	}

	for _, fld := range []string{"labels", "annotations"} {
		obj, found := responseMetaData[fld]
		if !found || obj == nil {
			delete(convertedMetaData, fld)
			continue
		}
		responseField, ok := obj.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid metadata.%s of type %T in converted object", fld, obj)
		}

		originalField, ok := convertedMetaData[fld].(map[string]interface{})
		if !ok && convertedMetaData[fld] != nil {
			return fmt.Errorf("invalid metadata.%s of type %T in original object", fld, convertedMetaData[fld])
		}

		somethingChanged := len(originalField) != len(responseField)
		for k, v := range responseField {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("metadata.%s[%s] must be a string, but is %T in converted object", fld, k, v)
			}
			if originalField[k] != interface{}(v) {
				somethingChanged = true
			}
		}

		if somethingChanged {
			stringMap := make(map[string]string, len(responseField))
			for k, v := range responseField {
				stringMap[k] = v.(string)
			}
			var errs field.ErrorList
			if fld == "labels" {
				errs = metav1validation.ValidateLabels(stringMap, field.NewPath("metadata", "labels"))
			} else {
				errs = apivalidation.ValidateAnnotations(stringMap, field.NewPath("metadata", "annotation"))
			}
			if len(errs) > 0 {
				return errs.ToAggregate()
			}
		}

		convertedMetaData[fld] = responseField
	}

	return nil
}

// isEmptyUnstructuredObject 判断 in 是否为空 unstructured 对象，
// 即除 apiVersion 和 kind 外不含任何字段。
func isEmptyUnstructuredObject(in runtime.Object) bool {
	u, ok := in.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	if len(u.Object) != 2 {
		return false
	}
	if _, ok := u.Object["kind"]; !ok {
		return false
	}
	if _, ok := u.Object["apiVersion"]; !ok {
		return false
	}
	return true
}
