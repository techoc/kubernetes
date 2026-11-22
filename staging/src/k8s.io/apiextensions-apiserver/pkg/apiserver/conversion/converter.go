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

// Package conversion 实现 CRD 自定义资源的版本转换，支持 None 与 webhook 两种策略。
package conversion

import (
	"fmt"
	"strings"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/webhook"
	typedscheme "k8s.io/client-go/kubernetes/scheme"
)

// CRConverterFactory 是所有 CR 转换器的工厂。
type CRConverterFactory struct {
	// webhookConverterFactory 是 webhook 转换器的工厂。
	// 若 CustomResourceWebhookConversion 特性门控关闭，不应使用该字段。
	webhookConverterFactory *webhookConverterFactory
}

// converterMetricFactorySingleton 防止 apiextensions-apiserver 多次运行时指标被重复注册。
var converterMetricFactorySingleton = newConverterMetricFactory()

// NewCRConverterFactory 创建新的 CRConverterFactory。
func NewCRConverterFactory(serviceResolver webhook.ServiceResolver, authResolverWrapper webhook.AuthenticationInfoResolverWrapper) (*CRConverterFactory, error) {
	converterFactory := &CRConverterFactory{}
	webhookConverterFactory, err := newWebhookConverterFactory(serviceResolver, authResolverWrapper)
	if err != nil {
		return nil, err
	}
	converterFactory.webhookConverterFactory = webhookConverterFactory
	return converterFactory, nil
}

// NewConverter 根据 CRD 对象中的转换配置返回新的 CR 转换器。
// 返回 safe 与 unsafe 两个转换器：前者在转换前深拷贝输入，后者会原地修改入参。
func (m *CRConverterFactory) NewConverter(crd *apiextensionsv1.CustomResourceDefinition) (safe, unsafe runtime.ObjectConvertor, err error) {
	validVersions := map[schema.GroupVersion]bool{}
	for _, version := range crd.Spec.Versions {
		validVersions[schema.GroupVersion{Group: crd.Spec.Group, Version: version.Name}] = true
	}

	var converter crConverterInterface
	switch crd.Spec.Conversion.Strategy {
	case apiextensionsv1.NoneConverter:
		converter = &nopConverter{}
	case apiextensionsv1.WebhookConverter:
		converter, err = m.webhookConverterFactory.NewWebhookConverter(crd)
		if err != nil {
			return nil, nil, err
		}
		converter, err = converterMetricFactorySingleton.addMetrics(crd.Name, converter)
		if err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fmt.Errorf("unknown conversion strategy %q for CRD %s", crd.Spec.Conversion.Strategy, crd.Name)
	}

	// 判断是否需要处理 autoscaling/v1 Scale 类型的“转换”请求
	convertScale := false
	selectableFields := map[schema.GroupVersion]sets.Set[string]{}
	for _, version := range crd.Spec.Versions {
		gv := schema.GroupVersion{Group: crd.Spec.Group, Version: version.Name}
		// 任一版本提供 scale 子资源，就需要支持 Scale 类型的转换。
		if version.Subresources != nil && version.Subresources.Scale != nil {
			convertScale = true
		}
		if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourceFieldSelectors) {
			fieldPaths := sets.New[string]()
			for _, sf := range version.SelectableFields {
				// JSONPath 以 "." 开头，去掉后与字段标签名对齐。
				fieldPaths.Insert(strings.TrimPrefix(sf.JSONPath, "."))
			}
			selectableFields[gv] = fieldPaths
		}
	}

	unsafe = &crConverter{
		convertScale:     convertScale,
		validVersions:    validVersions,
		clusterScoped:    crd.Spec.Scope == apiextensionsv1.ClusterScoped,
		converter:        converter,
		selectableFields: selectableFields,
	}
	return &safeConverterWrapper{unsafe}, unsafe, nil
}

// crConverterInterface 是所有 CR 转换器必须实现的接口。
type crConverterInterface interface {
	// Convert 将 in 对象转换为目标 gvk 并返回结果。
	// 注意：该函数可能修改 in 并返回它；safe 包装器会保证对调用方返回安全的结果。
	Convert(in runtime.Object, targetGVK schema.GroupVersion) (runtime.Object, error)
}

// crConverter 在 delegate 转换器之上叠加通用 CR 转换行为；
// delegate 实现 CustomResourceDefinition 中用户配置的转换策略。
type crConverter struct {
	convertScale     bool
	converter        crConverterInterface
	validVersions    map[schema.GroupVersion]bool
	clusterScoped    bool
	selectableFields map[schema.GroupVersion]sets.Set[string]
}

// ConvertFieldLabel 校验字段标签是否可用于该资源，返回转换后的标签与值。
func (c *crConverter) ConvertFieldLabel(gvk schema.GroupVersionKind, label, value string) (string, string, error) {
	switch {
	case label == "metadata.name":
		return label, value, nil
	case !c.clusterScoped && label == "metadata.namespace":
		return label, value, nil
	default:
		if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourceFieldSelectors) {
			groupFields := c.selectableFields[gvk.GroupVersion()]
			if groupFields != nil && groupFields.Has(label) {
				return label, value, nil
			}
		}
		return "", "", fmt.Errorf("field label not supported: %s", label)
	}
}

// Convert 将 in 转换为 out，是 runtime.ObjectConvertor 的实现。
func (c *crConverter) Convert(in, out, context interface{}) error {
	// 若该自定义资源支持 scale 端点，Scale 类型走类型化转换。
	if c.convertScale {
		_, isInScale := in.(*autoscalingv1.Scale)
		_, isOutScale := out.(*autoscalingv1.Scale)
		if isInScale || isOutScale {
			return typedscheme.Scheme.Convert(in, out, context)
		}
	}

	unstructIn, ok := in.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("input type %T in not valid for unstructured conversion to %T", in, out)
	}

	unstructOut, ok := out.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("output type %T in not valid for unstructured conversion from %T", out, in)
	}

	outGVK := unstructOut.GroupVersionKind()
	converted, err := c.ConvertToVersion(unstructIn, outGVK.GroupVersion())
	if err != nil {
		return err
	}
	unstructuredConverted, ok := converted.(runtime.Unstructured)
	if !ok {
		// 不应发生
		return fmt.Errorf("CR conversion failed")
	}
	unstructOut.SetUnstructuredContent(unstructuredConverted.UnstructuredContent())
	return nil
}

// ConvertToVersion 将 in 对象原地转换为目标 gvk 并返回同一个 `in` 对象。
// in 可以是单个对象或 UnstructuredList。CRD 存储实现会先用请求的 GV 创建
// UnstructuredList、从存储填充，再对每个条目调用转换。该函数假定不会收到 v1.List。
func (c *crConverter) ConvertToVersion(in runtime.Object, target runtime.GroupVersioner) (runtime.Object, error) {
	fromGVK := in.GetObjectKind().GroupVersionKind()
	toGVK, ok := target.KindForGroupVersionKinds([]schema.GroupVersionKind{fromGVK})
	if !ok {
		// TODO: 是否应改为类型化错误？
		return nil, fmt.Errorf("%v is unstructured and is not suitable for converting to %q", fromGVK.String(), target)
	}
	// 若该自定义资源支持 scale 端点，Scale 类型走类型化转换。
	if c.convertScale {
		if _, isInScale := in.(*autoscalingv1.Scale); isInScale {
			return typedscheme.Scheme.ConvertToVersion(in, target)
		}
	}

	if !c.validVersions[toGVK.GroupVersion()] {
		return nil, fmt.Errorf("request to convert CR to an invalid group/version: %s", toGVK.GroupVersion().String())
	}
	// 注意：即使请求的是列表，需要转换的也是请求中 UnstructuredList 的 GV。
	// 如函数文档所述，这里预期不会收到 v1.List。
	if !c.validVersions[fromGVK.GroupVersion()] {
		return nil, fmt.Errorf("request to convert CR from an invalid group/version: %s", fromGVK.GroupVersion().String())
	}
	// 列表转换要求每个条目自身的 apiVersion 也合法，否则合并后的列表会混入无效版本。
	if list, ok := in.(*unstructured.UnstructuredList); ok {
		for i := range list.Items {
			expectedGV := list.Items[i].GroupVersionKind().GroupVersion()
			if !c.validVersions[expectedGV] {
				return nil, fmt.Errorf("request to convert CR list failed, list index %d has invalid group/version: %s", i, expectedGV.String())
			}
		}
	}
	return c.converter.Convert(in, toGVK.GroupVersion())
}

// safeConverterWrapper 包装不安全的对象转换器：先拷贝输入，再委托给不安全转换器。
type safeConverterWrapper struct {
	unsafe runtime.ObjectConvertor
}

var _ runtime.ObjectConvertor = &safeConverterWrapper{}

// ConvertFieldLabel 将调用委托给不安全转换器。
func (c *safeConverterWrapper) ConvertFieldLabel(gvk schema.GroupVersionKind, label, value string) (string, string, error) {
	return c.unsafe.ConvertFieldLabel(gvk, label, value)
}

// Convert 先拷贝 in 对象，再将调用委托给不安全转换器。
func (c *safeConverterWrapper) Convert(in, out, context interface{}) error {
	inObject, ok := in.(runtime.Object)
	if !ok {
		return fmt.Errorf("input type %T in not valid for object conversion", in)
	}
	return c.unsafe.Convert(inObject.DeepCopyObject(), out, context)
}

// ConvertToVersion 先拷贝 in 对象，再将调用委托给不安全转换器。
func (c *safeConverterWrapper) ConvertToVersion(in runtime.Object, target runtime.GroupVersioner) (runtime.Object, error) {
	return c.unsafe.ConvertToVersion(in.DeepCopyObject(), target)
}
