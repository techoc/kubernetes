/*
Copyright 2021 The Kubernetes Authors.

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

// 本文件实现 ensurer 的 REST Strategy，定义 APF 配置对象的字段隔离、校验和更新规则。
package ensurer

import (
	"context"
	"fmt"
	"strconv"

	flowcontrolv1 "k8s.io/api/flowcontrol/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

const (
	fieldManager = "api-priority-and-fairness-config-producer-v1"
)

// EnsureStrategy 定义 APF 引导配置的维护策略，分为强制配置和建议配置两类：
//
//   - 强制配置（mandatory）：用于保证 APF 自身能够正常工作，例如为所有未匹配请求提供兜底处理规则。
//     API Server 会覆盖集群管理员对这类对象所做的 Spec 修改，并在对象缺失时重新创建。
//
//   - 建议配置（suggested）：用于提供集群初始行为的推荐配置。
//     集群管理员可以根据实际需求修改或删除这些对象。
type EnsureStrategy[ObjectType configurationObjectType] interface {
	// Name 返回策略名称，目前只有 mandatory（强制）和 suggested（建议）两种，主要用于日志标识。
	Name() string

	// ReviseIfNeeded 比较集群中的现有配置与 kube-apiserver 内存中的引导配置，判断是否需要自动更新。
	// current 是集群中已经存在的配置对象，bootstrap 是 kube-apiserver 维护的目标配置。
	// revised 是需要写回集群的新对象；ok 表示是否需要更新；err 表示无法判断是否需要更新时发生的错误。
	ReviseIfNeeded(objectOps objectLocalOps[ObjectType], current, bootstrap ObjectType) (revised ObjectType, ok bool, err error)
}

// objectLocalOps 定义单个配置对象所需的本地操作，不依赖 API Server 或客户端连接。
type objectLocalOps[ObjectType configurationObject] interface {
	DeepCopy(ObjectType) ObjectType

	// ReplaceSpec 深拷贝 into，并将其 Spec 替换为 from 的 Spec 深拷贝；其余字段保持 into 的值。
	ReplaceSpec(into, from ObjectType) ObjectType

	// SpecEqualish 判断 expected 经默认值处理后，其 Spec 是否与 actual 的 Spec 在当前资源的比较语义下等价。
	SpecEqualish(expected, actual ObjectType) bool
}

// ObjectOps 汇总配置对象协调所需的操作，包括 API 客户端访问、Informer 缓存读取和对象本地处理。
type ObjectOps[ObjectType configurationObject] interface {
	client[ObjectType]
	cache[ObjectType]
	objectLocalOps[ObjectType]
}

// client 是对应资源的类型化客户端接口子集，仅包含创建、更新和删除配置对象所需的方法。
type client[ObjectType configurationObject] interface {
	Create(ctx context.Context, obj ObjectType, opts metav1.CreateOptions) (ObjectType, error)
	Update(ctx context.Context, obj ObjectType, opts metav1.UpdateOptions) (ObjectType, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
}

// cache 是 Informer 本地缓存的 Lister 接口子集，用于按名称读取对象或列出匹配对象。
type cache[ObjectType configurationObject] interface {
	List(labels.Selector) ([]ObjectType, error)
	Get(name string) (ObjectType, error)
}

// configurationObject 约束可参与配置协调的 API 对象必须同时实现 metav1.Object 和 runtime.Object。
type configurationObject interface {
	metav1.Object
	runtime.Object
}

// configurationObjectType 在 configurationObject 的基础上增加 comparable 约束，供泛型类型参数使用。
type configurationObjectType interface {
	comparable
	configurationObject
}

type objectOps[ObjectType configurationObjectType] struct {
	client[ObjectType]
	cache[ObjectType]
	deepCopy     func(ObjectType) ObjectType
	replaceSpec  func(ObjectType, ObjectType) ObjectType
	specEqualish func(expected, actual ObjectType) bool
}

func NewObjectOps[ObjectType configurationObjectType](client client[ObjectType], cache cache[ObjectType],
	deepCopy func(ObjectType) ObjectType,
	replaceSpec func(ObjectType, ObjectType) ObjectType,
	specEqualish func(expected, actual ObjectType) bool,
) ObjectOps[ObjectType] {
	return objectOps[ObjectType]{client: client,
		cache:        cache,
		deepCopy:     deepCopy,
		replaceSpec:  replaceSpec,
		specEqualish: specEqualish}
}

func (oo objectOps[ObjectType]) DeepCopy(obj ObjectType) ObjectType { return oo.deepCopy(obj) }

func (oo objectOps[ObjectType]) ReplaceSpec(into, from ObjectType) ObjectType {
	return oo.replaceSpec(into, from)
}

func (oo objectOps[ObjectType]) SpecEqualish(expected, actual ObjectType) bool {
	return oo.specEqualish(expected, actual)
}

// NewSuggestedEnsureStrategy 创建建议配置的维护策略；管理员对 Spec 的修改不会被自动覆盖。
func NewSuggestedEnsureStrategy[ObjectType configurationObjectType]() EnsureStrategy[ObjectType] {
	return &strategy[ObjectType]{
		alwaysAutoUpdateSpec: false,
		name:                 "suggested",
	}
}

// NewMandatoryEnsureStrategy 创建强制配置的维护策略；每次协调都会以引导配置为准检查并修复 Spec。
func NewMandatoryEnsureStrategy[ObjectType configurationObjectType]() EnsureStrategy[ObjectType] {
	return &strategy[ObjectType]{
		alwaysAutoUpdateSpec: true,
		name:                 "mandatory",
	}
}

// strategy 实现 APF 配置对象的自动更新策略。
type strategy[ObjectType configurationObjectType] struct {
	alwaysAutoUpdateSpec bool
	name                 string
}

func (s *strategy[ObjectType]) Name() string {
	return s.name
}

func (s *strategy[ObjectType]) ReviseIfNeeded(objectOps objectLocalOps[ObjectType], current, bootstrap ObjectType) (ObjectType, bool, error) {
	var zero ObjectType
	if current == zero {
		return zero, false, nil
	}

	autoUpdateSpec := s.alwaysAutoUpdateSpec
	if !autoUpdateSpec {
		autoUpdateSpec = shouldUpdateSpec(current)
	}
	updateAnnotation := shouldUpdateAnnotation(current, autoUpdateSpec)

	specChanged := autoUpdateSpec && !objectOps.SpecEqualish(bootstrap, current)

	if !(updateAnnotation || specChanged) {
		// 自动更新注解已经符合目标值，且 Spec 无需调整，因此不必写回对象。
		return zero, false, nil
	}

	var revised ObjectType
	if specChanged {
		revised = objectOps.ReplaceSpec(current, bootstrap)
	} else {
		revised = objectOps.DeepCopy(current)
	}
	if updateAnnotation {
		setAutoUpdateAnnotation(revised, autoUpdateSpec)
	}

	return revised, true, nil
}

// shouldUpdateSpec 根据自动更新注解和 Generation 判断是否允许自动覆盖配置对象的 Spec。
func shouldUpdateSpec(accessor metav1.Object) bool {
	value := accessor.GetAnnotations()[flowcontrolv1.AutoUpdateAnnotationKey]
	if autoUpdate, err := strconv.ParseBool(value); err == nil {
		return autoUpdate
	}

	// 注解缺失，或注解值无法解析为布尔值时，使用 Generation 推断管理员是否修改过 Spec。
	// Generation 为 1 表示对象尚未经历 Spec 更新，因此可以安全地应用新的引导 Spec。
	// 但如果管理员删除对象后使用不同 Spec 重新创建同名对象，Generation 会重新从 1 开始，
	// 这种情况下无法识别并保护管理员的新配置。
	if accessor.GetGeneration() == 1 {
		return true
	}
	return false
}

// shouldUpdateAnnotation 判断对象上的自动更新注解是否已经等于期望值。
func shouldUpdateAnnotation(accessor metav1.Object, desired bool) bool {
	if value, ok := accessor.GetAnnotations()[flowcontrolv1.AutoUpdateAnnotationKey]; ok {
		if current, err := strconv.ParseBool(value); err == nil && current == desired {
			return false
		}
	}

	return true
}

// setAutoUpdateAnnotation 将自动更新注解设置为指定的布尔值；必要时先初始化 Annotations。
func setAutoUpdateAnnotation(accessor metav1.Object, autoUpdate bool) {
	if accessor.GetAnnotations() == nil {
		accessor.SetAnnotations(map[string]string{})
	}

	accessor.GetAnnotations()[flowcontrolv1.AutoUpdateAnnotationKey] = strconv.FormatBool(autoUpdate)
}

// EnsureConfigurations 按顺序对一组引导配置应用维护策略；遇到第一个错误时立即停止并返回。
func EnsureConfigurations[ObjectType configurationObjectType](ctx context.Context, ops ObjectOps[ObjectType], boots []ObjectType, strategy EnsureStrategy[ObjectType]) error {
	for _, bo := range boots {
		err := EnsureConfiguration(ctx, ops, bo, strategy)
		if err != nil {
			return err
		}
	}
	return nil
}

// EnsureConfiguration 确保单个引导配置对象存在且符合对应维护策略。
func EnsureConfiguration[ObjectType configurationObjectType](ctx context.Context, ops ObjectOps[ObjectType], bootstrap ObjectType, strategy EnsureStrategy[ObjectType]) error {
	name := bootstrap.GetName()
	configurationType := strategy.Name()

	var current ObjectType
	var err error
	for {
		current, err = ops.Get(name)
		if err == nil {
			break
		}
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to retrieve %T type=%s name=%q error=%w", bootstrap, configurationType, name, err)
		}

		// 引导配置对象属于系统维护对象，缺失时始终尝试重新创建。
		if _, err = ops.Create(ctx, ops.DeepCopy(bootstrap), metav1.CreateOptions{FieldManager: fieldManager}); err == nil {
			klog.V(2).InfoS(fmt.Sprintf("Successfully created %T", bootstrap), "type", configurationType, "name", name)
			return nil
		}

		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("cannot create %T type=%s name=%q error=%w", bootstrap, configurationType, name, err)
		}
		klog.V(5).InfoS(fmt.Sprintf("Something created the %T concurrently", bootstrap), "type", configurationType, "name", name)
	}

	klog.V(5).InfoS(fmt.Sprintf("The %T already exists, checking whether it is up to date", bootstrap), "type", configurationType, "name", name)
	newObject, update, err := strategy.ReviseIfNeeded(ops, current, bootstrap)
	if err != nil {
		return fmt.Errorf("failed to determine whether auto-update is required for %T type=%s name=%q error=%w", bootstrap, configurationType, name, err)
	}
	if !update {
		if klogV := klog.V(5); klogV.Enabled() {
			klogV.InfoS("No update required", "wrapper", bootstrap.GetObjectKind().GroupVersionKind().Kind, "type", configurationType, "name", name,
				"diff", diff.Diff(current, bootstrap))
		}
		return nil
	}

	if _, err = ops.Update(ctx, newObject, metav1.UpdateOptions{FieldManager: fieldManager}); err == nil {
		klog.V(2).Infof("Updated the %T type=%s name=%q diff: %s", bootstrap, configurationType, name, diff.Diff(current, bootstrap))
		return nil
	}

	if apierrors.IsConflict(err) {
		klog.V(2).InfoS(fmt.Sprintf("Something updated the %T concurrently, I will check its spec later", bootstrap), "type", configurationType, "name", name)
		return nil
	}

	return fmt.Errorf("failed to update the %T, will retry later type=%s name=%q error=%w", bootstrap, configurationType, name, err)
}

// RemoveUnwantedObjects 尝试删除不在目标名称集合中、且标记为 apf.kubernetes.io/autoupdate-spec=true 的现存配置对象。
// 如果对象因并发删除等原因已不存在，则记录日志并继续；后续协调周期还会再次检查。
func RemoveUnwantedObjects[ObjectType configurationObjectType](ctx context.Context, objectOps ObjectOps[ObjectType], boots []ObjectType) error {
	current, err := objectOps.List(labels.Everything())
	if err != nil {
		return err
	}
	wantedNames := namesOfBootstrapObjects(boots)
	for _, object := range current {
		name := object.GetName()
		if wantedNames.Has(name) {
			continue
		}
		var value string
		var ok, autoUpdate bool
		var err error
		if value, ok = object.GetAnnotations()[flowcontrolv1.AutoUpdateAnnotationKey]; !ok {
			// 对象没有自动更新注解，通常意味着它是管理员自行创建的配置，因此跳过删除。
			klog.V(5).InfoS("Skipping deletion of APF object with no "+flowcontrolv1.AutoUpdateAnnotationKey+" annotation", "name", name)
			continue
		}
		autoUpdate, err = strconv.ParseBool(value)
		if err != nil {
			// 注解值无法解析为布尔值属于异常状态，记录日志后跳过删除。
			klog.V(4).InfoS("Skipping deletion of APF object with malformed "+flowcontrolv1.AutoUpdateAnnotationKey+" annotation", "name", name, "annotationValue", value, "parseError", err)
			continue
		}
		if !autoUpdate {
			klog.V(5).InfoS("Skipping deletion of APF object with "+flowcontrolv1.AutoUpdateAnnotationKey+"=false annotation", "name", name)
			continue
		}
		// TODO: expectedResourceVersion := object.GetResourceVersion()
		err = objectOps.Delete(ctx, object.GetName(), metav1.DeleteOptions{ /* TODO: expectedResourceVersion */ })
		if err == nil {
			klog.V(2).InfoS(fmt.Sprintf("Successfully deleted the unwanted %s", object.GetObjectKind().GroupVersionKind().Kind), "name", name)
			continue
		}
		if apierrors.IsNotFound(err) {
			klog.V(5).InfoS("Unwanted APF object was concurrently deleted", "name", name)
		} else {
			return fmt.Errorf("failed to delete unwatned APF object %q - %w", name, err)
		}
	}
	return nil
}

func namesOfBootstrapObjects[ObjectType configurationObjectType](bos []ObjectType) sets.String {
	names := sets.NewString()
	for _, bo := range bos {
		names.Insert(bo.GetName())
	}
	return names
}
