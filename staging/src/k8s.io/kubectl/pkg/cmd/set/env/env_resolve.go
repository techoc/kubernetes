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

package env

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// ResourceStore 定义了一种新的资源存储数据结构。
type ResourceStore struct {
	SecretStore    map[string]*corev1.Secret
	ConfigMapStore map[string]*corev1.ConfigMap
}

// NewResourceStore 返回一个指向新资源存储数据结构的指针。
func NewResourceStore() *ResourceStore {
	return &ResourceStore{
		SecretStore:    make(map[string]*corev1.Secret),
		ConfigMapStore: make(map[string]*corev1.ConfigMap),
	}
}

// getSecretRefValue 返回指定命名空间中 secret 的值
func getSecretRefValue(client kubernetes.Interface, namespace string, store *ResourceStore, secretSelector *corev1.SecretKeySelector) (string, error) {
	secret, ok := store.SecretStore[secretSelector.Name]
	if !ok {
		var err error
		secret, err = client.CoreV1().Secrets(namespace).Get(context.TODO(), secretSelector.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		store.SecretStore[secretSelector.Name] = secret
	}
	if data, ok := secret.Data[secretSelector.Key]; ok {
		return string(data), nil
	}
	return "", fmt.Errorf("key %s not found in secret %s", secretSelector.Key, secretSelector.Name)

}

// getConfigMapRefValue 返回指定命名空间中 configmap 的值
func getConfigMapRefValue(client kubernetes.Interface, namespace string, store *ResourceStore, configMapSelector *corev1.ConfigMapKeySelector) (string, error) {
	configMap, ok := store.ConfigMapStore[configMapSelector.Name]
	if !ok {
		var err error
		configMap, err = client.CoreV1().ConfigMaps(namespace).Get(context.TODO(), configMapSelector.Name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		store.ConfigMapStore[configMapSelector.Name] = configMap
	}
	if data, ok := configMap.Data[configMapSelector.Key]; ok {
		return string(data), nil
	}
	return "", fmt.Errorf("key %s not found in config map %s", configMapSelector.Key, configMapSelector.Name)
}

// getFieldRef 返回给定对象中指定路径的值
func getFieldRef(obj runtime.Object, from *corev1.EnvVarSource) (string, error) {
	return extractFieldPathAsString(obj, from.FieldRef.FieldPath)
}

// extractFieldPathAsString 从给定对象中提取字段，
// 并以字符串形式返回。该对象必须是指向
// API 类型的指针。
func extractFieldPathAsString(obj interface{}, fieldPath string) (string, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", nil
	}

	if path, subscript, ok := splitMaybeSubscriptedPath(fieldPath); ok {
		switch path {
		case "metadata.annotations":
			if errs := validation.IsQualifiedName(strings.ToLower(subscript)); len(errs) != 0 {
				return "", fmt.Errorf("invalid key subscript in %s: %s", fieldPath, strings.Join(errs, ";"))
			}
			return accessor.GetAnnotations()[subscript], nil
		case "metadata.labels":
			if errs := validation.IsQualifiedName(subscript); len(errs) != 0 {
				return "", fmt.Errorf("invalid key subscript in %s: %s", fieldPath, strings.Join(errs, ";"))
			}
			return accessor.GetLabels()[subscript], nil
		default:
			return "", fmt.Errorf("fieldPath %q does not support subscript", fieldPath)
		}
	}

	switch fieldPath {
	case "metadata.annotations":
		return formatMap(accessor.GetAnnotations()), nil
	case "metadata.labels":
		return formatMap(accessor.GetLabels()), nil
	case "metadata.name":
		return accessor.GetName(), nil
	case "metadata.namespace":
		return accessor.GetNamespace(), nil
	case "metadata.uid":
		return string(accessor.GetUID()), nil
	}

	return "", fmt.Errorf("unsupported fieldPath: %v", fieldPath)
}

// splitMaybeSubscriptedPath 检查指定的 fieldPath 是否
// 带有下标，并且
//   - 如果是，该函数将 fieldPath 拆分为 path 和 subscript，并返回
//     (path, subscript, true)。
//   - 如果不是，则返回 (fieldPath, "", false)。
//
// 输入与输出示例：
//   - "metadata.annotations['myKey']" --> ("metadata.annotations", "myKey", true)
//   - "metadata.annotations['a[b]c']" --> ("metadata.annotations", "a[b]c", true)
//   - "metadata.labels[”]"           --> ("metadata.labels", "", true)
//   - "metadata.labels"               --> ("metadata.labels", "", false)
func splitMaybeSubscriptedPath(fieldPath string) (string, string, bool) {
	if !strings.HasSuffix(fieldPath, "']") {
		return fieldPath, "", false
	}
	s := strings.TrimSuffix(fieldPath, "']")
	parts := strings.SplitN(s, "['", 2)
	if len(parts) < 2 {
		return fieldPath, "", false
	}
	if len(parts[0]) == 0 {
		return fieldPath, "", false
	}
	return parts[0], parts[1], true
}

// formatMap 将 map[string]string 格式化为字符串。
func formatMap(m map[string]string) (fmtStr string) {
	// 按 key 排序输出，以提供稳定的结果
	keys := sets.New[string]()
	for key := range m {
		keys.Insert(key)
	}
	for _, key := range sets.List(keys) {
		fmtStr += fmt.Sprintf("%v=%q\n", key, m[key])
	}
	fmtStr = strings.TrimSuffix(fmtStr, "\n")

	return
}

// getResourceFieldRef 返回给定容器中某项资源的值
func getResourceFieldRef(from *corev1.EnvVarSource, container *corev1.Container) (string, error) {
	return extractContainerResourceValue(from.ResourceFieldRef, container)
}

// ExtractContainerResourceValue 从已知容器中
// 提取某项资源的值
func extractContainerResourceValue(fs *corev1.ResourceFieldSelector, container *corev1.Container) (string, error) {
	divisor := resource.Quantity{}
	if divisor.Cmp(fs.Divisor) == 0 {
		divisor = resource.MustParse("1")
	} else {
		divisor = fs.Divisor
	}

	switch fs.Resource {
	case "limits.cpu":
		return convertResourceCPUToString(container.Resources.Limits.Cpu(), divisor)
	case "limits.memory":
		return convertResourceMemoryToString(container.Resources.Limits.Memory(), divisor)
	case "limits.ephemeral-storage":
		return convertResourceEphemeralStorageToString(container.Resources.Limits.StorageEphemeral(), divisor)
	case "requests.cpu":
		return convertResourceCPUToString(container.Resources.Requests.Cpu(), divisor)
	case "requests.memory":
		return convertResourceMemoryToString(container.Resources.Requests.Memory(), divisor)
	case "requests.ephemeral-storage":
		return convertResourceEphemeralStorageToString(container.Resources.Requests.StorageEphemeral(), divisor)
	}
	// 处理具有动态名称的扩展标准资源
	// 例如：requests.hugepages-<pageSize> 或 limits.hugepages-<pageSize>
	if strings.HasPrefix(fs.Resource, "requests.") {
		resourceName := corev1.ResourceName(strings.TrimPrefix(fs.Resource, "requests."))
		if IsHugePageResourceName(resourceName) {
			return convertResourceHugePagesToString(container.Resources.Requests.Name(resourceName, resource.BinarySI), divisor)
		}
	}
	if strings.HasPrefix(fs.Resource, "limits.") {
		resourceName := corev1.ResourceName(strings.TrimPrefix(fs.Resource, "limits."))
		if IsHugePageResourceName(resourceName) {
			return convertResourceHugePagesToString(container.Resources.Limits.Name(resourceName, resource.BinarySI), divisor)
		}
	}
	return "", fmt.Errorf("Unsupported container resource : %v", fs.Resource)
}

// convertResourceCPUToString 将 cpu 值转换为 divisor 的格式，并返回
// 该值的向上取整结果。
func convertResourceCPUToString(cpu *resource.Quantity, divisor resource.Quantity) (string, error) {
	c := int64(math.Ceil(float64(cpu.MilliValue()) / float64(divisor.MilliValue())))
	return strconv.FormatInt(c, 10), nil
}

// convertResourceMemoryToString 将 memory 值转换为 divisor 的格式，并返回
// 该值的向上取整结果。
func convertResourceMemoryToString(memory *resource.Quantity, divisor resource.Quantity) (string, error) {
	m := int64(math.Ceil(float64(memory.Value()) / float64(divisor.Value())))
	return strconv.FormatInt(m, 10), nil
}

// convertResourceHugePagesToString 将 hugepages 值转换为 divisor 的格式，并返回
// 该值的向上取整结果。
func convertResourceHugePagesToString(hugePages *resource.Quantity, divisor resource.Quantity) (string, error) {
	m := int64(math.Ceil(float64(hugePages.Value()) / float64(divisor.Value())))
	return strconv.FormatInt(m, 10), nil
}

// convertResourceEphemeralStorageToString 将 ephemeral storage 值转换为 divisor 的格式，并返回
// 该值的向上取整结果。
func convertResourceEphemeralStorageToString(ephemeralStorage *resource.Quantity, divisor resource.Quantity) (string, error) {
	m := int64(math.Ceil(float64(ephemeralStorage.Value()) / float64(divisor.Value())))
	return strconv.FormatInt(m, 10), nil
}

// GetEnvVarRefValue 根据其他提供的信息，返回给定 EnvVarSource 所引用的值。
func GetEnvVarRefValue(kc kubernetes.Interface, ns string, store *ResourceStore, from *corev1.EnvVarSource, obj runtime.Object, c *corev1.Container) (string, error) {
	if from.SecretKeyRef != nil {
		return getSecretRefValue(kc, ns, store, from.SecretKeyRef)
	}

	if from.ConfigMapKeyRef != nil {
		return getConfigMapRefValue(kc, ns, store, from.ConfigMapKeyRef)
	}

	if from.FieldRef != nil {
		return getFieldRef(obj, from)
	}

	if from.ResourceFieldRef != nil {
		return getResourceFieldRef(from, c)
	}

	return "", fmt.Errorf("invalid valueFrom")
}

// GetEnvVarRefString 返回给定 EnvVarSource 参数中所设置字段的文本描述。
func GetEnvVarRefString(from *corev1.EnvVarSource) string {
	if from.ConfigMapKeyRef != nil {
		return fmt.Sprintf("configmap %s, key %s", from.ConfigMapKeyRef.Name, from.ConfigMapKeyRef.Key)
	}

	if from.SecretKeyRef != nil {
		return fmt.Sprintf("secret %s, key %s", from.SecretKeyRef.Name, from.SecretKeyRef.Key)
	}

	if from.FieldRef != nil {
		return fmt.Sprintf("field path %s", from.FieldRef.FieldPath)
	}

	if from.ResourceFieldRef != nil {
		containerPrefix := ""
		if from.ResourceFieldRef.ContainerName != "" {
			containerPrefix = fmt.Sprintf("%s/", from.ResourceFieldRef.ContainerName)
		}
		return fmt.Sprintf("resource field %s%s", containerPrefix, from.ResourceFieldRef.Resource)
	}

	return "invalid valueFrom"
}

// 如果资源名称带有 huge page
// 资源前缀，IsHugePageResourceName 返回 true。
func IsHugePageResourceName(name corev1.ResourceName) bool {
	return strings.HasPrefix(string(name), corev1.ResourceHugePagesPrefix)
}
