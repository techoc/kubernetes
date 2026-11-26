/*
Copyright 2024 The Kubernetes Authors.

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

package benchmark

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/utils/ptr"
)

// updateAny 定义从 YAML 文件读取并更新对象的操作，可指定要使用的 nameset。
type updateAny struct {
	// 必须与 updateAny 的操作码一致。
	Opcode operationCode
	// 目标对象所在的 namespace；集群级资源必须留空。
	Namespace string
	// 描述待更新对象的文件，会经 text/template 处理。
	// 更新多个对象时，.Index 取值 [0, Count-1]，.Count 为对象总数。
	TemplatePath string
	// 要更新的对象数量，未设置时默认为 1。
	Count *int
	// Count 对应的模板参数名。
	CountParam string
	// 每秒更新的对象数；为 0 表示一次性全部更新。可选。
	UpdatePerSecond int
	// 内部缓存字段，用于保存 RESTMapping。
	cachedMapping *meta.RESTMapping
	// 要更新的子资源列表；为空时更新主资源。可选。
	Subresources []string
}

var _ runnableOp = &updateAny{}

func (c *updateAny) isValid(allowParameterization bool) error {
	if c.TemplatePath == "" {
		return fmt.Errorf("TemplatePath must be set")
	}
	if c.UpdatePerSecond < 0 {
		return fmt.Errorf("invalid UpdatePerSecond=%d; should be non-negative", c.UpdatePerSecond)
	}
	// namespace 的合法性只能在读取对象后校验，此时还不知道对象是命名空间级还是集群级。
	return nil
}

func (c *updateAny) collectsMetrics() bool {
	return false
}

func (c updateAny) patchParams(w *Workload) (realOp, error) {
	if c.CountParam != "" {
		count, err := w.Params.get(c.CountParam[1:])
		if err != nil {
			return nil, err
		}
		c.Count = ptr.To(count)
	}
	c.cachedMapping = nil
	return &c, c.isValid(false)
}

func (c *updateAny) requiredNamespaces() []string {
	if c.Namespace == "" {
		return nil
	}
	return []string{c.Namespace}
}

func (c *updateAny) run(tCtx ktesting.TContext) {
	count := 1
	if c.Count != nil {
		count = *c.Count
	}

	if c.UpdatePerSecond == 0 {
		for index := 0; index < count; index++ {
			err := c.update(tCtx, map[string]any{"Index": index, "Count": count})
			if err != nil {
				tCtx.Fatalf("Failed to update object: %v", err)
			}
		}
		return
	}

	ticker := time.NewTicker(time.Second / time.Duration(c.UpdatePerSecond))
	defer ticker.Stop()
	for index := 0; index < count; index++ {
		select {
		case <-ticker.C:
			err := c.update(tCtx, map[string]any{"Index": index, "Count": count})
			if err != nil {
				tCtx.Fatalf("Failed to update object: %v", err)
			}
		case <-tCtx.Done():
			return
		}
	}
}

func (c *updateAny) update(tCtx ktesting.TContext, env map[string]any) error {
	var obj *unstructured.Unstructured
	if _, err := getSpecFromTextTemplateFile(c.TemplatePath, env, &obj); err != nil {
		return fmt.Errorf("%s: parsing failed: %w", c.TemplatePath, err)
	}

	if c.cachedMapping == nil {
		mapping, err := restMappingFromUnstructuredObj(tCtx, obj)
		if err != nil {
			return err
		}
		c.cachedMapping = mapping
	}
	resourceClient := tCtx.Dynamic().Resource(c.cachedMapping.Resource)

	options := metav1.UpdateOptions{
		// 如果 YAML 输入无效，
		// 我们希望 apiserver 通过错误告知我们。
		// 之所以会这样，是因为解码到 unstructured 对象时
		// 不会进行校验。
		FieldValidation: "Strict",
	}
	if c.Namespace != "" {
		if c.cachedMapping.Scope.Name() != meta.RESTScopeNameNamespace {
			return fmt.Errorf("namespace %q set for %q, but %q has scope %q", c.Namespace, c.TemplatePath, c.cachedMapping.GroupVersionKind, c.cachedMapping.Scope.Name())
		}
		_, err := resourceClient.Namespace(c.Namespace).Update(tCtx, obj, options, c.Subresources...)
		if err != nil {
			return fmt.Errorf("failed to update object in namespace %q: %w", c.Namespace, err)
		}
		_, err = resourceClient.Namespace(c.Namespace).UpdateStatus(tCtx, obj, options)
		if err != nil {
			return fmt.Errorf("failed to update object status in namespace %q: %w", c.Namespace, err)
		}
		return nil
	}
	if c.cachedMapping.Scope.Name() != meta.RESTScopeNameRoot {
		return fmt.Errorf("namespace not set for %q, but %q has scope %q", c.TemplatePath, c.cachedMapping.GroupVersionKind, c.cachedMapping.Scope.Name())
	}
	_, err := resourceClient.Update(tCtx, obj, options, c.Subresources...)
	if err != nil {
		return fmt.Errorf("failed to update object: %w", err)
	}
	_, err = resourceClient.UpdateStatus(tCtx, obj, options)
	if err != nil {
		return fmt.Errorf("failed to update object status: %w", err)
	}
	return nil
}
