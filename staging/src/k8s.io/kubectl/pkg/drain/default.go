/*
Copyright 2019 The Kubernetes Authors.

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

package drain

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// 本文件包含如何 drain/cordon/uncordon 节点的
// 默认实现。这些函数可以直接调用，或者将其功能
// 复制到你自己的代码中，例如当你想要
// 不同的输出行为时。

// RunNodeDrain 展示 drain 节点的规范方式。
// 你应首先 cordon 该节点，例如使用 RunCordonOrUncordon
func RunNodeDrain(drainer *Helper, nodeName string) error {
	// TODO(justinsb): 确保库使用者对该函数有足够的 e2e 覆盖
	list, errs := drainer.GetPodsForDeletion(nodeName)
	if errs != nil {
		return utilerrors.NewAggregate(errs)
	}
	if warnings := list.Warnings(); warnings != "" {
		fmt.Fprintf(drainer.ErrOut, "WARNING: %s\n", warnings)
	}

	if err := drainer.DeleteOrEvictPods(list.Pods()); err != nil {
		// 可以在此处对未删除的 pod 发出警告
		return err
	}
	return nil
}

// RunCordonOrUncordon 展示 cordon 或 uncordon 节点的规范方式
func RunCordonOrUncordon(drainer *Helper, node *corev1.Node, desired bool) error {
	if drainer.Ctx == nil {
		return fmt.Errorf("RunCordonOrUncordon error: drainer.Ctx can't be nil")
	}
	if drainer.Client == nil {
		return fmt.Errorf("RunCordonOrUncordon error: drainer.Client can't be nil")
	}
	// TODO(justinsb): 确保库使用者对该函数有足够的 e2e 覆盖
	c := NewCordonHelper(node)

	if updateRequired := c.UpdateIfRequired(desired); !updateRequired {
		// 无需改动
		return nil
	}

	err, patchErr := c.PatchOrReplaceWithContext(drainer.Ctx, drainer.Client, false)
	if err != nil {
		if patchErr != nil {
			return fmt.Errorf("cordon error: %w; merge patch error: %w", err, patchErr)
		}
		return fmt.Errorf("cordon error: %w", err)
	}

	return nil
}
