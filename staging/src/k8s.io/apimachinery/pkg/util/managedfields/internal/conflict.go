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

package internal

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/merge"
)

// NewConflictError 构造包含 apply 冲突详情的错误：
// 每个冲突路径生成一条 FieldManagerConflict 类型的 StatusCause。
func NewConflictError(conflicts merge.Conflicts) *errors.StatusError {
	causes := []metav1.StatusCause{}
	for _, conflict := range conflicts {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldManagerConflict,
			Message: fmt.Sprintf("conflict with %v", printManager(conflict.Manager)),
			Field:   conflict.Path.String(),
		})
	}
	return errors.NewApplyConflict(causes, getConflictMessage(conflicts))
}

// getConflictMessage 生成人类可读的冲突摘要：
// 单个冲突直接给出 manager 与路径；多个冲突按 manager 排序后分组列出，保证输出稳定。
func getConflictMessage(conflicts merge.Conflicts) string {
	if len(conflicts) == 1 {
		return fmt.Sprintf("Apply failed with 1 conflict: conflict with %v: %v", printManager(conflicts[0].Manager), conflicts[0].Path)
	}

	m := map[string][]fieldpath.Path{}
	for _, conflict := range conflicts {
		m[conflict.Manager] = append(m[conflict.Manager], conflict.Path)
	}

	uniqueManagers := []string{}
	for manager := range m {
		uniqueManagers = append(uniqueManagers, manager)
	}

	// 按排序后的 manager 输出冲突，避免 map 遍历顺序导致信息不稳定。
	sort.Strings(uniqueManagers)

	messages := []string{}
	for _, manager := range uniqueManagers {
		messages = append(messages, fmt.Sprintf("conflicts with %v:", printManager(manager)))
		for _, path := range m[manager] {
			messages = append(messages, fmt.Sprintf("- %v", path))
		}
	}
	return fmt.Sprintf("Apply failed with %d conflicts: %s", len(conflicts), strings.Join(messages, "\n"))
}

// printManager 把内部编码的 manager 字符串（JSON 形式的 ManagedFieldsEntry）转换为可读描述。
// 解码失败时按普通字符串加引号输出，保证错误信息总能生成。
func printManager(manager string) string {
	encodedManager := &metav1.ManagedFieldsEntry{}
	if err := json.Unmarshal([]byte(manager), encodedManager); err != nil {
		return fmt.Sprintf("%q", manager)
	}
	managerStr := fmt.Sprintf("%q", encodedManager.Manager)
	if encodedManager.Subresource != "" {
		managerStr = fmt.Sprintf("%s with subresource %q", managerStr, encodedManager.Subresource)
	}
	if encodedManager.Operation == metav1.ManagedFieldsOperationUpdate {
		// update 类条目补充 apiVersion 与时间，便于定位是何时由谁写入的。
		if encodedManager.Time == nil {
			return fmt.Sprintf("%s using %v", managerStr, encodedManager.APIVersion)
		}
		return fmt.Sprintf("%s using %v at %v", managerStr, encodedManager.APIVersion, encodedManager.Time.UTC().Format(time.RFC3339))
	}
	return managerStr
}
