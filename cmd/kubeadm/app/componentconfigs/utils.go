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

package componentconfigs

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// UnsupportedConfigVersionError 是遇到过旧配置版本时返回的特殊错误类型。
type UnsupportedConfigVersionError struct {
	// OldVersion 是导致问题的配置版本。
	OldVersion schema.GroupVersion

	// CurrentVersion 是原生支持的配置版本。
	CurrentVersion schema.GroupVersion

	// Document 指向导致问题的 YAML/JSON 文档。
	Document []byte
}

// Error 为 UnsupportedConfigVersionError 实现标准 Go error 接口。
func (err *UnsupportedConfigVersionError) Error() string {
	return fmt.Sprintf("unsupported apiVersion %q, you may have to do manual conversion to %q and run kubeadm again", err.OldVersion, err.CurrentVersion)
}

// UnsupportedConfigVersionsErrorMap 汇总多个 UnsupportedConfigVersionError。
type UnsupportedConfigVersionsErrorMap map[string]*UnsupportedConfigVersionError

// Error 为 UnsupportedConfigVersionsErrorMap 实现标准 Go error 接口。
func (errs UnsupportedConfigVersionsErrorMap) Error() string {
	// 按相关组名排序，确保输出的错误消息顺序稳定。
	groups := make([]string, 0, len(errs))
	for group := range errs {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	msgs := make([]string, 1, 1+len(errs))
	msgs[0] = "multiple unsupported config version errors encountered:"
	for _, group := range groups {
		msgs = append(msgs, errs[group].Error())
	}

	return strings.Join(msgs, "\n\t- ")
}

// warnDefaultComponentConfigValue 在用户将 ComponentConfig 中的字段改为非 kubeadm 推荐默认值时输出警告。
func warnDefaultComponentConfigValue(componentConfigKind, paramName string, defaultValue, userValue interface{}) {
	klog.Warningf("The recommended value for %q in %q is: %v; the provided value is: %v",
		paramName, componentConfigKind, defaultValue, userValue)
}
