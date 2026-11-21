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

package v1beta1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

// addConversionFuncs 为 Job、JobTemplate、CronJob 注册字段标签转换函数。
// 这些类型可选择字段很少，只允许 metadata 字段和 status.successful。
func addConversionFuncs(scheme *runtime.Scheme) error {
	var err error
	// 为仅支持 ObjectMeta 字段选择的 Kind 注册字段标签转换。
	for _, kind := range []string{"Job", "JobTemplate", "CronJob"} { // don't close over range variables
		err = scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind(kind),
			func(label, value string) (string, string, error) {
				switch label {
				case "metadata.name", "metadata.namespace", "status.successful":
					return label, value, nil
				default:
					return "", "", fmt.Errorf("field label %q not supported for %q", label, kind)
				}
			})
		if err != nil {
			return err
		}
	}
	return nil
}
