/*
Copyright The Kubernetes Authors.

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

	conversion "k8s.io/apimachinery/pkg/conversion"
	config "k8s.io/kubectl/pkg/config"
)

func Convert_config_AllowlistEntry_To_v1beta1_AllowlistEntry(in *config.AllowlistEntry, out *AllowlistEntry, s conversion.Scope) error {
	return autoConvert_config_AllowlistEntry_To_v1beta1_AllowlistEntry(in, out, s)
}

// 内部 AllowlistEntry 类型没有 `Name` 字段，该字段自 v1.36 起已弃用。在可能的情况下将 `Name` 转换为 `Command`，如果同时提供了 `Name` 和 `Command`，则返回错误。
func Convert_v1beta1_AllowlistEntry_To_config_AllowlistEntry(in *AllowlistEntry, out *config.AllowlistEntry, s conversion.Scope) error {
	if err := autoConvert_v1beta1_AllowlistEntry_To_config_AllowlistEntry(in, out, s); err != nil {
		return err
	}

	switch {
	case len(in.Name) != 0 && len(in.Command) != 0 && in.Name != in.Command:
		return fmt.Errorf("both `Name` and `Command` were supplied with different values. `Name` is deprecated, use `Command` instead")
	case len(in.Command) != 0:
		out.Command = in.Command
	case len(in.Name) != 0:
		out.Command = in.Name
	default:
		// `Name` 和 `Command` 都为空，传播该空值，
		// 并留待后续校验捕获，因为这属于校验错误，而非
		// 转换错误
	}
	return nil
}
