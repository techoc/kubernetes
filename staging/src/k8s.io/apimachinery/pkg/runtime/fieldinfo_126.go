//go:build !go1.27

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

package runtime

import "reflect"

// isInlinedFromTag 在 Go 1.27 之前的版本中恒返回 false，用于屏蔽新版本才支持的
// ",inline" tag 语义，保证同一套 fieldinfo 逻辑在各 Go 版本下行为一致。
func isInlinedFromTag(fieldType reflect.StructField, tagName string, tagDirectives []string) bool {
	// Go 1.27 之前不识别 ",inline"。
	return false
}
