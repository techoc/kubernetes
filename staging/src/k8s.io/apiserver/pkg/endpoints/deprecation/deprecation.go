/*
Copyright 2020 The Kubernetes Authors.

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

// Package deprecation 根据对象实现的 APILifecycle* 接口判断 API 版本是否已弃用、
// 何时移除以及推荐的替代版本，并生成对应的警告文本。
package deprecation

import (
	"fmt"
	"regexp"
	"strconv"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
)

// apiLifecycleDeprecated 由生成的类型实现，返回弃用起始版本。
type apiLifecycleDeprecated interface {
	APILifecycleDeprecated() (major, minor int)
}

// apiLifecycleRemoved 由生成的类型实现，返回移除版本。
type apiLifecycleRemoved interface {
	APILifecycleRemoved() (major, minor int)
}

// apiLifecycleReplacement 由生成的类型实现，返回替代的 GVK。
type apiLifecycleReplacement interface {
	APILifecycleReplacement() schema.GroupVersionKind
}

// extract all digits at the beginning of the string
var leadingDigits = regexp.MustCompile(`^(\d+)`)

// MajorMinor parses a numeric major/minor version from the provided version info.
// The minor version drops all characters after the first non-digit character:
//
//	version.Info{Major:"1", Minor:"2+"} -> 1,2
//	version.Info{Major:"1", Minor:"2.3-build4"} -> 1,2
func MajorMinor(v version.Info) (int, int, error) {
	major, err := strconv.Atoi(v.Major)
	if err != nil {
		return 0, 0, err
	}
	// Minor 可能带 "+" 或 "-build" 等后缀，只取开头的数字部分。
	minor, err := strconv.Atoi(leadingDigits.FindString(v.Minor))
	if err != nil {
		return 0, 0, err
	}
	return major, minor, nil
}

// IsDeprecated returns true if obj implements APILifecycleDeprecated() and returns
// a major/minor version that is non-zero and is <= the specified current major/minor version.
func IsDeprecated(obj runtime.Object, currentMajor, currentMinor int) bool {
	deprecated, isDeprecated := obj.(apiLifecycleDeprecated)
	if !isDeprecated {
		return false
	}

	deprecatedMajor, deprecatedMinor := deprecated.APILifecycleDeprecated()
	// no deprecation version expressed
	if deprecatedMajor == 0 && deprecatedMinor == 0 {
		return false
	}
	// no current version info available
	// 无法确定当前版本时保守地视为已弃用，以便尽早提示用户。
	if currentMajor == 0 && currentMinor == 0 {
		return true
	}
	// compare deprecation version to current version
	if deprecatedMajor > currentMajor {
		return false
	}
	if deprecatedMajor == currentMajor && deprecatedMinor > currentMinor {
		return false
	}
	return true
}

// RemovedRelease returns the major/minor version in which the given object is unavailable (in the form "<major>.<minor>")
// if the object implements APILifecycleRemoved() to indicate a non-zero removal version, and returns an empty string otherwise.
func RemovedRelease(obj runtime.Object) string {
	if removed, hasRemovalInfo := obj.(apiLifecycleRemoved); hasRemovalInfo {
		removedMajor, removedMinor := removed.APILifecycleRemoved()
		if removedMajor != 0 || removedMinor != 0 {
			return fmt.Sprintf("%d.%d", removedMajor, removedMinor)
		}
	}
	return ""
}

// WarningMessage returns a human-readable deprecation warning if the object implements APILifecycleDeprecated()
// to indicate a non-zero deprecated major/minor version and has a populated GetObjectKind().GroupVersionKind().
//
// 缺少 GVK 时返回空字符串：警告文本需要明确是哪个组版本的哪个 Kind 被弃用，
// 信息不完整时宁可不发警告，也不要输出令人困惑的消息。
func WarningMessage(obj runtime.Object) string {
	deprecated, isDeprecated := obj.(apiLifecycleDeprecated)
	if !isDeprecated {
		return ""
	}

	deprecatedMajor, deprecatedMinor := deprecated.APILifecycleDeprecated()
	if deprecatedMajor == 0 && deprecatedMinor == 0 {
		return ""
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Empty() {
		return ""
	}
	deprecationWarning := fmt.Sprintf("%s %s is deprecated in v%d.%d+", gvk.GroupVersion().String(), gvk.Kind, deprecatedMajor, deprecatedMinor)

	if removed, hasRemovalInfo := obj.(apiLifecycleRemoved); hasRemovalInfo {
		removedMajor, removedMinor := removed.APILifecycleRemoved()
		if removedMajor != 0 || removedMinor != 0 {
			deprecationWarning = deprecationWarning + fmt.Sprintf(", unavailable in v%d.%d+", removedMajor, removedMinor)
		}
	}

	if replaced, hasReplacement := obj.(apiLifecycleReplacement); hasReplacement {
		replacement := replaced.APILifecycleReplacement()
		if !replacement.Empty() {
			deprecationWarning = deprecationWarning + fmt.Sprintf("; use %s %s", replacement.GroupVersion().String(), replacement.Kind)
		}
	}

	return deprecationWarning
}
