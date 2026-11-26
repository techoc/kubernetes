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

package util

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 按 RFC3339Nano 或 RFC3339 格式解析日期。
func ParseRFC3339(s string, nowFn func() metav1.Time) (metav1.Time, error) {
	if t, timeErr := time.Parse(time.RFC3339Nano, s); timeErr == nil {
		return metav1.Time{Time: t}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return metav1.Time{}, err
	}
	return metav1.Time{Time: t}, nil
}

// 解析给定的 source。
//
// 可接受格式包括：
// 1. source-path：以文件名作为键名；
// 2. source-name=source-path：以 source-name 为键名，
// source-path 是键文件路径。
//
// 键名不能包含 '='。
func ParseFileSource(source string) (keyName, filePath string, err error) {
	numSeparators := strings.Count(source, "=")
	switch {
	case numSeparators == 0:
		return path.Base(filepath.ToSlash(source)), source, nil
	case numSeparators == 1 && strings.HasPrefix(source, "="):
		return "", "", fmt.Errorf("key name for file path %v missing", strings.TrimPrefix(source, "="))
	case numSeparators == 1 && strings.HasSuffix(source, "="):
		return "", "", fmt.Errorf("file path for key name %v missing", strings.TrimSuffix(source, "="))
	case numSeparators > 1:
		return "", "", errors.New("key names or file paths cannot contain '='")
	default:
		components := strings.Split(source, "=")
		return components[0], components[1], nil
	}
}

// 把 key=val 形式的 source 拆开。
// 与 strings.SplitN(source, "=", 2) 的区别在于：
// 键、值为空或缺少等号时返回错误。
func ParseLiteralSource(source string) (keyName, value string, err error) {
	// 开头的等号和号无效。
	if strings.Index(source, "=") == 0 {
		return "", "", fmt.Errorf("invalid literal source %v, expected key=value", source)
	}
	// 在第一个等号处拆分，因此值中可以含 =。
	items := strings.SplitN(source, "=", 2)
	if len(items) != 2 {
		return "", "", fmt.Errorf("invalid literal source %v, expected key=value", source)
	}

	return items[0], items[1], nil
}
