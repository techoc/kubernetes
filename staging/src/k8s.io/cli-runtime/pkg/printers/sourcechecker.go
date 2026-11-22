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

package printers

import (
	"strings"
)

var (
	// InternalObjectPrinterErr 是拒绝打印 internal 类型时返回的固定文案；
	// IsInternalObjectError 以字符串比较来识别它，所以文案不能随意改动。
	InternalObjectPrinterErr = "a versioned object must be passed to a printer"

	// disallowedPackagePrefixes contains regular expression templates
	// for object package paths that are not allowed by printers.
	// 中文：k8s.io/kubernetes/pkg/apis 下是未版本化的 internal 类型，直接打印会缺少 apiVersion/kind。
	disallowedPackagePrefixes = []string{
		"k8s.io/kubernetes/pkg/apis/",
	}
)

// InternalObjectPreventer 是共享的校验器，打印前统一检查对象来源。
var InternalObjectPreventer = &illegalPackageSourceChecker{disallowedPackagePrefixes}

// IsInternalObjectError 判断错误是否来自 internal 类型拦截，便于调用方区分处理。
func IsInternalObjectError(err error) bool {
	if err == nil {
		return false
	}

	return err.Error() == InternalObjectPrinterErr
}

// illegalPackageSourceChecker compares a given
// object's package path, and determines if the
// object originates from a disallowed source.
type illegalPackageSourceChecker struct {
	// disallowedPrefixes is a slice of disallowed package path
	// prefixes for a given runtime.Object that we are printing.
	disallowedPrefixes []string
}

func (c *illegalPackageSourceChecker) IsForbidden(pkgPath string) bool {
	for _, forbiddenPrefix := range c.disallowedPrefixes {
		// 同时匹配直接引用与经由 vendor 目录引入的路径，防止绕过检查。
		if strings.HasPrefix(pkgPath, forbiddenPrefix) || strings.Contains(pkgPath, "/vendor/"+forbiddenPrefix) {
			return true
		}
	}

	return false
}
