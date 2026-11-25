//go:build !linux

/*
Copyright 2022 The Kubernetes Authors.

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

package preflight

import (
	system "k8s.io/system-validators/validators"
)

// addOSValidator 添加一个新的 OSValidator。
// 在 Darwin（macOS）和 Windows 上为空操作。
func addOSValidator(validators []system.Validator, _ *system.StreamReporter, _ string) []system.Validator {
	return validators
}

// addIPv6Checks 添加 IPv6 相关检查。
// 在 Darwin（macOS）和 Windows 上为空操作。
func addIPv6Checks(checks []Checker) []Checker {
	return checks
}

// addIPv4Checks 添加 IPv4 相关检查。
// 在 Darwin（macOS）和 Windows 上为空操作。
func addIPv4Checks(checks []Checker) []Checker {
	return checks
}

// addSwapCheck 添加 swap 检查。
// 在 Darwin（macOS）和 Windows 上为空操作。
func addSwapCheck(checks []Checker) []Checker {
	return checks
}
