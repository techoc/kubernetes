//go:build !coverage

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

package coverage

// InitCoverage 在未启用覆盖率检测时调用属于非法操作，直接 panic 以尽早暴露误用。
func InitCoverage(name string) {
	panic("Called InitCoverage when not built with coverage instrumentation.")
}

// FlushCoverage 在未启用覆盖率检测时为空操作。
func FlushCoverage() {

}
