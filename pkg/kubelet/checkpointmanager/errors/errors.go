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

package errors

import "fmt"

// CorruptCheckpointError 在校验和不匹配时返回。
// 可用如下方式判断：
//
//	var csErr *CorruptCheckpointError
//	if errors.As(err, &csErr) { ... }
//	if errors.Is(err, CorruptCheckpointError{}) { ... }
type CorruptCheckpointError struct {
	ActualCS, ExpectedCS uint64
}

// Error 返回固定的损坏提示信息。
func (err CorruptCheckpointError) Error() string {
	return "checkpoint is corrupted"
}

// Is 使 CorruptCheckpointError 支持 errors.Is 的匹配。
func (err CorruptCheckpointError) Is(target error) bool {
	switch target.(type) {
	case *CorruptCheckpointError, CorruptCheckpointError:
		return true
	default:
		return false
	}
}

// ErrCheckpointNotFound 在按 key 找不到 checkpoint 时返回。
var ErrCheckpointNotFound = fmt.Errorf("checkpoint is not found")
