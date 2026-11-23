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

package errors

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestErrorIsNotFound 验证 IsNotFound 能正确识别 gRPC NotFound 错误。
// 构造一个 codes.NotFound 状态的 gRPC 错误，期望 IsNotFound 返回 true。
func TestErrorIsNotFound(t *testing.T) {
	enf := status.Errorf(codes.NotFound, "container not found")
	// 正向用例：NotFound 错误应当通过检查。
	if !IsNotFound(enf) {
		t.Errorf("%v expected to pass not found check", enf)
	}
}

// TestSimpleErrorDoesNotTriggerNotFound 验证普通（非 gRPC）错误不会被误判为 NotFound。
// status.FromError 对这类错误会返回 ok=false，IsNotFound 应当返回 false。
func TestSimpleErrorDoesNotTriggerNotFound(t *testing.T) {
	err := fmt.Errorf("Some random error")
	// 反向用例：普通 error 不应被当成 NotFound。
	if IsNotFound(err) {
		t.Errorf("%v unexpectedly passed not found check", err)
	}
}

// TestOtherGrpcErrorDoesNotTriggerNotFound 验证非 NotFound 的 gRPC 错误不会被误判。
// 这里使用 DeadlineExceeded（超时）作为对照，确保 IsNotFound 只认 NotFound 这一种状态码。
func TestOtherGrpcErrorDoesNotTriggerNotFound(t *testing.T) {
	gerr := status.Errorf(codes.DeadlineExceeded, "timed out")
	// 反向用例：其他 gRPC 状态码不应被当成 NotFound。
	if IsNotFound(gerr) {
		t.Errorf("%v unexpectedly passed not found check", gerr)
	}
}
