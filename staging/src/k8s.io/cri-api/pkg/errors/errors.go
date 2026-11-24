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
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrRegistryUnavailable 表示调用 PullImage（拉取镜像）时与镜像仓库通信发生了 HTTP 层错误，
	// 例如仓库不可达、认证失败或返回了非成功的 HTTP 状态码。
	ErrRegistryUnavailable = errors.New("RegistryUnavailable")

	// ErrSignatureValidationFailed 表示在 PullImage 过程中无法校验镜像签名，
	// 通常由镜像签名策略配置或签名数据本身的问题引起。
	ErrSignatureValidationFailed = errors.New("SignatureValidationFailed")

	// ErrRROUnsupported 表示底层运行时无法支持递归只读挂载（Recursive Read-Only mount），
	// 即无法将整个目录树以只读方式挂载进容器。
	ErrRROUnsupported = errors.New("RROUnsupported")

	// ErrImageVolumeMountFailed 表示挂载镜像卷（image volume）失败，
	// 即无法把镜像中的文件或目录作为卷挂载到容器里。
	ErrImageVolumeMountFailed = errors.New("ImageVolumeMountFailed")
)

// IsNotFound 判断给定的错误是否为 gRPC 的 NotFound（资源不存在）错误。
// gRPC 状态码参见 https://github.com/grpc/grpc/blob/master/doc/statuscodes.md。
// 处理流程：
//  1. 调用 status.FromError 尝试把普通 error 转换为 gRPC status；
//  2. 若转换失败（说明不是 gRPC 错误），直接返回 false；
//  3. 若转换成功，检查其状态码是否等于 codes.NotFound。
//
// kubelet 用它区分"容器/沙箱确实不存在"与其他类型的失败，从而决定是否需要重新同步状态。
func IsNotFound(err error) bool {
	s, ok := status.FromError(err)
	// 非 gRPC 错误（例如本地普通 error）不属于 NotFound。
	if !ok {
		return ok
	}
	// 只有状态码明确为 NotFound 时才返回 true。
	if s.Code() == codes.NotFound {
		return true
	}

	// 其余 gRPC 状态码（如 Internal、Unavailable 等）均返回 false。
	return false
}
