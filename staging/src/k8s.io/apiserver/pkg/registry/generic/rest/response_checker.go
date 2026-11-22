/*
Copyright 2014 The Kubernetes Authors.

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

package rest

import (
	"fmt"
	"io"
	"net/http"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Check the http error status from a location URL.
// And convert an error into a structured API object.
// Finally ensure we close the body before returning the error
//
// 中文说明：该接口用于「重定向/代理到外部地址」的存储场景（如某些读操作被转发到
// 其他 apiserver）。返回值必须是结构化 API 错误，而非裸 HTTP 错误，
// 否则调用方无法映射到正确的 StatusReason 与 HTTP 状态码。
type HttpResponseChecker interface {
	Check(resp *http.Response) error
}

// Max length read from the response body of a location which returns error status
//
// 中文说明：限制读取量既是防御性措施（避免恶意或异常服务端返回超大响应耗尽内存），
// 也保证错误信息不会大到污染日志与 API 响应；50KB 足以包含典型错误详情。
const (
	maxReadLength = 50000
)

// A generic http response checker to transform the error.
//
// 中文说明：QualifiedResource 与 Name 用于构造符合 Kubernetes 错误规范的
// StatusError，使客户端能识别资源类型与对象名，而不是只拿到一段文本。
type GenericHttpResponseChecker struct {
	QualifiedResource schema.GroupResource
	Name              string
}

// Check 检查响应状态码，非 2xx/206 时读取（截断后的）响应体并转换为 StatusError。
// 中文说明：关闭 body 放在 defer 中，确保任何错误分支都不会泄漏连接；
// 状态码分支显式处理 500/400/404 以保留最精确的错误原因，
// 其余情况统一走 NewGenericServerResponse，由它推断合适的 reason。
func (checker GenericHttpResponseChecker) Check(resp *http.Response) error {
	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPartialContent {
		defer resp.Body.Close()
		bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxReadLength))
		if err != nil {
			return errors.NewInternalError(err)
		}
		bodyText := string(bodyBytes)

		switch {
		case resp.StatusCode == http.StatusInternalServerError:
			return errors.NewInternalError(fmt.Errorf("%s", bodyText))
		case resp.StatusCode == http.StatusBadRequest:
			return errors.NewBadRequest(bodyText)
		case resp.StatusCode == http.StatusNotFound:
			return errors.NewGenericServerResponse(resp.StatusCode, "", checker.QualifiedResource, checker.Name, bodyText, 0, false)
		}
		return errors.NewGenericServerResponse(resp.StatusCode, "", checker.QualifiedResource, checker.Name, bodyText, 0, false)
	}
	return nil
}

// NewGenericHttpResponseChecker 构造默认检查器；资源与名称仅用于错误信息，
// 不影响成功路径的行为。
func NewGenericHttpResponseChecker(qualifiedResource schema.GroupResource, name string) GenericHttpResponseChecker {
	return GenericHttpResponseChecker{QualifiedResource: qualifiedResource, Name: name}
}
