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

package responsewriters

import (
	"fmt"
	"net/http"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// Avoid emitting errors that look like valid HTML. Quotes are okay.
//
// 错误信息里可能包含用户可控的请求路径/资源名，若不转义，浏览器在
// 直接访问 API 时会把它当作 HTML 渲染，形成反射型 XSS。
var sanitizer = strings.NewReplacer(`&`, "&amp;", `<`, "&lt;", `>`, "&gt;")

// Forbidden renders a simple forbidden error
func Forbidden(attributes authorizer.Attributes, w http.ResponseWriter, req *http.Request, reason string, s runtime.NegotiatedSerializer) {
	RespondWithError(w, req, ForbiddenStatusError(attributes, reason), s)
}

// RespondWithError 以内容协商方式写出错误响应。
// nosniff 阻止浏览器根据内容猜测类型，进一步降低 XSS 风险。
func RespondWithError(w http.ResponseWriter, req *http.Request, err error, s runtime.NegotiatedSerializer) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	ErrorNegotiated(err, s, metav1.Unversioned, w, req)
}

// ForbiddenStatusError 构造 403 StatusError；reason 为空时只使用基础描述，
// 避免出现结尾多余的冒号。
func ForbiddenStatusError(attributes authorizer.Attributes, reason string) *apierrors.StatusError {
	msg := sanitizer.Replace(forbiddenMessage(attributes))

	var errMsg error
	if len(reason) == 0 {
		errMsg = fmt.Errorf("%s", msg)
	} else {
		errMsg = fmt.Errorf("%s: %s", msg, reason)
	}

	gr := schema.GroupResource{Group: attributes.GetAPIGroup(), Resource: attributes.GetResource()}

	return apierrors.NewForbidden(gr, attributes.GetName(), errMsg)
}

// forbiddenMessage 生成面向用户的拒绝说明：区分资源请求与非资源请求，
// 并分别给出命名空间级和集群级两种措辞。
func forbiddenMessage(attributes authorizer.Attributes) string {
	username := ""
	if user := attributes.GetUser(); user != nil {
		username = user.GetName()
	}

	if !attributes.IsResourceRequest() {
		return fmt.Sprintf("User %q cannot %s path %q", username, attributes.GetVerb(), attributes.GetPath())
	}

	resource := attributes.GetResource()
	if subresource := attributes.GetSubresource(); len(subresource) > 0 {
		resource = resource + "/" + subresource
	}

	if ns := attributes.GetNamespace(); len(ns) > 0 {
		return fmt.Sprintf("User %q cannot %s resource %q in API group %q in the namespace %q", username, attributes.GetVerb(), resource, attributes.GetAPIGroup(), ns)
	}

	return fmt.Sprintf("User %q cannot %s resource %q in API group %q at the cluster scope", username, attributes.GetVerb(), resource, attributes.GetAPIGroup())
}

// InternalError renders a simple internal error
//
// 同样经过 sanitizer 转义；同时把原始错误交给 utilruntime 记录，避免
// 内部细节只出现在响应体里。
func InternalError(w http.ResponseWriter, req *http.Request, err error) {
	http.Error(w, sanitizer.Replace(fmt.Sprintf("Internal Server Error: %q: %v", req.RequestURI, err)),
		http.StatusInternalServerError)
	utilruntime.HandleError(err)
}
