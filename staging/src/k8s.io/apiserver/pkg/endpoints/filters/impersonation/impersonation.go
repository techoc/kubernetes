/*
Copyright 2016 The Kubernetes Authors.

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

package impersonation

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/klog/v2"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/httplog"
)

// WithImpersonation 是一个过滤器，用于检查那些试图替换请求中 user.Info 的请求。
//
// 安全边界：每一个 Impersonate-* 头都映射为一次独立的授权检查，全部通过后才替换
// 请求中的用户。任何一项被拒绝都立即返回 Forbidden，避免部分生效的模拟身份。
func WithImpersonation(handler http.Handler, a authorizer.UnconditionalAuthorizer, s runtime.NegotiatedSerializer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		impersonationRequests, err := buildImpersonationRequests(req.Header)
		if err != nil {
			klog.V(4).Infof("%v", err)
			responsewriters.RespondWithError(w, req, err, s)
			return
		}
		// 无模拟请求时零开销直通。
		if len(impersonationRequests) == 0 {
			handler.ServeHTTP(w, req)
			return
		}

		ctx := req.Context()
		requestor, exists := request.UserFrom(ctx)
		if !exists {
			responsewriters.InternalError(w, req, errors.New("no user found for request"))
			return
		}

		// 如果未指定组，则需要根据用户类型用不同方式查出其所属组；
		// 如果指定了组，那么以指定内容为准（包括是否包含
		// system:authenticated/system:unauthenticated 组）。
		groupsSpecified := len(req.Header[authenticationv1.ImpersonateGroupHeader]) > 0

		// 逐个确认我们有权模拟所请求的每一项。遍历的同时开始构造用户名
		// 和组信息。
		username := ""
		groups := []string{}
		userExtra := map[string][]string{}
		uid := ""
		for _, impersonationRequest := range impersonationRequests {
			gvk := impersonationRequest.GetObjectKind().GroupVersionKind()
			// 以 "impersonate" 动词 + 目标资源构造授权属性，让 RBAC 可以精确到
			// serviceaccounts/users/groups/userextras/uids 及其子资源。
			actingAsAttributes := &authorizer.AttributesRecord{
				User:            requestor,
				Verb:            "impersonate",
				APIGroup:        gvk.Group,
				APIVersion:      gvk.Version,
				Namespace:       impersonationRequest.Namespace,
				Name:            impersonationRequest.Name,
				ResourceRequest: true,
			}

			switch gvk.GroupKind() {
			case v1.SchemeGroupVersion.WithKind("ServiceAccount").GroupKind():
				actingAsAttributes.Resource = "serviceaccounts"
				username = serviceaccount.MakeUsername(impersonationRequest.Namespace, impersonationRequest.Name)
				if !groupsSpecified {
					// 若未为服务账号指定组，我们可以推导出它所属的组，因为这是固定映射，直接补上。
					// ServiceAccount 的组是固定映射（system:serviceaccounts 等），无需请求方显式声明。
					groups = serviceaccount.MakeGroupNames(impersonationRequest.Namespace)
				}

			case v1.SchemeGroupVersion.WithKind("User").GroupKind():
				actingAsAttributes.Resource = "users"
				username = impersonationRequest.Name

			case v1.SchemeGroupVersion.WithKind("Group").GroupKind():
				actingAsAttributes.Resource = "groups"
				groups = append(groups, impersonationRequest.Name)

			case authenticationv1.SchemeGroupVersion.WithKind("UserExtra").GroupKind():
				// userextras 的键作为子资源参与授权，因此可以只允许模拟特定 extra 键。
				extraKey := impersonationRequest.FieldPath
				extraValue := impersonationRequest.Name
				actingAsAttributes.Resource = "userextras"
				actingAsAttributes.Subresource = extraKey
				userExtra[extraKey] = append(userExtra[extraKey], extraValue)

			case authenticationv1.SchemeGroupVersion.WithKind("UID").GroupKind():
				uid = string(impersonationRequest.Name)
				actingAsAttributes.Resource = "uids"

			default:
				// 未知类型一律拒绝，避免将来新增头被静默忽略而绕过授权。
				klog.V(4).InfoS("unknown impersonation request type", "request", impersonationRequest)
				responsewriters.Forbidden(actingAsAttributes, w, req, fmt.Sprintf("unknown impersonation request type: %v", impersonationRequest), s)
				return
			}

			decision, reason, err := a.Authorize(ctx, actingAsAttributes)
			if err != nil || decision != authorizer.DecisionAllow {
				klog.V(4).InfoS("Forbidden", "URI", req.RequestURI, "reason", reason, "err", err)
				responsewriters.Forbidden(actingAsAttributes, w, req, reason, s)
				return
			}
		}

		// 组语义：模拟非匿名用户时默认补上 system:authenticated，除非调用方已显式
		// 指定了 system:authenticated 或 system:unauthenticated（显式即权威）。
		if username != user.Anonymous {
			// 模拟非匿名用户时，要在模拟出的 user info 中加入 'system:authenticated' 组：
			// - 如果没有指定任何组
			// - 如果指定了除 'system:authenticated' 之外的组
			//
			// 如果指定了 'system:unauthenticated' 组，则不应加入
			// 'system:authenticated' 组。
			addAuthenticated := true
			for _, group := range groups {
				if group == user.AllAuthenticated || group == user.AllUnauthenticated {
					addAuthenticated = false
					break
				}
			}

			if addAuthenticated {
				groups = append(groups, user.AllAuthenticated)
			}
		} else {
			// 模拟匿名用户则归属 system:unauthenticated，保持与真实匿名请求一致。
			addUnauthenticated := true
			for _, group := range groups {
				if group == user.AllUnauthenticated {
					addUnauthenticated = false
					break
				}
			}

			if addUnauthenticated {
				groups = append(groups, user.AllUnauthenticated)
			}
		}

		newUser := &user.DefaultInfo{
			Name:   username,
			Groups: groups,
			Extra:  userExtra,
			UID:    uid,
		}
		req = req.WithContext(request.WithUser(ctx, newUser))

		oldUser, _ := request.UserFrom(ctx)
		httplog.LogOf(req, w).Addf("%v is impersonating %v", userString(oldUser), userString(newUser))

		// 审计记录同时保留原用户与模拟用户，便于事后追责。
		audit.LogImpersonatedUser(audit.WithAuditContext(ctx), newUser, "")

		// 清除请求中所有的模拟头。
		// 必须清除，否则下游的聚合层/代理会再次解释这些头，造成二次模拟或越权。
		req.Header.Del(authenticationv1.ImpersonateUserHeader)
		req.Header.Del(authenticationv1.ImpersonateGroupHeader)
		req.Header.Del(authenticationv1.ImpersonateUIDHeader)
		for headerName := range req.Header {
			if strings.HasPrefix(headerName, authenticationv1.ImpersonateUserExtraHeaderPrefix) {
				req.Header.Del(headerName)
			}
		}

		handler.ServeHTTP(w, req)
	})
}

// userString 生成 "name[group1,group2]" 形式的可读表示，仅用于日志。
func userString(u user.Info) string {
	if u == nil {
		return "<none>"
	}
	b := strings.Builder{}
	if name := u.GetName(); name == "" {
		b.WriteString("<empty>")
	} else {
		b.WriteString(name)
	}
	if groups := u.GetGroups(); len(groups) > 0 {
		b.WriteString("[")
		b.WriteString(strings.Join(groups, ","))
		b.WriteString("]")
	}
	return b.String()
}

// unescapeExtraKey 对 extra 头中的键做百分号解码。解码失败时保留原值，
// 因为审计/日志必须如实记录客户端发送的内容，不能因格式问题丢弃。
func unescapeExtraKey(encodedKey string) string {
	key, err := url.PathUnescape(encodedKey) // 解码 %-编码的字节。
	if err != nil {
		return encodedKey // 即使格式非法或未编码，也照常记录 extra 字符串。
	}
	return key
}

// buildImpersonationRequests 返回一组 objectreference，代表我们请求模拟的不同对象，
// 其中也包含表示 user.Info.Extra 的 map[string][]string。
// 在切换上下文之前，每一项请求都必须针对当前用户通过授权。
//
// 顺序无关紧要：所有请求都必须通过授权，任一失败即整体拒绝。
func buildImpersonationRequests(headers http.Header) ([]v1.ObjectReference, error) {
	impersonationRequests := []v1.ObjectReference{}

	requestedUser := headers.Get(authenticationv1.ImpersonateUserHeader)
	hasUser := len(requestedUser) > 0
	if hasUser {
		// serviceaccounts 形式的用户名会被拆成 namespace/name 分别参与授权，
		// 这样 RBAC 可以按命名空间授予模拟权限。
		if namespace, name, err := serviceaccount.SplitUsername(requestedUser); err == nil {
			impersonationRequests = append(impersonationRequests, v1.ObjectReference{Kind: "ServiceAccount", Namespace: namespace, Name: name})
		} else {
			impersonationRequests = append(impersonationRequests, v1.ObjectReference{Kind: "User", Name: requestedUser})
		}
	}

	hasGroups := false
	for _, group := range headers[authenticationv1.ImpersonateGroupHeader] {
		hasGroups = true
		impersonationRequests = append(impersonationRequests, v1.ObjectReference{Kind: "Group", Name: group})
	}

	hasUserExtra := false
	for headerName, values := range headers {
		if !strings.HasPrefix(headerName, authenticationv1.ImpersonateUserExtraHeaderPrefix) {
			continue
		}

		hasUserExtra = true
		// 头名在 HTTP/2 中已小写，这里显式转小写以兼容不同传输层。
		extraKey := unescapeExtraKey(strings.ToLower(headerName[len(authenticationv1.ImpersonateUserExtraHeaderPrefix):]))

		// 为对方要设置的每个 extra 值单独生成一项请求。
		// 每个 extra 值单独授权，避免一个键通过后批量注入多个值。
		for _, value := range values {
			impersonationRequests = append(impersonationRequests,
				v1.ObjectReference{
					Kind: "UserExtra",
					// 上面只解析出了 group，但如果没有某个 version，解析就会失败；
					// 使用内部版本可以在有人开始使用它时及时暴露问题。
					APIVersion: authenticationv1.SchemeGroupVersion.String(),
					Name:       value,
					// ObjectReference 没有 subresource 字段；FieldPath 含义接近且可用，因此用它代替。
					// TODO 继续推动 ObjectReference 支持引用资源和子资源
					FieldPath: extraKey,
				})
		}
	}

	requestedUID := headers.Get(authenticationv1.ImpersonateUIDHeader)
	hasUID := len(requestedUID) > 0
	if hasUID {
		impersonationRequests = append(impersonationRequests, v1.ObjectReference{
			Kind:       "UID",
			Name:       requestedUID,
			APIVersion: authenticationv1.SchemeGroupVersion.String(),
		})
	}

	// 只指定组/extra/UID 而不指定用户是无意义的请求，直接 400；
	// 否则模拟出的身份将没有名字，后续授权与审计都无从解释。
	if (hasGroups || hasUserExtra || hasUID) && !hasUser {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("requested %v without impersonating a user", impersonationRequests))
	}

	return impersonationRequests, nil
}
