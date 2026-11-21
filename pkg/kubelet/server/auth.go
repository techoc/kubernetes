/*
Copyright 2015 The Kubernetes Authors.

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

package server

import (
	"context"
	"net/http"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/statusz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/configz"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
)

// KubeletAuth 实现 AuthInterface，组合认证、属性构建、授权与客户端 CA 校验。
type KubeletAuth struct {
	// 认证器负责识别访问 Kubelet API 的用户。
	authenticator.Request
	// 为发往 Kubelet API 的请求构建授权属性。
	NodeRequestAttributesGetter
	// 判断给定授权属性是否被允许。
	authorizer.UnconditionalAuthorizer
	// 提供客户端 CA 内容，用于基于客户端证书的鉴权。
	dynamiccertificates.CAContentProvider
}

// NewKubeletAuth 返回由给定认证器、属性获取器与授权器组合而成的 AuthInterface。
func NewKubeletAuth(authenticator authenticator.Request, authorizerAttributeGetter NodeRequestAttributesGetter, authorizer authorizer.UnconditionalAuthorizer, clientCAProvider dynamiccertificates.CAContentProvider) AuthInterface {
	return &KubeletAuth{authenticator, authorizerAttributeGetter, authorizer, clientCAProvider}
}

// NewNodeAuthorizerAttributesGetter 为节点创建授权属性获取器。
func NewNodeAuthorizerAttributesGetter(nodeName types.NodeName) NodeRequestAttributesGetter {
	return nodeAuthorizerAttributesGetter{nodeName: nodeName}
}

// nodeAuthorizerAttributesGetter 以节点名为中心构建请求属性。
type nodeAuthorizerAttributesGetter struct {
	nodeName types.NodeName
}

// isSubpath 判断 subpath 是否等于 path 或位于其子路径下；忽略 path 末尾的斜杠。
func isSubpath(subpath, path string) bool {
	path = strings.TrimSuffix(path, "/")
	return subpath == path || (strings.HasPrefix(subpath, path) && subpath[len(path)] == '/')
}

// GetRequestAttributes 为发往 kubelet API 的请求填充授权属性。
// 默认属性为：{apiVersion=v1, verb=<请求中的 HTTP 动词>, resource=nodes, name=<节点名>, subresource=proxy}。
// 以下请求模式会设置更具体的 verb/resource：
//
//	/stats/*		=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=stats
//	/metrics/*		=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=metrics
//	/logs/*			=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=log
//	/checkpoint/*	=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=checkpoint
//	/statusz 		=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=statusz
//	/pods/*			=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=pods,proxy
//	/runningPods/*	=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=pods,proxy
//	/healthz/* 		=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=healthz,proxy
//	/configz 		=> verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=configz,proxy
//	/flagz 		    => verb=<请求中的 api 动词>, resource=nodes, name=<节点名>, subresource(s)=configz,proxy
func (n nodeAuthorizerAttributesGetter) GetRequestAttributes(ctx context.Context, u user.Info, r *http.Request) []authorizer.Attributes {
	logger := klog.FromContext(ctx)
	// 把 HTTP 方法映射为 Kubernetes 授权动词。
	apiVerb := ""
	switch r.Method {
	case "POST":
		apiVerb = "create"
	case "GET":
		apiVerb = "get"
	case "PUT":
		apiVerb = "update"
	case "PATCH":
		apiVerb = "patch"
	case "DELETE":
		apiVerb = "delete"
	}

	requestPath := r.URL.Path

	// 精细授权开启时先按路径追加子资源；关闭时仅使用下方的基础子资源判断。
	var subresources []string
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletFineGrainedAuthz) {
		switch {
		case isSubpath(requestPath, allocatedPodsPath):
			subresources = append(subresources, "pods")
		case isSubpath(requestPath, podsPath):
			subresources = append(subresources, "pods")
		case isSubpath(requestPath, healthz.DefaultHealthzPath):
			subresources = append(subresources, "healthz")
		case isSubpath(requestPath, configz.DefaultConfigzPath):
			subresources = append(subresources, "configz")
		// 把 runningPods 放在最后判断：它的 handler 路径带尾部斜杠，每次检查都会分配新字符串。
		case isSubpath(requestPath, runningPodsPath):
			subresources = append(subresources, "pods")
		}
	}

	switch {
	case isSubpath(requestPath, statsPath):
		subresources = append(subresources, "stats")
	case isSubpath(requestPath, metricsPath):
		subresources = append(subresources, "metrics")
	case isSubpath(requestPath, logsPath):
		// 使用 "log" 与其他日志子资源（pods/log 等）保持一致。
		subresources = append(subresources, "log")
	case isSubpath(requestPath, checkpointPath):
		subresources = append(subresources, "checkpoint")
	case isSubpath(requestPath, statusz.DefaultStatuszPath):
		subresources = append(subresources, "statusz")
	case isSubpath(requestPath, flagz.DefaultFlagzPath):
		subresources = append(subresources, "configz")
	default:
		subresources = append(subresources, "proxy")
	}

	// 每个子资源生成一条属性，授权器需要全部允许。
	var attrs []authorizer.Attributes
	for _, subresource := range subresources {
		attr := authorizer.AttributesRecord{
			User:            u,
			Verb:            apiVerb,
			Namespace:       "",
			APIGroup:        "",
			APIVersion:      "v1",
			Resource:        "nodes",
			Subresource:     subresource,
			Name:            string(n.nodeName),
			ResourceRequest: true,
			Path:            requestPath,
		}
		attrs = append(attrs, attr)
	}

	// 仅在高详细度日志中输出属性，便于排查授权问题。
	logger.V(5).Info("Node request attributes", "user", attrs[0].GetUser().GetName(), "verb", attrs[0].GetVerb(), "resource", attrs[0].GetResource(), "subresource(s)", subresources)

	return attrs
}
