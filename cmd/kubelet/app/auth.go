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

package app

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	clientset "k8s.io/client-go/kubernetes"
	authenticationclient "k8s.io/client-go/kubernetes/typed/authentication/v1"
	authorizationclient "k8s.io/client-go/kubernetes/typed/authorization/v1"

	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/server"
)

// BuildAuth 为 Kubelet 组装认证器、授权器及节点授权属性，并返回客户端 CA 热加载函数。
// It returns AuthInterface, a run method to start internal controllers (like cert reloading) and error.
// BuildAuth 将认证与授权两条链路组合为 Kubelet HTTP Server 使用的 AuthInterface。
func BuildAuth(nodeName types.NodeName, client clientset.Interface, config kubeletconfig.KubeletConfiguration) (server.AuthInterface, func(<-chan struct{}), error) {
	// Get clients, if provided
	var (
		tokenClient authenticationclient.AuthenticationV1Interface
		sarClient   authorizationclient.AuthorizationV1Interface
	)
	// client 可能是接口中封装的 typed nil；仅判断接口本身不够，因此还需检查底层值。
	if client != nil && !reflect.ValueOf(client).IsNil() {
		tokenClient = client.AuthenticationV1()
		sarClient = client.AuthorizationV1()
	}

	// 先构建认证器和客户端 CA Provider；后者用于证书校验，也可能需要动态重载。
	authenticator, runAuthenticatorCAReload, clientCAProvider, err := BuildAuthn(tokenClient, config.Authentication)
	if err != nil {
		return nil, nil, err
	}

	attributes := server.NewNodeAuthorizerAttributesGetter(nodeName)

	// 认证回答“请求者是谁”，授权再依据节点身份和请求属性判断“是否允许执行”。
	authorizer, err := BuildAuthz(sarClient, config.Authorization)
	if err != nil {
		return nil, nil, err
	}

	return server.NewKubeletAuth(authenticator, attributes, authorizer, clientCAProvider), runAuthenticatorCAReload, nil
}

// BuildAuthn creates an authenticator compatible with the kubelet's needs
// BuildAuthn 根据 X509、匿名访问和 Webhook 配置创建 HTTP 请求认证器。
func BuildAuthn(client authenticationclient.AuthenticationV1Interface, authn kubeletconfig.KubeletAuthentication) (authenticator.Request, func(<-chan struct{}), dynamiccertificates.CAContentProvider, error) {
	var caContentProvider dynamiccertificates.CAContentProvider
	var dynamicCAContentFromFile *dynamiccertificates.DynamicFileCAContent
	var err error
	// 配置客户端 CA 文件时使用动态文件 Provider，以便文件更新后重新加载信任根。
	if len(authn.X509.ClientCAFile) > 0 {
		dynamicCAContentFromFile, err = dynamiccertificates.NewDynamicCAContentFromFile("client-ca-bundle", authn.X509.ClientCAFile)
		if err != nil {
			return nil, nil, nil, err
		}
		caContentProvider = dynamicCAContentFromFile
	} else {
		// 未配置客户端 CA 时仍返回非 nil 的空 Provider，避免下游重复判断 Provider 是否为空。
		caContentProvider = dynamiccertificates.NewUnionCAContentProvider()
	}

	authenticatorConfig := authenticatorfactory.DelegatingAuthenticatorConfig{
		Anonymous:                          &apiserver.AnonymousAuthConfig{Enabled: authn.Anonymous.Enabled},
		CacheTTL:                           authn.Webhook.CacheTTL.Duration,
		ClientCertificateCAContentProvider: dynamicCAContentFromFile,
	}

	// Webhook 认证通过 TokenReview 委托给 API Server，因此必须先确保存在可用客户端。
	if authn.Webhook.Enabled {
		if client == nil {
			return nil, nil, nil, errors.New("no client provided, cannot use webhook authentication")
		}
		authenticatorConfig.WebhookRetryBackoff = genericoptions.DefaultAuthWebhookRetryBackoff()
		authenticatorConfig.TokenAccessReviewClient = client
	}

	authenticator, _, err := authenticatorConfig.New()
	if err != nil {
		return nil, nil, nil, err
	}

	// 返回启动函数，由调用方在 Kubelet 生命周期内运行 CA 文件监视器，并通过 stopCh 停止它。
	return authenticator, func(stopCh <-chan struct{}) {
		// generate a context from stopCh. This is to avoid modifying files which are relying on this method
		// TODO: See if we can pass ctx to the current method
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-stopCh:
				cancel() // stopCh closed, so cancel our context
			case <-ctx.Done():
			}
		}()
		if dynamicCAContentFromFile != nil {
			go dynamicCAContentFromFile.Run(ctx, 1)
		}
	}, caContentProvider, err
}

// BuildAuthz creates an authorizer compatible with the kubelet's needs
// BuildAuthz 按配置选择本地 AlwaysAllow 或 API Server Webhook 授权策略。
func BuildAuthz(client authorizationclient.AuthorizationV1Interface, authz kubeletconfig.KubeletAuthorization) (authorizer.UnconditionalAuthorizer, error) {
	switch authz.Mode {
	case kubeletconfig.KubeletAuthorizationModeAlwaysAllow:
		return authorizerfactory.NewAlwaysAllowAuthorizer(), nil

	case kubeletconfig.KubeletAuthorizationModeWebhook:
		// Webhook 授权将请求转换为 SubjectAccessReview；缓存 TTL 决定授权结果的复用时间。
		if client == nil {
			return nil, errors.New("no client provided, cannot use webhook authorization")
		}
		authorizerConfig := authorizerfactory.DelegatingAuthorizerConfig{
			SubjectAccessReviewClient: client,
			AllowCacheTTL:             authz.Webhook.CacheAuthorizedTTL.Duration,
			DenyCacheTTL:              authz.Webhook.CacheUnauthorizedTTL.Duration,
			WebhookRetryBackoff:       genericoptions.DefaultAuthWebhookRetryBackoff(),
		}
		return authorizerConfig.New()

	case "":
		return nil, fmt.Errorf("no authorization mode specified")

	default:
		return nil, fmt.Errorf("unknown authorization mode %s", authz.Mode)

	}
}
