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

// Package authorizerfactory 提供内置授权器的构造方法。
// 本文件实现“委托授权”（delegating authorization）：通过 SubjectAccessReview
// 把授权决策交给另一个 kube-apiserver，典型用于聚合 apiserver。
package authorizerfactory

import (
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	authorizationcel "k8s.io/apiserver/pkg/authorization/cel"
	"k8s.io/apiserver/plugin/pkg/authorizer/webhook"
	authorizationclient "k8s.io/client-go/kubernetes/typed/authorization/v1"
)

// DelegatingAuthorizerConfig 是创建「把授权委托给 kube API server」的授权器所需的最小配置。
//
// 缓存 TTL 通常允许值长于拒绝值：允许结果变化慢、拒绝后用户重试往往很快，
// 过长的拒绝缓存会让重试无效。
type DelegatingAuthorizerConfig struct {
	SubjectAccessReviewClient authorizationclient.AuthorizationV1Interface

	// 评估策略所用的 CEL 编译器；为 nil 时使用默认编译器。
	Compiler authorizationcel.Compiler

	// 成功授权结果的缓存时长。
	AllowCacheTTL time.Duration

	// 拒绝授权的缓存时长。通常应较短，便于用户在被拒后快速重试。
	DenyCacheTTL time.Duration

	// 授权 Webhook 的重试退避参数：设置每轮等待时长与最大重试次数，
	// 避免系统降级时失败请求引发的扇出放大。
	WebhookRetryBackoff *wait.Backoff
}

// New 构造一个基于 webhook 的委托授权器。
// WebhookRetryBackoff 必填：没有退避策略就无法在后端故障时限制重试风暴。
// 未提供 Compiler 时使用默认的 CEL 编译器；授权器对未知请求默认返回 NoOpinion，
// 以便与其它授权器组成链。
func (c DelegatingAuthorizerConfig) New() (authorizer.Authorizer, error) {
	if c.WebhookRetryBackoff == nil {
		return nil, errors.New("retry backoff parameters for delegating authorization webhook has not been specified")
	}
	compiler := c.Compiler
	if compiler == nil {
		compiler = authorizationcel.NewDefaultCompiler()
	}

	return webhook.NewFromInterface(
		c.SubjectAccessReviewClient,
		c.AllowCacheTTL,
		c.DenyCacheTTL,
		*c.WebhookRetryBackoff,
		authorizer.DecisionNoOpinion,
		NewDelegatingAuthorizerMetrics(),
		compiler,
	)
}
