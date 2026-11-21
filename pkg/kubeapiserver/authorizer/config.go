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

package authorizer

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	authzconfig "k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/apis/apiserver/load"
	"k8s.io/apiserver/pkg/apis/apiserver/validation"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	authorizationcel "k8s.io/apiserver/pkg/authorization/cel"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	versionedinformers "k8s.io/client-go/informers"
	certinformersv1 "k8s.io/client-go/informers/certificates/v1"
	resourceinformers "k8s.io/client-go/informers/resource/v1"
	"k8s.io/kubernetes/pkg/auth/authorizer/abac"
	"k8s.io/kubernetes/pkg/auth/nodeidentifier"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/node"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac/bootstrappolicy"
)

// Config 包含如何对 Kube API Server 的请求进行授权的数据。
type Config struct {
	// ModeABAC 的选项

	// ABAC 策略文件的路径。
	PolicyFile string

	// ModeWebhook 的选项

	// WebhookRetryBackoff 指定授权 webhook 重试逻辑的退避参数。
	// 通过配置每次迭代的休眠时间与最大重试次数，可在系统降级时限制请求放大效应。
	WebhookRetryBackoff *wait.Backoff

	VersionedInformerFactory versionedinformers.SharedInformerFactory

	// 可选字段，连接 webhook 时使用的自定义拨号函数。
	CustomDial utilnet.DialFunc

	// ReloadFile 指定重新加载授权配置的文件名。
	ReloadFile string
	// AuthorizationConfiguration 存储授权器链的配置。
	// 当该特性 GA 后，它将取代上面的大部分标志。
	AuthorizationConfiguration *authzconfig.AuthorizationConfiguration
	// InitialAuthorizationConfigurationData 保存从授权配置文件中读取的初始配置数据。
	InitialAuthorizationConfigurationData string
}

// New 根据授权模式返回将多个 authorizer.Authorizer 对象组合而成的联合授权器，或返回错误。
// ctx 用于在 server 关闭时终止配置重载的 goroutine。
//
// 注意：cel 编译器的构造依赖已初始化的特性门控与兼容性版本。
func (config Config) New(ctx context.Context, serverID string) (authorizer.Authorizer, authorizer.RuleResolver, error) {
	if len(config.AuthorizationConfiguration.Authorizers) == 0 {
		return nil, nil, fmt.Errorf("at least one authorization mode must be passed")
	}

	r := &reloadableAuthorizerResolver{
		initialConfig:    config,
		apiServerID:      serverID,
		lastLoadedConfig: config.AuthorizationConfiguration,
		lastReadData:     []byte(config.InitialAuthorizationConfigurationData),
		reloadInterval:   time.Minute,
		compiler:         authorizationcel.NewDefaultCompiler(),
	}

	seenTypes := sets.New[authzconfig.AuthorizerType]()

	// 构建并保存可在多次重载间复用的授权器（如 Node/RBAC/ABAC）。
	// webhook 授权器依赖配置，因此每次重载时重建。
	for _, configuredAuthorizer := range config.AuthorizationConfiguration.Authorizers {
		seenTypes.Insert(configuredAuthorizer.Type)

		// Keep cases in sync with constant list in k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes/modes.go.
		switch configuredAuthorizer.Type {
		case authzconfig.AuthorizerType(modes.ModeNode):
			var slices resourceinformers.ResourceSliceInformer
			if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
				slices = config.VersionedInformerFactory.Resource().V1().ResourceSlices()
			}
			var podCertificateRequestInformer certinformersv1.PodCertificateRequestInformer
			if utilfeature.DefaultFeatureGate.Enabled(features.PodCertificateRequest) {
				podCertificateRequestInformer = config.VersionedInformerFactory.Certificates().V1().PodCertificateRequests()
			}
			node.RegisterMetrics()
			graph := node.NewGraph()
			node.AddGraphEventHandlers(
				graph,
				config.VersionedInformerFactory.Core().V1().Nodes(),
				config.VersionedInformerFactory.Core().V1().Pods(),
				config.VersionedInformerFactory.Core().V1().PersistentVolumes(),
				config.VersionedInformerFactory.Storage().V1().VolumeAttachments(),
				slices, // Nil check in AddGraphEventHandlers can be removed when always creating this.
				podCertificateRequestInformer,
			)
			r.nodeAuthorizer = node.NewAuthorizer(graph, nodeidentifier.NewDefaultNodeIdentifier(), bootstrappolicy.NodeRules())

		case authzconfig.AuthorizerType(modes.ModeABAC):
			var err error
			r.abacAuthorizer, err = abac.NewFromFile(config.PolicyFile)
			if err != nil {
				return nil, nil, err
			}
		case authzconfig.AuthorizerType(modes.ModeRBAC):
			r.rbacAuthorizer = rbac.New(
				&rbac.RoleGetter{Lister: config.VersionedInformerFactory.Rbac().V1().Roles().Lister()},
				&rbac.RoleBindingLister{Lister: config.VersionedInformerFactory.Rbac().V1().RoleBindings().Lister()},
				&rbac.ClusterRoleGetter{Lister: config.VersionedInformerFactory.Rbac().V1().ClusterRoles().Lister()},
				&rbac.ClusterRoleBindingLister{Lister: config.VersionedInformerFactory.Rbac().V1().ClusterRoleBindings().Lister()},
			)
		}
	}

	// 重载后必须在配置文件中保留所有非 webhook 类型的授权器：
	// 只有 webhook 可以重复和动态重建，其余类型仅在此处构建一次。
	seenTypes.Delete(authzconfig.TypeWebhook)
	r.requireNonWebhookTypes = seenTypes

	// 根据当前配置构造授权器与规则解析器。
	authorizer, ruleResolver, err := r.newForConfig(r.initialConfig.AuthorizationConfiguration)
	if err != nil {
		return nil, nil, err
	}

	r.current.Store(&authorizerResolver{
		authorizer:   authorizer,
		ruleResolver: ruleResolver,
	})

	// 仅在配置了重载文件时启动定时重载，避免无谓的文件轮询。
	if r.initialConfig.ReloadFile != "" {
		go r.runReload(ctx)
	}

	return r, r, nil
}

// repeatableAuthorizerTypes 列出在授权配置中允许重复出现的授权器类型。
var repeatableAuthorizerTypes = []string{modes.ModeWebhook}

// GetNameForAuthorizerMode 返回在 AuthorizationConfiguration 中为该模式设置的名称。
// 目前仅将模式名转为小写。
func GetNameForAuthorizerMode(mode string) string {
	return strings.ToLower(mode)
}

// LoadAndValidateFile 从文件读取授权配置并校验，同时返回原始文件内容供重载时比对。
func LoadAndValidateFile(configFile string, compiler authorizationcel.Compiler, requireNonWebhookTypes sets.Set[authzconfig.AuthorizerType]) (*authzconfig.AuthorizationConfiguration, string, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, "", err
	}
	config, err := LoadAndValidateData(data, compiler, requireNonWebhookTypes)
	if err != nil {
		return nil, "", err
	}
	return config, string(data), nil
}

// LoadAndValidateData 解析并校验给定的授权配置数据。
func LoadAndValidateData(data []byte, compiler authorizationcel.Compiler, requireNonWebhookTypes sets.Set[authzconfig.AuthorizerType]) (*authzconfig.AuthorizationConfiguration, error) {
	// 加载配置并检查错误。
	authorizationConfiguration, err := load.LoadFromData(data)
	if err != nil {
		return nil, fmt.Errorf("failed to load AuthorizationConfiguration from file: %w", err)
	}

	// 校验配置，返回所有聚合错误。
	if errors := validation.ValidateAuthorizationConfiguration(compiler, nil, authorizationConfiguration,
		sets.New(modes.AuthorizationModeChoices...),
		sets.New(repeatableAuthorizerTypes...),
	); len(errors) != 0 {
		return nil, errors.ToAggregate()
	}

	// 检查非 webhook 授权器的名称是否符合约定。
	// 这是 kube-apiserver 特有的校验，故在此处进行，
	// 以保持与 o.buildAuthorizationConfiguration 的兼容性。
	var allErrors []error
	seenModes := sets.New[authzconfig.AuthorizerType]()
	for _, authorizer := range authorizationConfiguration.Authorizers {
		if string(authorizer.Type) == modes.ModeWebhook {
			continue
		}
		seenModes.Insert(authorizer.Type)

		expectedName := GetNameForAuthorizerMode(string(authorizer.Type))
		if expectedName != authorizer.Name {
			allErrors = append(allErrors, fmt.Errorf("expected name %s for authorizer %s instead of %s", expectedName, authorizer.Type, authorizer.Name))
		}
	}

	if missingTypes := requireNonWebhookTypes.Difference(seenModes); missingTypes.Len() > 0 {
		allErrors = append(allErrors, fmt.Errorf("missing required types: %v", sets.List(missingTypes)))
	}

	if len(allErrors) > 0 {
		return nil, utilerrors.NewAggregate(allErrors)
	}

	return authorizationConfiguration, nil
}
