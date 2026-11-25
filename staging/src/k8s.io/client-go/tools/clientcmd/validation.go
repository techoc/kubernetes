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

package clientcmd

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	authexec "k8s.io/client-go/plugin/pkg/client/auth/exec"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var (
	// ErrNoContext 表示既未显式指定 context，配置里也没有 current-context。
	ErrNoContext = errors.New("no context chosen")
	// ErrEmptyConfig 用 errEmptyConfig 类型承载，以便 IsEmptyConfig 能通过类型断言识别。
	ErrEmptyConfig = NewEmptyConfigError("no configuration has been provided, try setting KUBERNETES_MASTER environment variable")
	// 错误文本沿用旧行为以保持一致
	// ErrEmptyCluster 保留旧错误文本，调用方可能依赖它做字符串判断。
	ErrEmptyCluster = errors.New("cluster has no server defined")
)

// NewEmptyConfigError 返回包装了给定信息的错误，IsEmptyConfig() 会将其识别为空配置错误
func NewEmptyConfigError(message string) error {
	return &errEmptyConfig{message}
}

type errEmptyConfig struct {
	message string
}

func (e *errEmptyConfig) Error() string {
	return e.message
}

// errContextNotFound 记录缺失的 context 名，便于错误信息定位具体配置项。
type errContextNotFound struct {
	ContextName string
}

func (e *errContextNotFound) Error() string {
	return fmt.Sprintf("context was not found for specified context: %v", e.ContextName)
}

// IsContextNotFound 返回一个布尔值，表示该错误是否表示未找到 context
func IsContextNotFound(err error) bool {
	if err == nil {
		return false
	}
	// 类型断言之外还做字符串匹配，是为了兼容被 wrap 或跨进程序列化后丢失具体类型的错误。
	if _, ok := err.(*errContextNotFound); ok || err == ErrNoContext {
		return true
	}
	return strings.Contains(err.Error(), "context was not found for specified context")
}

// IsEmptyConfig 在给定错误表示所提供的配置为空时返回 true。
// 空配置可能被包装在聚合错误中，因此两种形态都要识别；聚合里必须只有空配置错误，
// 否则说明还存在其他真实问题，不能简单当作“没配”。
func IsEmptyConfig(err error) bool {
	switch t := err.(type) {
	case errConfigurationInvalid:
		if len(t) != 1 {
			return false
		}
		_, ok := t[0].(*errEmptyConfig)
		return ok
	}
	_, ok := err.(*errEmptyConfig)
	return ok
}

// errConfigurationInvalid 表示配置无效的一组错误。
//
// 它同时充当错误集合与 utilerrors.Aggregate，既保留全部校验错误，又能参与 errors.Is 判定。
type errConfigurationInvalid []error

// errConfigurationInvalid 实现 error 和 Aggregate 接口
var _ error = errConfigurationInvalid{}
var _ utilerrors.Aggregate = errConfigurationInvalid{}

func newErrConfigurationInvalid(errs []error) error {
	switch len(errs) {
	case 0:
		return nil
	default:
		return errConfigurationInvalid(errs)
	}
}

// Error 实现 error 接口
func (e errConfigurationInvalid) Error() string {
	return fmt.Sprintf("invalid configuration: %v", utilerrors.NewAggregate(e).Error())
}

// Errors 实现 utilerrors.Aggregate 接口
func (e errConfigurationInvalid) Errors() []error {
	return e
}

// Is 实现 utilerrors.Aggregate 接口
func (e errConfigurationInvalid) Is(target error) bool {
	return e.visit(func(err error) bool {
		return errors.Is(err, target)
	})
}

// visit 递归遍历嵌套的聚合错误，供 Is 实现使用；遇到第一个匹配即返回。
func (e errConfigurationInvalid) visit(f func(err error) bool) bool {
	for _, err := range e {
		switch err := err.(type) {
		case errConfigurationInvalid:
			if match := err.visit(f); match {
				return match
			}
		case utilerrors.Aggregate:
			for _, nestedErr := range err.Errors() {
				if match := f(nestedErr); match {
					return match
				}
			}
		default:
			if match := f(err); match {
				return match
			}
		}
	}

	return false
}

// IsConfigurationInvalid 在给定错误表示配置无效时返回 true。
func IsConfigurationInvalid(err error) bool {
	switch err.(type) {
	case *errContextNotFound, errConfigurationInvalid:
		return true
	}
	return IsContextNotFound(err)
}

// Validate 检查 Config 中的错误。它不会提前返回，以便尽可能多地发现问题。
//
// 校验覆盖 current-context 是否存在、每个 context 的引用关系、各用户与集群自身的一致性。
func Validate(config clientcmdapi.Config) error {
	validationErrors := make([]error, 0)

	if clientcmdapi.IsConfigEmpty(&config) {
		return newErrConfigurationInvalid([]error{ErrEmptyConfig})
	}

	if len(config.CurrentContext) != 0 {
		if _, exists := config.Contexts[config.CurrentContext]; !exists {
			validationErrors = append(validationErrors, &errContextNotFound{config.CurrentContext})
		}
	}

	for contextName, context := range config.Contexts {
		validationErrors = append(validationErrors, validateContext(contextName, *context, config)...)
	}

	for authInfoName, authInfo := range config.AuthInfos {
		validationErrors = append(validationErrors, validateAuthInfo(authInfoName, *authInfo)...)
	}

	for clusterName, clusterInfo := range config.Clusters {
		validationErrors = append(validationErrors, validateClusterInfo(clusterName, *clusterInfo)...)
	}

	return newErrConfigurationInvalid(validationErrors)
}

// ConfirmUsable 检查特定 context，判断配置中该部分是否可用。配置中可能仍有错误，
// 但所请求或引用的部分没有错误。它不会提前返回，以便尽可能多地发现问题。
//
// 与 Validate 的差别在于只检查当前 context 及其引用，忽略其他未使用的配置项。
func ConfirmUsable(config clientcmdapi.Config, passedContextName string) error {
	validationErrors := make([]error, 0)

	if clientcmdapi.IsConfigEmpty(&config) {
		return newErrConfigurationInvalid([]error{ErrEmptyConfig})
	}

	var contextName string
	if len(passedContextName) != 0 {
		contextName = passedContextName
	} else {
		contextName = config.CurrentContext
	}

	if len(contextName) == 0 {
		return ErrNoContext
	}

	context, exists := config.Contexts[contextName]
	if !exists {
		validationErrors = append(validationErrors, &errContextNotFound{contextName})
	}

	if exists {
		validationErrors = append(validationErrors, validateContext(contextName, *context, config)...)

		// 用户与集群缺省为空对象，交由校验函数报告错误。
		authInfo := config.AuthInfos[context.AuthInfo]
		if authInfo == nil {
			authInfo = &clientcmdapi.AuthInfo{}
		}
		validationErrors = append(validationErrors, validateAuthInfo(context.AuthInfo, *authInfo)...)

		cluster := config.Clusters[context.Cluster]
		if cluster == nil {
			cluster = &clientcmdapi.Cluster{}
		}
		validationErrors = append(validationErrors, validateClusterInfo(context.Cluster, *cluster)...)
	}

	return newErrConfigurationInvalid(validationErrors)
}

// validateClusterInfo 检查 cluster info 中的冲突和错误
//
// 只做静态检查：server 非空、proxy-url 可解析、CA 的 file 与 data 不同时存在且文件可读。
func validateClusterInfo(clusterName string, clusterInfo clientcmdapi.Cluster) []error {
	validationErrors := make([]error, 0)

	emptyCluster := clientcmdapi.NewCluster()
	if reflect.DeepEqual(*emptyCluster, clusterInfo) {
		return []error{ErrEmptyCluster}
	}

	if len(clusterInfo.Server) == 0 {
		if len(clusterName) == 0 {
			validationErrors = append(validationErrors, fmt.Errorf("default cluster has no server defined"))
		} else {
			validationErrors = append(validationErrors, fmt.Errorf("no server found for cluster %q", clusterName))
		}
	}
	if proxyURL := clusterInfo.ProxyURL; proxyURL != "" {
		if _, err := parseProxyURL(proxyURL); err != nil {
			validationErrors = append(validationErrors, fmt.Errorf("invalid 'proxy-url' %q for cluster %q: %w", proxyURL, clusterName, err))
		}
	}
	// 确保 CA 数据与 CA 文件没有同时指定
	if len(clusterInfo.CertificateAuthority) != 0 && len(clusterInfo.CertificateAuthorityData) != 0 {
		validationErrors = append(validationErrors, fmt.Errorf("certificate-authority-data and certificate-authority are both specified for %v. certificate-authority-data will override.", clusterName))
	}
	if len(clusterInfo.CertificateAuthority) != 0 {
		clientCertCA, err := os.Open(clusterInfo.CertificateAuthority)
		if err != nil {
			validationErrors = append(validationErrors, fmt.Errorf("unable to read certificate-authority %v for %v due to %w", clusterInfo.CertificateAuthority, clusterName, err))
		} else {
			defer clientCertCA.Close()
		}
	}

	return validationErrors
}

// validateAuthInfo 检查 auth info 中的冲突和错误
//
// 重点检查互斥的认证方式、证书与私钥的配对，以及 exec 插件必填字段（命令、apiVersion、
// interactiveMode、env 名与非空插件策略）。
func validateAuthInfo(authInfoName string, authInfo clientcmdapi.AuthInfo) []error {
	validationErrors := make([]error, 0)

	usingAuthPath := false
	methods := make([]string, 0, 3)
	if len(authInfo.Token) != 0 {
		methods = append(methods, "token")
	}
	if len(authInfo.Username) != 0 || len(authInfo.Password) != 0 {
		methods = append(methods, "basicAuth")
	}

	if len(authInfo.ClientCertificate) != 0 || len(authInfo.ClientCertificateData) != 0 {
		// 确保证书数据与证书文件没有同时指定
		if len(authInfo.ClientCertificate) != 0 && len(authInfo.ClientCertificateData) != 0 {
			validationErrors = append(validationErrors, fmt.Errorf("client-cert-data and client-cert are both specified for %v. client-cert-data will override.", authInfoName))
		}
		// 确保私钥数据与私钥文件没有同时指定
		if len(authInfo.ClientKey) != 0 && len(authInfo.ClientKeyData) != 0 {
			validationErrors = append(validationErrors, fmt.Errorf("client-key-data and client-key are both specified for %v; client-key-data will override", authInfoName))
		}
		// 确保已指定私钥
		if len(authInfo.ClientKey) == 0 && len(authInfo.ClientKeyData) == 0 {
			validationErrors = append(validationErrors, fmt.Errorf("client-key-data or client-key must be specified for %v to use the clientCert authentication method.", authInfoName))
		}

		if len(authInfo.ClientCertificate) != 0 {
			clientCertFile, err := os.Open(authInfo.ClientCertificate)
			if err != nil {
				validationErrors = append(validationErrors, fmt.Errorf("unable to read client-cert %v for %v due to %w", authInfo.ClientCertificate, authInfoName, err))
			} else {
				defer clientCertFile.Close()
			}
		}
		if len(authInfo.ClientKey) != 0 {
			clientKeyFile, err := os.Open(authInfo.ClientKey)
			if err != nil {
				validationErrors = append(validationErrors, fmt.Errorf("unable to read client-key %v for %v due to %w", authInfo.ClientKey, authInfoName, err))
			} else {
				defer clientKeyFile.Close()
			}
		}
	}

	if authInfo.Exec != nil {
		if authInfo.AuthProvider != nil {
			validationErrors = append(validationErrors, fmt.Errorf("authProvider cannot be provided in combination with an exec plugin for %s", authInfoName))
		}
		if len(authInfo.Exec.Command) == 0 {
			validationErrors = append(validationErrors, fmt.Errorf("command must be specified for %v to use exec authentication plugin", authInfoName))
		}
		if len(authInfo.Exec.APIVersion) == 0 {
			validationErrors = append(validationErrors, fmt.Errorf("apiVersion must be specified for %v to use exec authentication plugin", authInfoName))
		}
		for _, v := range authInfo.Exec.Env {
			if len(v.Name) == 0 {
				validationErrors = append(validationErrors, fmt.Errorf("env variable name must be specified for %v to use exec authentication plugin", authInfoName))
			}
		}
		switch authInfo.Exec.InteractiveMode {
		case "":
			validationErrors = append(validationErrors, fmt.Errorf("interactiveMode must be specified for %v to use exec authentication plugin", authInfoName))
		case clientcmdapi.NeverExecInteractiveMode, clientcmdapi.IfAvailableExecInteractiveMode, clientcmdapi.AlwaysExecInteractiveMode:
			// 这些都是合法取值
		default:
			validationErrors = append(validationErrors, fmt.Errorf("invalid interactiveMode for %v: %q", authInfoName, authInfo.Exec.InteractiveMode))
		}

		if err := authexec.ValidatePluginPolicy(authInfo.Exec.PluginPolicy); err != nil {
			validationErrors = append(validationErrors, fmt.Errorf("allowlist misconfiguration: %w", err))
		}
	}

	// authPath 同时提供了客户端识别服务端所需的信息，因此这种情况下允许多种认证方式并存
	if (len(methods) > 1) && (!usingAuthPath) {
		validationErrors = append(validationErrors, fmt.Errorf("more than one authentication method found for %v; found %v, only one is allowed", authInfoName, methods))
	}

	// ImpersonateUID、ImpersonateGroups 或 ImpersonateUserExtra 必须与 Impersonate 用户一起使用
	if (len(authInfo.ImpersonateUID) > 0 || len(authInfo.ImpersonateGroups) > 0 || len(authInfo.ImpersonateUserExtra) > 0) && (len(authInfo.Impersonate) == 0) {
		validationErrors = append(validationErrors, fmt.Errorf("requesting uid, groups or user-extra for %v without impersonating a user", authInfoName))
	}
	return validationErrors
}

// validateContext 检查 context 中的错误。它不具有传递性，因此所引用的 authInfo 或 cluster 配置中的错误不包含在返回值里
//
// “非传递”意味着这里只确认引用目标存在，目标内部的配置错误由各自校验函数负责。
func validateContext(contextName string, context clientcmdapi.Context, config clientcmdapi.Config) []error {
	validationErrors := make([]error, 0)

	if len(contextName) == 0 {
		validationErrors = append(validationErrors, fmt.Errorf("empty context name for %#v is not allowed", context))
	}

	if len(context.AuthInfo) == 0 {
		validationErrors = append(validationErrors, fmt.Errorf("user was not specified for context %q", contextName))
	} else if _, exists := config.AuthInfos[context.AuthInfo]; !exists {
		validationErrors = append(validationErrors, fmt.Errorf("user %q was not found for context %q", context.AuthInfo, contextName))
	}

	if len(context.Cluster) == 0 {
		validationErrors = append(validationErrors, fmt.Errorf("cluster was not specified for context %q", contextName))
	} else if _, exists := config.Clusters[context.Cluster]; !exists {
		validationErrors = append(validationErrors, fmt.Errorf("cluster %q was not found for context %q", context.Cluster, contextName))
	}

	if len(context.Namespace) != 0 {
		if len(validation.IsDNS1123Label(context.Namespace)) != 0 {
			validationErrors = append(validationErrors, fmt.Errorf("namespace %q for context %q does not conform to the kubernetes DNS_LABEL rules", context.Namespace, contextName))
		}
	}

	return validationErrors
}
