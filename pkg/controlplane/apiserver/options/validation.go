/*
Copyright 2023 The Kubernetes Authors.

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

package options

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apiserverfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kubernetes/pkg/features"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// validateTokenRequest 校验 ServiceAccount token 请求相关的 flag 是否成套配置。
func validateTokenRequest(options *Options) []error {
	var errs []error

	enableAttempted := options.ServiceAccountSigningKeyFile != "" ||
		(len(options.Authentication.ServiceAccounts.Issuers) != 0 && options.Authentication.ServiceAccounts.Issuers[0] != "") ||
		len(options.Authentication.APIAudiences) != 0

	enableSucceeded := options.ServiceAccountIssuer != nil

	if !enableAttempted {
		errs = append(errs, errors.New("--service-account-signing-key-file and --service-account-issuer are required flags"))
	}

	if enableAttempted && !enableSucceeded {
		errs = append(errs, errors.New("--service-account-signing-key-file, --service-account-issuer, and --api-audiences should be specified together"))
	}

	return errs
}

// validateAPIPriorityAndFairness 校验启用 APF 时未通过 runtime-config 禁用其所需的 API 组。
func validateAPIPriorityAndFairness(options *Options) []error {
	if options.Features.EnablePriorityAndFairness {
		// 未显式指定以下 runtime-config 时默认认为 APF 开启；内部 APF 控制器使用 v1，故必须启用。
		enabledAPIString := options.APIEnablement.RuntimeConfig.String()
		testConfigs := []string{"flowcontrol.apiserver.k8s.io/v1", "api/ga", "api/all"} // 按优先级从高到低排列
		for _, testConfig := range testConfigs {
			if strings.Contains(enabledAPIString, fmt.Sprintf("%s=false", testConfig)) {
				return []error{fmt.Errorf("--runtime-config=%s=false conflicts with --enable-priority-and-fairness=true", testConfig)}
			}
			if strings.Contains(enabledAPIString, fmt.Sprintf("%s=true", testConfig)) {
				return nil
			}
		}
	}

	return nil
}

// validateUnknownVersionInteroperabilityProxyFlags 校验对等代理相关 flag 只有在对应特性开启时才能使用。
func validateUnknownVersionInteroperabilityProxyFlags(options *Options) []error {
	err := []error{}
	if !utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.UnknownVersionInteroperabilityProxy) {
		if options.PeerCAFile != "" {
			err = append(err, fmt.Errorf("--peer-ca-file requires UnknownVersionInteroperabilityProxy feature to be turned on"))
		}
		if options.PeerAdvertiseAddress.PeerAdvertiseIP != "" {
			err = append(err, fmt.Errorf("--peer-advertise-ip requires UnknownVersionInteroperabilityProxy feature to be turned on"))
		}
		if options.PeerAdvertiseAddress.PeerAdvertisePort != "" {
			err = append(err, fmt.Errorf("--peer-advertise-port requires UnknownVersionInteroperabilityProxy feature to be turned on"))
		}
	}
	return err
}

// abstractSocketRegex 校验以 @ 开头的 abstract socket 名称是否合法。
var abstractSocketRegex = regexp.MustCompile(`^@([a-zA-Z0-9_-]+\.)*[a-zA-Z0-9_-]+$`)

// validateServiceAccountTokenSigningConfig 校验外部 JWT 签发端点与其他签发方式的互斥关系及特性开关。
func validateServiceAccountTokenSigningConfig(options *Options) []error {
	if len(options.ServiceAccountSigningEndpoint) == 0 {
		return nil
	}

	errors := []error{}

	if len(options.ServiceAccountSigningKeyFile) != 0 || len(options.Authentication.ServiceAccounts.KeyFiles) != 0 {
		errors = append(errors, fmt.Errorf("can't set `--service-account-signing-key-file` and/or `--service-account-key-file` with `--service-account-signing-endpoint` (They are mutually exclusive)"))
	}
	if !utilfeature.DefaultFeatureGate.Enabled(features.ExternalServiceAccountTokenSigner) {
		errors = append(errors, fmt.Errorf("setting `--service-account-signing-endpoint` requires enabling ExternalServiceAccountTokenSigner feature gate"))
	}
	// 以 @ 开头时必须是合法的 abstract socket 名称。
	if strings.HasPrefix(options.ServiceAccountSigningEndpoint, "@") && !abstractSocketRegex.MatchString(options.ServiceAccountSigningEndpoint) {
		errors = append(errors, fmt.Errorf("invalid value %q passed for `--service-account-signing-endpoint`, when prefixed with @ must be a valid abstract socket name", options.ServiceAccountSigningEndpoint))
	}

	return errors
}

// validateCoordinatedLeadershipFlags 校验协调式选主的租约时长、续约期限与重试周期满足递减关系。
func validateCoordinatedLeadershipFlags(options *Options) []error {
	var errs []error
	if options.CoordinatedLeadershipLeaseDuration <= options.CoordinatedLeadershipRenewDeadline {
		errs = append(errs, fmt.Errorf("--coordinated-leadership-lease-duration must be greater than --coordinated-leadership-renew-deadline"))
	}
	if options.CoordinatedLeadershipRenewDeadline <= options.CoordinatedLeadershipRetryPeriod {
		errs = append(errs, fmt.Errorf("--coordinated-leadership-renew-deadline must be greater than --coordinated-leadership-retry-period"))
	}
	return errs
}

// Validate 校验 Options 并返回发现的所有错误。
func (s *Options) Validate() []error {
	var errs []error

	errs = append(errs, s.GenericServerRunOptions.Validate()...)
	errs = append(errs, s.Etcd.Validate()...)
	errs = append(errs, validateAPIPriorityAndFairness(s)...)
	errs = append(errs, s.SecureServing.Validate()...)
	errs = append(errs, s.Authentication.Validate()...)
	errs = append(errs, s.Authorization.Validate()...)
	errs = append(errs, s.Audit.Validate()...)
	errs = append(errs, s.Admission.Validate()...)
	errs = append(errs, s.APIEnablement.Validate(legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme)...)
	errs = append(errs, validateTokenRequest(s)...)
	errs = append(errs, s.Metrics.Validate()...)
	errs = append(errs, validateUnknownVersionInteroperabilityProxyFlags(s)...)
	errs = append(errs, validateServiceAccountTokenSigningConfig(s)...)
	errs = append(errs, validateCoordinatedLeadershipFlags(s)...)

	return errs
}
