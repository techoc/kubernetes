/*
Copyright 2017 The Kubernetes Authors.

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

// Package policy 负责从磁盘加载审计策略，并校验其版本与内容。
package policy

import (
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	"k8s.io/apiserver/pkg/apis/audit/validation"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/klog/v2"
)

// apiGroupVersions 是审计策略文件允许声明的版本白名单。新增版本时需要同时
// 在这里登记，否则加载会以 "unknown group version" 拒绝。
var (
	apiGroupVersions = []schema.GroupVersion{
		auditv1.SchemeGroupVersion,
	}
	apiGroupVersionSet = map[schema.GroupVersion]bool{}
)

func init() {
	for _, gv := range apiGroupVersions {
		apiGroupVersionSet[gv] = true
	}
}

// LoadPolicyFromFile 读取并解析指定路径的审计策略文件。
//
// 文件内容为空或路径缺失都会返回错误：审计策略缺失属于启动期配置错误，
// 静默地退化为"不审计"会掩盖安全问题，因此调用方必须显式处理。
func LoadPolicyFromFile(filePath string) (*auditinternal.Policy, error) {
	if filePath == "" {
		return nil, fmt.Errorf("file path not specified")
	}
	policyDef, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file path %q: %+v", filePath, err)
	}

	ret, err := LoadPolicyFromBytes(policyDef)
	if err != nil {
		return nil, fmt.Errorf("%v: from file %v", err.Error(), filePath)
	}

	return ret, nil
}

// LoadPolicyFromBytes 解析审计策略字节内容，返回内部版本的 Policy。
//
// 解码策略：先严格解码，若仅因存在未知字段而失败，则退回到宽松解码并打印
// 警告。这样可以向前兼容——新版策略文件降级给旧版 apiserver 时不会直接
// 启动失败，但仍会提示配置中可能有被忽略的字段。
func LoadPolicyFromBytes(policyDef []byte) (*auditinternal.Policy, error) {
	policy := &auditinternal.Policy{}
	strictDecoder := serializer.NewCodecFactory(audit.Scheme, serializer.EnableStrict).UniversalDecoder()

	// Try strict decoding first.
	_, gvk, err := strictDecoder.Decode(policyDef, nil, policy)
	if err != nil {
		if !runtime.IsStrictDecodingError(err) {
			return nil, fmt.Errorf("failed decoding: %w", err)
		}
		// 严格解码失败但错误类型表明只是未知字段：放宽到非严格解码，
		// 忽略无法识别的字段继续加载。
		var (
			lenientDecoder = audit.Codecs.UniversalDecoder(apiGroupVersions...)
			lenientErr     error
		)
		_, gvk, lenientErr = lenientDecoder.Decode(policyDef, nil, policy)
		if lenientErr != nil {
			return nil, fmt.Errorf("failed lenient decoding: %w", lenientErr)
		}
		klog.Warningf("Audit policy contains errors, falling back to lenient decoding: %v", err)
	}

	// Ensure the policy file contained an apiVersion and kind.
	// 未写 apiVersion/kind 时 gvk 为空，不在白名单内，会被下面拒绝。
	gv := schema.GroupVersion{Group: gvk.Group, Version: gvk.Version}
	if !apiGroupVersionSet[gv] {
		return nil, fmt.Errorf("unknown group version field %v in policy", gvk)
	}

	if err := validation.ValidatePolicy(policy); err != nil {
		return nil, err.ToAggregate()
	}

	// 空规则集会让审计静默失效，几乎必然是配置错误，直接拒绝。
	policyCnt := len(policy.Rules)
	if policyCnt == 0 {
		return nil, fmt.Errorf("loaded illegal policy with 0 rules")
	}

	klog.V(4).InfoS("Load audit policy rules success", "policyCnt", policyCnt)
	return policy, nil
}
