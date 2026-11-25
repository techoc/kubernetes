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

package v1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
)

const (
	// DefaultTokenDuration 指定引导令牌默认的有效时长。
	// 默认行为为 24 小时。
	DefaultTokenDuration = 24 * time.Hour
)

var (
	// DefaultTokenUsages 指定令牌默认具备的用途。
	DefaultTokenUsages = bootstrapapi.KnownTokenUsages

	// DefaultTokenGroups 指定此令牌用于身份验证时默认所属的组。
	DefaultTokenGroups = []string{"system:bootstrappers:kubeadm:default-node-token"}
)

// SetDefaults_BootstrapToken 为单个引导令牌设置默认值。
func SetDefaults_BootstrapToken(bt *BootstrapToken) {
	if bt.TTL == nil {
		bt.TTL = &metav1.Duration{
			Duration: DefaultTokenDuration,
		}
	}
	if len(bt.Usages) == 0 {
		bt.Usages = DefaultTokenUsages
	}

	if len(bt.Groups) == 0 {
		bt.Groups = DefaultTokenGroups
	}
}
