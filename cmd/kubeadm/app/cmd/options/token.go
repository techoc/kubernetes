/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	bootstrapapi "k8s.io/cluster-bootstrap/token/api"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
)

// NewBootstrapTokenOptions 创建带默认值的 BootstrapTokenOptions 对象。
func NewBootstrapTokenOptions() *BootstrapTokenOptions {
	bto := &BootstrapTokenOptions{&bootstraptokenv1.BootstrapToken{}, ""}
	bootstraptokenv1.SetDefaults_BootstrapToken(bto.BootstrapToken)
	return bto
}

// BootstrapTokenOptions 是包装结构体，用于向 FlagSet 添加引导令牌相关参数，
// 并在运行时将解析后的参数应用到 InitConfiguration 对象。
// TODO: 未来可能改用更好的方式组织参数，而不是像现在这样逐个添加。
type BootstrapTokenOptions struct {
	*bootstraptokenv1.BootstrapToken
	TokenStr string `datapolicy:"token"`
}

// AddTokenFlag 将 --token 参数添加到指定参数集。
func (bto *BootstrapTokenOptions) AddTokenFlag(fs *pflag.FlagSet) {
	fs.StringVar(
		&bto.TokenStr, TokenStr, "",
		"The token to use for establishing bidirectional trust between nodes and control-plane nodes. The format is [a-z0-9]{6}\\.[a-z0-9]{16} - e.g. abcdef.0123456789abcdef",
	)
}

// AddTTLFlag 将 --token-ttl 参数添加到指定参数集。
func (bto *BootstrapTokenOptions) AddTTLFlag(fs *pflag.FlagSet) {
	bto.AddTTLFlagWithName(fs, TokenTTL)
}

// AddTTLFlagWithName 将带自定义名称的 --token-ttl 参数添加到指定参数集。
func (bto *BootstrapTokenOptions) AddTTLFlagWithName(fs *pflag.FlagSet, flagName string) {
	fs.DurationVar(
		&bto.TTL.Duration, flagName, bto.TTL.Duration,
		"The duration before the token is automatically deleted (e.g. 1s, 2m, 3h). If set to '0', the token will never expire",
	)
}

// AddUsagesFlag 将 --usages 参数添加到指定参数集。
func (bto *BootstrapTokenOptions) AddUsagesFlag(fs *pflag.FlagSet) {
	fs.StringSliceVar(
		&bto.Usages, TokenUsages, bto.Usages,
		fmt.Sprintf("Describes the ways in which this token can be used. You can pass --usages multiple times or provide a comma separated list of options. Valid options: [%s]", strings.Join(bootstraptokenv1.DefaultTokenUsages, ",")),
	)
}

// AddGroupsFlag 将 --groups 参数添加到指定参数集。
func (bto *BootstrapTokenOptions) AddGroupsFlag(fs *pflag.FlagSet) {
	fs.StringSliceVar(
		&bto.Groups, TokenGroups, bto.Groups,
		fmt.Sprintf("Extra groups that this token will authenticate as when used for authentication. Must match %q", bootstrapapi.BootstrapGroupPattern),
	)
}

// AddDescriptionFlag 将 --description 参数添加到指定参数集。
func (bto *BootstrapTokenOptions) AddDescriptionFlag(fs *pflag.FlagSet) {
	fs.StringVar(
		&bto.Description, TokenDescription, bto.Description,
		"A human friendly description of how this token is used.",
	)
}

// ApplyTo 在运行时将 BootstrapTokenOptions 内部设置的值应用到 InitConfiguration 对象。
// 若命令行以字符串形式指定 --token，则在加入 BootstrapToken 对象前会先解析并校验。
func (bto *BootstrapTokenOptions) ApplyTo(cfg *kubeadmapiv1.InitConfiguration) error {
	if len(bto.TokenStr) > 0 {
		var err error
		bto.Token, err = bootstraptokenv1.NewBootstrapTokenString(bto.TokenStr)
		if err != nil {
			return err
		}
	}

	// 未指定 --config 时，将参数指定的令牌设为要创建的第一个也是唯一一个令牌。
	cfg.BootstrapTokens = []bootstraptokenv1.BootstrapToken{*bto.BootstrapToken}
	return nil
}
