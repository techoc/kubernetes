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

package version

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/version"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// TODO(knverey): 一旦使用模块支持构建的 kubectl 能提供 BuildInfo，就移除这段硬编码。
const kustomizeVersion = "v5.8.1"

// Version 是承载版本信息的结构体
type Version struct {
	ClientVersion    *apimachineryversion.Info `json:"clientVersion,omitempty" yaml:"clientVersion,omitempty"`
	KustomizeVersion string                    `json:"kustomizeVersion,omitempty" yaml:"kustomizeVersion,omitempty"`
	ServerVersion    *apimachineryversion.Info `json:"serverVersion,omitempty" yaml:"serverVersion,omitempty"`
}

var (
	versionExample = templates.Examples(i18n.T(`
		# Print the client and server versions for the current context
		kubectl version`))
)

// Options 是支持 version 命令的结构体
type Options struct {
	ClientOnly       bool
	Output           string
	WarningsAsErrors bool

	args []string

	discoveryClient discovery.CachedDiscoveryInterface

	genericiooptions.IOStreams
}

// NewOptions 返回已初始化的 Options
func NewOptions(ioStreams genericiooptions.IOStreams) *Options {
	return &Options{
		IOStreams: ioStreams,
	}

}

// NewCmdVersion 返回用于获取版本信息的 cobra 命令
func NewCmdVersion(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewOptions(ioStreams)
	cmd := &cobra.Command{
		Use:     "version",
		Short:   i18n.T("Print the client and server version information"),
		Long:    i18n.T("Print the client and server version information for the current context."),
		Example: versionExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}
	cmd.Flags().BoolVar(&o.ClientOnly, "client", o.ClientOnly, "If true, shows client version only (no server required).")
	cmd.Flags().StringVarP(&o.Output, "output", "o", o.Output, "One of 'yaml' or 'json'.")
	return cmd
}

// Complete 补全所有必需的选项
func (o *Options) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	var err error

	o.WarningsAsErrors = cmd.Flags().Lookup("warnings-as-errors").Value.String() == "true"

	if o.ClientOnly {
		return nil
	}
	o.discoveryClient, err = f.ToDiscoveryClient()
	// 如果 rest.Config 为空，则继续并只打印客户端信息。
	// 如果错误不是因无法构建 rest.Config 引起的，则失败。
	if err != nil && !clientcmd.IsEmptyConfig(err) {
		return err
	}

	o.args = args
	return nil
}

// Validate 校验提供的选项
func (o *Options) Validate() error {
	if len(o.args) != 0 {
		return errors.New(fmt.Sprintf("extra arguments: %v", o.args))
	}

	if o.Output != "" && o.Output != "yaml" && o.Output != "json" {
		return errors.New(`--output must be 'yaml' or 'json'`)
	}

	return nil
}

// Run 执行 version 命令
func (o *Options) Run() error {
	var (
		serverErr   error
		versionInfo Version
	)

	versionInfo.ClientVersion = func() *apimachineryversion.Info { v := version.Get(); return &v }()
	versionInfo.KustomizeVersion = getKustomizeVersion()

	if !o.ClientOnly && o.discoveryClient != nil {
		// 始终向服务端请求最新数据
		o.discoveryClient.Invalidate()
		versionInfo.ServerVersion, serverErr = o.discoveryClient.ServerVersion()
	}

	switch o.Output {
	case "":
		fmt.Fprintf(o.Out, "Client Version: %s\n", versionInfo.ClientVersion.GitVersion)
		fmt.Fprintf(o.Out, "Kustomize Version: %s\n", versionInfo.KustomizeVersion)
		if versionInfo.ServerVersion != nil {
			fmt.Fprintf(o.Out, "Server Version: %s\n", versionInfo.ServerVersion.GitVersion)
		}
	case "yaml":
		marshalled, err := yaml.Marshal(&versionInfo)
		if err != nil {
			return err
		}
		fmt.Fprintln(o.Out, string(marshalled))
	case "json":
		marshalled, err := json.MarshalIndent(&versionInfo, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(o.Out, string(marshalled))
	default:
		// 如果走到这种情况，说明程序中存在 bug。
		// 不过，我们遵循绝不 panic 的原则。
		return fmt.Errorf("VersionOptions were not validated: --output=%q should have been rejected", o.Output)
	}

	if versionInfo.ServerVersion != nil {
		warningMessage, err := getVersionSkewWarning(*versionInfo.ClientVersion, *versionInfo.ServerVersion)
		if err != nil {
			return err
		}
		if warningMessage != "" {
			if o.WarningsAsErrors {
				return errors.New(warningMessage)
			}
			fmt.Fprintf(o.ErrOut, "Warning: %s\n", warningMessage) //nolint:errcheck
		}
	}
	return serverErr
}

func getKustomizeVersion() string {
	if modVersion, ok := GetKustomizeModVersion(); ok {
		return modVersion
	}
	return kustomizeVersion // other clients should provide their own fallback
}

func GetKustomizeModVersion() (string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, dep := range info.Deps {
		if dep.Path == "sigs.k8s.io/kustomize/kustomize/v4" || dep.Path == "sigs.k8s.io/kustomize/kustomize/v5" {
			return dep.Version, true
		}
	}
	return "", false
}
