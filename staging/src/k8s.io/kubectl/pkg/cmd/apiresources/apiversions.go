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

package apiresources

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/discovery"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	apiversionsExample = templates.Examples(i18n.T(`
		# Print the supported API versions
		kubectl api-versions`))
)

// APIVersionsOptions 包含 API 版本所需的数据
type APIVersionsOptions struct {
	discoveryClient discovery.CachedDiscoveryInterface

	genericiooptions.IOStreams
}

// NewAPIVersionsOptions 创建 APIVersions 所需的选项
func NewAPIVersionsOptions(ioStreams genericiooptions.IOStreams) *APIVersionsOptions {
	return &APIVersionsOptions{
		IOStreams: ioStreams,
	}
}

// NewCmdAPIVersions 创建 `api-versions` 命令
func NewCmdAPIVersions(restClientGetter genericclioptions.RESTClientGetter, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewAPIVersionsOptions(ioStreams)
	cmd := &cobra.Command{
		Use:                   "api-versions",
		Short:                 i18n.T("Print the supported API versions on the server, in the form of \"group/version\""),
		Long:                  i18n.T("Print the supported API versions on the server, in the form of \"group/version\"."),
		Example:               apiversionsExample,
		DisableFlagsInUseLine: true,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(restClientGetter, cmd, args))
			cmdutil.CheckErr(o.RunAPIVersions())
		},
	}
	return cmd
}

// Complete 将命令行参数和 factory 适配为所需的数据
func (o *APIVersionsOptions) Complete(restClientGetter genericclioptions.RESTClientGetter, cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return cmdutil.UsageErrorf(cmd, "unexpected arguments: %v", args)
	}
	var err error
	o.discoveryClient, err = restClientGetter.ToDiscoveryClient()
	return err
}

// RunAPIVersions 执行实际工作
func (o *APIVersionsOptions) RunAPIVersions() error {
	// 始终向服务端请求最新数据
	o.discoveryClient.Invalidate()

	groupList, err := o.discoveryClient.ServerGroups()
	if err != nil {
		return fmt.Errorf("couldn't get available api versions from server: %v", err)
	}
	apiVersions := metav1.ExtractGroupVersions(groupList)
	sort.Strings(apiVersions)
	for _, v := range apiVersions {
		fmt.Fprintln(o.Out, v)
	}
	return nil
}
