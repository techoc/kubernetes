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

package config

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/liggitt/tabwriter"
	"github.com/spf13/cobra"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// GetContextsOptions 包含从参数中可赋值的选项。
type GetContextsOptions struct {
	configAccess clientcmd.ConfigAccess
	nameOnly     bool
	showHeaders  bool
	contextNames []string

	outputFormat string
	noHeaders    bool

	genericiooptions.IOStreams
}

var (
	getContextsLong = templates.LongDesc(i18n.T(`Display one or many contexts from the kubeconfig file.`))

	getContextsExample = templates.Examples(`
		# List all the contexts in your kubeconfig file
		kubectl config get-contexts

		# Describe one context in your kubeconfig file
		kubectl config get-contexts my-context`)
)

// NewCmdConfigGetContexts 为 "get-contexts" 操作创建命令对象，
// 从 kubeconfig 中检索一个或多个 context。
func NewCmdConfigGetContexts(streams genericiooptions.IOStreams, configAccess clientcmd.ConfigAccess) *cobra.Command {
	options := &GetContextsOptions{
		configAccess: configAccess,

		IOStreams: streams,
	}

	cmd := &cobra.Command{
		Use:                   "get-contexts [(-o|--output=)name)]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Describe one or many contexts"),
		Long:                  getContextsLong,
		Example:               getContextsExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(options.Complete(cmd, args))
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.RunGetContexts())
		},
	}

	cmd.Flags().BoolVar(&options.noHeaders, "no-headers", options.noHeaders, "When using the default or custom-column output format, don't print headers (default print headers).")
	cmd.Flags().StringVarP(&options.outputFormat, "output", "o", options.outputFormat, `Output format. One of: (name).`)
	return cmd
}

// Complete 从参数中赋值 GetContextsOptions。
func (o *GetContextsOptions) Complete(cmd *cobra.Command, args []string) error {
	supportedOutputTypes := sets.New[string]("", "name")
	if !supportedOutputTypes.Has(o.outputFormat) {
		return fmt.Errorf("--output %v is not available in kubectl config get-contexts; resetting to default output format", o.outputFormat)
	}
	o.contextNames = args
	o.nameOnly = false
	if o.outputFormat == "name" {
		o.nameOnly = true
	}
	o.showHeaders = true
	if cmdutil.GetFlagBool(cmd, "no-headers") || o.nameOnly {
		o.showHeaders = false
	}

	return nil
}

// Validate 确保输出格式有效
func (o *GetContextsOptions) Validate() error {
	return nil
}

// RunGetContexts 实现 context 检索所需的全部功能。
func (o GetContextsOptions) RunGetContexts() error {
	config, err := o.configAccess.GetStartingConfig()
	if err != nil {
		return err
	}

	out, found := o.Out.(*tabwriter.Writer)
	if !found {
		out = printers.GetNewTabWriter(o.Out)
		defer out.Flush()
	}

	// 构建要打印的 context 名称列表，并在请求的 context 未找到时发出警告。
	// 在打印表头之前执行此操作，以免输出显得难看。
	allErrs := []error{}
	toPrint := []string{}
	if len(o.contextNames) == 0 {
		for name := range config.Contexts {
			toPrint = append(toPrint, name)
		}
	} else {
		for _, name := range o.contextNames {
			_, ok := config.Contexts[name]
			if ok {
				toPrint = append(toPrint, name)
			} else {
				allErrs = append(allErrs, fmt.Errorf("context %v not found", name))
			}
		}
	}
	if o.showHeaders {
		err = printContextHeaders(out, o.nameOnly)
		if err != nil {
			allErrs = append(allErrs, err)
		}
	}

	sort.Strings(toPrint)
	for _, name := range toPrint {
		err = printContext(name, config.Contexts[name], out, o.nameOnly, config.CurrentContext == name)
		if err != nil {
			allErrs = append(allErrs, err)
		}
	}

	return utilerrors.NewAggregate(allErrs)
}

func printContextHeaders(out io.Writer, nameOnly bool) error {
	columnNames := []string{"CURRENT", "NAME", "CLUSTER", "AUTHINFO", "NAMESPACE"}
	if nameOnly {
		columnNames = columnNames[:1]
	}
	_, err := fmt.Fprintf(out, "%s\n", strings.Join(columnNames, "\t"))
	return err
}

func printContext(name string, context *clientcmdapi.Context, w io.Writer, nameOnly, current bool) error {
	if nameOnly {
		_, err := fmt.Fprintf(w, "%s\n", name)
		return err
	}
	prefix := " "
	if current {
		prefix = "*"
	}
	_, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", prefix, name, context.Cluster, context.AuthInfo, context.Namespace)
	return err
}
