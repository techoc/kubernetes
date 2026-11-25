/*
Copyright 2015 The Kubernetes Authors.

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

package create

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	namespaceLong = templates.LongDesc(i18n.T(`
		Create a namespace with the specified name.`))

	namespaceExample = templates.Examples(i18n.T(`
	  # Create a new namespace named my-namespace
	  kubectl create namespace my-namespace`))
)

// NamespaceOptions 是 'create namespace' 子命令的选项
type NamespaceOptions struct {
	// PrintFlags 保存获取 printer 所需的选项
	PrintFlags *genericclioptions.PrintFlags
	// 待创建资源的名称
	Name string

	DryRunStrategy      cmdutil.DryRunStrategy
	ValidationDirective string
	CreateAnnotation    bool
	FieldManager        string

	Client *coreclient.CoreV1Client

	PrintObj func(obj runtime.Object) error

	genericiooptions.IOStreams
}

// NewNamespaceOptions 创建一个带有合理默认值的 *NamespaceOptions
func NewNamespaceOptions(ioStreams genericiooptions.IOStreams) *NamespaceOptions {
	return &NamespaceOptions{
		PrintFlags: genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		IOStreams:  ioStreams,
	}
}

// NewCmdCreateNamespace 是一个用于创建新 namespace 的宏命令
func NewCmdCreateNamespace(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {

	o := NewNamespaceOptions(ioStreams)

	cmd := &cobra.Command{
		Use:                   "namespace NAME [--dry-run=server|client|none]",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"ns"},
		Short:                 i18n.T("Create a namespace with the specified name"),
		Long:                  namespaceLong,
		Example:               namespaceExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}

	o.PrintFlags.AddFlags(cmd)

	cmdutil.AddApplyAnnotationFlags(cmd)
	cmdutil.AddValidateFlags(cmd)
	cmdutil.AddDryRunFlag(cmd)
	cmdutil.AddFieldManagerFlagVar(cmd, &o.FieldManager, "kubectl-create")

	return cmd
}

// Complete 完成所有必需选项的设置
func (o *NamespaceOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	name, err := NameFromCommandArgs(cmd, args)
	if err != nil {
		return err
	}

	restConfig, err := f.ToRESTConfig()
	if err != nil {
		return err
	}
	o.Client, err = coreclient.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	o.Name = name
	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}
	o.CreateAnnotation = cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag)
	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)
	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}
	o.PrintObj = func(obj runtime.Object) error {
		return printer.PrintObj(obj, o.Out)
	}

	o.ValidationDirective, err = cmdutil.GetValidationDirective(cmd)
	return err
}

// Run 执行 'create namespace' 子命令
func (o *NamespaceOptions) Run() error {
	namespace := o.createNamespace()
	if err := util.CreateOrUpdateAnnotation(o.CreateAnnotation, namespace, scheme.DefaultJSONEncoder()); err != nil {
		return err
	}

	if o.DryRunStrategy != cmdutil.DryRunClient {
		createOptions := metav1.CreateOptions{}
		if o.FieldManager != "" {
			createOptions.FieldManager = o.FieldManager
		}
		createOptions.FieldValidation = o.ValidationDirective
		if o.DryRunStrategy == cmdutil.DryRunServer {
			createOptions.DryRun = []string{metav1.DryRunAll}
		}
		var err error
		namespace, err = o.Client.Namespaces().Create(context.TODO(), namespace, createOptions)
		if err != nil {
			return err
		}
	}
	return o.PrintObj(namespace)
}

// createNamespace 使用配置的字段输出一个 namespace 对象
func (o *NamespaceOptions) createNamespace() *corev1.Namespace {
	namespace := &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: o.Name},
	}
	return namespace
}

// Validate 校验必填字段是否已设置，以支持结构化生成
func (o *NamespaceOptions) Validate() error {
	if len(o.Name) == 0 {
		return fmt.Errorf("name must be specified")
	}
	return nil
}
