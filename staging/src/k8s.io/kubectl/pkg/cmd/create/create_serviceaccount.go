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

package create

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	serviceAccountLong = templates.LongDesc(i18n.T(`
		Create a service account with the specified name.`))

	serviceAccountExample = templates.Examples(i18n.T(`
	  # Create a new service account named my-service-account
	  kubectl create serviceaccount my-service-account`))
)

// ServiceAccountOpts 保存 'create serviceaccount' 子命令的选项
type ServiceAccountOpts struct {
	// PrintFlags 保存获取 printer 所需的选项
	PrintFlags *genericclioptions.PrintFlags
	PrintObj   func(obj runtime.Object) error
	// 待创建资源的名称
	Name                string
	DryRunStrategy      cmdutil.DryRunStrategy
	ValidationDirective string
	CreateAnnotation    bool
	FieldManager        string

	Namespace        string
	EnforceNamespace bool

	Mapper meta.RESTMapper
	Client *coreclient.CoreV1Client

	genericiooptions.IOStreams
}

// NewServiceAccountOpts 创建一个带有合理默认值的 *ServiceAccountOpts
func NewServiceAccountOpts(ioStreams genericiooptions.IOStreams) *ServiceAccountOpts {
	return &ServiceAccountOpts{
		PrintFlags: genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		IOStreams:  ioStreams,
	}
}

// NewCmdCreateServiceAccount 是一个用于创建新 service account 的宏命令
func NewCmdCreateServiceAccount(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewServiceAccountOpts(ioStreams)

	cmd := &cobra.Command{
		Use:                   "serviceaccount NAME [--dry-run=server|client|none]",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"sa"},
		Short:                 i18n.T("Create a service account with the specified name"),
		Long:                  serviceAccountLong,
		Example:               serviceAccountExample,
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
func (o *ServiceAccountOpts) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	var err error
	o.Name, err = NameFromCommandArgs(cmd, args)
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

	o.CreateAnnotation = cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag)

	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}

	o.Namespace, o.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)

	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}

	o.PrintObj = func(obj runtime.Object) error {
		return printer.PrintObj(obj, o.Out)
	}

	o.ValidationDirective, err = cmdutil.GetValidationDirective(cmd)
	if err != nil {
		return err
	}

	return nil
}

// Validate 检查 ServiceAccountOpts 是否包含运行命令所需的足够信息。
func (o *ServiceAccountOpts) Validate() error {
	if len(o.Name) == 0 {
		return fmt.Errorf("name must be specified")
	}
	return nil
}

// Run 向服务端发起 API 调用
func (o *ServiceAccountOpts) Run() error {
	serviceAccount, err := o.createServiceAccount()
	if err != nil {
		return err
	}

	if err := util.CreateOrUpdateAnnotation(o.CreateAnnotation, serviceAccount, scheme.DefaultJSONEncoder()); err != nil {
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
		serviceAccount, err = o.Client.ServiceAccounts(o.Namespace).Create(context.TODO(), serviceAccount, createOptions)
		if err != nil {
			return fmt.Errorf("failed to create serviceaccount: %v", err)
		}
	}
	return o.PrintObj(serviceAccount)
}

func (o *ServiceAccountOpts) createServiceAccount() (*corev1.ServiceAccount, error) {
	namespace := ""
	if o.EnforceNamespace {
		namespace = o.Namespace
	}
	serviceAccount := &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{APIVersion: corev1.SchemeGroupVersion.String(), Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      o.Name,
			Namespace: namespace,
		},
	}
	serviceAccount.Name = o.Name
	return serviceAccount, nil
}
