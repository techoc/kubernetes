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

package rollout

import (
	"fmt"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/kubectl/pkg/cmd/set"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// PauseOptions 是执行该操作所需数据的起点。新增字段时应添加到此处，
// 而不是引用 cmd.Flags()
type PauseOptions struct {
	PrintFlags *genericclioptions.PrintFlags
	ToPrinter  func(string) (printers.ResourcePrinter, error)

	Pauser           polymorphichelpers.ObjectPauserFunc
	Builder          func() *resource.Builder
	Namespace        string
	EnforceNamespace bool
	Resources        []string
	LabelSelector    string

	resource.FilenameOptions
	genericiooptions.IOStreams

	fieldManager string
}

var (
	pauseLong = templates.LongDesc(i18n.T(`
		Mark the provided resource as paused.

		Paused resources will not be reconciled by a controller.
		Use "kubectl rollout resume" to resume a paused resource.
		Currently only deployments support being paused.`))

	pauseExample = templates.Examples(`
		# Mark the nginx deployment as paused
		# Any current state of the deployment will continue its function; new updates
		# to the deployment will not have an effect as long as the deployment is paused
		kubectl rollout pause deployment/nginx`)
)

// NewCmdRolloutPause 返回 'rollout pause' 子命令的 Command 实例
func NewCmdRolloutPause(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := &PauseOptions{
		PrintFlags: genericclioptions.NewPrintFlags("paused").WithTypeSetter(scheme.Scheme),
		IOStreams:  streams,
	}

	validArgs := []string{"deployment"}

	cmd := &cobra.Command{
		Use:                   "pause RESOURCE",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Mark the provided resource as paused"),
		Long:                  pauseLong,
		Example:               pauseExample,
		ValidArgsFunction:     completion.SpecifiedResourceTypeAndNameCompletionFunc(f, validArgs),
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.RunPause())
		},
	}

	o.PrintFlags.AddFlags(cmd)

	usage := "identifying the resource to get from a server."
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, usage)
	cmdutil.AddFieldManagerFlagVar(cmd, &o.fieldManager, "kubectl-rollout")
	cmdutil.AddLabelSelectorFlagVar(cmd, &o.LabelSelector)
	return cmd
}

// Complete 补全所有必需的选项
func (o *PauseOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	o.Pauser = polymorphichelpers.ObjectPauserFn

	var err error
	o.Namespace, o.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.Resources = args
	o.Builder = f.NewBuilder

	o.ToPrinter = func(operation string) (printers.ResourcePrinter, error) {
		o.PrintFlags.NamePrintFlags.Operation = operation
		return o.PrintFlags.ToPrinter()
	}

	return nil
}

func (o *PauseOptions) Validate() error {
	if len(o.Resources) == 0 && cmdutil.IsFilenameSliceEmpty(o.Filenames, o.Kustomize) {
		return fmt.Errorf("required resource not specified")
	}
	return nil
}

// RunPause 执行 'rollout pause' 子命令
func (o *PauseOptions) RunPause() error {
	r := o.Builder().
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		NamespaceParam(o.Namespace).DefaultNamespace().
		LabelSelectorParam(o.LabelSelector).
		FilenameParam(o.EnforceNamespace, &o.FilenameOptions).
		ResourceTypeOrNameArgs(true, o.Resources...).
		ContinueOnError().
		Latest().
		Flatten().
		Do()
	if err := r.Err(); err != nil {
		return err
	}

	allErrs := []error{}
	infos, err := r.Infos()
	if err != nil {
		// 恢复此前命令的行为：
		// 当至少有一个损坏的对象导致
		// 获取 infos 出错时，不会立即返回，
		// 而是先汇总所有错误，
		// 然后一并返回。
		allErrs = append(allErrs, err)
	}

	patches := set.CalculatePatches(infos, scheme.DefaultJSONEncoder(), set.PatchFn(o.Pauser))

	if len(patches) == 0 && len(allErrs) == 0 {
		fmt.Fprintf(o.ErrOut, "No resources found in %s namespace.\n", o.Namespace)
		return nil
	}

	for _, patch := range patches {
		info := patch.Info

		if patch.Err != nil {
			resourceString := info.Mapping.Resource.Resource
			if len(info.Mapping.Resource.Group) > 0 {
				resourceString = resourceString + "." + info.Mapping.Resource.Group
			}
			allErrs = append(allErrs, fmt.Errorf("error: %s %q %v", resourceString, info.Name, patch.Err))
			continue
		}

		if string(patch.Patch) == "{}" || len(patch.Patch) == 0 {
			printer, err := o.ToPrinter("already paused")
			if err != nil {
				allErrs = append(allErrs, err)
				continue
			}
			if err = printer.PrintObj(info.Object, o.Out); err != nil {
				allErrs = append(allErrs, err)
			}
			continue
		}

		obj, err := resource.NewHelper(info.Client, info.Mapping).
			WithFieldManager(o.fieldManager).
			Patch(info.Namespace, info.Name, types.StrategicMergePatchType, patch.Patch, nil)
		if err != nil {
			allErrs = append(allErrs, fmt.Errorf("failed to patch: %v", err))
			continue
		}

		info.Refresh(obj, true)
		printer, err := o.ToPrinter("paused")
		if err != nil {
			allErrs = append(allErrs, err)
			continue
		}
		if err = printer.PrintObj(info.Object, o.Out); err != nil {
			allErrs = append(allErrs, err)
		}
	}

	return utilerrors.NewAggregate(allErrs)
}
