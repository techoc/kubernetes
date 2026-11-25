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

package set

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// SetSelectorOptions 是执行该操作所需数据的起点。新增字段时请添加在这里，而不是
// 引用 cmd.Flags()
type SetSelectorOptions struct {
	// 已绑定
	ResourceBuilderFlags *genericclioptions.ResourceBuilderFlags
	PrintFlags           *genericclioptions.PrintFlags
	RecordFlags          *genericclioptions.RecordFlags
	dryRunStrategy       cmdutil.DryRunStrategy
	fieldManager         string

	// 由参数设置
	resources       []string
	selector        *metav1.LabelSelector
	resourceVersion string

	// 计算所得
	WriteToServer  bool
	PrintObj       printers.ResourcePrinterFunc
	Recorder       genericclioptions.Recorder
	ResourceFinder genericclioptions.ResourceFinder

	// 在初始化时设置
	genericiooptions.IOStreams
}

var (
	selectorLong = templates.LongDesc(i18n.T(`
		Set the selector on a resource. Note that the new selector will overwrite the old selector if the resource had one prior to the invocation
		of 'set selector'.

		A selector must begin with a letter or number, and may contain letters, numbers, hyphens, dots, and underscores, up to %[1]d characters.
		If --resource-version is specified, then updates will use this resource version, otherwise the existing resource-version will be used.
        Note: currently selectors can only be set on Service objects.`))
	selectorExample = templates.Examples(`
        # Set the labels and selector before creating a deployment/service pair
        kubectl create service clusterip my-svc --clusterip="None" -o yaml --dry-run=client | kubectl set selector --local -f - 'environment=qa' -o yaml | kubectl create -f -
        kubectl create deployment my-dep --image=nginx -o yaml --dry-run=client | kubectl label --local -f - environment=qa -o yaml | kubectl create -f -`)
)

// NewSelectorOptions 返回一个已初始化的 SelectorOptions 实例
func NewSelectorOptions(streams genericiooptions.IOStreams) *SetSelectorOptions {
	return &SetSelectorOptions{
		ResourceBuilderFlags: genericclioptions.NewResourceBuilderFlags().
			WithScheme(scheme.Scheme).
			WithAll(false).
			WithLocal(false).
			WithLatest(),
		PrintFlags:  genericclioptions.NewPrintFlags("selector updated").WithTypeSetter(scheme.Scheme),
		RecordFlags: genericclioptions.NewRecordFlags(),

		Recorder: genericclioptions.NoopRecorder{},

		IOStreams: streams,
	}
}

// NewCmdSelector 是 "set selector" 命令。
func NewCmdSelector(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := NewSelectorOptions(streams)

	cmd := &cobra.Command{
		Use:                   "selector (-f FILENAME | TYPE NAME) EXPRESSIONS [--resource-version=version]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Set the selector on a resource"),
		Long:                  fmt.Sprintf(selectorLong, validation.LabelValueMaxLength),
		Example:               selectorExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.RunSelector())
		},
	}

	o.ResourceBuilderFlags.AddFlags(cmd.Flags())
	o.PrintFlags.AddFlags(cmd)
	o.RecordFlags.AddFlags(cmd)
	cmdutil.AddFieldManagerFlagVar(cmd, &o.fieldManager, "kubectl-set")

	cmd.Flags().StringVarP(&o.resourceVersion, "resource-version", "", o.resourceVersion, "If non-empty, the selectors update will only succeed if this is the current resource-version for the object. Only valid when specifying a single resource.")
	cmdutil.AddDryRunFlag(cmd)

	return cmd
}

// Complete 根据参数赋值 SelectorOptions。
func (o *SetSelectorOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	var err error

	o.RecordFlags.Complete(cmd)
	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	o.dryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}

	o.resources, o.selector, err = getResourcesAndSelector(args)
	if err != nil {
		return err
	}

	o.ResourceFinder = o.ResourceBuilderFlags.ToBuilder(f, o.resources)
	o.WriteToServer = !(*o.ResourceBuilderFlags.Local || o.dryRunStrategy == cmdutil.DryRunClient)

	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.dryRunStrategy)
	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}
	o.PrintObj = printer.PrintObj

	return err
}

// Validate 校验基本输入
func (o *SetSelectorOptions) Validate() error {
	if o.selector == nil {
		return fmt.Errorf("one selector is required")
	}
	return nil
}

// RunSelector 执行该命令。
func (o *SetSelectorOptions) RunSelector() error {
	r := o.ResourceFinder.Do()

	return r.Visit(func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}
		patch := &Patch{Info: info}

		if len(o.resourceVersion) != 0 {
			// 从起始 JSON 中清除 resourceVersion，以确保补丁中总会发送它
			accessor, err := meta.Accessor(info.Object)
			if err != nil {
				return err
			}
			accessor.SetResourceVersion("")
		}

		CalculatePatch(patch, scheme.DefaultJSONEncoder(), func(obj runtime.Object) ([]byte, error) {

			if len(o.resourceVersion) != 0 {
				accessor, err := meta.Accessor(info.Object)
				if err != nil {
					return nil, err
				}
				accessor.SetResourceVersion(o.resourceVersion)
			}

			selectErr := updateSelectorForObject(info.Object, *o.selector)
			if selectErr != nil {
				return nil, selectErr
			}

			// 记录此次变更（用于 rollout 历史）
			if err := o.Recorder.Record(patch.Info.Object); err != nil {
				klog.V(4).Infof("error recording current command: %v", err)
			}

			return runtime.Encode(scheme.DefaultJSONEncoder(), info.Object)
		})

		if patch.Err != nil {
			return patch.Err
		}
		if !o.WriteToServer {
			return o.PrintObj(info.Object, o.Out)
		}

		actual, err := resource.
			NewHelper(info.Client, info.Mapping).
			DryRun(o.dryRunStrategy == cmdutil.DryRunServer).
			WithFieldManager(o.fieldManager).
			Patch(info.Namespace, info.Name, types.StrategicMergePatchType, patch.Patch, nil)
		if err != nil {
			return err
		}

		return o.PrintObj(actual, o.Out)
	})
}

func updateSelectorForObject(obj runtime.Object, selector metav1.LabelSelector) error {
	copyOldSelector := func() (map[string]string, error) {
		if len(selector.MatchExpressions) > 0 {
			return nil, fmt.Errorf("match expression %v not supported on this object", selector.MatchExpressions)
		}
		dst := make(map[string]string)
		for label, value := range selector.MatchLabels {
			dst[label] = value
		}
		return dst, nil
	}
	var err error
	switch t := obj.(type) {
	case *v1.Service:
		t.Spec.Selector, err = copyOldSelector()
	default:
		err = fmt.Errorf("setting a selector is only supported for Services")
	}
	return err
}

// getResourcesAndSelector 从给定参数中检索资源和 selector 表达式（假设 selector 是最后一个参数）
func getResourcesAndSelector(args []string) (resources []string, selector *metav1.LabelSelector, err error) {
	if len(args) == 0 {
		return []string{}, nil, nil
	}
	resources = args[:len(args)-1]
	selector, err = metav1.ParseToLabelSelector(args[len(args)-1])
	return resources, selector, err
}
