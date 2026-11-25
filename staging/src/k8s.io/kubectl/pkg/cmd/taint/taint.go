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

package taint

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/explain"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// TaintOptions 包含执行 taint 操作所需的数据
type TaintOptions struct {
	PrintFlags *genericclioptions.PrintFlags
	ToPrinter  func(string) (printers.ResourcePrinter, error)

	DryRunStrategy      cmdutil.DryRunStrategy
	ValidationDirective string

	resources      []string
	taintsToAdd    []v1.Taint
	taintsToRemove []v1.Taint
	builder        *resource.Builder
	selector       string
	overwrite      bool
	all            bool
	fieldManager   string

	ClientForMapping func(*meta.RESTMapping) (resource.RESTClient, error)

	genericiooptions.IOStreams

	Mapper meta.RESTMapper
}

var (
	taintLong = templates.LongDesc(i18n.T(`
		Update the taints on one or more nodes.

		* A taint consists of a key, value, and effect. As an argument here, it is expressed as key=value:effect.
		* The key must begin with a letter or number, and may contain letters, numbers, hyphens, dots, and underscores, up to %[1]d characters.
		* Optionally, the key can begin with a DNS subdomain prefix and a single '/', like example.com/my-app.
		* The value is optional. If given, it must begin with a letter or number, and may contain letters, numbers, hyphens, dots, and underscores, up to %[2]d characters.
		* The effect must be NoSchedule, PreferNoSchedule or NoExecute.
		* Currently taint can only apply to node.`))

	taintExample = templates.Examples(i18n.T(`
		# Update node 'foo' with a taint with key 'dedicated' and value 'special-user' and effect 'NoSchedule'
		# If a taint with that key and effect already exists, its value is replaced as specified
		kubectl taint nodes foo dedicated=special-user:NoSchedule

		# Remove from node 'foo' the taint with key 'dedicated' and effect 'NoSchedule' if one exists
		kubectl taint nodes foo dedicated:NoSchedule-

		# Remove from node 'foo' all the taints with key 'dedicated'
		kubectl taint nodes foo dedicated-

		# Add a taint with key 'dedicated' on nodes having label myLabel=X
		kubectl taint node -l myLabel=X  dedicated=foo:PreferNoSchedule

		# Add to node 'foo' a taint with key 'bar' and no value
		kubectl taint nodes foo bar:NoSchedule`))
)

func NewCmdTaint(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	options := &TaintOptions{
		PrintFlags: genericclioptions.NewPrintFlags("tainted").WithTypeSetter(scheme.Scheme),
		IOStreams:  streams,
	}

	validArgs := []string{"node"}

	cmd := &cobra.Command{
		Use:                   "taint NODE NAME KEY_1=VAL_1:TAINT_EFFECT_1 ... KEY_N=VAL_N:TAINT_EFFECT_N",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Update the taints on one or more nodes"),
		Long:                  fmt.Sprintf(taintLong, validation.DNS1123SubdomainMaxLength, validation.LabelValueMaxLength),
		Example:               taintExample,
		ValidArgsFunction:     completion.SpecifiedResourceTypeAndNameCompletionFunc(f, validArgs),
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(options.Complete(f, cmd, args))
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.RunTaint())
		},
	}

	options.PrintFlags.AddFlags(cmd)
	cmdutil.AddDryRunFlag(cmd)
	cmdutil.AddValidateFlags(cmd)
	cmdutil.AddLabelSelectorFlagVar(cmd, &options.selector)
	cmd.Flags().BoolVar(&options.overwrite, "overwrite", options.overwrite, "If true, allow taints to be overwritten, otherwise reject taint updates that overwrite existing taints.")
	cmd.Flags().BoolVar(&options.all, "all", options.all, "Select all nodes in the cluster")
	cmdutil.AddFieldManagerFlagVar(cmd, &options.fieldManager, "kubectl-taint")
	return cmd
}

// Complete 从命令行参数和 factory 中整理出所需的数据。
func (o *TaintOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) (err error) {
	namespace, _, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.Mapper, err = f.ToRESTMapper()
	if err != nil {
		return err
	}

	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}
	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)

	o.ValidationDirective, err = cmdutil.GetValidationDirective(cmd)
	if err != nil {
		return err
	}

	// 从 args 中提取资源和 taint 参数
	// 同时检查参数，确保所有资源都在 taint 之前指定
	taintArgs := []string{}
	metTaintArg := false
	for _, s := range args {
		isTaint := strings.Contains(s, "=") || strings.Contains(s, ":") || strings.HasSuffix(s, "-")
		switch {
		case !metTaintArg && isTaint:
			metTaintArg = true
			fallthrough
		case metTaintArg && isTaint:
			taintArgs = append(taintArgs, s)
		case !metTaintArg && !isTaint:
			o.resources = append(o.resources, s)
		case metTaintArg && !isTaint:
			return fmt.Errorf("all resources must be specified before taint changes: %s", s)
		}
	}

	o.ToPrinter = func(operation string) (printers.ResourcePrinter, error) {
		o.PrintFlags.NamePrintFlags.Operation = operation
		return o.PrintFlags.ToPrinter()
	}

	if len(o.resources) < 1 {
		return fmt.Errorf("one or more resources must be specified as <resource> <name>")
	}
	if len(taintArgs) < 1 {
		return fmt.Errorf("at least one taint update is required")
	}

	if o.taintsToAdd, o.taintsToRemove, err = parseTaints(taintArgs); err != nil {
		return cmdutil.UsageErrorf(cmd, "%s", err.Error())
	}
	o.builder = f.NewBuilder().
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		ContinueOnError().
		NamespaceParam(namespace).DefaultNamespace()
	if o.selector != "" {
		o.builder = o.builder.LabelSelectorParam(o.selector).ResourceTypes("node")
	}
	if o.all {
		o.builder = o.builder.SelectAllParam(o.all).ResourceTypes("node").Flatten().Latest()
	}
	if !o.all && o.selector == "" && len(o.resources) >= 2 {
		o.builder = o.builder.ResourceNames("node", o.resources[1:]...)
	}
	o.builder = o.builder.LabelSelectorParam(o.selector).
		Flatten().
		Latest()

	o.ClientForMapping = f.ClientForMapping
	return nil
}

// validateFlags 校验 kubectl taint 的各个 flag。
func (o TaintOptions) validateFlags() error {
	// selector 非空时不能同时设置 all，两者互斥。
	if o.all && o.selector != "" {
		return fmt.Errorf("setting 'all' parameter with a non empty selector is prohibited")
	}
	// selector 和 all 均未设置的情况。
	if !o.all && o.selector == "" {
		if len(o.resources) < 2 {
			return fmt.Errorf("at least one resource name must be specified since 'all' parameter is not set")
		} else {
			return nil
		}
	}
	return nil
}

// Validate 检查 TaintOptions 中是否有足够的信息来运行命令。
func (o TaintOptions) Validate() error {
	resourceType := strings.ToLower(o.resources[0])
	fullySpecifiedGVR, _, err := explain.SplitAndParseResourceRequest(resourceType, o.Mapper)
	if err != nil {
		return err
	}

	gvk, err := o.Mapper.KindFor(fullySpecifiedGVR)
	if err != nil {
		return err
	}

	if gvk.Kind != "Node" {
		return fmt.Errorf("invalid resource type %s, only node types are supported", resourceType)
	}

	// 检查 taint 参数的格式，并确认待删除的 taint 不在新增 taint 列表中
	var conflictTaints []string
	for _, taintAdd := range o.taintsToAdd {
		for _, taintRemove := range o.taintsToRemove {
			if taintAdd.Key != taintRemove.Key {
				continue
			}
			if len(taintRemove.Effect) == 0 || taintAdd.Effect == taintRemove.Effect {
				conflictTaint := fmt.Sprintf("%s=%s", taintRemove.Key, taintRemove.Effect)
				conflictTaints = append(conflictTaints, conflictTaint)
			}
		}
	}
	if len(conflictTaints) > 0 {
		return fmt.Errorf("can not both modify and remove the following taint(s) in the same command: %s", strings.Join(conflictTaints, ", "))
	}
	return o.validateFlags()
}

// RunTaint 执行具体操作
func (o TaintOptions) RunTaint() error {
	r := o.builder.Do()
	if err := r.Err(); err != nil {
		return err
	}

	return r.Visit(func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}

		obj := info.Object
		name, namespace := info.Name, info.Namespace
		oldData, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		operation, err := o.updateTaints(obj)
		if err != nil {
			return err
		}
		newData, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, obj)
		createdPatch := err == nil
		if err != nil {
			klog.V(2).Infof("couldn't compute patch: %v", err)
		}

		printer, err := o.ToPrinter(operation)
		if err != nil {
			return err
		}
		if o.DryRunStrategy == cmdutil.DryRunClient {
			if createdPatch {
				typedObj, err := scheme.Scheme.ConvertToVersion(info.Object, info.Mapping.GroupVersionKind.GroupVersion())
				if err != nil {
					return err
				}

				nodeObj, ok := typedObj.(*v1.Node)
				if !ok {
					return fmt.Errorf("unexpected type %T", typedObj)
				}

				originalObjJS, err := json.Marshal(nodeObj)
				if err != nil {
					return err
				}

				originalPatchedObjJS, err := strategicpatch.StrategicMergePatch(originalObjJS, patchBytes, nodeObj)
				if err != nil {
					return err
				}

				targetObj, err := runtime.Decode(unstructured.UnstructuredJSONScheme, originalPatchedObjJS)
				if err != nil {
					return err
				}
				return printer.PrintObj(targetObj, o.Out)
			}
			return printer.PrintObj(obj, o.Out)
		}

		mapping := info.ResourceMapping()
		client, err := o.ClientForMapping(mapping)
		if err != nil {
			return err
		}
		helper := resource.
			NewHelper(client, mapping).
			DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
			WithFieldManager(o.fieldManager).
			WithFieldValidation(o.ValidationDirective)

		var outputObj runtime.Object
		if createdPatch {
			outputObj, err = helper.Patch(namespace, name, types.StrategicMergePatchType, patchBytes, nil)
		} else {
			outputObj, err = helper.Replace(namespace, name, false, obj)
		}
		if err != nil {
			return err
		}

		return printer.PrintObj(outputObj, o.Out)
	})
}

// updateTaints 在计算操作的最终效果（即是否会导致覆盖）之后，将 taint 选项 (o) 应用到集群中的节点，并以用户易于理解的方式报告最终结果。
func (o TaintOptions) updateTaints(obj runtime.Object) (string, error) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return "", fmt.Errorf("unexpected type %T, expected Node", obj)
	}
	if !o.overwrite {
		if exists := checkIfTaintsAlreadyExists(node.Spec.Taints, o.taintsToAdd); len(exists) != 0 {
			return "", fmt.Errorf("node %s already has %v taint(s) with same effect(s) and --overwrite is false", node.Name, exists)
		}
	}
	operation, newTaints, err := reorganizeTaints(node, o.overwrite, o.taintsToAdd, o.taintsToRemove)
	if err != nil {
		return "", err
	}
	node.Spec.Taints = newTaints
	return operation, nil
}
