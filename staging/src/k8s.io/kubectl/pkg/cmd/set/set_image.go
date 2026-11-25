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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/tools/clientcmd"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// ImageOptions 是执行该操作所需数据的起点。新增字段时请添加在这里，而不是
// 引用 cmd.Flags()
type SetImageOptions struct {
	resource.FilenameOptions

	PrintFlags  *genericclioptions.PrintFlags
	RecordFlags *genericclioptions.RecordFlags

	Infos          []*resource.Info
	Selector       string
	DryRunStrategy cmdutil.DryRunStrategy
	All            bool
	Output         string
	Local          bool
	ResolveImage   ImageResolverFunc
	fieldManager   string

	PrintObj printers.ResourcePrinterFunc
	Recorder genericclioptions.Recorder

	UpdatePodSpecForObject polymorphichelpers.UpdatePodSpecForObjectFunc
	Resources              []string
	ContainerImages        map[string]string

	genericiooptions.IOStreams
}

// ImageResolver 是一个接收镜像名称的函数，
// 并将其解析为合适/兼容的镜像名称。
// 为将来的镜像解析方式提供灵活性。
type ImageResolverFunc func(in string) (string, error)

// 要使用的 ImageResolver。
var ImageResolver = resolveImageFunc

var (
	imageResources = i18n.T(`
  	pod (po), replicationcontroller (rc), deployment (deploy), daemonset (ds), statefulset (sts), cronjob (cj), replicaset (rs)`)

	imageLong = templates.LongDesc(i18n.T(`
		Update existing container image(s) of resources.

		Possible resources include (case insensitive):
		`) + imageResources)

	imageExample = templates.Examples(`
		# Set a deployment's nginx container image to 'nginx:1.9.1', and its busybox container image to 'busybox'
		kubectl set image deployment/nginx busybox=busybox nginx=nginx:1.9.1

		# Update all deployments' and rc's nginx container's image to 'nginx:1.9.1'
		kubectl set image deployments,rc nginx=nginx:1.9.1 --all

		# Update image of all containers of daemonset abc to 'nginx:1.9.1'
		kubectl set image daemonset abc *=nginx:1.9.1

		# Print result (in yaml format) of updating nginx container image from local file, without hitting the server
		kubectl set image -f path/to/file.yaml nginx=nginx:1.9.1 --local -o yaml`)
)

// NewImageOptions 返回一个已初始化的 SetImageOptions 实例
func NewImageOptions(streams genericiooptions.IOStreams) *SetImageOptions {
	return &SetImageOptions{
		PrintFlags:  genericclioptions.NewPrintFlags("image updated").WithTypeSetter(scheme.Scheme),
		RecordFlags: genericclioptions.NewRecordFlags(),

		Recorder: genericclioptions.NoopRecorder{},

		IOStreams: streams,
	}
}

// NewCmdImage 返回 'set image' 子命令的已初始化 Command 实例
func NewCmdImage(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := NewImageOptions(streams)
	validArgs := []string{"daemonsets", "deployments", "pods", "cronjobs", "replicasets", "replicationcontrollers", "statefulsets"}

	cmd := &cobra.Command{
		Use:                   "image (-f FILENAME | TYPE NAME) CONTAINER_NAME_1=CONTAINER_IMAGE_1 ... CONTAINER_NAME_N=CONTAINER_IMAGE_N",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Update the image of a pod template"),
		Long:                  imageLong,
		Example:               imageExample,
		ValidArgsFunction:     completion.SpecifiedResourceTypeAndNameNoRepeatCompletionFunc(f, validArgs),
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}

	o.PrintFlags.AddFlags(cmd)
	o.RecordFlags.AddFlags(cmd)

	usage := "identifying the resource to get from a server."
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, usage)
	cmd.Flags().BoolVar(&o.All, "all", o.All, "Select all resources, in the namespace of the specified resource types")
	cmd.Flags().BoolVar(&o.Local, "local", o.Local, "If true, set image will NOT contact api-server but run locally.")
	cmdutil.AddDryRunFlag(cmd)
	cmdutil.AddFieldManagerFlagVar(cmd, &o.fieldManager, "kubectl-set")
	cmdutil.AddLabelSelectorFlagVar(cmd, &o.Selector)

	return cmd
}

// Complete 补全所有必需的选项
func (o *SetImageOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	var err error

	o.RecordFlags.Complete(cmd)
	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	o.UpdatePodSpecForObject = polymorphichelpers.UpdatePodSpecForObjectFn
	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}

	o.Output = cmdutil.GetFlagString(cmd, "output")
	o.ResolveImage = ImageResolver

	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)
	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}

	o.PrintObj = printer.PrintObj

	cmdNamespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil && !(o.Local && clientcmd.IsEmptyConfig(err)) {
		return err
	}

	o.Resources, o.ContainerImages, err = getResourcesAndImages(args)
	if err != nil {
		return err
	}

	builder := f.NewBuilder().
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		LocalParam(o.Local).
		ContinueOnError().
		NamespaceParam(cmdNamespace).DefaultNamespace().
		FilenameParam(enforceNamespace, &o.FilenameOptions).
		Flatten()

	if !o.Local {
		builder.LabelSelectorParam(o.Selector).
			ResourceTypeOrNameArgs(o.All, o.Resources...).
			Latest()
	} else {
		// 如果提供了 --local 标志，且资源以
		// <resource>/<name> 形式指定，则立即失败，因为 --local 无法向 api server
		// 查询指定资源。
		if len(o.Resources) > 0 {
			return resource.LocalResourceError
		}
	}

	o.Infos, err = builder.Do().Infos()
	if err != nil {
		return err
	}

	return nil
}

// Validate 确保 SetImageOptions 中提供的值有效
func (o *SetImageOptions) Validate() error {
	errors := []error{}
	if o.All && len(o.Selector) > 0 {
		errors = append(errors, fmt.Errorf("cannot set --all and --selector at the same time"))
	}
	if len(o.Resources) < 1 && cmdutil.IsFilenameSliceEmpty(o.Filenames, o.Kustomize) {
		errors = append(errors, fmt.Errorf("one or more resources must be specified as <resource> <name> or <resource>/<name>"))
	}
	if len(o.ContainerImages) < 1 {
		errors = append(errors, fmt.Errorf("at least one image update is required"))
	} else if len(o.ContainerImages) > 1 && hasWildcardKey(o.ContainerImages) {
		errors = append(errors, fmt.Errorf("all containers are already specified by *, but saw more than one container_name=container_image pairs"))
	}
	if o.Local && o.DryRunStrategy == cmdutil.DryRunServer {
		errors = append(errors, fmt.Errorf("cannot specify --local and --dry-run=server - did you mean --dry-run=client?"))
	}
	return utilerrors.NewAggregate(errors)
}

// Run 执行 'set image' 子命令
func (o *SetImageOptions) Run() error {
	allErrs := []error{}

	patches := CalculatePatches(o.Infos, scheme.DefaultJSONEncoder(), func(obj runtime.Object) ([]byte, error) {
		_, err := o.UpdatePodSpecForObject(obj, func(spec *v1.PodSpec) error {
			for name, image := range o.ContainerImages {
				resolvedImageName, err := o.ResolveImage(image)
				if err != nil {
					allErrs = append(allErrs, fmt.Errorf("error: unable to resolve image %q for container %q: %v", image, name, err))
					if name == "*" {
						break
					}
					continue
				}

				initContainerFound := setImage(spec.InitContainers, name, resolvedImageName)
				containerFound := setImage(spec.Containers, name, resolvedImageName)
				if !containerFound && !initContainerFound {
					allErrs = append(allErrs, fmt.Errorf("error: unable to find container named %q", name))
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		// 记录此次变更（用于 rollout 历史）
		if err := o.Recorder.Record(obj); err != nil {
			klog.V(4).Infof("error recording current command: %v", err)
		}

		return runtime.Encode(scheme.DefaultJSONEncoder(), obj)
	})

	for _, patch := range patches {
		info := patch.Info
		if patch.Err != nil {
			name := info.ObjectName()
			allErrs = append(allErrs, fmt.Errorf("error: %s %v\n", name, patch.Err))
			continue
		}

		// 无变更
		if string(patch.Patch) == "{}" || len(patch.Patch) == 0 {
			continue
		}

		if o.Local || o.DryRunStrategy == cmdutil.DryRunClient {
			if err := o.PrintObj(info.Object, o.Out); err != nil {
				allErrs = append(allErrs, err)
			}
			continue
		}

		// 应用该变更的补丁
		actual, err := resource.
			NewHelper(info.Client, info.Mapping).
			DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
			WithFieldManager(o.fieldManager).
			Patch(info.Namespace, info.Name, types.StrategicMergePatchType, patch.Patch, nil)
		if err != nil {
			allErrs = append(allErrs, fmt.Errorf("failed to patch image update to pod template: %v", err))
			continue
		}

		if err := o.PrintObj(actual, o.Out); err != nil {
			allErrs = append(allErrs, err)
		}
	}
	return utilerrors.NewAggregate(allErrs)
}

func setImage(containers []v1.Container, containerName string, image string) bool {
	containerFound := false
	// 找到要更新的容器，并更新其镜像
	for i, c := range containers {
		if c.Name == containerName || containerName == "*" {
			containerFound = true
			containers[i].Image = image
		}
	}
	return containerFound
}

// getResourcesAndImages 从给定参数中检索资源及容器名:镜像对
func getResourcesAndImages(args []string) (resources []string, containerImages map[string]string, err error) {
	pairType := "image"
	resources, imageArgs, err := cmdutil.GetResourcesAndPairs(args, pairType)
	if err != nil {
		return
	}
	containerImages, _, err = cmdutil.ParsePairs(imageArgs, pairType, false)
	return
}

func hasWildcardKey(containerImages map[string]string) bool {
	_, ok := containerImages["*"]
	return ok
}

// 实现 ImageResolver
func resolveImageFunc(in string) (string, error) {
	return in, nil
}
