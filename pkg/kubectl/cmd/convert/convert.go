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

package convert

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/kubectl/pkg/validation"
	scheme "k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
)

var (
	convertLong = templates.LongDesc(i18n.T(`
		Convert config files between different API versions. Both YAML
		and JSON formats are accepted.

		The command takes filename, directory, or URL as input, and convert it into format
		of version specified by --output-version flag. If target version is not specified or
		not supported, convert to latest version.

		The default output will be printed to stdout in YAML format. One can use -o option
		to change to output destination.`))

	convertExample = templates.Examples(i18n.T(`
		# Convert 'pod.yaml' to latest version and print to stdout.
		kubectl convert -f pod.yaml

		# Convert the live state of the resource specified by 'pod.yaml' to the latest version
		# and print to stdout in JSON format.
		kubectl convert -f pod.yaml --local -o json

		# Convert all files under current directory to latest version and create them all.
		kubectl convert -f . | kubectl create -f -`))
)

// ConvertOptions 保存执行 convert 操作所需的参数与状态。
type ConvertOptions struct {
	PrintFlags *genericclioptions.PrintFlags
	Printer    printers.ResourcePrinter

	OutputVersion string
	Namespace     string

	builder   func() *resource.Builder
	local     bool
	validator func() (validation.Schema, error)

	resource.FilenameOptions
	genericiooptions.IOStreams
}

// NewConvertOptions 构造 ConvertOptions 并设置默认输出格式为 yaml、默认在本地转换。
func NewConvertOptions(ioStreams genericiooptions.IOStreams) *ConvertOptions {
	return &ConvertOptions{
		PrintFlags: genericclioptions.NewPrintFlags("converted").WithTypeSetter(scheme.Scheme).WithDefaultOutput("yaml"),
		local:      true,
		IOStreams:  ioStreams,
	}
}

// NewCmdConvert 创建 "convert" 命令对象，把配置文件转换为指定 API 版本。
func NewCmdConvert(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewConvertOptions(ioStreams)

	cmd := &cobra.Command{
		Use:                   "convert -f FILENAME",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Convert config files between different API versions"),
		Long:                  convertLong,
		Example:               convertExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd))
			cmdutil.CheckErr(o.RunConvert())
		},
	}

	cmd.Flags().BoolVar(&o.local, "local", o.local, "If true, convert will NOT try to contact api-server but run locally.")
	cmd.Flags().StringVar(&o.OutputVersion, "output-version", o.OutputVersion, i18n.T("Output the formatted object with the given group version (for ex: 'extensions/v1beta1')."))
	o.PrintFlags.AddFlags(cmd)

	cmdutil.AddValidateFlags(cmd)
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, "to need to get converted.")
	return cmd
}

// Complete 从命令行收集执行 Convert 命令所需的信息：文件名、命名空间、校验器和 printer。
func (o *ConvertOptions) Complete(f cmdutil.Factory, cmd *cobra.Command) (err error) {
	err = o.FilenameOptions.RequireFilenameOrKustomize()
	if err != nil {
		return err
	}
	o.builder = f.NewBuilder

	o.Namespace, _, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.validator = func() (validation.Schema, error) {
		directive, err := cmdutil.GetValidationDirective(cmd)
		if err != nil {
			return nil, err
		}
		return f.Validator(directive)
	}

	// build the printer
	o.Printer, err = o.PrintFlags.ToPrinter()
	return err
}

// RunConvert 实现 convert 命令：读取资源、转换为目标版本并输出。
func (o *ConvertOptions) RunConvert() error {
	b := o.builder().
		WithScheme(scheme.Scheme).
		LocalParam(o.local)
	// 非本地模式需要向 API server 校验资源，先构造校验器。
	if !o.local {
		schema, err := o.validator()
		if err != nil {
			return err
		}
		b.Schema(schema)
	}

	r := b.NamespaceParam(o.Namespace).
		ContinueOnError().
		FilenameParam(false, &o.FilenameOptions).
		Flatten().
		Do()

	err := r.Err()
	if err != nil {
		return err
	}

	singleItemImplied := false
	infos, err := r.IntoSingleItemImplied(&singleItemImplied).Infos()
	if err != nil {
		return err
	}

	if len(infos) == 0 {
		return fmt.Errorf("no objects passed to convert")
	}

	var specifiedOutputVersion schema.GroupVersion
	if len(o.OutputVersion) > 0 {
		specifiedOutputVersion, err = schema.ParseGroupVersion(o.OutputVersion)
		if err != nil {
			return err
		}
	}

	internalEncoder := scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...)
	internalVersionJSONEncoder := unstructured.NewJSONFallbackEncoder(internalEncoder)
	objects, err := asVersionedObject(infos, !singleItemImplied, specifiedOutputVersion, internalVersionJSONEncoder, o.IOStreams)
	if err != nil {
		return err
	}

	return o.Printer.PrintObj(objects, o.Out)
}

// asVersionedObject 把一组 info 合并为单个对象：对象只有一个时直接返回它，
// 否则包装成 List。优先转换为指定版本，未指定时退回对象映射中的版本。
func asVersionedObject(infos []*resource.Info, forceList bool, specifiedOutputVersion schema.GroupVersion, encoder runtime.Encoder, iostream genericclioptions.IOStreams) (runtime.Object, error) {
	objects, err := asVersionedObjects(infos, specifiedOutputVersion, encoder, iostream)
	if err != nil {
		return nil, err
	}

	var object runtime.Object
	if len(objects) == 1 && !forceList {
		object = objects[0]
	} else {
		object = &api.List{Items: objects}
		targetVersions := []schema.GroupVersion{}
		if !specifiedOutputVersion.Empty() {
			targetVersions = append(targetVersions, specifiedOutputVersion)
		}
		targetVersions = append(targetVersions, schema.GroupVersion{Group: "", Version: "v1"})

		converted, err := tryConvert(scheme.Scheme, object, targetVersions...)
		if err != nil {
			return nil, err
		}
		object = converted
	}

	actualVersion := object.GetObjectKind().GroupVersionKind()
	if actualVersion.Version != specifiedOutputVersion.Version {
		defaultVersionInfo := ""
		if len(actualVersion.Version) > 0 {
			defaultVersionInfo = fmt.Sprintf("Defaulting to %q", actualVersion.Version)
		}
		klog.V(1).Infof("info: the output version specified is invalid. %s\n", defaultVersionInfo)
	}
	return object, nil
}

// asVersionedObjects 把一组 info 逐个转换为带版本的对象。
// 优先使用指定版本，未指定时使用对象自身映射的版本。
func asVersionedObjects(infos []*resource.Info, specifiedOutputVersion schema.GroupVersion, encoder runtime.Encoder, iostream genericclioptions.IOStreams) ([]runtime.Object, error) {
	objects := []runtime.Object{}
	for _, info := range infos {
		if info.Object == nil {
			continue
		}

		targetVersions := []schema.GroupVersion{}
		// objects that are not part of api.Scheme must be converted to JSON
		// TODO: 未来考虑转成 map[string]interface{} 并挂到 runtime.Unknown 上。
		if !specifiedOutputVersion.Empty() {
			if _, _, err := scheme.Scheme.ObjectKinds(info.Object); runtime.IsNotRegisteredError(err) {
				// TODO: 理想情况下应按目标版本编码，但这里没有暴露多个 codec。
				data, err := runtime.Encode(encoder, info.Object)
				if err != nil {
					return nil, err
				}
				// TODO: 设置 ContentEncoding 与 ContentType。
				objects = append(objects, &runtime.Unknown{Raw: data})
				continue
			}
			targetVersions = append(targetVersions, specifiedOutputVersion)
		} else {
			gvks, _, err := scheme.Scheme.ObjectKinds(info.Object)
			if err == nil {
				for _, gvk := range gvks {
					targetVersions = append(targetVersions, scheme.Scheme.PrioritizedVersionsForGroup(gvk.Group)...)
				}
			}
		}

		converted, err := tryConvert(scheme.Scheme, info.Object, targetVersions...)
		if err != nil {
			// 转换时遇到“类型未注册”不算致命错误：向用户告警后继续处理其余对象；
			// 其他转换错误仍直接失败。
			if runtime.IsNotRegisteredError(err) {
				fmt.Fprintln(iostream.ErrOut, err.Error())
				continue
			}

			return nil, err
		}
		objects = append(objects, converted)
	}
	return objects, nil
}

// tryConvert 按给定顺序尝试把对象转换为某个版本，假定输入是 internal 版本。
// 全部失败时返回最后一个错误。
func tryConvert(converter runtime.ObjectConvertor, object runtime.Object, versions ...schema.GroupVersion) (runtime.Object, error) {
	var last error
	for _, version := range versions {
		if version.Empty() {
			return object, nil
		}
		obj, err := converter.ConvertToVersion(object, version)
		if err != nil {
			last = err
			continue
		}
		return obj, nil
	}
	return nil, last
}
