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

package explain

import (
	"fmt"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	openapiclient "k8s.io/client-go/openapi"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/explain"
	openapiv3explain "k8s.io/kubectl/pkg/explain/v2"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/openapi"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	explainLong = templates.LongDesc(i18n.T(`
		Describe fields and structure of various resources.

		This command describes the fields associated with each supported API resource.
		Fields are identified via a simple JSONPath identifier:

			TYPE.FIELDNAME[.FIELDNAME]

		Information about each field is retrieved from the server in OpenAPI format.`))

	explainExamples = templates.Examples(i18n.T(`
		# Get the documentation of the resource and its fields
		kubectl explain pods

		# Get all the fields in the resource
		kubectl explain pods --recursive

		# Get fields in the resource up to a specific recursive depth
		kubectl explain pods --recursive --max-depth=2

		# Get the explanation for deployment in supported api versions
		kubectl explain deployments --api-version=apps/v1

		# Get the documentation of a specific field of a resource
		kubectl explain pods.spec.containers

		# Get the documentation of resources in different format
		kubectl explain deployment --output=plaintext-openapiv2`))
)

const (
	plaintextTemplateName          = "plaintext"
	plaintextOpenAPIV2TemplateName = "plaintext-openapiv2"
)

// ExplainFlags 直接反映 CLI 通过 flag 收集的信息。
// 它们将被转换为 Options，
// 后者反映命令的运行时需求。
type ExplainFlags struct {
	APIVersion   string
	OutputFormat string
	Recursive    bool
	MaxDepth     int

	genericiooptions.IOStreams
}

// NewExplainFlags 返回带默认值的 ExplainFlags。
func NewExplainFlags(streams genericiooptions.IOStreams) *ExplainFlags {
	return &ExplainFlags{
		OutputFormat: plaintextTemplateName,
		IOStreams:    streams,
	}
}

// AddFlags 为命令行注册标志。
func (flags *ExplainFlags) AddFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(&flags.Recursive, "recursive", "R", flags.Recursive, "Print the fields of fields. Use --max-depth to cap the recursion depth.")
	cmd.Flags().StringVar(&flags.APIVersion, "api-version", flags.APIVersion, "Get different explanations for particular API version (API group/version)")
	cmd.Flags().StringVarP(&flags.OutputFormat, "output", "o", plaintextTemplateName, "Format in which to render the schema (plaintext, plaintext-openapiv2)")
	cmd.Flags().IntVar(&flags.MaxDepth, "max-depth", flags.MaxDepth, "Maximum recursion depth when printing nested fields with --recursive. 0 means no limit. Requires --recursive when greater than 0.")
}

// ToOptions 把命令行输入转换为运行时选项。
func (flags *ExplainFlags) ToOptions(f cmdutil.Factory, parent string, args []string) (*ExplainOptions, error) {
	mapper, err := f.ToRESTMapper()
	if err != nil {
		return nil, err
	}

	// 只有 openapi v3 需要发现客户端。
	openAPIV3Client, err := f.OpenAPIV3Client()
	if err != nil {
		return nil, err
	}

	o := &ExplainOptions{
		IOStreams: flags.IOStreams,

		Recursive:    flags.Recursive,
		APIVersion:   flags.APIVersion,
		OutputFormat: flags.OutputFormat,
		MaxDepth:     flags.MaxDepth,

		CmdParent: parent,
		args:      args,

		Mapper:        mapper,
		openAPIGetter: f,

		OpenAPIV3Client: openAPIV3Client,
	}

	return o, nil
}

// NewCmdExplain 返回展示 swagger 文档的 cobra 命令。
func NewCmdExplain(parent string, f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	flags := NewExplainFlags(streams)

	cmd := &cobra.Command{
		Use:                   "explain TYPE [--recursive=FALSE|TRUE] [--api-version=api-version-group] [-o|--output=plaintext|plaintext-openapiv2]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Get documentation for a resource"),
		Long:                  explainLong + "\n\n" + cmdutil.SuggestAPIResources(parent),
		Example:               explainExamples,
		Run: func(cmd *cobra.Command, args []string) {
			o, err := flags.ToOptions(f, parent, args)
			cmdutil.CheckErr(err)
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}

	flags.AddFlags(cmd)

	return cmd
}

type ExplainOptions struct {
	genericiooptions.IOStreams

	Recursive  bool
	APIVersion string
	// openapiv3 模板渲染器使用的模板名。
	OutputFormat string
	MaxDepth     int

	CmdParent string
	args      []string

	Mapper        meta.RESTMapper
	openAPIGetter openapi.OpenAPIResourcesGetter

	// 能从用户集群获取 openapi 文档的客户端。
	OpenAPIV3Client openapiclient.Client
}

func (o *ExplainOptions) Validate() error {
	if len(o.args) == 0 {
		return fmt.Errorf("You must specify the type of resource to explain. %s\n", cmdutil.SuggestAPIResources(o.CmdParent))
	}
	if len(o.args) > 1 {
		return fmt.Errorf("We accept only this format: explain RESOURCE\n")
	}
	if o.MaxDepth > 0 && !o.Recursive {
		return fmt.Errorf("--max-depth requires --recursive")
	}
	if o.MaxDepth < 0 {
		return fmt.Errorf("--max-depth must be non-negative")
	}

	return nil
}

// Run 执行打印模型文档所需的步骤。
func (o *ExplainOptions) Run() error {
	var fullySpecifiedGVR schema.GroupVersionResource
	var fieldsPath []string
	var err error
	if len(o.APIVersion) == 0 {
		fullySpecifiedGVR, fieldsPath, err = explain.SplitAndParseResourceRequestWithMatchingPrefix(o.args[0], o.Mapper)
		if err != nil {
			return err
		}
	} else {
		// TODO: 确定分离 group 与 resource 的新语法后，
		// 允许在 explain 中使用（kubectl explain <group><syntax><resource>）。
		// 背景见 issue #16039；曾使用 "/" 语法的尝试见 PR #15808。
		fullySpecifiedGVR, fieldsPath, err = explain.SplitAndParseResourceRequest(o.args[0], o.Mapper)
		if err != nil {
			return err
		}
	}

	// 用特殊模板名回退到 openapiv2 实现。
	switch o.OutputFormat {
	case plaintextOpenAPIV2TemplateName:
		return o.renderOpenAPIV2(fullySpecifiedGVR, fieldsPath)
	case plaintextTemplateName:
		// 检查服务端是否支持 OpenAPIV3。
		if _, err := o.OpenAPIV3Client.Paths(); err != nil {
			// 服务端不支持 v3 时使用 v2 渲染器。
			return o.renderOpenAPIV2(fullySpecifiedGVR, fieldsPath)
		}

		fallthrough
	default:
		if len(o.APIVersion) > 0 {
			apiVersion, err := schema.ParseGroupVersion(o.APIVersion)
			if err != nil {
				return err
			}
			fullySpecifiedGVR.Group = apiVersion.Group
			fullySpecifiedGVR.Version = apiVersion.Version
		}

		return openapiv3explain.PrintModelDescription(
			fieldsPath,
			o.Out,
			o.OpenAPIV3Client,
			fullySpecifiedGVR,
			o.Recursive,
			o.MaxDepth,
			o.OutputFormat,
		)
	}
}

func (o *ExplainOptions) renderOpenAPIV2(
	fullySpecifiedGVR schema.GroupVersionResource,
	fieldsPath []string,
) error {
	var err error

	gvk, _ := o.Mapper.KindFor(fullySpecifiedGVR)
	if gvk.Empty() {
		gvk, err = o.Mapper.KindFor(fullySpecifiedGVR.GroupResource().WithVersion(""))
		if err != nil {
			return err
		}
	}

	if len(o.APIVersion) != 0 {
		apiVersion, err := schema.ParseGroupVersion(o.APIVersion)
		if err != nil {
			return err
		}
		gvk = apiVersion.WithKind(gvk.Kind)
	}

	resources, err := o.openAPIGetter.OpenAPISchema()
	if err != nil {
		return err
	}
	schema := resources.LookupResource(gvk)
	if schema == nil {
		return fmt.Errorf("couldn't find resource for %q", gvk)
	}

	return explain.PrintModelDescription(fieldsPath, o.Out, schema, gvk, o.Recursive, o.MaxDepth)
}
