/*
Copyright 2018 The Kubernetes Authors.

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
	"io"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/discovery"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	apiresourcesExample = templates.Examples(`
		# Print the supported API resources
		kubectl api-resources

		# Print the supported API resources with more information
		kubectl api-resources -o wide

		# Print the supported API resources sorted by a column
		kubectl api-resources --sort-by=name

		# Print the supported namespaced resources
		kubectl api-resources --namespaced=true

		# Print the supported non-namespaced resources
		kubectl api-resources --namespaced=false

		# Print the supported API resources with a specific APIGroup
		kubectl api-resources --api-group=rbac.authorization.k8s.io`)
)

// APIResourceOptions 是执行该操作所需数据的起点。
// 新增字段时请添加在这里，而不是引用 cmd.Flags()
type APIResourceOptions struct {
	SortBy     string
	APIGroup   string
	Namespaced bool
	Verbs      []string
	Cached     bool
	Categories []string

	groupChanged bool
	nsChanged    bool

	discoveryClient discovery.CachedDiscoveryInterface

	genericiooptions.IOStreams
	PrintFlags *PrintFlags
	PrintObj   printers.ResourcePrinterFunc
}

// NewAPIResourceOptions 创建 APIResource 所需的选项
func NewAPIResourceOptions(ioStreams genericiooptions.IOStreams) *APIResourceOptions {
	return &APIResourceOptions{
		IOStreams:  ioStreams,
		Namespaced: true,
		PrintFlags: NewPrintFlags(),
	}
}

// NewCmdAPIResources 创建 `api-resources` 命令
func NewCmdAPIResources(restClientGetter genericclioptions.RESTClientGetter, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewAPIResourceOptions(ioStreams)

	cmd := &cobra.Command{
		Use:     "api-resources",
		Short:   i18n.T("Print the supported API resources on the server"),
		Long:    i18n.T("Print the supported API resources on the server."),
		Example: apiresourcesExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(restClientGetter, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.RunAPIResources())
		},
	}

	o.PrintFlags.AddFlags(cmd)

	cmd.Flags().StringVar(&o.APIGroup, "api-group", o.APIGroup, "Limit to resources in the specified API group.")
	cmd.Flags().BoolVar(&o.Namespaced, "namespaced", o.Namespaced, "If false, non-namespaced resources will be returned, otherwise returning namespaced resources by default.")
	cmd.Flags().StringSliceVar(&o.Verbs, "verbs", o.Verbs, "Limit to resources that support the specified verbs.")
	cmd.Flags().StringVar(&o.SortBy, "sort-by", o.SortBy, "If non-empty, sort list of resources using specified field. The field can be either 'name' or 'kind'.")
	cmd.Flags().BoolVar(&o.Cached, "cached", o.Cached, "Use the cached list of resources if available.")
	cmd.Flags().StringSliceVar(&o.Categories, "categories", o.Categories, "Limit to resources that belong to the specified categories.")
	return cmd
}

// Validate 检查 APIResourceOptions，判断是否有足够信息来运行命令
func (o *APIResourceOptions) Validate() error {
	supportedSortTypes := sets.New[string]("", "name", "kind")
	if len(o.SortBy) > 0 {
		if !supportedSortTypes.Has(o.SortBy) {
			return fmt.Errorf("--sort-by accepts only name or kind")
		}
	}
	return nil
}

// Complete 适配命令行参数并对其进行校验
func (o *APIResourceOptions) Complete(restClientGetter genericclioptions.RESTClientGetter, cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return cmdutil.UsageErrorf(cmd, "unexpected arguments: %v", args)
	}

	discoveryClient, err := restClientGetter.ToDiscoveryClient()
	if err != nil {
		return err
	}
	o.discoveryClient = discoveryClient

	o.groupChanged = cmd.Flags().Changed("api-group")
	o.nsChanged = cmd.Flags().Changed("namespaced")

	var printer printers.ResourcePrinter
	if o.PrintFlags.OutputFormat != nil {
		printer, err = o.PrintFlags.ToPrinter()
		if err != nil {
			return err
		}

		o.PrintObj = func(object runtime.Object, out io.Writer) error {
			errs := []error{}
			if !*o.PrintFlags.NoHeaders &&
				(o.PrintFlags.OutputFormat == nil || *o.PrintFlags.OutputFormat == "" || *o.PrintFlags.OutputFormat == "wide") {
				if err = printContextHeaders(out, *o.PrintFlags.OutputFormat); err != nil {
					errs = append(errs, err)
				}
			}
			if err := printer.PrintObj(object, out); err != nil {
				errs = append(errs, err)
			}
			return utilerrors.NewAggregate(errs)
		}
	}

	return nil
}

// RunAPIResources 执行实际工作
func (o *APIResourceOptions) RunAPIResources() error {
	w := printers.GetNewTabWriter(o.Out)
	defer w.Flush()

	if !o.Cached {
		// 始终向服务端请求最新数据
		o.discoveryClient.Invalidate()
	}

	errs := []error{}
	lists, err := o.discoveryClient.ServerPreferredResources()
	if err != nil {
		errs = append(errs, err)
	}

	var allResources []*metav1.APIResourceList

	for _, list := range lists {
		if len(list.APIResources) == 0 {
			continue
		}
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		apiList := &metav1.APIResourceList{
			TypeMeta: metav1.TypeMeta{
				Kind:       "APIResourceList",
				APIVersion: "v1",
			},
			GroupVersion: gv.String(),
		}
		var apiResources []metav1.APIResource
		for _, resource := range list.APIResources {
			if len(resource.Verbs) == 0 {
				continue
			}
			// 过滤 apiGroup
			if o.groupChanged && o.APIGroup != gv.Group {
				continue
			}
			// 过滤 namespaced
			if o.nsChanged && o.Namespaced != resource.Namespaced {
				continue
			}
			// 过滤出支持指定 verbs 的资源
			if len(o.Verbs) > 0 && !sets.New[string](resource.Verbs...).HasAll(o.Verbs...) {
				continue
			}
			// 过滤出属于指定 categories 的资源
			if len(o.Categories) > 0 && !sets.New[string](resource.Categories...).HasAll(o.Categories...) {
				continue
			}
			// 设置这些值，因为在人类可读输出的 APIVERSION 列中显示的是这两个值的拼接结果
			resource.Group = gv.Group
			resource.Version = gv.Version
			apiResources = append(apiResources, resource)
		}
		apiList.APIResources = apiResources
		allResources = append(allResources, apiList)
	}

	if len(allResources) == 0 {
		return utilerrors.NewAggregate(errs)
	}
	flatList := &metav1.APIResourceList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: allResources[0].APIVersion,
			Kind:       allResources[0].Kind,
		},
	}
	for _, resource := range allResources {
		flatList.APIResources = append(flatList.APIResources, resource.APIResources...)
	}

	sort.Stable(sortableResource{flatList.APIResources, o.SortBy})

	err = o.PrintObj(flatList, w)
	if err != nil {
		errs = append(errs, err)
	}
	return utilerrors.NewAggregate(errs)
}

func printContextHeaders(out io.Writer, output string) error {
	columnNames := []string{"NAME", "SHORTNAMES", "APIVERSION", "NAMESPACED", "KIND"}
	if output == "wide" {
		columnNames = append(columnNames, "VERBS", "CATEGORIES")
	}
	_, err := fmt.Fprintf(out, "%s\n", strings.Join(columnNames, "\t"))
	return err
}

type sortableResource struct {
	resources []metav1.APIResource
	sortBy    string
}

func (s sortableResource) Len() int { return len(s.resources) }
func (s sortableResource) Swap(i, j int) {
	s.resources[i], s.resources[j] = s.resources[j], s.resources[i]
}
func (s sortableResource) Less(i, j int) bool {
	ret := strings.Compare(s.compareValues(i, j))
	if ret > 0 {
		return false
	} else if ret == 0 {
		return strings.Compare(s.resources[i].Name, s.resources[j].Name) < 0
	}
	return true
}

func (s sortableResource) compareValues(i, j int) (string, string) {
	switch s.sortBy {
	case "name":
		return s.resources[i].Name, s.resources[j].Name
	case "kind":
		return s.resources[i].Kind, s.resources[j].Kind
	}
	return s.resources[i].Group, s.resources[j].Group
}
