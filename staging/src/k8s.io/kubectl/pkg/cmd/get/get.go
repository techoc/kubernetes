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

package get

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	watchtools "k8s.io/client-go/tools/watch"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/rawhttp"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/interrupt"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/utils/ptr"
)

// GetOptions 包含 get 命令的输入。
type GetOptions struct {
	PrintFlags             *PrintFlags
	ToPrinter              func(*meta.RESTMapping, *bool, bool, bool) (printers.ResourcePrinterFunc, error)
	IsHumanReadablePrinter bool

	CmdParent string

	resource.FilenameOptions

	Raw       string
	Watch     bool
	WatchOnly bool
	ChunkSize int64

	OutputWatchEvents bool

	LabelSelector     string
	FieldSelector     string
	AllNamespaces     bool
	Namespace         string
	ExplicitNamespace bool
	Subresource       string
	SortBy            string

	ServerPrint bool

	NoHeaders      bool
	IgnoreNotFound bool

	genericiooptions.IOStreams
}

var (
	getLong = templates.LongDesc(i18n.T(`
		Display one or many resources.

		Prints a table of the most important information about the specified resources.
		You can filter the list using a label selector and the --selector flag. If the
		desired resource type is namespaced you will only see results in the current
		namespace if you don't specify any namespace.

		By specifying the output as 'template' and providing a Go template as the value
		of the --template flag, you can filter the attributes of the fetched resources.`))

	getExample = templates.Examples(i18n.T(`
		# List all pods in ps output format
		kubectl get pods

		# List all pods in ps output format with more information (such as node name)
		kubectl get pods -o wide

		# List a single replication controller with specified NAME in ps output format
		kubectl get replicationcontroller web

		# List deployments in JSON output format, in the "v1" version of the "apps" API group
		kubectl get deployments.v1.apps -o json

		# List a single pod in JSON output format
		kubectl get -o json pod web-pod-13je7

		# List a pod identified by type and name specified in "pod.yaml" in JSON output format
		kubectl get -f pod.yaml -o json

		# List resources from a directory with kustomization.yaml - e.g. dir/kustomization.yaml
		kubectl get -k dir/

		# Return only the phase value of the specified pod
		kubectl get -o template pod/web-pod-13je7 --template={{.status.phase}}

		# List resource information in custom columns
		kubectl get pod test-pod -o custom-columns=CONTAINER:.spec.containers[0].name,IMAGE:.spec.containers[0].image

		# List all replication controllers and services together in ps output format
		kubectl get rc,services

		# List one or more resources by their type and names
		kubectl get rc/web service/frontend pods/web-pod-13je7

		# List the 'status' subresource for a single pod
		kubectl get pod web-pod-13je7 --subresource status

		# List all deployments in namespace 'backend'
		kubectl get deployments.apps --namespace backend

		# List all pods existing in all namespaces
		kubectl get pods --all-namespaces`))
)

const (
	useServerPrintColumns = "server-print"
)

// NewGetOptions 返回一个默认分块大小为 500 的 GetOptions。
func NewGetOptions(parent string, streams genericiooptions.IOStreams) *GetOptions {
	return &GetOptions{
		PrintFlags: NewGetPrintFlags(),
		CmdParent:  parent,

		IOStreams:   streams,
		ChunkSize:   cmdutil.DefaultChunkSize,
		ServerPrint: true,
	}
}

// NewCmdGet 为通用的 "get" 操作创建命令对象，该操作
// 从服务端获取一个或多个资源。
func NewCmdGet(parent string, f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := NewGetOptions(parent, streams)

	cmd := &cobra.Command{
		Use:                   fmt.Sprintf("get [(-o|--output=)%s] (TYPE[.VERSION][.GROUP] [NAME | -l label] | TYPE[.VERSION][.GROUP]/NAME ...) [flags]", strings.Join(o.PrintFlags.AllowedFormats(), "|")),
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Display one or many resources"),
		Long:                  getLong + "\n\n" + cmdutil.SuggestAPIResources(parent),
		Example:               getExample,
		// 在调用此函数时设置 ValidArgsFunction，以便能够访问 util 包
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run(f, args))
		},
		SuggestFor: []string{"list", "ls"},
	}

	o.PrintFlags.AddFlags(cmd)

	cmd.Flags().StringVar(&o.Raw, "raw", o.Raw, "Raw URI to request from the server.  Uses the transport specified by the kubeconfig file.")
	cmd.Flags().BoolVarP(&o.Watch, "watch", "w", o.Watch, "After listing/getting the requested object, watch for changes.")
	cmd.Flags().BoolVar(&o.WatchOnly, "watch-only", o.WatchOnly, "Watch for changes to the requested object(s), without listing/getting first.")
	cmd.Flags().BoolVar(&o.OutputWatchEvents, "output-watch-events", o.OutputWatchEvents, "Output watch event objects when --watch or --watch-only is used. Existing objects are output as initial ADDED events.")
	cmd.Flags().BoolVar(&o.IgnoreNotFound, "ignore-not-found", o.IgnoreNotFound, "If set to true, suppresses NotFound error for specific objects that do not exist. Using this flag with commands that query for collections of resources has no effect when no resources are found.")
	cmd.Flags().StringVar(&o.FieldSelector, "field-selector", o.FieldSelector, "Selector (field query) to filter on, supports '=', '==', and '!='.(e.g. --field-selector key1=value1,key2=value2). The server only supports a limited number of field queries per type.")
	cmd.Flags().BoolVarP(&o.AllNamespaces, "all-namespaces", "A", o.AllNamespaces, "If present, list the requested object(s) across all namespaces. Namespace in current context is ignored even if specified with --namespace.")
	addServerPrintColumnFlags(cmd, o)
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, "identifying the resource to get from a server.")
	cmdutil.AddChunkSizeFlag(cmd, &o.ChunkSize)
	cmdutil.AddLabelSelectorFlagVar(cmd, &o.LabelSelector)
	cmdutil.AddSubresourceFlags(cmd, &o.Subresource, "If specified, gets the subresource of the requested object.")
	return cmd
}

// Complete 接收命令参数和 factory，并推断其余选项。
func (o *GetOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	if len(o.Raw) > 0 {
		if len(args) > 0 {
			return fmt.Errorf("arguments may not be passed when --raw is specified")
		}
		return nil
	}

	var err error
	o.Namespace, o.ExplicitNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}
	if o.AllNamespaces {
		o.ExplicitNamespace = false
	}

	if o.PrintFlags.HumanReadableFlags.SortBy != nil {
		o.SortBy = *o.PrintFlags.HumanReadableFlags.SortBy
	}

	o.NoHeaders = cmdutil.GetFlagBool(cmd, "no-headers")

	// TODO (soltysh): 目前服务端打印不支持自定义列，
	// 因此在这些情况下强制使用旧行为。
	outputOption := cmd.Flags().Lookup("output").Value.String()
	if strings.Contains(outputOption, "custom-columns") || outputOption == "yaml" || strings.Contains(outputOption, "json") {
		o.ServerPrint = false
	}

	templateArg := ""
	if o.PrintFlags.TemplateFlags != nil && o.PrintFlags.TemplateFlags.TemplateArgument != nil {
		templateArg = *o.PrintFlags.TemplateFlags.TemplateArgument
	}

	// 人类可读打印器有特殊的转换规则，因此需要判断是否在使用它。
	if (len(*o.PrintFlags.OutputFormat) == 0 && len(templateArg) == 0) || *o.PrintFlags.OutputFormat == "wide" {
		o.IsHumanReadablePrinter = true
	}

	o.ToPrinter = func(mapping *meta.RESTMapping, outputObjects *bool, withNamespace bool, withKind bool) (printers.ResourcePrinterFunc, error) {
		// 在修改之前，先复制一份当前的 flags / opts
		printFlags := o.PrintFlags.Copy()

		if mapping != nil {
			printFlags.SetKind(mapping.GroupVersionKind.GroupKind())
		}
		if withNamespace {
			printFlags.EnsureWithNamespace()
		}
		if withKind {
			printFlags.EnsureWithKind()
		}

		printer, err := printFlags.ToPrinter()
		if err != nil {
			return nil, err
		}
		printer, err = printers.NewTypeSetter(scheme.Scheme).WrapToPrinter(printer, nil)
		if err != nil {
			return nil, err
		}

		if len(o.SortBy) > 0 {
			printer = &SortingPrinter{Delegate: printer, SortField: o.SortBy}
		}
		if outputObjects != nil {
			printer = &skipPrinter{delegate: printer, output: outputObjects}
		}
		if o.ServerPrint {
			printer = &TablePrinter{Delegate: printer}
		}
		return printer.PrintObj, nil
	}

	switch {
	case o.Watch:
		if len(o.SortBy) > 0 {
			fmt.Fprintf(o.IOStreams.ErrOut, "warning: --watch requested, --sort-by will be ignored for watch events received\n")
		}
	case o.WatchOnly:
		if len(o.SortBy) > 0 {
			fmt.Fprintf(o.IOStreams.ErrOut, "warning: --watch-only requested, --sort-by will be ignored\n")
		}
	default:
		if len(args) == 0 && cmdutil.IsFilenameSliceEmpty(o.Filenames, o.Kustomize) {
			fmt.Fprintf(o.ErrOut, "You must specify the type of resource to get. %s\n\n", cmdutil.SuggestAPIResources(o.CmdParent))
			fullCmdName := cmd.Parent().CommandPath()
			usageString := "Required resource not specified."
			if len(fullCmdName) > 0 && cmdutil.IsSiblingCommandExists(cmd, "explain") {
				usageString = fmt.Sprintf("%s\nUse \"%s explain <resource>\" for a detailed description of that resource (e.g. %[2]s explain pods).", usageString, fullCmdName)
			}

			return cmdutil.UsageErrorf(cmd, "%s", usageString)
		}
	}

	return nil
}

// Validate 检查用户提供的标志集合。
func (o *GetOptions) Validate() error {
	if len(o.Raw) > 0 {
		if o.Watch || o.WatchOnly || len(o.LabelSelector) > 0 {
			return fmt.Errorf("--raw may not be specified with other flags that filter the server request or alter the output")
		}
		if o.PrintFlags.OutputFormat != nil && len(*o.PrintFlags.OutputFormat) > 0 {
			return fmt.Errorf("--raw and --output are mutually exclusive")
		}
		if _, err := url.ParseRequestURI(o.Raw); err != nil {
			return fmt.Errorf("--raw must be a valid URL path: %v", err)
		}
	}
	if o.PrintFlags.HumanReadableFlags.ShowLabels != nil && *o.PrintFlags.HumanReadableFlags.ShowLabels && o.PrintFlags.OutputFormat != nil {
		outputOption := *o.PrintFlags.OutputFormat
		if outputOption != "" && outputOption != "wide" {
			return fmt.Errorf("--show-labels option cannot be used with %s printer", outputOption)
		}
	}
	if o.PrintFlags.HumanReadableFlags.ColumnLabels != nil && len(*o.PrintFlags.HumanReadableFlags.ColumnLabels) > 0 && o.PrintFlags.OutputFormat != nil {
		outputOption := *o.PrintFlags.OutputFormat
		if strings.HasPrefix(outputOption, "custom-columns") {
			return fmt.Errorf("--label-columns option cannot be used with %s printer", outputOption)
		}
	}
	if o.OutputWatchEvents && !(o.Watch || o.WatchOnly) {
		return fmt.Errorf("--output-watch-events option can only be used with --watch or --watch-only")
	}
	return nil
}

// 对 table 格式的数据进行交换/排序操作时需要 OriginalPositioner 和 NopPositioner
type OriginalPositioner interface {
	OriginalPosition(int) int
}

// 对 table 格式的数据进行交换/排序操作时需要 NopPositioner 和 OriginalPositioner
type NopPositioner struct{}

// OriginalPosition 从 NopPositioner 对象返回原始位置
func (t *NopPositioner) OriginalPosition(ix int) int {
	return ix
}

// RuntimeSorter 持有对 runtime 对象进行排序所需的对象
type RuntimeSorter struct {
	field      string
	decoder    runtime.Decoder
	objects    []runtime.Object
	positioner OriginalPositioner
}

// Sort 执行对 runtime 对象的排序
func (r *RuntimeSorter) Sort() error {
	// 只有当列表中包含 0 或 1 个项时，才认为它是"已排序的"，
	// 并且（如果有 1 个项）该项不是 Table 对象
	if len(r.objects) == 0 {
		return nil
	}
	if len(r.objects) == 1 {
		_, isTable := r.objects[0].(*metav1.Table)
		if !isTable {
			return nil
		}
	}

	includesTable := false
	includesRuntimeObjs := false

	for _, obj := range r.objects {
		switch t := obj.(type) {
		case *metav1.Table:
			includesTable = true

			if sorter, err := NewTableSorter(t, r.field); err != nil {
				return err
			} else if err := sorter.Sort(); err != nil {
				return err
			}
		default:
			includesRuntimeObjs = true
		}
	}

	// 处理 Table 对象时使用 NopPositioner，
	// 因为被交换/排序的不是对象本身，
	// 而是每个对象中的行。
	r.positioner = &NopPositioner{}

	if includesRuntimeObjs && includesTable {
		return fmt.Errorf("sorting is not supported on mixed Table and non-Table object lists")
	}
	if includesTable {
		return nil
	}

	// 如果处理的不是来自服务端的 Table 响应，则照常假设
	// 所有对象都是 runtime.Object，并使用旧方法排序。
	var err error
	if r.positioner, err = SortObjects(r.decoder, r.objects, r.field); err != nil {
		return err
	}
	return nil
}

// OriginalPosition 返回 runtime 对象的原始位置
func (r *RuntimeSorter) OriginalPosition(ix int) int {
	if r.positioner == nil {
		return 0
	}
	return r.positioner.OriginalPosition(ix)
}

// WithDecoder 允许设置自定义解码器以用于测试
func (r *RuntimeSorter) WithDecoder(decoder runtime.Decoder) *RuntimeSorter {
	r.decoder = decoder
	return r
}

// NewRuntimeSorter 返回一个新的 RuntimeSorter 实例
func NewRuntimeSorter(objects []runtime.Object, sortBy string) *RuntimeSorter {
	parsedField, err := RelaxedJSONPathExpression(sortBy)
	if err != nil {
		parsedField = sortBy
	}

	return &RuntimeSorter{
		field:   parsedField,
		decoder: kubernetesscheme.Codecs.UniversalDecoder(),
		objects: objects,
	}
}

func (o *GetOptions) transformRequests(req *rest.Request) {
	if !o.ServerPrint || !o.IsHumanReadablePrinter {
		return
	}

	req.SetHeader("Accept", strings.Join([]string{
		fmt.Sprintf("application/json;as=Table;v=%s;g=%s", metav1.SchemeGroupVersion.Version, metav1.GroupName),
		fmt.Sprintf("application/json;as=Table;v=%s;g=%s", metav1beta1.SchemeGroupVersion.Version, metav1beta1.GroupName),
		"application/json",
	}, ","))

	// 如果要排序，确保接收到完整对象，以便通过 jsonpath 检查其字段
	if len(o.SortBy) > 0 {
		req.Param("includeObject", "Object")
	}
}

// Run 执行 get 操作。
// TODO: 像其他命令一样，去掉传递这些参数的必要性。
func (o *GetOptions) Run(f cmdutil.Factory, args []string) error {
	if len(o.Raw) > 0 {
		restClient, err := f.RESTClient()
		if err != nil {
			return err
		}
		return rawhttp.RawGet(restClient, o.IOStreams, o.Raw)
	}
	if o.Watch || o.WatchOnly {
		return o.watch(f, args)
	}

	chunkSize := o.ChunkSize
	if len(o.SortBy) > 0 {
		// TODO(juanvallejo): 将来可以让客户端使用分块
		// 收集所有结果，最后统一排序，以减轻服务端负载。
		chunkSize = 0
	}

	r := f.NewBuilder().
		Unstructured().
		NamespaceParam(o.Namespace).DefaultNamespace().AllNamespaces(o.AllNamespaces).
		FilenameParam(o.ExplicitNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.LabelSelector).
		FieldSelectorParam(o.FieldSelector).
		Subresource(o.Subresource).
		RequestChunksOf(chunkSize).
		ResourceTypeOrNameArgs(true, args...).
		ContinueOnError().
		Latest().
		Flatten().
		TransformRequests(o.transformRequests).
		Do()

	if o.IgnoreNotFound {
		r.IgnoreErrors(apierrors.IsNotFound)
	}
	if err := r.Err(); err != nil {
		return err
	}

	if !o.IsHumanReadablePrinter {
		return o.printGeneric(r)
	}

	allErrs := []error{}
	errs := sets.New[string]()
	infos, err := r.Infos()
	if err != nil {
		allErrs = append(allErrs, err)
	}
	printWithKind := multipleGVKsRequested(infos)

	objs := make([]runtime.Object, len(infos))
	for ix := range infos {
		objs[ix] = infos[ix].Object
	}

	var positioner OriginalPositioner
	if len(o.SortBy) > 0 {
		sorter := NewRuntimeSorter(objs, o.SortBy)
		if err := sorter.Sort(); err != nil {
			return err
		}
		positioner = sorter
	}

	var printer printers.ResourcePrinter
	var lastMapping *meta.RESTMapping

	// 跟踪是否写入了任何输出
	trackingWriter := &trackingWriterWrapper{Delegate: o.Out}
	// 输出一个空行来分隔输出内容
	separatorWriter := &separatorWriterWrapper{Delegate: trackingWriter}

	w := printers.GetNewTabWriter(separatorWriter)
	allResourcesNamespaced := !o.AllNamespaces
	for ix := range objs {
		var mapping *meta.RESTMapping
		var info *resource.Info
		if positioner != nil {
			info = infos[positioner.OriginalPosition(ix)]
			mapping = info.Mapping
		} else {
			info = infos[ix]
			mapping = info.Mapping
		}

		allResourcesNamespaced = allResourcesNamespaced && info.Namespaced()
		printWithNamespace := o.AllNamespaces

		if mapping != nil && mapping.Scope.Name() == meta.RESTScopeNameRoot {
			printWithNamespace = false
		}

		if shouldGetNewPrinterForMapping(printer, lastMapping, mapping) {
			w.Flush()
			w.SetRememberedWidths(nil)

			// 在资源组之间添加换行（如果有多个资源组），
			// 需满足以下全部三个条件：
			// 1）不是第一个资源组
			// 2）带有行表头
			// 3）自上次输出新表头以来已写入过内容
			if lastMapping != nil && !o.NoHeaders && trackingWriter.Written > 0 {
				separatorWriter.SetReady(true)
			}

			printer, err = o.ToPrinter(mapping, nil, printWithNamespace, printWithKind)
			if err != nil {
				if !errs.Has(err.Error()) {
					errs.Insert(err.Error())
					allErrs = append(allErrs, err)
				}
				continue
			}

			lastMapping = mapping
		}

		printer.PrintObj(info.Object, w)
	}
	w.Flush()
	if trackingWriter.Written == 0 && !o.IgnoreNotFound && len(allErrs) == 0 {
		// 如果没有写入任何输出、没有错误且不忽略 NotFound，请确保输出一些内容
		if allResourcesNamespaced {
			fmt.Fprintf(o.ErrOut, "No resources found in %s namespace.\n", o.Namespace)
		} else {
			fmt.Fprintln(o.ErrOut, "No resources found")
		}
	}
	return utilerrors.NewAggregate(allErrs)
}

type trackingWriterWrapper struct {
	Delegate io.Writer
	Written  int
}

func (t *trackingWriterWrapper) Write(p []byte) (n int, err error) {
	t.Written += len(p)
	return t.Delegate.Write(p)
}

type separatorWriterWrapper struct {
	Delegate io.Writer
	Ready    bool
}

func (s *separatorWriterWrapper) Write(p []byte) (n int, err error) {
	// 如果即将写入非空字节且 `s` 已就绪，
	// 就在 `p` 前面加一个空行，并重置 `s.Read`。
	if len(p) != 0 && s.Ready {
		fmt.Fprintln(s.Delegate)
		s.Ready = false
	}
	return s.Delegate.Write(p)
}

func (s *separatorWriterWrapper) SetReady(state bool) {
	s.Ready = state
}

// watch 启动对一个或多个资源的客户端 watch。
// TODO: 在此去掉对参数的需要。
func (o *GetOptions) watch(f cmdutil.Factory, args []string) error {
	r := f.NewBuilder().
		Unstructured().
		NamespaceParam(o.Namespace).DefaultNamespace().AllNamespaces(o.AllNamespaces).
		FilenameParam(o.ExplicitNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.LabelSelector).
		FieldSelectorParam(o.FieldSelector).
		RequestChunksOf(o.ChunkSize).
		ResourceTypeOrNameArgs(true, args...).
		SingleResourceType().
		Latest().
		TransformRequests(o.transformRequests).
		Do()
	if err := r.Err(); err != nil {
		return err
	}
	infos, err := r.Infos()
	if err != nil {
		// 当 ignore-not-found 设为 true 时，忽略 "NotFound" 错误
		if apierrors.IsNotFound(err) && o.IgnoreNotFound {
			return nil
		}
		return err
	}

	info := infos[0]
	mapping := info.ResourceMapping()
	outputObjects := ptr.To(!o.WatchOnly)
	printer, err := o.ToPrinter(mapping, outputObjects, o.AllNamespaces, false)
	if err != nil {
		return err
	}
	obj, err := r.Object()
	if err != nil {
		return err
	}

	// 从 resourceVersion 0 开始 watch，会从大约当前时刻开始，并
	// 返回初始的 watch 事件。从大约当前时刻（而非对象自身的 rv）开始，
	// 可以确保 watch 的起点位于
	// watch 窗口之内，而对象的 rv 可能并不在此窗口内。
	rv := "0"
	isList := meta.IsListType(obj)
	if isList {
		// list 对象的 resourceVersion 大约是当前时刻，但不会返回
		// 初始 watch 事件
		rv, err = meta.NewAccessor().ResourceVersion(obj)
		if err != nil {
			return err
		}
	}

	writer := printers.GetNewTabWriter(o.Out)

	// 打印当前对象
	var objsToPrint []runtime.Object
	if isList {
		objsToPrint, _ = meta.ExtractList(obj)
	} else {
		objsToPrint = append(objsToPrint, obj)
	}
	for _, objToPrint := range objsToPrint {
		if o.OutputWatchEvents {
			objToPrint = &metav1.WatchEvent{Type: string(watch.Added), Object: runtime.RawExtension{Object: objToPrint}}
		}
		if err := printer.PrintObj(objToPrint, writer); err != nil {
			return fmt.Errorf("unable to output the provided object: %v", err)
		}
	}
	writer.Flush()
	if isList {
		// 现在可以开始输出对象了，从 list 启动的 watch 不会发出合成的 added 事件
		*outputObjects = true
	} else {
		// 抑制输出，因为从单个项启动的 watch 会先发出一个合成的 ADDED 事件
		*outputObjects = false
	}

	// 打印 watch 到的变更
	w, err := r.Watch(rv)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	intr := interrupt.New(nil, cancel)
	intr.Run(func() error {
		_, err := watchtools.UntilWithoutRetry(ctx, w, func(e watch.Event) (bool, error) {
			objToPrint := e.Object
			if o.OutputWatchEvents {
				objToPrint = &metav1.WatchEvent{Type: string(e.Type), Object: runtime.RawExtension{Object: objToPrint}}
			}
			if err := printer.PrintObj(objToPrint, writer); err != nil {
				return false, err
			}
			writer.Flush()
			// 处理完至少一个事件后，开始输出对象
			*outputObjects = true
			return false, nil
		})
		return err
	})
	return nil
}

func (o *GetOptions) printGeneric(r *resource.Result) error {
	// 我们已经将 builder 中的数据展平，得到的是单个项，但现在我们希望：
	// 1. 如果有多个项，将它们全部合并为一个列表
	// 2. 如果只有一个项且该项是列表，则保留为其原有的具体列表类型
	// 3. 如果只有一个项且不是列表，则保留为单个项
	var errs []error
	singleItemImplied := false
	infos, err := r.IntoSingleItemImplied(&singleItemImplied).Infos()
	if err != nil {
		if singleItemImplied {
			return err
		}
		errs = append(errs, err)
	}

	if len(infos) == 0 && o.IgnoreNotFound {
		return utilerrors.Reduce(utilerrors.Flatten(utilerrors.NewAggregate(errs)))
	}

	printer, err := o.ToPrinter(nil, nil, false, false)
	if err != nil {
		return err
	}

	var obj runtime.Object
	if !singleItemImplied || len(infos) != 1 {
		// 我们有零个或多个项，因此将所有项强制转换为一个列表。
		// 暂时不想要 *unstructured.Unstructured 列表，因为可能
		// 处理的是非 unstructured 对象。先将所有项组合成
		// corev1.List，再用 unstructured scheme 解码。
		list := corev1.List{
			TypeMeta: metav1.TypeMeta{
				Kind:       "List",
				APIVersion: "v1",
			},
			ListMeta: metav1.ListMeta{},
		}
		for _, info := range infos {
			list.Items = append(list.Items, runtime.RawExtension{Object: info.Object})
		}

		listData, err := json.Marshal(list)
		if err != nil {
			return err
		}

		converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, listData)
		if err != nil {
			return err
		}

		obj = converted
	} else {
		obj = infos[0].Object
	}

	isList := meta.IsListType(obj)
	if isList {
		items, err := meta.ExtractList(obj)
		if err != nil {
			return err
		}

		// 取出这些项并创建一个新列表用于展示
		list := &unstructured.UnstructuredList{
			Object: map[string]interface{}{
				"kind":       "List",
				"apiVersion": "v1",
				"metadata":   map[string]interface{}{},
			},
		}
		if listMeta, err := meta.ListAccessor(obj); err == nil {
			list.Object["metadata"] = map[string]interface{}{
				"resourceVersion": listMeta.GetResourceVersion(),
			}
		}

		for _, item := range items {
			list.Items = append(list.Items, *item.(*unstructured.Unstructured))
		}
		if err := printer.PrintObj(list, o.Out); err != nil {
			errs = append(errs, err)
		}
		return utilerrors.Reduce(utilerrors.Flatten(utilerrors.NewAggregate(errs)))
	}

	if printErr := printer.PrintObj(obj, o.Out); printErr != nil {
		errs = append(errs, printErr)
	}

	return utilerrors.Reduce(utilerrors.Flatten(utilerrors.NewAggregate(errs)))
}

func addServerPrintColumnFlags(cmd *cobra.Command, opt *GetOptions) {
	cmd.Flags().BoolVar(&opt.ServerPrint, useServerPrintColumns, opt.ServerPrint, "If true, have the server return the appropriate table output. Supports extension APIs and CRDs.")
}

func shouldGetNewPrinterForMapping(printer printers.ResourcePrinter, lastMapping, mapping *meta.RESTMapping) bool {
	return printer == nil || lastMapping == nil || mapping == nil || mapping.Resource != lastMapping.Resource
}

func multipleGVKsRequested(infos []*resource.Info) bool {
	if len(infos) < 2 {
		return false
	}
	gvk := infos[0].Mapping.GroupVersionKind
	for _, info := range infos {
		if info.Mapping.GroupVersionKind != gvk {
			return true
		}
	}
	return false
}
