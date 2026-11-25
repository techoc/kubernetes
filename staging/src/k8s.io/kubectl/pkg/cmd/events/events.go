/*
Copyright 2021 The Kubernetes Authors.

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

package events

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	runtimeresource "k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	watchtools "k8s.io/client-go/tools/watch"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/interrupt"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	eventsLong = templates.LongDesc(i18n.T(`
		Display events.

		Prints a table of the most important information about events.
		You can request events for a namespace, for all namespace, or
		filtered to only those pertaining to a specified resource.`))

	eventsExample = templates.Examples(i18n.T(`
	# List recent events in the default namespace
	kubectl events

	# List recent events in all namespaces
	kubectl events --all-namespaces

	# List recent events for the specified pod, then wait for more events and list them as they arrive
	kubectl events --for pod/web-pod-13je7 --watch

	# List recent events in YAML format
	kubectl events -oyaml

	# List recent only events of type 'Warning' or 'Normal'
	kubectl events --types=Warning,Normal`))
)

// EventsFlags 直接反映 CLI 通过 flag 收集的信息。它们会被转换为 Options，
// 后者反映命令运行时的需求。这种结构将转换简化为单纯的接线，并使
// 逻辑本身易于单元测试。
type EventsFlags struct {
	RESTClientGetter genericclioptions.RESTClientGetter
	PrintFlags       *genericclioptions.PrintFlags

	AllNamespaces bool
	Watch         bool
	NoHeaders     bool
	ForObject     string
	FilterTypes   []string
	ChunkSize     int64
	genericiooptions.IOStreams
}

// NewEventsFlags 返回默认的 EventsFlags
func NewEventsFlags(restClientGetter genericclioptions.RESTClientGetter, streams genericiooptions.IOStreams) *EventsFlags {
	return &EventsFlags{
		RESTClientGetter: restClientGetter,
		PrintFlags:       genericclioptions.NewPrintFlags("events").WithTypeSetter(scheme.Scheme),
		IOStreams:        streams,
		ChunkSize:        cmdutil.DefaultChunkSize,
	}
}

// EventsOptions 是一组用于列出 event 的选项。该对象反映 events 命令的
// 运行时需求，使逻辑本身易于单元测试。
type EventsOptions struct {
	Namespace     string
	AllNamespaces bool
	Watch         bool
	FilterTypes   []string

	forGVK  schema.GroupVersionKind
	forName string

	client *kubernetes.Clientset

	PrintObj printers.ResourcePrinterFunc

	genericiooptions.IOStreams
}

// NewCmdEvents 创建一个新的 events 命令
func NewCmdEvents(restClientGetter genericclioptions.RESTClientGetter, streams genericiooptions.IOStreams) *cobra.Command {
	flags := NewEventsFlags(restClientGetter, streams)

	cmd := &cobra.Command{
		Use:                   fmt.Sprintf("events [(-o|--output=)%s] [--for TYPE/NAME] [--watch] [--types=Normal,Warning]", strings.Join(flags.PrintFlags.AllowedFormats(), "|")),
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("List events"),
		Long:                  eventsLong,
		Example:               eventsExample,
		Run: func(cmd *cobra.Command, args []string) {
			o, err := flags.ToOptions()
			cmdutil.CheckErr(err)
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}
	flags.AddFlags(cmd)
	flags.PrintFlags.AddFlags(cmd)
	return cmd
}

// AddFlags 为 CLI 注册 flag。
func (flags *EventsFlags) AddFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(&flags.Watch, "watch", "w", flags.Watch, "After listing the requested events, watch for more events.")
	cmd.Flags().BoolVarP(&flags.AllNamespaces, "all-namespaces", "A", flags.AllNamespaces, "If present, list the requested object(s) across all namespaces. Namespace in current context is ignored even if specified with --namespace.")
	cmd.Flags().StringVar(&flags.ForObject, "for", flags.ForObject, "Filter events to only those pertaining to the specified resource.")
	cmd.Flags().StringSliceVar(&flags.FilterTypes, "types", flags.FilterTypes, "Output only events of given types.")
	cmd.Flags().BoolVar(&flags.NoHeaders, "no-headers", flags.NoHeaders, "When using the default output format, don't print headers.")
	cmdutil.AddChunkSizeFlag(cmd, &flags.ChunkSize)
}

// ToOptions 将 CLI 输入转换为运行时输入。
func (flags *EventsFlags) ToOptions() (*EventsOptions, error) {
	o := &EventsOptions{
		AllNamespaces: flags.AllNamespaces,
		Watch:         flags.Watch,
		FilterTypes:   flags.FilterTypes,
		IOStreams:     flags.IOStreams,
	}
	var err error
	o.Namespace, _, err = flags.RESTClientGetter.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return nil, err
	}

	if flags.ForObject != "" {
		mapper, err := flags.RESTClientGetter.ToRESTMapper()
		if err != nil {
			return nil, err
		}
		var found bool
		o.forGVK, o.forName, found, err = decodeResourceTypeName(mapper, flags.ForObject)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("--for must be in resource/name form")
		}
	}

	clientConfig, err := flags.RESTClientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	o.client, err = kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, err
	}

	if len(o.FilterTypes) > 0 {
		o.FilterTypes = sets.List(sets.New[string](o.FilterTypes...))
	}

	var printer printers.ResourcePrinter
	if flags.PrintFlags.OutputFormat != nil && len(*flags.PrintFlags.OutputFormat) > 0 {
		printer, err = flags.PrintFlags.ToPrinter()
		if err != nil {
			return nil, err
		}
	} else {
		printer = NewEventPrinter(flags.NoHeaders, flags.AllNamespaces)
	}

	o.PrintObj = func(object runtime.Object, writer io.Writer) error {
		return printer.PrintObj(object, writer)
	}

	return o, nil
}

func (o *EventsOptions) Validate() error {
	for _, val := range o.FilterTypes {
		if !strings.EqualFold(val, "Normal") && !strings.EqualFold(val, "Warning") {
			return fmt.Errorf("valid --types are Normal or Warning")
		}
	}

	return nil
}

// Run 获取 event
func (o *EventsOptions) Run() error {
	ctx := context.TODO()
	namespace := o.Namespace
	if o.AllNamespaces {
		namespace = ""
	}
	listOptions := metav1.ListOptions{Limit: cmdutil.DefaultChunkSize}
	if o.forName != "" {
		listOptions.FieldSelector = fields.AndSelectors(
			fields.OneTermEqualSelector("involvedObject.kind", o.forGVK.Kind),
			fields.OneTermEqualSelector("involvedObject.apiVersion", o.forGVK.GroupVersion().String()),
			fields.OneTermEqualSelector("involvedObject.name", o.forName)).String()
	}
	if o.Watch {
		return o.runWatch(ctx, namespace, listOptions)
	}

	e := o.client.CoreV1().Events(namespace)
	el := &corev1.EventList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EventList",
			APIVersion: "v1",
		},
	}
	err := runtimeresource.FollowContinue(&listOptions,
		func(options metav1.ListOptions) (runtime.Object, error) {
			newEvents, err := e.List(ctx, options)
			if err != nil {
				return nil, runtimeresource.EnhanceListError(err, options, "events")
			}
			el.Items = append(el.Items, newEvents.Items...)
			return newEvents, nil
		})

	if err != nil {
		return err
	}

	var filteredEvents []corev1.Event
	for _, e := range el.Items {
		if !o.filteredEventType(e.Type) {
			continue
		}
		if e.GetObjectKind().GroupVersionKind().Empty() {
			e.SetGroupVersionKind(schema.GroupVersionKind{
				Version: "v1",
				Kind:    "Event",
			})
		}
		filteredEvents = append(filteredEvents, e)
	}

	el.Items = filteredEvents

	if len(el.Items) == 0 {
		if o.AllNamespaces {
			fmt.Fprintln(o.ErrOut, "No events found.")
		} else {
			fmt.Fprintf(o.ErrOut, "No events found in %s namespace.\n", o.Namespace)
		}
		return nil
	}

	w := printers.GetNewTabWriter(o.Out)

	sort.Sort(SortableEvents(el.Items))

	o.PrintObj(el, w)
	w.Flush()
	return nil
}

func (o *EventsOptions) runWatch(ctx context.Context, namespace string, listOptions metav1.ListOptions) error {
	eventWatch, err := o.client.CoreV1().Events(namespace).Watch(ctx, listOptions)
	if err != nil {
		return err
	}
	w := printers.GetNewTabWriter(o.Out)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	intr := interrupt.New(nil, cancel)
	intr.Run(func() error {
		_, err := watchtools.UntilWithoutRetry(cctx, eventWatch, func(e watch.Event) (bool, error) {
			if e.Type == watch.Deleted { // event 会在 1 小时后被删除，不要打印这类事件
				return false, nil
			}

			if ev, ok := e.Object.(*corev1.Event); !ok || !o.filteredEventType(ev.Type) {
				return false, nil
			}

			if e.Object.GetObjectKind().GroupVersionKind().Empty() {
				e.Object.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{
					Version: "v1",
					Kind:    "Event",
				})
			}

			o.PrintObj(e.Object, w)
			w.Flush()
			return false, nil
		})
		return err
	})

	return nil
}

// filteredEventType 通过将给定 event 与过滤用的 event flag 比对，
// 检查它是否可以打印。
// 如果用户未设置 --event flag，此函数允许
// 打印所有 event。
func (o *EventsOptions) filteredEventType(et string) bool {
	if len(o.FilterTypes) == 0 {
		return true
	}

	for _, t := range o.FilterTypes {
		if strings.EqualFold(t, et) {
			return true
		}
	}

	return false
}

// SortableEvents 按时间为 []api.Event 实现 sort.Interface
type SortableEvents []corev1.Event

func (list SortableEvents) Len() int {
	return len(list)
}

func (list SortableEvents) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list SortableEvents) Less(i, j int) bool {
	return eventTime(list[i]).Before(eventTime(list[j]))
}

// 返回应用于排序的时间，该时间可能来自 corev1.Event 中的
// 不同字段。
func eventTime(event corev1.Event) time.Time {
	if event.Series != nil {
		return event.Series.LastObservedTime.Time
	}
	if !event.LastTimestamp.Time.IsZero() {
		return event.LastTimestamp.Time
	}
	return event.EventTime.Time
}

// 灵感来自 k8s.io/cli-runtime/pkg/resource 中的 splitResourceTypeName()

// decodeResourceTypeName 处理 type/name 格式的资源，返回资源元组
// （可能为空）、是否成功找到资源，以及错误
func decodeResourceTypeName(mapper meta.RESTMapper, s string) (gvk schema.GroupVersionKind, name string, found bool, err error) {
	if !strings.Contains(s, "/") {
		return
	}
	seg := strings.Split(s, "/")
	if len(seg) != 2 {
		err = fmt.Errorf("arguments in resource/name form may not have more than one slash")
		return
	}
	resource, name := seg[0], seg[1]

	fullySpecifiedGVR, groupResource := schema.ParseResourceArg(strings.ToLower(resource))
	gvr := schema.GroupVersionResource{}
	if fullySpecifiedGVR != nil {
		gvr, _ = mapper.ResourceFor(*fullySpecifiedGVR)
	}
	if gvr.Empty() {
		gvr, err = mapper.ResourceFor(groupResource.WithVersion(""))
		if err != nil {
			return
		}
	}

	gvk, err = mapper.KindFor(gvr)
	if err != nil {
		return
	}
	found = true

	return
}
