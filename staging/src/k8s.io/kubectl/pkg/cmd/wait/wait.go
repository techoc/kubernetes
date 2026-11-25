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

package wait

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/jsonpath"
	cmdget "k8s.io/kubectl/pkg/cmd/get"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	waitLong = templates.LongDesc(i18n.T(`
		Wait for a specific condition on one or many resources.

		The command takes multiple resources and waits until the specified condition
		is seen in the Status field of every given resource.

		Alternatively, the command can wait for the given set of resources to be created or
		deleted by providing the "create" or "delete" keyword as the value to the --for flag.

		A successful message will be printed to stdout indicating when the specified
        condition has been met. You can use -o option to change to output destination.`))

	waitExample = templates.Examples(i18n.T(`
		# Wait for the pod "busybox1" to contain the status condition of type "Ready"
		kubectl wait --for=condition=Ready pod/busybox1

		# The default value of status condition is true; you can wait for other targets after an equal delimiter (compared after Unicode simple case folding, which is a more general form of case-insensitivity)
		kubectl wait --for=condition=Ready=false pod/busybox1

		# Wait for the pod "busybox1" to contain the status phase to be "Running"
		kubectl wait --for=jsonpath='{.status.phase}'=Running pod/busybox1

		# Wait for pod "busybox1" to be Ready
		kubectl wait --for='jsonpath={.status.conditions[?(@.type=="Ready")].status}=True' pod/busybox1

		# Wait for the service "loadbalancer" to have ingress
		kubectl wait --for=jsonpath='{.status.loadBalancer.ingress}' service/loadbalancer

		# Wait for the secret "busybox1" to be created, with a timeout of 30s
		kubectl create secret generic busybox1
		kubectl wait --for=create secret/busybox1 --timeout=30s

		# Wait for the pod "busybox1" to be deleted, with a timeout of 60s, after having issued the "delete" command
		kubectl delete pod/busybox1
		kubectl wait --for=delete pod/busybox1 --timeout=60s

		# Wait for pod "busybox1" to be created AND reach the "Ready" status condition
		kubectl wait --for=condition=Ready --for=create pod/busybox1

		# Wait for pod "busybox1" to reach the "Ready" status OR for its containers to report a "False" readiness state
		until kubectl wait pod/busybox1 --for=condition=Ready --timeout=1s 2>/dev/null || \
		kubectl wait pod/busybox1 --for=condition=ContainersReady=False --timeout=1s 2>/dev/null; \
		do echo "Checking conditions..."; sleep 1; done`))
)

// 当没有资源匹配查询时，返回 errNoMatchingResources。
var errNoMatchingResources = errors.New("no matching resources found")

// WaitFlags 直接反映 CLI 通过标志收集的信息。它们会被转换为 Options，后者
// 反映命令的运行时需求。这种结构把转换简化为接线，并使
// 逻辑本身易于单元测试
type WaitFlags struct {
	RESTClientGetter     genericclioptions.RESTClientGetter
	PrintFlags           *genericclioptions.PrintFlags
	ResourceBuilderFlags *genericclioptions.ResourceBuilderFlags

	Timeout      time.Duration
	ForCondition []string

	genericiooptions.IOStreams
}

// NewWaitFlags 返回默认的 WaitFlags
func NewWaitFlags(restClientGetter genericclioptions.RESTClientGetter, streams genericiooptions.IOStreams) *WaitFlags {
	return &WaitFlags{
		RESTClientGetter: restClientGetter,
		PrintFlags:       genericclioptions.NewPrintFlags("condition met"),
		ResourceBuilderFlags: genericclioptions.NewResourceBuilderFlags().
			WithLabelSelector("").
			WithFieldSelector("").
			WithAll(false).
			WithAllNamespaces(false).
			WithLocal(false).
			WithLatest(),

		Timeout: 30 * time.Second,

		IOStreams: streams,
	}
}

// NewCmdWait 返回用于等待的 cobra 命令
func NewCmdWait(restClientGetter genericclioptions.RESTClientGetter, streams genericiooptions.IOStreams) *cobra.Command {
	flags := NewWaitFlags(restClientGetter, streams)

	cmd := &cobra.Command{
		Use:     "wait ([-f FILENAME] | resource.group/resource.name | resource.group [(-l label | --all)]) [--for=create|--for=delete|--for condition=available|--for=jsonpath='{}'[=value]]",
		Short:   i18n.T("Wait for a specific condition on one or many resources"),
		Long:    waitLong,
		Example: waitExample,

		DisableFlagsInUseLine: true,
		Run: func(cmd *cobra.Command, args []string) {
			o, err := flags.ToOptions(args)
			cmdutil.CheckErr(err)
			cmdutil.CheckErr(o.RunWaitContext(cmd.Context()))
		},
	}

	flags.AddFlags(cmd)

	return cmd
}

// AddFlags 为 CLI 注册标志
func (flags *WaitFlags) AddFlags(cmd *cobra.Command) {
	flags.PrintFlags.AddFlags(cmd)
	flags.ResourceBuilderFlags.AddFlags(cmd.Flags())

	cmd.Flags().DurationVar(&flags.Timeout, "timeout", flags.Timeout, "The length of time to wait before giving up. Zero means check once and don't wait, negative means wait for a week.")
	cmd.Flags().StringArrayVar(&flags.ForCondition, "for", flags.ForCondition, "The condition to wait on: [create|delete|condition=condition-name[=condition-value]|jsonpath='{JSONPath expression}'=[JSONPath value]]. The default condition-value is true. Condition values are compared after Unicode simple case folding, which is a more general form of case-insensitivity. Multiple conditions are supported and AND'ed to each other in a sequential order. If --for=create is passed, it is always waited first.")
}

// ToOptions 将 CLI 输入转换为运行时输入
func (flags *WaitFlags) ToOptions(args []string) (*WaitOptions, error) {
	printer, err := flags.PrintFlags.ToPrinter()
	if err != nil {
		return nil, err
	}
	builder := flags.ResourceBuilderFlags.ToBuilder(flags.RESTClientGetter, args)
	clientConfig, err := flags.RESTClientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		return nil, err
	}
	conditionFn, err := conditionFuncsFor(flags.ForCondition, flags.ErrOut)
	if err != nil {
		return nil, err
	}

	effectiveTimeout := flags.Timeout
	if effectiveTimeout < 0 {
		effectiveTimeout = 168 * time.Hour
	}

	o := &WaitOptions{
		ResourceFinder: builder,
		DynamicClient:  dynamicClient,
		Timeout:        effectiveTimeout,
		ForCondition:   flags.ForCondition,

		Printer:     printer,
		ConditionFn: conditionFn,
		IOStreams:   flags.IOStreams,
	}

	return o, nil
}

func conditionFuncsFor(conditions []string, errOut io.Writer) ([]ConditionFunc, error) {
	var condFuncs []ConditionFunc
	for _, cond := range conditions {
		lowercaseCond := strings.ToLower(cond)
		switch {
		case lowercaseCond == "delete":
			condFuncs = append(condFuncs, IsDeleted)

		case lowercaseCond == "create":
			condFuncs = append(condFuncs, IsCreated)

		case strings.HasPrefix(cond, "condition="):
			conditionName := strings.TrimPrefix(cond, "condition=")
			conditionValue := "true"
			if equalsIndex := strings.Index(conditionName, "="); equalsIndex != -1 {
				conditionValue = conditionName[equalsIndex+1:]
				conditionName = conditionName[0:equalsIndex]
			}

			condFuncs = append(condFuncs, ConditionalWait{
				conditionName:   conditionName,
				conditionStatus: conditionValue,
				errOut:          errOut,
			}.IsConditionMet)

		case strings.HasPrefix(cond, "jsonpath="):
			jsonPathInput := strings.TrimPrefix(cond, "jsonpath=")
			jsonPathExp, jsonPathValue, err := processJSONPathInput(jsonPathInput)
			if err != nil {
				return nil, err
			}
			j, err := newJSONPathParser(jsonPathExp)
			if err != nil {
				return nil, err
			}
			condFuncs = append(condFuncs, JSONPathWait{
				matchAnyValue:  jsonPathValue == "",
				jsonPathValue:  jsonPathValue,
				jsonPathParser: j,
				errOut:         errOut,
			}.IsJSONPathConditionMet)
		default:
			return nil, fmt.Errorf("unrecognized condition: %q", cond)
		}
	}
	if condFuncs == nil {
		return nil, fmt.Errorf("unrecognized condition: %q", conditions)
	}

	return condFuncs, nil
}

// newJSONPathParser 根据 jsonPathExpression 创建一个新的 JSONPath 解析器
func newJSONPathParser(jsonPathExpression string) (*jsonpath.JSONPath, error) {
	j := jsonpath.New("wait").AllowMissingKeys(true)
	if jsonPathExpression == "" {
		return nil, errors.New("jsonpath expression cannot be empty")
	}
	if err := j.Parse(jsonPathExpression); err != nil {
		return nil, err
	}
	return j, nil
}

// processJSONPathInput 将解析并处理提供的 JSONPath 输入，其中包含 JSON 表达式，以及可选的
// 匹配条件的值。
func processJSONPathInput(input string) (string, string, error) {
	jsonPathInput := splitJSONPathInput(input)
	if numOfArgs := len(jsonPathInput); numOfArgs < 1 || numOfArgs > 2 {
		return "", "", fmt.Errorf("jsonpath wait format must be --for=jsonpath='{.status.readyReplicas}'=3 or --for=jsonpath='{.status.readyReplicas}'")
	}
	relaxedJSONPathExp, err := cmdget.RelaxedJSONPathExpression(jsonPathInput[0])
	if err != nil {
		return "", "", err
	}
	if len(jsonPathInput) == 1 {
		return relaxedJSONPathExp, "", nil
	}
	jsonPathValue := strings.Trim(jsonPathInput[1], `'"`)
	if jsonPathValue == "" {
		return "", "", errors.New("jsonpath wait has to have a value after equal sign, like --for=jsonpath='{.status.readyReplicas}'=3")
	}
	return relaxedJSONPathExp, jsonPathValue, nil
}

// splitJSONPathInput 按单个 '=' 拆分输入字符串。连续的 '==' 不会导致字符串被
// 拆分。例如，"a.b.c====d.e.f===g.h.i===" 会被拆分为 ["a.b.c====d.e.f==","g.h.i==",""]。
func splitJSONPathInput(input string) []string {
	var output []string
	var element strings.Builder
	for i := 0; i < len(input); i++ {
		if input[i] == '=' {
			if i < len(input)-1 && input[i+1] == '=' {
				element.WriteString("==")
				i++
				continue
			}
			output = append(output, element.String())
			element.Reset()
			continue
		}
		element.WriteByte(input[i])
	}
	return append(output, element.String())
}

// ResourceLocation 保存资源的位置
type ResourceLocation struct {
	GroupResource schema.GroupResource
	Namespace     string
	Name          string
}

// UIDMap 将 ResourceLocation 映射到 UID
type UIDMap map[ResourceLocation]types.UID

// WaitOptions 是一组用于等待的选项。该对象反映 wait 命令的运行时需求，
// 从而使逻辑本身易于用现有 mock 进行单元测试。
type WaitOptions struct {
	ResourceFinder genericclioptions.ResourceFinder
	// UIDMap 将资源位置映射到 UID。它是可选的，但 ConditionFuncs 可以选择使用它来使结果
	// 更可靠。例如，delete 可以在委托调用期间检查 UID 是否一致。
	UIDMap        UIDMap
	DynamicClient dynamic.Interface
	Timeout       time.Duration
	ForCondition  []string

	Printer     printers.ResourcePrinter
	ConditionFn []ConditionFunc
	genericiooptions.IOStreams
}

// ConditionFunc 是提供条件检查的接口
type ConditionFunc func(ctx context.Context, info *resource.Info, o *WaitOptions) (finalObject runtime.Object, done bool, err error)

// Deprecated: Use RunWaitContext instead, which allows canceling.
func (o *WaitOptions) RunWait() error {
	return o.RunWaitContext(context.Background())
}

// RunWaitContext 运行等待逻辑
func (o *WaitOptions) RunWaitContext(ctx context.Context) error {
	ctx, cancel := watchtools.ContextWithOptionalTimeout(ctx, o.Timeout)
	defer cancel()
	if containsCondition(o.ForCondition, "create") {
		// TODO(soltysh): 这并非理想方案，因为我们每 0.5 秒轮询一次，
		// 而且必须使用包含资源名称的 ResourceFinder。
		// 长远来看，我们应从 ResourceFinder 暴露资源信息，
		// 或提供 ResourceBuilder 中的函数来解析这些信息。最后，此轮询
		// 应替换为 ListWatch 缓存。
		if err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, o.Timeout, true, func(context.Context) (done bool, err error) {
			foundResource := false
			visitErr := o.ResourceFinder.Do().Visit(func(info *resource.Info, err error) error {
				foundResource = true
				return nil
			})
			if apierrors.IsNotFound(visitErr) {
				return false, nil
			}
			if visitErr != nil {
				return false, visitErr
			}
			return foundResource, nil
		}); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%s", wait.ErrorInterrupted(nil).Error()) // nolint:staticcheck // SA1019
			}
			return err
		}
	}

	visitCount := 0
	visitFunc := func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}

		visitCount++
		for _, condFn := range o.ConditionFn {
			finalObject, success, err := condFn(ctx, info, o)
			if success {
				o.Printer.PrintObj(finalObject, o.Out) //nolint:errcheck
				continue
			}
			if err == nil {
				return fmt.Errorf("%v unsatisfied for unknown reason", finalObject)
			}
			return err
		}
		return nil
	}
	visitor := o.ResourceFinder.Do()
	isForDelete := containsCondition(o.ForCondition, "delete")
	if visitor, ok := visitor.(*resource.Result); ok && isForDelete {
		visitor.IgnoreErrors(apierrors.IsNotFound)
	}

	err := visitor.Visit(visitFunc)
	if err != nil {
		return err
	}
	if visitCount == 0 && !isForDelete {
		return errNoMatchingResources
	}
	return err
}

func containsCondition(conditions []string, condition string) bool {
	return slices.ContainsFunc(conditions, func(cond string) bool {
		return strings.ToLower(cond) == condition
	})
}
