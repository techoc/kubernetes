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

package util

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
)

const (
	ApplyAnnotationsFlag = "save-config"
	DefaultErrorExitCode = 1
	DefaultChunkSize     = 500
)

type debugError interface {
	DebugError() (msg string, args []interface{})
}

// AddSourceToErr 在错误消息中加入 handleResourcePrefix 和 source 字符串。
// verb 是类似 "creating"、"deleting" 等的字符串。
// source 是模板文件（*.json 或 *.yaml）的文件名或 URL，或者是用于处理资源的 stdin。
func AddSourceToErr(verb string, source string, err error) error {
	if source != "" {
		if statusError, ok := err.(apierrors.APIStatus); ok {
			status := statusError.Status()
			status.Message = fmt.Sprintf("error when %s %q: %v", verb, source, status.Message)
			return &apierrors.StatusError{ErrStatus: status}
		}
		return fmt.Errorf("error when %s %q: %v", verb, source, err)
	}
	return err
}

var fatalErrHandler = fatal

// BehaviorOnFatal 允许覆盖发生致命错误时的默认行为，
// 默认行为是调用 os.Exit(code)。如果更倾向于 panic() 而非 os.Exit(1)，
// 可以在此处传入 'panic' 函数。
func BehaviorOnFatal(f func(string, int)) {
	fatalErrHandler = f
}

// DefaultBehaviorOnFatal 用于撤销之前的覆盖行为，在测试中很有用。
func DefaultBehaviorOnFatal() {
	fatalErrHandler = fatal
}

// fatal 打印消息（如果提供）然后退出。如果 V(99) 或更高，
// 会调用 klog.Fatal 以输出更多信息。这是为维护者调试设计的，
// 超出了普通用户合理使用的范围。
func fatal(msg string, code int) {
	// nolint:logcheck // Not using the result of klog.V(99) inside the if
	// 分支中没有使用 klog.V(99) 的结果是可以的，我们只是用它来决定如何终止。
	if klog.V(99).Enabled() {
		klog.FatalDepth(2, msg)
	}
	if len(msg) > 0 {
		// 必要时添加换行符
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		fmt.Fprint(os.Stderr, msg)
	}
	os.Exit(code)
}

// ErrExit 可传给 CheckError，指示它不输出任何内容，
// 仅以状态码 1 退出。
var ErrExit = fmt.Errorf("exit")

// CheckErr 将用户可读的错误信息打印到 STDERR，并以非零退出码退出。
// 无法识别的错误会加上 "error: " 前缀输出。
//
// 该方法与具体命令无关，也可供非 Kubectl 命令使用。
func CheckErr(err error) {
	checkErr(err, fatalErrHandler)
}

// CheckDiffErr 将用户可读的错误信息打印到 STDERR，并以非零且非 1 的
// 退出码退出。无法识别的错误会加上 "error: " 前缀输出。
//
// 该方法专为 `kubectl diff` 设计，也可供其他命令使用。
func CheckDiffErr(err error) {
	checkErr(err, func(msg string, code int) {
		fatalErrHandler(msg, code+1)
	})
}

// isInvalidReasonStatusError 在错误是 reason=Invalid 的 API Status 错误时返回 true。
// 这不同于普通的 422 错误，后者我们会回退到通用错误处理。
func isInvalidReasonStatusError(err error) bool {
	if !apierrors.IsInvalid(err) {
		return false
	}
	statusError, isStatusError := err.(*apierrors.StatusError)
	if !isStatusError {
		return false
	}
	status := statusError.Status()
	return status.Reason == metav1.StatusReasonInvalid
}

// checkErr 将给定错误格式化为字符串，并以该字符串和 kubectl 退出码
// 调用传入的 handleErr 函数。
func checkErr(err error, handleErr func(string, int)) {
	// 解包只包含一个错误的聚合错误
	if agg, ok := err.(utilerrors.Aggregate); ok && len(agg.Errors()) == 1 {
		err = agg.Errors()[0]
	}

	if err == nil {
		return
	}

	switch {
	case err == ErrExit:
		handleErr("", DefaultErrorExitCode)
	case isInvalidReasonStatusError(err):
		status := err.(*apierrors.StatusError).Status()
		details := status.Details
		s := "The request is invalid"
		if details == nil {
			// 如果没有其他详情，则附上服务端返回的消息（如有）
			if len(status.Message) > 0 {
				s += ": " + status.Message
			}
			handleErr(s, DefaultErrorExitCode)
			return
		}
		if len(details.Kind) != 0 || len(details.Name) != 0 {
			s = fmt.Sprintf("The %s %q is invalid", details.Kind, details.Name)
		} else if len(status.Message) > 0 && len(details.Causes) == 0 {
			// 仅在没有 kind/name 详情且没有 causes 时才附上该消息，
			// 因为默认的 invalid 错误构造函数会在消息中重复这些信息
			s += ": " + status.Message
		}

		if len(details.Causes) > 0 {
			errs := statusCausesToAggrError(details.Causes)
			handleErr(MultilineError(s+": ", errs), DefaultErrorExitCode)
		} else {
			handleErr(s, DefaultErrorExitCode)
		}
	case clientcmd.IsConfigurationInvalid(err):
		handleErr(MultilineError("Error in configuration: ", err), DefaultErrorExitCode)
	default:
		switch err := err.(type) {
		case *meta.NoResourceMatchError:
			switch {
			case len(err.PartialResource.Group) > 0 && len(err.PartialResource.Version) > 0:
				handleErr(fmt.Sprintf("the server doesn't have a resource type %q in group %q and version %q", err.PartialResource.Resource, err.PartialResource.Group, err.PartialResource.Version), DefaultErrorExitCode)
			case len(err.PartialResource.Group) > 0:
				handleErr(fmt.Sprintf("the server doesn't have a resource type %q in group %q", err.PartialResource.Resource, err.PartialResource.Group), DefaultErrorExitCode)
			case len(err.PartialResource.Version) > 0:
				handleErr(fmt.Sprintf("the server doesn't have a resource type %q in version %q", err.PartialResource.Resource, err.PartialResource.Version), DefaultErrorExitCode)
			default:
				handleErr(fmt.Sprintf("the server doesn't have a resource type %q", err.PartialResource.Resource), DefaultErrorExitCode)
			}
		case utilerrors.Aggregate:
			handleErr(MultipleErrors(``, err.Errors()), DefaultErrorExitCode)
		case utilexec.ExitError:
			handleErr(err.Error(), err.ExitStatus())
		default: // 适用于任何其他错误类型
			msg, ok := StandardErrorMessage(err)
			if !ok {
				msg = err.Error()
				if !strings.HasPrefix(msg, "error: ") {
					msg = fmt.Sprintf("error: %s", msg)
				}
			}
			handleErr(msg, DefaultErrorExitCode)
		}
	}
}

func statusCausesToAggrError(scs []metav1.StatusCause) utilerrors.Aggregate {
	errs := make([]error, 0, len(scs))
	errorMsgs := sets.New[string]()
	for _, sc := range scs {
		// 检查重复的错误消息并跳过
		msg := fmt.Sprintf("%s: %s", sc.Field, sc.Message)
		if errorMsgs.Has(msg) {
			continue
		}
		errorMsgs.Insert(msg)
		errs = append(errs, errors.New(msg))
	}
	return utilerrors.NewAggregate(errs)
}

// StandardErrorMessage 将常见错误转换为用户可读的消息；
// 如果错误不属于已知类型，则返回 false。它还可能向 klog
// 记录扩展信息。
//
// 该方法与具体命令无关，也可供非 Kubectl 命令使用。
func StandardErrorMessage(err error) (string, bool) {
	if debugErr, ok := err.(debugError); ok {
		msg, args := debugErr.DebugError()
		klog.V(4).Infof(msg, args...)
	}
	status, isStatus := err.(apierrors.APIStatus)
	switch {
	case isStatus:
		switch s := status.Status(); {
		case s.Reason == metav1.StatusReasonUnauthorized:
			return fmt.Sprintf("error: You must be logged in to the server (%s)", s.Message), true
		case len(s.Reason) > 0:
			return fmt.Sprintf("Error from server (%s): %s", s.Reason, err.Error()), true
		default:
			return fmt.Sprintf("Error from server: %s", err.Error()), true
		}
	case apierrors.IsUnexpectedObjectError(err):
		return fmt.Sprintf("Server returned an unexpected response: %s", err.Error()), true
	}
	switch t := err.(type) {
	case *url.Error:
		klog.V(4).Infof("Connection error: %s %s: %v", t.Op, t.URL, t.Err)
		switch {
		case strings.Contains(t.Err.Error(), "connection refused"):
			host := t.URL
			if server, err := url.Parse(t.URL); err == nil {
				host = server.Host
			}
			return fmt.Sprintf("The connection to the server %s was refused - did you specify the right host or port?", host), true
		}
		return fmt.Sprintf("Unable to connect to the server: %v", t.Err), true
	}
	return "", false
}

// MultilineError 返回一个表示错误的字符串，其中每个子错误
// 各占一行。返回的字符串以换行符结尾。
func MultilineError(prefix string, err error) string {
	if agg, ok := err.(utilerrors.Aggregate); ok {
		errs := utilerrors.Flatten(agg).Errors()
		buf := &bytes.Buffer{}
		switch len(errs) {
		case 0:
			return fmt.Sprintf("%s%v\n", prefix, err)
		case 1:
			return fmt.Sprintf("%s%v\n", prefix, messageForError(errs[0]))
		default:
			fmt.Fprintln(buf, prefix)
			for _, err := range errs {
				fmt.Fprintf(buf, "* %v\n", messageForError(err))
			}
			return buf.String()
		}
	}
	return fmt.Sprintf("%s%s\n", prefix, err)
}

// PrintErrorWithCauses 将错误的 kind、name 以及每个 cause 分别打印在新的一行。
// 返回的字符串以换行符结尾。
// 如果存在处理该错误类型的分支，则返回 true，否则返回 false。
func PrintErrorWithCauses(err error, errOut io.Writer) bool {
	switch t := err.(type) {
	case *apierrors.StatusError:
		errorDetails := t.Status().Details
		if errorDetails != nil {
			fmt.Fprintf(errOut, "error: %s %q is invalid\n\n", errorDetails.Kind, errorDetails.Name)
			for _, cause := range errorDetails.Causes {
				fmt.Fprintf(errOut, "* %s: %s\n", cause.Field, cause.Message)
			}
			return true
		}
	}

	fmt.Fprintf(errOut, "error: %v\n", err)
	return false
}

// MultipleErrors 返回一个以换行符分隔的字符串，
// 其中包含前缀及标准格式的各个错误。
func MultipleErrors(prefix string, errs []error) string {
	buf := &bytes.Buffer{}
	for _, err := range errs {
		fmt.Fprintf(buf, "%s%v\n", prefix, messageForError(err))
	}
	return buf.String()
}

// messageForError 返回表示错误的字符串。
func messageForError(err error) string {
	msg, ok := StandardErrorMessage(err)
	if !ok {
		msg = err.Error()
	}
	return msg
}

func UsageErrorf(cmd *cobra.Command, format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s\nSee '%s -h' for help and examples", msg, cmd.CommandPath())
}

func IsFilenameSliceEmpty(filenames []string, directory string) bool {
	return len(filenames) == 0 && directory == ""
}

func GetFlagString(cmd *cobra.Command, flag string) string {
	s, err := cmd.Flags().GetString(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return s
}

// GetFlagStringSlice 可用于通过重复 flag 来接受多个参数（例如 -f arg1,arg2 -f arg3 ...）
func GetFlagStringSlice(cmd *cobra.Command, flag string) []string {
	s, err := cmd.Flags().GetStringSlice(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return s
}

// GetFlagStringArray 可用于通过重复 flag 来接受多个参数（例如 -f arg1 -f arg2 ...）
func GetFlagStringArray(cmd *cobra.Command, flag string) []string {
	s, err := cmd.Flags().GetStringArray(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return s
}

func GetFlagBool(cmd *cobra.Command, flag string) bool {
	b, err := cmd.Flags().GetBool(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return b
}

// 假定该 flag 具有默认值。
func GetFlagInt(cmd *cobra.Command, flag string) int {
	i, err := cmd.Flags().GetInt(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return i
}

// 假定该 flag 具有默认值。
func GetFlagInt32(cmd *cobra.Command, flag string) int32 {
	i, err := cmd.Flags().GetInt32(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return i
}

// 假定该 flag 具有默认值。
func GetFlagInt64(cmd *cobra.Command, flag string) int64 {
	i, err := cmd.Flags().GetInt64(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return i
}

func GetFlagDuration(cmd *cobra.Command, flag string) time.Duration {
	d, err := cmd.Flags().GetDuration(flag)
	if err != nil {
		klog.Fatalf("error accessing flag %s for command %s: %v", flag, cmd.Name(), err)
	}
	return d
}

func GetPodRunningTimeoutFlag(cmd *cobra.Command) (time.Duration, error) {
	timeout := GetFlagDuration(cmd, "pod-running-timeout")
	if timeout <= 0 {
		return timeout, fmt.Errorf("--pod-running-timeout must be higher than zero")
	}
	return timeout, nil
}

type FeatureGate string

const (
	// owner: @ardaguclu
	// kep: https://kep.k8s.io/3104
	//
	// 分离 kubectl 用户偏好设置。
	KubeRC FeatureGate = "KUBECTL_KUBERC"

	// owner: @justinb
	// kep: https://kep.k8s.io/3659
	//
	// 改进 kubectl apply --prune 的行为。
	ApplySet FeatureGate = "KUBECTL_APPLYSET"

	// owner: @seans
	// kep: https://kep.k8s.io/4006
	//
	// 过渡到 WebSockets。
	RemoteCommandWebsockets FeatureGate = "KUBECTL_REMOTE_COMMAND_WEBSOCKETS"
	PortForwardWebsockets   FeatureGate = "KUBECTL_PORT_FORWARD_WEBSOCKETS"
)

// IsEnabled 仅在环境变量被设置为 true 时返回 true，
// 其他所有情况均返回 false。
func (f FeatureGate) IsEnabled() bool {
	return strings.ToLower(os.Getenv(string(f))) == "true"
}

// IsDisabled 仅在环境变量被设置为 false 时返回 true，
// 其他所有情况均返回 true。
// 该函数用于功能默认启用的场景，
// 此时可能需要提供一种方式来禁用该功能。
func (f FeatureGate) IsDisabled() bool {
	return strings.ToLower(os.Getenv(string(f))) == "false"
}

func AddValidateFlags(cmd *cobra.Command) {
	cmd.Flags().String(
		"validate",
		"strict",
		`Must be one of: strict (or true), warn, ignore (or false). "true" or "strict" will use a schema to validate the input and fail the request if invalid. It will perform server side validation if ServerSideFieldValidation is enabled on the api-server, but will fall back to less reliable client-side validation if not. "warn" will warn about unknown or duplicate fields without blocking the request if server-side field validation is enabled on the API server, and behave as "ignore" otherwise. "false" or "ignore" will not perform any schema validation, silently dropping any unknown or duplicate fields.`,
	)

	cmd.Flags().Lookup("validate").NoOptDefVal = "strict"
}

func AddFilenameOptionFlags(cmd *cobra.Command, options *resource.FilenameOptions, usage string) {
	AddJsonFilenameFlag(cmd.Flags(), &options.Filenames, "Filename, directory, or URL to files "+usage)
	AddKustomizeFlag(cmd.Flags(), &options.Kustomize)
	cmd.Flags().BoolVarP(&options.Recursive, "recursive", "R", options.Recursive, "Process the directory used in -f, --filename recursively. Useful when you want to manage related manifests organized within the same directory.")
}

func AddJsonFilenameFlag(flags *pflag.FlagSet, value *[]string, usage string) {
	flags.StringSliceVarP(value, "filename", "f", *value, usage)
	annotations := make([]string, 0, len(resource.FileExtensions))
	for _, ext := range resource.FileExtensions {
		annotations = append(annotations, strings.TrimLeft(ext, "."))
	}
	flags.SetAnnotation("filename", cobra.BashCompFilenameExt, annotations)
}

// AddKustomizeFlag 为命令添加 kustomize flag
func AddKustomizeFlag(flags *pflag.FlagSet, value *string) {
	flags.StringVarP(value, "kustomize", "k", *value, "Process the kustomization directory. This flag can't be used together with -f or -R.")
}

// AddDryRunFlag 为命令添加 dry-run flag，通常用于变更类操作。
func AddDryRunFlag(cmd *cobra.Command) {
	cmd.Flags().String(
		"dry-run",
		"none",
		`Must be "none", "server", or "client". If client strategy, only print the object that would be sent, without sending it. If server strategy, submit server-side request without persisting the resource.`,
	)
	cmd.Flags().Lookup("dry-run").NoOptDefVal = "unchanged"
}

func AddFieldManagerFlagVar(cmd *cobra.Command, p *string, defaultFieldManager string) {
	cmd.Flags().StringVar(p, "field-manager", defaultFieldManager, "Name of the manager used to track field ownership.")
}

func AddContainerVarFlags(cmd *cobra.Command, p *string, containerName string) {
	cmd.Flags().StringVarP(p, "container", "c", containerName, "Container name. If omitted, use the kubectl.kubernetes.io/default-container annotation for selecting the container to be attached or the first container in the pod will be chosen")
}

func AddServerSideApplyFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("server-side", false, "If true, apply runs in the server instead of the client.")
	cmd.Flags().Bool("force-conflicts", false, "If true, server-side apply will force the changes against conflicts.")
}

func AddPodRunningTimeoutFlag(cmd *cobra.Command, defaultTimeout time.Duration) {
	cmd.Flags().Duration("pod-running-timeout", defaultTimeout, "The length of time (like 5s, 2m, or 3h, higher than zero) to wait until at least one pod is running")
}

func AddApplyAnnotationFlags(cmd *cobra.Command) {
	cmd.Flags().Bool(ApplyAnnotationsFlag, false, "If true, the configuration of current object will be saved in its annotation. Otherwise, the annotation will be unchanged. This flag is useful when you want to perform kubectl apply on this object in the future.")
}

func AddApplyAnnotationVarFlags(cmd *cobra.Command, applyAnnotation *bool) {
	cmd.Flags().BoolVar(applyAnnotation, ApplyAnnotationsFlag, *applyAnnotation, "If true, the configuration of current object will be saved in its annotation. Otherwise, the annotation will be unchanged. This flag is useful when you want to perform kubectl apply on this object in the future.")
}

func AddChunkSizeFlag(cmd *cobra.Command, value *int64) {
	cmd.Flags().Int64Var(value, "chunk-size", *value,
		"Return large lists in chunks rather than all at once. Pass 0 to disable.")
}

func AddLabelSelectorFlagVar(cmd *cobra.Command, p *string) {
	cmd.Flags().StringVarP(p, "selector", "l", *p, "Selector (label query) to filter on, supports '=', '==', '!=', 'in', 'notin'.(e.g. -l key1=value1,key2=value2,key3 in (value3)). Matching objects must satisfy all of the specified label constraints.")
}

func AddPruningFlags(cmd *cobra.Command, prune *bool, pruneAllowlist *[]string, all *bool, applySetRef *string) {
	// 与最初基于 allowlist 的 alpha 相关的 flag
	cmd.Flags().StringArrayVar(pruneAllowlist, "prune-allowlist", *pruneAllowlist, "Overwrite the default allowlist with <group/version/kind> for --prune")
	cmd.Flags().BoolVar(all, "all", *all, "Select all resources in the namespace of the specified resource types.")

	// 与新的基于 ApplySet 的 alpha 相关的 flag
	if ApplySet.IsEnabled() {
		cmd.Flags().StringVar(applySetRef, "applyset", *applySetRef, "[alpha] The name of the ApplySet that tracks which resources are being managed, for the purposes of determining what to prune. Live resources that are part of the ApplySet but have been removed from the provided configs will be deleted. Format: [RESOURCE][.GROUP]/NAME. A Secret will be used if no resource or group is specified.")
		cmd.Flags().BoolVar(prune, "prune", *prune, "Automatically delete previously applied resource objects that do not appear in the provided configs. For alpha1, use with either -l or --all. For alpha2, use with --applyset.")
	} else {
		// 如果只启用了 alpha1，则共享的 --prune flag 使用不同的说明文档
		cmd.Flags().BoolVar(prune, "prune", *prune, "Automatically delete resource objects, that do not appear in the configs and are created by either apply or create --save-config. Should be used with either -l or --all.")
	}
}

func AddSubresourceFlags(cmd *cobra.Command, subresource *string, usage string) {
	cmd.Flags().StringVar(subresource, "subresource", "", usage)
	CheckErr(cmd.RegisterFlagCompletionFunc("subresource", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		var commonSubresources = []string{"status", "scale", "resize"}
		return commonSubresources, cobra.ShellCompDirectiveNoFileComp
	}))
}

type ValidateOptions struct {
	ValidationDirective string
}

// Merge 将传入的对象转换为 JSON，使用 RFC7396 JSON Merge Patch
// 将 fragment 合并进去，并返回结果对象
// TODO: merge 假定使用 JSON 序列化，并且没有恰当地抽象 API 的获取方式
func Merge(codec runtime.Codec, dst runtime.Object, fragment string) (runtime.Object, error) {
	// 将 dst 编码为带版本的 json，然后直接对其应用 fragment
	target, err := runtime.Encode(codec, dst)
	if err != nil {
		return nil, err
	}
	patched, err := jsonpatch.MergePatch(target, []byte(fragment))
	if err != nil {
		return nil, err
	}
	out, err := runtime.Decode(codec, patched)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// StrategicMerge 将传入的对象转换为 JSON，使用 Strategic Merge Patch
// 将 fragment 合并进去，并返回结果对象
func StrategicMerge(codec runtime.Codec, dst runtime.Object, fragment string, dataStruct runtime.Object) (runtime.Object, error) {
	target, err := runtime.Encode(codec, dst)
	if err != nil {
		return nil, err
	}
	patched, err := strategicpatch.StrategicMergePatch(target, []byte(fragment), dataStruct)
	if err != nil {
		return nil, err
	}
	out, err := runtime.Decode(codec, patched)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JSONPatch 将传入的对象转换为 JSON，按照 fragment 中指定的操作执行
// RFC6902 JSON Patch，并返回结果对象
func JSONPatch(codec runtime.Codec, dst runtime.Object, fragment string) (runtime.Object, error) {
	target, err := runtime.Encode(codec, dst)
	if err != nil {
		return nil, err
	}
	patch, err := jsonpatch.DecodePatch([]byte(fragment))
	if err != nil {
		return nil, err
	}
	patched, err := patch.Apply(target)
	if err != nil {
		return nil, err
	}
	out, err := runtime.Decode(codec, patched)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DumpReaderToFile 将给定 io.Reader 中的所有数据写入指定文件
// （通常用于临时用途）。
func DumpReaderToFile(reader io.Reader, filename string) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	buffer := make([]byte, 1024)
	for {
		count, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		_, err = f.Write(buffer[:count])
		if err != nil {
			return err
		}
	}
	return nil
}

func GetServerSideApplyFlag(cmd *cobra.Command) bool {
	return GetFlagBool(cmd, "server-side")
}

func GetForceConflictsFlag(cmd *cobra.Command) bool {
	return GetFlagBool(cmd, "force-conflicts")
}

func GetFieldManagerFlag(cmd *cobra.Command) string {
	return GetFlagString(cmd, "field-manager")
}

func GetValidationDirective(cmd *cobra.Command) (string, error) {
	var validateFlag = GetFlagString(cmd, "validate")
	b, err := strconv.ParseBool(validateFlag)
	if err != nil {
		switch validateFlag {
		case "strict":
			return metav1.FieldValidationStrict, nil
		case "warn":
			return metav1.FieldValidationWarn, nil
		case "ignore":
			return metav1.FieldValidationIgnore, nil
		default:
			return metav1.FieldValidationStrict, fmt.Errorf(`invalid - validate option %q; must be one of: strict (or true), warn, ignore (or false)`, validateFlag)
		}
	}
	// 该 flag 是布尔值
	if b {
		return metav1.FieldValidationStrict, nil
	}
	return metav1.FieldValidationIgnore, nil
}

type DryRunStrategy int

const (
	// DryRunNone 表示客户端将发起所有变更类调用
	DryRunNone DryRunStrategy = iota

	// DryRunClient（即 client-side dry-run）表示客户端将阻止
	// 发起 CREATE、PATCH 和 DELETE 等变更类调用
	DryRunClient

	// DryRunServer（即 server-side dry-run）表示客户端将携带
	// dry-run 参数向 APIServer 发送变更类调用，以防止
	// 变更被持久化。
	//
	// 注意，发送 server-side dry-run 调用的客户端应确认
	// APIServer 和资源支持 server-side dry-run，否则
	// 客户端应尽早失败。
	//
	// 如果客户端向不支持 server-side dry-run 的 APIServer
	// 发送 server-side dry-run 调用，APIServer 将意外地持久化变更。
	DryRunServer
)

func GetDryRunStrategy(cmd *cobra.Command) (DryRunStrategy, error) {
	var dryRunFlag = GetFlagString(cmd, "dry-run")
	b, err := strconv.ParseBool(dryRunFlag)
	// 该 flag 不是布尔值
	if err != nil {
		switch dryRunFlag {
		case cmd.Flag("dry-run").NoOptDefVal:
			klog.Warning(`--dry-run is deprecated and can be replaced with --dry-run=client.`)
			return DryRunClient, nil
		case "client":
			return DryRunClient, nil
		case "server":
			return DryRunServer, nil
		case "none":
			return DryRunNone, nil
		default:
			return DryRunNone, fmt.Errorf(`Invalid dry-run value (%v). Must be "none", "server", or "client".`, dryRunFlag)
		}
	}
	// 该 flag 是布尔值
	if b {
		klog.Warningf(`--dry-run=%v is deprecated (boolean value) and can be replaced with --dry-run=%s.`, dryRunFlag, "client")
		return DryRunClient, nil
	}
	klog.Warningf(`--dry-run=%v is deprecated (boolean value) and can be replaced with --dry-run=%s.`, dryRunFlag, "none")
	return DryRunNone, nil
}

// PrintFlagsWithDryRunStrategy 在打印时根据 dry run 策略设置成功消息
//
// TODO(juanvallejo): 可以进一步清理：创建一个绑定 --dry-run flag 的
// PrintFlags 结构体，其 ToPrinter 方法返回一个了解如何打印
// 该成功消息的 printer。
func PrintFlagsWithDryRunStrategy(printFlags *genericclioptions.PrintFlags, dryRunStrategy DryRunStrategy) *genericclioptions.PrintFlags {
	switch dryRunStrategy {
	case DryRunClient:
		printFlags.Complete("%s (dry run)")
	case DryRunServer:
		printFlags.Complete("%s (server dry run)")
	}
	return printFlags
}

// GetResourcesAndPairs 从给定参数中提取资源以及形如 "KEY=VALUE 或 KEY-" 的键值对参数
func GetResourcesAndPairs(args []string, pairType string) (resources []string, pairArgs []string, err error) {
	foundPair := false
	for _, s := range args {
		nonResource := (strings.Contains(s, "=") && s[0] != '=') || (strings.HasSuffix(s, "-") && s != "-")
		switch {
		case !foundPair && nonResource:
			foundPair = true
			fallthrough
		case foundPair && nonResource:
			pairArgs = append(pairArgs, s)
		case !foundPair && !nonResource:
			resources = append(resources, s)
		case foundPair && !nonResource:
			err = fmt.Errorf("all resources must be specified before %s changes: %s", pairType, s)
			return
		}
	}
	return
}

// ParsePairs 从 "KEY=VALUE 或 KEY-" 形式的参数中提取新增和删除的键值对
// （当 supportRemove 为 true 时提取删除项）
func ParsePairs(pairArgs []string, pairType string, supportRemove bool) (newPairs map[string]string, removePairs []string, err error) {
	newPairs = map[string]string{}
	if supportRemove {
		removePairs = []string{}
	}
	var invalidBuf bytes.Buffer
	var invalidBufNonEmpty bool
	for _, pairArg := range pairArgs {
		if strings.Contains(pairArg, "=") && pairArg[0] != '=' {
			parts := strings.SplitN(pairArg, "=", 2)
			if len(parts) != 2 {
				if invalidBufNonEmpty {
					invalidBuf.WriteString(", ")
				}
				invalidBuf.WriteString(pairArg)
				invalidBufNonEmpty = true
			} else {
				newPairs[parts[0]] = parts[1]
			}
		} else if supportRemove && strings.HasSuffix(pairArg, "-") && pairArg != "-" {
			removePairs = append(removePairs, pairArg[:len(pairArg)-1])
		} else {
			if invalidBufNonEmpty {
				invalidBuf.WriteString(", ")
			}
			invalidBuf.WriteString(pairArg)
			invalidBufNonEmpty = true
		}
	}
	if invalidBufNonEmpty {
		err = fmt.Errorf("invalid %s format: %s", pairType, invalidBuf.String())
		return
	}

	return
}

// IsSiblingCommandExists 接受一个指向 cobra 命令的指针和一个目标字符串。
// 如果在同级命令列表中找到了目标字符串，则返回 true。
func IsSiblingCommandExists(cmd *cobra.Command, targetCmdName string) bool {
	for _, c := range cmd.Parent().Commands() {
		if c.Name() == targetCmdName {
			return true
		}
	}

	return false
}

// DefaultSubCommandRun 在没有参数（子命令）时将命令的帮助字符串打印到指定输出，
// 否则输出用法错误。
func DefaultSubCommandRun(out io.Writer) func(c *cobra.Command, args []string) {
	return func(c *cobra.Command, args []string) {
		c.SetOut(out)
		c.SetErr(out)
		RequireNoArguments(c, args)
		c.Help()
		CheckErr(ErrExit)
	}
}

// RequireNoArguments 在提供了多余参数时以用法错误退出。
func RequireNoArguments(c *cobra.Command, args []string) {
	if len(args) > 0 {
		CheckErr(UsageErrorf(c, "unknown command %q", strings.Join(args, " ")))
	}
}

// StripComments 会将 YAML 文件转换为 JSON，从而去掉其中的所有注释。
// 注意，如果给定文件存在语法错误，转换会失败，
// 此时我们会手动删除文件中的所有注释。
func StripComments(file []byte) []byte {
	stripped := file
	stripped, err := yaml.ToJSON(stripped)
	if err != nil {
		stripped = ManualStrip(file)
	}
	return stripped
}

// ManualStrip 用于删除 YAML 文件中的注释
func ManualStrip(file []byte) []byte {
	stripped := []byte{}
	lines := bytes.Split(file, []byte("\n"))
	for i, line := range lines {
		trimline := bytes.TrimSpace(line)

		if bytes.HasPrefix(trimline, []byte("#")) && !bytes.HasPrefix(trimline, []byte("#!")) {
			continue
		}
		stripped = append(stripped, line...)
		if i < len(lines)-1 {
			stripped = append(stripped, '\n')
		}
	}
	return stripped
}

// ScaleClientFunc 提供一个 ScalesGetter
type ScaleClientFunc func(genericclioptions.RESTClientGetter) (scale.ScalesGetter, error)

// ScaleClientFn 用于在需要时轻松覆盖该函数以进行单元测试。
var ScaleClientFn ScaleClientFunc = scaleClient

// scaleClient 返回一个 scale getter
func scaleClient(restClientGetter genericclioptions.RESTClientGetter) (scale.ScalesGetter, error) {
	discoveryClient, err := restClientGetter.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}

	clientConfig, err := restClientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	setKubernetesDefaults(clientConfig)
	restClient, err := rest.RESTClientFor(clientConfig)
	if err != nil {
		return nil, err
	}
	resolver := scale.NewDiscoveryScaleKindResolver(discoveryClient)
	mapper, err := restClientGetter.ToRESTMapper()
	if err != nil {
		return nil, err
	}

	return scale.New(restClient, mapper, dynamic.LegacyAPIPathResolverFunc, resolver), nil
}

// Difference 从 fullArray 中移除 subArray 中的所有元素并返回结果
func Difference(fullArray []string, subArray []string) []string {
	exclude := make(map[string]bool, len(subArray))
	for _, elem := range subArray {
		exclude[elem] = true
	}
	var result []string
	for _, elem := range fullArray {
		if _, found := exclude[elem]; !found {
			result = append(result, elem)
		}
	}
	return result
}
