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

package exec

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	dockerterm "github.com/moby/term"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"

	"k8s.io/apimachinery/pkg/api/meta"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/util/podcmd"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/interrupt"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/kubectl/pkg/util/term"
)

var (
	execExample = templates.Examples(i18n.T(`
		# Get output from running the 'date' command from pod mypod, using the first container by default
		kubectl exec mypod -- date

		# Get output from running the 'date' command in ruby-container from pod mypod
		kubectl exec mypod -c ruby-container -- date

		# Switch to raw terminal mode; sends stdin to 'bash' in ruby-container from pod mypod
		# and sends stdout/stderr from 'bash' back to the client
		kubectl exec mypod -c ruby-container -i -t -- bash -il

		# List contents of /usr from the first container of pod mypod and sort by modification time
		# If the command you want to execute in the pod has any flags in common (e.g. -i),
		# you must use two dashes (--) to separate your command's flags/arguments
		# Also note, do not surround your command and its flags/arguments with quotes
		# unless that is how you would execute it normally (i.e., do ls -t /usr, not "ls -t /usr")
		kubectl exec mypod -i -t -- ls -t /usr

		# Get output from running 'date' command from the first pod of the deployment mydeployment, using the first container by default
		kubectl exec deploy/mydeployment -- date

		# Get output from running 'date' command from the first pod of the service myservice, using the first container by default
		kubectl exec svc/myservice -- date
		`))
)

const (
	defaultPodExecTimeout = 60 * time.Second
)

func NewCmdExec(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	options := &ExecOptions{
		StreamOptions: StreamOptions{
			IOStreams: streams,
		},

		Executor: &DefaultRemoteExecutor{},
	}
	cmd := &cobra.Command{
		Use:                   "exec (POD | TYPE/NAME) [-c CONTAINER] [flags] -- COMMAND [args...]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Execute a command in a container"),
		Long:                  i18n.T("Execute a command in a container."),
		Example:               execExample,
		ValidArgsFunction:     completion.PodResourceNameCompletionFunc(f),
		Run: func(cmd *cobra.Command, args []string) {
			argsLenAtDash := cmd.ArgsLenAtDash()
			cmdutil.CheckErr(options.Complete(f, cmd, args, argsLenAtDash))
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.Run())
		},
	}
	cmdutil.AddPodRunningTimeoutFlag(cmd, defaultPodExecTimeout)
	cmdutil.AddJsonFilenameFlag(cmd.Flags(), &options.FilenameOptions.Filenames, "to use to exec into the resource")
	// TODO: 支持 UID
	cmdutil.AddContainerVarFlags(cmd, &options.ContainerName, options.ContainerName)
	cmdutil.CheckErr(cmd.RegisterFlagCompletionFunc("container", completion.ContainerCompletionFunc(f)))

	cmd.Flags().BoolVarP(&options.Stdin, "stdin", "i", options.Stdin, "Pass stdin to the container")
	cmd.Flags().BoolVarP(&options.TTY, "tty", "t", options.TTY, "Stdin is a TTY")
	cmd.Flags().BoolVarP(&options.Quiet, "quiet", "q", options.Quiet, "Only print output from the remote session")
	return cmd
}

// RemoteExecutor 定义 Exec 命令接受的接口，用于测试时打桩
type RemoteExecutor interface {
	// Execute 支持在 pod 中执行远程命令。
	Execute(url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) error

	// 与 Execute 不同，ExecuteWithContext 支持通过取消 context 来停止远程命令。
	ExecuteWithContext(ctx context.Context, url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) error
}

// DefaultRemoteExecutor 是远程命令执行的标准实现
type DefaultRemoteExecutor struct{}

func (d *DefaultRemoteExecutor) Execute(url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) error {
	return d.ExecuteWithContext(context.Background(), url, config, stdin, stdout, stderr, tty, terminalSizeQueue)
}

func (*DefaultRemoteExecutor) ExecuteWithContext(ctx context.Context, url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) error {
	exec, err := createExecutor(url, config)
	if err != nil {
		return err
	}
	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:             stdin,
		Stdout:            stdout,
		Stderr:            stderr,
		Tty:               tty,
		TerminalSizeQueue: terminalSizeQueue,
	})
}

// createExecutor 返回 Executor，若发生错误则返回错误。
func createExecutor(url *url.URL, config *restclient.Config) (remotecommand.Executor, error) {
	exec, err := remotecommand.NewSPDYExecutor(config, "POST", url)
	if err != nil {
		return nil, err
	}
	// 除非显式禁用 feature flag，否则默认使用 fallback executor。
	if !cmdutil.RemoteCommandWebsockets.IsDisabled() {
		// 按照 RFC 6455 第 4.1 节（第 17 页）的描述，WebSocketExecutor 必须使用 "GET" 方法。
		websocketExec, err := remotecommand.NewWebSocketExecutor(config, "GET", url.String())
		if err != nil {
			return nil, err
		}
		exec, err = remotecommand.NewFallbackExecutor(websocketExec, exec, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		})
		if err != nil {
			return nil, err
		}
	}
	return exec, nil
}

type StreamOptions struct {
	Namespace     string
	PodName       string
	ContainerName string
	Stdin         bool
	TTY           bool
	// 尽量减少不必要的输出
	Quiet bool
	// 如果设置了 InterruptParent，则在 attached 状态下用它处理中断
	InterruptParent *interrupt.Handler

	genericiooptions.IOStreams

	// 用于测试
	overrideStreams func() (io.ReadCloser, io.Writer, io.Writer)
	isTerminalIn    func(t term.TTY) bool
}

// ExecOptions 声明 Exec 命令接受的参数
type ExecOptions struct {
	StreamOptions
	resource.FilenameOptions

	ResourceName     string
	Command          []string
	EnforceNamespace bool

	Builder          func() *resource.Builder
	ExecutablePodFn  polymorphichelpers.AttachablePodForObjectFunc
	restClientGetter genericclioptions.RESTClientGetter

	Pod           *corev1.Pod
	Executor      RemoteExecutor
	PodClient     coreclient.PodsGetter
	GetPodTimeout time.Duration
	Config        *restclient.Config
}

// Complete 校验命令行参数，并从命令环境中加载数据
func (p *ExecOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, argsIn []string, argsLenAtDash int) error {
	if len(argsIn) > 0 && argsLenAtDash != 0 {
		p.ResourceName = argsIn[0]
	}
	// 我们期望在短横线分隔符之前恰好有一个参数（pod/资源名称）。
	// pflag 保证 `argsLenAtDash <= len(args)`。
	if argsLenAtDash == 0 || argsLenAtDash == 1 {
		p.Command = argsIn[argsLenAtDash:]
	} else if len(argsIn) > 1 || (len(argsIn) > 0 && len(p.FilenameOptions.Filenames) != 0) {
		return cmdutil.UsageErrorf(cmd, "exec [POD] [COMMAND] is not supported anymore. Use exec [POD] -- [COMMAND] instead")
	}

	var err error
	p.Namespace, p.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	p.ExecutablePodFn = polymorphichelpers.AttachablePodForObjectFn

	p.GetPodTimeout, err = cmdutil.GetPodRunningTimeoutFlag(cmd)
	if err != nil {
		return cmdutil.UsageErrorf(cmd, "%s", err.Error())
	}

	p.Builder = f.NewBuilder
	p.restClientGetter = f

	p.Config, err = f.ToRESTConfig()
	if err != nil {
		return err
	}

	clientset, err := f.KubernetesClientSet()
	if err != nil {
		return err
	}
	p.PodClient = clientset.CoreV1()

	return nil
}

// Validate 检查所提供的 exec 选项是否已指定。
func (p *ExecOptions) Validate() error {
	if len(p.PodName) == 0 && len(p.ResourceName) == 0 && len(p.FilenameOptions.Filenames) == 0 {
		return fmt.Errorf("pod, type/name or --filename must be specified")
	}
	if len(p.Command) == 0 {
		return fmt.Errorf("you must specify at least one command for the container")
	}
	if p.Out == nil || p.ErrOut == nil {
		return fmt.Errorf("both output and error output must be provided")
	}
	return nil
}

func (o *StreamOptions) SetupTTY() term.TTY {
	t := term.TTY{
		Parent: o.InterruptParent,
		Out:    o.Out,
	}

	if !o.Stdin {
		// 需要将 o.In 置为 nil，以确保不会为 stdin 创建流
		o.In = nil
		o.TTY = false
		return t
	}

	t.In = o.In
	if !o.TTY {
		return t
	}

	if o.isTerminalIn == nil {
		o.isTerminalIn = func(tty term.TTY) bool {
			return tty.IsTerminalIn()
		}
	}
	if !o.isTerminalIn(t) {
		o.TTY = false

		if !o.Quiet && o.ErrOut != nil {
			fmt.Fprintln(o.ErrOut, "Unable to use a TTY - input is not a terminal or the right kind of file")
		}

		return t
	}

	// 能执行到这里，说明用户希望 attach stdin、请求 TTY，且 o.In 是终端，因此
	// 可以安全地将 t.Raw 设为 true
	t.Raw = true

	if o.overrideStreams == nil {
		// 在 Windows 上使用 dockerterm.StdStreams() 获取正确的 I/O 句柄
		o.overrideStreams = dockerterm.StdStreams
	}
	stdin, stdout, _ := o.overrideStreams()
	o.In = stdin
	t.In = stdin
	if o.Out != nil {
		o.Out = stdout
		t.Out = stdout
	}

	return t
}

// Run 针对 pod 执行经过校验的远程执行。
func (p *ExecOptions) Run() error {
	var err error
	// 当 ExecOptions 结构体中提供了 PodName 时，仍需使用旧的 pod getter，
	// 因为其他命令可能通过提供 PodName 和 PodsGetter 来调用此函数，
	// 且不提供 resource builder，例如 `kubectl cp`。
	if len(p.PodName) != 0 {
		p.Pod, err = p.PodClient.Pods(p.Namespace).Get(context.TODO(), p.PodName, metav1.GetOptions{})
		if err != nil {
			return err
		}
	} else {
		builder := p.Builder().
			WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
			FilenameParam(p.EnforceNamespace, &p.FilenameOptions).
			NamespaceParam(p.Namespace).DefaultNamespace()
		if len(p.ResourceName) > 0 {
			builder = builder.ResourceNames("pods", p.ResourceName)
		}

		obj, err := builder.Do().Object()
		if err != nil {
			return err
		}

		if meta.IsListType(obj) {
			return fmt.Errorf("cannot exec into multiple objects at a time")
		}

		p.Pod, err = p.ExecutablePodFn(p.restClientGetter, obj, p.GetPodTimeout)
		if err != nil {
			return err
		}
	}

	pod := p.Pod

	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return fmt.Errorf("cannot exec into a container in a completed pod; current phase is %s", pod.Status.Phase)
	}

	containerName := p.ContainerName
	if len(containerName) == 0 {
		container, err := podcmd.FindOrDefaultContainerByName(pod, containerName, p.Quiet, p.ErrOut)
		if err != nil {
			return err
		}
		containerName = container.Name
	} else {
		container, _ := podcmd.FindContainerByName(pod, p.ContainerName)
		if container == nil {
			return fmt.Errorf("container %s is not valid for pod %s out of: %s", p.ContainerName, pod.Name, podcmd.AllContainerNames(pod))
		}
	}

	// 确保在 attached 状态下可以恢复终端
	t := p.SetupTTY()

	var sizeQueue remotecommand.TerminalSizeQueue
	if t.Raw {
		// 此调用会启动一个 goroutine 来监控/更新终端大小
		sizeQueue = &terminalSizeQueueAdapter{
			delegate: t.MonitorSize(t.GetSize()),
		}

		// 如果之前设置了 p.Err，则将其清除，因为在 tty 为 true 时 stdout 和 stderr
		// 都会通过 p.Out 输出
		p.ErrOut = nil
	}

	fn := func() error {
		restClient, err := restclient.RESTClientFor(p.Config)
		if err != nil {
			return err
		}

		// TODO: 考虑抽象为 client 调用或 client helper
		req := restClient.Post().
			Resource("pods").
			Name(pod.Name).
			Namespace(pod.Namespace).
			SubResource("exec")
		req.VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   p.Command,
			Stdin:     p.Stdin,
			Stdout:    p.Out != nil,
			Stderr:    p.ErrOut != nil,
			TTY:       t.Raw,
		}, scheme.ParameterCodec)

		return p.Executor.Execute(req.URL(), p.Config, p.In, p.Out, p.ErrOut, t.Raw, sizeQueue)
	}

	if err := t.Safe(fn); err != nil {
		return err
	}

	return nil
}

type terminalSizeQueueAdapter struct {
	delegate term.TerminalSizeQueue
}

func (a *terminalSizeQueueAdapter) Next() *remotecommand.TerminalSize {
	if a.delegate == nil {
		return nil
	}

	next := a.delegate.Next()
	if next == nil {
		return nil
	}
	return &remotecommand.TerminalSize{
		Width:  next.Width,
		Height: next.Height,
	}
}
