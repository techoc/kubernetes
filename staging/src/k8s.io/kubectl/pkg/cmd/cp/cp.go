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

package cp

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/cmd/exec"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	cpExample = templates.Examples(i18n.T(`
		# !!!Important Note!!!
		# Requires that the 'tar' binary is present in your container
		# image.  If 'tar' is not present, 'kubectl cp' will fail.
		#
		# For advanced use cases, such as symlinks, wildcard expansion or
		# file mode preservation, consider using 'kubectl exec'.

		# Copy /tmp/foo local file to /tmp/bar in a remote pod in namespace <some-namespace>
		tar cf - /tmp/foo | kubectl exec -i -n <some-namespace> <some-pod> -- tar xf - -C /tmp/bar

		# Copy /tmp/foo from a remote pod to /tmp/bar locally
		kubectl exec -n <some-namespace> <some-pod> -- tar cf - /tmp/foo | tar xf - -C /tmp/bar

		# Copy /tmp/foo_dir local directory to /tmp/bar_dir in a remote pod in the default namespace
		kubectl cp /tmp/foo_dir <some-pod>:/tmp/bar_dir

		# Copy /tmp/foo local file to /tmp/bar in a remote pod in a specific container
		kubectl cp /tmp/foo <some-pod>:/tmp/bar -c <specific-container>

		# Copy /tmp/foo local file to /tmp/bar in a remote pod in namespace <some-namespace>
		kubectl cp /tmp/foo <some-namespace>/<some-pod>:/tmp/bar

		# Copy /tmp/foo from a remote pod to /tmp/bar locally
		kubectl cp <some-namespace>/<some-pod>:/tmp/foo /tmp/bar`))
)

// CopyOptions 包含执行复制操作所需的数据
type CopyOptions struct {
	Container  string
	Namespace  string
	NoPreserve bool
	MaxTries   int

	ClientConfig      *restclient.Config
	Clientset         kubernetes.Interface
	ExecParentCmdName string
	Executor          exec.RemoteExecutor

	args []string

	genericiooptions.IOStreams
}

// NewCopyOptions 创建复制所需的选项
func NewCopyOptions(ioStreams genericiooptions.IOStreams) *CopyOptions {
	return &CopyOptions{
		IOStreams: ioStreams,
	}
}

// NewCmdCp 创建一个新的 Copy 命令。
func NewCmdCp(f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := NewCopyOptions(ioStreams)

	cmd := &cobra.Command{
		Use:                   "cp <file-spec-src> <file-spec-dest>",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Copy files and directories to and from containers"),
		Long:                  i18n.T("Copy files and directories to and from containers."),
		Example:               cpExample,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			var comps []string
			if len(args) == 0 {
				if strings.IndexAny(toComplete, "/.~") == 0 {
					// 看起来是路径，不做处理
				} else if strings.Contains(toComplete, ":") {
					// TODO: 补全 pod 中的远端文件
				} else if idx := strings.Index(toComplete, "/"); idx > 0 {
					// 补全 <namespace>/<pod>
					namespace := toComplete[:idx]
					template := "{{ range .items }}{{ .metadata.namespace }}/{{ .metadata.name }}: {{ end }}"
					comps = completion.CompGetFromTemplate(&template, f, namespace, []string{"pod"}, toComplete)
				} else {
					// 补全后面跟着 / 的命名空间
					for _, ns := range completion.CompGetResource(f, "namespace", toComplete) {
						comps = append(comps, fmt.Sprintf("%s/", ns))
					}
					// 补全后面跟着 : 的 pod 名称
					for _, pod := range completion.CompGetResource(f, "pod", toComplete) {
						comps = append(comps, fmt.Sprintf("%s:", pod))
					}

					// 最后，在需要时提供文件补全。
					// 仅在以下情况下这样做：
					// 1- 已找到其他补全结果（如果没有补全结果，
					//    shell 会自行完成文件补全）
					// 2- 用户已输入一些内容（否则最终会
					//    列出当前目录的全部内容，选项可能
					//    过多，对用户不便）
					if len(comps) > 0 && len(toComplete) > 0 {
						if files, err := os.ReadDir("."); err == nil {
							for _, file := range files {
								filename := file.Name()
								if strings.HasPrefix(filename, toComplete) {
									if file.IsDir() {
										filename = fmt.Sprintf("%s/", filename)
									}
									// 我们正在补全文件前缀
									comps = append(comps, filename)
								}
							}
						}
					} else if len(toComplete) == 0 {
						// 如果用户未提供任何待补全的输入，
						// 就提示也可以使用路径
						comps = append(comps, "./", "/")
					}
				}
			}
			return comps, cobra.ShellCompDirectiveNoSpace
		},
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}
	cmdutil.AddContainerVarFlags(cmd, &o.Container, o.Container)
	cmd.Flags().BoolVarP(&o.NoPreserve, "no-preserve", "", false, "The copied file/directory's ownership and permissions will not be preserved in the container")
	cmd.Flags().IntVarP(&o.MaxTries, "retries", "", 0, "Set number of retries to complete a copy operation from a container. Specify 0 to disable or any negative value for infinite retrying. The default is 0 (no retry).")

	return cmd
}

var (
	errFileSpecDoesntMatchFormat = errors.New("filespec must match the canonical format: [[namespace/]pod:]file/path")
)

func extractFileSpec(arg string) (fileSpec, error) {
	i := strings.Index(arg, ":")

	// 以分号开头的 filespec 无效
	if i == 0 {
		return fileSpec{}, errFileSpecDoesntMatchFormat
	}
	if i == -1 {
		return fileSpec{
			File: newLocalPath(arg),
		}, nil
	}

	pod, file := arg[:i], arg[i+1:]
	pieces := strings.Split(pod, "/")
	switch len(pieces) {
	case 1:
		return fileSpec{
			PodName: pieces[0],
			File:    newRemotePath(file),
		}, nil
	case 2:
		return fileSpec{
			PodNamespace: pieces[0],
			PodName:      pieces[1],
			File:         newRemotePath(file),
		}, nil
	default:
		return fileSpec{}, errFileSpecDoesntMatchFormat
	}
}

// Complete 补全所有必需的选项
func (o *CopyOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	if cmd.Parent() != nil {
		o.ExecParentCmdName = cmd.Parent().CommandPath()
	}
	o.Executor = &exec.DefaultRemoteExecutor{}

	var err error
	o.Namespace, _, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.Clientset, err = f.KubernetesClientSet()
	if err != nil {
		return err
	}

	o.ClientConfig, err = f.ToRESTConfig()
	if err != nil {
		return err
	}

	o.args = args
	return nil
}

// Validate 确保 CopyOptions 中提供的值有效
func (o *CopyOptions) Validate() error {
	if len(o.args) != 2 {
		return fmt.Errorf("source and destination are required")
	}
	return nil
}

// Run 执行操作
func (o *CopyOptions) Run() error {
	srcSpec, err := extractFileSpec(o.args[0])
	if err != nil {
		return err
	}
	destSpec, err := extractFileSpec(o.args[1])
	if err != nil {
		return err
	}

	if len(srcSpec.PodName) != 0 && len(destSpec.PodName) != 0 {
		return fmt.Errorf("one of src or dest must be a local file specification")
	}
	if len(srcSpec.File.String()) == 0 || len(destSpec.File.String()) == 0 {
		return errors.New("filepath can not be empty")
	}

	if len(srcSpec.PodName) != 0 {
		return o.copyFromPod(srcSpec, destSpec)
	}
	if len(destSpec.PodName) != 0 {
		return o.copyToPod(srcSpec, destSpec, &exec.ExecOptions{})
	}
	return fmt.Errorf("one of src or dest must be a remote file specification")
}

// checkDestinationIsDir 接收目标 fileSpec，
// 判断给定目标路径是否存在于
// pod 上。如果目标路径不存在，或者_不是_
// 目录，则返回错误及收到的退出码。
func (o *CopyOptions) checkDestinationIsDir(dest fileSpec) error {
	options := &exec.ExecOptions{
		StreamOptions: exec.StreamOptions{
			IOStreams: genericiooptions.IOStreams{
				Out:    io.Discard,
				ErrOut: io.Discard,
			},

			Namespace: dest.PodNamespace,
			PodName:   dest.PodName,
		},

		Command:  []string{"test", "-d", dest.File.String()},
		Executor: o.Executor,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan error)

	go func() {
		done <- o.execute(options)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (o *CopyOptions) copyToPod(src, dest fileSpec, options *exec.ExecOptions) error {
	if _, err := os.Stat(src.File.String()); err != nil {
		return fmt.Errorf("%s doesn't exist in local filesystem", src.File)
	}
	reader, writer := io.Pipe()

	srcFile := src.File.(localPath)
	destFile := dest.File.(remotePath)

	if err := o.checkDestinationIsDir(dest); err == nil {
		// 没有错误表示 dest.File 是一个目录。
		// 将指定的 src 复制到其中
		destFile = destFile.Join(srcFile.Base())
	} else if errors.Is(err, context.DeadlineExceeded) {
		// 由于超出 context 超时时间，我们尚未确定目标是否为目录。
		// 因此，这里应直接跳过后续流程。
		return err
	}

	go func(src localPath, dest remotePath, writer io.WriteCloser) {
		defer writer.Close()
		cmdutil.CheckErr(makeTar(src, dest, writer))
	}(srcFile, destFile, writer)
	var cmdArr []string

	if o.NoPreserve {
		cmdArr = []string{"tar", "--no-same-permissions", "--no-same-owner", "-xmf", "-"}
	} else {
		cmdArr = []string{"tar", "-xmf", "-"}
	}
	destFileDir := destFile.Dir().String()
	if len(destFileDir) > 0 {
		cmdArr = append(cmdArr, "-C", destFileDir)
	}

	options.StreamOptions = exec.StreamOptions{
		IOStreams: genericiooptions.IOStreams{
			In:     reader,
			Out:    o.Out,
			ErrOut: o.ErrOut,
		},
		Stdin: true,

		Namespace: dest.PodNamespace,
		PodName:   dest.PodName,
	}

	options.Command = cmdArr
	options.Executor = o.Executor
	return o.execute(options)
}

func (o *CopyOptions) copyFromPod(src, dest fileSpec) error {
	reader := newTarPipe(src, o)
	srcFile := src.File.(remotePath)
	destFile := dest.File.(localPath)
	// 移除多余的路径快捷符——如果路径包含多余的 "../" 就可能出现这种情况，
	// 并试图在远端文件系统中导航到 "/" 之外
	prefix := stripPathShortcuts(srcFile.StripSlashes().Clean().String())
	return o.untarAll(src.PodNamespace, src.PodName, prefix, srcFile, destFile, reader)
}

type TarPipe struct {
	src       fileSpec
	o         *CopyOptions
	reader    *io.PipeReader
	outStream *io.PipeWriter
	bytesRead uint64
	retries   int
}

func newTarPipe(src fileSpec, o *CopyOptions) *TarPipe {
	t := new(TarPipe)
	t.src = src
	t.o = o
	t.initReadFrom(0)
	return t
}

func (t *TarPipe) initReadFrom(n uint64) {
	t.reader, t.outStream = io.Pipe()
	options := &exec.ExecOptions{
		StreamOptions: exec.StreamOptions{
			IOStreams: genericiooptions.IOStreams{
				In:     nil,
				Out:    t.outStream,
				ErrOut: t.o.Out,
			},

			Namespace: t.src.PodNamespace,
			PodName:   t.src.PodName,
		},

		Command:  []string{"tar", "cf", "-", t.src.File.String()},
		Executor: t.o.Executor,
	}
	if t.o.MaxTries != 0 {
		escapedPath := strings.ReplaceAll(t.src.File.String(), "'", `'\''`)
		options.Command = []string{"sh", "-c", fmt.Sprintf("tar cf - '%s' | tail -c+%d", escapedPath, n)}
	}

	go func() {
		defer t.outStream.Close()
		cmdutil.CheckErr(t.o.execute(options))
	}()
}

func (t *TarPipe) Read(p []byte) (n int, err error) {
	n, err = t.reader.Read(p)
	if err != nil {
		if t.o.MaxTries < 0 || t.retries < t.o.MaxTries {
			t.retries++
			fmt.Printf("Resuming copy at %d bytes, retry %d/%d\n", t.bytesRead, t.retries, t.o.MaxTries)
			t.initReadFrom(t.bytesRead + 1)
			err = nil
		} else {
			fmt.Printf("Dropping out copy after %d retries\n", t.retries)
		}
	} else {
		t.bytesRead += uint64(n)
	}
	return
}

func makeTar(src localPath, dest remotePath, writer io.Writer) error {
	// TODO: 这里是否使用压缩？
	tarWriter := tar.NewWriter(writer)
	defer tarWriter.Close()

	srcPath := src.Clean()
	destPath := dest.Clean()
	return recursiveTar(srcPath.Dir(), srcPath.Base(), destPath.Dir(), destPath.Base(), tarWriter)
}

func recursiveTar(srcDir, srcFile localPath, destDir, destFile remotePath, tw *tar.Writer) error {
	matchedPaths, err := srcDir.Join(srcFile).Glob()
	if err != nil {
		return err
	}
	for _, fpath := range matchedPaths {
		stat, err := os.Lstat(fpath)
		if err != nil {
			return err
		}
		if stat.IsDir() {
			files, err := os.ReadDir(fpath)
			if err != nil {
				return err
			}
			if len(files) == 0 {
				//空目录的情况
				hdr, _ := tar.FileInfoHeader(stat, fpath)
				hdr.Name = destFile.String()
				if err := tw.WriteHeader(hdr); err != nil {
					return err
				}
			}
			for _, f := range files {
				if err := recursiveTar(srcDir, srcFile.Join(newLocalPath(f.Name())),
					destDir, destFile.Join(newRemotePath(f.Name())), tw); err != nil {
					return err
				}
			}
			return nil
		} else if stat.Mode()&os.ModeSymlink != 0 {
			//软链接的情况
			hdr, _ := tar.FileInfoHeader(stat, fpath)
			target, err := os.Readlink(fpath)
			if err != nil {
				return err
			}

			hdr.Linkname = target
			hdr.Name = destFile.String()
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
		} else {
			//普通文件或管道等其他文件类型的情况
			hdr, err := tar.FileInfoHeader(stat, fpath)
			if err != nil {
				return err
			}
			hdr.Name = destFile.String()

			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}

			f, err := os.Open(fpath)
			if err != nil {
				return err
			}
			defer f.Close()

			if _, err := io.Copy(tw, f); err != nil {
				return err
			}
			return f.Close()
		}
	}
	return nil
}

func (o *CopyOptions) untarAll(ns, pod string, prefix string, src remotePath, dest localPath, reader io.Reader) error {
	symlinkWarningPrinted := false
	// TODO: 这里是否使用压缩？
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		// 所有文件都会以前缀开头，该前缀是它们在
		// pod 上所在目录，我们需要去除该前缀；但如果
		// 前缀缺失，则说明 tar 被篡改过。
		// 对于前缀为空的情况，我们需要确保路径
		// 不是绝对路径，这同样表明 tar 文件被篡改过。
		if !strings.HasPrefix(header.Name, prefix) {
			return fmt.Errorf("tar contents corrupted")
		}

		// 基本文件信息
		mode := header.FileInfo().Mode()
		// header.Name 是远端文件的名称，因此我们需要创建
		// remotePath，使其经过与清理远端路径相关的
		// 适当处理
		destFileName := dest.Join(newRemotePath(header.Name[len(prefix):]))

		if !isRelative(dest, destFileName) {
			fmt.Fprintf(o.IOStreams.ErrOut, "warning: file %q is outside target destination, skipping\n", destFileName)
			continue
		}

		if err := os.MkdirAll(destFileName.Dir().String(), 0755); err != nil {
			return err
		}
		if header.FileInfo().IsDir() {
			if err := os.MkdirAll(destFileName.String(), 0755); err != nil {
				return err
			}
			continue
		}

		if mode&os.ModeSymlink != 0 {
			if !symlinkWarningPrinted && len(o.ExecParentCmdName) > 0 {
				fmt.Fprintf(o.IOStreams.ErrOut,
					"warning: skipping symlink: %q -> %q (consider using \"%s exec -n %q %q -- tar cf - %q | tar xf -\")\n",
					destFileName, header.Linkname, o.ExecParentCmdName, ns, pod, src)
				symlinkWarningPrinted = true
				continue
			}
			fmt.Fprintf(o.IOStreams.ErrOut, "warning: skipping symlink: %q -> %q\n", destFileName, header.Linkname)
			continue
		}
		outFile, err := os.Create(destFileName.String())
		if err != nil {
			return err
		}
		defer outFile.Close()
		if _, err := io.Copy(outFile, tarReader); err != nil {
			return err
		}
		if err := outFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (o *CopyOptions) execute(options *exec.ExecOptions) error {
	if len(options.Namespace) == 0 {
		options.Namespace = o.Namespace
	}

	if len(o.Container) > 0 {
		options.ContainerName = o.Container
	}

	options.Config = o.ClientConfig
	options.PodClient = o.Clientset.CoreV1()

	if err := options.Validate(); err != nil {
		return err
	}

	return options.Run()
}
