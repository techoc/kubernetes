/*
Copyright 2017 The Kubernetes Authors.

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

package diff

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jonboulle/clockwork"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/openapi3"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/cmd/apply"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/openapi"
	"k8s.io/kubectl/pkg/util/prune"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/utils/exec"
	"sigs.k8s.io/yaml"
)

var (
	diffLong = templates.LongDesc(i18n.T(`
		Diff configurations specified by file name or stdin between the current online
		configuration, and the configuration as it would be if applied.

		The output is always YAML.

		KUBECTL_EXTERNAL_DIFF environment variable can be used to select your own
		diff command. Users can use external commands with params too, example:
		KUBECTL_EXTERNAL_DIFF="colordiff -N -u"

		By default, the "diff" command available in your path will be
		run with the "-u" (unified diff) and "-N" (treat absent files as empty) options.

		Exit status:
		 0
		No differences were found.
		 1
		Differences were found.
		 >1
		Kubectl or diff failed with an error.

		Note: KUBECTL_EXTERNAL_DIFF, if used, is expected to follow that convention.`))

	diffExample = templates.Examples(i18n.T(`
		# Diff resources included in pod.json
		kubectl diff -f pod.json

		# Diff file read from stdin
		cat service.yaml | kubectl diff -f -`))
)

// 放弃之前尝试 diff 的次数
const maxRetries = 4

// 用于遮盖敏感值的常量
const (
	sensitiveMaskDefault = "***"
	sensitiveMaskBefore  = "*** (before)"
	sensitiveMaskAfter   = "*** (after)"
)

// 如果状态码小于 1，diffError 返回 ExitError，
// 否则返回 nil。
func diffError(err error) exec.ExitError {
	if err, ok := err.(exec.ExitError); ok && err.ExitStatus() <= 1 {
		return err
	}
	return nil
}

type DiffOptions struct {
	FilenameOptions resource.FilenameOptions

	ServerSideApply   bool
	FieldManager      string
	ForceConflicts    bool
	ShowManagedFields bool
	ShowSecrets       bool

	Concurrency      int
	Selector         string
	OpenAPIGetter    openapi.OpenAPIResourcesGetter
	OpenAPIV3Root    openapi3.Root
	DynamicClient    dynamic.Interface
	CmdNamespace     string
	EnforceNamespace bool
	Builder          *resource.Builder
	Diff             *DiffProgram

	pruner  *pruner
	tracker *tracker
}

func NewDiffOptions(ioStreams genericiooptions.IOStreams) *DiffOptions {
	return &DiffOptions{
		Diff: &DiffProgram{
			Exec:      exec.New(),
			IOStreams: ioStreams,
		},
	}
}

func NewCmdDiff(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	options := NewDiffOptions(streams)
	cmd := &cobra.Command{
		Use:                   "diff -f FILENAME",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Diff the live version against a would-be applied version"),
		Long:                  diffLong,
		Example:               diffExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckDiffErr(options.Complete(f, cmd, args))
			cmdutil.CheckDiffErr(options.Validate())
			// `kubectl diff` 会传播 diff 或 `KUBECTL_EXTERNAL_DIFF`
			// 返回的错误码。此外，我们不想在 diff 返回错误码 1 时
			// 打印错误，因为这仅仅表示发现了变更；
			// 同样，出现问题时也不希望 kubectl 返回 1。
			if err := options.Run(); err != nil {
				if exitErr := diffError(err); exitErr != nil {
					cmdutil.CheckErr(cmdutil.ErrExit)
				}
				cmdutil.CheckDiffErr(err)
			}
		},
	}

	// flag 错误以退出码 1 结束，但按照 diff 命令的约定，
	// 退出码 1 表示发现了变更。
	// 因此此处应返回大于 1 的状态码。
	cmd.SetFlagErrorFunc(func(command *cobra.Command, err error) error {
		cmdutil.CheckDiffErr(cmdutil.UsageErrorf(cmd, "%s", err.Error()))
		return nil
	})

	usage := "contains the configuration to diff"
	cmd.Flags().StringArray("prune-allowlist", []string{}, "Overwrite the default allowlist with <group/version/kind> for --prune")
	cmd.Flags().Bool("prune", false, "Include resources that would be deleted by pruning. Can be used with -l and default shows all resources would be pruned")
	cmd.Flags().BoolVar(&options.ShowManagedFields, "show-managed-fields", options.ShowManagedFields, "If true, include managed fields in the diff.")
	cmd.Flags().BoolVar(&options.ShowSecrets, "show-secrets", false, "If true, do not mask secret values in the diff.")
	cmd.Flags().IntVar(&options.Concurrency, "concurrency", 1, "Number of objects to process in parallel when diffing against the live version. Larger number = faster, but more memory, I/O and CPU over that shorter period of time.")
	cmdutil.AddFilenameOptionFlags(cmd, &options.FilenameOptions, usage)
	cmdutil.AddServerSideApplyFlags(cmd)
	cmdutil.AddFieldManagerFlagVar(cmd, &options.FieldManager, apply.FieldManagerClientSideApply)
	cmdutil.AddLabelSelectorFlagVar(cmd, &options.Selector)

	return cmd
}

// DiffProgram 查找并运行 diff 程序。
// KUBECTL_EXTERNAL_DIFF 环境变量的值将用作 diff 程序。
// 默认使用 `diff(1)`。
type DiffProgram struct {
	Exec exec.Interface
	genericiooptions.IOStreams
}

func (d *DiffProgram) getCommand(args ...string) (string, exec.Cmd) {
	diff := ""
	if envDiff := os.Getenv("KUBECTL_EXTERNAL_DIFF"); envDiff != "" {
		diffCommand := strings.Split(envDiff, " ")
		diff = diffCommand[0]

		if len(diffCommand) > 1 {
			// 正则表达式接受：字母数字（不区分大小写）、短横线和等号
			isValidChar := regexp.MustCompile(`^[a-zA-Z0-9-=]+$`).MatchString
			for i := 1; i < len(diffCommand); i++ {
				if isValidChar(diffCommand[i]) {
					args = append(args, diffCommand[i])
				}
			}
		}
	} else {
		diff = "diff"
		args = append([]string{"-u", "-N"}, args...)
	}

	cmd := d.Exec.Command(diff, args...)
	cmd.SetStdout(d.Out)
	cmd.SetStderr(d.ErrOut)

	return diff, cmd
}

// Run 运行检测到的 diff 程序。`from` 和 `to` 是要进行 diff 的目录。
func (d *DiffProgram) Run(from, to string) error {
	diff, cmd := d.getCommand(from, to)
	if err := cmd.Run(); err != nil {
		// 不要包装 diff 错误，否则之后将无法
		// 区分它们。
		if diffErr := diffError(err); diffErr != nil {
			return diffErr
		}
		return fmt.Errorf("failed to run %q: %v", diff, err)
	}
	return nil
}

// Printer 用于打印对象。
type Printer struct{}

// 将对象打印到 writer w 中。
func (p *Printer) Print(obj runtime.Object, w io.Writer) error {
	if obj == nil {
		return nil
	}
	data, err := yaml.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err

}

// DiffVersion 获取对象的相应版本，并将它们聚合到一个目录中。
type DiffVersion struct {
	Dir  *Directory
	Name string
}

// NewDiffVersion 使用指定版本创建一个新的 DiffVersion。
func NewDiffVersion(name string) (*DiffVersion, error) {
	dir, err := CreateDirectory(name)
	if err != nil {
		return nil, err
	}
	return &DiffVersion{
		Dir:  dir,
		Name: name,
	}, nil
}

func (v *DiffVersion) getObject(obj Object) (runtime.Object, error) {
	switch v.Name {
	case "LIVE":
		return obj.Live(), nil
	case "MERGED":
		return obj.Merged()
	}
	return nil, fmt.Errorf("Unknown version: %v", v.Name)
}

// Print 使用 printer 将对象打印到目录中的新文件里。
func (v *DiffVersion) Print(name string, obj runtime.Object, printer Printer) error {
	f, err := v.Dir.NewFile(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return printer.Print(obj, f)
}

// Directory 创建一个新的临时目录，并支持在其中轻松创建新文件。
type Directory struct {
	Name string
}

// CreateDirectory 创建实际的磁盘目录，
// 并返回它的新表示。
func CreateDirectory(prefix string) (*Directory, error) {
	name, err := os.MkdirTemp("", prefix+"-")
	if err != nil {
		return nil, err
	}

	return &Directory{
		Name: name,
	}, nil
}

// NewFile 在目录中创建一个新文件。
func (d *Directory) NewFile(name string) (*os.File, error) {
	return os.OpenFile(filepath.Join(d.Name, name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0700)
}

// Delete 递归删除该目录。
func (d *Directory) Delete() error {
	return os.RemoveAll(d.Name)
}

// Object 是一个接口，可用于获取对象的
// 多个版本。
type Object interface {
	Live() runtime.Object
	Merged() (runtime.Object, error)

	Name() string
}

// InfoObject 是 Object 接口的实现，从 Info 对象中获取
// 全部信息。
type InfoObject struct {
	LocalObj        runtime.Object
	Info            *resource.Info
	Encoder         runtime.Encoder
	OpenAPIGetter   openapi.OpenAPIResourcesGetter
	OpenAPIV3Root   openapi3.Root
	Force           bool
	ServerSideApply bool
	FieldManager    string
	ForceConflicts  bool
	genericiooptions.IOStreams
}

var _ Object = &InfoObject{}

// 返回对象的 live 版本
func (obj InfoObject) Live() runtime.Object {
	return obj.Info.Object
}

// 返回 "merged" 对象，即应用或创建后
// 呈现的样子。
func (obj InfoObject) Merged() (runtime.Object, error) {
	helper := resource.NewHelper(obj.Info.Client, obj.Info.Mapping).
		DryRun(true).
		WithFieldManager(obj.FieldManager)
	if obj.ServerSideApply {
		data, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj.LocalObj)
		if err != nil {
			return nil, err
		}
		options := metav1.PatchOptions{
			Force:        &obj.ForceConflicts,
			FieldManager: obj.FieldManager,
		}
		return helper.Patch(
			obj.Info.Namespace,
			obj.Info.Name,
			types.ApplyPatchType,
			data,
			&options,
		)
	}

	// 构建 patcher，然后以 dry-run 方式应用 patch；如果对象不存在，则需要创建它。
	if obj.Live() == nil {
		// 如果对象不存在，则以 dry-run 方式创建。
		return helper.CreateWithOptions(
			obj.Info.Namespace,
			true,
			obj.LocalObj,
			&metav1.CreateOptions{},
		)
	}

	var resourceVersion *string
	if !obj.Force {
		accessor, err := meta.Accessor(obj.Info.Object)
		if err != nil {
			return nil, err
		}
		str := accessor.GetResourceVersion()
		resourceVersion = &str
	}

	modified, err := util.GetModifiedConfiguration(obj.LocalObj, false, unstructured.UnstructuredJSONScheme)
	if err != nil {
		return nil, err
	}

	// 这里使用了 apply 中的 patcher，以保持行为一致。
	// 待 server-side apply 可用后，我们计划用它替换这段逻辑。
	patcher := &apply.Patcher{
		Mapping:         obj.Info.Mapping,
		Helper:          helper,
		Overwrite:       true,
		BackOff:         clockwork.NewRealClock(),
		OpenAPIGetter:   obj.OpenAPIGetter,
		OpenAPIV3Root:   obj.OpenAPIV3Root,
		ResourceVersion: resourceVersion,
	}

	_, result, err := patcher.Patch(obj.Info.Object, modified, obj.Info.Source, obj.Info.Namespace, obj.Info.Name, obj.ErrOut)
	return result, err
}

func (obj InfoObject) Name() string {
	group := ""
	if obj.Info.Mapping.GroupVersionKind.Group != "" {
		group = fmt.Sprintf("%v.", obj.Info.Mapping.GroupVersionKind.Group)
	}
	return group + fmt.Sprintf(
		"%v.%v.%v.%v",
		obj.Info.Mapping.GroupVersionKind.Version,
		obj.Info.Mapping.GroupVersionKind.Kind,
		obj.Info.Namespace,
		obj.Info.Name,
	)
}

// toUnstructured 将 runtime.Object 转换为 unstructured.Unstructured 对象。
func toUnstructured(obj runtime.Object) (*unstructured.Unstructured, error) {
	if obj == nil {
		return nil, nil
	}
	c, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj.DeepCopyObject())
	if err != nil {
		return nil, fmt.Errorf("convert to unstructured: %w", err)
	}
	u := &unstructured.Unstructured{}
	u.SetUnstructuredContent(c)
	return u, nil
}

// Masker 遮盖对象中的敏感值，同时保留可进行 diff 的
// 变更。
//
// 对象中的所有敏感值都会被替换为固定长度的
// 星号掩码。如果两个值不同，会额外添加后缀，
// 以便能够进行 diff。
type Masker struct {
	from *unstructured.Unstructured
	to   *unstructured.Unstructured
}

func NewMasker(from, to runtime.Object) (*Masker, error) {
	// 将对象转换为 unstructured
	f, err := toUnstructured(from)
	if err != nil {
		return nil, fmt.Errorf("convert to unstructured: %w", err)
	}
	t, err := toUnstructured(to)
	if err != nil {
		return nil, fmt.Errorf("convert to unstructured: %w", err)
	}

	// 运行 masker
	m := &Masker{
		from: f,
		to:   t,
	}
	if err := m.run(); err != nil {
		return nil, fmt.Errorf("run masker: %w", err)
	}
	return m, nil
}

// dataFromUnstructured 返回 data 键下的底层嵌套 map。
func (m Masker) dataFromUnstructured(u *unstructured.Unstructured) (map[string]interface{}, error) {
	if u == nil {
		return nil, nil
	}
	data, found, err := unstructured.NestedMap(u.UnstructuredContent(), "data")
	if err != nil {
		return nil, fmt.Errorf("get nested map: %w", err)
	}
	if !found {
		return nil, nil
	}
	return data, nil
}

// run 比较并修补敏感值。
func (m *Masker) run() error {
	// 提取嵌套的 map 对象
	from, err := m.dataFromUnstructured(m.from)
	if err != nil {
		return fmt.Errorf("extract 'data' field: %w", err)
	}
	to, err := m.dataFromUnstructured(m.to)
	if err != nil {
		return fmt.Errorf("extract 'data' field: %w", err)
	}

	for k := range from {
		// 当两个对象中都存在该键且值不相等时，
		// 添加 before/after 后缀，使其在 diff 中
		// 可见。
		if _, ok := to[k]; ok {
			if from[k] != to[k] {
				from[k] = sensitiveMaskBefore
				to[k] = sensitiveMaskAfter
				continue
			}
			to[k] = sensitiveMaskDefault
		}
		from[k] = sensitiveMaskDefault
	}
	for k := range to {
		// 遮盖 'from' 中不存在的其余键
		if _, ok := from[k]; !ok {
			to[k] = sensitiveMaskDefault
		}
	}

	// 用遮盖后的数据修补对象
	if m.from != nil && from != nil {
		if err := unstructured.SetNestedMap(m.from.UnstructuredContent(), from, "data"); err != nil {
			return fmt.Errorf("patch masked data: %w", err)
		}
	}
	if m.to != nil && to != nil {
		if err := unstructured.SetNestedMap(m.to.UnstructuredContent(), to, "data"); err != nil {
			return fmt.Errorf("patch masked data: %w", err)
		}
	}
	return nil
}

// From 返回 'from' 对象遮盖后的版本。
func (m *Masker) From() runtime.Object {
	return m.from
}

// To 返回 'to' 对象遮盖后的版本。
func (m *Masker) To() runtime.Object {
	return m.to
}

// Differ 创建两个 DiffVersion 并对它们进行 diff。
type Differ struct {
	From *DiffVersion
	To   *DiffVersion
}

func NewDiffer(from, to string) (*Differ, error) {
	differ := Differ{}
	var err error
	differ.From, err = NewDiffVersion(from)
	if err != nil {
		return nil, err
	}
	differ.To, err = NewDiffVersion(to)
	if err != nil {
		differ.From.Dir.Delete()
		return nil, err
	}

	return &differ, nil
}

// Diff 对特定对象的两个版本进行 diff，并将两个版本打印到目录中。
func (d *Differ) Diff(obj Object, printer Printer, showManagedFields, showSecrets bool) error {
	from, err := d.From.getObject(obj)
	if err != nil {
		return err
	}
	to, err := d.To.getObject(obj)
	if err != nil {
		return err
	}

	if !showManagedFields {
		from = omitManagedFields(from)
		to = omitManagedFields(to)
	}

	// 如果对象是 V1Secret，则遮盖其敏感值
	if gvk := to.GetObjectKind().GroupVersionKind(); !showSecrets && gvk.Version == "v1" && gvk.Kind == "Secret" {
		m, err := NewMasker(from, to)
		if err != nil {
			return err
		}
		from, to = m.From(), m.To()
	}

	if err := d.From.Print(obj.Name(), from, printer); err != nil {
		return err
	}
	if err := d.To.Print(obj.Name(), to, printer); err != nil {
		return err
	}
	return nil
}

func omitManagedFields(o runtime.Object) runtime.Object {
	a, err := meta.Accessor(o)
	if err != nil {
		// 该对象不是 `metav1.Object`，忽略它。
		return o
	}
	a.SetManagedFields(nil)
	return o
}

// Run 针对两个目录运行 diff 程序。
func (d *Differ) Run(diff *DiffProgram) error {
	return diff.Run(d.From.Dir.Name, d.To.Dir.Name)
}

// TearDown 递归删除两个临时目录。
func (d *Differ) TearDown() {
	d.From.Dir.Delete() // 忽略错误
	d.To.Dir.Delete()   // 忽略错误
}

func isConflict(err error) bool {
	return err != nil && errors.IsConflict(err)
}

func (o *DiffOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return cmdutil.UsageErrorf(cmd, "Unexpected args: %v", args)
	}

	var err error

	err = o.FilenameOptions.RequireFilenameOrKustomize()
	if err != nil {
		return err
	}

	o.ServerSideApply = cmdutil.GetServerSideApplyFlag(cmd)
	o.FieldManager = apply.GetApplyFieldManagerFlag(cmd, o.ServerSideApply)
	o.ForceConflicts = cmdutil.GetForceConflictsFlag(cmd)
	if o.ForceConflicts && !o.ServerSideApply {
		return fmt.Errorf("--force-conflicts only works with --server-side")
	}

	if !o.ServerSideApply {
		o.OpenAPIGetter = f
		openAPIV3Client, err := f.OpenAPIV3Client()
		if err == nil {
			o.OpenAPIV3Root = openapi3.NewRoot(openAPIV3Client)
		} else {
			klog.V(4).Infof("warning: OpenAPI V3 Patch is enabled but is unable to be loaded. Will fall back to OpenAPI V2")
		}
	}

	o.DynamicClient, err = f.DynamicClient()
	if err != nil {
		return err
	}

	o.CmdNamespace, o.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	if cmdutil.GetFlagBool(cmd, "prune") {
		mapper, err := f.ToRESTMapper()
		if err != nil {
			return err
		}

		resources, err := prune.ParseResources(mapper, cmdutil.GetFlagStringArray(cmd, "prune-allowlist"))
		if err != nil {
			return err
		}
		o.tracker = newTracker()
		o.pruner = newPruner(o.DynamicClient, mapper, resources, o.Selector)
	}

	o.Builder = f.NewBuilder()
	return nil
}

// Run 使用 factory 解析文件参数，确定要 diff 的版本，
// 为每个文件找到对应的 Info 对象，然后交给 differ
// 进行处理。
func (o *DiffOptions) Run() error {
	differ, err := NewDiffer("LIVE", "MERGED")
	if err != nil {
		return err
	}
	defer differ.TearDown()

	printer := Printer{}

	r := o.Builder.
		Unstructured().
		VisitorConcurrency(o.Concurrency).
		NamespaceParam(o.CmdNamespace).DefaultNamespace().
		FilenameParam(o.EnforceNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.Selector).
		Flatten().
		Do()
	if err := r.Err(); err != nil {
		return err
	}

	err = r.Visit(func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}

		local := info.Object.DeepCopyObject()
		for i := 1; i <= maxRetries; i++ {
			if err = info.Get(); err != nil {
				if !errors.IsNotFound(err) {
					return err
				}
				info.Object = nil
			}

			force := i == maxRetries
			if force {
				klog.Warningf(
					"Object (%v: %v) keeps changing, diffing without lock",
					info.Object.GetObjectKind().GroupVersionKind(),
					info.Name,
				)
			}
			obj := InfoObject{
				LocalObj:        local,
				Info:            info,
				Encoder:         scheme.DefaultJSONEncoder(),
				OpenAPIGetter:   o.OpenAPIGetter,
				OpenAPIV3Root:   o.OpenAPIV3Root,
				Force:           force,
				ServerSideApply: o.ServerSideApply,
				FieldManager:    o.FieldManager,
				ForceConflicts:  o.ForceConflicts,
				IOStreams:       o.Diff.IOStreams,
			}

			if o.tracker != nil {
				o.tracker.MarkVisited(info)
			}

			err = differ.Diff(obj, printer, o.ShowManagedFields, o.ShowSecrets)
			if !isConflict(err) {
				break
			}
		}

		apply.WarnIfDeleting(info.Object, o.Diff.ErrOut)

		return err
	})

	if o.pruner != nil {
		prunedObjs, err := o.pruner.pruneAll(o.tracker, o.CmdNamespace != "")
		if err != nil {
			klog.Warningf("pruning failed and could not be evaluated err: %v", err)
		}

		// 将被清理的对象打印到旧文件中，这样 diff
		// 命令会将它们显示为已清理。
		for _, p := range prunedObjs {
			name, err := getObjectName(p)
			if err != nil {
				klog.Warningf("pruning failed and object name could not be retrieved: %v", err)
				continue
			}
			if err := differ.From.Print(name, p, printer); err != nil {
				return err
			}
		}
	}

	if err != nil {
		return err
	}

	return differ.Run(o.Diff)
}

// Validate 确保为 DiffOptions 提供的值有效
func (o *DiffOptions) Validate() error {
	return nil
}

func getObjectName(obj runtime.Object) (string, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	name := metadata.GetName()
	ns := metadata.GetNamespace()

	group := ""
	if gvk.Group != "" {
		group = fmt.Sprintf("%v.", gvk.Group)
	}
	return group + fmt.Sprintf(
		"%v.%v.%v.%v",
		gvk.Version,
		gvk.Kind,
		ns,
		name,
	), nil
}
