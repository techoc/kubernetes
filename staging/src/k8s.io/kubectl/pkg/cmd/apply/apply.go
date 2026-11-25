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

package apply

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/openapi3"
	"k8s.io/client-go/util/csaupgrade"
	"k8s.io/component-base/version"
	"k8s.io/klog/v2"
	cmddelete "k8s.io/kubectl/pkg/cmd/delete"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/openapi"
	"k8s.io/kubectl/pkg/util/prune"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/kubectl/pkg/validation"
)

// ApplyFlags 直接反映 CLI 通过 flag 收集的信息。它们会被转换为 Options，
// 后者反映命令的运行时需求。这种结构将转换简化为单纯的接线，并使
// 逻辑本身易于单元测试。
type ApplyFlags struct {
	RecordFlags *genericclioptions.RecordFlags
	PrintFlags  *genericclioptions.PrintFlags

	DeleteFlags *cmddelete.DeleteFlags

	FieldManager   string
	Selector       string
	Prune          bool
	PruneResources []prune.Resource
	ApplySetRef    string
	All            bool
	Overwrite      bool
	OpenAPIPatch   bool
	Subresource    string

	PruneAllowlist []string

	genericiooptions.IOStreams
}

// ApplyOptions 定义 `apply` 命令的 flag 及其他配置参数
type ApplyOptions struct {
	Recorder genericclioptions.Recorder

	PrintFlags *genericclioptions.PrintFlags
	ToPrinter  func(string) (printers.ResourcePrinter, error)

	DeleteOptions *cmddelete.DeleteOptions

	ServerSideApply bool
	ForceConflicts  bool
	FieldManager    string
	Selector        string
	DryRunStrategy  cmdutil.DryRunStrategy
	Prune           bool
	PruneResources  []prune.Resource
	cmdBaseName     string
	All             bool
	Overwrite       bool
	OpenAPIPatch    bool
	Subresource     string

	ValidationDirective string
	Validator           validation.Schema
	Builder             *resource.Builder
	Mapper              meta.RESTMapper
	DynamicClient       dynamic.Interface
	OpenAPIGetter       openapi.OpenAPIResourcesGetter
	OpenAPIV3Root       openapi3.Root

	Namespace        string
	EnforceNamespace bool

	genericiooptions.IOStreams

	// 待应用的
	// 对象（及部分反规范化数据）。填充此结构的标准方式
	// 是调用 "GetObjects()"，当 "objectsCached" 为 false 时，
	// 它会使用 resource builder。另一种
	// 设置该字段的方式是使用 "SetObjects()"。
	// 设置之后再次调用 "GetObjects()" 不会
	// 调用 resource builder，只会返回已设置的对象。
	objects       []*resource.Info
	objectsCached bool

	// 保存已访问的对象/namespace，供之后
	// 计算待清理对象集合时使用。
	VisitedUids       sets.Set[types.UID]
	VisitedNamespaces sets.Set[string]

	// 在对象生成并
	// 存入 "objects" 字段之后、
	// 对这些对象执行 apply 之前运行的函数。
	PreProcessorFn func() error
	// 在所有对象都被应用后运行的函数。
	// 标准的 PostProcessorFn 是 "PrintAndPrunePostProcessor()"。
	PostProcessorFn func() error

	// ApplySet 记录已应用的对象集合，用于清理。
	// 参见 git.k8s.io/enhancements/keps/sig-cli/3659-kubectl-apply-prune
	ApplySet *ApplySet
}

var (
	applyLong = templates.LongDesc(i18n.T(`
		Apply a configuration to a resource by file name or stdin.
		The resource name must be specified. This resource will be created if it doesn't exist yet.
		To use 'apply', always create the resource initially with either 'apply' or 'create --save-config'.

		JSON and YAML formats are accepted.

		Alpha Disclaimer: the --prune functionality is not yet complete. Do not use unless you are aware of what the current state is. See https://issues.k8s.io/34274.`))

	applyExample = templates.Examples(i18n.T(`
		# Apply the configuration in pod.json to a pod
		kubectl apply -f ./pod.json

		# Apply resources from a directory containing kustomization.yaml - e.g. dir/kustomization.yaml
		kubectl apply -k dir/

		# Apply the JSON passed into stdin to a pod
		cat pod.json | kubectl apply -f -

		# Apply the configuration from all files that end with '.json'
		kubectl apply -f '*.json'

		# Note: --prune is still in Alpha
		# Apply the configuration in manifest.yaml that matches label app=nginx and delete all other resources that are not in the file and match label app=nginx
		kubectl apply --prune -f manifest.yaml -l app=nginx

		# Apply the configuration in manifest.yaml and delete all the other config maps that are not in the file
		kubectl apply --prune -f manifest.yaml --all --prune-allowlist=core/v1/ConfigMap`))

	warningNoLastAppliedConfigAnnotation = "Warning: resource %[1]s is missing the %[2]s annotation which is required by %[3]s apply. %[3]s apply should only be used on resources created declaratively by either %[3]s create --save-config or %[3]s apply. The missing annotation will be patched automatically.\n"
	warningChangesOnDeletingResource     = "Warning: Detected changes to resource %[1]s which is currently being deleted.\n"
	warningMigrationLastAppliedFailed    = "Warning: failed to migrate kubectl.kubernetes.io/last-applied-configuration for Server-Side Apply. This is non-fatal and will be retried next time you apply. Error: %[1]s\n"
	warningMigrationPatchFailed          = "Warning: server rejected managed fields migration to Server-Side Apply. This is non-fatal and will be retried next time you apply. Error: %[1]s\n"
	warningMigrationReapplyFailed        = "Warning: failed to re-apply configuration after performing Server-Side Apply migration. This is non-fatal and will be retried next time you apply. Error: %[1]s\n"
)

var ApplySetToolVersion = version.Get().GitVersion

// NewApplyFlags 返回默认的 ApplyFlags
func NewApplyFlags(streams genericiooptions.IOStreams) *ApplyFlags {
	return &ApplyFlags{
		RecordFlags: genericclioptions.NewRecordFlags(),
		DeleteFlags: cmddelete.NewDeleteFlags("The files, directories or URLs that contain the configurations to apply."),
		PrintFlags:  genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),

		Overwrite:    true,
		OpenAPIPatch: true,

		IOStreams: streams,
	}
}

// NewCmdApply 创建 `apply` 命令
func NewCmdApply(baseName string, f cmdutil.Factory, ioStreams genericiooptions.IOStreams) *cobra.Command {
	flags := NewApplyFlags(ioStreams)

	cmd := &cobra.Command{
		Use:                   "apply (-f FILENAME | -k DIRECTORY)",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Apply a configuration to a resource by file name or stdin"),
		Long:                  applyLong,
		Example:               applyExample,
		Run: func(cmd *cobra.Command, args []string) {
			o, err := flags.ToOptions(f, cmd, baseName, args)
			cmdutil.CheckErr(err)
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run())
		},
	}

	flags.AddFlags(cmd)

	// apply 子命令
	cmd.AddCommand(NewCmdApplyViewLastApplied(f, flags.IOStreams))
	cmd.AddCommand(NewCmdApplySetLastApplied(f, flags.IOStreams))
	cmd.AddCommand(NewCmdApplyEditLastApplied(f, flags.IOStreams))

	return cmd
}

// AddFlags 为 CLI 注册 flag
func (flags *ApplyFlags) AddFlags(cmd *cobra.Command) {
	// 绑定 flag 结构体
	flags.DeleteFlags.AddFlags(cmd)
	flags.RecordFlags.AddFlags(cmd)
	flags.PrintFlags.AddFlags(cmd)

	cmdutil.AddValidateFlags(cmd)
	cmdutil.AddDryRunFlag(cmd)
	cmdutil.AddServerSideApplyFlags(cmd)
	cmdutil.AddFieldManagerFlagVar(cmd, &flags.FieldManager, FieldManagerClientSideApply)
	cmdutil.AddLabelSelectorFlagVar(cmd, &flags.Selector)
	cmdutil.AddPruningFlags(cmd, &flags.Prune, &flags.PruneAllowlist, &flags.All, &flags.ApplySetRef)
	cmd.Flags().BoolVar(&flags.Overwrite, "overwrite", flags.Overwrite, "Automatically resolve conflicts between the modified and live configuration by using values from the modified configuration")
	cmd.Flags().BoolVar(&flags.OpenAPIPatch, "openapi-patch", flags.OpenAPIPatch, "If true, use openapi to calculate diff when the openapi presents and the resource can be found in the openapi spec. Otherwise, fall back to use baked-in types.")
	cmdutil.AddSubresourceFlags(cmd, &flags.Subresource, "If specified, apply will operate on the subresource of the requested object.  Only allowed when using --server-side.")
}

// ToOptions 将 CLI 输入转换为运行时输入
func (flags *ApplyFlags) ToOptions(f cmdutil.Factory, cmd *cobra.Command, baseName string, args []string) (*ApplyOptions, error) {
	if len(args) != 0 {
		return nil, cmdutil.UsageErrorf(cmd, "Unexpected args: %v", args)
	}

	serverSideApply := cmdutil.GetServerSideApplyFlag(cmd)
	forceConflicts := cmdutil.GetForceConflictsFlag(cmd)
	dryRunStrategy, err := cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := f.DynamicClient()
	if err != nil {
		return nil, err
	}

	fieldManager := GetApplyFieldManagerFlag(cmd, serverSideApply)

	// 允许在打印时指定成功消息的操作
	toPrinter := func(operation string) (printers.ResourcePrinter, error) {
		flags.PrintFlags.NamePrintFlags.Operation = operation
		cmdutil.PrintFlagsWithDryRunStrategy(flags.PrintFlags, dryRunStrategy)
		return flags.PrintFlags.ToPrinter()
	}

	flags.RecordFlags.Complete(cmd)
	recorder, err := flags.RecordFlags.ToRecorder()
	if err != nil {
		return nil, err
	}

	deleteOptions, err := flags.DeleteFlags.ToOptions(dynamicClient, flags.IOStreams)
	if err != nil {
		return nil, err
	}

	err = deleteOptions.FilenameOptions.RequireFilenameOrKustomize()
	if err != nil {
		return nil, err
	}

	var openAPIV3Root openapi3.Root
	openAPIV3Client, err := f.OpenAPIV3Client()
	if err == nil {
		openAPIV3Root = openapi3.NewRoot(openAPIV3Client)
	} else {
		klog.V(4).Infof("warning: OpenAPI V3 Patch is enabled but is unable to be loaded. Will fall back to OpenAPI V2")
	}

	validationDirective, err := cmdutil.GetValidationDirective(cmd)
	if err != nil {
		return nil, err
	}
	validator, err := f.Validator(validationDirective)
	if err != nil {
		return nil, err
	}
	builder := f.NewBuilder()
	mapper, err := f.ToRESTMapper()
	if err != nil {
		return nil, err
	}

	namespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return nil, err
	}

	var applySet *ApplySet
	if flags.ApplySetRef != "" {
		parent, err := ParseApplySetParentRef(flags.ApplySetRef, mapper)
		if err != nil {
			return nil, fmt.Errorf("invalid parent reference %q: %w", flags.ApplySetRef, err)
		}
		// ApplySet 使用 flag 中的 namespace 值，而不使用 kubeconfig 或默认值
		// 这意味着使用带 namespace 的 parent 时必须提供 namespace flag。
		if enforceNamespace && parent.IsNamespaced() {
			parent.Namespace = namespace
		}
		tooling := ApplySetTooling{Name: baseName, Version: ApplySetToolVersion}
		restClient, err := f.UnstructuredClientForMapping(parent.RESTMapping)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize RESTClient for ApplySet: %w", err)
		}
		if restClient == nil {
			return nil, fmt.Errorf("could not build RESTClient for ApplySet")
		}
		applySet = NewApplySet(parent, tooling, mapper, restClient)
	}
	if flags.Prune {
		flags.PruneResources, err = prune.ParseResources(mapper, flags.PruneAllowlist)
		if err != nil {
			return nil, err
		}
	}

	o := &ApplyOptions{
		// 	保存 baseName，用于打印涉及基础命令名称的警告/消息。
		// 	这对包装此命令的下游命令很有用。
		cmdBaseName: baseName,

		PrintFlags: flags.PrintFlags,

		DeleteOptions:   deleteOptions,
		ToPrinter:       toPrinter,
		ServerSideApply: serverSideApply,
		ForceConflicts:  forceConflicts,
		FieldManager:    fieldManager,
		Selector:        flags.Selector,
		DryRunStrategy:  dryRunStrategy,
		Prune:           flags.Prune,
		PruneResources:  flags.PruneResources,
		All:             flags.All,
		Overwrite:       flags.Overwrite,
		OpenAPIPatch:    flags.OpenAPIPatch,
		Subresource:     flags.Subresource,

		Recorder:            recorder,
		Namespace:           namespace,
		EnforceNamespace:    enforceNamespace,
		Validator:           validator,
		ValidationDirective: validationDirective,
		Builder:             builder,
		Mapper:              mapper,
		DynamicClient:       dynamicClient,
		OpenAPIGetter:       f,
		OpenAPIV3Root:       openAPIV3Root,

		IOStreams: flags.IOStreams,

		objects:       []*resource.Info{},
		objectsCached: false,

		VisitedUids:       sets.New[types.UID](),
		VisitedNamespaces: sets.New[string](),

		ApplySet: applySet,
	}

	o.PostProcessorFn = o.PrintAndPrunePostProcessor()

	return o, nil
}

// Validate 验证 ApplyOptions 是否有效且无冲突。
func (o *ApplyOptions) Validate() error {
	if o.ForceConflicts && !o.ServerSideApply {
		return fmt.Errorf("--force-conflicts only works with --server-side")
	}

	if o.DryRunStrategy == cmdutil.DryRunClient && o.ServerSideApply {
		return fmt.Errorf("--dry-run=client doesn't work with --server-side (did you mean --dry-run=server instead?)")
	}

	if o.ServerSideApply && o.DeleteOptions.ForceDeletion {
		return fmt.Errorf("--force cannot be used with --server-side")
	}

	if o.DryRunStrategy == cmdutil.DryRunServer && o.DeleteOptions.ForceDeletion {
		return fmt.Errorf("--dry-run=server cannot be used with --force")
	}

	if o.All && len(o.Selector) > 0 {
		return fmt.Errorf("cannot set --all and --selector at the same time")
	}

	if o.ApplySet != nil {
		if !o.Prune {
			return fmt.Errorf("--applyset requires --prune")
		}
		if err := o.ApplySet.Validate(context.TODO(), o.DynamicClient); err != nil {
			return err
		}
	}
	if o.Prune {
		// 如果正在清理，则不要强制重建对象；因为对象 UID 会发生变化，
		// 这可能导致未定义行为。
		if o.DeleteOptions.ForceDeletion {
			return fmt.Errorf("--force cannot be used with --prune")
		}

		if o.ApplySet != nil {
			if o.All {
				return fmt.Errorf("--all is incompatible with --applyset")
			} else if o.Selector != "" {
				return fmt.Errorf("--selector is incompatible with --applyset")
			} else if len(o.PruneResources) > 0 {
				return fmt.Errorf("--prune-allowlist is incompatible with --applyset")
			}
		} else {
			if !o.All && o.Selector == "" {
				return fmt.Errorf("all resources selected for prune without explicitly passing --all. To prune all resources, pass the --all flag. If you did not mean to prune all resources, specify a label selector")
			}
			if o.ServerSideApply {
				return fmt.Errorf("--prune is in alpha and doesn't currently work on objects created by server-side apply")
			}
		}
	}
	if len(o.Subresource) > 0 && !o.ServerSideApply {
		return fmt.Errorf("--subresource can only be specified for --server-side")
	}

	return nil
}

func isIncompatibleServerError(err error) bool {
	// 415: Unsupported media type 表示所连接的服务器
	// 不支持 server-side apply。
	if _, ok := err.(*errors.StatusError); !ok {
		// 非 StatusError 表示该错误并非由服务器不兼容引起。
		return false
	}
	return err.(*errors.StatusError).Status().Code == http.StatusUnsupportedMediaType
}

// GetObjects 返回所有待应用有效对象的（可能缓存的）版本，
// 形式为指向 resource.Info 的指针切片，以及发生的错误（如有）。
// 重要：此函数可能同时返回有效对象和错误，因为
// builder 上设置了 "ContinueOnError"。在调用 "complete" 和 "validate"
// 方法之前不应调用此函数，以确保
// ApplyOptions 已被填充且有效。
func (o *ApplyOptions) GetObjects() ([]*resource.Info, error) {
	var err error = nil
	if !o.objectsCached {
		r := o.Builder.
			Unstructured().
			Schema(o.Validator).
			ContinueOnError().
			NamespaceParam(o.Namespace).DefaultNamespace().
			FilenameParam(o.EnforceNamespace, &o.DeleteOptions.FilenameOptions).
			LabelSelectorParam(o.Selector).
			Flatten().
			Do()

		o.objects, err = r.Infos()

		if o.ApplySet != nil {
			if err := o.ApplySet.AddLabels(o.objects...); err != nil {
				return nil, err
			}
		}

		o.objectsCached = true
	}
	return o.objects, err
}

// SetObjects 存储待应用的对象集合（以 resource.Info 表示），
// 供后续应用。
func (o *ApplyOptions) SetObjects(infos []*resource.Info) {
	o.objects = infos
	o.objectsCached = true
}

// Run 执行 `apply` 命令。
func (o *ApplyOptions) Run() error {
	if o.PreProcessorFn != nil {
		klog.V(4).Infof("Running apply pre-processor function")
		if err := o.PreProcessorFn(); err != nil {
			return err
		}
	}

	// 在服务请求上强制使用 CLI 指定的 namespace。
	if o.EnforceNamespace {
		o.VisitedNamespaces.Insert(o.Namespace)
	}

	// 如果预处理程序中没有通过 "SetObjects()"
	// 存储对象，则使用 resource builder 生成对象。
	errs := []error{}
	infos, err := o.GetObjects()
	if err != nil {
		errs = append(errs, err)
	}
	if len(infos) == 0 && len(errs) == 0 {
		return fmt.Errorf("no objects passed to apply")
	}

	if o.ApplySet != nil {
		if err := o.ApplySet.BeforeApply(infos, o.DryRunStrategy, o.ValidationDirective); err != nil {
			return err
		}
	}

	// 遍历所有对象，逐个应用。
	for _, info := range infos {
		if err := o.applyOneObject(info); err != nil {
			errs = append(errs, err)
		}
	}
	// 如果 apply 过程中出现任何错误，则返回错误
	// （或错误的聚合）。
	if len(errs) == 1 {
		return errs[0]
	}
	if len(errs) > 1 {
		return utilerrors.NewAggregate(errs)
	}

	if o.PostProcessorFn != nil {
		klog.V(4).Infof("Running apply post-processor function")
		if err := o.PostProcessorFn(); err != nil {
			return err
		}
	}

	return nil
}

func (o *ApplyOptions) applyOneObject(info *resource.Info) error {
	o.MarkNamespaceVisited(info)

	if err := o.Recorder.Record(info.Object); err != nil {
		klog.V(4).Infof("error recording current command: %v", err)
	}

	if len(info.Name) == 0 {
		metadata, _ := meta.Accessor(info.Object)
		generatedName := metadata.GetGenerateName()
		if len(generatedName) > 0 {
			return fmt.Errorf("from %s: cannot use generate name with apply", generatedName)
		}
	}

	helper := resource.NewHelper(info.Client, info.Mapping).
		DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
		WithFieldManager(o.FieldManager).
		WithFieldValidation(o.ValidationDirective)

	if o.ServerSideApply {
		// 将完整对象发送到服务端进行应用。
		data, err := runtime.Encode(unstructured.UnstructuredJSONScheme, info.Object)
		if err != nil {
			return cmdutil.AddSourceToErr("serverside-apply", info.Source, err)
		}

		options := metav1.PatchOptions{
			Force: &o.ForceConflicts,
		}
		obj, err := helper.
			WithSubresource(o.Subresource).
			Patch(
				info.Namespace,
				info.Name,
				types.ApplyPatchType,
				data,
				&options,
			)
		if err != nil {
			if isIncompatibleServerError(err) {
				err = fmt.Errorf("Server-side apply not available on the server: (%v)", err)
			}
			if errors.IsConflict(err) {
				err = fmt.Errorf(`%v
Please review the fields above--they currently have other managers. Here
are the ways you can resolve this warning:
* If you intend to manage all of these fields, please re-run the apply
  command with the `+"`--force-conflicts`"+` flag.
* If you do not intend to manage all of the fields, please edit your
  manifest to remove references to the fields that should keep their
  current managers.
* You may co-own fields by updating your manifest to match the existing
  value; in this case, you'll become the manager if the other manager(s)
  stop managing the field (remove it from their configuration).
See https://kubernetes.io/docs/reference/using-api/server-side-apply/#conflicts`, err)
			}
			return err
		}

		info.Refresh(obj, true)

		// 如有必要，迁移 managed fields。
		//
		// 事后检查而非事先获取对象，可以避免无条件获取，
		// 从而在极少发生迁移的情况下发起 3 次网络请求，
		// 在无需迁移时只需 1 次请求。
		//
		// 事先检查意味着大多数操作需要 2 次请求，最坏情况下
		// 需要 3 次请求。
		if err = o.saveLastApplyAnnotationIfNecessary(helper, info); err != nil {
			fmt.Fprintf(o.ErrOut, warningMigrationLastAppliedFailed, err.Error())
		} else if performedMigration, err := o.migrateToSSAIfNecessary(helper, info); err != nil {
			// 将打印错误作为警告处理。
			// 这是非致命错误，因为对象已在上面成功应用，
			// 但迁移失败可能会导致问题。
			//
			// 如有必要，下次 apply 时会重试此迁移。
			//
			fmt.Fprintf(o.ErrOut, warningMigrationPatchFailed, err.Error())
		} else if performedMigration {
			if obj, err = helper.Patch(
				info.Namespace,
				info.Name,
				types.ApplyPatchType,
				data,
				&options,
			); err != nil {
				// 重新发送原始的 SSA patch（这样可以让被丢弃的字段
				// 最终被移除）
				fmt.Fprintf(o.ErrOut, warningMigrationReapplyFailed, err.Error())
			} else {
				info.Refresh(obj, false)
			}
		}

		WarnIfDeleting(info.Object, o.ErrOut)

		if err := o.MarkObjectVisited(info); err != nil {
			return err
		}

		if o.shouldPrintObject() {
			return nil
		}

		printer, err := o.ToPrinter("serverside-applied")
		if err != nil {
			return err
		}

		if err = printer.PrintObj(info.Object, o.Out); err != nil {
			return err
		}
		return nil
	}

	// 获取对象的修改后配置。将结果作为注解
	// 嵌入修改后配置，使其出现在
	// 发送给服务端的 patch 中。
	modified, err := util.GetModifiedConfiguration(info.Object, true, unstructured.UnstructuredJSONScheme)
	if err != nil {
		return cmdutil.AddSourceToErr(fmt.Sprintf("retrieving modified configuration from:\n%s\nfor:", info.String()), info.Source, err)
	}

	if err := info.Get(); err != nil {
		if !errors.IsNotFound(err) {
			return cmdutil.AddSourceToErr(fmt.Sprintf("retrieving current configuration of:\n%s\nfrom server for:", info.String()), info.Source, err)
		}

		// 如果资源不存在，则创建它
		// 首先更新 kubectl apply 使用的注解
		if err := util.CreateApplyAnnotation(info.Object, unstructured.UnstructuredJSONScheme); err != nil {
			return cmdutil.AddSourceToErr("creating", info.Source, err)
		}

		// client-side apply 执行创建时清除 null 值，以与 client-side apply 执行更新时的行为保持一致。
		// 在 CreateApplyAnnotation 之后执行此操作，使注解与同一 manifest 更新 apply 时持久化的内容一致。
		if u, ok := info.Object.(runtime.Unstructured); ok {
			pruneNullsFromMap(u.UnstructuredContent())
		}

		if o.DryRunStrategy != cmdutil.DryRunClient {
			// 然后创建资源，跳过三方合并
			obj, err := helper.Create(info.Namespace, true, info.Object)
			if err != nil {
				return cmdutil.AddSourceToErr("creating", info.Source, err)
			}
			info.Refresh(obj, true)
		}

		if err := o.MarkObjectVisited(info); err != nil {
			return err
		}

		if o.shouldPrintObject() {
			return nil
		}

		printer, err := o.ToPrinter("created")
		if err != nil {
			return err
		}
		if err = printer.PrintObj(info.Object, o.Out); err != nil {
			return err
		}
		return nil
	}

	if err := o.MarkObjectVisited(info); err != nil {
		return err
	}

	metadata, _ := meta.Accessor(info.Object)
	annotationMap := metadata.GetAnnotations()
	if _, ok := annotationMap[corev1.LastAppliedConfigAnnotation]; !ok {
		fmt.Fprintf(o.ErrOut, warningNoLastAppliedConfigAnnotation, info.ObjectName(), corev1.LastAppliedConfigAnnotation, o.cmdBaseName) //nolint:errcheck
	}

	patcher, err := newPatcher(o, info, helper)
	if err != nil {
		return err
	}

	var patchBytes []byte
	var patchedObject runtime.Object

	if o.DryRunStrategy != cmdutil.DryRunClient {
		patchBytes, patchedObject, err = patcher.Patch(info.Object, modified, info.Source, info.Namespace, info.Name, o.ErrOut)
	} else {
		patchBytes, patchedObject, err = patcher.PatchLocal(info.Object, modified, o.ErrOut)
	}

	if err != nil {
		return cmdutil.AddSourceToErr(fmt.Sprintf("applying patch:\n%s\nto:\n%v\nfor:", patchBytes, info), info.Source, err)
	}

	info.Refresh(patchedObject, true) //nolint:errcheck

	WarnIfDeleting(info.Object, o.ErrOut)

	if string(patchBytes) == "{}" && !o.shouldPrintObject() {
		printer, err := o.ToPrinter("unchanged")
		if err != nil {
			return err
		}
		if err = printer.PrintObj(info.Object, o.Out); err != nil {
			return err
		}
		return nil
	}

	if o.shouldPrintObject() {
		return nil
	}

	printer, err := o.ToPrinter("configured")
	if err != nil {
		return err
	}
	if err = printer.PrintObj(info.Object, o.Out); err != nil {
		return err
	}

	return nil
}

func pruneNullsFromMap(data map[string]interface{}) {
	for k, v := range data {
		if v == nil {
			delete(data, k)
		} else {
			pruneNulls(v)
		}
	}
}
func pruneNullsFromSlice(data []interface{}) {
	for _, v := range data {
		pruneNulls(v)
	}
}
func pruneNulls(v interface{}) {
	switch v := v.(type) {
	case map[string]interface{}:
		pruneNullsFromMap(v)
	case []interface{}:
		pruneNullsFromSlice(v)
	}
}

// 将 last-applied-configuration 注解保存在单独的 SSA field manager 中，
// 防止已转向 SSA 的用户丢弃该注解。
//
// 如果不执行此操作，那么在首次使用 SSA 时，last-applied-configuration 注解
// 就会从对象中被移除。我们希望将其保留几个版本，
// 因为根据 [1] 和 [2]，降级到
// SSA 时需要该注解。待该注解被弃用后，应移除这段代码。
//
// - [1] https://kubernetes.io/docs/reference/using-api/server-side-apply/#downgrading-from-server-side-apply-to-client-side-apply
// - [2] https://github.com/kubernetes/kubernetes/pull/90187
//
// 如果该注解尚不存在，或已由单独的 SSA fieldmanager 管理，
// 则此操作为空操作。
func (o *ApplyOptions) saveLastApplyAnnotationIfNecessary(
	helper *resource.Helper,
	info *resource.Info,
) error {
	if o.FieldManager != fieldManagerServerSideApply {
		// 如果 field manager 不会保持默认值，则保留该注解没有意义，
		// 因为服务端不会持续更新
		// 该注解。
		return nil
	}

	// 发送带有 last-applied 注解的 apply patch，
	// 使其在后续 patch 中不会被 SSA 孤立：
	accessor, err := meta.Accessor(info.Object)
	if err != nil {
		return err
	}

	// 从对象中获取当前注解。
	annots := accessor.GetAnnotations()
	if annots == nil {
		annots = map[string]string{}
	}

	fieldManager := fieldManagerLastAppliedAnnotation
	originalAnnotation, hasAnnotation := annots[corev1.LastAppliedConfigAnnotation]

	// 如果注解尚不存在，则不执行任何操作
	if !hasAnnotation {
		return nil
	}

	// 如果已有 SSA field manager 拥有该字段，
	// 则无需在此处执行任何操作。
	if owners := csaupgrade.FindFieldsOwners(
		accessor.GetManagedFields(),
		metav1.ManagedFieldsOperationApply,
		lastAppliedAnnotationFieldPath,
	); len(owners) > 0 {
		return nil
	}

	justAnnotation := &unstructured.Unstructured{}
	justAnnotation.SetGroupVersionKind(info.Mapping.GroupVersionKind)
	justAnnotation.SetName(accessor.GetName())
	justAnnotation.SetNamespace(accessor.GetNamespace())
	justAnnotation.SetAnnotations(map[string]string{
		corev1.LastAppliedConfigAnnotation: originalAnnotation,
	})

	modified, err := runtime.Encode(unstructured.UnstructuredJSONScheme, justAnnotation)
	if err != nil {
		return nil
	}

	helperCopy := *helper
	newObj, err := helperCopy.WithFieldManager(fieldManager).Patch(
		info.Namespace,
		info.Name,
		types.ApplyPatchType,
		modified,
		nil,
	)

	if err != nil {
		return err
	}

	return info.Refresh(newObj, false)
}

// 检查返回对象是否需要将 kubectl-client-side-apply
// 的 managed fields 迁移到 server-side-apply。
//
// 字段所有权元数据存储在这三个位置：
//   - server-side managed fields
//   - client-side managed fields
//   - 以及 last_applied_configuration 注解。
//
// 迁移会将 client-side 管理的字段合并到
// server-side 管理的字段中，并保留 last_applied_configuration
// 注解。在满足以下条件时，服务端会在每次 server-side-apply
// 后持续更新该注解：
//
//  1. field manager 为 'kubectl'
//  2. 注解已存在
func (o *ApplyOptions) migrateToSSAIfNecessary(
	helper *resource.Helper,
	info *resource.Info,
) (migrated bool, err error) {
	accessor, err := meta.Accessor(info.Object)
	if err != nil {
		return false, err
	}

	// 要确定 kubectl 曾使用哪些 field manager 执行 client-side-apply，
	// 我们会查找在 `Update` 操作中使用过、且拥有
	// last-applied 注解的 manager。
	//
	// 这是最后一次修改该字段的 client-side-apply manager。
	//
	// 如果多个 manager 写入了完全相同的配置，则可能存在多个所有者。
	// 在这种情况下，我们希望将所有所有者都迁移。
	//
	csaManagers := csaupgrade.FindFieldsOwners(
		accessor.GetManagedFields(),
		metav1.ManagedFieldsOperationUpdate,
		lastAppliedAnnotationFieldPath)

	managerNames := sets.New[string]()
	for _, entry := range csaManagers {
		managerNames.Insert(entry.Manager)
	}

	// 因 ResourceVersion 测试失败而冲突时，重试 patch
	//
	for i := 0; i < maxPatchRetry; i++ {
		var patchData []byte
		var obj runtime.Object

		patchData, err = csaupgrade.UpgradeManagedFieldsPatch(
			info.Object, managerNames, o.FieldManager)

		if err != nil {
			// 如果 patch 生成失败，很可能存在 bug。
			return false, err
		} else if patchData == nil {
			// patch 数据为 nil 表示无需操作——对象已经迁移完成
			return false, nil
		}

		// 如果 patch 非 nil，则发送它以升级 managed fields
		obj, err = helper.Patch(
			info.Namespace,
			info.Name,
			types.JSONPatchType,
			patchData,
			nil,
		)

		if err == nil {
			// 成功后停止重试。
			info.Refresh(obj, false)
			return true, nil
		} else if !errors.IsConflict(err) {
			// 仅在发生冲突时重试
			return false, err
		}

		// 刷新对象以供下一轮迭代使用
		err = info.Get()
		if err != nil {
			// 如果获取时出错，则返回错误
			return false, err
		}
	}

	// 执行到这里且错误非 nil，表示发生了冲突且
	// 已达到最大重试次数
	// 返回最后观察到的错误（将是冲突错误）
	return false, err
}

func (o *ApplyOptions) shouldPrintObject() bool {
	// 仅在指定了非 "name" 的输出格式时打印对象
	shouldPrint := false
	output := *o.PrintFlags.OutputFormat
	shortOutput := output == "name"
	if len(output) > 0 && !shortOutput {
		shouldPrint = true
	}
	return shouldPrint
}

func (o *ApplyOptions) printObjects() error {

	if !o.shouldPrintObject() {
		return nil
	}

	infos, err := o.GetObjects()
	if err != nil {
		return err
	}

	if len(infos) > 0 {
		printer, err := o.ToPrinter("")
		if err != nil {
			return err
		}

		objToPrint := infos[0].Object
		if len(infos) > 1 {
			objs := []runtime.Object{}
			for _, info := range infos {
				objs = append(objs, info.Object)
			}
			list := &corev1.List{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
				ListMeta: metav1.ListMeta{},
			}
			if err := meta.SetList(list, objs); err != nil {
				return err
			}

			objToPrint = list
		}
		if err := printer.PrintObj(objToPrint, o.Out); err != nil {
			return err
		}
	}

	return nil
}

// MarkNamespaceVisited 记录已应用对象所属的
// namespace。用于清理。
func (o *ApplyOptions) MarkNamespaceVisited(info *resource.Info) {
	if info.Namespaced() {
		o.VisitedNamespaces.Insert(info.Namespace)
	}
}

// MarkObjectVisited 记录已应用
// 对象的 UID。用于清理。
func (o *ApplyOptions) MarkObjectVisited(info *resource.Info) error {
	metadata, err := meta.Accessor(info.Object)
	if err != nil {
		return err
	}
	o.VisitedUids.Insert(metadata.GetUID())

	return nil
}

// PrintAndPrunePostProcessor 返回一个符合 PostProcessorFn
// 函数签名的函数。该函数会将所有
// 对象打印为列表（如果如此配置），并清理
// 未被应用的对象。返回的函数是标准的
// apply 后处理器。
func (o *ApplyOptions) PrintAndPrunePostProcessor() func() error {

	return func() error {
		ctx := context.TODO()
		if err := o.printObjects(); err != nil {
			return err
		}

		if o.Prune {
			if cmdutil.ApplySet.IsEnabled() && o.ApplySet != nil {
				if err := o.ApplySet.Prune(ctx, o); err != nil {
					// 不要更新 ApplySet。如果清理失败，我们希望保留
					// ApplySet 中之前和当前资源的并集，以便下次 apply
					// 的清理步骤能够正确地清理该集合。
					return err
				}
			} else {
				p := newPruner(o)
				return p.pruneAll(o)
			}
		}

		return nil
	}
}

const (
	// FieldManagerClientSideApply 是默认的 client-side apply field manager。
	//
	// 默认 field manager 不是 `kubectl-apply`，以区别于
	// server-side apply。
	FieldManagerClientSideApply = "kubectl-client-side-apply"
	// 默认的 server-side apply field manager 是 `kubectl`，
	// 而不是类似 `kubectl-server-side-apply` 的名称，
	// 这是为了向后兼容，避免与旧版本
	// kubectl server-side apply 中 `kubectl` 已作为 field manager 的情况冲突。
	fieldManagerServerSideApply = "kubectl"

	fieldManagerLastAppliedAnnotation = "kubectl-last-applied"
)

var (
	lastAppliedAnnotationFieldPath = fieldpath.NewSet(
		fieldpath.MakePathOrDie(
			"metadata", "annotations",
			corev1.LastAppliedConfigAnnotation),
	)
)

// GetApplyFieldManagerFlag 获取 kubectl apply 的 field manager，
// 前提是它尚未设置。
//
// 默认 field manager 不是 `kubectl-apply`，以区分
// client-side 和 server-side apply。
func GetApplyFieldManagerFlag(cmd *cobra.Command, serverSide bool) string {
	// field manager flag 已设置
	if cmd.Flag("field-manager").Changed {
		return cmdutil.GetFlagString(cmd, "field-manager")
	}

	if serverSide {
		return fieldManagerServerSideApply
	}

	return FieldManagerClientSideApply
}

// WarnIfDeleting 在资源正在被删除时打印警告
func WarnIfDeleting(obj runtime.Object, stderr io.Writer) {
	metadata, _ := meta.Accessor(obj)
	if metadata != nil && metadata.GetDeletionTimestamp() != nil {
		// 仅向用户发出冲突警告
		fmt.Fprintf(stderr, warningChangesOnDeletingResource, metadata.GetName())
	}
}
