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

package editor

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"

	"github.com/spf13/cobra"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/mergepatch"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/util/editor/crlf"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
)

// EditOptions 包含 kubectl edit 命令的所有选项
type EditOptions struct {
	resource.FilenameOptions
	RecordFlags *genericclioptions.RecordFlags

	PrintFlags *genericclioptions.PrintFlags
	ToPrinter  func(string) (printers.ResourcePrinter, error)

	OutputPatch        bool
	WindowsLineEndings bool

	cmdutil.ValidateOptions
	ValidationDirective string

	OriginalResult *resource.Result

	EditMode EditMode

	CmdNamespace    string
	ApplyAnnotation bool
	ChangeCause     string

	managedFields map[types.UID][]metav1.ManagedFieldsEntry

	genericiooptions.IOStreams

	Recorder            genericclioptions.Recorder
	f                   cmdutil.Factory
	editPrinterOptions  *editPrinterOptions
	updatedResultGetter func(data []byte) *resource.Result

	FieldManager string

	Subresource string
}

// NewEditOptions 返回初始化的 EditOptions 实例
func NewEditOptions(editMode EditMode, ioStreams genericiooptions.IOStreams) *EditOptions {
	return &EditOptions{
		RecordFlags: genericclioptions.NewRecordFlags(),

		EditMode: editMode,

		PrintFlags: genericclioptions.NewPrintFlags("edited").WithTypeSetter(scheme.Scheme),

		editPrinterOptions: &editPrinterOptions{
			// 为编辑器单独创建 PrintFlags，
			// 禁用所有输出标志，只保留 json / yaml。
			printFlags: (&genericclioptions.PrintFlags{
				JSONYamlPrintFlags: genericclioptions.NewJSONYamlPrintFlags(),
			}).WithDefaultOutput("yaml"),
			ext:       ".yaml",
			addHeader: true,
		},

		WindowsLineEndings: goruntime.GOOS == "windows",

		Recorder: genericclioptions.NoopRecorder{},

		IOStreams: ioStreams,
	}
}

type editPrinterOptions struct {
	printFlags *genericclioptions.PrintFlags
	ext        string
	addHeader  bool
}

func (e *editPrinterOptions) Complete(fromPrintFlags *genericclioptions.PrintFlags) error {
	if e.printFlags == nil {
		return fmt.Errorf("missing PrintFlags in editor printer options")
	}

	// 从已有 PrintFlags 继承输出格式。
	if fromPrintFlags != nil && len(*fromPrintFlags.OutputFormat) > 0 {
		e.printFlags.OutputFormat = fromPrintFlags.OutputFormat
	}

	// 以 JSON 形式展示内容时，在用户默认编辑器顶部
	// 插入注释头，防止误编辑。
	if *e.printFlags.OutputFormat == "json" {
		e.addHeader = false
		e.ext = ".json"
		return nil
	}

	// 上述检查不通过时默认用 YAML，因为只支持 json 和 yaml。
	e.addHeader = true
	e.ext = ".yaml"
	return nil
}

func (e *editPrinterOptions) PrintObj(obj runtime.Object, out io.Writer) error {
	p, err := e.printFlags.ToPrinter()
	if err != nil {
		return err
	}

	return p.PrintObj(obj, out)
}

// Complete 完成所有必需的选项初始化
func (o *EditOptions) Complete(f cmdutil.Factory, args []string, cmd *cobra.Command) error {
	var err error

	o.RecordFlags.Complete(cmd)
	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	if o.EditMode != NormalEditMode && o.EditMode != EditBeforeCreateMode && o.EditMode != ApplyEditMode {
		return fmt.Errorf("unsupported edit mode %q", o.EditMode)
	}

	o.editPrinterOptions.Complete(o.PrintFlags)

	if o.OutputPatch && o.EditMode != NormalEditMode {
		return fmt.Errorf("the edit mode doesn't support output the patch")
	}

	cmdNamespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}
	b := f.NewBuilder().
		Unstructured()
	if o.EditMode == NormalEditMode || o.EditMode == ApplyEditMode {
		// 普通 edit 和 apply edit 都必须先从服务端取最新资源。
		b = b.ResourceTypeOrNameArgs(true, args...).Latest()
	}
	r := b.NamespaceParam(cmdNamespace).DefaultNamespace().
		FilenameParam(enforceNamespace, &o.FilenameOptions).
		Subresource(o.Subresource).
		ContinueOnError().
		Flatten().
		Do()
	err = r.Err()
	if err != nil {
		return err
	}
	o.OriginalResult = r

	o.updatedResultGetter = func(data []byte) *resource.Result {
		// 用 resource builder 从编辑后的数据读取对象。
		return f.NewBuilder().
			Unstructured().
			Stream(bytes.NewReader(data), "edited-file").
			Subresource(o.Subresource).
			ContinueOnError().
			Flatten().
			Do()
	}

	o.ToPrinter = func(operation string) (printers.ResourcePrinter, error) {
		o.PrintFlags.NamePrintFlags.Operation = operation
		return o.PrintFlags.ToPrinter()
	}

	o.ValidationDirective, err = cmdutil.GetValidationDirective(cmd)
	if err != nil {
		return err
	}

	o.CmdNamespace = cmdNamespace
	o.f = f

	return nil
}

// Validate 校验 EditOptions（edit 命令不需要额外校验）
func (o *EditOptions) Validate() error {
	return nil
}

// Run 执行编辑命令的核心逻辑
func (o *EditOptions) Run() error {
	edit := NewDefaultEditor(editorEnvs())
	// editFn 是每次编辑会话的回调函数：普通编辑时传入资源列表调用一次，创建前编辑时每个资源调用一次
	editFn := func(infos []*resource.Info) error {
		var (
			results  = editResults{} // 编辑结果，包含错误信息和临时文件路径
			original = []byte{}      // 原始内容，用于比较是否有修改
			edited   = []byte{}      // 编辑后的内容
			file     string          // 临时文件路径
			err      error
		)

		containsError := false // 标记上一次循环是否发生过错误（用于重试逻辑）
		// 循环编辑直到成功或用户取消
		for {
			// 获取要序列化并传入编辑器的对象
			var originalObj runtime.Object
			switch len(infos) {
			case 1:
				// 单个资源直接使用
				originalObj = infos[0].Object
			default:
				// 多个资源时包装为 List 对象，方便在编辑器中一次性编辑
				l := &unstructured.UnstructuredList{
					Object: map[string]interface{}{
						"kind":       "List",
						"apiVersion": "v1",
						"metadata":   map[string]interface{}{},
					},
				}
				for _, info := range infos {
					l.Items = append(l.Items, *info.Object.(*unstructured.Unstructured))
				}
				originalObj = l
			}

			// 生成要编辑的文件内容
			buf := &bytes.Buffer{}
			var w io.Writer = buf
			// Windows 系统使用 CRLF 换行符
			if o.WindowsLineEndings {
				w = crlf.NewCRLFWriter(w)
			}

			// 写入编辑器提示头（注释行，告诉用户如何操作）
			if o.editPrinterOptions.addHeader {
				results.header.writeTo(w, o.EditMode)
			}

			if !containsError {
				// 首次编辑：提取 managedFields 并序列化原始对象
				// 提取 managedFields 是因为编辑器不应暴露字段管理器内部信息
				if err := o.extractManagedFields(originalObj); err != nil {
					return preservedFile(err, results.file, o.ErrOut)
				}

				if err := o.editPrinterOptions.PrintObj(originalObj, w); err != nil {
					return preservedFile(err, results.file, o.ErrOut)
				}
				original = buf.Bytes()
			} else {
				// 重试时：保留上次编辑的内容，去掉旧注释头（新头已写入）
				// 这样用户不需要重新输入之前修改的内容
				buf.Write(cmdutil.ManualStrip(edited))
			}

			// 启动编辑器，创建临时文件并等待用户保存退出
			editedDiff := edited // 保存上次编辑内容，用于检测用户是否修改
			edited, file, err = edit.LaunchTempFile(fmt.Sprintf("%s-edit-", filepath.Base(os.Args[0])), o.editPrinterOptions.ext, buf)
			if err != nil {
				return preservedFile(err, results.file, o.ErrOut)
			}

			// 重试时如果用户没有修改文件（去掉注释后内容相同），直接退出
			// 防止用户不修改就保存导致无限循环
			if containsError && bytes.Equal(cmdutil.StripComments(editedDiff), cmdutil.StripComments(edited)) {
				return preservedFile(fmt.Errorf("%s", "Edit cancelled, no valid changes were saved."), file, o.ErrOut)
			}
			// 清理上一次循环创建的临时文件
			if len(results.file) > 0 {
				os.Remove(results.file)
			}
			klog.V(4).Infof("User edited:\n%s", string(edited))

			// 对编辑后的内容进行 Schema 校验
			schema, err := o.f.Validator(o.ValidationDirective)
			if err != nil {
				return preservedFile(err, file, o.ErrOut)
			}
			// 去掉注释行后再校验，避免注释被当作 YAML/JSON 内容解析
			err = schema.ValidateBytes(cmdutil.StripComments(edited))
			if err != nil {
				// 校验失败：记录错误、设置重试标志，重新打开编辑器
				results = editResults{
					file: file,
				}
				containsError = true
				fmt.Fprintln(o.ErrOut, results.addError(apierrors.NewInvalid(corev1.SchemeGroupVersion.WithKind("").GroupKind(),
					"", field.ErrorList{field.Invalid(nil, "The edited file failed validation", fmt.Sprintf("%v", err))}), infos[0]))
				continue
			}

			// 比较原始内容和编辑后内容（去掉注释），无变化则取消编辑
			if bytes.Equal(cmdutil.StripComments(original), cmdutil.StripComments(edited)) {
				os.Remove(file)
				fmt.Fprintln(o.ErrOut, "Edit cancelled, no changes made.")
				return nil
			}

			// 检查文件是否为空（只有空白或注释）
			lines, err := hasLines(bytes.NewBuffer(edited))
			if err != nil {
				return preservedFile(err, file, o.ErrOut)
			}
			if !lines {
				os.Remove(file)
				fmt.Fprintln(o.ErrOut, "Edit cancelled, saved file was empty.")
				return nil
			}

			results = editResults{
				file: file,
			}

			// 解析编辑后的文件内容
			updatedInfos, err := o.updatedResultGetter(edited).Infos()
			if err != nil {
				// YAML/JSON 语法错误：记录原因并重新打开编辑器
				containsError = true
				results.header.reasons = append(results.header.reasons, editReason{head: fmt.Sprintf("The edited file had a syntax error: %v", err)})
				continue
			}

			// 解析成功，清除错误标志
			containsError = false
			updatedVisitor := resource.InfoListVisitor(updatedInfos)

			// 恢复之前提取的 managedFields，确保字段管理器信息不丢失
			if err := o.restoreManagedFields(updatedInfos); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}
			if err := o.restoreManagedFields(infos); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}

			// 校验 namespace 是否被用户修改，防止跨命名空间更新
			if err := updatedVisitor.Visit(resource.RequireNamespace(o.CmdNamespace)); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}

			// 为所有资源添加 annotation（如 last-applied-configuration 和 kubectl 命令记录）
			if err := o.visitAnnotation(updatedVisitor); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}

			// 根据编辑模式执行不同的更新策略
			switch o.EditMode {
			case NormalEditMode:
				// 普通编辑：计算 diff 并打 patch
				err = o.visitToPatch(infos, updatedVisitor, &results)
			case ApplyEditMode:
				// 编辑 last-applied-configuration annotation
				err = o.visitToApplyEditPatch(infos, updatedVisitor)
			case EditBeforeCreateMode:
				// 创建前编辑：直接创建资源
				err = o.visitToCreate(updatedVisitor)
			default:
				err = fmt.Errorf("unsupported edit mode %q", o.EditMode)
			}
			if err != nil {
				return preservedFile(err, results.file, o.ErrOut)
			}

			// 处理所有可能的错误类型
			//
			// 1. retryable（如冲突）：建议用户用 kubectl replace -f 手动重试
			// 2. notfound（资源已被删除）：保存编辑内容到文件，提示用户
			// 3. invalid（校验失败）：通过 containsError=true 进入下一轮循环，重新打开编辑器
			if results.retryable > 0 {
				fmt.Fprintf(o.ErrOut, "You can run `%s replace -f %s` to try this update again.\n", filepath.Base(os.Args[0]), file)
				return cmdutil.ErrExit
			}
			if results.notfound > 0 {
				fmt.Fprintf(o.ErrOut, "The edits you made on deleted resources have been saved to %q\n", file)
				return cmdutil.ErrExit
			}

			if len(results.edit) == 0 {
				// 没有需要重试的编辑，清理临时文件并退出
				if results.notfound == 0 {
					os.Remove(file)
				} else {
					fmt.Fprintf(o.Out, "The edits you made on deleted resources have been saved to %q\n", file)
				}
				return nil
			}

			// 还有需要重试的无效资源，重新打开编辑器
			if len(results.header.reasons) > 0 {
				containsError = true
			}
		}
	}

	// 根据编辑模式获取资源并调用 editFn
	switch o.EditMode {
	// 普通编辑模式：一次性获取所有资源，作为列表传入编辑器（方便批量编辑）
	case NormalEditMode:
		infos, err := o.OriginalResult.Infos()
		if err != nil {
			return err
		}
		if len(infos) == 0 {
			return errors.New("edit cancelled, no objects found")
		}
		return editFn(infos)
	case ApplyEditMode:
		// 编辑 last-applied-configuration：从 annotation 中提取原始配置进行编辑
		infos, err := o.OriginalResult.Infos()
		if err != nil {
			return err
		}
		var annotationInfos []*resource.Info
		for i := range infos {
			data, err := util.GetOriginalConfiguration(infos[i].Object)
			if err != nil {
				return err
			}
			if data == nil {
				// 跳过没有 last-applied-configuration annotation 的资源
				continue
			}

			tempInfos, err := o.updatedResultGetter(data).Infos()
			if err != nil {
				return err
			}
			annotationInfos = append(annotationInfos, tempInfos[0])
		}
		if len(annotationInfos) == 0 {
			return errors.New("no last-applied-configuration annotation found on resources, to create the annotation, use command `kubectl apply set-last-applied --create-annotation`")
		}
		return editFn(annotationInfos)
	// 创建前编辑模式：逐个资源调用 editFn，类似 kubectl create 的行为
	case EditBeforeCreateMode:
		return o.OriginalResult.Visit(func(info *resource.Info, err error) error {
			return editFn([]*resource.Info{info})
		})
	default:
		return fmt.Errorf("unsupported edit mode %q", o.EditMode)
	}
}

// extractManagedFields 从对象中提取 managedFields 并缓存
// 序列化到编辑器之前需要清除 managedFields，避免用户看到字段管理器内部信息
func (o *EditOptions) extractManagedFields(obj runtime.Object) error {
	o.managedFields = make(map[types.UID][]metav1.ManagedFieldsEntry)
	if meta.IsListType(obj) {
		err := meta.EachListItem(obj, func(obj runtime.Object) error {
			uid, mf, err := clearManagedFields(obj)
			if err != nil {
				return err
			}
			o.managedFields[uid] = mf
			return nil
		})
		return err
	}
	uid, mf, err := clearManagedFields(obj)
	if err != nil {
		return err
	}
	o.managedFields[uid] = mf
	return nil
}

// clearManagedFields 清除单个对象的 managedFields 并返回 UID 和原值
func clearManagedFields(obj runtime.Object) (types.UID, []metav1.ManagedFieldsEntry, error) {
	metaObjs, err := meta.Accessor(obj)
	if err != nil {
		return "", nil, err
	}
	mf := metaObjs.GetManagedFields()
	metaObjs.SetManagedFields(nil)
	return metaObjs.GetUID(), mf, nil
}

// restoreManagedFields 将之前提取的 managedFields 恢复到对象中
// 确保提交到 API server 时字段管理器信息完整
func (o *EditOptions) restoreManagedFields(infos []*resource.Info) error {
	for _, info := range infos {
		metaObjs, err := meta.Accessor(info.Object)
		if err != nil {
			return err
		}
		mf := o.managedFields[metaObjs.GetUID()]
		metaObjs.SetManagedFields(mf)
	}
	return nil
}

// visitToApplyEditPatch 处理 apply edit 模式：将编辑后的 last-applied-configuration 打 patch 到服务器
func (o *EditOptions) visitToApplyEditPatch(originalInfos []*resource.Info, patchVisitor resource.Visitor) error {
	err := patchVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		editObjUID, err := meta.NewAccessor().UID(info.Object)
		if err != nil {
			return err
		}

		// 通过 UID 匹配原始对象
		var originalInfo *resource.Info
		for _, i := range originalInfos {
			originalObjUID, err := meta.NewAccessor().UID(i.Object)
			if err != nil {
				return err
			}
			if editObjUID == originalObjUID {
				originalInfo = i
				break
			}
		}
		if originalInfo == nil {
			return fmt.Errorf("no original object found for %#v", info.Object)
		}

		originalJS, err := encodeToJSON(originalInfo.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		editedJS, err := encodeToJSON(info.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		// 无变化则跳过
		if reflect.DeepEqual(originalJS, editedJS) {
			printer, err := o.ToPrinter("skipped")
			if err != nil {
				return err
			}
			return printer.PrintObj(info.Object, o.Out)
		}
		// 将修改后的 annotation 打 patch 到服务器
		err = o.annotationPatch(info)
		if err != nil {
			return err
		}

		printer, err := o.ToPrinter("edited")
		if err != nil {
			return err
		}
		return printer.PrintObj(info.Object, o.Out)
	})
	return err
}

// annotationPatch 将 last-applied-configuration annotation 的变更打 patch 到服务器
func (o *EditOptions) annotationPatch(update *resource.Info) error {
	patch, _, patchType, err := GetApplyPatch(update.Object.(runtime.Unstructured))
	if err != nil {
		return err
	}
	mapping := update.ResourceMapping()
	client, err := o.f.UnstructuredClientForMapping(mapping)
	if err != nil {
		return err
	}
	helper := resource.NewHelper(client, mapping).
		WithFieldManager(o.FieldManager).
		WithFieldValidation(o.ValidationDirective).
		WithSubresource(o.Subresource)
	_, err = helper.Patch(o.CmdNamespace, update.Name, patchType, patch, nil)
	return err
}

// GetApplyPatch 计算包含 last-applied-configuration annotation 变更的 merge patch
// 原理：比较添加 annotation 前后的 JSON，生成只更新 annotation 的 patch
func GetApplyPatch(obj runtime.Unstructured) ([]byte, []byte, types.PatchType, error) {
	beforeJSON, err := encodeToJSON(obj)
	if err != nil {
		return nil, []byte(""), types.MergePatchType, err
	}
	objCopy := obj.DeepCopyObject()
	accessor := meta.NewAccessor()
	annotations, err := accessor.Annotations(objCopy)
	if err != nil {
		return nil, beforeJSON, types.MergePatchType, err
	}
	if annotations == nil {
		annotations = map[string]string{}
	}
	// 设置 last-applied-configuration annotation
	annotations[corev1.LastAppliedConfigAnnotation] = string(beforeJSON)
	accessor.SetAnnotations(objCopy, annotations)
	afterJSON, err := encodeToJSON(objCopy.(runtime.Unstructured))
	if err != nil {
		return nil, beforeJSON, types.MergePatchType, err
	}
	// 生成只包含 annotation 变更的 merge patch
	patch, err := jsonpatch.CreateMergePatch(beforeJSON, afterJSON)
	return patch, beforeJSON, types.MergePatchType, err
}

// encodeToJSON 将 unstructured 对象序列化为 JSON 字节
func encodeToJSON(obj runtime.Unstructured) ([]byte, error) {
	serialization, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		return nil, err
	}
	js, err := yaml.ToJSON(serialization)
	if err != nil {
		return nil, err
	}
	return js, nil
}

// visitToPatch 处理普通编辑模式：计算原始对象和编辑后对象的 diff，然后打 patch 到服务器
func (o *EditOptions) visitToPatch(originalInfos []*resource.Info, patchVisitor resource.Visitor, results *editResults) error {
	err := patchVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		editObjUID, err := meta.NewAccessor().UID(info.Object)
		if err != nil {
			return err
		}

		// 通过 UID 匹配原始对象，用于计算 diff
		var originalInfo *resource.Info
		for _, i := range originalInfos {
			originalObjUID, err := meta.NewAccessor().UID(i.Object)
			if err != nil {
				return err
			}
			if editObjUID == originalObjUID {
				originalInfo = i
				break
			}
		}
		if originalInfo == nil {
			return fmt.Errorf("no original object found for %#v", info.Object)
		}

		originalJS, err := encodeToJSON(originalInfo.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		editedJS, err := encodeToJSON(info.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		// 无变化则跳过
		if reflect.DeepEqual(originalJS, editedJS) {
			printer, err := o.ToPrinter("skipped")
			if err != nil {
				return err
			}
			return printer.PrintObj(info.Object, o.Out)
		}

		// 设置前置条件：apiVersion、kind、name、managedFields 不允许被修改
		preconditions := []mergepatch.PreconditionFunc{
			mergepatch.RequireKeyUnchanged("apiVersion"),
			mergepatch.RequireKeyUnchanged("kind"),
			mergepatch.RequireMetadataKeyUnchanged("name"),
			mergepatch.RequireKeyUnchanged("managedFields"),
		}

		// 根据 GVK 创建版本化对象，用于 strategic merge patch
		versionedObject, err := scheme.Scheme.New(info.Mapping.GroupVersionKind)
		var patchType types.PatchType
		var patch []byte
		switch {
		case runtime.IsNotRegisteredError(err):
			// 类型未注册（如 CRD），回退到通用的 JSON merge patch
			patchType = types.MergePatchType
			patch, err = jsonpatch.CreateMergePatch(originalJS, editedJS)
			if err != nil {
				klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
				return err
			}
			// 校验前置条件：确保关键字段未被修改
			var patchMap map[string]interface{}
			err = json.Unmarshal(patch, &patchMap)
			if err != nil {
				klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
				return err
			}
			for _, precondition := range preconditions {
				if !precondition(patchMap) {
					klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
					return fmt.Errorf("%s", "At least one of apiVersion, kind and name was changed")
				}
			}
		case err != nil:
			return err
		default:
			// 类型已注册，使用 strategic merge patch（能正确处理 list 合并等语义）
			patchType = types.StrategicMergePatchType
			patch, err = strategicpatch.CreateTwoWayMergePatch(originalJS, editedJS, versionedObject, preconditions...)
			if err != nil {
				klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
				if mergepatch.IsPreconditionFailed(err) {
					return fmt.Errorf("%s", "At least one of apiVersion, kind and name was changed")
				}
				return err
			}
		}

		if o.OutputPatch {
			fmt.Fprintf(o.Out, "Patch: %s\n", string(patch))
		}

		// 将 patch 提交到 API server
		patched, err := resource.NewHelper(info.Client, info.Mapping).
			WithFieldManager(o.FieldManager).
			WithFieldValidation(o.ValidationDirective).
			WithSubresource(o.Subresource).
			Patch(info.Namespace, info.Name, patchType, patch, nil)
		if err != nil {
			// patch 失败时记录错误，继续处理下一个资源
			fmt.Fprintln(o.ErrOut, results.addError(err, info))
			return nil
		}
		// 用服务器返回的对象更新本地 info
		info.Refresh(patched, true)
		printer, err := o.ToPrinter("edited")
		if err != nil {
			return err
		}
		return printer.PrintObj(info.Object, o.Out)
	})
	return err
}

// visitToCreate 处理创建前编辑模式：直接将编辑后的对象创建到服务器
func (o *EditOptions) visitToCreate(createVisitor resource.Visitor) error {
	err := createVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		obj, err := resource.NewHelper(info.Client, info.Mapping).
			WithFieldManager(o.FieldManager).
			WithFieldValidation(o.ValidationDirective).
			Create(info.Namespace, true, info.Object)
		if err != nil {
			return err
		}
		info.Refresh(obj, true)
		printer, err := o.ToPrinter("created")
		if err != nil {
			return err
		}
		return printer.PrintObj(info.Object, o.Out)
	})
	return err
}

// visitAnnotation 为所有资源添加 annotation
func (o *EditOptions) visitAnnotation(annotationVisitor resource.Visitor) error {
	err := annotationVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		// 如果需要，添加 last-applied-configuration annotation
		if o.ApplyAnnotation {
			if err := util.CreateOrUpdateAnnotation(true, info.Object, scheme.DefaultJSONEncoder()); err != nil {
				return err
			}
		}
		// 记录 kubectl 命令到 annotation（用于 kubectl annotate --all 等场景）
		if err := o.Recorder.Record(info.Object); err != nil {
			klog.V(4).Infof("error recording current command: %v", err)
		}

		return nil

	})
	return err
}

// EditMode 编辑模式：普通编辑、创建前编辑、或编辑 last-applied-configuration
type EditMode string

const (
	// NormalEditMode 普通编辑模式：编辑已存在的资源
	NormalEditMode EditMode = "normal_mode"

	// EditBeforeCreateMode 创建前编辑模式：先编辑再创建资源（kubectl apply --edit-last-applied）
	EditBeforeCreateMode EditMode = "edit_before_create_mode"

	// ApplyEditMode 编辑 last-applied-configuration annotation
	ApplyEditMode EditMode = "edit_last_applied_mode"
)

// editReason 记录需要重新编辑的原因
type editReason struct {
	head  string   // 主要错误信息
	other []string // 详细的错误原因列表
}

// editHeader 编辑头信息，包含所有需要重试的原因
// 在重新打开编辑器时，这些原因会作为注释写入文件顶部
type editHeader struct {
	reasons []editReason
}

// writeTo 将编辑头信息（注释行）写入输出流
func (h *editHeader) writeTo(w io.Writer, editMode EditMode) error {
	if editMode == ApplyEditMode {
		fmt.Fprint(w, `# Please edit the 'last-applied-configuration' annotations below.
# Lines beginning with a '#' will be ignored, and an empty file will abort the edit.
#
`)
	} else {
		fmt.Fprint(w, `# Please edit the object below. Lines beginning with a '#' will be ignored,
# and an empty file will abort the edit. If an error occurs while saving this file will be
# reopened with the relevant failures.
#
`)
	}

	for _, r := range h.reasons {
		if len(r.other) > 0 {
			fmt.Fprintf(w, "# %s:\n", hashOnLineBreak(r.head))
		} else {
			fmt.Fprintf(w, "# %s\n", hashOnLineBreak(r.head))
		}
		for _, o := range r.other {
			fmt.Fprintf(w, "# * %s\n", hashOnLineBreak(o))
		}
		fmt.Fprintln(w, "#")
	}
	return nil
}

// editResults 记录编辑结果：错误统计、需要重试的资源、临时文件路径
type editResults struct {
	header    editHeader       // 编辑头，包含错误原因
	retryable int              // 可重试的错误数（如冲突）
	notfound  int              // 资源不存在的错误数
	edit      []*resource.Info // 需要重新编辑的资源列表
	file      string           // 临时文件路径，出错时保留
}

// addError 根据错误类型分类统计，并返回错误消息
// Invalid 错误会加入重试列表，NotFound 表示资源已删除，其他错误标记为可重试
func (r *editResults) addError(err error, info *resource.Info) string {
	resourceString := info.Mapping.Resource.Resource
	if len(info.Mapping.Resource.Group) > 0 {
		resourceString = resourceString + "." + info.Mapping.Resource.Group
	}

	switch {
	case apierrors.IsInvalid(err):
		// 校验失败：加入需要重新编辑的列表，记录详细原因
		r.edit = append(r.edit, info)
		reason := editReason{
			head: fmt.Sprintf("%s %q was not valid", resourceString, info.Name),
		}
		if err, ok := err.(apierrors.APIStatus); ok {
			if details := err.Status().Details; details != nil {
				for _, cause := range details.Causes {
					reason.other = append(reason.other, fmt.Sprintf("%s: %s", cause.Field, cause.Message))
				}
			}
		}
		r.header.reasons = append(r.header.reasons, reason)
		return fmt.Sprintf("error: %s %q is invalid", resourceString, info.Name)
	case apierrors.IsNotFound(err):
		// 资源已被删除，无法更新
		r.notfound++
		return fmt.Sprintf("error: %s %q could not be found on the server", resourceString, info.Name)
	default:
		// 其他错误（如冲突）：标记为可重试
		r.retryable++
		return fmt.Sprintf("error: %s %q could not be patched: %v", resourceString, info.Name, err)
	}
}

// preservedFile 出错时通知用户临时文件的保存位置
// 确保用户的编辑内容不会丢失
func preservedFile(err error, path string, out io.Writer) error {
	if len(path) > 0 {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			fmt.Fprintf(out, "A copy of your changes has been stored to %q\n", path)
		}
	}
	return err
}

// hasLines 检查文件是否有有效内容（非空、非纯注释的行）
// 用于判断用户是否真的编辑了文件，还是只保存了空文件
func hasLines(r io.Reader) (bool, error) {
	// TODO: 若读取的文件存在超过 64KB 的行，需改用 bytes.ReadLine。
	// TODO: 这里读到的很可能是 Secret。
	s := bufio.NewScanner(r)
	for s.Scan() {
		if line := strings.TrimSpace(s.Text()); len(line) > 0 && line[0] != '#' {
			return true, nil
		}
	}
	if err := s.Err(); err != nil && err != io.EOF {
		return false, err
	}
	return false, nil
}

// hashOnLineBreak 在多行字符串的每行开头添加 '#' 注释符
// 用于将错误信息格式化为编辑器注释头中的多行注释
func hashOnLineBreak(s string) string {
	r := ""
	for i, ch := range s {
		j := i + 1
		if j < len(s) && ch == '\n' && s[j] != '#' {
			r += "\n# "
		} else {
			r += string(ch)
		}
	}
	return r
}

// editorEnvs 返回用于查找编辑器的环境变量列表（按优先级排序）
func editorEnvs() []string {
	return []string{
		"KUBE_EDITOR",
		"EDITOR",
	}
}
