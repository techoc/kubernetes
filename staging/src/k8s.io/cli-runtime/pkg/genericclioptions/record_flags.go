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

package genericclioptions

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
)

// ChangeCauseAnnotation 记录对变更原因的推测。
//
// ChangeCauseAnnotation 记录“这次改动大概是因为执行了什么命令”，
// 由 --record 系列 flag 写入对象，供 rollout history 等场景解读。
const ChangeCauseAnnotation = "kubernetes.io/change-cause"

// RecordFlags 包含 "--record" 操作相关的全部标志。
//
// RecordFlags 绑定 --record flag 并保存待写入的 change-cause。
type RecordFlags struct {
	// Record 表示是否开启变更原因记录；用指针是为了让调用方关闭或重新绑定该标志。
	// 指针语义：nil 表示调用方完全不想暴露 --record。
	Record *bool

	// changeCause 是即将写入 annotation 的命令串，由 Complete 或显式设置填充。
	changeCause string
}

// 用户未显式给出 --record=false 时，ToRecorder 返回 ChangeCause recorder。
//
// 未开启记录时返回 NoopRecorder，调用方无需再判断是否启用。
func (f *RecordFlags) ToRecorder() (Recorder, error) {
	if f == nil {
		return NoopRecorder{}, nil
	}

	shouldRecord := false
	if f.Record != nil {
		shouldRecord = *f.Record
	}

	// 用户显式设为 false 时
	// 不记录。
	if !shouldRecord {
		return NoopRecorder{}, nil
	}

	return &ChangeCauseRecorder{
		changeCause: f.changeCause,
	}, nil
}

// Complete 在命令运行之前、但在其被调用之后调用，以在使用前完成结构体的状态设置。
func (f *RecordFlags) Complete(cmd *cobra.Command) error {
	if f == nil {
		return nil
	}

	f.changeCause = parseCommandArguments(cmd)
	return nil
}

// CompleteWithChangeCause 用新的原因改写 changeCause 值。
//
// 代替 Complete 直接指定 cause，供调用方用自定义文案覆盖命令行推断结果。
func (f *RecordFlags) CompleteWithChangeCause(cause string) error {
	if f == nil {
		return nil
	}

	f.changeCause = cause
	return nil
}

// AddFlags 把所需标志绑定到给定的 FlagSet。
// TODO: 后续应只接收一个 FlagSet。
//
// --record 已计划移除，这里同时标记为 deprecated，保留只为兼容旧脚本。
func (f *RecordFlags) AddFlags(cmd *cobra.Command) {
	if f == nil {
		return
	}

	if f.Record != nil {
		cmd.Flags().BoolVar(f.Record, "record", *f.Record, "Record current kubectl command in the resource annotation. If set to false, do not record the command. If set to true, record the command. If not set, default to updating the existing annotation value only if one already exists.")
		cmd.Flags().MarkDeprecated("record", "--record will be removed in the future")
	}
}

// NewRecordFlags 返回带合理默认值的 RecordFlags。
//
// 默认关闭记录；需要记录的命令再显式打开。
func NewRecordFlags() *RecordFlags {
	record := false

	return &RecordFlags{
		Record: &record,
	}
}

// Recorder 负责把 runtime.Object 的变更原因写入注解。
type Recorder interface {
	// Record 记录对象变更的原因。
	Record(runtime.Object) error
	// MakeRecordMergePatch 只生成“仅含 change-cause 变更”的 merge patch，
	// 用于在不覆盖服务端其它字段的前提下更新 annotation。
	MakeRecordMergePatch(runtime.Object) ([]byte, error)
}

// NoopRecorder 什么都不做；返回它可让调用方无需按类型分支判断。
//
// 空实现，避免调用方到处判断 recorder 是否为 nil。
type NoopRecorder struct{}

// Record 实现 Recorder 接口。
func (r NoopRecorder) Record(obj runtime.Object) error {
	return nil
}

// MakeRecordMergePatch 实现 Recorder 接口。
func (r NoopRecorder) MakeRecordMergePatch(obj runtime.Object) ([]byte, error) {
	return nil, nil
}

// ChangeCauseRecorder 给输入的 runtime 对象添加 "change-cause" 注解。
//
// 持有完整命令串，在每次 Record 时写入 ChangeCauseAnnotation。
type ChangeCauseRecorder struct {
	changeCause string
}

// 当 shouldRecord 为真，或资源原本就带有 "change-cause" 注解时，
// Record 给对应 Info 添加变更原因注解。
func (r *ChangeCauseRecorder) Record(obj runtime.Object) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	// annotation map 可能为 nil，先初始化再写入。
	annotations := accessor.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[ChangeCauseAnnotation] = r.changeCause
	accessor.SetAnnotations(annotations)
	return nil
}

// MakeRecordMergePatch 生成用于更新变更原因注解的 merge patch。
//
// 在副本上写入 annotation 后与原对象做 merge patch，避免直接改写调用方的对象。
func (r *ChangeCauseRecorder) MakeRecordMergePatch(obj runtime.Object) ([]byte, error) {
	// 先复制，避免改动原对象。
	objCopy := obj.DeepCopyObject()
	if err := r.Record(objCopy); err != nil {
		return nil, err
	}

	oldData, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	newData, err := json.Marshal(objCopy)
	if err != nil {
		return nil, err
	}

	return jsonpatch.CreateMergePatch(oldData, newData)
}

// parseCommandArguments 将对所有环境参数进行字符串化并返回，即客户端通过工厂执行的命令
// 使用工厂。
// 将 showSecrets 设为 false 以过滤掉类似密钥之类的敏感信息。
func parseCommandArguments(cmd *cobra.Command) string {
	if len(os.Args) == 0 {
		return ""
	}

	// ParseAll 会为每个 flag（包括未显式设置的）回调 parseFunc，这里借回调逐个
	// 序列化 flag，因为普通的 Parse() 拿不到“用户实际写了什么”。
	flags := ""
	parseFunc := func(flag *pflag.Flag, value string) error {
		flags = flags + " --" + flag.Name
		// "classified" 注解标记敏感 flag（如凭据、secret）。change-cause 会写入
		// 全局可读的 annotation，脱敏可以避免把密钥泄漏到那里。
		if set, ok := flag.Annotations["classified"]; !ok || len(set) == 0 {
			flags = flags + "=" + value
		} else {
			flags = flags + "=CLASSIFIED"
		}
		return nil
	}
	var err error
	err = cmd.Flags().ParseAll(os.Args[1:], parseFunc)
	// 解析失败时宁缺毋滥：残缺的 change-cause 会误导排查。
	if err != nil || !cmd.Flags().Parsed() {
		return ""
	}

	args := ""
	if arguments := cmd.Flags().Args(); len(arguments) > 0 {
		args = " " + strings.Join(arguments, " ")
	}

	// 只取程序名，避免把二进制的绝对路径写进 annotation。
	// 顺序为“位置参数在前、flags 在后”，与 pflag 语义等价（flags 可出现在任意位置）。
	base := filepath.Base(os.Args[0])
	return base + args + flags
}
