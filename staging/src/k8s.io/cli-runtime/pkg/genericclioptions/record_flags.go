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

// ChangeCauseAnnotation is the annotation indicating a guess at "why" something was changed
const ChangeCauseAnnotation = "kubernetes.io/change-cause"

// RecordFlags contains all flags associated with the "--record" operation
type RecordFlags struct {
	// Record indicates the state of the recording flag.  It is a pointer so a caller can opt out or rebind
	Record *bool

	changeCause string
}

// ToRecorder returns a ChangeCause recorder if --record=false was not
// explicitly given by the user
func (f *RecordFlags) ToRecorder() (Recorder, error) {
	if f == nil {
		return NoopRecorder{}, nil
	}

	shouldRecord := false
	if f.Record != nil {
		shouldRecord = *f.Record
	}

	// if flag was explicitly set to false by the user,
	// do not record
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

// CompleteWithChangeCause alters changeCause value with a new cause
func (f *RecordFlags) CompleteWithChangeCause(cause string) error {
	if f == nil {
		return nil
	}

	f.changeCause = cause
	return nil
}

// AddFlags binds the requested flags to the provided flagset
// TODO have this only take a flagset
func (f *RecordFlags) AddFlags(cmd *cobra.Command) {
	if f == nil {
		return
	}

	if f.Record != nil {
		cmd.Flags().BoolVar(f.Record, "record", *f.Record, "Record current kubectl command in the resource annotation. If set to false, do not record the command. If set to true, record the command. If not set, default to updating the existing annotation value only if one already exists.")
		cmd.Flags().MarkDeprecated("record", "--record will be removed in the future")
	}
}

// NewRecordFlags provides a RecordFlags with reasonable default values set for use
func NewRecordFlags() *RecordFlags {
	record := false

	return &RecordFlags{
		Record: &record,
	}
}

// Recorder is used to record why a runtime.Object was changed in an annotation.
type Recorder interface {
	// Record records why a runtime.Object was changed in an annotation.
	Record(runtime.Object) error
	MakeRecordMergePatch(runtime.Object) ([]byte, error)
}

// NoopRecorder does nothing.  It is a "do nothing" that can be returned so code doesn't switch on it.
type NoopRecorder struct{}

// Record implements Recorder
func (r NoopRecorder) Record(obj runtime.Object) error {
	return nil
}

// MakeRecordMergePatch implements Recorder
func (r NoopRecorder) MakeRecordMergePatch(obj runtime.Object) ([]byte, error) {
	return nil, nil
}

// ChangeCauseRecorder annotates a "change-cause" to an input runtime object
type ChangeCauseRecorder struct {
	changeCause string
}

// Record annotates a "change-cause" to a given info if either "shouldRecord" is true,
// or the resource info previously contained a "change-cause" annotation.
func (r *ChangeCauseRecorder) Record(obj runtime.Object) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	annotations := accessor.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[ChangeCauseAnnotation] = r.changeCause
	accessor.SetAnnotations(annotations)
	return nil
}

// MakeRecordMergePatch produces a merge patch for updating the recording annotation.
func (r *ChangeCauseRecorder) MakeRecordMergePatch(obj runtime.Object) ([]byte, error) {
	// copy so we don't mess with the original
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
