/*
Copyright 2020 The Kubernetes Authors.

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

package patches

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// PatchTarget 定义待修补目标，例如控制平面静态 Pod。
type PatchTarget struct {
	// Name 必须是已知目标名称。对于 Kubernetes 对象，通常与目标的 ObjectMeta.Name 匹配。
	Name string

	// StrategicMergePatchObject 仅用于战略合并补丁，表示被修补的底层对象类型，如 "v1.Pod"。
	StrategicMergePatchObject interface{}

	// Data 必须包含待修补的字节。
	Data []byte
}

// PatchManager 定义可应用补丁的对象。
type PatchManager struct {
	patchSets    []*PatchSet
	knownTargets []string
	output       io.Writer
}

// PatchSet 定义一组可修补 PatchTarget 的同类补丁。
type PatchSet struct {
	targetName string
	patchType  types.PatchType
	patches    []string
}

// String() 用于单元测试。
func (ps *PatchSet) String() string {
	return fmt.Sprintf(
		"{%q, %q, %#v}",
		ps.targetName,
		ps.patchType,
		ps.patches,
	)
}

const (
	// KubeletConfiguration 定义 kubeletconfiguration 补丁目标。
	KubeletConfiguration = "kubeletconfiguration"
	// CoreDNSDeployment 定义 corednsdeployment 补丁目标。
	CoreDNSDeployment = "corednsdeployment"
	// KubeProxyDaemonSet 定义 kubeproxydaemonset 补丁目标。
	KubeProxyDaemonSet = "kubeproxydaemonset"
)

var (
	pathLock  = &sync.RWMutex{}
	pathCache = map[string]*PatchManager{}

	patchTypes = map[string]types.PatchType{
		"json":      types.JSONPatchType,
		"merge":     types.MergePatchType,
		"strategic": types.StrategicMergePatchType,
		"":          types.StrategicMergePatchType, // "":          types.StrategicMergePatchType, // 默认
	}
	patchTypeList    = []string{"json", "merge", "strategic"}
	patchTypesJoined = strings.Join(patchTypeList, "|")
	knownExtensions  = []string{"json", "yaml"}

	regExtension = regexp.MustCompile(`.+\.(` + strings.Join(knownExtensions, "|") + `)$`)

	knownTargets = []string{
		kubeadmconstants.Etcd,
		kubeadmconstants.KubeAPIServer,
		kubeadmconstants.KubeControllerManager,
		kubeadmconstants.KubeScheduler,
		KubeletConfiguration,
		CoreDNSDeployment,
		KubeProxyDaemonSet,
	}
)

// KnownTargets 返回本地定义的 knownTargets。
func KnownTargets() []string {
	return knownTargets
}

// NewPatchManager 创建可将补丁应用到 "knownTargets" 的补丁管理器。
func NewPatchManager(patchSets []*PatchSet, knownTargets []string, output io.Writer) *PatchManager {
	return &PatchManager{
		patchSets:    patchSets,
		knownTargets: knownTargets,
		output:       output,
	}
}

// GetPatchManagerForPath 创建可将补丁应用到 "knownTargets" 的补丁管理器。
// "path" 应包含可用于这些目标的补丁；output 非空时，操作消息写入该 io.Writer。
func GetPatchManagerForPath(path string, knownTargets []string, output io.Writer) (*PatchManager, error) {
	pathLock.RLock()
	if pm, known := pathCache[path]; known {
		pathLock.RUnlock()
		return pm, nil
	}
	pathLock.RUnlock()

	if output == nil {
		output = io.Discard
	}

	fmt.Fprintf(output, "[patches] Reading patches from path %q\n", path)

	// 获取路径中的文件。
	patchSets, patchFiles, ignoredFiles, err := getPatchSetsFromPath(path, knownTargets, output)
	if err != nil {
		return nil, err
	}

	if len(patchFiles) > 0 {
		fmt.Fprintf(output, "[patches] Found the following patch files: %v\n", patchFiles)
	}
	if len(ignoredFiles) > 0 {
		fmt.Fprintf(output, "[patches] Ignored the following files: %v\n", ignoredFiles)
	}

	pm := &PatchManager{
		patchSets:    patchSets,
		knownTargets: knownTargets,
		output:       output,
	}
	pathLock.Lock()
	pathCache[path] = pm
	pathLock.Unlock()

	return pm, nil
}

// ApplyPatchesToTarget 使用管理器中保存的补丁修补目标的 "Data"，结果始终转换为 JSON。
func (pm *PatchManager) ApplyPatchesToTarget(patchTarget *PatchTarget) error {
	var err error
	var patchedData []byte

	if !slices.Contains(pm.knownTargets, patchTarget.Name) {
		return errors.Errorf("unknown patch target name %q, must be one of %v", patchTarget.Name, pm.knownTargets)
	}

	// 始终将目标数据转换为 JSON。
	patchedData, err = yaml.YAMLToJSON(patchTarget.Data)
	if err != nil {
		return err
	}

	// 遍历 patchSets。
	for _, patchSet := range pm.patchSets {
		if patchSet.targetName != patchTarget.Name {
			continue
		}

		// 遍历 patchSets 中的补丁。
		for _, patch := range patchSet.patches {
			patchBytes := []byte(patch)

			// 根据补丁类型应用。
			switch patchSet.patchType {

			// JSON 补丁。
			case types.JSONPatchType:
				var patchObj jsonpatch.Patch
				patchObj, err = jsonpatch.DecodePatch(patchBytes)
				if err == nil {
					patchedData, err = patchObj.Apply(patchedData)
				}

				// 合并补丁。
			case types.MergePatchType:
				patchedData, err = jsonpatch.MergePatch(patchedData, patchBytes)

				// 战略合并补丁。
			case types.StrategicMergePatchType:
				patchedData, err = strategicpatch.StrategicMergePatch(
					patchedData,
					patchBytes,
					patchTarget.StrategicMergePatchObject,
				)
			}

			if err != nil {
				return errors.Wrapf(err, "could not apply the following patch of type %q to target %q:\n%s\n",
					patchSet.patchType,
					patchTarget.Name,
					patch)
			}
			fmt.Fprintf(pm.output, "[patches] Applied patch of type %q to target %q\n", patchSet.patchType, patchTarget.Name)
		}

		// 更新此补丁目标的数据。
		patchTarget.Data = patchedData
	}

	return nil
}

// parseFilename 校验文件名并提取目标名称和补丁类型。
// - 扩展名或目标名未知时返回警告。
// - 补丁类型未知时返回错误。
// - 成功时返回目标名和补丁类型。
func parseFilename(fileName string, knownTargets []string) (string, types.PatchType, error, error) {
	// 扩展名无法匹配时返回警告。
	if !regExtension.MatchString(fileName) {
		return "", "", errors.Errorf("the file extension must be one of %v", knownExtensions), nil
	}

	regFileNameSplit := regexp.MustCompile(
		fmt.Sprintf(`^(%s)([^.+\n]*)?(\+)?(%s)?`, strings.Join(knownTargets, "|"), patchTypesJoined),
	)
	// 提取目标名和补丁类型，结果子串切片如下：
	//   [full-match, targetName, suffix, +, patchType]
	sub := regFileNameSplit.FindStringSubmatch(fileName)
	if sub == nil {
		return "", "", errors.Errorf("unknown target, must be one of %v", knownTargets), nil
	}
	targetName := sub[1]

	if len(sub[3]) > 0 && len(sub[4]) == 0 {
		return "", "", nil, errors.Errorf("unknown or missing patch type after '+', must be one of %v", patchTypeList)
	}
	patchType := patchTypes[sub[4]]

	return targetName, patchType, nil, nil
}

// CreatePatchSet 按 "\n---" 分割给定 "data"，创建 patchSet。
func CreatePatchSet(targetName string, patchType types.PatchType, data string) (*PatchSet, error) {
	var patches []string

	// 拆分补丁并转换为 JSON；已是 JSON 的数据不会报错。
	buf := bytes.NewBuffer([]byte(data))
	reader := utilyaml.NewYAMLReader(bufio.NewReader(buf))
	for {
		patch, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrapf(err, "could not split patches for data:\n%s\n", data)
		}

		patch = bytes.TrimSpace(patch)
		if len(patch) == 0 {
			continue
		}

		patchJSON, err := yaml.YAMLToJSON(patch)
		if err != nil {
			return nil, errors.Wrapf(err, "could not convert patch to JSON:\n%s\n", patch)
		}
		patches = append(patches, string(patchJSON))
	}

	return &PatchSet{
		targetName: targetName,
		patchType:  patchType,
		patches:    patches,
	}, nil
}

// getPatchSetsFromPath 遍历路径，忽略子目录和非补丁文件，返回 patchFile 列表。
func getPatchSetsFromPath(targetPath string, knownTargets []string, output io.Writer) ([]*PatchSet, []string, []string, error) {
	patchFiles := []string{}
	ignoredFiles := []string{}
	patchSets := []*PatchSet{}

	// 检查 targetPath 是否为目录。
	info, err := os.Lstat(targetPath)
	if err != nil {
		goto return_path_error
	}
	if !info.IsDir() {
		err = &os.PathError{
			Op:   "getPatchSetsFromPath",
			Path: info.Name(),
			Err:  errors.New("not a directory"),
		}
		goto return_path_error
	}

	err = filepath.Walk(targetPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 忽略子目录和 "."。
		if info.IsDir() {
			return nil
		}

		baseName := info.Name()

		// 解析文件名，获取目标和补丁类型。
		targetName, patchType, warn, err := parseFilename(baseName, knownTargets)
		if err != nil {
			return err
		}
		if warn != nil {
			fmt.Fprintf(output, "[patches] Ignoring file %q: %v\n", baseName, warn)
			ignoredFiles = append(ignoredFiles, baseName)
			return nil
		}

		// 读取补丁文件。
		data, err := os.ReadFile(path)
		if err != nil {
			return errors.Wrapf(err, "could not read the file %q", path)
		}

		if len(data) == 0 {
			fmt.Fprintf(output, "[patches] Ignoring empty file: %q\n", baseName)
			ignoredFiles = append(ignoredFiles, baseName)
			return nil
		}

		// 创建 patchSet。
		patchSet, err := CreatePatchSet(targetName, patchType, string(data))
		if err != nil {
			return err
		}

		patchFiles = append(patchFiles, baseName)
		patchSets = append(patchSets, patchSet)
		return nil
	})

return_path_error:
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "could not list patch files for path %q", targetPath)
	}

	return patchSets, patchFiles, ignoredFiles, nil
}
