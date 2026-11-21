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

package history

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"

	apps "k8s.io/api/apps/v1"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	clientset "k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/kubernetes/pkg/controller"
	hashutil "k8s.io/kubernetes/pkg/util/hash"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
)

// ControllerRevisionHashLabel 标注 ControllerRevision.Data 的哈希值，用于快速识别内容相同的版本。
const ControllerRevisionHashLabel = "controller.kubernetes.io/hash"

// ControllerRevisionControllerIndex 是 ControllerRevision 按控制器归属建立索引的名称。
const ControllerRevisionControllerIndex = "controllerRevisionController"

// ControllerRevisionControllerIndexKey 返回用于定位指定控制器 ownerReference 的 ControllerRevision 的索引键。
// 有 ownerReference 时键为 "namespace/kind/ownerReference.Name/ownerReference.UID"；
// 为 nil 时只返回命名空间，表示孤儿版本，便于按命名空间整体查询。
func ControllerRevisionControllerIndexKey(namespace string, ownerReference *metav1.OwnerReference) string {
	if ownerReference == nil {
		return namespace
	}
	return namespace + "/" + ownerReference.Kind + "/" + ownerReference.Name + "/" + string(ownerReference.UID)
}

// AddControllerRevisionControllerIndexer 给 informer 添加按 controllerRef 归属索引 ControllerRevision 的能力，
// 以便按 ControllerRef.UID 高效查找版本；索引已存在时直接返回，避免重复注册。
// 检查与注册之间若有其他控制器抢先注册，二次确认后也视为成功。
func AddControllerRevisionControllerIndexer(informer cache.SharedIndexInformer) error {
	if _, exists := informer.GetIndexer().GetIndexers()[ControllerRevisionControllerIndex]; exists {
		// indexer already exists, do nothing
		return nil
	}

	err := informer.AddIndexers(cache.Indexers{
		ControllerRevisionControllerIndex: func(obj interface{}) ([]string, error) {
			cr, ok := obj.(*apps.ControllerRevision)
			if !ok {
				return nil, nil
			}
			// 用控制归属（或非归属）作为索引键：非 nil 表示已被控制器拥有，nil 表示孤儿版本。
			return []string{ControllerRevisionControllerIndexKey(cr.Namespace, metav1.GetControllerOfNoCopy(cr))}, nil
		},
	})
	if err != nil {
		// 检查与注册之间可能有其他控制器抢先注册，二次确认后视为成功，避免误报错误。
		if _, exists := informer.GetIndexer().GetIndexers()[ControllerRevisionControllerIndex]; exists {
			return nil
		}
	}
	return err
}

// ControllerRevisionName 生成 prefix-hash 形式的 ControllerRevision 名称。
// 前缀超过 223 字节时截断，确保总长度不超过 Kubernetes 名称的 253 字节上限。
func ControllerRevisionName(prefix string, hash string) string {
	if len(prefix) > 223 {
		prefix = prefix[:223]
	}

	return fmt.Sprintf("%s-%s", prefix, hash)
}

// NewControllerRevision 构造一个 ControllerRevision：其 ControllerRef 指向 parent，标签取自 templateLabels，
// Data 为 data，Revision 为 revision。collisionCount 参与名称哈希，降低同名冲突概率；
// 返回错误为 nil 时结果可用，否则不应使用。
func NewControllerRevision(parent metav1.Object,
	parentKind schema.GroupVersionKind,
	templateLabels map[string]string,
	data runtime.RawExtension,
	revision int64,
	collisionCount *int32) (*apps.ControllerRevision, error) {
	labelMap := make(map[string]string)
	for k, v := range templateLabels {
		labelMap[k] = v
	}
	cr := &apps.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          labelMap,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(parent, parentKind)},
		},
		Data:     data,
		Revision: revision,
	}
	hash := HashControllerRevision(cr, collisionCount)
	cr.Name = ControllerRevisionName(parent.GetName(), hash)
	cr.Labels[ControllerRevisionHashLabel] = hash
	return cr, nil
}

// HashControllerRevision 用 FNV 哈希 revision 的 Data；probe 非 nil 时其值也写入哈希，
// 使名称冲突时可通过改变 probe 得到不同哈希。返回串经过安全编码，避免出现敏感词。
func HashControllerRevision(revision *apps.ControllerRevision, probe *int32) string {
	hf := fnv.New32()
	if len(revision.Data.Raw) > 0 {
		hf.Write(revision.Data.Raw)
	}
	if revision.Data.Object != nil {
		hashutil.DeepHashObject(hf, revision.Data.Object)
	}
	if probe != nil {
		hf.Write([]byte(strconv.FormatInt(int64(*probe), 10)))
	}
	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32()))
}

// SortControllerRevisions 按 Revision 升序稳定排序，保证相同版本号的相对顺序不变。
func SortControllerRevisions(revisions []*apps.ControllerRevision) {
	sort.Stable(byRevision(revisions))
}

// EqualRevision 判断两个版本是否语义等价：都为 nil，或都非 nil 且 Data 原始字节与对象语义均相等。
// 仅比较 Data，忽略 ownerReference、标签等元数据差异。
func EqualRevision(lhs *apps.ControllerRevision, rhs *apps.ControllerRevision) bool {
	if lhs == nil || rhs == nil {
		return lhs == rhs
	}
	return bytes.Equal(lhs.Data.Raw, rhs.Data.Raw) && apiequality.Semantic.DeepEqual(lhs.Data.Object, rhs.Data.Object)
}

// FindEqualRevisions 返回 revisions 中所有与 needle 语义相等的版本，保持原始顺序，
// 便于调用方判断某个版本是否已存在。
func FindEqualRevisions(revisions []*apps.ControllerRevision, needle *apps.ControllerRevision) []*apps.ControllerRevision {
	var eq []*apps.ControllerRevision
	for i := range revisions {
		if EqualRevision(revisions[i], needle) {
			eq = append(eq, revisions[i])
		}
	}
	return eq
}

// byRevision 实现 sort.Interface，用于按 Revision 排序 ControllerRevision。
type byRevision []*apps.ControllerRevision

// Len 返回版本数量，是 sort.Interface 要求的实现。
func (br byRevision) Len() int {
	return len(br)
}

// Less 先按 Revision 排序；版本号相同时按创建时间，仍相同则按名称，保证排序结果确定。
func (br byRevision) Less(i, j int) bool {
	if br[i].Revision == br[j].Revision {
		if br[j].CreationTimestamp.Equal(&br[i].CreationTimestamp) {
			return br[i].Name < br[j].Name
		}
		return br[j].CreationTimestamp.After(br[i].CreationTimestamp.Time)
	}
	return br[i].Revision < br[j].Revision
}

// Swap 交换两个版本的位置，是 sort.Interface 要求的实现。
func (br byRevision) Swap(i, j int) {
	br[i], br[j] = br[j], br[i]
}

// Interface 定义控制器历史（以 ControllerRevision 形式记录）的管理能力。
// 实例可通过 NewHistory 获取；实现必须把所有指针参数当作只读输入，不得修改。
type Interface interface {
	// ListControllerRevisions 列出所有匹配 selector、且归 parent 所有或无人拥有的 ControllerRevision。
	// 返回错误为 nil 时结果有效，否则返回的切片不可用。
	ListControllerRevisions(parent metav1.Object, parentKind schema.GroupVersionKind, selector labels.Selector) ([]*apps.ControllerRevision, error)
	// CreateControllerRevision 尝试以 parent 为拥有者创建修订版本。发生名称冲突时，
	// 把 collisionCount 加入版本哈希并重命名后重试（可限制重试次数）。调用方必须保证
	// collisionCount 非 nil，否则返回错误；成功时返回已创建的版本。
	CreateControllerRevision(parent metav1.Object, revision *apps.ControllerRevision, collisionCount *int32) (*apps.ControllerRevision, error)
	// DeleteControllerRevision 尝试删除修订版本，返回非 nil 错误表示删除失败。
	DeleteControllerRevision(revision *apps.ControllerRevision) error
	// UpdateControllerRevision 将 revision 的 Revision 更新为 newRevision，实现可在冲突时重试。
	// 返回错误为 nil 表示更新成功且返回值有效，否则更新失败、返回值不可用。
	UpdateControllerRevision(revision *apps.ControllerRevision, newRevision int64) (*apps.ControllerRevision, error)
	// AdoptControllerRevision 通过添加 ControllerRef 将 revision 归到 parent 名下。
	// 若已被其他对象拥有或补丁更新失败则返回错误；成功时返回有效的版本对象。
	AdoptControllerRevision(parent metav1.Object, parentKind schema.GroupVersionKind, revision *apps.ControllerRevision) (*apps.ControllerRevision, error)
	// ReleaseControllerRevision 通过从 OwnerReferences 中移除 parent 来解除其对 revision 的所有权。
	// 返回错误时所有权不变；成功时返回有效的版本对象。
	ReleaseControllerRevision(parent metav1.Object, revision *apps.ControllerRevision) (*apps.ControllerRevision, error)
}

// NewHistory 创建生产环境使用的 Interface：通过 client 与 API Server 交互，通过 lister 列举版本。
// indexer 非 nil 时用于优化 ListControllerRevisions，且必须已调用 AddControllerRevisionControllerIndexer。
func NewHistory(client clientset.Interface, lister appslisters.ControllerRevisionLister, indexer cache.Indexer) Interface {
	return &realHistory{client, lister, indexer}
}

// NewFakeHistory 创建测试用 Interface：直接基于 informer 的索引器完成版本的增删改查，避免访问 API Server。
func NewFakeHistory(informer appsinformers.ControllerRevisionInformer) Interface {
	return &fakeHistory{informer.Informer().GetIndexer(), informer.Lister()}
}

// realHistory 是基于 API Server 客户端与 lister 的生产环境实现。
type realHistory struct {
	client  clientset.Interface
	lister  appslisters.ControllerRevisionLister
	indexer cache.Indexer
}

// ListControllerRevisions 通过索引或 lister 获取匹配 selector 的版本，并过滤出归 parent 所有或孤儿版本。
func (rh *realHistory) ListControllerRevisions(parent metav1.Object, parentKind schema.GroupVersionKind, selector labels.Selector) ([]*apps.ControllerRevision, error) {
	// 先收集命名空间内所有匹配 selector 的版本，再按归属过滤。
	var history []*apps.ControllerRevision
	var err error
	if rh.indexer != nil {
		keys := []string{
			ControllerRevisionControllerIndexKey(parent.GetNamespace(), metav1.NewControllerRef(parent, parentKind)),
			ControllerRevisionControllerIndexKey(parent.GetNamespace(), nil), // 同时包含孤儿版本，避免漏掉可领养的历史。
		}

		for _, key := range keys {
			objs, err := rh.indexer.ByIndex(ControllerRevisionControllerIndex, key)
			if err != nil {
				return nil, err
			}
			for _, obj := range objs {
				cr, ok := obj.(*apps.ControllerRevision)
				if !ok {
					continue
				}
				if selector.Matches(labels.Set(cr.Labels)) {
					history = append(history, cr)
				}
			}
		}
	} else {
		// 没有索引器时退化为全量列举，虽然慢但保证功能正确。
		history, err = rh.lister.ControllerRevisions(parent.GetNamespace()).List(selector)
		if err != nil {
			return nil, err
		}
	}

	var owned []*apps.ControllerRevision
	for i := range history {
		ref := metav1.GetControllerOfNoCopy(history[i])
		if ref == nil || ref.UID == parent.GetUID() {
			owned = append(owned, history[i])
		}

	}
	return owned, err
}

// CreateControllerRevision 调用 API Server 创建版本；名称冲突时若已存在相同内容则复用，否则增加碰撞计数后重试。
func (rh *realHistory) CreateControllerRevision(parent metav1.Object, revision *apps.ControllerRevision, collisionCount *int32) (*apps.ControllerRevision, error) {
	if collisionCount == nil {
		return nil, fmt.Errorf("collisionCount should not be nil")
	}

	// 深拷贝输入对象。
	clone := revision.DeepCopy()

	// 持续尝试创建修订版本，每次迭代用新的哈希更新名称。
	for {
		hash := HashControllerRevision(revision, collisionCount)
		// 用新哈希重命名版本，规避名称冲突。
		clone.Name = ControllerRevisionName(parent.GetName(), hash)
		ns := parent.GetNamespace()
		created, err := rh.client.AppsV1().ControllerRevisions(ns).Create(context.TODO(), clone, metav1.CreateOptions{})
		if errors.IsAlreadyExists(err) {
			exists, err := rh.client.AppsV1().ControllerRevisions(ns).Get(context.TODO(), clone.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if bytes.Equal(exists.Data.Raw, clone.Data.Raw) {
				return exists, nil
			}
			*collisionCount++
			continue
		}
		return created, err
	}
}

// UpdateControllerRevision 通过 API Server 更新版本号，冲突时从 lister 重新获取最新对象后重试。
func (rh *realHistory) UpdateControllerRevision(revision *apps.ControllerRevision, newRevision int64) (*apps.ControllerRevision, error) {
	clone := revision.DeepCopy()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if clone.Revision == newRevision {
			return nil
		}
		clone.Revision = newRevision
		updated, updateErr := rh.client.AppsV1().ControllerRevisions(clone.Namespace).Update(context.TODO(), clone, metav1.UpdateOptions{})
		if updateErr == nil {
			return nil
		}
		if updated != nil {
			clone = updated
		}
		if updated, err := rh.lister.ControllerRevisions(clone.Namespace).Get(clone.Name); err == nil {
			// 深拷贝缓存对象后再修改，避免污染共享 informer 缓存。
			clone = updated.DeepCopy()
		}
		return updateErr
	})
	return clone, err
}

// DeleteControllerRevision 通过 API Server 删除指定版本。
func (rh *realHistory) DeleteControllerRevision(revision *apps.ControllerRevision) error {
	return rh.client.AppsV1().ControllerRevisions(revision.Namespace).Delete(context.TODO(), revision.Name, metav1.DeleteOptions{})
}

// objectForPatch 是添加/删除 ownerReference 时使用的 strategic merge patch 载荷。
type objectForPatch struct {
	Metadata objectMetaForPatch `json:"metadata"`
}

// objectMetaForPatch 定义补丁所需的元数据字段；UID 用于确保补丁只作用于目标版本。
type objectMetaForPatch struct {
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
	UID             types.UID               `json:"uid"`
}

// AdoptControllerRevision 用 strategic merge patch 给孤儿版本添加指向 parent 的 ControllerRef 以完成领养。
func (rh *realHistory) AdoptControllerRevision(parent metav1.Object, parentKind schema.GroupVersionKind, revision *apps.ControllerRevision) (*apps.ControllerRevision, error) {
	blockOwnerDeletion := true
	isController := true
	// 只允许领养孤儿版本；若已有控制器拥有者则拒绝，防止归属冲突。
	if owner := metav1.GetControllerOfNoCopy(revision); owner != nil {
		return nil, fmt.Errorf("attempt to adopt revision owned by %v", owner)
	}
	addControllerPatch := objectForPatch{
		Metadata: objectMetaForPatch{
			UID: revision.UID,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         parentKind.GroupVersion().String(),
				Kind:               parentKind.Kind,
				Name:               parent.GetName(),
				UID:                parent.GetUID(),
				Controller:         &isController,
				BlockOwnerDeletion: &blockOwnerDeletion,
			}},
		},
	}
	patchBytes, err := json.Marshal(&addControllerPatch)
	if err != nil {
		return nil, err
	}
	// 使用 strategic merge patch 添加 ownerReference 以表明控制归属。
	return rh.client.AppsV1().ControllerRevisions(parent.GetNamespace()).Patch(context.TODO(), revision.GetName(),
		types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
}

// ReleaseControllerRevision 用 strategic merge patch 从所有引用中移除 parent，解除其所有权。
func (rh *realHistory) ReleaseControllerRevision(parent metav1.Object, revision *apps.ControllerRevision) (*apps.ControllerRevision, error) {
	dataBytes, err := controller.GenerateDeleteOwnerRefStrategicMergeBytes(revision.UID, []types.UID{parent.GetUID()})
	if err != nil {
		return nil, err
	}

	// 使用 strategic merge patch 添加 ownerReference 以表明控制归属。
	released, err := rh.client.AppsV1().ControllerRevisions(revision.GetNamespace()).Patch(context.TODO(), revision.GetName(),
		types.StrategicMergePatchType, dataBytes, metav1.PatchOptions{})

	if err != nil {
		if errors.IsNotFound(err) {
			// 版本已被删除时视为释放成功，无需报错。
			return nil, nil
		}
		if errors.IsInvalid(err) {
			// 忽略 parent 已不再拥有该版本或版本已无拥有者的情况，这些都属于幂等成功的情形。
			// 幂等场景，无需处理。
			return nil, nil
		}
	}
	return released, err
}

// fakeHistory 是基于 informer 索引器的测试实现，避免访问 API Server。
type fakeHistory struct {
	indexer cache.Indexer
	lister  appslisters.ControllerRevisionLister
}

// ListControllerRevisions 测试实现：从 lister 列举并过滤出归 parent 所有或孤儿版本。
func (fh *fakeHistory) ListControllerRevisions(parent metav1.Object, parentKind schema.GroupVersionKind, selector labels.Selector) ([]*apps.ControllerRevision, error) {
	history, err := fh.lister.ControllerRevisions(parent.GetNamespace()).List(selector)
	if err != nil {
		return nil, err
	}

	var owned []*apps.ControllerRevision
	for i := range history {
		ref := metav1.GetControllerOf(history[i])
		if ref == nil || ref.UID == parent.GetUID() {
			owned = append(owned, history[i])
		}

	}
	return owned, err
}

// addRevision 将版本写入测试索引器；键已存在时返回 AlreadyExists，模拟 API Server 的名称冲突。
func (fh *fakeHistory) addRevision(revision *apps.ControllerRevision) (*apps.ControllerRevision, error) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(revision)
	if err != nil {
		return nil, err
	}
	obj, found, err := fh.indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if found {
		foundRevision := obj.(*apps.ControllerRevision)
		return foundRevision, errors.NewAlreadyExists(apps.Resource("controllerrevision"), revision.Name)
	}
	return revision, fh.indexer.Update(revision)
}

// CreateControllerRevision 测试实现：把版本写入索引器，名称冲突时增加碰撞计数并重试。
func (fh *fakeHistory) CreateControllerRevision(parent metav1.Object, revision *apps.ControllerRevision, collisionCount *int32) (*apps.ControllerRevision, error) {
	if collisionCount == nil {
		return nil, fmt.Errorf("collisionCount should not be nil")
	}

	// 深拷贝输入对象。
	clone := revision.DeepCopy()
	clone.Namespace = parent.GetNamespace()

	// 持续尝试创建修订版本，每次迭代用新的哈希更新名称。
	for {
		hash := HashControllerRevision(revision, collisionCount)
		// 用新哈希重命名版本，规避名称冲突。
		clone.Name = ControllerRevisionName(parent.GetName(), hash)
		created, err := fh.addRevision(clone)
		if errors.IsAlreadyExists(err) {
			*collisionCount++
			continue
		}
		return created, err
	}
}

// DeleteControllerRevision 测试实现：从索引器删除版本，不存在时返回 NotFound。
func (fh *fakeHistory) DeleteControllerRevision(revision *apps.ControllerRevision) error {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(revision)
	if err != nil {
		return err
	}
	obj, found, err := fh.indexer.GetByKey(key)
	if err != nil {
		return err
	}
	if !found {
		return errors.NewNotFound(apps.Resource("controllerrevisions"), revision.Name)
	}
	return fh.indexer.Delete(obj)
}

// UpdateControllerRevision 测试实现：修改版本号后更新索引器中的对象。
func (fh *fakeHistory) UpdateControllerRevision(revision *apps.ControllerRevision, newRevision int64) (*apps.ControllerRevision, error) {
	clone := revision.DeepCopy()
	clone.Revision = newRevision
	return clone, fh.indexer.Update(clone)
}

// AdoptControllerRevision 测试实现：为孤儿版本添加指向 parent 的 ControllerRef 并更新索引器。
func (fh *fakeHistory) AdoptControllerRevision(parent metav1.Object, parentKind schema.GroupVersionKind, revision *apps.ControllerRevision) (*apps.ControllerRevision, error) {
	if owner := metav1.GetControllerOf(revision); owner != nil {
		return nil, fmt.Errorf("attempt to adopt revision owned by %v", owner)
	}
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(revision)
	if err != nil {
		return nil, err
	}
	_, found, err := fh.indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.NewNotFound(apps.Resource("controllerrevisions"), revision.Name)
	}
	clone := revision.DeepCopy()
	clone.OwnerReferences = append(clone.OwnerReferences, *metav1.NewControllerRef(parent, parentKind))
	return clone, fh.indexer.Update(clone)
}

// ReleaseControllerRevision 测试实现：移除 parent 的 ownerReference 后更新索引器；版本不存在时视为成功。
func (fh *fakeHistory) ReleaseControllerRevision(parent metav1.Object, revision *apps.ControllerRevision) (*apps.ControllerRevision, error) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(revision)
	if err != nil {
		return nil, err
	}
	_, found, err := fh.indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	clone := revision.DeepCopy()
	refs := clone.OwnerReferences
	clone.OwnerReferences = nil
	for i := range refs {
		if refs[i].UID != parent.GetUID() {
			clone.OwnerReferences = append(clone.OwnerReferences, refs[i])
		}
	}
	return clone, fh.indexer.Update(clone)
}
