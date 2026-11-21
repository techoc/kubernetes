/*
Copyright 2015 The Kubernetes Authors.

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

//go:generate mockery
package pod

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

// Manager 存储并管理 Pod 访问，同时维护静态 Pod 与镜像 Pod 之间的映射。
// kubelet 从 file、http、apiserver 三种来源发现 Pod；非 apiserver 来源的
// Pod 称为静态 Pod，apiserver 并不知道它们的存在。为了上报这些 Pod 的状态，
// kubelet 会通过 apiserver 为每个静态 Pod 创建镜像 Pod。
// 镜像 Pod 与静态 Pod 具有相同的全名（namespace/name），但 UID 等元数据不同。
// kubelet 以全名上报状态，因此镜像 Pod 的状态始终反映静态 Pod 的真实状态；
// 静态 Pod 被删除后，对应的孤儿镜像 Pod 也会被清理。
type Manager interface {
	// GetPodByFullName 返回匹配全名的非镜像 Pod，以及是否找到。
	GetPodByFullName(podFullName string) (*v1.Pod, bool)
	// GetPodByName 返回匹配 namespace 和 name 的非镜像 Pod，以及是否找到。
	GetPodByName(namespace, name string) (*v1.Pod, bool)
	// GetPodByUID 返回匹配 Pod UID 的非镜像 Pod，以及是否找到。
	GetPodByUID(types.UID) (*v1.Pod, bool)
	// GetPodByMirrorPod 返回给定镜像 Pod 对应的静态 Pod，以及管理器是否认识它。
	GetPodByMirrorPod(*v1.Pod) (*v1.Pod, bool)
	// GetMirrorPodByPod 返回给定静态 Pod 对应的镜像 Pod，以及管理器是否认识它。
	GetMirrorPodByPod(*v1.Pod) (*v1.Pod, bool)
	// GetPodAndMirrorPod 返回一个 Pod 的对端：传入普通 Pod 时尽量返回其镜像 Pod；
	// 传入镜像 Pod 时返回静态 Pod，并令 wasMirror 为 true。
	GetPodAndMirrorPod(*v1.Pod) (pod, mirrorPod *v1.Pod, wasMirror bool)

	// GetPods 返回绑定到该 kubelet 的所有普通 Pod。
	GetPods() []*v1.Pod

	// GetPodsAndMirrorPods 返回普通 Pod、镜像 Pod，以及所有孤儿镜像 Pod 的全名。
	GetPodsAndMirrorPods() (allPods []*v1.Pod, allMirrorPods []*v1.Pod, orphanedMirrorPodFullnames []string)

	// GetStaticPodToMirrorPodMap 返回静态 Pod 到其镜像 Pod 的映射。
	// kubelet 以 standalone 模式运行或镜像 Pod 尚在创建时，对应值可能为 nil。
	GetStaticPodToMirrorPodMap() map[*v1.Pod]*v1.Pod

	// SetPods 用新 Pod 集合整体替换内部状态，目前仅用于测试。
	SetPods(pods []*v1.Pod)
	// AddPod 把给定 Pod 加入管理器。
	AddPod(pod *v1.Pod)
	// UpdatePod 更新管理器中的给定 Pod。
	UpdatePod(pod *v1.Pod)
	// RemovePod 从管理器中删除给定 Pod：镜像 Pod 删除相关映射，
	// 非镜像 Pod 则从普通 Pod 的各个索引中删除。
	RemovePod(pod *v1.Pod)

	// TranslatePodUID 返回 Pod 的真实 UID：输入镜像 Pod UID 时返回其静态 Pod 的 UID，
	// 否则原样返回。
	// 所有对外函数都应先做这个转换，因为用户可能提供镜像 Pod 的 UID，
	// 而 kubelet 内部函数只认识静态 Pod 的 UID。
	TranslatePodUID(uid types.UID) kubetypes.ResolvedPodUID
	// GetUIDTranslations 返回静态 Pod UID 到镜像 Pod UID 及反向的映射。
	GetUIDTranslations() (podToMirror map[kubetypes.ResolvedPodUID]kubetypes.MirrorPodUID, mirrorToPod map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID)
}

// basicManager 是 Manager 的功能实现。
// 所有字段对外只读，只能通过 SetPods、AddPod、UpdatePod 或 RemovePod 更新。
type basicManager struct {
	// 保护所有内部 map。
	lock sync.RWMutex

	// 普通 Pod，按 UID 索引。
	podByUID map[kubetypes.ResolvedPodUID]*v1.Pod
	// 镜像 Pod，按 UID 索引。
	mirrorPodByUID map[kubetypes.MirrorPodUID]*v1.Pod

	// Pod 按全名索引，便于按名称快速查找。
	podByFullName       map[string]*v1.Pod
	mirrorPodByFullName map[string]*v1.Pod

	// 镜像 Pod UID 到静态 Pod UID 的映射。
	translationByUID map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID
}

// NewBasicPodManager 返回一个可用的 Manager 实现。
func NewBasicPodManager() Manager {
	pm := &basicManager{}
	pm.SetPods(nil)
	return pm
}

// SetPods 用新 Pod 集合重建所有内部索引。
func (pm *basicManager) SetPods(newPods []*v1.Pod) {
	pm.lock.Lock()
	defer pm.lock.Unlock()

	pm.podByUID = make(map[kubetypes.ResolvedPodUID]*v1.Pod)
	pm.podByFullName = make(map[string]*v1.Pod)
	pm.mirrorPodByUID = make(map[kubetypes.MirrorPodUID]*v1.Pod)
	pm.mirrorPodByFullName = make(map[string]*v1.Pod)
	pm.translationByUID = make(map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID)

	pm.updatePodsInternal(newPods...)
}

// AddPod 直接复用 UpdatePod：新增与更新在索引维护上没有区别。
func (pm *basicManager) AddPod(pod *v1.Pod) {
	pm.UpdatePod(pod)
}

// UpdatePod 在写锁内插入或替换 Pod，并同步维护静态 Pod/镜像 Pod 映射。
func (pm *basicManager) UpdatePod(pod *v1.Pod) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.updatePodsInternal(pod)
}

// updateMetrics 调整 Pod manager 暴露的临时容器计数指标。
// oldPod 或 newPod 为 nil 分别表示创建或删除。
func updateMetrics(oldPod, newPod *v1.Pod) {
	var numEC int
	if oldPod != nil {
		numEC -= len(oldPod.Spec.EphemeralContainers)
	}
	if newPod != nil {
		numEC += len(newPod.Spec.EphemeralContainers)
	}
	if numEC != 0 {
		metrics.ManagedEphemeralContainers.Add(float64(numEC))
	}
}

// updatePodsInternal 把给定 Pod 写入管理器并更新各索引，
// 调用方必须已持有写锁。
func (pm *basicManager) updatePodsInternal(pods ...*v1.Pod) {
	for _, pod := range pods {
		podFullName := kubecontainer.GetPodFullName(pod)
		// 该逻辑依赖静态 Pod 与镜像 Pod 同名；
		// 已由 IsMirrorPod 判断保证类型转换安全。
		if kubetypes.IsMirrorPod(pod) {
			mirrorPodUID := kubetypes.MirrorPodUID(pod.UID)
			pm.mirrorPodByUID[mirrorPodUID] = pod
			pm.mirrorPodByFullName[podFullName] = pod
			if p, ok := pm.podByFullName[podFullName]; ok {
				pm.translationByUID[mirrorPodUID] = kubetypes.ResolvedPodUID(p.UID)
			}
		} else {
			resolvedPodUID := kubetypes.ResolvedPodUID(pod.UID)
			updateMetrics(pm.podByUID[resolvedPodUID], pod)
			pm.podByUID[resolvedPodUID] = pod
			pm.podByFullName[podFullName] = pod
			if mirror, ok := pm.mirrorPodByFullName[podFullName]; ok {
				pm.translationByUID[kubetypes.MirrorPodUID(mirror.UID)] = resolvedPodUID
			}
		}
	}
}

// RemovePod 从所有索引中删除 Pod；镜像 Pod 还要清除 UID 翻译映射。
func (pm *basicManager) RemovePod(pod *v1.Pod) {
	updateMetrics(pod, nil)
	pm.lock.Lock()
	defer pm.lock.Unlock()
	podFullName := kubecontainer.GetPodFullName(pod)
	// 已由 IsMirrorPod 判断保证类型转换安全。
	if kubetypes.IsMirrorPod(pod) {
		mirrorPodUID := kubetypes.MirrorPodUID(pod.UID)
		delete(pm.mirrorPodByUID, mirrorPodUID)
		delete(pm.mirrorPodByFullName, podFullName)
		delete(pm.translationByUID, mirrorPodUID)
	} else {
		delete(pm.podByUID, kubetypes.ResolvedPodUID(pod.UID))
		delete(pm.podByFullName, podFullName)
	}
}

// GetPods 返回普通 Pod 的快照切片。
func (pm *basicManager) GetPods() []*v1.Pod {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	return podsMapToPods(pm.podByUID)
}

// GetPodsAndMirrorPods 一次性返回普通 Pod、镜像 Pod 和孤儿镜像 Pod 全名。
func (pm *basicManager) GetPodsAndMirrorPods() (allPods []*v1.Pod, allMirrorPods []*v1.Pod, orphanedMirrorPodFullnames []string) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	allPods = podsMapToPods(pm.podByUID)
	allMirrorPods = mirrorPodsMapToMirrorPods(pm.mirrorPodByUID)

	// 镜像 Pod 存在但找不到同名普通 Pod，即为孤儿。
	for podFullName := range pm.mirrorPodByFullName {
		if _, ok := pm.podByFullName[podFullName]; !ok {
			orphanedMirrorPodFullnames = append(orphanedMirrorPodFullnames, podFullName)
		}
	}
	return allPods, allMirrorPods, orphanedMirrorPodFullnames
}

// GetStaticPodToMirrorPodMap 构造静态 Pod 到镜像 Pod 的映射，
// 没有镜像 Pod 时值为 nil。
func (pm *basicManager) GetStaticPodToMirrorPodMap() map[*v1.Pod]*v1.Pod {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	staticPodsMapToMirrorPods := make(map[*v1.Pod]*v1.Pod)
	for _, pod := range podsMapToPods(pm.podByUID) {
		if kubetypes.IsStaticPod(pod) {
			staticPodsMapToMirrorPods[pod] = pm.mirrorPodByFullName[kubecontainer.GetPodFullName(pod)]
		}
	}
	return staticPodsMapToMirrorPods
}

// GetPodByUID 只查询普通 Pod 索引。
func (pm *basicManager) GetPodByUID(uid types.UID) (*v1.Pod, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	pod, ok := pm.podByUID[kubetypes.ResolvedPodUID(uid)] // 安全转换：该 map 只存放非镜像 Pod。
	return pod, ok
}

// GetPodByName 先拼出全名，再走全名索引。
func (pm *basicManager) GetPodByName(namespace, name string) (*v1.Pod, bool) {
	podFullName := kubecontainer.BuildPodFullName(name, namespace)
	return pm.GetPodByFullName(podFullName)
}

// GetPodByFullName 按全名查询普通 Pod。
func (pm *basicManager) GetPodByFullName(podFullName string) (*v1.Pod, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	pod, ok := pm.podByFullName[podFullName]
	return pod, ok
}

// TranslatePodUID 把镜像 Pod UID 翻译为静态 Pod UID。
func (pm *basicManager) TranslatePodUID(uid types.UID) kubetypes.ResolvedPodUID {
	// 类型转换是幂等的，因此可以直接把 UID 转为 ResolvedPodUID。
	if uid == "" {
		return kubetypes.ResolvedPodUID(uid)
	}

	pm.lock.RLock()
	defer pm.lock.RUnlock()
	if translated, ok := pm.translationByUID[kubetypes.MirrorPodUID(uid)]; ok {
		return translated
	}
	return kubetypes.ResolvedPodUID(uid)
}

// GetUIDTranslations 返回双向 UID 映射，调用方无需再次加锁。
func (pm *basicManager) GetUIDTranslations() (podToMirror map[kubetypes.ResolvedPodUID]kubetypes.MirrorPodUID,
	mirrorToPod map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	podToMirror = make(map[kubetypes.ResolvedPodUID]kubetypes.MirrorPodUID, len(pm.translationByUID))
	mirrorToPod = make(map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID, len(pm.translationByUID))
	// 先为所有静态 Pod 插入空映射，表示默认没有镜像 Pod。
	for uid, pod := range pm.podByUID {
		if !kubetypes.IsStaticPod(pod) {
			continue
		}
		podToMirror[uid] = ""
	}
	// 再填充真实映射。静态 Pod 没有镜像 Pod 时其值保持空字符串，
	// 这是有意为之：调用方由此可知该静态 Pod 没有镜像 Pod，而不是误用静态 Pod UID。
	for k, v := range pm.translationByUID {
		mirrorToPod[k] = v
		podToMirror[v] = k
	}
	return podToMirror, mirrorToPod
}

// IsMirrorPodOf 判断 pod 与 mirrorPod 是否互为静态/镜像 Pod。
func IsMirrorPodOf(mirrorPod, pod *v1.Pod) bool {
	// 先比较名称和命名空间，再比较配置哈希。
	if pod.Name != mirrorPod.Name || pod.Namespace != mirrorPod.Namespace {
		return false
	}
	hash, ok := getHashFromMirrorPod(mirrorPod)
	if !ok {
		return false
	}
	return hash == getPodHash(pod)
}

// podsMapToPods 把按 UID 索引的普通 Pod map 转为切片。
func podsMapToPods(UIDMap map[kubetypes.ResolvedPodUID]*v1.Pod) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(UIDMap))
	for _, pod := range UIDMap {
		pods = append(pods, pod)
	}
	return pods
}

// mirrorPodsMapToMirrorPods 把按 UID 索引的镜像 Pod map 转为切片。
func mirrorPodsMapToMirrorPods(UIDMap map[kubetypes.MirrorPodUID]*v1.Pod) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(UIDMap))
	for _, pod := range UIDMap {
		pods = append(pods, pod)
	}
	return pods
}

// GetMirrorPodByPod 按全名查找静态 Pod 对应的镜像 Pod。
func (pm *basicManager) GetMirrorPodByPod(pod *v1.Pod) (*v1.Pod, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	mirrorPod, ok := pm.mirrorPodByFullName[kubecontainer.GetPodFullName(pod)]
	return mirrorPod, ok
}

// GetPodByMirrorPod 按全名查找镜像 Pod 对应的静态 Pod。
func (pm *basicManager) GetPodByMirrorPod(mirrorPod *v1.Pod) (*v1.Pod, bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	pod, ok := pm.podByFullName[kubecontainer.GetPodFullName(mirrorPod)]
	return pod, ok
}

// GetPodAndMirrorPod 同时返回静态 Pod 与镜像 Pod；传入哪一侧由 wasMirror 区分。
func (pm *basicManager) GetPodAndMirrorPod(aPod *v1.Pod) (pod, mirrorPod *v1.Pod, wasMirror bool) {
	pm.lock.RLock()
	defer pm.lock.RUnlock()

	fullName := kubecontainer.GetPodFullName(aPod)
	if kubetypes.IsMirrorPod(aPod) {
		return pm.podByFullName[fullName], aPod, true
	}
	return aPod, pm.mirrorPodByFullName[fullName], false
}
