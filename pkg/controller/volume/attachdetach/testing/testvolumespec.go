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

package testing

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

// GetTestVolumeSpec 构造测试用的 volume.Spec：使用 GCE PD 作为卷源，
// 文件系统类型固定为 "fake"，访问模式为 ReadWriteOnce。
// diskName 直接作为 PDName，便于断言时识别目标卷。
func GetTestVolumeSpec(volumeName string, diskName v1.UniqueVolumeName) *volume.Spec {
	return &volume.Spec{
		Volume: &v1.Volume{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
					PDName:   string(diskName),
					FSType:   "fake",
					ReadOnly: false,
				},
			},
		},
		PersistentVolume: &v1.PersistentVolume{
			Spec: v1.PersistentVolumeSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{
					v1.ReadWriteOnce,
				},
			},
		},
	}
}

// CreateTestClient 创建并配置 attach/detach 控制器测试用的 fake clientset。
// 通过自定义 reactor 提供一组固定的 Pod/Node/CSINode 列表，并让 Create/Update
// 操作写回内存中的列表，从而模拟 API server 的状态；同时注册 fake watch 以便
// 控制器接收变更事件。
func CreateTestClient(logger klog.Logger) *fake.Clientset {
	var extraPods *v1.PodList
	var volumeAttachments *storagev1.VolumeAttachmentList
	var pvs *v1.PersistentVolumeList
	var nodes *v1.NodeList

	fakeClient := &fake.Clientset{}

	// extraPods 保存测试中动态创建的 Pod，供后续 list 一并返回，
	// 从而模拟持久化存储（fake client 默认不会把 create 的对象加入 list 结果）。
	extraPods = &v1.PodList{}
	// list pods 返回 5 个固定名称的运行中 Pod，再叠加测试动态创建的 Pod。
	fakeClient.AddReactor("list", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		obj := &v1.PodList{}
		podNamePrefix := "mypod"
		namespace := "mynamespace"
		for i := 0; i < 5; i++ {
			podName := fmt.Sprintf("%s-%d", podNamePrefix, i)
			pod := v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					UID:       types.UID(podName),
					Namespace: namespace,
					Labels: map[string]string{
						"name": podName,
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "containerName",
							Image: "containerImage",
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "volumeMountName",
									ReadOnly:  true,
									MountPath: "/mnt",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "volumeName",
							VolumeSource: v1.VolumeSource{
								GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
									PDName: "pdName",
									FSType: "ext4",
									// 设为只读是为了让迁移后的卷允许多挂载（Multi-Attach）。
									ReadOnly: true,
								},
							},
						},
					},
					NodeName: "mynode",
				},
			}
			obj.Items = append(obj.Items, pod)
		}
		obj.Items = append(obj.Items, extraPods.Items...)
		return true, obj, nil
	})
	// create pods 将对象写入 extraPods，确保后续 list pods 能够看到它。
	fakeClient.AddReactor("create", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		createAction := action.(core.CreateAction)
		pod := createAction.GetObject().(*v1.Pod)
		extraPods.Items = append(extraPods.Items, *pod)
		return true, createAction.GetObject(), nil
	})
	// nodes 预先构造节点状态：5 个节点挂载了 "lostVolumeName"，
	// 其中 "mynode" 还挂载了 "inUseVolume" 且标记为正在使用。
	nodes = &v1.NodeList{}
	nodeNamePrefix := "mynode"
	for i := 0; i < 5; i++ {
		var nodeName string
		if i != 0 {
			nodeName = fmt.Sprintf("%s-%d", nodeNamePrefix, i)
		} else {
			// 第一个节点保留裸名 "mynode"，因为测试 Pod 都调度在该节点上。
			nodeName = nodeNamePrefix
		}
		attachVolumeToNode(nodes, "lostVolumeName", nodeName, false)
	}
	attachVolumeToNode(nodes, "inUseVolume", nodeNamePrefix, true)
	// update nodes 用更新后的对象替换本地列表中的同名节点，
	// 让控制器对节点状态的修改在后续 list 中可见。
	fakeClient.AddReactor("update", "nodes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		updateAction := action.(core.UpdateAction)
		node := updateAction.GetObject().(*v1.Node)
		for index, n := range nodes.Items {
			if n.Name == node.Name {
				nodes.Items[index] = *node
			}
		}
		return true, updateAction.GetObject(), nil
	})
	// list nodes 返回内存中的节点列表副本，避免调用方修改底层切片。
	fakeClient.AddReactor("list", "nodes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		obj := &v1.NodeList{}
		obj.Items = append(obj.Items, nodes.Items...)
		return true, obj, nil
	})
	// list csinodes 为每个节点生成 CSINode，声明所有 in-tree 插件均已迁移，
	// 使控制器走 CSI 迁移后的 attach/detach 路径。
	fakeClient.AddReactor("list", "csinodes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		obj := &storagev1.CSINodeList{}
		for _, node := range nodes.Items {
			csiNode := storagev1.CSINode{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						// 自 v1.27 起所有 in-tree 插件均已迁移到 CSI，
						// 因此这里直接硬编码已迁移的插件列表。
						"storage.alpha.kubernetes.io/migrated-plugins": "kubernetes.io/aws-ebs,kubernetes.io/azure-disk,kubernetes.io/azure-file,kubernetes.io/cinder,kubernetes.io/gce-pd,kubernetes.io/vsphere-volume",
					},
					Name: node.Name,
				},
			}
			obj.Items = append(obj.Items, csiNode)
		}
		return true, obj, nil
	})
	// volumeAttachments 保存创建的 VolumeAttachment，模拟 ATTACH 记录的存储。
	volumeAttachments = &storagev1.VolumeAttachmentList{}
	fakeClient.AddReactor("list", "volumeattachments", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		obj := &storagev1.VolumeAttachmentList{}
		obj.Items = append(obj.Items, volumeAttachments.Items...)
		return true, obj, nil
	})
	// create volumeattachments 将新附件写入内存列表，供 list 时返回。
	fakeClient.AddReactor("create", "volumeattachments", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		createAction := action.(core.CreateAction)
		va := createAction.GetObject().(*storagev1.VolumeAttachment)
		volumeAttachments.Items = append(volumeAttachments.Items, *va)
		return true, createAction.GetObject(), nil
	})

	// pvs 保存测试中创建的 PersistentVolume，供 list 时返回。
	pvs = &v1.PersistentVolumeList{}
	fakeClient.AddReactor("list", "persistentvolumes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		obj := &v1.PersistentVolumeList{}
		obj.Items = append(obj.Items, pvs.Items...)
		return true, obj, nil
	})
	// create persistentvolumes 将新 PV 写入内存列表。
	fakeClient.AddReactor("create", "persistentvolumes", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		createAction := action.(core.CreateAction)
		pv := createAction.GetObject().(*v1.PersistentVolume)
		pvs.Items = append(pvs.Items, *pv)
		return true, createAction.GetObject(), nil
	})

	// 所有资源共用同一个 fake watch，以便控制器通过事件通道感知变更。
	fakeWatch := watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger})
	fakeClient.AddWatchReactor("*", core.DefaultWatchReactor(fakeWatch, nil))

	return fakeClient
}

// NewPod 构造一个最小的测试 Pod：UID 和 Name 取自参数，Namespace 与 Name 相同，
// 适用于只关心 Pod 身份标识的场景。
func NewPod(uid, name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(uid),
			Name:      name,
			Namespace: name,
		},
	}
}

// NewPodWithVolume 构造一个已调度到 nodeName、挂载 GCE PD 卷的测试 Pod。
// 卷以只读方式挂载，使迁移后的卷允许 Multi-Attach；命名空间固定为 "mynamespace"。
func NewPodWithVolume(podName, volumeName, nodeName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(podName),
			Name:      podName,
			Namespace: "mynamespace",
			Labels: map[string]string{
				"name": podName,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "containerName",
					Image: "containerImage",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "volumeMountName",
							ReadOnly:  true,
							MountPath: "/mnt",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
							PDName: "pdName",
							FSType: "ext4",
							// 设为只读是为了让迁移后的卷允许多挂载（Multi-Attach）。
							ReadOnly: true,
						},
					},
				},
			},
			NodeName: nodeName,
		},
	}
}

// NewVolumeAttachment 构造测试用 VolumeAttachment：关联 pvName 指定的 PV 与
// nodeName 指定的节点，附加器为测试插件 "test.storage.gke.io"，
// status 参数决定 .status.attached 的值。
func NewVolumeAttachment(vaName, pvName, nodeName string, status bool) *storagev1.VolumeAttachment {
	return &storagev1.VolumeAttachment{

		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID(vaName),
			Name: vaName,
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "test.storage.gke.io",
			NodeName: nodeName,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: status,
		},
	}
}

// NewPV 构造以 GCE PD（PDName 为 volumeName）为卷源的测试 PersistentVolume。
func NewPV(pvName, volumeName string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID(pvName),
			Name: pvName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
					PDName: volumeName,
				},
			},
		},
	}
}

// NewNFSPV 构造以 NFS（Server 为 volumeName）为卷源的测试 PersistentVolume。
// 与 NewPV 使用 GCE PD 不同，NFS 属于尚未迁移到 CSI 的 in-tree 卷，
// 可用于测试非迁移路径。
func NewNFSPV(pvName, volumeName string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			UID:  types.UID(pvName),
			Name: pvName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				NFS: &v1.NFSVolumeSource{
					Server: volumeName,
				},
			},
		},
	}
}

// attachVolumeToNode 将卷追加到指定节点的 status.VolumesAttached，
// 并在 inUse 为 true 时同时追加到 VolumesInUse。若节点不存在则先创建，
// 并打上 controller-managed attach 注解。
func attachVolumeToNode(nodes *v1.NodeList, volumeName, nodeName string, inUse bool) {
	// 同名节点已存在则复用，否则新建一个节点对象。
	var node *v1.Node
	for i := range nodes.Items {
		curNode := &nodes.Items[i]
		if curNode.ObjectMeta.Name == nodeName {
			node = curNode
			break
		}
	}
	if node == nil {
		nodes.Items = append(nodes.Items, v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					"name": nodeName,
				},
				Annotations: map[string]string{
					util.ControllerManagedAttachAnnotation: "true",
				},
			},
		})
		node = &nodes.Items[len(nodes.Items)-1]
	}
	// 卷名需带上测试插件前缀，才能匹配控制器的 attached volume 标识格式。
	uniqueVolumeName := v1.UniqueVolumeName(TestPluginName + "/" + volumeName)
	volumeAttached := v1.AttachedVolume{
		Name:       uniqueVolumeName,
		DevicePath: "fake/path",
	}
	node.Status.VolumesAttached = append(node.Status.VolumesAttached, volumeAttached)

	// VolumesInUse 表示卷已被节点上的 Pod 使用，与仅完成挂载的 VolumesAttached 区分。
	if inUse {
		node.Status.VolumesInUse = append(node.Status.VolumesInUse, uniqueVolumeName)
	}
}
