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

package persistentvolume

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/storage/volume"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	pvtesting "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/testing"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/util"
)

// TestControllerSync 借助伪 API Server 调用控制器真实的 add/update/delete
// Claim/Volume 方法，模拟控制器接到事件后的完整处理流程。
// 控制器没有"立即触发 syncAll"的接口，因此这里无法可靠地模拟周期性同步：
// 等待真实周期要么对时序过于敏感，要么会拖慢测试。
func TestControllerSync(t *testing.T) {
	tests := []controllerTest{
		// 单元测试集 5：像连接真实 API Server 一样测试控制器，
		// 直接调用 add/update/delete Claim/Volume 方法；所有变更也像
		// 真实控制器那样经由这些方法通知，而非手动改缓存。
		{
			// 模拟收到新的 Claim：期望控制器将其绑定到已有 Volume。
			name:            "5-2 - complete bind",
			initialVolumes:  newVolumeArray("volume5-2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume5-2", "1Gi", "uid5-2", "claim5-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   noclaims, /* added in testAddClaim5_2 */
			expectedClaims:  newClaimArray("claim5-2", "uid5-2", "1Gi", "volume5-2", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			// 自定义测试函数主动触发 add 事件，而非依赖初始数据。
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				claim := newClaim("claim5-2", "uid5-2", "1Gi", "", v1.ClaimPending, nil)
				reactor.AddClaimEvent(claim)
				return nil
			},
		},
		{
			name:            "5-2-2 - complete bind when PV and PVC both exist",
			initialVolumes:  newVolumeArray("volume5-2", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: newVolumeArray("volume5-2", "1Gi", "uid5-2", "claim5-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim5-2", "uid5-2", "1Gi", "", v1.ClaimPending, nil),
			expectedClaims:  newClaimArray("claim5-2", "uid5-2", "1Gi", "volume5-2", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  noevents,
			errors:          noerrors,
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				return nil
			},
		},
		{
			name:            "5-2-3 - complete bind when PV and PVC both exist and PV has AnnPreResizeCapacity annotation",
			initialVolumes:  volumesWithAnnotation(util.AnnPreResizeCapacity, "1Gi", newVolumeArray("volume5-2", "2Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimRetain, classEmpty)),
			expectedVolumes: volumesWithAnnotation(util.AnnPreResizeCapacity, "1Gi", newVolumeArray("volume5-2", "2Gi", "uid5-2", "claim5-2", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController)),
			initialClaims:   noclaims,
			expectedClaims:  withExpectedCapacity("1Gi", newClaimArray("claim5-2", "uid5-2", "2Gi", "volume5-2", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedEvents:  noevents,
			errors:          noerrors,
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				claim := withExpectedCapacity("2Gi", newClaimArray("claim5-2", "uid5-2", "2Gi", "", v1.ClaimPending, nil))[0]
				reactor.AddClaimEvent(claim)
				return nil
			},
		},
		{
			// 删除已绑定 Claim，期望对应 Volume 转为 Released。
			name:            "5-3 - delete claim",
			initialVolumes:  newVolumeArray("volume5-3", "10Gi", "uid5-3", "claim5-3", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			expectedVolumes: newVolumeArray("volume5-3", "10Gi", "uid5-3", "claim5-3", v1.VolumeReleased, v1.PersistentVolumeReclaimRetain, classEmpty, volume.AnnBoundByController),
			initialClaims:   newClaimArray("claim5-3", "uid5-3", "1Gi", "volume5-3", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			// 从控制器缓存中取出 Claim，触发一次真实的删除事件。
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				obj := ctrl.claims.List()[0]
				claim := obj.(*v1.PersistentVolumeClaim)
				reactor.DeleteClaimEvent(claim)
				return nil
			},
		},
		{
			// 删除已绑定 Volume，期望 Claim 被标记为 Lost。
			name:            "5-4 - delete volume",
			initialVolumes:  newVolumeArray("volume5-4", "1Gi", "uid5-4", "claim5-4", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classEmpty),
			expectedVolumes: novolumes,
			initialClaims:   newClaimArray("claim5-4", "uid5-4", "1Gi", "volume5-4", v1.ClaimBound, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedClaims:  newClaimArray("claim5-4", "uid5-4", "1Gi", "volume5-4", v1.ClaimLost, nil, volume.AnnBoundByController, volume.AnnBindCompleted),
			expectedEvents:  []string{"Warning ClaimLost"},
			errors:          noerrors,
			// 从控制器缓存中取出 Volume，触发一次真实的删除事件。
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				obj := ctrl.volumes.store.List()[0]
				volume := obj.(*v1.PersistentVolume)
				reactor.DeleteVolumeEvent(volume)
				return nil
			},
		},
		{
			// 外部 deleter 场景：删除绑定 Claim 使 Volume 转为 Released，
			// 随后从 API Server 删除 Volume，验证删除延迟指标被上报。
			name: "5-5 - delete claim and delete volume report metric",
			initialVolumes: volumesWithAnnotation(volume.AnnDynamicallyProvisioned, "gcr.io/vendor-csi",
				newVolumeArray("volume5-5", "10Gi", "uid5-5", "claim5-5", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classExternal, volume.AnnBoundByController)),
			expectedVolumes: novolumes,
			initialClaims: claimWithAnnotation(volume.AnnStorageProvisioner, "gcr.io/vendor-csi",
				newClaimArray("claim5-5", "uid5-5", "1Gi", "volume5-5", v1.ClaimBound, &classExternal, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedClaims: noclaims,
			expectedEvents: noevents,
			errors:         noerrors,
			// 先发 Claim 删除事件让 deleteClaim 清理缓存，再发 Volume 删除事件，
			// 以触发 deleteVolume 中的指标上报，顺序不可颠倒。
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				test.initialVolumes[0].Annotations[volume.AnnDynamicallyProvisioned] = "gcr.io/vendor-csi"
				obj := ctrl.claims.List()[0]
				claim := obj.(*v1.PersistentVolumeClaim)
				reactor.DeleteClaimEvent(claim)
				err := wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					return len(ctrl.claims.ListKeys()) == 0, nil
				})
				if err != nil {
					return err
				}
				// 确认 Claim 已从缓存移除后再删除 Volume，否则时序会被指标统计误解。
				volume := ctrl.volumes.store.List()[0].(*v1.PersistentVolume)
				reactor.DeleteVolumeEvent(volume)
				return nil
			},
		},
		{
			// 外部 deleter 尚未完成删除时，Volume 转 Released 且操作时间戳缓存中
			// 应保留对应条目，供后续指标统计使用。
			name:            "5-6 - delete claim and waiting for external volume deletion",
			initialVolumes:  volumesWithAnnotation(volume.AnnDynamicallyProvisioned, "gcr.io/vendor-csi", []*v1.PersistentVolume{newExternalProvisionedVolume("volume5-6", "10Gi", "uid5-6", "claim5-6", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classExternal, "fake.driver.csi", nil, volume.AnnBoundByController)}),
			expectedVolumes: volumesWithAnnotation(volume.AnnDynamicallyProvisioned, "gcr.io/vendor-csi", []*v1.PersistentVolume{newExternalProvisionedVolume("volume5-6", "10Gi", "uid5-6", "claim5-6", v1.VolumeReleased, v1.PersistentVolumeReclaimDelete, classExternal, "fake.driver.csi", nil, volume.AnnBoundByController)}),
			initialClaims: claimWithAnnotation(volume.AnnStorageProvisioner, "gcr.io/vendor-csi",
				newClaimArray("claim5-6", "uid5-6", "1Gi", "volume5-6", v1.ClaimBound, &classExternal, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedClaims: noclaims,
			expectedEvents: noevents,
			errors:         noerrors,
			// 触发 Claim 删除事件，使 deleteClaim 清理缓存并将 Volume 标记为 Released。
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				// 该卷由外部 provisioner 提供，删除需等待外部组件完成。
				obj := ctrl.claims.List()[0]
				claim := obj.(*v1.PersistentVolumeClaim)
				reactor.DeleteClaimEvent(claim)
				// 等待 Claim 从缓存清空，即 deleteClaim 已被调用。
				err := wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					return len(ctrl.claims.ListKeys()) == 0, nil
				})
				if err != nil {
					return err
				}
				// 等 volumeWorker 处理后写入删除操作时间戳，异步过程需要轮询。
				return wait.PollImmediate(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					// 时间戳缓存非空说明删除操作已被记录。
					if ctrl.operationTimestamps.Has("volume5-6") {
						return true, nil
					}
					t.Logf("missing volume5-6 from timestamp cache, will retry")
					return false, nil
				})
			},
		},
		{
			// Volume 删除事件先于 Claim 到达，Claim 会转为 Lost；
			// 此时不应上报指标，也不应写入删除操作时间戳。
			name:            "5-7 - delete volume event makes claim lost, delete claim event will not report metric",
			initialVolumes:  newVolumeArray("volume5-7", "10Gi", "uid5-7", "claim5-7", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classExternal, volume.AnnBoundByController, volume.AnnDynamicallyProvisioned),
			expectedVolumes: novolumes,
			initialClaims: claimWithAnnotation(volume.AnnStorageProvisioner, "gcr.io/vendor-csi",
				newClaimArray("claim5-7", "uid5-7", "1Gi", "volume5-7", v1.ClaimBound, &classExternal, volume.AnnBoundByController, volume.AnnBindCompleted)),
			expectedClaims: noclaims,
			expectedEvents: []string{"Warning ClaimLost"},
			errors:         noerrors,
			// 先发 Volume 删除事件使 Claim 变为 Lost，再发 Claim 删除事件，
			// 验证后到者不会再触发指标上报。
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				volume := ctrl.volumes.store.List()[0].(*v1.PersistentVolume)
				reactor.DeleteVolumeEvent(volume)
				err := wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					return len(ctrl.volumes.store.ListKeys()) == 0, nil
				})
				if err != nil {
					return err
				}

				// 等 Claim 进入 Lost 再继续，避免与下面的 DeleteClaimEvent 产生竞态。
				err = wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					obj := ctrl.claims.List()[0]
					claim := obj.(*v1.PersistentVolumeClaim)
					return claim.Status.Phase == v1.ClaimLost, nil
				})
				if err != nil {
					return err
				}

				// 再尝试删除 Claim，确认不会产生删除操作时间戳。
				obj := ctrl.claims.List()[0]
				claim := obj.(*v1.PersistentVolumeClaim)
				reactor.DeleteClaimEvent(claim)
				// 等 Claim 从缓存清空，确认 deleteClaim 已被调用。
				err = wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					return len(ctrl.claims.ListKeys()) == 0, nil
				})
				if err != nil {
					return err
				}
				// 确认时间戳缓存为空，即本场景未记录删除操作。
				if ctrl.operationTimestamps.Has("volume5-7") {
					return errors.New("failed checking timestamp cache")
				}
				return nil
			},
		},
		{
			// 删除仍在等待绑定的 Claim，应清理其 provision 操作时间戳
			// （此时 volumeRef 为空），避免遗留条目干扰指标统计。
			name:            "5-8 - delete claim cleans up operation timestamp cache for provision",
			initialVolumes:  novolumes,
			expectedVolumes: novolumes,
			initialClaims: claimWithAnnotation(volume.AnnStorageProvisioner, "gcr.io/vendor-csi",
				newClaimArray("claim5-8", "uid5-8", "1Gi", "", v1.ClaimPending, &classExternal)),
			expectedClaims: noclaims,
			expectedEvents: []string{"Normal ExternalProvisioning"},
			errors:         noerrors,
			// 模拟 Claim 删除事件，验证 deleteClaim 清理时间戳缓存。
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				// 先等 provision 时间戳写入，才有可验证的清理对象。
				err := wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					return ctrl.operationTimestamps.Has("default/claim5-8"), nil
				})
				if err != nil {
					return err
				}
				// 发出删除事件，触发 deleteClaim 清理缓存与时间戳。
				obj := ctrl.claims.List()[0]
				claim := obj.(*v1.PersistentVolumeClaim)
				reactor.DeleteClaimEvent(claim)
				// 等 Claim 从缓存清空，确认 deleteClaim 已被调用。
				err = wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
					return len(ctrl.claims.ListKeys()) == 0, nil
				})
				if err != nil {
					return err
				}
				// 确认 provision 时间戳已被清理，未遗留条目。
				if ctrl.operationTimestamps.Has("default/claim5-8") {
					return errors.New("failed checking timestamp cache")
				}
				return nil
			},
		},
		{
			// CSI 迁移关闭时移除 external-provisioner 加的 finalizer，
			// 并重新加上 in-tree 的 PV 删除保护 finalizer。
			name: "5-9 - volume has its external PV deletion protection finalizer removed as CSI migration is disabled",
			initialVolumes: volumesWithFinalizers(
				volumesWithAnnotation(volume.AnnMigratedTo, "pd.csi.storage.gke.io",
					newVolumeArray("volume-5-9", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnDynamicallyProvisioned)),
				[]string{volume.PVDeletionProtectionFinalizer},
			),
			expectedVolumes: volumesWithFinalizers(newVolumeArray("volume-5-9", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty, volume.AnnDynamicallyProvisioned), []string{volume.PVDeletionInTreeProtectionFinalizer}),
			initialClaims:   noclaims,
			expectedClaims:  noclaims,
			expectedEvents:  noevents,
			errors:          noerrors,
			test: func(ctrl *PersistentVolumeController, reactor *pvtesting.VolumeReactor, test controllerTest) error {
				return nil
			},
		},
	}
	logger, ctx := ktesting.NewTestContext(t)
	doit := func(test controllerTest) {
		// 用空的 fake client 起步，各类 watch reactor 决定其事件流。
		client := &fake.Clientset{}

		fakeVolumeWatch := watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger})
		client.PrependWatchReactor("persistentvolumes", core.DefaultWatchReactor(fakeVolumeWatch, nil))
		fakeClaimWatch := watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger})
		client.PrependWatchReactor("persistentvolumeclaims", core.DefaultWatchReactor(fakeClaimWatch, nil))
		client.PrependWatchReactor("storageclasses", core.DefaultWatchReactor(watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger}), nil))
		client.PrependWatchReactor("nodes", core.DefaultWatchReactor(watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger}), nil))
		client.PrependWatchReactor("pods", core.DefaultWatchReactor(watch.NewFakeWithOptions(watch.FakeOptions{Logger: &logger}), nil))

		informers := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
		ctrl, err := newTestController(ctx, client, informers, true)
		if err != nil {
			t.Fatalf("Test %q construct persistent volume failed: %v", test.name, err)
		}

		// fake client 缺乏真实的 StorageClass 索引，直接注入自定义 lister，
		// 供用例 5-5 的类查询使用。
		storageClasses := []*storagev1.StorageClass{
			makeStorageClass(classExternal, &modeImmediate),
		}

		storageClasses[0].Provisioner = "gcr.io/vendor-csi"
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		for _, class := range storageClasses {
			indexer.Add(class)
		}
		ctrl.classLister = storagelisters.NewStorageClassLister(indexer)

		reactor := newVolumeReactor(ctx, client, ctrl, fakeVolumeWatch, fakeClaimWatch, test.errors)
		for _, claim := range test.initialClaims {
			claim = claim.DeepCopy()
			reactor.AddClaim(claim)
			go func(claim *v1.PersistentVolumeClaim) {
				fakeClaimWatch.Add(claim)
			}(claim)
		}
		for _, volume := range test.initialVolumes {
			volume = volume.DeepCopy()
			reactor.AddVolume(volume)
			go func(volume *v1.PersistentVolume) {
				fakeVolumeWatch.Add(volume)
			}(volume)
		}

		// 控制器在独立 goroutine 运行，测试结束前必须等待其退出。
		var wg sync.WaitGroup
		defer wg.Wait()
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		informers.Start(ctx.Done())
		informers.WaitForCacheSync(ctx.Done())

		wg.Go(func() {
			ctrl.Run(ctx)
		})

		// 等初始同步完成、缓存填满后再执行测试动作，否则事件可能被丢弃。
		err = wait.Poll(10*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
			return len(ctrl.claims.ListKeys()) >= len(test.initialClaims) &&
				len(ctrl.volumes.store.ListKeys()) >= len(test.initialVolumes), nil
		})
		if err != nil {
			t.Errorf("Test %q controller sync failed: %v", test.name, err)
		}
		logger.V(4).Info("controller synced, starting test")

		// 执行用例自己的事件触发逻辑。
		err = test.test(ctrl, reactor.VolumeReactor, test)
		if err != nil {
			t.Errorf("Test %q initial test call failed: %v", test.name, err)
		}
		// 主动触发一次 resync，消除事件乱序带来的影响后再校验结果。
		ctrl.resync(ctx)

		err = reactor.waitTest(test)
		if err != nil {
			t.Errorf("Failed to run test %s: %v", test.name, err)
		}

		evaluateTestResults(ctx, ctrl, reactor.VolumeReactor, test, t)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doit(test)
		})
	}
}

// storeVersion 用给定 resourceVersion 写入缓存并校验 storeObjectUpdate 的返回值
// 与最终存入的版本，覆盖新旧版本覆盖逻辑。
func storeVersion(t *testing.T, prefix string, c cache.Store, version string, expectedReturn bool) {
	pv := newVolume("pvName", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty)
	pv.ResourceVersion = version
	logger, _ := ktesting.NewTestContext(t)
	ret, err := storeObjectUpdate(logger, c, pv, "volume")
	if err != nil {
		t.Errorf("%s: expected storeObjectUpdate to succeed, got: %v", prefix, err)
	}
	if expectedReturn != ret {
		t.Errorf("%s: expected storeObjectUpdate to return %v, got: %v", prefix, expectedReturn, ret)
	}

	// 从缓存读回，确认实际存入的版本符合预期。
	pvObj, found, err := c.GetByKey("pvName")
	if err != nil {
		t.Errorf("expected volume 'pvName' in the cache, got error instead: %v", err)
	}
	if !found {
		t.Errorf("expected volume 'pvName' in the cache but it was not found")
	}
	pv, ok := pvObj.(*v1.PersistentVolume)
	if !ok {
		t.Errorf("expected volume in the cache, got different object instead: %#v", pvObj)
	}

	if ret {
		if pv.ResourceVersion != version {
			t.Errorf("expected volume with version %s in the cache, got %s instead", version, pv.ResourceVersion)
		}
	} else {
		if pv.ResourceVersion == version {
			t.Errorf("expected volume with version other than %s in the cache, got %s instead", version, pv.ResourceVersion)
		}
	}
}

// TestControllerCache 验证 storeObjectUpdate 仅接受更新的版本：
// 重复或过期事件不得覆盖缓存，且版本号按整数而非字符串比较。
func TestControllerCache(t *testing.T) {
	// 被测缓存，使用处理删除对象的键函数。
	c := cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)

	// 首次写入新 PV。
	storeVersion(t, "Step1", c, "1", true)
	// 重复写入同一版本，不应视为更新。
	storeVersion(t, "Step2", c, "1", true)
	// 写入更高版本，应覆盖缓存。
	storeVersion(t, "Step3", c, "2", true)
	// 写入旧版本，模拟迟到的更新事件或带旧数据的周期同步，应被忽略。
	storeVersion(t, "Step4", c, "1", false)
	// "10" 按字符串小于 "2"、按整数大于 "2"，用来验证版本号按数值解析。
	storeVersion(t, "Step5", c, "10", true)
}

// TestControllerCacheParsingError 验证非法 resourceVersion 会导致更新失败，
// 避免缓存被无法比较的版本污染。
func TestControllerCacheParsingError(t *testing.T) {
	c := cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
	// 先放入一个合法版本，版本比较才有基准。
	storeVersion(t, "Step1", c, "1", true)

	pv := newVolume("pvName", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classEmpty)
	pv.ResourceVersion = "xxx"
	logger, _ := ktesting.NewTestContext(t)
	_, err := storeObjectUpdate(logger, c, pv, "volume")
	if err == nil {
		t.Errorf("Expected parsing error, got nil instead")
	}
}

// makeStorageClass 构造使用 no-provisioner 的 StorageClass，专注于绑定模式的测试。
func makeStorageClass(scName string, mode *storagev1.VolumeBindingMode) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
		},
		Provisioner:       "kubernetes.io/no-provisioner",
		VolumeBindingMode: mode,
	}
}

// makeDefaultStorageClass 构造带默认注解的 StorageClass，用于测试默认类追溯分配。
func makeDefaultStorageClass(scName string, mode *storagev1.VolumeBindingMode) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
			Annotations: map[string]string{
				util.IsDefaultStorageClassAnnotation: "true",
			},
		},
		Provisioner:       "kubernetes.io/no-provisioner",
		VolumeBindingMode: mode,
	}
}

// TestAnnealMigrationAnnotations 验证 CSI 迁移开启/关闭时 PV 与 PVC 上迁移注解的
// 增删行为，包括回滚、Beta provisioner 注解及未迁移插件不受影响等场景。
func TestAnnealMigrationAnnotations(t *testing.T) {
	// gce-pd 自 1.25 起特性锁定，用作已迁移插件；rbd 当时仍为 alpha，用作未迁移插件。
	// 待 rbd 也被迁移后，未迁移相关用例即可删除，仅保留 gce-pd 用例。
	const testPlugin = "non-migrated-plugin"
	const migratedPlugin = "kubernetes.io/gce-pd"
	const migratedDriver = "pd.csi.storage.gke.io"
	const nonmigratedPlugin = "kubernetes.io/rbd"
	const nonmigratedDriver = "rbd.csi.ceph.com"
	tests := []struct {
		name                 string
		volumeAnnotations    map[string]string
		expVolumeAnnotations map[string]string
		claimAnnotations     map[string]string
		expClaimAnnotations  map[string]string
		testMigration        bool
	}{
		{
			name:                 "migration on",
			volumeAnnotations:    map[string]string{volume.AnnDynamicallyProvisioned: migratedPlugin},
			expVolumeAnnotations: map[string]string{volume.AnnDynamicallyProvisioned: migratedPlugin, volume.AnnMigratedTo: migratedDriver},
			claimAnnotations:     map[string]string{volume.AnnStorageProvisioner: migratedPlugin},
			expClaimAnnotations:  map[string]string{volume.AnnStorageProvisioner: migratedPlugin, volume.AnnMigratedTo: migratedDriver},
		},
		{
			name:                 "migration on with Beta storage provisioner annontation",
			volumeAnnotations:    map[string]string{volume.AnnDynamicallyProvisioned: migratedPlugin},
			expVolumeAnnotations: map[string]string{volume.AnnDynamicallyProvisioned: migratedPlugin, volume.AnnMigratedTo: migratedDriver},
			claimAnnotations:     map[string]string{volume.AnnBetaStorageProvisioner: migratedPlugin},
			expClaimAnnotations:  map[string]string{volume.AnnBetaStorageProvisioner: migratedPlugin, volume.AnnMigratedTo: migratedDriver},
		},
		{
			name:                 "migration off",
			volumeAnnotations:    map[string]string{volume.AnnDynamicallyProvisioned: nonmigratedPlugin},
			expVolumeAnnotations: map[string]string{volume.AnnDynamicallyProvisioned: nonmigratedPlugin},
			claimAnnotations:     map[string]string{volume.AnnStorageProvisioner: nonmigratedPlugin},
			expClaimAnnotations:  map[string]string{volume.AnnStorageProvisioner: nonmigratedPlugin},
		},
		{
			name:                 "migration off removes migrated to (rollback)",
			volumeAnnotations:    map[string]string{volume.AnnDynamicallyProvisioned: nonmigratedPlugin, volume.AnnMigratedTo: nonmigratedDriver},
			expVolumeAnnotations: map[string]string{volume.AnnDynamicallyProvisioned: nonmigratedPlugin},
			claimAnnotations:     map[string]string{volume.AnnStorageProvisioner: nonmigratedPlugin, volume.AnnMigratedTo: nonmigratedDriver},
			expClaimAnnotations:  map[string]string{volume.AnnStorageProvisioner: nonmigratedPlugin},
		},
		{
			name:                 "migration off removes migrated to (rollback) with Beta storage provisioner annontation",
			volumeAnnotations:    map[string]string{volume.AnnDynamicallyProvisioned: nonmigratedPlugin, volume.AnnMigratedTo: nonmigratedDriver},
			expVolumeAnnotations: map[string]string{volume.AnnDynamicallyProvisioned: nonmigratedPlugin},
			claimAnnotations:     map[string]string{volume.AnnBetaStorageProvisioner: nonmigratedPlugin, volume.AnnMigratedTo: nonmigratedDriver},
			expClaimAnnotations:  map[string]string{volume.AnnBetaStorageProvisioner: nonmigratedPlugin},
		},
		{
			name:                 "migration on, other plugin not affected",
			volumeAnnotations:    map[string]string{volume.AnnDynamicallyProvisioned: testPlugin},
			expVolumeAnnotations: map[string]string{volume.AnnDynamicallyProvisioned: testPlugin},
			claimAnnotations:     map[string]string{volume.AnnStorageProvisioner: testPlugin},
			expClaimAnnotations:  map[string]string{volume.AnnStorageProvisioner: testPlugin},
		},
		{
			name:                 "not dynamically provisioned",
			volumeAnnotations:    map[string]string{},
			expVolumeAnnotations: map[string]string{},
			claimAnnotations:     map[string]string{},
			expClaimAnnotations:  map[string]string{},
			testMigration:        false,
		},
		{
			name:                 "nil annotations",
			volumeAnnotations:    nil,
			expVolumeAnnotations: nil,
			claimAnnotations:     nil,
			expClaimAnnotations:  nil,
			testMigration:        false,
		},
	}

	translator := csitrans.New()
	cmpm := csimigration.NewPluginManager(translator)
	logger, _ := ktesting.NewTestContext(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// nil 注解表示无需测试该对象类型；非 nil 时直接原地修改并检查期望结果。
			if tc.volumeAnnotations != nil {
				ann := tc.volumeAnnotations
				updateMigrationAnnotations(logger, cmpm, translator, ann, false)
				if !reflect.DeepEqual(tc.expVolumeAnnotations, ann) {
					t.Errorf("got volume annoations: %v, but expected: %v", ann, tc.expVolumeAnnotations)
				}
			}
			if tc.claimAnnotations != nil {
				ann := tc.claimAnnotations
				updateMigrationAnnotations(logger, cmpm, translator, ann, true)
				if !reflect.DeepEqual(tc.expClaimAnnotations, ann) {
					t.Errorf("got volume annoations: %v, but expected: %v", ann, tc.expVolumeAnnotations)
				}
			}

		})
	}
}

// TestModifyDeletionFinalizers 验证 CSI 迁移状态变化时 PV 删除保护 finalizer 的
// 增删逻辑，覆盖回滚、多种 finalizer 共存及不同 reclaimPolicy 等场景。
func TestModifyDeletionFinalizers(t *testing.T) {
	// 迁移关闭时需移除 external-provisioner 加的 finalizer。rbd 作为未迁移插件，
	// 其迁移被禁用；待其被迁移后需换用其他未迁移插件，若全部迁移则本测试可删除。
	// gce 自 1.25 起特性锁定，用作已迁移驱动。
	const nonmigratedDriver = "rbd.csi.ceph.com"
	const migratedPlugin = "kubernetes.io/gce-pd"
	const migratedDriver = "pd.csi.storage.gke.io"
	const customFinalizer = "test.volume.kubernetes.io/finalizer"
	tests := []struct {
		name                string
		initialVolume       *v1.PersistentVolume
		volumeAnnotations   map[string]string
		expVolumeFinalizers []string
		expModified         bool
	}{
		{
			// 未启用迁移的 CSI 卷：finalizer 应保持原样，不做修改。
			name:                "13-1 migration was never enabled, volume has the finalizer",
			initialVolume:       newExternalProvisionedVolume("volume-13-1", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, nonmigratedDriver, []string{volume.PVDeletionProtectionFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: []string{volume.PVDeletionProtectionFinalizer},
			expModified:         false,
		},
		{
			// external-provisioner 尚未同步加上新 finalizer，控制器不应擅自补写。
			name:                "13-2 migration was never enabled, volume does not have the finalizer",
			initialVolume:       newExternalProvisionedVolume("volume-13-2", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, nonmigratedDriver, nil, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: nil,
			expModified:         false,
		},
		{
			// in-tree 卷带迁移注解但外部组件尚未加删除保护 finalizer；自定义 finalizer
			// 模拟已有的 pv-protection。迁移关闭后应保留它并补回 in-tree 保护 finalizer。
			name:                "13-3 migration was disabled, volume has existing custom finalizer, does not have in-tree pv deletion protection finalizer",
			initialVolume:       newVolumeWithFinalizers("volume-13-3", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{customFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: []string{customFinalizer, volume.PVDeletionInTreeProtectionFinalizer},
			expModified:         true,
		},
		{
			name:                "13-4 migration was disabled, volume has no finalizers",
			initialVolume:       newVolumeWithFinalizers("volume-13-4", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, nil, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: []string{volume.PVDeletionInTreeProtectionFinalizer},
			expModified:         true,
		},
		{
			// 回滚场景：迁移关闭后移除外部组件加的删除保护 finalizer，
			// 改由 in-tree 的 finalizer 接管。
			name:                "13-5 migration was disabled, volume has external PV deletion finalizer",
			initialVolume:       newVolumeWithFinalizers("volume-13-5", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{volume.PVDeletionProtectionFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: []string{volume.PVDeletionInTreeProtectionFinalizer},
			expModified:         true,
		},
		{
			// 同 13-5，但存在多个 finalizer：只替换删除保护项，其余保持不动。
			name:                "13-6 migration was disabled, volume has multiple finalizers",
			initialVolume:       newVolumeWithFinalizers("volume-13-6", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{volume.PVDeletionProtectionFinalizer, customFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: []string{customFinalizer, volume.PVDeletionInTreeProtectionFinalizer},
			expModified:         true,
		},
		{
			// 迁移开启时保留外部组件加的 finalizer，并移除 in-tree 的重复保护项。
			name:                "13-7 migration is enabled, volume has both the in-tree and external PV deletion protection finalizer",
			initialVolume:       newVolumeWithFinalizers("volume-13-7", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{volume.PVDeletionProtectionFinalizer, volume.PVDeletionInTreeProtectionFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			volumeAnnotations:   map[string]string{volume.AnnDynamicallyProvisioned: migratedPlugin, volume.AnnMigratedTo: migratedDriver},
			expVolumeFinalizers: []string{volume.PVDeletionProtectionFinalizer},
			expModified:         true,
		},
		{
			// 插件级迁移特性未开启等同于迁移关闭，仍需切换为 in-tree finalizer。
			name:                "13-8 migration is enabled but plugin migration feature is disabled, volume has the external PV deletion protection finalizer",
			initialVolume:       newVolumeWithFinalizers("volume-13-8", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{volume.PVDeletionProtectionFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: []string{volume.PVDeletionInTreeProtectionFinalizer},
			expModified:         true,
		},
		{
			// 同 13-8，多个 finalizer 时仅替换删除保护项。
			name:                "13-9 migration is enabled but plugin migration feature is disabled, volume has multiple finalizers including external PV deletion protection finalizer",
			initialVolume:       newVolumeWithFinalizers("volume-13-9", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{volume.PVDeletionProtectionFinalizer, customFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: []string{customFinalizer, volume.PVDeletionInTreeProtectionFinalizer},
			expModified:         true,
		},
		{
			// 边界情况：缺少注解时无法判断是否迁移，保持 finalizer 原样。
			name:                "13-10 missing annotations but finalizers exist",
			initialVolume:       newVolumeWithFinalizers("volume-13-10", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, []string{volume.PVDeletionProtectionFinalizer}),
			expVolumeFinalizers: []string{volume.PVDeletionProtectionFinalizer},
			expModified:         false,
		},
		{
			name:                "13-11 missing annotations and finalizers",
			initialVolume:       newVolumeWithFinalizers("volume-13-11", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimDelete, classCopper, nil),
			expVolumeFinalizers: nil,
			expModified:         false,
		},
		{
			// Retain 策略下卷不会被自动删除，无需添加 in-tree 删除保护 finalizer。
			name:                "13-12 migration is disabled, volume has no finalizers, reclaimPolicy is Retain",
			initialVolume:       newVolumeWithFinalizers("volume-13-12", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classCopper, nil, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: nil,
			expModified:         false,
		},
		{
			// Recycle 策略同样不需要 in-tree 删除保护 finalizer。
			name:                "13-13 migration is disabled, volume has no finalizers, reclaimPolicy is Recycle",
			initialVolume:       newVolumeWithFinalizers("volume-13-13", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimRecycle, classCopper, nil, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: nil,
			expModified:         false,
		},
		{
			// Retain 策略下已有的 in-tree 删除保护 finalizer 也应被清理。
			name:                "13-14 migration is disabled, volume has in-tree pv deletion finalizers, reclaimPolicy is Retain",
			initialVolume:       newVolumeWithFinalizers("volume-13-14", "1Gi", "uid11-23", "claim11-23", v1.VolumeBound, v1.PersistentVolumeReclaimRetain, classCopper, []string{volume.PVDeletionInTreeProtectionFinalizer}, volume.AnnDynamicallyProvisioned, volume.AnnBoundByController),
			expVolumeFinalizers: nil,
			expModified:         true,
		},
		{
			// 静态创建的卷不是动态供给流程的一部分，不添加删除保护 finalizer。
			name:                "13-15 migration is disabled, statically provisioned PV",
			initialVolume:       newVolumeWithFinalizers("volume-13-14", "1Gi", "", "", v1.VolumeAvailable, v1.PersistentVolumeReclaimDelete, classCopper, nil),
			expVolumeFinalizers: nil,
			expModified:         false,
		},
	}

	translator := csitrans.New()
	cmpm := csimigration.NewPluginManager(translator)
	logger, _ := ktesting.NewTestContext(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.volumeAnnotations != nil {
				tc.initialVolume.SetAnnotations(tc.volumeAnnotations)
			}
			modifiedFinalizers, modified := modifyDeletionFinalizers(logger, cmpm, tc.initialVolume)
			if modified != tc.expModified {
				t.Errorf("got modified: %v, but expected: %v", modified, tc.expModified)
			}
			if !reflect.DeepEqual(tc.expVolumeFinalizers, modifiedFinalizers) {
				t.Errorf("got volume finaliers: %v, but expected: %v", modifiedFinalizers, tc.expVolumeFinalizers)
			}

		})
	}
}

// TestRetroactiveStorageClassAssignment 验证默认 StorageClass 的追溯分配：
// 仅在存在默认类且 Claim 尚未绑定、未指定存储类时才补上，多个默认类时按名称确定选择。
func TestRetroactiveStorageClassAssignment(t *testing.T) {
	tests := []struct {
		storageClasses []*storagev1.StorageClass
		tests          []controllerTest
	}{
		// 单元测试集 15：默认 StorageClass 的追溯分配测试。
		{
			storageClasses: []*storagev1.StorageClass{},
			tests: []controllerTest{
				{
					name:            "15-1 - pvc storage class is not assigned retroactively if there are no default storage classes",
					initialVolumes:  novolumes,
					expectedVolumes: novolumes,
					initialClaims:   newClaimArray("claim15-1", "uid15-1", "1Gi", "", v1.ClaimPending, nil),
					expectedClaims:  newClaimArray("claim15-1", "uid15-1", "1Gi", "", v1.ClaimPending, nil),
					expectedEvents:  noevents,
					errors:          noerrors,
					test:            testSyncClaim,
				},
			},
		},
		{
			storageClasses: []*storagev1.StorageClass{
				makeDefaultStorageClass(classGold, &modeImmediate),
				makeStorageClass(classSilver, &modeImmediate),
			},
			tests: []controllerTest{
				{
					name:            "15-3 - pvc storage class is not assigned retroactively if claim is already bound",
					initialVolumes:  novolumes,
					expectedVolumes: novolumes,
					initialClaims:   newClaimArray("claim15-3", "uid15-3", "1Gi", "test", v1.ClaimBound, &classCopper, volume.AnnBoundByController, volume.AnnBindCompleted),
					expectedClaims:  newClaimArray("claim15-3", "uid15-3", "1Gi", "test", v1.ClaimLost, &classCopper, volume.AnnBoundByController, volume.AnnBindCompleted),
					expectedEvents:  noevents,
					errors:          noerrors,
					test:            testSyncClaim,
				},
			},
		},
		{
			storageClasses: []*storagev1.StorageClass{
				makeDefaultStorageClass(classGold, &modeImmediate),
				makeStorageClass(classSilver, &modeImmediate),
			},
			tests: []controllerTest{
				{
					name:            "15-4 - pvc storage class is not assigned retroactively if claim is already bound but annotations are missing",
					initialVolumes:  novolumes,
					expectedVolumes: novolumes,
					initialClaims:   newClaimArray("claim15-4", "uid15-4", "1Gi", "test", v1.ClaimBound, &classCopper),
					expectedClaims:  newClaimArray("claim15-4", "uid15-4", "1Gi", "test", v1.ClaimPending, &classCopper),
					expectedEvents:  noevents,
					errors:          noerrors,
					test:            testSyncClaim,
				},
			},
		},
		{
			storageClasses: []*storagev1.StorageClass{
				makeDefaultStorageClass(classGold, &modeImmediate),
				makeStorageClass(classSilver, &modeImmediate),
			},
			tests: []controllerTest{
				{
					name:            "15-5 - pvc storage class is assigned retroactively if there is a default",
					initialVolumes:  novolumes,
					expectedVolumes: novolumes,
					initialClaims:   newClaimArray("claim15-5", "uid15-5", "1Gi", "", v1.ClaimPending, nil),
					expectedClaims:  newClaimArray("claim15-5", "uid15-5", "1Gi", "", v1.ClaimPending, &classGold),
					expectedEvents:  noevents,
					errors:          noerrors,
					test:            testSyncClaim,
				},
			},
		},
		{
			storageClasses: []*storagev1.StorageClass{
				makeDefaultStorageClass(classGold, &modeImmediate),
				makeDefaultStorageClass(classSilver, &modeImmediate)},
			tests: []controllerTest{
				{
					name:            "15-2 - pvc storage class is assigned retroactively if there are multiple default storage classes",
					initialVolumes:  novolumes,
					expectedVolumes: novolumes,
					initialClaims:   newClaimArray("claim15-2", "uid15-2", "1Gi", "", v1.ClaimPending, nil),
					expectedClaims:  newClaimArray("claim15-2", "uid15-2", "1Gi", "", v1.ClaimPending, &classGold),
					expectedEvents:  noevents,
					errors:          noerrors,
					test:            testSyncClaim,
				},
			},
		},
		{
			storageClasses: []*storagev1.StorageClass{
				makeDefaultStorageClass(classGold, &modeImmediate),
				makeStorageClass(classCopper, &modeImmediate),
			},
			tests: []controllerTest{
				{
					name:            "15-6 - pvc storage class is not changed if claim is not bound but already has a storage class",
					initialVolumes:  novolumes,
					expectedVolumes: novolumes,
					initialClaims:   newClaimArray("claim15-6", "uid15-6", "1Gi", "", v1.ClaimPending, &classCopper),
					expectedClaims:  newClaimArray("claim15-6", "uid15-6", "1Gi", "", v1.ClaimPending, &classCopper),
					expectedEvents:  noevents,
					errors:          noerrors,
					test:            testSyncClaim,
				},
			},
		},
		{
			storageClasses: []*storagev1.StorageClass{
				makeDefaultStorageClass(classGold, &modeImmediate),
				makeStorageClass(classCopper, &modeImmediate),
			},
			tests: []controllerTest{
				{
					name:            "15-7 - pvc storage class is not changed if claim is not bound but already set annotation \"volume.beta.kubernetes.io/storage-class\"",
					initialVolumes:  novolumes,
					expectedVolumes: novolumes,
					initialClaims:   newClaimArray("claim15-7", "uid15-7", "1Gi", "", v1.ClaimPending, nil, v1.BetaStorageClassAnnotation),
					expectedClaims:  newClaimArray("claim15-7", "uid15-7", "1Gi", "", v1.ClaimPending, nil, v1.BetaStorageClassAnnotation),
					expectedEvents:  noevents,
					errors:          noerrors,
					test:            testSyncClaim,
				},
			},
		},
	}
	_, ctx := ktesting.NewTestContext(t)
	for _, test := range tests {
		runSyncTests(t, ctx, test.tests, test.storageClasses, nil)
	}
}
