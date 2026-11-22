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

package volume

import (
	"sync"
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPluginName = "kubernetes.io/testPlugin"

// TestSpecSourceConverters 验证从 Volume 和 PersistentVolume 构造 Spec 时字段被正确转换。
func TestSpecSourceConverters(t *testing.T) {
	v := &v1.Volume{
		Name:         "foo",
		VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}},
	}

	converted := NewSpecFromVolume(v)
	if converted.Volume.EmptyDir == nil {
		t.Errorf("Unexpected nil EmptyDir: %#v", converted)
	}
	if v.Name != converted.Name() {
		t.Errorf("Expected %v but got %v", converted.Name(), v.Name)
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "bar"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{AWSElasticBlockStore: &v1.AWSElasticBlockStoreVolumeSource{}},
		},
	}

	converted = NewSpecFromPersistentVolume(pv, false)
	if converted.PersistentVolume.Spec.AWSElasticBlockStore == nil {
		t.Errorf("Unexpected nil AWSElasticBlockStore: %#v", converted)
	}
	if pv.Name != converted.Name() {
		t.Errorf("Expected %v but got %v", converted.Name(), pv.Name)
	}
}

// testPlugins 是满足 VolumePlugin 接口的最小测试实现。
type testPlugins struct {
}

// Init 实现 VolumePlugin 接口，测试插件无需初始化。
func (plugin *testPlugins) Init(host VolumeHost) error {
	return nil
}

// GetPluginName 返回测试插件的固定名称。
func (plugin *testPlugins) GetPluginName() string {
	return testPluginName
}

// GetVolumeName 实现 VolumePlugin 接口，测试插件返回空卷名。
func (plugin *testPlugins) GetVolumeName(spec *Spec) (string, error) {
	return "", nil
}

// CanSupport 实现 VolumePlugin 接口，测试插件支持所有 Spec。
func (plugin *testPlugins) CanSupport(spec *Spec) bool {
	return true
}

// RequiresRemount 实现 VolumePlugin 接口，测试插件不需要重挂载。
func (plugin *testPlugins) RequiresRemount(spec *Spec) bool {
	return false
}

// SupportsMountOption 实现 VolumePlugin 接口，测试插件不支持挂载选项。
func (plugin *testPlugins) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount 实现 VolumePlugin 接口，测试插件不支持 SELinux context 挂载。
func (plugin *testPlugins) SupportsSELinuxContextMount(spec *Spec) (bool, error) {
	return false, nil
}

// NewMounter 实现 VolumePlugin 接口，测试插件不提供 Mounter。
func (plugin *testPlugins) NewMounter(spec *Spec, podRef *v1.Pod) (Mounter, error) {
	return nil, nil
}

// NewUnmounter 实现 VolumePlugin 接口，测试插件不提供 Unmounter。
func (plugin *testPlugins) NewUnmounter(name string, podUID types.UID) (Unmounter, error) {
	return nil, nil
}

// ConstructVolumeSpec 实现 VolumePlugin 接口，测试插件返回空的重建结果。
func (plugin *testPlugins) ConstructVolumeSpec(volumeName, mountPath string) (ReconstructedVolume, error) {
	return ReconstructedVolume{}, nil
}

// newTestPlugin 返回包含单个测试插件的插件列表。
func newTestPlugin() []VolumePlugin {
	return []VolumePlugin{&testPlugins{}}
}

// TestVolumePluginMgrFunc 验证插件管理器能按名称和 Spec 查找到已初始化的插件。
func TestVolumePluginMgrFunc(t *testing.T) {
	vpm := VolumePluginMgr{}
	var prober DynamicPluginProber = nil // TODO (#51147) 注入 mock
	vpm.InitPlugins(newTestPlugin(), prober, nil)

	plug, err := vpm.FindPluginByName(testPluginName)
	if err != nil {
		t.Fatal("Can't find the plugin by name")
	}
	if plug.GetPluginName() != testPluginName {
		t.Errorf("Wrong name: %s", plug.GetPluginName())
	}

	_, err = vpm.FindPluginBySpec(nil)
	if err == nil {
		t.Errorf("Should return error if volume spec is nil")
	}

	volumeSpec := &Spec{}
	_, err = vpm.FindPluginBySpec(volumeSpec)
	if err != nil {
		t.Errorf("Should return test plugin if volume spec is not nil")
	}
}

// Test_ValidatePodTemplate 验证回收器 Pod 模板的校验：空卷、默认模板和缺少卷的模板。
func Test_ValidatePodTemplate(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name:         "vol",
					VolumeSource: v1.VolumeSource{},
				},
			},
		},
	}
	var want error
	if got := ValidateRecyclerPodTemplate(pod); got != want {
		t.Errorf("isPodTemplateValid(%v) returned (%v), want (%v)", pod.String(), got.Error(), want)
	}

	// 默认的回收 Pod 模板应通过校验。
	pod = NewPersistentVolumeRecyclerPodTemplate()
	want = nil
	if got := ValidateRecyclerPodTemplate(pod); got != want {
		t.Errorf("isPodTemplateValid(%v) returned (%v), want (%v)", pod.String(), got.Error(), want)
	}

	pod = &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "pv-recycler",
				},
			},
		},
	}
	// 没有卷的 Pod 应校验失败。
	if got := ValidateRecyclerPodTemplate(pod); got == nil {
		t.Errorf("isPodTemplateValid(%v) returned (%v), want (%v)", pod.String(), got, "Error: pod specification does not contain any volume(s).")
	}
}

// TestVolumePluginMultiThreaded 在多线程环境下测试 FindPluginByName/FindPluginBySpec。
// 多个线程同时调用时应仍能正确协调插件并得到一致结果，不出现插件丢失。
func TestVolumePluginMultiThreaded(t *testing.T) {
	vpm := VolumePluginMgr{}
	var prober DynamicPluginProber = &fakeProber{events: []ProbeEvent{{PluginName: testPluginName, Op: ProbeAddOrUpdate, Plugin: &testPlugins{}}}}
	err := vpm.InitPlugins([]VolumePlugin{}, prober, nil)
	require.NoError(t, err)

	volumeSpec := &Spec{}
	totalErrors := atomic.Int32{}
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := vpm.FindPluginByName(testPluginName)
			if err != nil {
				totalErrors.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(0), totalErrors.Load())
	totalErrors.Store(0)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := vpm.FindPluginBySpec(volumeSpec)
			if err != nil {
				totalErrors.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(0), totalErrors.Load())
}

// fakeProber 是只在首次 Probe 时返回预置事件的假插件探测器。
type fakeProber struct {
	events         []ProbeEvent
	firstExecution atomic.Bool
}

// Init 标记探测器进入首次执行状态。
func (prober *fakeProber) Init() error {
	prober.firstExecution.Store(true)
	return nil
}

// Probe 首次返回预置事件，之后返回空事件，模拟插件只被探测到一次。
func (prober *fakeProber) Probe() (events []ProbeEvent, err error) {
	if prober.firstExecution.CompareAndSwap(true, false) {
		return prober.events, nil
	}
	return []ProbeEvent{}, nil
}
