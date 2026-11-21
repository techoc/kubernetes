//go:build linux

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

package cadvisor

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	// Register supported container handlers.
	_ "github.com/google/cadvisor/lib/container/containerd/install"
	_ "github.com/google/cadvisor/lib/container/crio/install"
	_ "github.com/google/cadvisor/lib/container/systemd/install"

	// Register filesystem plugins needed for container stats.
	_ "github.com/google/cadvisor/lib/fs/btrfs/install"
	_ "github.com/google/cadvisor/lib/fs/nfs/install"
	_ "github.com/google/cadvisor/lib/fs/overlay/install"
	_ "github.com/google/cadvisor/lib/fs/tmpfs/install"
	_ "github.com/google/cadvisor/lib/fs/vfs/install"
	_ "github.com/google/cadvisor/lib/fs/zfs/install"

	"github.com/google/cadvisor/lib/cache/memory"
	cadvisormetrics "github.com/google/cadvisor/lib/container"
	"github.com/google/cadvisor/lib/manager"
	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/google/cadvisor/lib/utils/sysfs"
	"github.com/opencontainers/cgroups"
	cgroupfs2 "github.com/opencontainers/cgroups/fs2"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// cadvisorClient 是 cadvisor.Interface 的 Linux 实现，封装 cAdvisor 的 manager。
type cadvisorClient struct {
	imageFsInfoProvider ImageFsInfoProvider
	rootPath            string
	manager.Manager
}

// 编译期断言：cadvisorClient 必须实现 Interface 接口。
var _ Interface = new(cadvisorClient)

// TODO(vmarmol): Make configurable.
// 统计信息在内存中保留的时长。
const statsCacheDuration = 2 * time.Minute

// housekeeping 间隔与是否允许动态调整的默认值。
const maxHousekeepingInterval = 15 * time.Second
const defaultHousekeepingInterval = 10 * time.Second
const allowDynamicHousekeeping = true

// init 覆盖 cAdvisor 的默认 flag 值。
func init() {
	// 覆盖 cAdvisor 的默认 flag 值。
	flagOverrides := map[string]string{
		// 覆盖默认的 cAdvisor housekeeping 间隔。
		"housekeeping_interval": defaultHousekeepingInterval.String(),
		// 默认关闭事件存储。
		"event_storage_event_limit": "default=0",
		"event_storage_age_limit":   "default=0",
	}
	for name, defaultValue := range flagOverrides {
		if f := flag.Lookup(name); f != nil {
			f.DefValue = defaultValue
			f.Value.Set(defaultValue)
		} else {
			// flag 不存在时记录错误，便于发现 cAdvisor 版本不匹配。
			ctx := context.Background()
			klog.FromContext(ctx).Error(nil, "Expected cAdvisor flag not found", "flag", name)
		}
	}
}

// New 为 Linux 系统创建新的 cAdvisor Interface。
func New(logger klog.Logger, imageFsInfoProvider ImageFsInfoProvider, rootPath string, cgroupRoots []string, usingLegacyStats, localStorageCapacityIsolation, disableContainerDiscovery bool) (Interface, error) {
	sysFs := sysfs.NewRealSysFs()

	// 只启用 kubelet 需要的指标集合，避免不必要的采集开销。
	includedMetrics := cadvisormetrics.MetricSet{
		cadvisormetrics.CpuUsageMetrics:     struct{}{},
		cadvisormetrics.MemoryUsageMetrics:  struct{}{},
		cadvisormetrics.DiskIOMetrics:       struct{}{},
		cadvisormetrics.NetworkUsageMetrics: struct{}{},
		cadvisormetrics.ProcessMetrics:      struct{}{},
		cadvisormetrics.OOMMetrics:          struct{}{},
	}

	// PSI 指标需要内核支持，特性门控开启且实际可用时才启用。
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		if IsPsiEnabled(logger) {
			includedMetrics[cadvisormetrics.PressureMetrics] = struct{}{}
		} else {
			logger.Info("PSI support not available")
		}
	}

	// 使用旧版统计或本地存储容量隔离时，需要磁盘用量指标。
	if usingLegacyStats || localStorageCapacityIsolation {
		includedMetrics[cadvisormetrics.DiskUsageMetrics] = struct{}{}
	}

	duration := maxHousekeepingInterval
	housekeepingConfig := manager.HousekeepingConfig{
		Interval:                  &duration,
		AllowDynamic:              ptr.To(allowDynamicHousekeeping),
		DisableContainerDiscovery: disableContainerDiscovery,
	}

	// Create the cAdvisor container manager.
	m, err := manager.New(memory.New(statsCacheDuration, nil), sysFs, housekeepingConfig, includedMetrics, cgroupRoots, nil /* containerEnvMetadataWhiteList */, "" /* perfEventsFile */, time.Duration(0) /*resctrlInterval*/)
	if err != nil {
		return nil, err
	}

	// rootPath 不存在时创建，权限 0750 仅允许所有者与同组访问。
	if _, err := os.Stat(rootPath); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(path.Clean(rootPath), 0750); err != nil {
				return nil, fmt.Errorf("error creating root directory %q: %v", rootPath, err)
			}
		} else {
			return nil, fmt.Errorf("failed to Stat %q: %v", rootPath, err)
		}
	}

	return &cadvisorClient{
		imageFsInfoProvider: imageFsInfoProvider,
		rootPath:            rootPath,
		Manager:             m,
	}, nil
}

// Start 启动 cAdvisor manager。
func (cc *cadvisorClient) Start() error {
	return cc.Manager.Start()
}

// ContainerInfoV2 返回容器的 cAdvisor v2 信息。
func (cc *cadvisorClient) ContainerInfoV2(name string, options cadvisorapi.RequestOptions) (map[string]cadvisorapi.ContainerInfo, error) {
	return cc.GetContainerInfoV2(name, options)
}

// VersionInfo 返回 cAdvisor 版本信息。
func (cc *cadvisorClient) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	return cc.GetVersionInfo()
}

// MachineInfo 返回机器信息。
func (cc *cadvisorClient) MachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error) {
	return cc.GetMachineInfo()
}

// ImagesFsInfo 返回镜像文件系统的信息。
func (cc *cadvisorClient) ImagesFsInfo(ctx context.Context) (cadvisorapi.FsInfo, error) {
	label, err := cc.imageFsInfoProvider.ImageFsInfoLabel()
	if err != nil {
		return cadvisorapi.FsInfo{}, err
	}
	return cc.getFsInfo(ctx, label)
}

// IsPsiEnabled 通过 opencontainers/cgroups 库打开根 cgroup 的 cpu.pressure 文件，
// 检查宿主机是否支持 PSI（Pressure Stall Information）。该库与 cAdvisor 读取 PSI
// 值时使用的库相同。PSI 是单一内核特性（CONFIG_PSI / 启动参数 "psi="），
// 因此只检查 cpu.pressure 就足以判断 cpu、memory、io 三种资源是否支持。
func IsPsiEnabled(logger klog.Logger) bool {
	return isPsiEnabled(logger, cgroupfs2.UnifiedMountpoint, "cpu.pressure")
}

// isPsiEnabled 尝试打开指定 PSI 文件，判断 PSI 是否可用。
func isPsiEnabled(logger klog.Logger, cgroupDir, psiFile string) bool {
	f, err := cgroups.OpenFile(cgroupDir, psiFile, os.O_RDONLY)
	if err != nil {
		logger.V(4).Info("PSI not available", "dir", cgroupDir, "file", psiFile, "err", err)
		return false
	}
	_ = f.Close()
	return true
}

// RootFsInfo 返回根目录所在文件系统的信息。
func (cc *cadvisorClient) RootFsInfo() (cadvisorapi.FsInfo, error) {
	return cc.GetDirFsInfo(cc.rootPath)
}

// getFsInfo 返回指定标签文件系统的信息。
func (cc *cadvisorClient) getFsInfo(ctx context.Context, label string) (cadvisorapi.FsInfo, error) {
	res, err := cc.GetFsInfo(label)
	if err != nil {
		return cadvisorapi.FsInfo{}, err
	}
	if len(res) == 0 {
		return cadvisorapi.FsInfo{}, fmt.Errorf("failed to find information for the filesystem labeled %q", label)
	}
	// TODO(vmarmol): Handle this better when a label has more than one image filesystem.
	// 同一标签对应多个文件系统时只使用第一个。
	if len(res) > 1 {
		klog.FromContext(ctx).Info("More than one filesystem labeled. Only using the first one", "label", label, "fileSystem", res)
	}

	return res[0], nil
}

// ContainerFsInfo 返回容器文件系统的信息。
func (cc *cadvisorClient) ContainerFsInfo(ctx context.Context) (cadvisorapi.FsInfo, error) {
	label, err := cc.imageFsInfoProvider.ContainerFsInfoLabel()
	if err != nil {
		return cadvisorapi.FsInfo{}, err
	}
	return cc.getFsInfo(ctx, label)
}
