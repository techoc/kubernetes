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

package image

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	yaml "go.yaml.in/yaml/v2"
)

// RegistryList 保存公共和私有镜像仓库
type RegistryList struct {
	PromoterE2eRegistry      string `yaml:"promoterE2eRegistry"`
	BuildImageRegistry       string `yaml:"buildImageRegistry"`
	InvalidRegistry          string `yaml:"invalidRegistry"`
	GcEtcdRegistry           string `yaml:"gcEtcdRegistry"`
	GcRegistry               string `yaml:"gcRegistry"`
	SigStorageRegistry       string `yaml:"sigStorageRegistry"`
	PrivateRegistry          string `yaml:"privateRegistry"`
	DockerLibraryRegistry    string `yaml:"dockerLibraryRegistry"`
	CloudProviderGcpRegistry string `yaml:"cloudProviderGcpRegistry"`
}

// Config 保存镜像的仓库、名称和版本
type Config struct {
	registry string
	name     string
	version  string
}

// SetRegistry 设置 Config 结构体中的镜像仓库
func (i *Config) SetRegistry(registry string) {
	i.registry = registry
}

// SetName 设置 Config 结构体中的镜像名称
func (i *Config) SetName(name string) {
	i.name = name
}

// SetVersion 设置 Config 结构体中的镜像版本
func (i *Config) SetVersion(version string) {
	i.version = version
}

func Init(repoList string) {
	registry, imageConfigs, originalImageConfigs = readRepoList(repoList)
}

func readRepoList(repoList string) (RegistryList, map[ImageID]Config, map[ImageID]Config) {
	registry := initRegistry

	if repoList == "" {
		imageConfigs, originalImageConfigs := initImageConfigs(registry)
		return registry, imageConfigs, originalImageConfigs
	}

	var fileContent []byte
	var err error
	if strings.HasPrefix(repoList, "https://") || strings.HasPrefix(repoList, "http://") {
		var b bytes.Buffer
		err = readFromURL(repoList, bufio.NewWriter(&b))
		if err != nil {
			panic(fmt.Errorf("error reading '%v' url contents: %v", repoList, err))
		}
		fileContent = b.Bytes()
	} else {
		fileContent, err = os.ReadFile(repoList)
		if err != nil {
			panic(fmt.Errorf("error reading '%v' file contents: %v", repoList, err))
		}
	}

	err = yaml.Unmarshal(fileContent, &registry)
	if err != nil {
		panic(fmt.Errorf("error unmarshalling '%v' YAML file: %v", repoList, err))
	}

	imageConfigs, originalImageConfigs := initImageConfigs(registry)

	return registry, imageConfigs, originalImageConfigs

}

// 本质上就是 curl url | writer
func readFromURL(url string, writer io.Writer) error {
	httpTransport := new(http.Transport)
	httpTransport.Proxy = http.ProxyFromEnvironment

	c := &http.Client{Transport: httpTransport}
	r, err := c.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode >= 400 {
		return fmt.Errorf("%v returned %d", url, r.StatusCode)
	}
	_, err = io.Copy(writer, r.Body)
	if err != nil {
		return err
	}
	return nil
}

var (
	initRegistry = RegistryList{
		// TODO: https://github.com/kubernetes/kubernetes/issues/130271
		// 移除 PrivateRegistry。
		PromoterE2eRegistry:      "registry.k8s.io/e2e-test-images",
		BuildImageRegistry:       "registry.k8s.io/build-image",
		InvalidRegistry:          "invalid.registry.k8s.io/invalid",
		GcEtcdRegistry:           "registry.k8s.io",
		GcRegistry:               "registry.k8s.io",
		SigStorageRegistry:       "registry.k8s.io/sig-storage",
		PrivateRegistry:          "gcr.io/k8s-authenticated-test",
		DockerLibraryRegistry:    "docker.io/library",
		CloudProviderGcpRegistry: "registry.k8s.io/cloud-provider-gcp",
	}

	registry, imageConfigs, originalImageConfigs = readRepoList(os.Getenv("KUBE_TEST_REPO_LIST"))
)

type ImageID int

const (
	// None 用于未设置/默认的镜像
	None ImageID = iota
	// AgnhostPrev 镜像
	AgnhostPrev
	// Agnhost 镜像
	Agnhost
	// AgnhostPrivate 镜像
	AgnhostPrivate
	// APIServer 镜像
	APIServer
	// AppArmorLoader 镜像
	AppArmorLoader
	// BusyBox 镜像
	BusyBox
	// DistrolessIptables 镜像
	DistrolessIptables
	// Etcd 镜像
	Etcd
	// InvalidRegistryImage 镜像
	InvalidRegistryImage
	// IpcUtils 镜像
	IpcUtils
	// GlibcDnsTesting 镜像（旧称 JessieDnsutils）
	GlibcDnsTesting
	// Kitten 镜像
	Kitten
	// Nautilus 镜像
	Nautilus
	// NFSProvisioner 镜像
	NFSProvisioner
	// Nginx 镜像
	Nginx
	// NginxNew 镜像
	NginxNew
	// NodePerfNpbEp 镜像
	NodePerfNpbEp
	// NodePerfNpbIs 镜像
	NodePerfNpbIs
	// NodePerfPytorchWideDeep 镜像
	NodePerfPytorchWideDeep
	// Nonewprivs 镜像
	Nonewprivs
	// NonRoot 以默认用户 1234 运行
	NonRoot
	// Pause - 更新这些值时，也要更新 cmd/kubelet/app/options/container_runtime.go
	// Pause 镜像
	Pause
	// Perl 镜像
	Perl
	// RegressionIssue74839 镜像
	RegressionIssue74839
	// ResourceConsumer 镜像
	ResourceConsumer
	// VolumeNFSServer 镜像
	VolumeNFSServer
	// VolumeISCSIServer 镜像
	VolumeISCSIServer
	// SampleDevicePlugin 镜像
	SampleDevicePlugin
)

func initImageConfigs(list RegistryList) (map[ImageID]Config, map[ImageID]Config) {
	configs := map[ImageID]Config{}
	configs[AgnhostPrev] = Config{list.PromoterE2eRegistry, "agnhost", "2.55"}
	configs[Agnhost] = Config{list.PromoterE2eRegistry, "agnhost", "2.66.1"}
	configs[AgnhostPrivate] = Config{list.PrivateRegistry, "agnhost", "2.6"}
	configs[APIServer] = Config{list.PromoterE2eRegistry, "sample-apiserver", "1.29.2"}
	configs[AppArmorLoader] = Config{list.PromoterE2eRegistry, "apparmor-loader", "1.4"}
	configs[BusyBox] = Config{list.PromoterE2eRegistry, "busybox", "1.37.0-2"}
	configs[DistrolessIptables] = Config{list.BuildImageRegistry, "distroless-iptables", "v0.9.9"}
	configs[Etcd] = Config{list.GcEtcdRegistry, "etcd", "3.7.0-0"}
	configs[InvalidRegistryImage] = Config{list.InvalidRegistry, "alpine", "3.1"}
	configs[IpcUtils] = Config{list.PromoterE2eRegistry, "ipc-utils", "1.4"}
	configs[GlibcDnsTesting] = Config{list.PromoterE2eRegistry, "glibc-dns-testing", "2.1.1"}
	configs[Kitten] = Config{list.PromoterE2eRegistry, "kitten", "1.8"}
	configs[Nautilus] = Config{list.PromoterE2eRegistry, "nautilus", "1.8"}
	configs[NFSProvisioner] = Config{list.SigStorageRegistry, "nfs-provisioner", "v4.0.8"}
	configs[Nginx] = Config{list.PromoterE2eRegistry, "nginx", "1.27.0-2"}
	configs[NginxNew] = Config{list.PromoterE2eRegistry, "nginx", "1.28.0-2"}
	configs[NodePerfNpbEp] = Config{list.PromoterE2eRegistry, "node-perf/npb-ep", "1.6.0"}
	configs[NodePerfNpbIs] = Config{list.PromoterE2eRegistry, "node-perf/npb-is", "1.7.0"}
	configs[NodePerfPytorchWideDeep] = Config{list.PromoterE2eRegistry, "node-perf/pytorch-wide-deep", "1.0.0"}
	configs[Nonewprivs] = Config{list.PromoterE2eRegistry, "nonewprivs", "1.4"}
	configs[NonRoot] = Config{list.PromoterE2eRegistry, "nonroot", "1.5"}
	// Pause - 更新这些值时，也要更新 cmd/kubelet/app/options/container_runtime.go
	configs[Pause] = Config{list.GcRegistry, "pause", "3.10.2"}
	configs[Perl] = Config{list.PromoterE2eRegistry, "perl", "5.26"}
	configs[RegressionIssue74839] = Config{list.PromoterE2eRegistry, "regression-issue-74839", "1.5"}
	configs[ResourceConsumer] = Config{list.PromoterE2eRegistry, "resource-consumer", "1.14"}
	configs[VolumeNFSServer] = Config{list.PromoterE2eRegistry, "volume/nfs", "1.6.0"}
	configs[VolumeISCSIServer] = Config{list.PromoterE2eRegistry, "volume/iscsi", "2.7"}
	configs[SampleDevicePlugin] = Config{list.PromoterE2eRegistry, "sample-device-plugin", "1.7"}

	// 这会增加更多配置项。这些配置项没有预定义的 ImageID 编号，
	// 但在部署 CSI 驱动时会通过 ReplaceRegistryInImageURL 使用
	// （test/e2e/storage/util/create.go）。
	appendCSIImageConfigs(configs)

	// 如果有要求，则根据输入将所有 SHA 映射为已知格式
	originalImageConfigs := configs
	if repo := os.Getenv("KUBE_TEST_REPO"); len(repo) > 0 {
		configs = GetMappedImageConfigs(originalImageConfigs, repo)
	}

	return configs, originalImageConfigs
}

// GetMappedImageConfigs 如果镜像被映射到所提供的
// 镜像仓库，则返回这些镜像的配置。
func GetMappedImageConfigs(originalImageConfigs map[ImageID]Config, repo string) map[ImageID]Config {
	configs := make(map[ImageID]Config)
	for i, config := range originalImageConfigs {
		switch i {
		case InvalidRegistryImage, AgnhostPrivate:
			// 这些镜像比较特殊，无法从云端运行——有些需要认证，
			// 另一些则不是真实的镜像。依赖这些镜像的测试
			// 在没有公网访问权限的情况下无法运行。
			configs[i] = config
			continue
		}

		// 用 ImageID、镜像 spec 的哈希（保证唯一性）构建一个新标签，
		// 并缩短 pull spec 使其“安全”，以便能放进标签中
		configs[i] = getRepositoryMappedConfig(i, config, repo)
	}
	return configs
}

var (
	reCharSafe = regexp.MustCompile(`[^\w]`)
	reDashes   = regexp.MustCompile(`-+`)
)

// getRepositoryMappedConfig 将已有镜像映射到所提供的仓库，生成一个
// 对输入配置而言唯一的标签。标签将包含 imageID、镜像 spec
// 的哈希（保证唯一性），并缩短 pull spec 使其“安全”，
// 以便能放进标签中让人类识别出其值。如果 imageID 为 None，
// 则标签中不会添加 imageID。
func getRepositoryMappedConfig(imageID ImageID, config Config, repo string) Config {
	parts := strings.SplitN(repo, "/", 2)
	registry, name := parts[0], parts[1]

	pullSpec := config.GetE2EImage()

	h := sha256.New()
	h.Write([]byte(pullSpec))
	hash := base64.RawURLEncoding.EncodeToString(h.Sum(nil))[:16]

	shortName := reCharSafe.ReplaceAllLiteralString(pullSpec, "-")
	shortName = reDashes.ReplaceAllLiteralString(shortName, "-")
	maxLength := 127 - 16 - 6 - 10
	if len(shortName) > maxLength {
		shortName = shortName[len(shortName)-maxLength:]
	}
	var version string
	if imageID == None {
		version = fmt.Sprintf("e2e-%s-%s", shortName, hash)
	} else {
		version = fmt.Sprintf("e2e-%d-%s-%s", imageID, shortName, hash)
	}

	return Config{
		registry: registry,
		name:     name,
		version:  version,
	}
}

// GetOriginalImageConfigs 返回应用任何映射规则之前的配置。
func GetOriginalImageConfigs() map[ImageID]Config {
	return originalImageConfigs
}

// GetImageConfigs 返回 imageConfigs 映射
func GetImageConfigs() map[ImageID]Config {
	return imageConfigs
}

// GetConfig 返回某个镜像的 Config 对象
func GetConfig(image ImageID) Config {
	return imageConfigs[image]
}

// GetE2EImage 返回镜像的完整 URI（包括版本）
func GetE2EImage(image ImageID) string {
	return fmt.Sprintf("%s/%s:%s", imageConfigs[image].registry, imageConfigs[image].name, imageConfigs[image].version)
}

// GetE2EImage 返回镜像的完整 URI（包括版本）
func (i *Config) GetE2EImage() string {
	return fmt.Sprintf("%s/%s:%s", i.registry, i.name, i.version)
}

// GetPauseImageName 返回带有正确版本的 pause 镜像名称
func GetPauseImageName() string {
	return GetE2EImage(Pause)
}

// ReplaceRegistryInImageURL 根据配置的仓库，
// 将镜像 URL 中的仓库替换为自定义仓库。
func ReplaceRegistryInImageURL(imageURL string) (string, error) {
	return replaceRegistryInImageURLWithList(imageURL, registry)
}

// replaceRegistryInImageURLWithList 根据给定的仓库列表，
// 将镜像 URL 中的仓库替换为自定义仓库。
func replaceRegistryInImageURLWithList(imageURL string, reg RegistryList) (string, error) {
	parts := strings.Split(imageURL, "/")
	countParts := len(parts)
	registryAndUser := strings.Join(parts[:countParts-1], "/")

	if repo := os.Getenv("KUBE_TEST_REPO"); len(repo) > 0 {
		imageID := None
		for i, v := range originalImageConfigs {
			if v.GetE2EImage() == imageURL {
				imageID = i
				break
			}
		}
		last := strings.SplitN(parts[countParts-1], ":", 2)
		if len(last) == 1 {
			return "", fmt.Errorf("image %q is required to be in an image:tag format", imageURL)
		}
		config := getRepositoryMappedConfig(imageID, Config{
			registry: parts[0],
			name:     strings.Join([]string{strings.Join(parts[1:countParts-1], "/"), last[0]}, "/"),
			version:  last[1],
		}, repo)
		return config.GetE2EImage(), nil
	}

	switch registryAndUser {
	case initRegistry.GcRegistry:
		registryAndUser = reg.GcRegistry
	case initRegistry.SigStorageRegistry:
		registryAndUser = reg.SigStorageRegistry
	case initRegistry.PrivateRegistry:
		registryAndUser = reg.PrivateRegistry
	case initRegistry.InvalidRegistry:
		registryAndUser = reg.InvalidRegistry
	case initRegistry.PromoterE2eRegistry:
		registryAndUser = reg.PromoterE2eRegistry
	case initRegistry.BuildImageRegistry:
		registryAndUser = reg.BuildImageRegistry
	case initRegistry.DockerLibraryRegistry:
		registryAndUser = reg.DockerLibraryRegistry
	case initRegistry.CloudProviderGcpRegistry:
		registryAndUser = reg.CloudProviderGcpRegistry
	default:
		if countParts == 1 {
			// 假设找到的是来自 docker hub library 的镜像
			// 例如 openjdk -> docker.io/library/openjdk
			registryAndUser = reg.DockerLibraryRegistry
			break
		}

		return "", fmt.Errorf("Registry: %s is missing in test/utils/image/manifest.go, please add the registry, otherwise the test will fail on air-gapped clusters", registryAndUser)
	}

	return fmt.Sprintf("%s/%s", registryAndUser, parts[countParts-1]), nil
}
