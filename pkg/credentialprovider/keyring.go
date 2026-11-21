/*
Copyright 2014 The Kubernetes Authors.

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

package credentialprovider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/url"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
)

// DockerKeyring 保存一组镜像仓库凭证，并维护按仓库端点（主机加可选路径）的反向索引。
// 索引有两个作用：端点可能重叠，需优先匹配更具体的路径；直接遍历 map 的顺序不可预测。
type DockerKeyring interface {
	Lookup(image string) ([]TrackedAuthConfig, bool)
}

// BasicDockerKeyring 是基于 map 的简单 DockerKeyring 实现。
type BasicDockerKeyring struct {
	index []string
	creds map[string][]TrackedAuthConfig
}

// providersDockerKeyring 根据一组 DockerConfigProvider 动态生成凭证。
type providersDockerKeyring struct {
	Providers []DockerConfigProvider
}

// TrackedAuthConfig 在 AuthConfig 之上记录凭证来源。
type TrackedAuthConfig struct {
	AuthConfig
	AuthConfigHash string

	Source *CredentialSource
}

// NewTrackedAuthConfig 把来源信息附加到 AuthConfig 上，并计算其哈希。
// 只有启用 KubeletEnsureSecretPulledImages 时才记录来源与哈希。
func NewTrackedAuthConfig(c *AuthConfig, src *CredentialSource) *TrackedAuthConfig {
	if c == nil {
		panic("cannot construct TrackedAuthConfig with a nil AuthConfig")
	}

	authConfig := &TrackedAuthConfig{
		AuthConfig: *c,
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletEnsureSecretPulledImages) {
		authConfig.Source = src
		authConfig.AuthConfigHash = hashAuthConfig(c)
	}
	return authConfig
}

// CredentialSource 描述凭证来自 Secret 还是 ServiceAccount。
type CredentialSource struct {
	Secret         *SecretCoordinates
	ServiceAccount *ServiceAccountCoordinates
}

// SecretCoordinates 定位提供凭证的 Secret。
type SecretCoordinates struct {
	UID       string
	Namespace string
	Name      string
}

// ServiceAccountCoordinates 定位提供凭证的 ServiceAccount。
type ServiceAccountCoordinates struct {
	UID       string
	Namespace string
	Name      string
}

// AuthConfig 保存连接镜像仓库所需的认证信息，对应 docker 的 AuthConfig 类型。
type AuthConfig struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Auth     string `json:"auth,omitempty"`

	// Email 是与用户名关联的可选字段，已废弃，后续 docker 版本会移除。
	Email string `json:"email,omitempty"`

	ServerAddress string `json:"serveraddress,omitempty"`

	// IdentityToken 用于认证用户并换取仓库访问令牌。
	IdentityToken string `json:"identitytoken,omitempty"`

	// RegistryToken 是直接发给仓库的 bearer 令牌。
	RegistryToken string `json:"registrytoken,omitempty"`
}

// Add 把 Docker 配置写入 keyring，并附上来源 src；节点全局可用的凭证 src 为 nil。
func (dk *BasicDockerKeyring) Add(src *CredentialSource, cfgs DockerConfig) {
	if dk.index == nil {
		dk.index = make([]string, 0)
		dk.creds = make(map[string][]TrackedAuthConfig)
	}
	for repository, dockerAuthCfg := range cfgs {
		creds := AuthConfig{
			Username: dockerAuthCfg.Username,
			Password: dockerAuthCfg.Password,
			Email:    dockerAuthCfg.Email,
		}

		// 配置项允许省略 scheme，统一补 https 后再解析。
		if !strings.HasPrefix(repository, "https://") && !strings.HasPrefix(repository, "http://") {
			repository = "https://" + repository
		}
		parsed, err := url.Parse(repository)
		if err != nil {
			klog.Errorf("Entry %q in dockercfg invalid (%v), ignoring", repository, err)
			continue
		}

		// docker 客户端既支持精确路径（foo.bar.com/namespace），也支持仅主机名（foo.bar.com），
		// 并把 /v2/、/v1/ 视作等价于主机名（见 docker/registry/auth.go 的 ResolveAuthConfig）。
		effectivePath := parsed.Path
		if strings.HasPrefix(effectivePath, "/v2/") || strings.HasPrefix(effectivePath, "/v1/") {
			effectivePath = effectivePath[3:]
		}
		var key string
		if (len(effectivePath) > 0) && (effectivePath != "/") {
			key = parsed.Host + effectivePath
		} else {
			key = parsed.Host
		}
		trackedCreds := NewTrackedAuthConfig(&creds, src)

		dk.creds[key] = append(dk.creds[key], *trackedCreds)
		dk.index = append(dk.index, key)
	}

	eliminateDupes := sets.NewString(dk.index...)
	dk.index = eliminateDupes.List()

	// 索引按逆序排列，确保更具体的路径优先匹配；例如 quay.io/coreos 应先于 quay.io 命中。
	sort.Sort(sort.Reverse(sort.StringSlice(dk.index)))
}

const (
	defaultRegistryHost = "index.docker.io"
	defaultRegistryKey  = defaultRegistryHost + "/v1/"
)

// isDefaultRegistryMatch 根据镜像名判断其是否来自默认仓库 DockerHub。
func isDefaultRegistryMatch(image string) bool {
	parts := strings.SplitN(image, "/", 2)

	if len(parts[0]) == 0 {
		return false
	}

	if len(parts) == 1 {
		// 例如 library/ubuntu。
		return true
	}

	if parts[0] == "docker.io" || parts[0] == "index.docker.io" {
		// docker.io/image 与 index.docker.io/image 都视为默认仓库。
		return true
	}

	// 参见 http://blog.docker.com/2013/07/how-to-use-your-own-registry/
	// Docker 用 "." 或 ":" 判断第一段是仓库地址而非用户名。
	return !strings.ContainsAny(parts[0], ".:")
}

// ParseSchemelessURL 解析无 scheme 的 URL：先补 https 让 url.Parse 接受，再清除 scheme。
func ParseSchemelessURL(schemelessURL string) (*url.URL, error) {
	parsed, err := url.Parse("https://" + schemelessURL)
	if err != nil {
		return nil, err
	}
	// 清除临时补上的 scheme。
	parsed.Scheme = ""
	return parsed, nil
}

// SplitURL 把主机名拆成各段，并返回端口。
func SplitURL(url *url.URL) (parts []string, port string) {
	host, port, err := net.SplitHostPort(url.Host)
	if err != nil {
		// 没有端口时按整个 Host 处理。
		host, port = url.Host, ""
	}
	return strings.Split(host, "."), port
}

// URLsMatchStr 是 URLsMatch 的字符串版本包装。
func URLsMatchStr(glob string, target string) (bool, error) {
	globURL, err := ParseSchemelessURL(glob)
	if err != nil {
		return false, err
	}
	targetURL, err := ParseSchemelessURL(target)
	if err != nil {
		return false, err
	}
	return URLsMatch(globURL, targetURL)
}

// URLsMatch 判断 targetURL 是否匹配 globURL（主机名支持通配符）。
//
// 例如：
//
//	globURL=*.docker.io, targetURL=blah.docker.io => 匹配
//	globURL=*.docker.io, targetURL=not.right.io   => 不匹配
//
// 端口和路径暂不支持通配符。
func URLsMatch(globURL *url.URL, targetURL *url.URL) (bool, error) {
	globURLParts, globPort := SplitURL(globURL)
	targetURLParts, targetPort := SplitURL(targetURL)
	if globPort != targetPort {
		// 端口不同。
		return false, nil
	}
	if len(globURLParts) != len(targetURLParts) {
		// 主机段数不同。
		return false, nil
	}
	if !strings.HasPrefix(targetURL.Path, globURL.Path) {
		// 凭证路径必须是目标路径的前缀。
		return false, nil
	}
	for k, globURLPart := range globURLParts {
		targetURLPart := targetURLParts[k]
		matched, err := filepath.Match(globURLPart, targetURLPart)
		if err != nil {
			return false, err
		}
		if !matched {
			// 某段通配符不匹配。
			return false, nil
		}
	}
	// 全部匹配。
	return true, nil
}

// Lookup 按镜像名查找凭证；可能返回多条候选凭证以支持凭证轮换。
func (dk *BasicDockerKeyring) Lookup(image string) ([]TrackedAuthConfig, bool) {
	// 遍历有序索引而非 map，避免匹配顺序不可预测。
	ret := []TrackedAuthConfig{}
	for _, k := range dk.index {
		// 配置中允许写 scheme，但 Add 已去掉，因此两边都是无 scheme 的 URL。
		if matched, _ := URLsMatchStr(k, image); matched {
			ret = append(ret, dk.creds[k]...)
		}
	}

	if len(ret) > 0 {
		return ret, true
	}

	// 没有显式命中时，若镜像属于默认仓库则回退到默认凭证。
	if isDefaultRegistryMatch(image) {
		if auth, ok := dk.creds[defaultRegistryHost]; ok {
			return auth, true
		}
	}

	return []TrackedAuthConfig{}, false
}

// Lookup 按镜像名查找凭证，逐个 provider 取配置后交由 BasicDockerKeyring 匹配。
func (dk *providersDockerKeyring) Lookup(image string) ([]TrackedAuthConfig, bool) {
	keyring := &BasicDockerKeyring{}

	// 每次查询都重新调用 provider，凭证变更无需重建 keyring。
	for _, p := range dk.Providers {
		keyring.Add(nil, p.Provide(image))
	}

	return keyring.Lookup(image)
}

// FakeKeyring 是测试用的固定凭证 keyring。
type FakeKeyring struct {
	auth []TrackedAuthConfig
	ok   bool
}

// Lookup 返回预设的假凭证及命中标志。
func (f *FakeKeyring) Lookup(image string) ([]TrackedAuthConfig, bool) {
	return f.auth, f.ok
}

// UnionDockerKeyring 把查询委托给一组 keyring 并汇总结果。
type UnionDockerKeyring []DockerKeyring

// Lookup 聚合所有子 keyring 的凭证。
func (k UnionDockerKeyring) Lookup(image string) ([]TrackedAuthConfig, bool) {
	authConfigs := []TrackedAuthConfig{}
	// 收集全部命中项，让调用方按优先级自行选择。
	for _, subKeyring := range k {
		if subKeyring == nil {
			continue
		}

		currAuthResults, _ := subKeyring.Lookup(image)
		authConfigs = append(authConfigs, currAuthResults...)
	}

	return authConfigs, (len(authConfigs) > 0)
}

// hashAuthConfig 计算凭证的 SHA256，用于识别凭证是否变化。
func hashAuthConfig(creds *AuthConfig) string {
	credBytes, err := json.Marshal(creds)
	if err != nil {
		return ""
	}

	hash := sha256.New()
	hash.Write([]byte(credBytes))
	return hex.EncodeToString(hash.Sum(nil))
}
