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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

const (
	maxReadLength = 10 * 1 << 20 // 10MB
)

// DockerConfigJSON 表示 ~/.docker/config.json 文件的内容。
// 参见 https://github.com/docker/docker/pull/12009
type DockerConfigJSON struct {
	Auths DockerConfig `json:"auths"`
	// +optional
	HTTPHeaders map[string]string `json:"HttpHeaders,omitempty"`
}

// DockerConfig 表示 docker CLI 使用的配置，记录从特定镜像仓库拉取镜像时应使用的凭证。
type DockerConfig map[string]DockerConfigEntry

// DockerConfigEntry 表示一条 Docker 仓库凭证配置。
type DockerConfigEntry struct {
	Username string
	Password string
	Email    string
	Provider DockerConfigProvider
}

var (
	preferredPathLock sync.Mutex
	preferredPath     = ""
	workingDirPath    = ""
	homeDirPath, _    = os.UserHomeDir()
	rootDirPath       = "/"
	homeJSONDirPath   = filepath.Join(homeDirPath, ".docker")
	rootJSONDirPath   = filepath.Join(rootDirPath, ".docker")

	configFileName     = ".dockercfg"
	configJSONFileName = "config.json"
)

// SetPreferredDockercfgPath 设置优先搜索的 docker 配置路径。
func SetPreferredDockercfgPath(path string) {
	preferredPathLock.Lock()
	defer preferredPathLock.Unlock()
	preferredPath = path
}

// GetPreferredDockercfgPath 返回优先搜索的 docker 配置路径。
func GetPreferredDockercfgPath() string {
	preferredPathLock.Lock()
	defer preferredPathLock.Unlock()
	return preferredPath
}

// DefaultDockercfgPaths 返回 .dockercfg 的默认搜索路径。
func DefaultDockercfgPaths() []string {
	return []string{GetPreferredDockercfgPath(), workingDirPath, homeDirPath, rootDirPath}
}

// DefaultDockerConfigJSONPaths 返回 .docker/config.json 的默认搜索路径。
func DefaultDockerConfigJSONPaths() []string {
	return []string{GetPreferredDockercfgPath(), workingDirPath, homeJSONDirPath, rootJSONDirPath}
}

// ReadDockercfgFile 从给定路径读取旧版 .dockercfg；searchPaths 为空时使用默认路径。
func ReadDockercfgFile(searchPaths []string) (cfg DockerConfig, err error) {
	if len(searchPaths) == 0 {
		searchPaths = DefaultDockercfgPaths()
	}

	// 按给定顺序逐个尝试，首个可解析的文件即命中。
	for _, configPath := range searchPaths {
		absDockerConfigFileLocation, err := filepath.Abs(filepath.Join(configPath, configFileName))
		if err != nil {
			klog.Errorf("while trying to canonicalize %s: %v", configPath, err)
			continue
		}
		klog.V(4).Infof("looking for .dockercfg at %s", absDockerConfigFileLocation)
		contents, err := os.ReadFile(absDockerConfigFileLocation)
		// 文件不存在属于正常情况，继续搜索下一个候选路径。
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			klog.V(4).Infof("while trying to read %s: %v", absDockerConfigFileLocation, err)
			continue
		}
		cfg, err := ReadDockerConfigFileFromBytes(contents)
		if err != nil {
			klog.V(4).Infof("couldn't get the config from %q contents: %v", absDockerConfigFileLocation, err)
			continue
		}

		klog.V(4).Infof("found .dockercfg at %s", absDockerConfigFileLocation)
		return cfg, nil

	}
	return nil, fmt.Errorf("couldn't find valid .dockercfg after checking in %v", searchPaths)
}

// ReadDockerConfigJSONFile 从给定路径读取 config.json；searchPaths 为空时使用默认路径。
func ReadDockerConfigJSONFile(searchPaths []string) (cfg DockerConfig, err error) {
	if len(searchPaths) == 0 {
		searchPaths = DefaultDockerConfigJSONPaths()
	}
	for _, configPath := range searchPaths {
		absDockerConfigFileLocation, err := filepath.Abs(filepath.Join(configPath, configJSONFileName))
		if err != nil {
			klog.Errorf("while trying to canonicalize %s: %v", configPath, err)
			continue
		}
		klog.V(4).Infof("looking for %s at %s", configJSONFileName, absDockerConfigFileLocation)
		cfg, err = ReadSpecificDockerConfigJSONFile(absDockerConfigFileLocation)
		if err != nil {
			if !os.IsNotExist(err) {
				klog.V(4).Infof("while trying to read %s: %v", absDockerConfigFileLocation, err)
			}
			continue
		}
		klog.V(4).Infof("found valid %s at %s", configJSONFileName, absDockerConfigFileLocation)
		return cfg, nil
	}
	return nil, fmt.Errorf("couldn't find valid %s after checking in %v", configJSONFileName, searchPaths)

}

// ReadSpecificDockerConfigJSONFile 从指定文件路径读取 config.json。
func ReadSpecificDockerConfigJSONFile(filePath string) (cfg DockerConfig, err error) {
	var contents []byte

	if contents, err = os.ReadFile(filePath); err != nil {
		return nil, err
	}
	return readDockerConfigJSONFileFromBytes(contents)
}

// ReadDockerConfigFile 从默认路径读取 Docker 凭证配置。
func ReadDockerConfigFile() (cfg DockerConfig, err error) {
	if cfg, err := ReadDockerConfigJSONFile(nil); err == nil {
		return cfg, nil
	}
	// 新版 config.json 不可用，回退到旧版 .dockercfg。
	return ReadDockercfgFile(nil)
}

// HTTPError 将非 StatusOK 的响应码包装为 error。
type HTTPError struct {
	StatusCode int
	URL        string
}

// Error 实现 error 接口。
func (he *HTTPError) Error() string {
	return fmt.Sprintf("http status code: %d while fetching url %s",
		he.StatusCode, he.URL)
}

// ReadURL 从给定 URL 读取内容。
func ReadURL(url string, client *http.Client, header *http.Header) (body []byte, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if header != nil {
		req.Header = *header
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		klog.V(2).InfoS("Failed to read URL", "statusCode", resp.StatusCode, "URL", url)
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        url,
		}
	}

	// 限制读取长度，避免远端返回超大响应耗尽内存。
	limitedReader := &io.LimitedReader{R: resp.Body, N: maxReadLength}
	contents, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, err
	}

	// 读满上限说明响应可能未结束，按超限处理。
	if limitedReader.N <= 0 {
		return nil, errors.New("the read limit is reached")
	}

	return contents, nil
}

// ReadDockerConfigFileFromBytes 从字节串解析旧版 Docker 凭证配置。
func ReadDockerConfigFileFromBytes(contents []byte) (cfg DockerConfig, err error) {
	if err = json.Unmarshal(contents, &cfg); err != nil {
		return nil, errors.New("error occurred while trying to unmarshal json")
	}
	return
}

// readDockerConfigJSONFileFromBytes 从字节串解析 config.json 中的 auths 字段。
func readDockerConfigJSONFileFromBytes(contents []byte) (cfg DockerConfig, err error) {
	var cfgJSON DockerConfigJSON
	if err = json.Unmarshal(contents, &cfgJSON); err != nil {
		return nil, errors.New("error occurred while trying to unmarshal json")
	}
	cfg = cfgJSON.Auths
	return
}

// dockerConfigEntryWithAuth 仅在反序列化时承载各字段，尤其是 base64 编码的 Auth。
type dockerConfigEntryWithAuth struct {
	// +optional
	Username string `json:"username,omitempty"`
	// +optional
	Password string `json:"password,omitempty"`
	// +optional
	Email string `json:"email,omitempty"`
	// +optional
	Auth string `json:"auth,omitempty"`
}

// UnmarshalJSON 实现 json.Unmarshaler 接口。
func (ident *DockerConfigEntry) UnmarshalJSON(data []byte) error {
	var tmp dockerConfigEntryWithAuth
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	// 先取明文字段，若存在 Auth 则由它覆盖。
	ident.Username = tmp.Username
	ident.Password = tmp.Password
	ident.Email = tmp.Email

	if len(tmp.Auth) == 0 {
		return nil
	}

	ident.Username, ident.Password, err = decodeDockerConfigFieldAuth(tmp.Auth)
	return err
}

// MarshalJSON 实现 json.Marshaler 接口。
func (ident DockerConfigEntry) MarshalJSON() ([]byte, error) {
	toEncode := dockerConfigEntryWithAuth{ident.Username, ident.Password, ident.Email, ""}
	toEncode.Auth = encodeDockerConfigFieldAuth(ident.Username, ident.Password)

	return json.Marshal(toEncode)
}

// decodeDockerConfigFieldAuth 把 dockercfg 的 auth 字段解码为用户名和密码，格式为 base64(<username>:<password>)。
func decodeDockerConfigFieldAuth(field string) (username, password string, err error) {

	var decoded []byte

	// 两种编码分别对应带填充和不带填充的字符串，需按输入形态选择。
	if strings.HasSuffix(strings.TrimSpace(field), "=") {
		// 带填充的数据。
		decoded, err = base64.StdEncoding.DecodeString(field)
	} else {
		// 不带填充的数据。
		decoded, err = base64.RawStdEncoding.DecodeString(field)
	}

	if err != nil {
		return
	}

	// 密码本身可能含冒号，因此只按第一个冒号切分。
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("unable to parse auth field, must be formatted as base64(username:password)")
		return
	}

	username = parts[0]
	password = parts[1]

	return
}

// encodeDockerConfigFieldAuth 把用户名和密码编码为 base64 格式的 auth 字段。
func encodeDockerConfigFieldAuth(username, password string) string {
	fieldValue := username + ":" + password

	return base64.StdEncoding.EncodeToString([]byte(fieldValue))
}
