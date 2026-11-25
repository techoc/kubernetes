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

/*
auth 包定义保存 Kubernetes 客户端所需认证信息的文件格式。
通常，Kubernetes 集群创建时会将该集群管理员的认证信息放在已知位置，
也会（希望很快）在需要访问 Kubernetes API 的容器的文件树中放入已知位置的认证信息。

统一格式便于：
  - 使用多种语言实现客户端
  - 链接客户端的应用在不同认证方式（例如有的使用 SSL 客户端证书，有的不用）的集群间保持可移植性
  - 格式变化时，应用只需更新此代码

该文件格式是由 authcfg.Info 结构体序列化得到的 JSON。
其他语言的客户端库也应使用相同格式。

它不用于保存默认命名空间、输出选项等通用偏好。CLI（如 kubectl）和 UI
应使用自己的格式，也可以选择内联 authcfg.Info 类型。

authcfg.Info 仅表示文件格式，不同于用于保存创建 client.Client 选项的
client.Config。本包提供辅助函数，可根据 authcfg.Info 填充 client.Client。

Example:

	import (
	    "pkg/client"
	    "pkg/client/auth"
	)

	info, err := auth.LoadFromFile(filename)
	if err != nil {
	  // handle error
	}
	clientConfig = client.Config{}
	clientConfig.Host = "example.com:4901"
	clientConfig = info.MergeWithConfig()
	client := client.New(clientConfig)
	client.Pods(ns).List()
*/
package auth

// TODO：需要轮换令牌的机制，因此在 authcfg 更新后还需要能够重置客户端对象。
import (
	"encoding/json"
	"os"

	restclient "k8s.io/client-go/rest"
)

// Info 保存 Kubernetes API 认证配置，可从文件读取或作为 JSON 对象写入文件。
type Info struct {
	User        string
	Password    string `datapolicy:"password"`
	CAFile      string
	CertFile    string
	KeyFile     string
	BearerToken string `datapolicy:"token"`
	// Insecure 使用指针以区分“未设置”和“显式设为 false”，合并配置时才有正确语义。
	Insecure *bool
}

// LoadFromFile 从指定文件解析 Info 对象。
// 文件不存在时，返回的错误满足 os.IsNotExist(err) == true。
func LoadFromFile(path string) (*Info, error) {
	var info Info
	// 先 Stat 是为了让“文件不存在”保持 os.IsNotExist 语义，方便调用方区分可选配置。
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &info)
	if err != nil {
		return nil, err
	}
	return &info, err
}

// MergeWithConfig 返回以 Info 中对应字段的值更新的 client.Config 副本。
// 返回副本而非原地修改，避免调用方共享的 Config 被意外覆盖。
func (info Info) MergeWithConfig(c restclient.Config) (restclient.Config, error) {
	var config = c
	config.Username = info.User
	config.Password = info.Password
	config.CAFile = info.CAFile
	config.CertFile = info.CertFile
	config.KeyFile = info.KeyFile
	config.BearerToken = info.BearerToken
	if info.Insecure != nil {
		config.Insecure = *info.Insecure
	}
	return config, nil
}

// Complete 判断 Kubernetes API 认证信息是否完整。
// 只要有任一可用凭据（用户名、客户端证书或 bearer token）即视为完整。
func (info Info) Complete() bool {
	return len(info.User) > 0 ||
		len(info.CertFile) > 0 ||
		len(info.BearerToken) > 0
}
