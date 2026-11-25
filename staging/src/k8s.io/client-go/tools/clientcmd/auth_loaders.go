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

package clientcmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"

	clientauth "k8s.io/client-go/tools/auth"
)

// AuthLoader 用于构造 clientauth.Info 对象。
// AuthLoader 把"从磁盘读取或交互式询问凭据"的差异抽象出来，供 kubeconfig 加载流程替换实现。
type AuthLoader interface {
	// LoadAuth 接收配置文件路径，并可以做任何必要的事来返回有效的 clientauth.Info
	LoadAuth(path string) (*clientauth.Info, error)
}

// AuthLoader 的默认实现。
type defaultAuthLoader struct{}

// defaultAuthLoader 的 LoadAuth 只是委托给 clientauth.LoadFromFile。
func (*defaultAuthLoader) LoadAuth(path string) (*clientauth.Info, error) {
	return clientauth.LoadFromFile(path)
}

// PromptingAuthLoader 在凭据文件不存在时通过交互式提示获取用户名/密码，并据此创建文件。
type PromptingAuthLoader struct {
	reader io.Reader
}

// LoadAuth 从文件路径解析 AuthInfo 对象；文件不存在时提示用户并创建文件。
func (a *PromptingAuthLoader) LoadAuth(path string) (*clientauth.Info, error) {
	// 若无凭据文件，则提示输入用户名/密码并写入文件。
	if _, err := os.Stat(path); os.IsNotExist(err) {
		authPtr, err := a.Prompt()
		if err != nil {
			return nil, err
		}
		auth := *authPtr
		data, err := json.Marshal(auth)
		if err != nil {
			return &auth, err
		}
		// 0600 权限：凭据文件只允许当前用户读写。
		err = os.WriteFile(path, data, 0600)
		return &auth, err
	}
	authPtr, err := clientauth.LoadFromFile(path)
	if err != nil {
		return nil, err
	}
	return authPtr, nil
}

// Prompt 从 reader 读取用户名和密码。
// 用户名从注入的 reader 读取（便于测试），密码则强制从终端读取且不回显。
func (a *PromptingAuthLoader) Prompt() (*clientauth.Info, error) {
	var err error
	auth := &clientauth.Info{}
	auth.User, err = promptForString("Username", a.reader, true)
	if err != nil {
		return nil, err
	}
	auth.Password, err = promptForString("Password", nil, false)
	if err != nil {
		return nil, err
	}
	return auth, nil
}

// promptForString 提示用户输入字段；show 为 false 时使用 ReadPassword 避免密码回显。
func promptForString(field string, r io.Reader, show bool) (result string, err error) {
	fmt.Printf("Please enter %s: ", field)
	if show {
		_, err = fmt.Fscan(r, &result)
	} else {
		var data []byte
		if term.IsTerminal(int(os.Stdin.Fd())) {
			data, err = term.ReadPassword(int(os.Stdin.Fd()))
			result = string(data)
		} else {
			// 非终端环境下无法安全地读取密码，直接报错而不是退化为明文读取。
			return "", fmt.Errorf("error reading input for %s", field)
		}
	}
	return result, err
}

// NewPromptingAuthLoader 返回一个 AuthLoader：从文件路径解析 AuthInfo 对象；
// 文件不存在时提示用户并创建文件。
func NewPromptingAuthLoader(reader io.Reader) *PromptingAuthLoader {
	return &PromptingAuthLoader{reader}
}

// NewDefaultAuthLoader 返回 AuthLoader 的默认实现，只从配置文件读取。
func NewDefaultAuthLoader() AuthLoader {
	return &defaultAuthLoader{}
}
