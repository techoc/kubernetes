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

// Package verflag defines utility functions to handle command line flags
// related to version of Kubernetes.
//
// Package verflag 负责 --version 相关命令行标志：
// 既可打印版本后退出，也允许在启动时覆盖上报的 git 版本，
// 后者供测试或自定义构建使用。
package verflag

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	flag "github.com/spf13/pflag"

	"k8s.io/component-base/version"
)

// versionValue 是 --version 的标志值，既可以是布尔语义（打印后退出），
// 也可以是 "raw"（打印结构体细节）或一个 vX.Y.Z 形式的版本覆盖。
type versionValue string

const (
	VersionFalse versionValue = "false"
	VersionTrue  versionValue = "true"
	VersionRaw   versionValue = "raw"
)

const strRawVersion string = "raw"

// IsBoolFlag 让 pflag 接受不带值的 --version，等价于 --version=true。
func (v *versionValue) IsBoolFlag() bool {
	return true
}

// Get 返回标志当前值的副本，供 pflag 反射使用。
func (v *versionValue) Get() interface{} {
	return versionValue(*v)
}

// Set 解析标志值，优先级：raw > vX.Y.Z 覆盖 > 布尔值。
// 覆盖版本会同时写入全局 dynamicGitVersion，因此后续 version.Get() 会反映它。
func (v *versionValue) Set(s string) error {
	if s == strRawVersion {
		*v = VersionRaw
		return nil
	}

	if strings.HasPrefix(s, "v") {
		// 覆盖必须能被 version.SetDynamicVersion 校验（主/次/补丁版本需与构建一致），
		// 校验失败时保持原值不变，把错误交给 pflag 报告。
		err := version.SetDynamicVersion(s)
		if err == nil {
			*v = versionValue(s)
		}
		return err
	}

	boolVal, err := strconv.ParseBool(s)
	if err == nil {
		if boolVal {
			*v = VersionTrue
		} else {
			*v = VersionFalse
		}
	}
	return err
}

// String 实现 pflag.Value。
func (v *versionValue) String() string {
	return string(*v)
}

// The type of the flag as required by the pflag.Value interface
func (v *versionValue) Type() string {
	return "version"
}

// VersionVar 把 versionValue 绑定到指定 FlagSet，并设置 Value 相关默认值。
func VersionVar(p *versionValue, name string, value versionValue, usage string) {
	*p = value
	flag.Var(p, name, usage)
	// "--version" will be treated as "--version=true"
	flag.Lookup(name).NoOptDefVal = "true"
}

// Version 是 VersionVar 的便捷形式：分配变量、注册并返回指针。
func Version(name string, value versionValue, usage string) *versionValue {
	p := new(versionValue)
	VersionVar(p, name, value, usage)
	return p
}

const versionFlagName = "version"

var (
	versionFlag = Version(versionFlagName, VersionFalse, "--version, --version=raw prints version information and quits; --version=vX.Y.Z... sets the reported version")
	programName = "Kubernetes"
)

// AddFlags registers this package's flags on arbitrary FlagSets, such that they point to the
// same value as the global flags.
//
// AddFlags 把全局 --version 标志挂到给定 FlagSet 上，多个 FlagSet 共享同一份状态。
func AddFlags(fs *flag.FlagSet) {
	fs.AddFlag(flag.Lookup(versionFlagName))
}

// variables for unit testing PrintAndExitIfRequested
var (
	output = io.Writer(os.Stdout)
	exit   = os.Exit
)

// PrintAndExitIfRequested will check if --version or --version=raw was passed
// and, if so, print the version and exit.
//
// raw 模式用 %#v 打印完整结构，便于脚本解析所有字段；普通模式只打印一行
// "程序名 版本"。遇到这两个值以外的值（包括被覆盖的 vX.Y.Z）不会退出。
func PrintAndExitIfRequested() {
	if *versionFlag == VersionRaw {
		fmt.Fprintf(output, "%#v\n", version.Get())
		exit(0)
	} else if *versionFlag == VersionTrue {
		fmt.Fprintf(output, "%s %s\n", programName, version.Get())
		exit(0)
	}
}
