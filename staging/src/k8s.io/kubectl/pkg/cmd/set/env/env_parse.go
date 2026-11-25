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

package env

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
)

var argumentEnvironment = regexp.MustCompile("(?ms)^(.+)\\=(.*)$")

// IsEnvironmentArgument 检查一个字符串是否为环境变量参数，即是否匹配 "anycharacters=anycharacters" 模式。
func IsEnvironmentArgument(s string) bool {
	return argumentEnvironment.MatchString(s)
}

// SplitEnvironmentFromResources 将资源与环境变量参数分开。
// 资源必须写在前面。参数可以使用 "DASH-" 语法。
func SplitEnvironmentFromResources(args []string) (resources, envArgs []string, ok bool) {
	first := true
	for _, s := range args {
		// 该方法还必须理解环境变量删除语法，即 KEY-
		isEnv := IsEnvironmentArgument(s) || strings.HasSuffix(s, "-")
		switch {
		case first && isEnv:
			first = false
			fallthrough
		case !first && isEnv:
			envArgs = append(envArgs, s)
		case first && !isEnv:
			resources = append(resources, s)
		case !first && !isEnv:
			return nil, nil, false
		}
	}
	return resources, envArgs, true
}

// parseIntoEnvVar 将键值对列表解析为 kubernetes EnvVar。
// envVarType 用于让错误信息更贴近用户的意图。
func parseIntoEnvVar(spec []string, defaultReader io.Reader, envVarType string) ([]v1.EnvVar, []string, bool, error) {
	env := []v1.EnvVar{}
	exists := sets.New[string]()
	var remove []string
	usedStdin := false
	for _, envSpec := range spec {
		switch {
		case envSpec == "-":
			if defaultReader == nil {
				return nil, nil, usedStdin, fmt.Errorf("when '-' is used, STDIN must be open")
			}
			fileEnv, err := readEnv(defaultReader, envVarType)
			if err != nil {
				return nil, nil, usedStdin, err
			}
			env = append(env, fileEnv...)
			usedStdin = true
		case strings.Contains(envSpec, "="):
			parts := strings.SplitN(envSpec, "=", 2)
			if len(parts) != 2 {
				return nil, nil, usedStdin, fmt.Errorf("invalid %s: %v", envVarType, envSpec)
			}
			if errs := validation.IsEnvVarName(parts[0]); len(errs) != 0 {
				return nil, nil, usedStdin, fmt.Errorf("%q is not a valid key name: %s", parts[0], strings.Join(errs, ";"))
			}
			exists.Insert(parts[0])
			env = append(env, v1.EnvVar{
				Name:  parts[0],
				Value: parts[1],
			})
		case strings.HasSuffix(envSpec, "-"):
			remove = append(remove, envSpec[:len(envSpec)-1])
		default:
			return nil, nil, usedStdin, fmt.Errorf("unknown %s: %v", envVarType, envSpec)
		}
	}
	for _, removeLabel := range remove {
		if _, found := exists[removeLabel]; found {
			return nil, nil, usedStdin, fmt.Errorf("can not both modify and remove the same %s in the same command", envVarType)
		}
	}
	return env, remove, usedStdin, nil
}

// ParseEnv 解析第一个参数中的元素，查找 key=value 形式的环境变量；如果其中某个值为 "-"，它还会扫描 reader，并在第三个返回值中返回 true。
// 同一个环境变量不能在同一条命令中既被修改又被删除。
func ParseEnv(spec []string, defaultReader io.Reader) ([]v1.EnvVar, []string, bool, error) {
	return parseIntoEnvVar(spec, defaultReader, "environment variable")
}

func readEnv(r io.Reader, envVarType string) ([]v1.EnvVar, error) {
	env := []v1.EnvVar{}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		envSpec := scanner.Text()
		if pos := strings.Index(envSpec, "#"); pos != -1 {
			envSpec = envSpec[:pos]
		}

		if strings.Contains(envSpec, "=") {
			parts := strings.SplitN(envSpec, "=", 2)
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid %s: %v", envVarType, envSpec)
			}
			env = append(env, v1.EnvVar{
				Name:  parts[0],
				Value: parts[1],
			})
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return nil, err
	}

	return env, nil
}
