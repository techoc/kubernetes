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

package config

import (
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

type navigationSteps struct {
	steps            []navigationStep
	currentStepIndex int
}

type navigationStep struct {
	stepValue string
	stepType  reflect.Type
}

func newNavigationSteps(path string) (*navigationSteps, error) {
	steps := []navigationStep{}
	individualParts := strings.Split(path, ".")

	currType := reflect.TypeOf(clientcmdapi.Config{})
	currPartIndex := 0
	for currPartIndex < len(individualParts) {
		switch currType.Kind() {
		case reflect.Map:
			// 在 map 中时，需要定位一个名称。该名称可能包含点，因此我们需要知道 map 值类型允许哪些 token
			// 例如，可能有这样的 set 请求：`set clusters.10.10.12.56.insecure-skip-tls-verify true`。进入此分支时，
			// 各步骤分别为 10、10、12、56、insecure-skip-tls-verify。名称是 "10.10.12.56"，因此我们要收集所有这些部分，
			// 将它们存储为单个步骤。为此，需要确定在 map 键名之后哪些 token 集合可以构成合法步骤
			// 下面这段反射代码获取 map 值的类型，用该类型查找合法的 tag 集合。这些合法 tag 用于
			// 遍历剩余部分列表，直到找到匹配的合法 tag 或字符串末尾。该名称用于消耗所有已使用的部分。
			mapValueType := currType.Elem().Elem()
			mapValueOptions, err := getPotentialTypeValues(mapValueType)
			if err != nil {
				return nil, err
			}
			nextPart := findNameStep(individualParts[currPartIndex:], sets.KeySet(mapValueOptions))

			steps = append(steps, navigationStep{nextPart, mapValueType})
			currPartIndex += len(strings.Split(nextPart, "."))
			currType = mapValueType

		case reflect.Struct:
			nextPart := individualParts[currPartIndex]

			options, err := getPotentialTypeValues(currType)
			if err != nil {
				return nil, err
			}
			fieldType, exists := options[nextPart]
			if !exists {
				return nil, fmt.Errorf("unable to parse %v after %v at %v", path, steps, currType)
			}

			steps = append(steps, navigationStep{nextPart, fieldType})
			currPartIndex += len(strings.Split(nextPart, "."))
			currType = fieldType
		default:
			return nil, fmt.Errorf("unable to parse one or more field values of %v", path)
		}
	}

	return &navigationSteps{steps, 0}, nil
}

func (s *navigationSteps) pop() navigationStep {
	if s.moreStepsRemaining() {
		s.currentStepIndex++
		return s.steps[s.currentStepIndex-1]
	}
	return navigationStep{}
}

func (s *navigationSteps) moreStepsRemaining() bool {
	return len(s.steps) > s.currentStepIndex
}

// findNameStep 接收部分列表和一组可在名称之后使用的有效 tag。然后遍历该列表，
// 直到找到有效的 "next" tag 或到达列表末尾，再将各个部分重新拼接成名称
func findNameStep(parts []string, typeOptions sets.Set[string]) string {
	if len(parts) == 0 {
		return ""
	}

	numberOfPartsInStep := findKnownValue(parts[1:], typeOptions) + 1
	// 如果未找到已知值，则整个部分必定是一个名称
	if numberOfPartsInStep == 0 {
		numberOfPartsInStep = len(parts)
	}
	nextParts := parts[0:numberOfPartsInStep]

	return strings.Join(nextParts, ".")
}

// getPotentialTypeValues 接收一个类型，并查找序列化时用于表示其字段的 tag。
func getPotentialTypeValues(typeValue reflect.Type) (map[string]reflect.Type, error) {
	if typeValue.Kind() == reflect.Pointer {
		typeValue = typeValue.Elem()
	}

	if typeValue.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%v is not of type struct", typeValue)
	}

	ret := make(map[string]reflect.Type)

	for fieldIndex := 0; fieldIndex < typeValue.NumField(); fieldIndex++ {
		fieldType := typeValue.Field(fieldIndex)
		yamlTag := fieldType.Tag.Get("json")
		yamlTagName := strings.Split(yamlTag, ",")[0]

		ret[yamlTagName] = fieldType.Type
	}

	return ret, nil
}

func findKnownValue(parts []string, valueOptions sets.Set[string]) int {
	for i := range parts {
		if valueOptions.Has(parts[i]) {
			return i
		}
	}

	return -1
}
