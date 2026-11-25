/*
Copyright 2023 The Kubernetes Authors.

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

package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	flag "github.com/spf13/pflag"

	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

var (
	typeSrc = flag.StringP("type-src", "s", "", "From where we are going to read the types")
	re      = regexp.MustCompile("`(\\b\\w+\\b)`")
)

// kubeTypesMap 是从字段名到其 tag 名和文档的映射。
type kubeTypesMap map[string]kruntime.Pair

func main() {
	flag.Parse()

	if *typeSrc == "" {
		klog.Fatalf("Please define -s flag as it is the api type file")
	}

	docsForTypes, err := kruntime.ParseDocumentationFrom(*typeSrc)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	rc := false

	for _, ks := range docsForTypes {
		typesMap := make(kubeTypesMap)

		for _, p := range ks[1:] {
			// 跳过没有 tag 名的字段
			if p.Name != "" {
				typesMap[strings.ToLower(p.Name)] = p
			}
		}

		structName := ks[0].Name

		rc = checkFieldNameAndDoc(structName, "", ks[0].Doc, typesMap) || rc
		for _, p := range ks[1:] {
			rc = checkFieldNameAndDoc(structName, p.Name, p.Doc, typesMap) || rc
		}
	}

	if rc {
		os.Exit(1)
	}
}

func checkFieldNameAndDoc(structName, fieldName, doc string, typesMap kubeTypesMap) bool {
	rc := false
	visited := sets.Set[string]{}

	// 规则如下：
	// 1. 取出文档中所有用反引号引用的名称
	// 2. 跳过已经发现不匹配的名称。
	// 3. 跳过小写形式与 tag 名小写形式不同的名称，
	//    因为有些文档用反引号引用字段值或 nil
	// 4. 检查名称是否与其 tag 名不同

	// TODO: 人工为文档字符串添加反引号，然后更新 linter
	// 以检查反引号是否存在
	nameGroups := re.FindAllStringSubmatch(doc, -1)
	for _, nameGroup := range nameGroups {
		name := nameGroup[1]
		if visited.Has(name) {
			continue
		}
		if p, ok := typesMap[strings.ToLower(name)]; ok && p.Name != name {
			rc = true
			visited.Insert(name)

			fmt.Fprintf(os.Stderr, "Error: doc for %s", structName)
			if fieldName != "" {
				fmt.Fprintf(os.Stderr, ".%s", fieldName)
			}

			fmt.Fprintf(os.Stderr, " contains: %s, which should be: %s\n", name, p.Name)
		}
	}

	return rc
}
