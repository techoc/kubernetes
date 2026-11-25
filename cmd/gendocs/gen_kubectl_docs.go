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

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra/doc"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/kubectl/pkg/cmd"
	"k8s.io/kubernetes/cmd/genutils"
)

func main() {
	// 使用 os.Args 而非 "flags"，否则会破坏 man page！
	path := "docs/"
	if len(os.Args) == 2 {
		path = os.Args[1]
	} else if len(os.Args) > 2 {
		fmt.Fprintf(os.Stderr, "usage: %s [output directory]\n", os.Args[0])
		os.Exit(1)
	}

	outDir, err := genutils.OutDir(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get output directory: %v\n", err)
		os.Exit(1)
	}

	// 设置 kubectl 使用的环境变量，使输出与运行位置无关，
	// 始终保持一致。
	os.Setenv("HOME", "/home/username")
	kubectl := cmd.NewKubectlCommand(cmd.KubectlOptions{IOStreams: genericiooptions.IOStreams{In: bytes.NewReader(nil), Out: io.Discard, ErrOut: io.Discard}})
	doc.GenMarkdownTree(kubectl, outDir)
}
