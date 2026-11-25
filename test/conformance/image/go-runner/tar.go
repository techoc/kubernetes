/*
Copyright 2019 The Kubernetes Authors.

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
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// tarDir 接受源目录和相应的 writer，遍历 'source' 并将找到的每个文件
// 写入 tar writer。
func tarDir(dir, outpath string) error {
	// 打包之前先确保源目录确实存在
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("tar unable to stat directory %v: %w", dir, err)
	}

	outfile, err := os.Create(outpath)
	if err != nil {
		return fmt.Errorf("creating tarball %v: %w", outpath, err)
	}
	defer outfile.Close()

	gzw := gzip.NewWriter(outfile)
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	return filepath.Walk(dir, func(file string, fi os.FileInfo, err error) error {
		// 遇到任何错误都直接返回。
		if err != nil {
			return err
		}

		// 只写入普通文件，且不包含归档文件本身。
		if !fi.Mode().IsRegular() || filepath.Join(dir, fi.Name()) == outpath {
			return nil
		}

		// 创建新的目录/文件头。
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return fmt.Errorf("creating file info header %v: %w", fi.Name(), err)
		}

		// 更新名称，使其正确反映解包时的目标路径。
		header.Name = strings.TrimPrefix(strings.Replace(file, dir, "", -1), string(filepath.Separator))
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("writing header for tarball %v: %w", header.Name, err)
		}

		// 打开文件，复制到 tar 归档，然后关闭。
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("opening file %v for writing into tarball: %w", file, err)
		}
		defer f.Close()

		_, err = io.Copy(tw, f)
		if err != nil {
			return fmt.Errorf("creating file %v contents into tarball: %w", file, err)
		}

		return nil
	})
}
