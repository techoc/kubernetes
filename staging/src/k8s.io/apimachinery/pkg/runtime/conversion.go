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

// Package runtime defines conversions between generic types and structs to map query strings
// to struct objects.
package runtime

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/conversion"
)

// DefaultMetaV1FieldSelectorConversion auto-accepts metav1 values for name and namespace.
// A cluster scoped resource specifying namespace empty works fine and specifying a particular
// namespace will return no results, as expected.
func DefaultMetaV1FieldSelectorConversion(label, value string) (string, string, error) {
	switch label {
	case "metadata.name":
		return label, value, nil
	case "metadata.namespace":
		return label, value, nil
	default:
		return "", "", fmt.Errorf("%q is not a known field selector: only %q, %q", label, "metadata.name", "metadata.namespace")
	}
}

// Convert_Slice_string_To_string converts the first element of a query-style
// string slice to a plain string; an absent parameter yields "".
func Convert_Slice_string_To_string(in *[]string, out *string, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = ""
		return nil
	}
	*out = (*in)[0]
	return nil
}

// Convert_Slice_string_To_int converts the first element of a query-style
// string slice to an int; an absent parameter yields 0.
func Convert_Slice_string_To_int(in *[]string, out *int, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = 0
		return nil
	}
	str := (*in)[0]
	i, err := strconv.Atoi(str)
	if err != nil {
		return err
	}
	*out = i
	return nil
}

// Convert_Slice_string_To_bool will convert a string parameter to boolean.
// Only the absence of a value (i.e. zero-length slice), a value of "false", or a
// value of "0" resolve to false.
// Any other value (including empty string) resolves to true.
func Convert_Slice_string_To_bool(in *[]string, out *bool, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = false
		return nil
	}
	// An explicitly empty string ("?flag=") is treated as true, matching the
	// historical behavior of Kubernetes query parameters.
	switch {
	case (*in)[0] == "0", strings.EqualFold((*in)[0], "false"):
		*out = false
	default:
		*out = true
	}
	return nil
}

// Convert_Slice_string_To_Pointer_bool is the pointer-returning form of
// Convert_Slice_string_To_bool. An absent parameter produces a pointer to
// false rather than nil, because API bool fields are not optional.
func Convert_Slice_string_To_Pointer_bool(in *[]string, out **bool, s conversion.Scope) error {
	if len(*in) == 0 {
		boolVar := false
		*out = &boolVar
		return nil
	}
	switch {
	case (*in)[0] == "0", strings.EqualFold((*in)[0], "false"):
		boolVar := false
		*out = &boolVar
	default:
		boolVar := true
		*out = &boolVar
	}
	return nil
}

// string_to_int64 parses a base-10 int64; the shared helper keeps all the
// integer conversions consistent about rejecting non-decimal input.
func string_to_int64(in string) (int64, error) {
	return strconv.ParseInt(in, 10, 64)
}

// Convert_string_To_int64 converts a plain string to int64; nil input yields 0.
func Convert_string_To_int64(in *string, out *int64, s conversion.Scope) error {
	if in == nil {
		*out = 0
		return nil
	}
	i, err := string_to_int64(*in)
	if err != nil {
		return err
	}
	*out = i
	return nil
}

// Convert_Slice_string_To_int64 converts the first element of a query-style
// string slice to int64; an absent parameter yields 0.
func Convert_Slice_string_To_int64(in *[]string, out *int64, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = 0
		return nil
	}
	i, err := string_to_int64((*in)[0])
	if err != nil {
		return err
	}
	*out = i
	return nil
}

// Convert_string_To_Pointer_int64 converts a string to *int64. A nil input
// means the API field was absent, which is represented as a nil pointer so
// callers can distinguish "unset" from an explicit zero.
func Convert_string_To_Pointer_int64(in *string, out **int64, s conversion.Scope) error {
	if in == nil {
		*out = nil
		return nil
	}
	i, err := string_to_int64(*in)
	if err != nil {
		return err
	}
	*out = &i
	return nil
}

// Convert_Slice_string_To_Pointer_int64 converts the first element of a
// query-style string slice to *int64; an absent parameter yields nil.
func Convert_Slice_string_To_Pointer_int64(in *[]string, out **int64, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = nil
		return nil
	}
	i, err := string_to_int64((*in)[0])
	if err != nil {
		return err
	}
	*out = &i
	return nil
}

// RegisterStringConversions registers the string-slice helpers with the scheme
// so generic reflection-based conversion can find them. Pointer-typed variants
// are registered on demand because they are not universally applicable.
func RegisterStringConversions(s *Scheme) error {
	if err := s.AddConversionFunc((*[]string)(nil), (*string)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_string(a.(*[]string), b.(*string), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*[]string)(nil), (*int)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_int(a.(*[]string), b.(*int), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*[]string)(nil), (*bool)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_bool(a.(*[]string), b.(*bool), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*[]string)(nil), (*int64)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_int64(a.(*[]string), b.(*int64), scope)
	}); err != nil {
		return err
	}
	return nil
}
