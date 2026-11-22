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

package validation_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/utils/ptr"
)

var zeroIntSchema *spec.Schema = &spec.Schema{
	SchemaProps: spec.SchemaProps{
		Type:    spec.StringOrArray{"number"},
		Minimum: ptr.To[float64](0),
		Maximum: ptr.To[float64](0),
	},
}

var smallIntSchema *spec.Schema = &spec.Schema{
	SchemaProps: spec.SchemaProps{
		Type:    spec.StringOrArray{"number"},
		Maximum: ptr.To[float64](50),
	},
}

var mediumIntSchema *spec.Schema = &spec.Schema{
	SchemaProps: spec.SchemaProps{
		Type:    spec.StringOrArray{"number"},
		Minimum: ptr.To[float64](50),
		Maximum: ptr.To[float64](10000),
	},
}

var largeIntSchema *spec.Schema = &spec.Schema{
	SchemaProps: spec.SchemaProps{
		Type:    spec.StringOrArray{"number"},
		Minimum: ptr.To[float64](10000),
	},
}

// TestScalarRatcheting 验证标量字段的 ratcheting：值未变时即使不满足约束也放行。
func TestScalarRatcheting(t *testing.T) {
	validator := validation.NewRatchetingSchemaValidator(mediumIntSchema, nil, "", strfmt.Default)
	require.True(t, validator.ValidateUpdate(1, 1, validation.WithRatcheting(nil)).IsValid())
	require.False(t, validator.ValidateUpdate(1, 2, validation.WithRatcheting(nil)).IsValid())
}

var objectSchema *spec.Schema = &spec.Schema{
	SchemaProps: spec.SchemaProps{
		Type: spec.StringOrArray{"object"},
		Properties: map[string]spec.Schema{
			"zero":   *zeroIntSchema,
			"small":  *smallIntSchema,
			"medium": *mediumIntSchema,
			"large":  *largeIntSchema,
		},
	},
}

var objectObjectSchema *spec.Schema = &spec.Schema{
	SchemaProps: spec.SchemaProps{
		Type: spec.StringOrArray{"object"},
		Properties: map[string]spec.Schema{
			"nested": *objectSchema,
		},
	},
}

// 展示对象中的标量字段可以进行 ratcheting
func TestObjectScalarFieldsRatcheting(t *testing.T) {
	validator := validation.NewRatchetingSchemaValidator(objectSchema, nil, "", strfmt.Default)
	assert.True(t, validator.ValidateUpdate(map[string]interface{}{
		"small": 500,
	}, map[string]interface{}{
		"small": 500,
	}, validation.WithRatcheting(nil)).IsValid())
	assert.True(t, validator.ValidateUpdate(map[string]interface{}{
		"small": 501,
	}, map[string]interface{}{
		"small":  501,
		"medium": 500,
	}, validation.WithRatcheting(nil)).IsValid())
	assert.False(t, validator.ValidateUpdate(map[string]interface{}{
		"small": 500,
	}, map[string]interface{}{
		"small": 501,
	}, validation.WithRatcheting(nil)).IsValid())
}

// 展示本身可被 ratcheting 的对象字段（嵌套对象）也可以进行 ratcheting
func TestObjectObjectFieldsRatcheting(t *testing.T) {
	validator := validation.NewRatchetingSchemaValidator(objectObjectSchema, nil, "", strfmt.Default)
	assert.True(t, validator.ValidateUpdate(map[string]interface{}{
		"nested": map[string]interface{}{
			"small": 500,
		}}, map[string]interface{}{
		"nested": map[string]interface{}{
			"small": 500,
		}}, validation.WithRatcheting(nil)).IsValid())
	assert.True(t, validator.ValidateUpdate(map[string]interface{}{
		"nested": map[string]interface{}{
			"small": 501,
		}}, map[string]interface{}{
		"nested": map[string]interface{}{
			"small":  501,
			"medium": 500,
		}}, validation.WithRatcheting(nil)).IsValid())
	assert.False(t, validator.ValidateUpdate(map[string]interface{}{
		"nested": map[string]interface{}{
			"small": 500,
		}}, map[string]interface{}{
		"nested": map[string]interface{}{
			"small": 501,
		}}, validation.WithRatcheting(nil)).IsValid())
}

var numericFormatSchema *spec.Schema = &spec.Schema{
	SchemaProps: spec.SchemaProps{
		Type: spec.StringOrArray{"object"},
		Properties: map[string]spec.Schema{
			"intThirtyTwo": {
				SchemaProps: spec.SchemaProps{
					Type:   spec.StringOrArray{"integer"},
					Format: "int32",
				},
			},
			"unrelated": {
				SchemaProps: spec.SchemaProps{
					Type: spec.StringOrArray{"string"},
				},
			},
		},
	},
}

// TestNumericFormatRatcheting 验证数值 format（如 int32）越界值在未变更时可被 ratcheting 放行。
func TestNumericFormatRatcheting(t *testing.T) {
	validator := validation.NewRatchetingSchemaValidator(numericFormatSchema, nil, "", strfmt.Default)

	// 值未变更时，ratcheting 应放行已有的非法值
	assert.True(t, validator.ValidateUpdate(map[string]interface{}{
		"intThirtyTwo": int64(math.MaxInt32 + 1),
	}, map[string]interface{}{
		"intThirtyTwo": int64(math.MaxInt32 + 1),
	}, validation.WithRatcheting(nil)).IsValid())

	// 其他字段变化时，ratcheting 仍应放行已有的非法值
	assert.True(t, validator.ValidateUpdate(map[string]interface{}{
		"intThirtyTwo": int64(math.MaxInt32 + 1),
		"unrelated":    "changed",
	}, map[string]interface{}{
		"intThirtyTwo": int64(math.MaxInt32 + 1),
		"unrelated":    "original",
	}, validation.WithRatcheting(nil)).IsValid())

	// 值变为另一个非法值时应失败
	assert.False(t, validator.ValidateUpdate(map[string]interface{}{
		"intThirtyTwo": int64(math.MaxInt32 + 2),
	}, map[string]interface{}{
		"intThirtyTwo": int64(math.MaxInt32 + 1),
	}, validation.WithRatcheting(nil)).IsValid())
}
