package types

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Document represents a single document in the index
type Document struct {
	ID      string                 `json:"id"`
	Fields  map[string]FieldValue `json:"fields"`
	Version int64                  `json:"version"` // For optimistic concurrency control
	Created time.Time              `json:"created"`
	Updated time.Time              `json:"updated"`
}

// FieldValue represents the value of a field, which can be of different types
type FieldValue interface {
	Type() FieldType
	String() string
}

// FieldType represents the type of a field
type FieldType string

const (
	FieldTypeText    FieldType = "text"     // Full-text searchable
	FieldTypeKeyword FieldType = "keyword"  // Exact match, not analyzed
	FieldTypeNumeric FieldType = "numeric"  // Integer or float
	FieldTypeVector  FieldType = "vector"   // Dense vector for similarity search
	FieldTypeBoolean FieldType = "boolean"  // Boolean value
	FieldTypeDate    FieldType = "date"     // Date/time
)

// TextValue represents a text field value
type TextValue struct {
	Value string
}

func (v TextValue) Type() FieldType { return FieldTypeText }
func (v TextValue) String() string  { return v.Value }

// KeywordValue represents a keyword field value
type KeywordValue struct {
	Value string
}

func (v KeywordValue) Type() FieldType { return FieldTypeKeyword }
func (v KeywordValue) String() string  { return v.Value }

// NumericValue represents a numeric field value
type NumericValue struct {
	Value float64
}

func (v NumericValue) Type() FieldType { return FieldTypeNumeric }
func (v NumericValue) String() string  { 
	return strconv.FormatFloat(v.Value, 'f', -1, 64)
}

// VectorValue represents a vector field value
type VectorValue struct {
	Value []float32
	Dim   int // Dimension of the vector
}

func (v VectorValue) Type() FieldType { return FieldTypeVector }
func (v VectorValue) String() string  { return "vector" }

// BooleanValue represents a boolean field value
type BooleanValue struct {
	Value bool
}

func (v BooleanValue) Type() FieldType { return FieldTypeBoolean }
func (v BooleanValue) String() string  {
	if v.Value {
		return "true"
	}
	return "false"
}

// DateValue represents a date field value
type DateValue struct {
	Value time.Time
}

func (v DateValue) Type() FieldType { return FieldTypeDate }
func (v DateValue) String() string  { return v.Value.Format(time.RFC3339) }

// NewDocument creates a new document with the given ID
func NewDocument(id string) *Document {
	now := time.Now()
	return &Document{
		ID:      id,
		Fields:  make(map[string]FieldValue),
		Version: 1,
		Created: now,
		Updated: now,
	}
}

// SetField sets a field value on the document
func (d *Document) SetField(name string, value FieldValue) {
	d.Fields[name] = value
	d.Updated = time.Now()
}

// GetField retrieves a field value from the document
func (d *Document) GetField(name string) (FieldValue, bool) {
	value, ok := d.Fields[name]
	return value, ok
}

// GetFieldAsText retrieves a field as text, returns empty string if not found or wrong type
func (d *Document) GetFieldAsText(name string) string {
	value, ok := d.Fields[name]
	if !ok {
		return ""
	}
	return value.String()
}

// MarshalJSON implements custom JSON marshaling for Document
func (d *Document) MarshalJSON() ([]byte, error) {
	type Alias Document
	aux := &struct {
		*Alias
		Fields map[string]interface{} `json:"fields"`
	}{
		Alias: (*Alias)(d),
		Fields: make(map[string]interface{}),
	}
	
	// Convert FieldValue to a serializable format
	for k, v := range d.Fields {
		aux.Fields[k] = map[string]interface{}{
			"type":  v.Type(),
			"value": v,
		}
	}
	
	return json.Marshal(aux)
}

// UnmarshalJSON implements custom JSON unmarshaling for Document
func (d *Document) UnmarshalJSON(data []byte) error {
	type Alias Document
	aux := &struct {
		*Alias
		Fields map[string]map[string]interface{} `json:"fields"`
	}{
		Alias: (*Alias)(d),
	}
	
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	
	// Convert back to FieldValue
	d.Fields = make(map[string]FieldValue)
	for k, v := range aux.Fields {
		fieldType, ok := v["type"].(string)
		if !ok {
			return fmt.Errorf("invalid field type for %s", k)
		}
		
		var fieldValue FieldValue
		switch FieldType(fieldType) {
		case FieldTypeText:
			if val, ok := v["value"].(map[string]interface{}); ok {
				if str, ok := val["Value"].(string); ok {
					fieldValue = TextValue{Value: str}
				}
			} else if str, ok := v["value"].(string); ok {
				fieldValue = TextValue{Value: str}
			}
		case FieldTypeKeyword:
			if val, ok := v["value"].(map[string]interface{}); ok {
				if str, ok := val["Value"].(string); ok {
					fieldValue = KeywordValue{Value: str}
				}
			} else if str, ok := v["value"].(string); ok {
				fieldValue = KeywordValue{Value: str}
			}
		case FieldTypeNumeric:
			if val, ok := v["value"].(map[string]interface{}); ok {
				if num, ok := val["Value"].(float64); ok {
					fieldValue = NumericValue{Value: num}
				}
			} else if num, ok := v["value"].(float64); ok {
				fieldValue = NumericValue{Value: num}
			}
		case FieldTypeBoolean:
			if val, ok := v["value"].(map[string]interface{}); ok {
				if b, ok := val["Value"].(bool); ok {
					fieldValue = BooleanValue{Value: b}
				}
			} else if b, ok := v["value"].(bool); ok {
				fieldValue = BooleanValue{Value: b}
			}
		}
		
		if fieldValue != nil {
			d.Fields[k] = fieldValue
		}
	}
	
	return nil
}

