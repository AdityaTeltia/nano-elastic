package types

// Schema defines the structure of an index
type Schema struct {
	Name        string            `json:"name"`
	Fields      map[string]FieldDef `json:"fields"`
	PrimaryKey  string            `json:"primary_key"` // Field name used as document ID if not provided
	Created     int64             `json:"created"`
	Version     int               `json:"version"` // Schema version for migrations
}

// FieldDef defines a field in the schema
type FieldDef struct {
	Type        FieldType `json:"type"`
	Indexed     bool      `json:"indexed"`      // Whether the field is indexed
	Stored      bool      `json:"stored"`       // Whether the field is stored for retrieval
	Analyzed    bool      `json:"analyzed"`     // Whether the field is analyzed (for text fields)
	VectorDim   int       `json:"vector_dim"`   // Dimension for vector fields
	Boost       float64   `json:"boost"`       // Boost factor for scoring (default 1.0)
	Description string    `json:"description"` // Optional description
}

// NewSchema creates a new schema with the given name
func NewSchema(name string) *Schema {
	return &Schema{
		Name:    name,
		Fields:  make(map[string]FieldDef),
		Version: 1,
	}
}

// AddField adds a field definition to the schema
func (s *Schema) AddField(name string, fieldType FieldType, options ...FieldOption) *FieldDef {
	def := FieldDef{
		Type:    fieldType,
		Indexed: true,  // Default to indexed
		Stored:  true,  // Default to stored
		Analyzed: fieldType == FieldTypeText, // Text fields are analyzed by default
		Boost:   1.0,
	}

	// Apply options
	for _, opt := range options {
		opt(&def)
	}

	s.Fields[name] = def
	return &def
}

// FieldOption is a function that modifies a FieldDef
type FieldOption func(*FieldDef)

// WithIndexed sets whether the field is indexed
func WithIndexed(indexed bool) FieldOption {
	return func(f *FieldDef) {
		f.Indexed = indexed
	}
}

// WithStored sets whether the field is stored
func WithStored(stored bool) FieldOption {
	return func(f *FieldDef) {
		f.Stored = stored
	}
}

// WithAnalyzed sets whether the field is analyzed
func WithAnalyzed(analyzed bool) FieldOption {
	return func(f *FieldDef) {
		f.Analyzed = analyzed
	}
}

// WithVectorDim sets the dimension for vector fields
func WithVectorDim(dim int) FieldOption {
	return func(f *FieldDef) {
		f.VectorDim = dim
	}
}

// WithBoost sets the boost factor for the field
func WithBoost(boost float64) FieldOption {
	return func(f *FieldDef) {
		f.Boost = boost
	}
}

// WithDescription sets the description for the field
func WithDescription(desc string) FieldOption {
	return func(f *FieldDef) {
		f.Description = desc
	}
}

// GetField returns the field definition for the given field name
func (s *Schema) GetField(name string) (*FieldDef, bool) {
	def, ok := s.Fields[name]
	if !ok {
		return nil, false
	}
	return &def, true
}

// ValidateDocument validates a document against the schema
func (s *Schema) ValidateDocument(doc *Document) error {
	// Check if all required fields are present
	// For now, we'll allow extra fields (flexible schema)
	// In the future, we can add strict mode
	
	// Validate field types
	for name, value := range doc.Fields {
		if def, ok := s.Fields[name]; ok {
			if value.Type() != def.Type {
				return &SchemaValidationError{
					Field: name,
					Expected: def.Type,
					Actual: value.Type(),
				}
			}
			
			// Validate vector dimension
			if def.Type == FieldTypeVector {
				if vec, ok := value.(VectorValue); ok {
					if vec.Dim != def.VectorDim {
						return &SchemaValidationError{
							Field: name,
							Message: "vector dimension mismatch",
						}
					}
				}
			}
		}
	}
	
	return nil
}

// SchemaValidationError represents a schema validation error
type SchemaValidationError struct {
	Field    string
	Expected FieldType
	Actual   FieldType
	Message  string
}

func (e *SchemaValidationError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "schema validation error"
}

