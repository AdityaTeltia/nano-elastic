package analyzer

import (
	"strings"
	"unicode"
)

// Tokenizer splits text into tokens (words)
type Tokenizer struct {
	// In Go, we can add fields here for configuration later
}

// NewTokenizer creates a new tokenizer
func NewTokenizer() *Tokenizer {
	return &Tokenizer{}
}

// Tokenize splits text into tokens
// In Go, strings are immutable, so we work with byte slices or strings
func (t *Tokenizer) Tokenize(text string) []string {
	// Split on whitespace and punctuation
	// Go's strings.Fields splits on whitespace, but we want more control
	
	// Convert to lowercase first (case-insensitive search)
	text = strings.ToLower(text)
	
	// Split into words
	// We'll use a simple approach: split on non-letter characters
	var tokens []string
	var current strings.Builder
	
	// Iterate over each rune (Unicode character) in the string
	// In Go, strings are UTF-8, so we use range to get runes
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			// Accumulate letters and digits
			current.WriteRune(r)
		} else {
			// Non-letter/digit found, save current token if any
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		}
	}
	
	// Don't forget the last token
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	
	return tokens
}

// TokenizeWithPositions splits text into tokens and returns their positions
// Returns: tokens and their positions (0-indexed)
func (t *Tokenizer) TokenizeWithPositions(text string) ([]string, []int) {
	text = strings.ToLower(text)
	
	var tokens []string
	var positions []int
	var current strings.Builder
	position := 0
	
	for i, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if current.Len() == 0 {
				// Start of new token, record position
				position = i
			}
			current.WriteRune(r)
		} else {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				positions = append(positions, position)
				current.Reset()
			}
		}
	}
	
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
		positions = append(positions, position)
	}
	
	return tokens, positions
}

