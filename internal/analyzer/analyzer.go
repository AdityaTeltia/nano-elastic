package analyzer

import (
	"strings"
)

// StopWords is a set of common words to filter out
// In Go, we use a map[string]bool as a set (map with bool values)
var StopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true,
	"at": true, "be": true, "by": true, "for": true, "from": true,
	"has": true, "he": true, "in": true, "is": true, "it": true,
	"its": true, "of": true, "on": true, "that": true, "the": true,
	"to": true, "was": true, "were": true, "will": true, "with": true,
}

// Analyzer processes text through a chain of analyzers
type Analyzer struct {
	tokenizer *Tokenizer
	useStopWords bool
	useStemming  bool
}

// NewAnalyzer creates a new analyzer
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		tokenizer:    NewTokenizer(),
		useStopWords: true,
		useStemming:  false, // We'll implement stemming later
	}
}

// NewAnalyzerWithOptions creates an analyzer with custom options
// This demonstrates Go's variadic function pattern
func NewAnalyzerWithOptions(useStopWords, useStemming bool) *Analyzer {
	return &Analyzer{
		tokenizer:    NewTokenizer(),
		useStopWords: useStopWords,
		useStemming:  useStemming,
	}
}

// Analyze processes text and returns normalized tokens
// This is the main entry point for text analysis
func (a *Analyzer) Analyze(text string) []string {
	// Step 1: Tokenize
	tokens := a.tokenizer.Tokenize(text)
	
	// Step 2: Filter stop words (if enabled)
	if a.useStopWords {
		tokens = a.filterStopWords(tokens)
	}
	
	// Step 3: Stem (if enabled) - TODO in future
	if a.useStemming {
		tokens = a.stem(tokens)
	}
	
	return tokens
}

// AnalyzeWithPositions processes text and returns tokens with positions
func (a *Analyzer) AnalyzeWithPositions(text string) ([]string, []int) {
	// Tokenize with positions
	tokens, positions := a.tokenizer.TokenizeWithPositions(text)
	
	// Filter stop words (need to adjust positions)
	if a.useStopWords {
		tokens, positions = a.filterStopWordsWithPositions(tokens, positions)
	}
	
	return tokens, positions
}

// filterStopWords removes stop words from tokens
// In Go, we use a slice to build the result
func (a *Analyzer) filterStopWords(tokens []string) []string {
	var filtered []string
	for _, token := range tokens {
		// Check if token is NOT in stop words map
		// In Go, accessing a map returns (value, exists)
		if !StopWords[token] {
			filtered = append(filtered, token)
		}
	}
	return filtered
}

// filterStopWordsWithPositions removes stop words and adjusts positions
func (a *Analyzer) filterStopWordsWithPositions(tokens []string, positions []int) ([]string, []int) {
	var filteredTokens []string
	var filteredPositions []int
	
	for i, token := range tokens {
		if !StopWords[token] {
			filteredTokens = append(filteredTokens, token)
			filteredPositions = append(filteredPositions, positions[i])
		}
	}
	
	return filteredTokens, filteredPositions
}

// stem applies stemming to tokens (Porter Stemmer - simplified version)
// This is a placeholder for now - we'll implement proper stemming later
func (a *Analyzer) stem(tokens []string) []string {
	// Simple stemming: remove common suffixes
	// Real implementation would use Porter Stemmer algorithm
	stemmed := make([]string, len(tokens))
	for i, token := range tokens {
		stemmed[i] = a.stemWord(token)
	}
	return stemmed
}

// stemWord applies basic stemming to a single word
func (a *Analyzer) stemWord(word string) string {
	// Very basic stemming - remove common suffixes
	// This is simplified - real stemmer is more complex
	if len(word) > 3 {
		if strings.HasSuffix(word, "ing") {
			return word[:len(word)-3]
		}
		if strings.HasSuffix(word, "ed") {
			return word[:len(word)-2]
		}
		if strings.HasSuffix(word, "s") && len(word) > 1 {
			return word[:len(word)-1]
		}
	}
	return word
}

