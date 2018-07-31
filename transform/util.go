// Package transform package contains canonical implementations of Kazaam transforms.
package transform

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/knetic/govaluate"
	"github.com/qntfy/jsonparser"
)

// ParseError should be thrown when there is an issue with parsing any of the specification or data
type ParseError string

func (p ParseError) Error() string {
	return string(p)
}

// RequireError should be thrown if a required key is missing in the data
type RequireError string

func (r RequireError) Error() string {
	return string(r)
}

// SpecError should be thrown if the spec for a transform is malformed
type SpecError string

func (s SpecError) Error() string {
	return string(s)
}

// Config contains the options that dictate the behavior of a transform. The internal
// `spec` object can be an arbitrary json configuration for the transform.
type Config struct {
	Spec    *map[string]interface{} `json:"spec"`
	Require bool                    `json:"require,omitempty"`
	InPlace bool                    `json:"inplace,omitempty"`
}

var (
	NonExistentPath = RequireError("Path does not exist")
	jsonPathRe      = regexp.MustCompile("([^\\[\\]]+)\\[(.*?)\\]")
	jsonArrayRe     = regexp.MustCompile(`\[[^\]]*\]`)
	jsonFilterRe    = regexp.MustCompile(`\?\(@.*\)`)
)

// GetJSONRaw makes getJSONRaw public
// Given a json byte slice `data` and a kazaam `path` string, return the object at the path in data if it exists.
func GetJSONRaw(data []byte, path string, pathRequired bool) ([]byte, error) {
	return getJSONRaw(data, path, pathRequired)
}

// SetJSONRaw makes setJSONRaw public
// setJSONRaw sets the value at a key and handles array indexing
func SetJSONRaw(data, out []byte, path string) ([]byte, error) {
	return setJSONRaw(data, out, path)
}

// Given a json byte slice `data` and a kazaam `path` string, return the object at the path in data if it exists.
func getJSONRaw(data []byte, path string, pathRequired bool) ([]byte, error) {
	objectKeys := customSplit(path)
	for element, k := range objectKeys {
		// check the object key to see if it also contains an array reference
		arrayRefs := jsonPathRe.FindAllStringSubmatch(k, -1)
		if arrayRefs != nil && len(arrayRefs) > 0 {
			arrayKeyStrs := jsonArrayRe.FindAllString(k, -1)
			extracted := false
			objKey := arrayRefs[0][1] // the key
			results, newPath, err := getArrayResults(data, objectKeys, objKey, element)
			if err == jsonparser.KeyPathNotFoundError {
				if pathRequired {
					return nil, NonExistentPath
				}
			} else if err != nil {
				return nil, err
			}
			for _, arrayKeyStr := range arrayKeyStrs {
				// trim the square brackets
				arrayKeyStr = arrayKeyStr[1 : len(arrayKeyStr)-1]
				err = validateArrayKeyString(arrayKeyStr)
				if err != nil {
					return nil, err
				}
				// if there's a wildcard array reference
				if arrayKeyStr == "*" {
					// We won't filter anything with a wildcard
					continue
				} else if filterPattern := jsonFilterRe.FindString(arrayKeyStr); filterPattern != "" {
					// MODIFICATIONS --- FILTER SUPPORT
					// right now we only support filter expressions of the form ?(@.some.json.path Op "someData")
					// Op must be a boolean operator: '==', '<' are fine, '+', '%' are not.  Spaces ARE required

					// get the filter jsonpath expression
					filterPattern = filterPattern[2 : len(filterPattern)-1]
					filterParts := strings.Split(filterPattern, " ")
					var filterPath string
					if len(filterParts[0]) > 2 {
						filterPath = filterParts[0][2:]
					}
					var filterObjs [][]byte
					var filteredResults [][]byte

					// evaluate the jsonpath filter jsonpath for each index
					if newPath != "" {
						// we are filtering against a list of objects, lookup the data recursively
						for _, v := range results {
							intermediate, err := getJSONRaw(v, filterPath, true)
							if err != nil {
								if err == NonExistentPath {
									// this is fine, we'll just filter these out
									filterObjs = append(filterObjs, nil)
								} else {
									return nil, err
								}
							} else {
								filterObjs = append(filterObjs, intermediate)
							}
						}
					} else if filterPath == "" {
						// we are filtering against a list of primitives - we won't match any non-empty filterPath
						for _, v := range results {
							filterObjs = append(filterObjs, v)
						}
					}

					// evaluate the filter
					for i, v := range filterObjs {
						if v != nil {
							filterParts[0] = string(v)
							exprString := strings.Join(filterParts, " ")
							// filter the objects based on the results
							expr, err := govaluate.NewEvaluableExpression(exprString)
							if err != nil {
								return nil, err
							}
							result, err := expr.Evaluate(nil)
							if err != nil {
								return nil, err
							}
							// We pass through the filter if the filter is a boolean expression
							// If the filter is not a boolean expression, just pass everything through
							if accepted, ok := result.(bool); accepted || !ok {
								filteredResults = append(filteredResults, results[i])
							}
						}
					}

					// Set the results for the next pass
					results = filteredResults
				} else {
					index, err := strconv.ParseInt(arrayKeyStr, 10, 64)
					if err != nil {
						return nil, err
					}
					if index < int64(len(results)) {
						results = [][]byte{results[index]}
					} else {
						results = [][]byte{}
					}
					extracted = true
				}
			}
			output, err := lookupAndWriteMulti(results, newPath, pathRequired)
			if err != nil {
				return nil, err
			}
			// if we have access a specific index, we want to extract the value from the array
			// we just do this manually
			if extracted {
				output = output[1 : len(output)-1]
			}
			return output, nil
		} else {
			// no array reference, good to go
			continue
		}
	}
	result, dataType, _, err := jsonparser.Get(data, objectKeys...)

	// jsonparser strips quotes from Strings
	if dataType == jsonparser.String {
		// bookend() is destructive to underlying slice, need to copy.
		// extra capacity saves an allocation and copy during bookend.
		result = HandleUnquotedStrings(result, dataType)
	}
	if len(result) == 0 {
		result = []byte("null")
	}
	if err == jsonparser.KeyPathNotFoundError {
		if pathRequired {
			return nil, NonExistentPath
		}
	} else if err != nil {
		return nil, err
	}
	return result, nil
}

// setJSONRaw sets the value at a key and handles array indexing
func setJSONRaw(data, out []byte, path string) ([]byte, error) {
	var err error
	splitPath := customSplit(path)
	numOfInserts := 0

	for element, k := range splitPath {
		arrayRefs := jsonPathRe.FindAllStringSubmatch(k, -1)
		if arrayRefs != nil && len(arrayRefs) > 0 {
			objKey := arrayRefs[0][1]      // the key
			arrayKeyStr := arrayRefs[0][2] // the array index
			err = validateArrayKeyString(arrayKeyStr)
			if err != nil {
				return nil, err
			}
			// Note: this branch of the function is not currently used by any
			// existing transforms. It is simpy here to support he generalized
			// form of this operation
			if arrayKeyStr == "*" {
				// ArrayEach setup
				splitPath[element+numOfInserts] = objKey
				beforePath := splitPath[:element+numOfInserts+1]
				afterPath := strings.Join(splitPath[element+numOfInserts+1:], ".")
				// use jsonparser.ArrayEach to count the number of items in the
				// array
				var arraySize int
				_, err = jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
					arraySize++
				}, beforePath...)
				if err != nil {
					return nil, err
				}

				// setJSONRaw() the rest of path for each element in results
				for i := 0; i < arraySize; i++ {
					var newPath string
					// iterate through each item in the array by replacing the
					// wildcard with an int and joining the path back together
					newArrayKey := strings.Join([]string{"[", strconv.Itoa(i), "]"}, "")
					beforePathStr := strings.Join(beforePath, ".")
					beforePathArrayKeyStr := strings.Join([]string{beforePathStr, newArrayKey}, "")
					// if there's nothing that comes after the array index,
					// don't join so that we avoid trailing cruft
					if len(afterPath) > 0 {
						newPath = strings.Join([]string{beforePathArrayKeyStr, afterPath}, ".")
					} else {
						newPath = beforePathArrayKeyStr
					}
					// now call the function, but this time with an array index
					// instead of a wildcard
					data, err = setJSONRaw(data, out, newPath)
					if err != nil {
						return nil, err
					}
				}
				return data, nil
			}
			// if not a wildcard then piece that path back together with the
			// array index as an entry in the splitPath slice
			splitPath = makePathWithIndex(arrayKeyStr, objKey, splitPath, element+numOfInserts)
			numOfInserts++
		} else {
			continue
		}
	}
	data, err = jsonparser.Set(data, out, splitPath...)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// delJSONRaw deletes the value at a path and handles array indexing
func delJSONRaw(data []byte, path string, pathRequired bool) ([]byte, error) {
	var err error
	splitPath := customSplit(path)
	numOfInserts := 0

	for element, k := range splitPath {
		arrayRefs := jsonPathRe.FindAllStringSubmatch(k, -1)
		if arrayRefs != nil && len(arrayRefs) > 0 {
			objKey := arrayRefs[0][1]      // the key
			arrayKeyStr := arrayRefs[0][2] // the array index
			err = validateArrayKeyString(arrayKeyStr)
			if err != nil {
				return nil, err
			}

			// not currently supported
			if arrayKeyStr == "*" {
				return nil, SpecError("Array wildcard not supported for this operation.")
			}

			// if not a wildcard then piece that path back together with the
			// array index as an entry in the splitPath slice
			splitPath = makePathWithIndex(arrayKeyStr, objKey, splitPath, element+numOfInserts)
			numOfInserts++
		} else {
			// no array reference, good to go
			continue
		}
	}

	if pathRequired {
		_, _, _, err = jsonparser.Get(data, splitPath...)
		if err == jsonparser.KeyPathNotFoundError {
			return nil, NonExistentPath
		} else if err != nil {
			return nil, err
		}
	}

	data = jsonparser.Delete(data, splitPath...)
	return data, nil
}

// validateArrayKeyString is a helper function to make sure the array index is
// legal
func validateArrayKeyString(arrayKeyStr string) error {
	if filterPattern := jsonFilterRe.FindString(arrayKeyStr); filterPattern == "" &&
		arrayKeyStr != "*" && arrayKeyStr != "+" && arrayKeyStr != "-" {

		val, err := strconv.Atoi(arrayKeyStr)
		if val < 0 || err != nil {
			return ParseError(fmt.Sprintf("Warn: Unable to coerce index to integer: %v", arrayKeyStr))
		}
	}
	return nil
}

// makePathWithIndex generats a path slice to pass to jsonparser
func makePathWithIndex(arrayKeyStr, objectKey string, pathSlice []string, pathIndex int) []string {
	arrayKey := string(bookend([]byte(arrayKeyStr), '[', ']'))
	pathSlice[pathIndex] = objectKey
	pathSlice = append(pathSlice, "")
	copy(pathSlice[pathIndex+2:], pathSlice[pathIndex+1:])
	pathSlice[pathIndex+1] = arrayKey
	return pathSlice
}

// add characters at beginning and end of []byte
func bookend(value []byte, bef, aft byte) []byte {
	value = append(value, ' ', aft)
	copy(value[1:], value[:len(value)-2])
	value[0] = bef
	return value
}

// jsonparser strips quotes from returned strings, this adds them back
func HandleUnquotedStrings(value []byte, dt jsonparser.ValueType) []byte {
	if dt == jsonparser.String {
		// bookend() is destructive to underlying slice, need to copy.
		// extra capacity saves an allocation and copy during bookend.
		tmp := make([]byte, len(value), len(value)+2)
		copy(tmp, value)
		value = bookend(tmp, '"', '"')
	}
	return value
}

// Given the data, find and return all objects in the array referenced by the current index in the jsonpath, as
// well as the remaining path we want to use against each object
// This function is dumb to whether or not the path needs to exist in the data
func getArrayResults(data []byte, objectKeys []string, objKey string, index int) ([][]byte, string, error) {
	// ArrayEach setup
	objectKeys[index] = objKey
	beforePath := objectKeys[:index+1]
	newPath := strings.Join(objectKeys[index+1:], ".")
	var results [][]byte

	// use jsonparser.ArrayEach to copy the array into results
	_, err := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		results = append(results, HandleUnquotedStrings(value, dataType))
	}, beforePath...)
	if err != nil {
		return nil, "", err
	}
	return results, newPath, nil
}

// performs lookups of the path in each of the results - top level lookups ($) are represented by an empty path
func lookupAndWriteMulti(dataObjects [][]byte, path string, pathRequired bool) ([]byte, error) {
	if path != "" {
		for i, value := range dataObjects {
			intermediate, err := getJSONRaw(value, path, pathRequired)
			if err != nil {
				return nil, err
			}
			dataObjects[i] = intermediate
		}
	}

	// copy into raw []byte format and return
	var buffer bytes.Buffer
	buffer.WriteByte('[')
	for i := 0; i < len(dataObjects)-1; i++ {
		buffer.Write(dataObjects[i])
		buffer.WriteByte(',')
	}
	if len(dataObjects) > 0 {
		buffer.Write(dataObjects[len(dataObjects)-1])
	}
	buffer.WriteByte(']')
	return buffer.Bytes(), nil
}

// splits on '.', except for those nested inside parenthesis
func customSplit(s string) []string {
	var result []string
	var cur string
	parenDepth := 0
	for _, v := range strings.Split(s, "") {
		if v == "(" {
			parenDepth++
		}
		if v == ")" && parenDepth > 0 {
			parenDepth--
		}
		if v == "." && parenDepth == 0 {
			result = append(result, cur)
			cur = ""
		} else {
			cur += v
		}
	}
	if cur != "" {
		result = append(result, cur)
	}
	return result
}
