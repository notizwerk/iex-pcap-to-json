package main

import "testing"
import "regexp"

var errorResponse1 = `{"took":291,"errors":true,"items":`

var errorResponse2 = `{"took":291,"errors" : true,"items":`

var errorResponse3 = `{"took":291,"errors":   true,"items":`

var indexResponse4 = `{"took":291,"errors":   false,"items":`
var indexResponse5 = `{"took":291,"items":`

func TestJsonDecoding(t *testing.T) {
	errorRegExp := regexp.MustCompile("errors\"\\s*:\\s*true")
	error := errorRegExp.FindString(errorResponse1)
	if len(error) == 0 {
		t.Error("expected error 1")
	}

	error = errorRegExp.FindString(errorResponse2)
	if len(error) == 0 {
		t.Error("expected error 2")
	}

	error = errorRegExp.FindString(errorResponse3)
	if len(error) == 0 {
		t.Error("expected error 3")
	}

	error = errorRegExp.FindString(indexResponse4)
	if len(error) > 0 {
		t.Error("expected NO error 4")
	}
	error = errorRegExp.FindString(indexResponse5)
	if len(error) > 0 {
		t.Error("expected NO error 5")
	}
}
