package broker

import (
	"testing"
)

func TestAnswers(t *testing.T) {

	// Create producers
	p1 := "p-addr-1"
	p2 := "p-addr-2"

	// Create consumers
	c1 := "c-addr-1"
	c2 := "c-addr-2"

	// create answers map
	answers := newAnswers()

	// Add to answers
	answers.add(answersData{p1, 11}, answersData{c1, 21})
	answers.add(answersData{p2, 11}, answersData{c2, 21})

	// Get from answers and check
	p := answers.get(answersData{c1, 21})
	if p == nil {
		t.Error("produser p1 not found")
		return
	}
	if p.addr != p1 || p.id != 11 {
		t.Error("wrong produser p1")
		return
	}

	p = answers.get(answersData{c2, 21})
	if p == nil {
		t.Error("produser p2 not found")
		return
	}
	if p.addr != p2 || p.id != 11 {
		t.Error("wrong produser p2")
		return
	}

	// Check length
	if answers.len() != 0 {
		t.Error("wrong maps length")
		return
	}
}
