package teomq

import (
	"testing"

	"github.com/teonet-go/teonet"
)

func TestAnswers(t *testing.T) {

	// Create producers
	p1 := new(teonet.Channel)
	p2 := new(teonet.Channel)

	// Create consumers
	c1 := new(teonet.Channel)
	c2 := new(teonet.Channel)

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
	if p.ch != p1 || p.id != 11 {
		t.Error("wrong produser p1")
		return
	}

	p = answers.get(answersData{c2, 21})
	if p == nil {
		t.Error("produser p2 not found")
		return
	}
	if p.ch != p2 || p.id != 11 {
		t.Error("wrong produser p2")
		return
	}

	// Check length
	if answers.len() != 0 {
		t.Error("wrong maps length")
		return
	}
}
