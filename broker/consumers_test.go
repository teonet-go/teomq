package broker

import (
	"testing"

	"github.com/teonet-go/teonet"
)

func TestConsumers(t *testing.T) {

	// create consumers list
	consumers := newConsumers()

	const numOfConsumers = 2

	// Create consumers and add it to consumers list
	c1 := new(teonet.Channel)
	if err := consumers.add(c1); err != nil {
		t.Errorf("can't add %p consumer, error: %s", c1, err)
		return
	}
	c2 := new(teonet.Channel)
	if err := consumers.add(c2); err != nil {
		t.Errorf("can't add %p consumer, error: %s", c1, err)
		return
	}

	// Check length of consumers list
	l := consumers.len()
	if l != numOfConsumers {
		t.Error("wrong consumers length")
		return
	}

	// Get consumers from list
	for i := 0; i < numOfConsumers; i++ {
		ch, err := consumers.get()
		if err != nil {
			t.Errorf("can't get %d consumer", i+1)
			return
		}
		var initCh *teonet.Channel
		switch i + 1 {
		case 1:
			initCh = c1
		case 2:
			initCh = c2
		}
		if ch != initCh {
			t.Errorf("get return wrong channel %p", ch)
			return
		}
	}

	// Remove c1 channel
	if err := consumers.del(c1); err != nil {
		t.Errorf("can't remove %p consumer, error: %s", c1, err)
		return
	}

	// Get should return c2 channel
	if ch, _ := consumers.get(); ch != c2 {
		t.Errorf("get return wrong channel %p", ch)
		return
	}

	// Remove c2 channel
	if err := consumers.del(c2); err != nil {
		t.Errorf("can't remove %p consumer, error: %s", c1, err)
		return
	}

	// Get should return nil
	if ch, err := consumers.get(); err == nil && ch != nil {
		t.Errorf("get return wrong channel %p", ch)
		return
	}

}
