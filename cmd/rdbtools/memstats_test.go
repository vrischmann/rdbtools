package main

import "testing"

func TestLessThanMax(t *testing.T) {
	s := newMemStats()

	s.updateTopEstimatedSize("b", 20)
	s.updateTopEstimatedSize("a", 10)
	s.updateTopEstimatedSize("foobar", 100)

	expected := estimatedSize{key: "foobar", size: 100}
	if s.topEstimatedSizes[0] != expected {
		t.Errorf("element 0 %s != %s", s.topEstimatedSizes[0], expected)
	}

	expected = estimatedSize{key: "b", size: 20}
	if s.topEstimatedSizes[1] != expected {
		t.Errorf("element 1 %s != %s", s.topEstimatedSizes[1], expected)
	}

	expected = estimatedSize{key: "a", size: 10}
	if s.topEstimatedSizes[2] != expected {
		t.Errorf("element 2 %s != %s", s.topEstimatedSizes[2], expected)
	}
}

func TestEqualsMax(t *testing.T) {
	s := newMemStats()

	for i, e := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		s.updateTopEstimatedSize(e, i)
	}

	if len(s.topEstimatedSizes) != 10 {
		t.Errorf("wrong length %d ; expected %d", len(s.topEstimatedSizes), 10)
	}

	for i := 0; i < 10; i++ {
		expected := estimatedSize{key: string(97 + (9 - i)), size: (9 - i)}
		if s.topEstimatedSizes[i] != expected {
			t.Errorf("element %d %s != %s", i, s.topEstimatedSizes[i], expected)
		}
	}
}

func TestGreaterThanMax(t *testing.T) {
	s := newMemStats()

	for i, e := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o"} {
		s.updateTopEstimatedSize(e, i)
	}

	if len(s.topEstimatedSizes) != 10 {
		t.Errorf("wrong length %d ; expected %d", len(s.topEstimatedSizes), 10)
	}

	for i := 0; i < 10; i++ {
		expected := estimatedSize{key: string(97 + (14 - i)), size: (14 - i)}
		if s.topEstimatedSizes[i] != expected {
			t.Errorf("element %d %s != %s", i, s.topEstimatedSizes[i], expected)
		}
	}
}
