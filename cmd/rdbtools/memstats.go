package main

import (
	"fmt"
	"sort"

	"github.com/vrischmann/rdbtools"
)

const (
	MaxTopEstimatedSizes = 10
)

type estimatedSize struct {
	key  string
	size int
}

func (e estimatedSize) String() string {
	return fmt.Sprintf("estimatedSize{key: '%s', size: %d}", e.key, e.size)
}

type estimatedSizeList []estimatedSize

func (l estimatedSizeList) Len() int {
	return len(l)
}

func (l estimatedSizeList) Less(i, j int) bool {
	return l[i].size < l[j].size
}

func (l estimatedSizeList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type memStats struct {
	databases  int
	strings    int
	lists      int
	sets       int
	hashes     int
	sortedSets int

	topEstimatedSizes estimatedSizeList
}

func newMemStats() *memStats {
	return &memStats{
		databases:         0,
		strings:           0,
		lists:             0,
		sets:              0,
		hashes:            0,
		sortedSets:        0,
		topEstimatedSizes: make(estimatedSizeList, 0, MaxTopEstimatedSizes),
	}
}

func (s *memStats) updateTopEstimatedSize(key string, size int) {
	exist := false
	for _, e := range s.topEstimatedSizes {
		if e.key == key {
			e.size += size
			exist = true
		}
	}

	if !exist {
		s.topEstimatedSizes = append(s.topEstimatedSizes, estimatedSize{key: key, size: size})
	}

	sort.Sort(sort.Reverse(s.topEstimatedSizes))

	var length int
	if len(s.topEstimatedSizes) < MaxTopEstimatedSizes {
		length = len(s.topEstimatedSizes)
	} else {
		length = MaxTopEstimatedSizes
	}

	s.topEstimatedSizes = s.topEstimatedSizes[:length]
}

func (s *memStats) printStats() {
	fmt.Println("Memory statistics\n")
	fmt.Printf("%-10d databases\n", s.databases)
	fmt.Printf("%-10d strings\n", s.strings)
	fmt.Printf("%-10d lists\n", s.lists)
	fmt.Printf("%-10d sets\n", s.sets)
	fmt.Printf("%-10d hashes\n", s.hashes)
	fmt.Printf("%-10d sorted sets\n", s.sortedSets)
	fmt.Println("\n")
	fmt.Println("Top:")

	for i, e := range s.topEstimatedSizes {
		fmt.Printf("%-10v %-10v\n", i+1, e)
	}
}

func computeMemoryStatistics(format string) {
	defer wg.Done()

	stats := newMemStats()
	var currentKey interface{}

	stop := false
	for !stop {
		select {
		case _, ok := <-ctx.DbCh:
			if !ok {
				ctx.DbCh = nil
				break
			}
			stats.databases++
		case d, ok := <-ctx.StringObjectCh:
			if !ok {
				ctx.StringObjectCh = nil
				break
			}
			stats.strings++
			stats.updateTopEstimatedSize(rdbtools.DataToString(d.Key), len(rdbtools.DataToString(d.Value)))
		case md, ok := <-ctx.ListMetadataCh:
			if !ok {
				ctx.ListMetadataCh = nil
				break
			}
			stats.lists++
			currentKey = md.Key
		case md, ok := <-ctx.SetMetadataCh:
			if !ok {
				ctx.SetMetadataCh = nil
				break
			}
			stats.sets++
			currentKey = md.Key
		case md, ok := <-ctx.HashMetadataCh:
			if !ok {
				ctx.HashMetadataCh = nil
				break
			}
			stats.hashes++
			currentKey = md.Key
		case md, ok := <-ctx.SortedSetMetadataCh:
			if !ok {
				ctx.SortedSetMetadataCh = nil
				break
			}
			stats.sortedSets++
			currentKey = md.Key
		case d, ok := <-ctx.ListDataCh:
			if !ok {
				ctx.ListDataCh = nil
				break
			}

			stats.updateTopEstimatedSize(rdbtools.DataToString(currentKey), len(rdbtools.DataToString(d)))
		case d, ok := <-ctx.SetDataCh:
			if !ok {
				ctx.SetDataCh = nil
				break
			}

			stats.updateTopEstimatedSize(rdbtools.DataToString(currentKey), len(rdbtools.DataToString(d)))
		case d, ok := <-ctx.HashDataCh:
			if !ok {
				ctx.HashDataCh = nil
				break
			}
			entryKey := rdbtools.DataToString(d.Key)
			entryValue := rdbtools.DataToString(d.Value)

			stats.updateTopEstimatedSize(rdbtools.DataToString(currentKey), len(entryKey)+len(entryValue))
		case d, ok := <-ctx.SortedSetEntriesCh:
			if !ok {
				ctx.SortedSetEntriesCh = nil
				break
			}
			entryValue := rdbtools.DataToString(d.Value)
			stats.updateTopEstimatedSize(rdbtools.DataToString(currentKey), len(entryValue))
		}

		if ctx.Invalid() {
			break
		}
	}

	stats.printStats()
}
