package rdbtools

import (
	"os"
	"strings"
	"testing"
)

// This file contains tests for all the dumps in the dumps/ directory
// Those dumps are taken from https://github.com/sripathikrishnan/redis-rdb-tools

func mustOpen(t *testing.T, path string) *os.File {
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Error while opening file '%s'; err=%s", path, err)
	}

	return f
}

func doParse(t *testing.T, p *Parser, path string) {
	err := p.Parse(mustOpen(t, path))
	if err != nil {
		p.ctx.closeChannels()
		t.Fatalf("Error while parsing '%s'; err=%s", path, err)
	}
}

func TestDumpDictionary(t *testing.T) {
	p := NewParser(ParserContext{
		HashMetadataCh: make(chan HashMetadata),
		HashDataCh:     make(chan HashEntry),
	})

	go doParse(t, p, "dumps/dictionary.rdb")

	var i, j int
	stop := false
	for !stop {
		select {
		case _, ok := <-p.ctx.HashMetadataCh:
			if !ok {
				p.ctx.HashMetadataCh = nil
				break
			}
			i++
		case _, ok := <-p.ctx.HashDataCh:
			if !ok {
				p.ctx.HashDataCh = nil
				break
			}
			j++
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, 1, i)
	equals(t, 1000, j)
}

func TestDumpEasilyCompressibleStringKey(t *testing.T) {
	p := NewParser(ParserContext{
		StringObjectCh: make(chan StringObject),
	})

	go doParse(t, p, "dumps/easily_compressible_string_key.rdb")

	stop := false
	for !stop {
		select {
		case d, ok := <-p.ctx.StringObjectCh:
			if !ok {
				p.ctx.StringObjectCh = nil
				break
			}

			equals(t, strings.Repeat("a", 200), DataToString(d.Key.Key))
			equals(t, true, d.Key.ExpiryTime.IsZero())
			equals(t, "Key that redis should compress easily", DataToString(d.Value))
		}

		if p.ctx.Invalid() {
			break
		}
	}
}

func TestDumpEmptyDatabase(t *testing.T) {
	p := NewParser(ParserContext{})

	doParse(t, p, "dumps/empty_database.rdb")
}

func TestDumpHashAsZipList(t *testing.T) {
	p := NewParser(ParserContext{
		HashMetadataCh: make(chan HashMetadata),
		HashDataCh:     make(chan HashEntry),
	})

	go doParse(t, p, "dumps/hash_as_ziplist.rdb")

	res := make([]HashEntry, 0)
	stop := false
	for !stop {
		select {
		case md, ok := <-p.ctx.HashMetadataCh:
			if !ok {
				p.ctx.HashMetadataCh = nil
				break
			}
			equals(t, int64(3), md.Len)
			equals(t, "zipmap_compresses_easily", DataToString(md.Key.Key))
		case d, ok := <-p.ctx.HashDataCh:
			if !ok {
				p.ctx.HashDataCh = nil
				break
			}
			res = append(res, d)
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, "a", DataToString(res[0].Key))
	equals(t, "aa", DataToString(res[0].Value))
	equals(t, "aa", DataToString(res[1].Key))
	equals(t, "aaaa", DataToString(res[1].Value))
	equals(t, "aaaaa", DataToString(res[2].Key))
	equals(t, "aaaaaaaaaaaaaa", DataToString(res[2].Value))
}

func TestDumpIntegerKeys(t *testing.T) {
	p := NewParser(ParserContext{
		StringObjectCh: make(chan StringObject),
	})

	go doParse(t, p, "dumps/integer_keys.rdb")

	res := make([]StringObject, 0)
	stop := false
	for !stop {
		select {
		case d, ok := <-p.ctx.StringObjectCh:
			if !ok {
				p.ctx.StringObjectCh = nil
				break
			}

			res = append(res, d)
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, int32(183358245), res[0].Key.Key.(int32))
	equals(t, "Positive 32 bit integer", DataToString(res[0].Value))
	equals(t, int8(125), res[1].Key.Key.(int8))
	equals(t, "Positive 8 bit integer", DataToString(res[1].Value))
	equals(t, int16(-29477), res[2].Key.Key.(int16))
	equals(t, "Negative 16 bit integer", DataToString(res[2].Value))
	equals(t, int8(-123), res[3].Key.Key.(int8))
	equals(t, "Negative 8 bit integer", DataToString(res[3].Value))
	equals(t, int32(43947), res[4].Key.Key.(int32))
	equals(t, "Positive 16 bit integer", DataToString(res[4].Value))
	equals(t, int32(-183358245), res[5].Key.Key.(int32))
	equals(t, "Negative 32 bit integer", DataToString(res[5].Value))
}

func TestDumpIntSet16(t *testing.T) {
	p := NewParser(ParserContext{
		SetMetadataCh: make(chan SetMetadata),
		SetDataCh:     make(chan interface{}),
	})

	go doParse(t, p, "dumps/intset_16.rdb")

	res := make([]int16, 0)
	stop := false
	for !stop {
		select {
		case md, ok := <-p.ctx.SetMetadataCh:
			if !ok {
				p.ctx.SetMetadataCh = nil
				break
			}

			equals(t, "intset_16", DataToString(md.Key))
			equals(t, int64(3), md.Len)
		case d, ok := <-p.ctx.SetDataCh:
			if !ok {
				p.ctx.SetDataCh = nil
				break
			}

			res = append(res, d.(int16))
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, int16(32764), res[0])
	equals(t, int16(32765), res[1])
	equals(t, int16(32766), res[2])
}

func TestDumpIntSet32(t *testing.T) {
	p := NewParser(ParserContext{
		SetMetadataCh: make(chan SetMetadata),
		SetDataCh:     make(chan interface{}),
	})

	go doParse(t, p, "dumps/intset_32.rdb")

	res := make([]int32, 0)
	stop := false
	for !stop {
		select {
		case md, ok := <-p.ctx.SetMetadataCh:
			if !ok {
				p.ctx.SetMetadataCh = nil
				break
			}

			equals(t, "intset_32", DataToString(md.Key))
			equals(t, int64(3), md.Len)
		case d, ok := <-p.ctx.SetDataCh:
			if !ok {
				p.ctx.SetDataCh = nil
				break
			}

			res = append(res, d.(int32))
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, int32(2147418108), res[0])
	equals(t, int32(2147418109), res[1])
	equals(t, int32(2147418110), res[2])
}

func TestDumpIntSet64(t *testing.T) {
	p := NewParser(ParserContext{
		SetMetadataCh: make(chan SetMetadata),
		SetDataCh:     make(chan interface{}),
	})

	go doParse(t, p, "dumps/intset_64.rdb")

	res := make([]int64, 0)
	stop := false
	for !stop {
		select {
		case md, ok := <-p.ctx.SetMetadataCh:
			if !ok {
				p.ctx.SetMetadataCh = nil
				break
			}

			equals(t, "intset_64", DataToString(md.Key))
			equals(t, int64(3), md.Len)
		case d, ok := <-p.ctx.SetDataCh:
			if !ok {
				p.ctx.SetDataCh = nil
				break
			}

			res = append(res, d.(int64))
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, int64(9223090557583032316), res[0])
	equals(t, int64(9223090557583032317), res[1])
	equals(t, int64(9223090557583032318), res[2])
}

func TestDumpKeysWithExpiry(t *testing.T) {
	p := NewParser(ParserContext{
		StringObjectCh: make(chan StringObject),
	})

	go doParse(t, p, "dumps/keys_with_expiry.rdb")

	stop := false
	for !stop {
		select {
		case v, ok := <-p.ctx.StringObjectCh:
			if !ok {
				p.ctx.StringObjectCh = nil
				break
			}
			equals(t, "expires_ms_precision", DataToString(v.Key.Key))
			equals(t, "2022-12-25 10:11:12 +0000 UTC", v.Key.ExpiryTime.UTC().String())
			equals(t, "2022-12-25 10:11:12.573 UTC", DataToString(v.Value))
		}

		if p.ctx.Invalid() {
			break
		}
	}
}

func TestDumpLinkedList(t *testing.T) {
	p := NewParser(ParserContext{
		ListMetadataCh: make(chan ListMetadata),
		ListDataCh:     make(chan interface{}),
	})

	go doParse(t, p, "dumps/linkedlist.rdb")

	i := 0
	stop := false
	for !stop {
		select {
		case md, ok := <-p.ctx.ListMetadataCh:
			if !ok {
				p.ctx.ListMetadataCh = nil
				break
			}

			equals(t, "force_linkedlist", DataToString(md.Key))
			equals(t, int64(1000), md.Len)
		case d, ok := <-p.ctx.ListDataCh:
			if !ok {
				p.ctx.ListDataCh = nil
				break
			}

			equals(t, 50, len(DataToString(d)))
			i++
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, 1000, i)
}

func TestDumpMultipleDatabases(t *testing.T) {
	p := NewParser(ParserContext{
		DbCh:           make(chan int),
		StringObjectCh: make(chan StringObject),
	})

	go doParse(t, p, "dumps/multiple_databases.rdb")

	data := make(map[int]StringObject)
	var db int
	stop := false
	for !stop {
		select {
		case d, ok := <-p.ctx.DbCh:
			if !ok {
				p.ctx.DbCh = nil
				break
			}

			db = d
		case d, ok := <-p.ctx.StringObjectCh:
			if !ok {
				p.ctx.StringObjectCh = nil
				break
			}

			data[db] = d
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, "key_in_zeroth_database", DataToString(data[0].Key.Key))
	equals(t, "zero", DataToString(data[0].Value))
	equals(t, "key_in_second_database", DataToString(data[2].Key.Key))
	equals(t, "second", DataToString(data[2].Value))
}

func TestDumpParserFilters(t *testing.T) {
	p := NewParser(ParserContext{
		DbCh:                make(chan int),
		StringObjectCh:      make(chan StringObject),
		ListMetadataCh:      make(chan ListMetadata),
		ListDataCh:          make(chan interface{}),
		SetMetadataCh:       make(chan SetMetadata),
		SetDataCh:           make(chan interface{}),
		HashMetadataCh:      make(chan HashMetadata),
		HashDataCh:          make(chan HashEntry),
		SortedSetMetadataCh: make(chan SortedSetMetadata),
		SortedSetEntriesCh:  make(chan SortedSetEntry),
	})

	go doParse(t, p, "dumps/parser_filters.rdb")

	strings := make([]StringObject, 0)
	lists := make(map[string][]interface{}, 0)
	var currentList string
	sets := make(map[string][]interface{}, 0)
	var currentSet string

	stop := false
	for !stop {
		select {
		case v, ok := <-p.ctx.DbCh:
			if !ok {
				p.ctx.DbCh = nil
				break
			}
			equals(t, int(0), v)
		case v, ok := <-p.ctx.StringObjectCh:
			if !ok {
				p.ctx.StringObjectCh = nil
				break
			}
			strings = append(strings, v)
		case v, ok := <-p.ctx.ListMetadataCh:
			if !ok {
				p.ctx.ListMetadataCh = nil
				break
			}
			lists[DataToString(v.Key.Key)] = make([]interface{}, 0)
			currentList = DataToString(v.Key.Key)
		case v, ok := <-p.ctx.ListDataCh:
			if !ok {
				p.ctx.ListDataCh = nil
				break
			}
			lists[currentList] = append(lists[currentList], v)
		case v, ok := <-p.ctx.SetMetadataCh:
			if !ok {
				p.ctx.SetMetadataCh = nil
				break
			}
			sets[DataToString(v.Key.Key)] = make([]interface{}, 0)
			currentSet = DataToString(v.Key.Key)
		case v, ok := <-p.ctx.SetDataCh:
			if !ok {
				p.ctx.SetDataCh = nil
				break
			}
			sets[currentSet] = append(sets[currentSet], v)
		case _, ok := <-p.ctx.SortedSetMetadataCh:
			if !ok {
				p.ctx.SortedSetMetadataCh = nil
				break
			}
		case _, ok := <-p.ctx.SortedSetEntriesCh:
			if !ok {
				p.ctx.SortedSetEntriesCh = nil
				break
			}
		case _, ok := <-p.ctx.HashMetadataCh:
			if !ok {
				p.ctx.HashMetadataCh = nil
				break
			}
		case _, ok := <-p.ctx.HashDataCh:
			if !ok {
				p.ctx.HashDataCh = nil
				break
			}
		}

		if p.ctx.Invalid() {
			break
		}
	}

	// Lists
	equals(t, false, lists["l1"] == nil)
	equals(t, "yup", DataToString(lists["l1"][0]))
	equals(t, "aha", DataToString(lists["l1"][1]))

	equals(t, false, lists["l2"] == nil)
	equals(t, "something", DataToString(lists["l2"][0]))
	equals(t, "now a bit longer and perhaps more interesting", DataToString(lists["l2"][1]))

	equals(t, false, lists["l3"] == nil)
	equals(t, "this one is going to be longer -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------", DataToString(lists["l3"][0]))
	equals(t, "a bit more", DataToString(lists["l3"][1]))

	equals(t, false, lists["l4"] == nil)
	equals(t, "b", DataToString(lists["l4"][0]))
	equals(t, "c", DataToString(lists["l4"][1]))
	equals(t, "d", DataToString(lists["l4"][2]))

	equals(t, false, lists["l5"] == nil)
	equals(t, "c", DataToString(lists["l5"][0]))
	equals(t, "a", DataToString(lists["l5"][1]))

	equals(t, false, lists["l6"] == nil)
	equals(t, "b", DataToString(lists["l6"][0]))

	equals(t, false, lists["l7"] == nil)
	equals(t, "a", DataToString(lists["l7"][0]))
	equals(t, "b", DataToString(lists["l7"][1]))

	equals(t, false, lists["l8"] == nil)
	equals(t, "c", DataToString(lists["l8"][0]))
	equals(t, int16(1), lists["l8"][1])
	equals(t, int16(2), lists["l8"][2])
	equals(t, int16(3), lists["l8"][3])
	equals(t, int16(4), lists["l8"][4])

	equals(t, false, lists["l9"] == nil)
	equals(t, int16(10001), lists["l9"][0])
	equals(t, int16(10002), lists["l9"][1])
	equals(t, int16(10003), lists["l9"][2])
	equals(t, int16(10004), lists["l9"][3])

	equals(t, false, lists["l10"] == nil)
	equals(t, int32(100001), lists["l10"][0])
	equals(t, int32(100002), lists["l10"][1])
	equals(t, int32(100003), lists["l10"][2])
	equals(t, int32(100004), lists["l10"][3])

	equals(t, false, lists["l11"] == nil)
	equals(t, int64(9999999999), lists["l11"][0])
	equals(t, int64(9999999998), lists["l11"][1])
	equals(t, int64(9999999997), lists["l11"][2])

	equals(t, false, lists["l12"] == nil)
	equals(t, int64(9999999997), lists["l12"][0])
	equals(t, int64(9999999998), lists["l12"][1])
	equals(t, int64(9999999999), lists["l12"][2])

	// Strings
	equals(t, "k1", DataToString(strings[0].Key.Key))
	equals(t, "ssssssss", DataToString(strings[0].Value))

	equals(t, "k3", DataToString(strings[1].Key.Key))
	equals(t, "wwwwwwww", DataToString(strings[1].Value))

	equals(t, "s1", DataToString(strings[2].Key.Key))
	equals(t, `.ahaa bit longer and with spaceslonger than 256 characters and trivially compressible --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------`, DataToString(strings[2].Value))

	equals(t, "s2", DataToString(strings[3].Key.Key))
	equals(t, "now_exists", DataToString(strings[3].Value))

	equals(t, "n5b", DataToString(strings[4].Key.Key))
	equals(t, int16(1000), strings[4].Value.(int16))

	equals(t, "b1", DataToString(strings[5].Key.Key))
	equals(t, []byte{0xFF}, strings[5].Value.([]byte))

	equals(t, "b2", DataToString(strings[6].Key.Key))
	equals(t, []byte{0, 0xFF}, strings[6].Value.([]byte))

	equals(t, "b3", DataToString(strings[7].Key.Key))
	equals(t, []byte{0, 0, 0xFF}, strings[7].Value.([]byte))

	equals(t, "b4", DataToString(strings[8].Key.Key))
	equals(t, []byte{0, 0, 0, 0xFF}, strings[8].Value.([]byte))

	equals(t, "b5", DataToString(strings[9].Key.Key))
	equals(t, []byte{0, 0, 0, 0, 0xFF}, strings[9].Value.([]byte))

	equals(t, "n1", DataToString(strings[10].Key.Key))
	equals(t, int8(-6), strings[10].Value.(int8))

	equals(t, "n2", DataToString(strings[11].Key.Key))
	equals(t, int16(501), strings[11].Value.(int16))

	equals(t, "n3", DataToString(strings[12].Key.Key))
	equals(t, int32(500001), strings[12].Value.(int32))

	equals(t, "n4", DataToString(strings[13].Key.Key))
	equals(t, int8(1), strings[13].Value.(int8))

	equals(t, "n5", DataToString(strings[14].Key.Key))
	equals(t, int16(1000), strings[14].Value.(int16))

	equals(t, "n6", DataToString(strings[15].Key.Key))
	equals(t, int32(1000000), strings[15].Value.(int32))

	equals(t, "n4b", DataToString(strings[16].Key.Key))
	equals(t, int8(1), strings[16].Value.(int8))

	equals(t, "n6b", DataToString(strings[17].Key.Key))
	equals(t, int32(1000000), strings[17].Value.(int32))

	// Sets
	equals(t, false, sets["set1"] == nil)
	equals(t, []interface{}{[]byte{0x63}, []byte{0x64}, []byte{0x61}, []byte{0x62}}, sets["set1"])

	equals(t, false, sets["set2"] == nil)
	equals(t, []interface{}{[]byte{0x64}, []byte{0x61}}, sets["set2"])

	equals(t, false, sets["set3"] == nil)
	equals(t, []interface{}{[]byte{0x62}}, sets["set3"])

	equals(t, false, sets["set4"] == nil)
	equals(t, []interface{}{int16(1), int16(2), int16(3), int16(4), int16(5), int16(6), int16(7), int16(8), int16(9), int16(10)}, sets["set4"])

	equals(t, false, sets["set5"] == nil)
	equals(t, []interface{}{int32(100000), int32(100001), int32(100002), int32(100003)}, sets["set5"])
}

func TestDumpWithChecksum(t *testing.T) {
	p := NewParser(ParserContext{
		StringObjectCh: make(chan StringObject),
	})

	go doParse(t, p, "dumps/rdb_version_5_with_checksum.rdb")

	stop := false
	res := make([]StringObject, 0)
	for !stop {
		select {
		case v, ok := <-p.ctx.StringObjectCh:
			if !ok {
				p.ctx.StringObjectCh = nil
				break
			}

			res = append(res, v)
		}

		if p.ctx.Invalid() {
			break
		}
	}

	equals(t, "abcd", DataToString(res[0].Key.Key))
	equals(t, true, res[0].Key.ExpiryTime.IsZero())
	equals(t, "efgh", DataToString(res[0].Value))

	equals(t, "foo", DataToString(res[1].Key.Key))
	equals(t, true, res[1].Key.ExpiryTime.IsZero())
	equals(t, "bar", DataToString(res[1].Value))

	equals(t, "bar", DataToString(res[2].Key.Key))
	equals(t, true, res[2].Key.ExpiryTime.IsZero())
	equals(t, "baz", DataToString(res[2].Value))

	equals(t, "abcdef", DataToString(res[3].Key.Key))
	equals(t, true, res[3].Key.ExpiryTime.IsZero())
	equals(t, "abcdef", DataToString(res[3].Value))

	equals(t, "longerstring", DataToString(res[4].Key.Key))
	equals(t, true, res[4].Key.ExpiryTime.IsZero())
	equals(t, "thisisalongerstring.idontknowwhatitmeans", DataToString(res[4].Value))

	equals(t, "abc", DataToString(res[5].Key.Key))
	equals(t, true, res[5].Key.ExpiryTime.IsZero())
	equals(t, "def", DataToString(res[5].Value))
}

func TestDumpRegularSet(t *testing.T) {

}

func TestDumpRegularSortedSet(t *testing.T) {

}

func TestDumpSortedSetAsZipList(t *testing.T) {

}

func TestDumpUncompressibleStringKeys(t *testing.T) {

}

func TestDumpZipListThatCompressesEasily(t *testing.T) {

}

func TestDumpZipListThatDoesntCompress(t *testing.T) {

}

func TestDumpZipListWithIntegers(t *testing.T) {

}

func TestDumpZipMapThatCompressesEasily(t *testing.T) {

}

func TestDumpZipMapThatDoesntCompress(t *testing.T) {

}

func TestDumpZipMapWithBigValues(t *testing.T) {

}
