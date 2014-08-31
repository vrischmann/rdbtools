// Package rdbtools is a Redis RDB snapshot file parser.
//
// Parsing a file
//
// Example of how to parse a RDB file.
//
//  ctx := rdbtools.ParserContext{
//  	ListMetadataCh: make(chan rdbtools.ListMetadata),
//  	ListDataCh: make(chan interface{}),
//  }
//  p := rdbtools.NewParser(ctx)
//
//  go func() {
//  	stop := false
//  	for !stop {
//  		select {
//  		case md, ok := <-ctx.ListMetadataCh:
//  			if !ok {
//  				ctx.ListMetadataCh = nil
//  				break
//  			}
//
//  			// do something with the metadata
//  		case d, ok := <-ctx.ListDataCh:
//  			if !ok {
//  				ctx.ListDataCh = nil
//  				break
//  			}
//
//  			str := rdbtools.DataToString(d)
//  			// do something with the string
//  		}
//
//  		if ctx.Invalid() {
//  			break
//  		}
//  	}
//  }()
//
//  f, _ := os.Open("/var/lib/redis/dump.rdb")
//  if err := p.Parse(f); err != nil {
//  	log.Fatalln(err)
//  }
//
// The context holds the channels you will use to receive data from the parser.
// You only need to provide a channel if you care about it.
// In the example above, we only care about the lists in the RDB file, so we don't
// provide all the other channels.
//
// The parser only has one method Parse(ParserContext) which takes a context. After a call to Parse,
// the parser can't be reused. We plan to change that though.
//
// Why interfaces everywhere
//
// interface{} is used everywhere in rdbtools. The reason is simple: in RDB files, keys and values
// can be encoded as strings or integers or even binary data.
// You might call a key "1" but Redis will happily encode that as an integer.
//
// The majority of the time you will have strings in your keys, and a lot of the times your values
// will be strings as well.
// For this reason, we provided the function DataToString(interface{}) which takes care of casting
// or converting to a string.
package rdbtools
