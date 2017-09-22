// © Copyright 2015, 2017 Lawrence E. Bakst
// All Rights Reserved

// This package loads kv pairs from a file with whitespace delimited kv pairs.
// comments below out of date
// The package is neither memory or time efficient.
// Try generating a 1 GB file and repeating it 1024 times to get 1 TiB
// The keys are expected to be delimited by doiuble quotes.
// Values are int64
// Since unqiue keys are expensive to generate the same file can be loaded more than once.
// The #### chars in the key will be replaced by an incrementing number.
// Values will be adjusted to be monotonically increasing.

package main

import (
	"bufio"
	_ "context"
	//gosql "database/sql"
	"flag"
	"fmt"
	logg "log"
	"os"
	_ "path/filepath"
	_ "strconv"
	"strings"
	"sync"
	_ "time"

	// "golang.org/x/net/context"

	"leb.io/cuckoo"
	_ "leb.io/hrff"
)

var wg sync.WaitGroup
var path = "-" // "../kvgen/kv.txt"
var files []string
var default_url = "rpc://root@hula.home:8080" // rpc://root@hula.home:8080 26257
var url = flag.String("url", default_url, "URL to connect to a running cockroach cluster.")
var b = flag.Bool("b", true, "use batches.")
var vf = flag.Bool("v", false, "verbose.")
var pr = flag.Bool("pr", false, "progress indicator.")
var t = flag.Int("t", 4, "number of tables.")
var s = flag.Int("s", 8, "number of slots.")
var n = flag.Int("n", 10, "number of kvs.")
var o = flag.Int("o", 1000, "number of operations per batch")
var p = flag.Bool("p", false, "load files in parallel.")
var reps = flag.Int("reps", 1, "number of reps.")
var af = flag.Float64("cf", 100.0, "additive factor.")
var mf = flag.Float64("mf", 1.0, "multiplicative factor.")
var lf = flag.Float64("lf", 1.0, "load factor.")

type Puts interface {
	PutKVI(key string, val int) bool
	BatchPutKVI(kv map[string]int) bool
	PutKVS(key string, val string) bool
	BatchPutKVS(kv map[string]string) bool
}

type Inserter interface {
	Insert(key cuckoo.Key, val cuckoo.Value) (ok bool)
}

func putKV(i Inserter, key string, val string) {
	//fmt.Printf("putKV: k=%q, v=%q\n", key, val)
	ok := i.Insert(cuckoo.Key(key), cuckoo.Value(val))
	if !ok {
		panic("putKV: Insert")
	}
}

func process(i Inserter, kfile string) {
	//var base, sav, batches int
	var file *os.File
	var quit bool
	//var ReadTime, WriteTime time.Duration
	var scanner *bufio.Scanner
	var check = func() {
		if err := scanner.Err(); err != nil {
			logg.Fatal(err)
		}
	}
	var fixk = func(r int, k string) string {
		if strings.ContainsAny(k, "#") {
			first := strings.Index(k, "#")
			last := strings.LastIndex(k, "#")
			width := last - first + 1
			f := fmt.Sprintf("%%0%dd", width)
			newstr := fmt.Sprintf(f, r)
			prefix := ""
			if first > 0 {
				prefix = k[0:first]
			}
			s := prefix + newstr + k[last+1:]
			return s
		}
		return k
	}
	var get = func() string {
		defer func() {
			if x := recover(); x != nil {
				quit = true
				//fmt.Printf("\n")
			}
		}()
		b := scanner.Scan()
		if !b {
			//fmt.Printf("eof\n")
			panic("eof")
		}
		s := scanner.Text()
		//fmt.Printf("get: b=%v, s=|%s|\n", b, s)
		return s
	}
	/*
		var getKV = func(r int) (string, int) {
			k := get()
			if quit {
				return "", -1
			}
			k = k[1 : len(k)-1]
			v, _ := strconv.Atoi(get())
			k = fixk(r, k)
			return k, v
		}
	*/
	var getKV = func(r int) (string, string) {
		k := get()
		if quit {
			return "", ""
		}
		k = k[1 : len(k)-1]
		v := get()
		v = v[1 : len(v)-1]
		k = fixk(r, k)
		return k, v
	}
	var open = func(kfile string) {
		var err error
		if kfile == "-" {
			file = os.Stdin
		} else {
			if file != nil {
				file.Close()
				file = nil
			}
			file, err = os.Open(kfile)
			if err != nil {
				logg.Fatal(err)
			}
		}
		scanner = bufio.NewScanner(file)
		scanner.Split(bufio.ScanWords) // ScanLines
		check()
	}

	//fmt.Printf("process: %q\n", kfile)
	defer wg.Done()
	//siz := 0
	//cnt := 0
	//mb := 1

	open(kfile)

	for r := 0; r < *reps; r++ {
		for {
			k, v := getKV(r)
			if quit {
				break
			}
			if *vf {
				logg.Printf("k=%s, v=%v\n", k, v)
			}
			putKV(i, k, v)
		}
		quit = false
		file.Close()
		open(kfile)
	}
	file.Close()
	fmt.Printf("\n")
	//fmt.Printf("avg batch ReadTime=%h\n", hrff.Float64{float64(ReadTime.Seconds()) / float64(batches), "secs"})
	//fmt.Printf("avg batch WriteTime=%h\n", hrff.Float64{float64(WriteTime.Seconds()) / float64(batches), "secs"})
}

func load(i Inserter, files []string) {
	wg.Add(len(files))
	fmt.Printf("start\n")
	for _, file := range files {
		go process(i, file)
	}
	fmt.Printf("wait\n")
	wg.Wait()
	fmt.Printf("end\n")
}

func main() {
	flag.Parse()
	//fmt.Printf("url=%q\n", *url)

	b := -int(float64(*n)**mf+*af) / (*t * *s)
	i := cuckoo.New(*t, b, *s, 0, *lf, "aes")
	fmt.Printf("t=%d, b=%d/%d, s=%d, n=%d\n", *t, b, i.Buckets, *s, *n)
	for i := 0; i < flag.NArg(); i++ {
		files = append(files, flag.Arg(i))
	}
	if *p {
		load(i, files)
	} else {
		for _, file := range files {
			fmt.Printf("load: %q\n", file)
			load(i, []string{file})
		}
	}
	fmt.Printf("%#v\n", i.Counters)
	for _, v := range i.TableCounters {
		fmt.Printf("%#v\n", v)
	}
}
