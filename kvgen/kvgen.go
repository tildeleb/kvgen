// © Copyright 2015 Lawrence E. Bakst All Rights Reserved

// kvgen generates random keys and increasing integer values using a fuzzed regular expression.
// The data are stored in a utf8, human readable file.
// The amount of keys generated can be limited by the number of keys or their size.
// kvgen is slow, taking about 2 hours to generate 1 GiB of KVs.
// However kvload is able to load the same file multiple times updating "####" in the key
// with a repitition number.
// I usually sort the keys
// right now you need -v to dump the keys, flush that.
// keys are output on stdout.

package main

import (
	"fmt"
	//"github.com/ymotongpoo/fuzzingo"
	//"leb.io/backup/kvgen/fuzzingo"
	"leb.io/fuzzingo"
	"leb.io/hrff"
	"leb.io/siginfo"
	"log"
	_ "regexp"
	"regexp/syntax"

	"flag"
	"math/rand"
	"sort"
	"sync"
	"time"
)

//var pat string = "[[:alpha:]]{12,25}"
var defaultPat string = "ks####:[a-z]{12,25}"

var workers = flag.Int("w", 1, "number of workers")
var kps = flag.Int("kps", 10, "keys per slice")
var n hrff.Int64 = hrff.Int64{100 * 1000 * 1000, "keys"} // 100M, 1G keys max is not as many as you think
var nbytes hrff.Int64 = hrff.Int64{1 * 1024 * 1024, "B"}
var sf = flag.Bool("s", false, "sort keys")
var vf = flag.Bool("v", false, "verbose")
var pat = flag.String("pat", defaultPat, "key generation pattern")
var vll = flag.Int("vll", 8, "value length low")
var vlh = flag.Int("vlh", 8, "value length high")
var t = flag.Bool("t", false, "tickle")
var tk = flag.Int("tk", 1024*1024, "tickle after this many keys")
var q = flag.Bool("q", false, "quote the integers")

var s = rand.NewSource(time.Now().UTC().UnixNano())
var r = rand.New(s)

type KVM struct {
	sync.Mutex
	m       map[string]int64
	cnt     int64
	size    int64
	skips   int64
	beg     time.Time
	end     time.Time
	prv     time.Time
	lastKey string
}

var M KVM
var wg sync.WaitGroup

//var n = flag.Int("n", 10, "number of keys to generate.")
//var nbytes = flag.Int("nbytes", 10, "number of keys to generate.")

// rbetween returns random int [a, b]
func rbetween(a int, b int) int {
	return r.Intn(b-a+1) + a
}

func (m *KVM) prt() time.Time {
	t := time.Now()
	totalTime := t.Sub(m.beg)
	tickTime := t.Sub(m.prv)
	log.Printf("m[%q]: %H / %H, %H / %H, len(m.m)=%d, skips=%d, totalTime=%v, tickTime=%v\n", m.lastKey,
		hrff.Int64{m.cnt, "keys"}, n, hrff.Int64{m.size, "B"}, nbytes,
		len(m.m), m.skips, totalTime, tickTime)
	return t
}

func lprt() {
	M.Lock()
	defer M.Unlock()
	(&M).prt()
}

func (m *KVM) add(inst int, s []string) bool {
	m.Lock()
	defer m.Unlock()
	for _, r := range s {
		_, ok := m.m[r]
		if ok {
			//fmt.Printf("r=%q, ok=%v\n", r, ok)
			m.skips++
			continue
		}
		m.lastKey = r
		m.m[r] = int64(m.cnt)
		m.cnt++
		m.size += int64(len(r)) + 8
		if *t && m.cnt%int64(*tk) == 0 {
			m.prv = m.prt()
		}
		if int64(nbytes.V) > 0 && m.size > int64(nbytes.V) {
			return true
		}
		if n.V > 0 && m.cnt >= n.V {
			//log.Printf("gen: max keys of %h\n", n)
			return true
		}
	}
	return false
}

func (m *KVM) gen(inst int, pat string) {
	defer wg.Done()
	g, err := fuzz.NewGenerator(pat, syntax.Perl)
	if err != nil {
		log.Printf("%v\n", err)
	}
	if *vf {
		log.Printf("gen[%d]: start\n", inst)
	}
	s := []string{}

	siginfo.SetHandler(lprt)
	m.beg = time.Now()
	m.prv = m.beg
	for {
		for i := 0; i < *kps; i++ {
			r, err := g.Gen()
			if err != nil {
				panic("Gen")
				//log.Printf("%v -> %v: min %v max %v\n", r, err, g.min, g.max)
			}
			s = append(s, r)
		}
		if m.add(inst, s) {
			break
		}
		s = []string{}
	}
	m.end = time.Now()
	tim := m.end.Sub(m.beg)
	if *vf {
		log.Printf("gen[%d] time=%v\n", inst, tim)
	}
}

func run() {
	beg := time.Now()
	M.m = make(map[string]int64, n.V)
	//v := make([]byte, *vlh)
	end := time.Now()
	tim := end.Sub(beg)
	if *vf {
		log.Printf("make time=%v\n", tim)
	}

	/*
		if cnt == 0 {
			fmt.Printf("first=%q\n", r)
		}
		fmt.Printf("(nbytes) last=%q\n", r)
		fmt.Printf("(n) last=%q\n", r)
	*/

	beg = time.Now()
	wg.Add(*workers)
	for i := 0; i < *workers; i++ {
		go (&M).gen(i, *pat)
	}
	wg.Wait()
	end = time.Now()
	tim = end.Sub(beg)

	if *sf {
		if *vf {
			log.Printf("pre sort\n")
		}
		kv := make(sort.StringSlice, M.cnt, M.cnt)
		//i := 0
		for k, v := range M.m {
			//fmt.Printf("m[%q]=%d %T %T\n", k, v, k, v)
			kv[v] = k
			//fmt.Printf("%v\n", m[k])
		}
		if *vf {
			log.Printf("sort\n")
		}
		sort.Sort(kv)
		if *vf {
			log.Printf("dump\n")
			log.Printf("range[%q, %q]\n", kv[0], kv[len(kv)-1])
		}
		for k, v := range kv {
			if *q {
				fmt.Printf("%q \"%d\"\n", v, k)
			} else {
				fmt.Printf("%q %v\n", v, k) // wow, the k is the value, and the v is the key
			}
		}
	} else {
		if *vf {
			log.Printf("dump\n")
		}
		for k, v := range M.m {
			//fmt.Printf("m[%q]=%d\n", k, v)
			if *q {
				fmt.Printf("%q \"%d\"\n", k, v)
			} else {
				fmt.Printf("%q %q\n", k, v)
			}
		}
	}

	mbsec := float64(M.size) / tim.Seconds()
	if *vf {
		log.Printf("generated %d kv of size %h with %d skips @ %h\n", M.cnt, hrff.Int64{M.size, "B"}, M.skips, hrff.Float64{mbsec, "B/sec"})
	}
	return
}

func main() {
	flag.Var(&n, "n", "number of keys to generate")
	flag.Var(&nbytes, "nbytes", "size of keys to generate")
	flag.Parse()
	//fmt.Printf("n=%v\n", n.V)
	//log.Printf("nbytes=%h\n", nbytes)
	run()

	/*
		for i := 0; i < flag.NArg(); i++ {
			fmt.Printf("arg %d=|%s|\n", i, flag.Arg(i))
			pat = flag.Arg(i)
			break
		}
	*/
}
