#kvtools
A collection of tools for KV databases and hash tables.

##kvgen

	Usage of ./kvgen:
	  -kps int
	    	keys per slice (default 10)
	  -n value
	    	number of keys to generate (default 100 Mkeys)
	  -nbytes value
	    	size of keys to generate (default 1 MB)
	  -pat string
	    	key generation pattern (default "ks####:[a-z]{12,25}")
	  -q	quote the integers
	  -s	sort keys
	  -t	tickle
	  -tk int
	    	tickle after this many keys (default 1048576)
	  -v	verbose
	  -vlh int
	    	value length high (default 8)
	  -vll int
	    	value length low (default 8)
	  -w int
	    	number of workers (default 1)


##kvload

To build do:

	go build -tags=string   
 

	Usage of ./kvload:
	  -b	use batches. (default true)
	  -cf float
	    	additive factor. (default 100)
	  -lf float
	    	load factor. (default 1)
	  -mf float
	    	multiplicative factor. (default 1)
	  -n int
	    	number of kvs. (default 10)
	  -o int
	    	number of operations per batch (default 1000)
	  -p	load files in parallel.
	  -pr
	    	progress indicator.
	  -reps int
	    	number of reps. (default 1)
	  -s int
	    	number of slots. (default 8)
	  -t int
	    	number of tables. (default 4)
	  -url string
	    	URL to connect to a running cockroach cluster. (default "rpc://root@hula.home:8080")
	  -v	verbose.
