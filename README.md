Accumulo RFile performance tests that can easily be run against different versions of Accumulo.

Below shows running the random seek test against Accumulo 1.7.1.  The first runs have to warm up the
cache and are much slower.

```
$ ./run.sh 1.7.1 rst --iterations 400 | tail -25
92
110
92
93
93
92
93
93
92
92
92

DescriptiveStatistics:
n: 400
min: 90.0
max: 1299.0
mean: 125.91750000000026
std dev: 86.71897800721892
median: 107.0
skewness: 8.492186788699055
kurtosis: 95.62165500459447

 index cache stats, request:  400,816  hit ratio:  1.00  miss ratio:  0.00 
  data cache stats, request:  399,950  hit ratio:  0.99  miss ratio:  0.01 

```

Below shows running the random seek test against Accumulo 1.7.2.

```
$ ./run.sh 1.7.2 rst --iterations 400 | tail -25
8
8
8
8
8
8
8
8
8
8
40

DescriptiveStatistics:
n: 400
min: 8.0
max: 892.0
mean: 37.15249999999989
std dev: 74.80078299450791
median: 20.5
skewness: 7.035914544188041
kurtosis: 61.31720635682221

 index cache stats, request:  400,773  hit ratio:  1.00  miss ratio:  0.00 
  data cache stats, request:  399,951  hit ratio:  0.99  miss ratio:  0.01 
```

## Tests

 * **rst** : Random seek test.  Creates an RFile w/ 10M key values and then continually times doing
   1,000 random seeks.
