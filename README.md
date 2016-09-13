Accumulo RFile performance tests that can easily be run against different versions of Accumulo.

Below shows running the random seek test against Accumulo 1.7.1.

```
$ ./run.sh 1.7.1 rst 200 | tail
99
98
96
98
95
97
97
98
200455 200449 6
199980 195966 4014
```

Below shows running the random seek test against Accumulo 1.7.2.

```
$ ./run.sh 1.7.2 rst 200 | tail
12
11
11
12
11
13
12
12
200457 200451 6
199970 195956 4014
```

## Tests

 * **rst** : Random seek test.  Creates an RFile w/ 10M key values and then continually times doing
   1,000 random seeks.
