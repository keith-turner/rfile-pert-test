Accumulo RFile performance tests that can easily be run against different versions of Accumulo.

```bash
./run.sh 1.7.1 rst
./run.sh 1.7.2 rst
```

## Tests

 * **rst** : Random seek test.  Creates an RFile w/ 10M key values and then continually times doing
   1,000 random seeks.
