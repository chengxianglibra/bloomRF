### bloomRF

This is a Java implementation of the EDBT 2023 best paper [bloomRF](https://openproceedings.org/2023/conf/edbt/paper-190.pdf). Just like BloomFilter, bloomRF is an approximate data structure for membership testing, it's supports both point- and range-queries.

### Build

```
mvn clean package
```

### Usage

bloomRF support 4 data types: Integer/Long/Float/Double, usage example is as following:

```java
BloomRF<Integer> rf = BloomRF.intBloomRF(256);
rf.add(1);
rf.add(5);
rf.add(23);
boolean isNull = rf.isNull();
boolean isNotNull = rf.isNotNull();
boolean isExisted = rf.exists(1);
boolean isExistedInRange = rf.existsInRange(10, true, 20, false);
boolean isGreaterThan = rf.greaterThan(10);
boolean isGtEq = rf.greatOrEqualsThan(10);
boolean isLessThan = rf.lessThan(10);
boolean isltEq = rf.lessOrEqualsThan(10);
```

