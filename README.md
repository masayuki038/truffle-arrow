# truffle-arrow (WIP)

truffle-arrow is a query engine that executes SQL against [Apache Arrow](https://arrow.apache.org/) data using [Apache Calcite](https://calcite.apache.org/) and [oracle/graal](https://github.com/oracle/graal/tree/master/truffle).

This is forked from [fivetran/truffle-sql](https://github.com/fivetran/truffle-sql).

## Build

```
mvn package
```

## Run

```
graalvm/graalvm-ce-java8-20.1.0/jre/bin/java -XX:-UseJVMCIClassLoader -Dgraalvm.locatorDisabled=true -cp .;.\log4j-slf4j-impl-2.13.0.jar;.\truffle-arrow.jar net.wrap_trap.truffle_arrow.Server
```
