# sirius
Rest based Couchbase doc loader for QA tests written in Golang.
**MakeFile** can be used to compile and run sirius. We can directly build it on a local environment or docker container.


Doc loader different capabilities are  described using  [**REST ENDPOINT**](task-config.generated.md).

**Start sirius directly**
```shell
make run
```
**Deploy sirius on docker ( Make sure docker desktop is running)**
```shell
make deploy
```
**To stop sirius after docker deployment**
```shell
make down
```
**Steps to compile and execute sirius**
```textmate
1. Execute make command
    $ make up_build

2. ./sirius    
```