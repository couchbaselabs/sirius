# sirius
Rest based Couchbase doc loader for QA tests written in Golang.

```text
Before building on local, add the following directories to you local copy of the repo.
$ mkdir ./internal/tasks-manager/result-logs
$ mkdir ./internal/tasks-manager/task-state
```

```textmate
Steps to build sirius
1. Execute make command
    $ make up_build

2. ./sirius    
```

Start server directly  
```textmate
  $ make run
```

Deploy sirius on docker ( Make sure docker desktop is running)
```textmate
$ make deploy
```
To stop sirius after docker deployment
```textmate
$ make down
```