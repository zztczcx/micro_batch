## MicroBatch

This is a library for creating microbatch, where incoming tasks to be executed are grouped into small batches to achieve some of the performance advantage of batch processing, without increasing the latency for each task completion too much.


## Features

* using Functional options to provide meaningful configuration parameters, which can be used for initializing complex values.
* using Dependency Injection for processors, which can have different implementations.


## How to use it 

```golang

        // create a processor which implement the `BatchProcessor` interface
        processor := mockBatchProcessor{}

        batcher := microbatch.NewBatcher(
                processor,
                microbatch.ProcessFrequency(1*time.Second),
                microbatch.BatchSize(2),
        )
        job1 := microbatch.Job{content: "job1"}
        job2 := microbatch.Job{content: "job2"}
        job3 := microbatch.Job{content: "job3"}
        jobResult1, err := batcher.Submit(job1)
        jobResult2, err := batcher.Submit(job2)
        jobResult3, err := batcher.Submit(job3)

        result, err := jobResult1.Get()

        batcher.ShutDown()
```

## How to test it 

```bash

go test ./...

```

## Info

* currently it is using interface{} for the job content and job result, we can use generics to improve it.
