package microbatch

import (
	"testing"
	"time"
)

type mockBatchProcessor struct {}


func (mbp mockBatchProcessor) Process(j Job) interface{} {
        return j.content
}

func Test_Submit(t *testing.T) {
        processor := mockBatchProcessor{}

        batcher := NewBatcher(
                processor,
                ProcessFrequency(1*time.Second),
                BatchSize(2),
        )
        job1 := Job{content: "job1"}
        job2 := Job{content: "job2"}
        job3 := Job{content: "job3"}
        result1, _ := batcher.Submit(job1)
        result2, _ := batcher.Submit(job2)
        result3, _ := batcher.Submit(job3)

        time.Sleep(1*time.Second)
        if result1.Get() != "job1" {
                t.Errorf("Expected result1 to be 'job1', got %v", result1.Get())
        }
        if result2.Get() != "job2" {
                t.Errorf("Expected result2 to be 'job2', got %v", result2.Get())
        }
        if result3.Get() != "job3" {
                t.Errorf("Expected result3 to be 'job3', got %v", result3.Get())
        }
}
