package microbatch

import (
	"errors"
	"testing"
	"time"
)

type mockBatchProcessor struct{}

func (mbp mockBatchProcessor) Process(j Job) (interface{}, error) {
	return j.content, nil
}

type mockErrorBatchProcessor struct{}

func (mbp mockErrorBatchProcessor) Process(j Job) (interface{}, error) {
	return nil, errors.New("error")
}

func Test_Submit_All_Jobs_Processed(t *testing.T) {
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

	time.Sleep(1 * time.Second)

        if r1, _ := result1.Get(); r1 != "job1" {
		t.Errorf("Expected result1 to be 'job1', got %v", r1)
	}
        if r2, _ := result2.Get(); r2 != "job2" {
		t.Errorf("Expected result2 to be 'job2', got %v", r2)
	}

        if r3, _ := result3.Get(); r3 != "job3" {
		t.Errorf("Expected result3 to be 'job3', got %v", r3)
	}
}

func Test_Submit_Batch_Full(t *testing.T) {
	processor := mockBatchProcessor{}

	batcher := NewBatcher(
		processor,
		ProcessFrequency(3*time.Second),
		BatchSize(2),
	)
	job1 := Job{content: "job1"}
	job2 := Job{content: "job2"}
	job3 := Job{content: "job3"}

	result1, _ := batcher.Submit(job1)
	result2, _ := batcher.Submit(job2)

        tm := time.Now()
	result3, _ := batcher.Submit(job3)

        if r1, _ := result1.Get(); r1 != "job1" {
		t.Errorf("Expected result1 to be 'job1', got %v", r1)
	}
        if r2, _ := result2.Get(); r2 != "job2" {
		t.Errorf("Expected result2 to be 'job2', got %v", r2)
	}

        if r3, _ := result3.Get(); r3 != "job3" {
		t.Errorf("Expected result3 to be 'job3', got %v", r3)
	}

        if time.Now().Sub(tm) < 3*time.Second {
		t.Errorf("Expected result3 to be executed by the frenquency is on")
        }
}

func Test_Submit_Batcher_Is_Shutdown(t *testing.T) {
	processor := mockBatchProcessor{}

	batcher := NewBatcher(
		processor,
		ProcessFrequency(2*time.Second),
		BatchSize(2),
	)
	job1 := Job{content: "job1"}
	job2 := Job{content: "job2"}
	job3 := Job{content: "job3"}

	result1, _ := batcher.Submit(job1)
	result2, _ := batcher.Submit(job2)

        batcher.Shutdown()

	_, err := batcher.Submit(job3)

        if r1, _ := result1.Get(); r1 != "job1" {
		t.Errorf("Expected result1 to be 'job1', got %v", r1)
	}
        if r2, _ := result2.Get(); r2 != "job2" {
		t.Errorf("Expected result2 to be 'job2', got %v", r2)
	}

	if err == nil {
		t.Errorf("Expected jobs was not submitted")
	}
}

func Test_Submit_Job_Processed_With_Error(t *testing.T) {

	processor := mockErrorBatchProcessor{}

	batcher := NewBatcher(
		processor,
		ProcessFrequency(1*time.Second),
		BatchSize(2),
	)
	job1 := Job{content: "job1"}

	result1, _ := batcher.Submit(job1)
	time.Sleep(1 * time.Second)

        if _, err := result1.Get(); err == nil {
		t.Errorf("Expected to get err.")
	}
}
