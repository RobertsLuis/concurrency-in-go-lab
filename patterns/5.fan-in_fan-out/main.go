package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	workerCount = 4
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go withFanOutFanInPattern(&wg)

	wg.Wait()
}

func withFanOutFanInPattern(wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("Fan-out / Fan-in pattern")

	values := []int{1, 2, 3, 4, 5, 6, 7, 8}

	runSingleWorkerPipeline(values)
	fmt.Println()
	runFanOutFanInPipeline(values)
}

func runSingleWorkerPipeline(values []int) {
	fmt.Println("Single-worker pipeline approach")

	done := make(chan struct{})
	defer close(done)

	startedAt := time.Now()

	intStream := generator(done, values...)

	// Only one expensive stage is consuming from intStream.
	// So values are processed one at a time.
	pipeline := expensiveDouble(done, intStream, "single-worker")

	var result []int
	for value := range pipeline {
		fmt.Printf("single-worker final received: %d\n", value)
		result = append(result, value)
	}

	fmt.Println("Final result:", result)
	fmt.Println("Elapsed time:", time.Since(startedAt))
}

func runFanOutFanInPipeline(values []int) {
	fmt.Println("Fan-out / Fan-in pipeline approach")

	done := make(chan struct{})
	defer close(done)

	startedAt := time.Now()

	intStream := generator(done, values...)

	// FAN-OUT:
	// Start multiple copies of the same expensive stage.
	// All workers read from the same input channel.
	workers := make([]<-chan int, workerCount)
	for i := 0; i < workerCount; i++ {
		workerName := fmt.Sprintf("worker-%d", i+1)
		workers[i] = expensiveDouble(done, intStream, workerName)
	}

	// FAN-IN:
	// Merge all worker output channels into one channel.
	pipeline := fanIn(done, workers...)

	var result []int
	for value := range pipeline {
		fmt.Printf("fan-in final received: %d\n", value)
		result = append(result, value)
	}

	// Important:
	// With fan-out/fan-in, result order is not guaranteed.
	fmt.Println("Final result:", result)
	fmt.Println("Elapsed time:", time.Since(startedAt))
}

func generator(done <-chan struct{}, integers ...int) <-chan int {
	intStream := make(chan int)

	go func() {
		defer close(intStream)

		for _, value := range integers {
			select {
			case <-done:
				return

			case intStream <- value:
				fmt.Printf("generator sent: %d\n", value)
			}
		}
	}()

	return intStream
}

func expensiveDouble(done <-chan struct{}, intStream <-chan int, workerName string) <-chan int {
	doubledStream := make(chan int)

	go func() {
		defer close(doubledStream)

		for {
			select {
			case <-done:
				return

			case value, ok := <-intStream:
				if !ok {
					return
				}

				fmt.Printf("%s started processing %d\n", workerName, value)

				// Simulates an expensive operation.
				time.Sleep(500 * time.Millisecond)

				result := value * 2

				select {
				case <-done:
					return

				case doubledStream <- result:
					fmt.Printf("%s finished: %d * 2 = %d\n", workerName, value, result)
				}
			}
		}
	}()

	return doubledStream
}

func fanIn(done <-chan struct{}, channels ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	multiplexedStream := make(chan int)

	multiplex := func(channel <-chan int) {
		defer wg.Done()

		for value := range channel {
			select {
			case <-done:
				return

			case multiplexedStream <- value:
			}
		}
	}

	wg.Add(len(channels))
	for _, channel := range channels {
		go multiplex(channel)
	}

	// Close the multiplexed channel only after all input channels are drained.
	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}