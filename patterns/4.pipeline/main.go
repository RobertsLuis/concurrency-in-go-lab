package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go withPipelinePattern(&wg)

	wg.Wait()
}

func withPipelinePattern(wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("Pipeline pattern")

	values := []int{1, 2, 3, 4}

	runSequentialApproach(values)
	fmt.Println()
	runPipelineApproach(values)
}

func runSequentialApproach(values []int) {
	fmt.Println("Sequential approach -------------")

	// In the sequential approach, each function receives the whole slice,
	// processes everything, and returns another whole slice.
	result := multiplySlice(values, 2)
	result = addSlice(result, 1)
	result = multiplySlice(result, 2)

	fmt.Println("Final result:", result)
}

func multiplySlice(values []int, multiplier int) []int {
	fmt.Printf("multiplySlice by %d started\n", multiplier)

	result := make([]int, len(values))

	for i, value := range values {
		time.Sleep(300 * time.Millisecond) // simulate expensive work

		result[i] = value * multiplier
		fmt.Printf("multiplySlice: %d * %d = %d\n", value, multiplier, result[i])
	}

	fmt.Printf("multiplySlice by %d finished\n", multiplier)
	return result
}

func addSlice(values []int, additive int) []int {
	fmt.Printf("addSlice by %d started\n", additive)

	result := make([]int, len(values))

	for i, value := range values {
		time.Sleep(300 * time.Millisecond) // simulate expensive work

		result[i] = value + additive
		fmt.Printf("addSlice: %d + %d = %d\n", value, additive, result[i])
	}

	fmt.Printf("addSlice by %d finished\n", additive)
	return result
}

func runPipelineApproach(values []int) {
	fmt.Println("Pipeline approach ----------")

	done := make(chan struct{})
	defer close(done)

	// In the pipeline approach, each stage receives a channel
	// and returns another channel.

	// Values flow item by item:
	// generator -> multiply -> add -> multiply -> final range
	intStream := generator(done, values...)
	pipeline := multiply(done, add(done, multiply(done, intStream, 2), 1), 2)

	var result []int

	for value := range pipeline {
		fmt.Printf("pipeline final received: %d\n", value)
		result = append(result, value)
	}

	fmt.Println("Final result:", result)
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

func multiply(done <-chan struct{}, intStream <-chan int, multiplier int) <-chan int {
	multipliedStream := make(chan int)

	go func() {
		defer close(multipliedStream)

		for value := range intStream {
			time.Sleep(300 * time.Millisecond) // simulate expensive work

			result := value * multiplier

			select {
			case <-done:
				return

			case multipliedStream <- result:
				fmt.Printf("multiply stage: %d * %d = %d\n", value, multiplier, result)
			}
		}
	}()

	return multipliedStream
}

func add(done <-chan struct{}, intStream <-chan int, additive int) <-chan int {
	addedStream := make(chan int)

	go func() {
		defer close(addedStream)

		for value := range intStream {
			time.Sleep(300 * time.Millisecond) // simulate expensive work

			result := value + additive

			select {
			case <-done:
				return

			case addedStream <- result:
				fmt.Printf("add stage: %d + %d = %d\n", value, additive, result)
			}
		}
	}()

	return addedStream
}