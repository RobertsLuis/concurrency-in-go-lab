package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	workTimeInSeconds = 3 * time.Second
)

func main() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go withForSelectLoop(&wg)
	wg.Wait()
}

func withForSelectLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("=== For-select loop ===")

	worker := func(done <-chan struct{}) <-chan int {
		ch := make(chan int)

		go func() {
			defer close(ch)

			for i := 0; ; i++ {
				select {
				case <-done:
					return

				default:
					fmt.Println("Working...")
					ch <- i // Possible deadlock if there is no consumer!
					time.Sleep(500 * time.Millisecond)
				}
			}
		}()

		return ch
	}

	consumer := func(results <-chan int, done <- chan struct{}, consumedItems *[]int) {
		go func() {
			for {
				select {
					case <-done:
						return
					// Possible unwanted read because the worker can be closed
					// and we would be reading 0 from the channel (default value for int chan)
					case collectedResult := <-results:
						fmt.Printf("Received %d from channel\n", collectedResult)
						*consumedItems = append(*consumedItems, collectedResult)
					}
			}
		}()
	}

	workerDoneCh := make(chan struct{})
	consumerDoneCh := make(chan struct{})
	workerResults := worker(workerDoneCh)
	var consumedItems []int
	consumer(workerResults, consumerDoneCh, &consumedItems)

	fmt.Println("Working for", workTimeInSeconds)
	time.Sleep(workTimeInSeconds)

	// This flow can cause data race
	// if the consumer is still pushing data to the slice while we are trying to read its length.
	close(workerDoneCh)
	close(consumerDoneCh)

	fmt.Printf("We received %d results after working for %v\n", len(consumedItems), workTimeInSeconds)
}
