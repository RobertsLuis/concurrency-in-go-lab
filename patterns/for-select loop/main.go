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
	var wg sync.WaitGroup
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

				// this won't block if there is no consumer ready:
				// it will try to send
				// fail
				// and go back to the for loop.
				// Then, the done chan can cancell the worker!!!

				// the previous way, we would be stuck trying to send to the channel at default statement
				// and never check the done chan again, so we would never be able to stop the worker.
				case ch <- i:
					fmt.Println("Working...")
					time.Sleep(500 * time.Millisecond)
				}
			}
		}()

		return ch
	}

	consumer := func(results <-chan int, finished chan<- struct{}, consumedItems *[]int) {
		go func() {
			defer close(finished)

			timeout := time.After(workTimeInSeconds + 1*time.Second) // consumer timeout

			for {
				select {
					case <-timeout:
						fmt.Println("Consumer timeout reached, stopping consumer.")
						return

					case value, ok := <-results:
						if !ok {
							fmt.Println("Results channel closed, stopping consumer.")
							return
						}
						fmt.Printf("Received %d from channel\n", value)
						*consumedItems = append(*consumedItems, value)
				}
			}
		}()
	}

	workerDoneCh := make(chan struct{})
	consumerFinishedCh := make(chan struct{})

	workerResults := worker(workerDoneCh)

	var consumedItems []int
	consumer(workerResults, consumerFinishedCh, &consumedItems)

	fmt.Println("Working for", workTimeInSeconds)
	time.Sleep(workTimeInSeconds)

	close(workerDoneCh)

	// Wait for the consumer to finish processing all results after the worker is done.
	<-consumerFinishedCh // similar to wg.Wait() but with channels / and similar with for select loop with 1 select statment in done ch

	fmt.Printf("We received %d results after working for %v\n", len(consumedItems), workTimeInSeconds)
}
