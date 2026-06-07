package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	workTimeInSeconds = 5 * time.Second
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
		const (
			generalTimeoutDuration = 6 * time.Second // permissive fn - set to 4 to check the general timeout
			perReadTimeoutDuration = 600 * time.Millisecond // permissive fn - set 400 to check the per-read timeout
		)
		go func() {
			defer close(finished)

			generalTimeout := time.After(generalTimeoutDuration) // consumer timeout
			// create a time 'channel' that will send a signal after a certain duration
			// as we can control and reset it manually, we can use it to implement a timeout for each read from the results channel 
			perReadTimeout := time.NewTimer(perReadTimeoutDuration)
			defer perReadTimeout.Stop()
			for {
				select {
					case <-generalTimeout:
						fmt.Println("Consumer timeout reached, stopping consumer.")
						return
					case <-perReadTimeout.C:
						fmt.Println("Per-read timeout reached, no new data received, stopping consumer.")
						return

					case value, ok := <-results:
						if !ok {
							fmt.Println("Results channel closed, stopping consumer.")
							return
						}
						fmt.Printf("Received %d from channel\n", value)
						*consumedItems = append(*consumedItems, value)

						// Reset the per-read timeout after successfully receiving a value
						perReadTimeout.Reset(perReadTimeoutDuration)
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
