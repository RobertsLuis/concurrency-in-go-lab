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

	go withOrChannel(&wg)

	wg.Wait()
}

func withOrChannel(wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("=== Or-channel pattern ===")

	// Creates a channel that closes after a duration.
	// This is useful to convert a timeout into a "done-like" channel.
	signalAfter := func(duration time.Duration) <-chan struct{} {
		ch := make(chan struct{})

		go func() {
			defer close(ch)
			time.Sleep(duration)
		}()

		return ch
	}

	// or receives many done-like channels and returns one channel.
	// The returned channel closes when ANY input channel closes.
	var or func(channels ...<-chan struct{}) <-chan struct{}
	or = func(channels ...<-chan struct{}) <-chan struct{} {
		switch len(channels) {
		case 0:
			return nil
		case 1:
			return channels[0]
		}

		orDone := make(chan struct{})

		go func() {
			defer close(orDone)

			switch len(channels) {
			case 2:
				select {
				case <-channels[0]:
				case <-channels[1]:
				}

			default:
				select {
				case <-channels[0]:
				case <-channels[1]:
				case <-channels[2]:

				// Recursively combines the remaining channels.
				// Passing orDone prevents the recursive goroutine tree
				// from leaking when this outer orDone already closed.
				case <-or(append(channels[3:], orDone)...):
				}
			}
		}()

		return orDone
	}

	worker := func(done <-chan struct{}) (<-chan int, <-chan struct{}) {
		results := make(chan int)
		finished := make(chan struct{})

		go func() {
			defer close(finished)
			defer close(results)

			for i := 0; ; i++ {
				select {
				case <-done:
					fmt.Println("Worker stopped.")
					return

				// The send is inside select.
				// If no consumer is ready, this case is not ready.
				// The worker can still stop if done is closed.
				case results <- i:
					fmt.Println("Working...")
					time.Sleep(500 * time.Millisecond)
				}
			}
		}()

		return results, finished
	}

	consumer := func(results <-chan int, finished chan<- struct{}, consumedItems *[]int) {
		const (
			generalTimeoutDuration = 4 * time.Second

			// Worker produces every 500ms.
			// Set this to 400ms to force the per-read timeout.
			// Set this to 600ms to allow normal reads.
			perReadTimeoutDuration = 600 * time.Millisecond
		)

		go func() {
			defer close(finished)

			// This timeout is created once.
			// It limits the total lifetime of the consumer.
			generalTimeout := time.After(generalTimeoutDuration)

			// This timer is reset after each successful read.
			// It means: "if the next read takes more than this, stop."
			perReadTimeout := time.NewTimer(perReadTimeoutDuration)
			defer perReadTimeout.Stop()

			resetPerReadTimeout := func() {
				if !perReadTimeout.Stop() {
					select {
					case <-perReadTimeout.C:
					default:
					}
				}

				perReadTimeout.Reset(perReadTimeoutDuration)
			}

			for {
				select {
				case <-generalTimeout:
					fmt.Println("Consumer general timeout reached.")
					return

				case <-perReadTimeout.C:
					fmt.Println("Consumer per-read timeout reached.")
					return

				case value, ok := <-results:
					if !ok {
						fmt.Println("Results channel closed, stopping consumer.")
						return
					}

					fmt.Printf("Received %d from channel\n", value)
					*consumedItems = append(*consumedItems, value)

					// Since we received a value, restart the per-read timeout
					// for the next value.
					resetPerReadTimeout()
				}
			}
		}()
	}

	// External cancellation channels.
	manualCancelCh := make(chan struct{})
	shutdownCh := make(chan struct{})

	// This is the or-channel use case:
	// the worker should stop if ANY of these things happen:
	// 1. work duration is reached;
	// 2. manual cancel is closed;
	// 3. shutdown is closed.
	workerDoneCh := or(
		signalAfter(workTimeInSeconds),
		manualCancelCh,
		shutdownCh,
	)

	consumerFinishedCh := make(chan struct{})

	workerResults, workerFinishedCh := worker(workerDoneCh)

	var consumedItems []int
	consumer(workerResults, consumerFinishedCh, &consumedItems)

	fmt.Println("Working for", workTimeInSeconds)

	// Just to demonstrate another possible cancellation source.
	// Change this to 2 seconds to stop the worker earlier by manual cancellation.
	// Change this to a value greater than workTimeInSeconds to let workTimeInSeconds win.
	go func() {
		time.Sleep(10 * time.Second)
		close(manualCancelCh)
	}()

	// Wait until the consumer really finishes.
	<-consumerFinishedCh

	// If the consumer stopped first, ask the worker to stop too.
	// This is useful when the consumer stops by timeout before the worker timeout.
	close(shutdownCh)

	// Wait until the worker really finishes.
	// This is different from waiting on workerDoneCh.
	// workerDoneCh means "please stop".
	// workerFinishedCh means "I actually stopped".
	<-workerFinishedCh

	fmt.Printf("We received %d results after working for %v\n", len(consumedItems), workTimeInSeconds)
}
