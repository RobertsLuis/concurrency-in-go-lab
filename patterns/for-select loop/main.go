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
					ch <- i
					time.Sleep(500 * time.Millisecond)
				}
			}
		}()

		return ch
	}

	consumer := func(results <-chan int, done chan<- struct{}, resultList *[]int) {
		defer close(done)

		for collectedResult := range results {
			fmt.Printf("Received %d from channel\n", collectedResult)
			*resultList = append(*resultList, collectedResult)
		}
	}

	workerDoneCh := make(chan struct{})
	consumerDoneCh := make(chan struct{})
	workerResults := worker(workerDoneCh)

	var consumerResults []int
	go consumer(workerResults, consumerDoneCh, &consumerResults)

	fmt.Println("Working for", workTimeInSeconds)
	time.Sleep(workTimeInSeconds)
	close(workerDoneCh)

	// Another way to wait to some goroutine to finish, instead of using WaitGroup
	// similar to for-select loop with 1 select case in consumerDoneCh and no default
	<-consumerDoneCh 

	fmt.Printf("We received %d results after working for %v\n", len(consumerResults), workTimeInSeconds)
}
