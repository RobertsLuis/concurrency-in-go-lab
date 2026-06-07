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

	consumer := func(results <-chan int, done <- chan struct{}, consumedItems *[]int) {
		go func() {
			for {
				select {
					case <-done:
						return
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
	close(workerDoneCh)
	close(consumerDoneCh)

	fmt.Printf("We received %d results after working for %v\n", len(consumedItems), workTimeInSeconds)
}
