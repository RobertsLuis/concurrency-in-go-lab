package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go withTeeChannelPattern(&wg)

	wg.Wait()
}

func withTeeChannelPattern(wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("Tee-channel pattern")

	done := make(chan struct{})
	defer close(done)

	input := generator(done, 1, 2, 3, 4)

	out1, out2 := tee(done, input)

	var consumersWg sync.WaitGroup
	consumersWg.Add(2)

	go func() {
		defer consumersWg.Done()

		for value := range out1 {
			fmt.Printf("first consumer received %d\n", value)
			time.Sleep(1 * time.Second)
		}

		fmt.Println("first consumer finished")
	}()

	go func() {
		defer consumersWg.Done()

		for value := range out2 {
			fmt.Printf("second consumer received %d\n", value)
			time.Sleep(2 * time.Second)
		}

		fmt.Println("second consumer finished")
	}()

	consumersWg.Wait()
	fmt.Println("all consumers finished")
}

func generator(done <-chan struct{}, values ...int) <-chan int {
	stream := make(chan int)

	go func() {
		defer close(stream)

		for _, value := range values {
			select {
			case <-done:
				return

			case stream <- value:
				fmt.Printf("generator sent %d\n", value)
			}
		}
	}()

	return stream
}

func tee(done <-chan struct{}, input <-chan int) (<-chan int, <-chan int) {
	out1 := make(chan int)
	out2 := make(chan int)

	go func() {
		defer close(out1)
		defer close(out2)

		for value := range input {
			// For each value, we need to send it once to out1
			// and once to out2.
			out1Current := out1
			out2Current := out2

			for i := 0; i < 2; i++ {
				select {
				case <-done:
					return

				case out1Current <- value:
					// After out1 receives this value, disable it.
					// A nil channel is never ready in a select.
					out1Current = nil

				case out2Current <- value:
					// After out2 receives this value, disable it.
					// A nil channel is never ready in a select.
					out2Current = nil
				}
			}
		}
	}()

	return out1, out2
}