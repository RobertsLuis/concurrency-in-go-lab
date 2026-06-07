package main

import (
	"fmt"
	"sync"
)

func main() {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go withAdHocConfinement(&wg)
	go withLexicalConfinement(&wg)
	wg.Wait()
}

func withAdHocConfinement(wg *sync.WaitGroup) {
	fmt.Println("=== Ad-hoc confinement ===")
	data := []int{1, 2, 3, 4, 5}
	ch := make(chan int)

	buildData := func() {
		for _, v := range data {
			fmt.Printf("A.H.C -> Sent %d to channel\n", v)
			ch <- v
		}
		close(ch)
	}

	go buildData()

	go func() {
		for v := range ch {
			fmt.Printf("A.H.C -> Received %d from channel\n", v)
		}
		wg.Done()
	}()
}

func withLexicalConfinement(wg *sync.WaitGroup) {
	fmt.Println("=== Lexical confinement ===")

	buildData := func() <-chan int {
		data := []int{1, 2, 3, 4, 5}
		ch := make(chan int)
		go func() {
			for _, v := range data {
				fmt.Printf("L.C -> Sent %d to channel\n", v)
				ch <- v
			}
			close(ch)
		}()
		return ch
	}

	go func() {
		ch := buildData()
		for v := range ch {
			fmt.Printf("L.C -> Received %d from channel\n", v)
		}
		wg.Done()
	}()
}

