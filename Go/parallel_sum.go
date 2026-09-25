package main

import (
	"flag"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// singleThreadSum обчислює суму елементів масиву в одному потоці
func singleThreadSum(arr []int32) int64 {
	var sum int64
	for _, v := range arr {
		sum += int64(v)
	}
	return sum
}

// goroutineChannelSum обчислює суму за допомогою горутин та каналів
func goroutineChannelSum(arr []int32, numWorkers int) int64 {
	n := len(arr)
	ch := make(chan int64, numWorkers)
	chunkSize := (n + numWorkers - 1) / numWorkers

	// Запускаємо горутини
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		go func(s, e int) {
			var partialSum int64
			for j := s; j < e; j++ {
				partialSum += int64(arr[j])
			}
			// Відправляємо локальну суму в канал
			ch <- partialSum
		}(start, end)
	}

	// Збираємо результати
	var totalSum int64
	for i := 0; i < numWorkers; i++ {
		totalSum += <-ch
	}
	return totalSum
}

// goroutineMutexSum обчислює суму за допомогою горутин та м'ютексу
func goroutineMutexSum(arr []int32, numWorkers int) int64 {
	n := len(arr)
	var totalSum int64
	var mu sync.Mutex
	var wg sync.WaitGroup

	chunkSize := (n + numWorkers - 1) / numWorkers

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			var partialSum int64
			for j := s; j < e; j++ {
				partialSum += int64(arr[j])
			}
			// Блокуємо доступ до спільної змінної лише один раз для кожної горутини
			mu.Lock()
			totalSum += partialSum
			mu.Unlock()
		}(start, end)
	}
	
	// Чекаємо завершення всіх горутин
	wg.Wait()
	return totalSum
}

// goroutineAtomicSum обчислює суму за допомогою горутин та атомарних операцій
func goroutineAtomicSum(arr []int32, numWorkers int) int64 {
	n := len(arr)
	var totalSum int64
	var wg sync.WaitGroup

	chunkSize := (n + numWorkers - 1) / numWorkers

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			var partialSum int64
			for j := s; j < e; j++ {
				partialSum += int64(arr[j])
			}
			// Використовуємо атомарне додавання для оновлення спільної змінної
			atomic.AddInt64(&totalSum, partialSum)
		}(start, end)
	}
	
	// Чекаємо завершення всіх горутин
	wg.Wait()
	return totalSum
}

// benchmark виконує функцію 3 рази, вираховує середній час і виводить результати
func benchmark(name string, f func() int64, expected int64, baseline time.Duration) time.Duration {
	const runs = 3
	var total time.Duration
	var result int64

	for i := 0; i < runs; i++ {
		start := time.Now()
		result = f()
		total += time.Since(start)
	}

	avg := total / time.Duration(runs)
	correct := result == expected
	ms := float64(avg.Nanoseconds()) / 1e6

	if baseline == 0 {
		fmt.Printf("%-35s | %10.2f | %7s | %v\n", name, ms, "-", correct)
	} else {
		speedup := float64(baseline) / float64(avg)
		fmt.Printf("%-35s | %10.2f | %7.2f | %v\n", name, ms, speedup, correct)
	}
	
	return avg
}

func main() {
	// Встановлюємо максимальну кількість потоків операційної системи
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Зчитування аргументів командного рядка
	sizePtr := flag.Int("size", 1000000000, "розмір масиву (кількість елементів)")
	workersPtr := flag.Int("workers", 0, "кількість горутин (якщо 0, будуть використані стандартні значення: 2, 4, 8, NumCPU)")
	flag.Parse()

	size := *sizePtr

	fmt.Printf("Системна інформація: %d CPU\n", runtime.NumCPU())
	fmt.Printf("Ініціалізація масиву з %d елементів...\n", size)
	
	// Ініціалізація масиву
	arr := make([]int32, size)
	for i := 0; i < size; i++ {
		arr[i] = int32(i % 100)
	}
	fmt.Println("Ініціалізація завершена.")
	
	// Отримання правильної суми для перевірки коректності
	expectedSum := singleThreadSum(arr)

	fmt.Println(strings.Repeat("-", 70))
	fmt.Printf("%-35s | %10s | %7s | %s\n", "Метод", "Час (мс)", "Speedup", "Вірно?")
	fmt.Println(strings.Repeat("-", 70))

	// Запуск та бенчмаркінг базового однопотокового методу
	baselineTime := benchmark("singleThreadSum", func() int64 {
		return singleThreadSum(arr)
	}, expectedSum, 0)

	// Визначення кількості горутин для тестування
	workersList := []int{2, 4, 8, runtime.NumCPU()}
	if *workersPtr > 0 {
		workersList = []int{*workersPtr}
	}

	// Бенчмаркінг кожного паралельного методу
	for _, w := range workersList {
		benchmark(fmt.Sprintf("goroutineChannelSum (w=%d)", w), func() int64 {
			return goroutineChannelSum(arr, w)
		}, expectedSum, baselineTime)
		
		benchmark(fmt.Sprintf("goroutineMutexSum (w=%d)", w), func() int64 {
			return goroutineMutexSum(arr, w)
		}, expectedSum, baselineTime)
		
		benchmark(fmt.Sprintf("goroutineAtomicSum (w=%d)", w), func() int64 {
			return goroutineAtomicSum(arr, w)
		}, expectedSum, baselineTime)
	}
	
	fmt.Println(strings.Repeat("-", 70))
}
