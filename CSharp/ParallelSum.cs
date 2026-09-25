using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CSharpParallelSum
{
    class Program
    {
        static void Main(string[] args)
        {
            int arraySize = 1_000_000_000;
            int[] threadCounts = { 2, 4, 8, Environment.ProcessorCount };

            // Опціональні параметри командного рядка: розмір масиву та кількість потоків
            if (args.Length > 0 && int.TryParse(args[0], out int size))
            {
                arraySize = size;
            }
            if (args.Length > 1 && int.TryParse(args[1], out int threads))
            {
                threadCounts = new[] { threads };
            }

            // 1. Виведення інформації про систему
            Console.WriteLine($"Кількість логічних процесорів: {Environment.ProcessorCount}");
            Console.WriteLine($"Розмір масиву: {arraySize:N0} елементів");
            
            // 2. Ініціалізація масиву
            Console.WriteLine("Ініціалізація масиву...");
            int[] array = new int[arraySize];
            for (int i = 0; i < arraySize; i++)
            {
                array[i] = i % 100;
            }

            Console.WriteLine("Масив ініціалізовано. Початок тестування...\n");
            
            int numRuns = 3;

            Console.WriteLine(new string('-', 95));
            Console.WriteLine($"{"Метод",-30} | {"Час (мс)",-10} | {"Прискорення",-15} | {"Коректність"}");
            Console.WriteLine(new string('-', 95));

            long baselineSum = 0;
            double baselineTime = 0;

            // 3. Запуск однопотокового базового методу
            for (int i = 0; i < numRuns; i++)
            {
                var sw = Stopwatch.StartNew();
                baselineSum = SingleThreadSum(array);
                sw.Stop();
                baselineTime += sw.Elapsed.TotalMilliseconds;
            }
            baselineTime /= numRuns;

            PrintResult("SingleThreadSum", baselineTime, 1.0, true);

            // 4. Запуск паралельних методів з різною кількістю потоків
            int[] distinctThreadCounts = threadCounts.Distinct().OrderBy(t => t).ToArray();
            foreach (int tc in distinctThreadCounts)
            {
                Console.WriteLine(new string('-', 95));
                Console.WriteLine($"--- Тестування з {tc} потоками ---");
                
                RunBenchmark(() => ThreadSum(array, tc), $"ThreadSum ({tc})", numRuns, baselineTime, baselineSum);
                RunBenchmark(() => TaskSum(array, tc), $"TaskSum ({tc})", numRuns, baselineTime, baselineSum);
                RunBenchmark(() => ParallelForSum(array, tc), $"ParallelForSum ({tc})", numRuns, baselineTime, baselineSum);
                RunBenchmark(() => PlinqSum(array, tc), $"PlinqSum ({tc})", numRuns, baselineTime, baselineSum);
            }
            Console.WriteLine(new string('-', 95));
        }

        static void PrintResult(string methodName, double time, double speedup, bool isCorrect)
        {
            Console.WriteLine($"{methodName,-30} | {time,10:F2} | {speedup,15:F2} | {(isCorrect ? "Так" : "Ні")}");
        }

        static void RunBenchmark(Func<long> sumMethod, string methodName, int runs, double baselineTime, long expectedSum)
        {
            long sum = 0;
            double totalTime = 0;

            // Тестуємо 3 рази
            for (int i = 0; i < runs; i++)
            {
                var sw = Stopwatch.StartNew();
                sum = sumMethod();
                sw.Stop();
                totalTime += sw.Elapsed.TotalMilliseconds;
            }

            // Розрахунок середніх значень
            double avgTime = totalTime / runs;
            double speedup = baselineTime / avgTime;
            bool isCorrect = sum == expectedSum;
            
            // Виведення результату
            PrintResult(methodName, avgTime, speedup, isCorrect);
        }

        // 1. SingleThreadSum — базовий цикл for
        static long SingleThreadSum(int[] array)
        {
            long sum = 0;
            for (int i = 0; i < array.Length; i++)
            {
                sum += array[i];
            }
            return sum;
        }

        // 2. ThreadSum — Створення потоків вручну
        static long ThreadSum(int[] array, int numThreads)
        {
            long[] partialSums = new long[numThreads];
            Thread[] threads = new Thread[numThreads];
            
            int sliceSize = array.Length / numThreads;

            for (int i = 0; i < numThreads; i++)
            {
                int threadIndex = i; // локальна копія для лямбда-виразу
                threads[i] = new Thread(() => 
                {
                    int start = threadIndex * sliceSize;
                    // Останній потік обробляє залишок
                    int end = (threadIndex == numThreads - 1) ? array.Length : start + sliceSize;
                    long sum = 0;
                    for (int j = start; j < end; j++)
                    {
                        sum += array[j];
                    }
                    partialSums[threadIndex] = sum; // Кожен потік пише у свій індекс без блокування
                });
                threads[i].Start();
            }

            // Очікування завершення
            foreach (var t in threads)
            {
                t.Join();
            }

            // Підсумовування результатів
            long totalSum = 0;
            for (int i = 0; i < numThreads; i++)
            {
                totalSum += partialSums[i];
            }
            return totalSum;
        }

        // 3. TaskSum — Використання Task<long> + Task.WhenAll
        static long TaskSum(int[] array, int numThreads)
        {
            Task<long>[] tasks = new Task<long>[numThreads];
            int sliceSize = array.Length / numThreads;

            for (int i = 0; i < numThreads; i++)
            {
                int taskIndex = i;
                tasks[i] = Task.Run(() =>
                {
                    int start = taskIndex * sliceSize;
                    int end = (taskIndex == numThreads - 1) ? array.Length : start + sliceSize;
                    long sum = 0;
                    for (int j = start; j < end; j++)
                    {
                        sum += array[j];
                    }
                    return sum;
                });
            }

            // Очікування завершення
            Task.WaitAll(tasks);

            // Підсумовування результатів
            long totalSum = 0;
            for (int i = 0; i < numThreads; i++)
            {
                totalSum += tasks[i].Result;
            }
            return totalSum;
        }

        // 4. ParallelForSum — Використання Parallel.For з локальним станом
        static long ParallelForSum(int[] array, int maxDegree)
        {
            long totalSum = 0;

            Parallel.For(0, array.Length, new ParallelOptions { MaxDegreeOfParallelism = maxDegree }, 
                () => 0L, // localInit
                (i, loopState, localSum) => // body
                {
                    localSum += array[i];
                    return localSum;
                },
                localSum => // localFinally
                {
                    Interlocked.Add(ref totalSum, localSum); // Накопичення без явних замків (lock)
                });

            return totalSum;
        }

        // 5. PlinqSum — Використання PLINQ
        static long PlinqSum(int[] array, int maxDegree)
        {
            return array.AsParallel()
                        .WithDegreeOfParallelism(maxDegree)
                        .Select(x => (long)x)
                        .Sum();
        }
    }
}
