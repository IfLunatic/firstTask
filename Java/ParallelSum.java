import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class ParallelSum {

    // Кількість запусків для усереднення часу
    private static final int RUNS = 3;

    public static void main(String[] args) {
        int defaultSize = 1_000_000_000;
        int processors = Runtime.getRuntime().availableProcessors();
        int maxThreads = processors;

        if (args.length > 0) {
            try {
                defaultSize = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("Невірний формат розміру масиву. Використовується розмір за замовчуванням.");
            }
        }
        if (args.length > 1) {
            try {
                maxThreads = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Невірний формат кількості потоків. Використовується кількість за замовчуванням.");
            }
        }

        System.out.println("=== Інформація про систему ===");
        System.out.println("Доступно процесорів: " + processors);
        System.out.println("Розмір масиву: " + defaultSize);
        System.out.println("Максимальна пам'ять JVM: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)) + " MB");
        System.out.println("==============================\n");

        System.out.println("Заповнення масиву...");
        int[] array = new int[defaultSize];
        // Заповнення масиву значеннями i % 100
        for (int i = 0; i < defaultSize; i++) {
            array[i] = i % 100;
        }
        System.out.println("Масив заповнено.\n");

        System.out.println(String.format("%-45s | %-12s | %-10s | %-10s", "Метод", "Час (мс)", "Прискорення", "Коректність"));
        System.out.println("-------------------------------------------------------------------------------------------------");

        // Базовий метод (один потік)
        long expectedSum = runBenchmark("Однопотоковий (Baseline)", array, () -> singleThreadSum(array), -1, -1);
        double baselineTime = lastTimeMs;

        // Тестування з різною кількістю потоків
        // Створюємо набір унікальних значень кількості потоків
        java.util.Set<Integer> threadSet = new java.util.LinkedHashSet<>();
        for (int t : new int[]{2, 4, 8, maxThreads}) {
            threadSet.add(t);
        }
        final int[] uniqueThreadCounts = threadSet.stream().mapToInt(Integer::intValue).sorted().toArray();

        for (int threads : uniqueThreadCounts) {
            runBenchmark("Manual Threads (" + threads + " потоків)", array, () -> threadSum(array, threads), baselineTime, expectedSum);
            runBenchmark("ExecutorService (" + threads + " потоків)", array, () -> executorServiceSum(array, threads), baselineTime, expectedSum);
        }

        // ForkJoin тестується з пулом за замовчуванням (або можна створити власний, але зазвичай використовують спільний або ForkJoinPool(threads))
        // Поріг для ForkJoin: розмір / (доступні процесори * 4) як евристика
        long threshold = array.length / (processors * 4L);
        if (threshold < 1000) threshold = 1000;
        final long finalThreshold = threshold;
        runBenchmark("ForkJoinPool (поріг " + threshold + ")", array, () -> forkJoinSum(array, finalThreshold), baselineTime, expectedSum);

        // Parallel Stream
        runBenchmark("Parallel Stream", array, () -> parallelStreamSum(array), baselineTime, expectedSum);
    }

    private static double lastTimeMs = 0;

    // Інтерфейс для передачі функцій сумування
    @FunctionalInterface
    interface SumFunction {
        long sum();
    }

    // Метод для запуску бенчмарку, вимірювання часу і виведення результатів
    private static long runBenchmark(String name, int[] array, SumFunction func, double baselineTime, long expectedSum) {
        long totalTime = 0;
        long result = 0;

        for (int i = 0; i < RUNS; i++) {
            long startTime = System.nanoTime();
            result = func.sum();
            long endTime = System.nanoTime();
            totalTime += (endTime - startTime);
        }

        double avgTimeMs = (totalTime / (double) RUNS) / 1_000_000.0;
        lastTimeMs = avgTimeMs;

        String speedup = baselineTime < 0 ? "-" : String.format("%.2fx", baselineTime / avgTimeMs);
        String correctness = (expectedSum == -1 || result == expectedSum) ? "Так" : "НІ (" + result + ")";

        System.out.println(String.format("%-45s | %-12.2f | %-10s | %-10s", name, avgTimeMs, speedup, correctness));
        return result;
    }

    // 1. Однопотокове сумування - базовий метод
    private static long singleThreadSum(int[] array) {
        long sum = 0;
        for (int i = 0; i < array.length; i++) {
            sum += array[i];
        }
        return sum;
    }

    // 2. Сумування з використанням Thread вручну
    private static long threadSum(int[] array, int numThreads) {
        Thread[] threads = new Thread[numThreads];
        long[] partialSums = new long[numThreads];
        
        int chunkSize = array.length / numThreads;

        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            final int start = i * chunkSize;
            // Останній потік бере залишок масиву
            final int end = (i == numThreads - 1) ? array.length : start + chunkSize;

            threads[i] = new Thread(() -> {
                long sum = 0;
                for (int j = start; j < end; j++) {
                    sum += array[j];
                }
                // Записуємо результат в масив, синхронізація не потрібна
                partialSums[threadIndex] = sum;
            });
            threads[i].start();
        }

        // Чекаємо завершення всіх потоків
        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Підсумовуємо часткові суми
        long totalSum = 0;
        for (long partialSum : partialSums) {
            totalSum += partialSum;
        }
        return totalSum;
    }

    // 3. Сумування через ExecutorService та Callable/Future
    private static long executorServiceSum(int[] array, int numThreads) {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Callable<Long>> tasks = new ArrayList<>();

        int chunkSize = array.length / numThreads;

        for (int i = 0; i < numThreads; i++) {
            final int start = i * chunkSize;
            final int end = (i == numThreads - 1) ? array.length : start + chunkSize;

            tasks.add(() -> {
                long sum = 0;
                for (int j = start; j < end; j++) {
                    sum += array[j];
                }
                return sum;
            });
        }

        long totalSum = 0;
        try {
            // Виконуємо всі завдання і отримуємо список Future
            List<Future<Long>> results = executor.invokeAll(tasks);
            for (Future<Long> result : results) {
                totalSum += result.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            // Обов'язково закриваємо пул потоків
            executor.shutdown();
        }

        return totalSum;
    }

    // 4. Сумування через ForkJoinPool та RecursiveTask
    private static long forkJoinSum(int[] array, long threshold) {
        ForkJoinPool pool = ForkJoinPool.commonPool();
        return pool.invoke(new SumTask(array, 0, array.length, threshold));
    }

    // Завдання для ForkJoinPool
    static class SumTask extends RecursiveTask<Long> {
        private final int[] array;
        private final int start;
        private final int end;
        private final long threshold;

        SumTask(int[] array, int start, int end, long threshold) {
            this.array = array;
            this.start = start;
            this.end = end;
            this.threshold = threshold;
        }

        @Override
        protected Long compute() {
            int length = end - start;
            
            // Якщо шматок менше порогу, рахуємо напряму
            if (length <= threshold) {
                long sum = 0;
                for (int i = start; i < end; i++) {
                    sum += array[i];
                }
                return sum;
            } else {
                // Інакше розбиваємо на два підзавдання
                int mid = start + length / 2;
                SumTask leftTask = new SumTask(array, start, mid, threshold);
                SumTask rightTask = new SumTask(array, mid, end, threshold);
                
                // Запускаємо ліве завдання асинхронно
                leftTask.fork();
                
                // Рахуємо праве завдання в поточному потоці
                long rightResult = rightTask.compute();
                
                // Чекаємо завершення лівого завдання
                long leftResult = leftTask.join();
                
                return leftResult + rightResult;
            }
        }
    }

    // 5. Сумування через Parallel Stream
    private static long parallelStreamSum(int[] array) {
        // Перетворюємо масив на стрім, робимо його паралельним і сумуємо як long
        return Arrays.stream(array).parallel().mapToLong(i -> i).sum();
    }
}
