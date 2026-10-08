import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Pool {

    static final int REQUESTS = 32;

    static final AtomicInteger inService = new AtomicInteger();
    static final AtomicInteger peak = new AtomicInteger();

    /** Підроблений сервіс: відповідає приблизно за 120 мс. Готово. */
    static String fetch(String path) {
        peak.accumulateAndGet(inService.incrementAndGet(), Math::max);

        try {
            Thread.sleep(120);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        inService.decrementAndGet();

        return path + " готово";
    }

    /** Фабрика потоків із лічильником.*/
    static final class CountingFactory implements ThreadFactory {

        private final AtomicInteger created = new AtomicInteger();

        @Override
        public Thread newThread(Runnable body) {
            return new Thread(body, "пул-" + created.incrementAndGet());
        }

        int created() {
            return created.get();
        }
    }

    /**Виконати всі REQUESTS запитів на пулі розміром size.Повертає витрачений час у мс.*/
    static long runOnPool(int size, CountingFactory factory) throws Exception {

        ExecutorService pool = Executors.newFixedThreadPool(size, factory);

        long t0 = System.currentTimeMillis();

        List<Future<String>> futures = new ArrayList<>();

        // Спочатку відправляємо всі задачі в пул
        for (int i = 0; i < REQUESTS; i++) {
            final String path = "/сторінка/" + i;

            Future<String> future =
                    pool.submit(() -> fetch(path));

            futures.add(future);
        }

        // Потім окремо отримуємо всі результати
        for (Future<String> future : futures) {
            future.get();
        }

        // Коректно завершуємо роботу пула
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        return System.currentTimeMillis() - t0;
    }

    /**«Потік на запит»: 32 потоки на 32 запити.*/
    static long perThread() throws Exception {

        long t0 = System.currentTimeMillis();

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < REQUESTS; i++) {

            final String path = "/сторінка/" + i;

            Thread t =
                    new Thread(() -> fetch(path));

            threads.add(t);

            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        return System.currentTimeMillis() - t0;
    }

    /**Готова CPU-крива.*/
    static final String CPU_CURVE = """
            CPU-задача без звернень у пам'ять: 256 порцій по 3 млн обчислень sqrt на пулі.
            Zulu 17.0.17, macOS, 10 ядер (4 швидких і 6 економних):
               1 потік  -> 776 мс     2 ->  277 мс     4 -> 129 мс     8 -> 72 мс
              10 потоків ->  64 мс    16 ->   64 мс   64 ->  69 мс
            Час падає до числа ядер і там зупиняється: 10 і 16 потоків дають ті самі
            64 мс, на 64 потоках уже 69. Наша задача не така: вона не рахує, а чекає,
            і тому виграє від потоків, яких більше за ядра.""";

    public static void main(String[] args) throws Exception {

        System.out.println(
                "ядер у машині: "
                        + Runtime.getRuntime().availableProcessors()
        );

        System.out.println(
                REQUESTS
                        + " запитів по ≈120 мс кожен.\n"
        );

        System.out.printf(
                "%16s | %8s | %10s | %s%n",
                "варіант",
                "час, мс",
                "створено",
                "пік у сервісі"
        );

        // Потік на кожен запит
        peak.set(0);

        System.out.printf(
                "%16s | %8d | %10d | %d%n",
                "потік на запит",
                perThread(),
                REQUESTS,
                peak.get()
        );

        // Пули 1, 4 та 32
        for (int size : new int[]{1, 4, 32}) {

            peak.set(0);

            CountingFactory factory =
                    new CountingFactory();

            long ms =
                    runOnPool(size, factory);

            System.out.printf(
                    "%16s | %8d | %10d | %d%n",
                    "пул " + size,
                    ms,
                    factory.created(),
                    peak.get()
            );
        }

        System.out.println(
                "\nЯкщо програма не завершилася сама, "
                        + "у пулі лишилися живі потоки:"
        );

        System.out.println(
                "це означає, що десь немає shutdown().\n"
        );

        System.out.println(CPU_CURVE);
    }
}