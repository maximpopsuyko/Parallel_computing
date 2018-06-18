package com.example;

import java.util.*;

public class MainJavaThreads {
    private static int SET_SIZE = 100_000;
    private static boolean NEED_PRINT_ARRAYS = false;
    private static boolean CLEAR_LOG_FOR_TEST = true;
    private static int NUM_OF_THREADS = 8;

    private static long timeStartInMillis;
    private static long timeFinishInMillis;

    private static List<Worker> workers = new ArrayList<>();

    private static long[] generateSet(int size) {
        Random random = new Random();
        Set<Integer> hashSet = new HashSet<>();
        while (hashSet.size() < size)
            hashSet.add(random.nextInt(size*3));
        return hashSet.stream().mapToLong(i -> i).toArray();
    }

    public static void main(String[] args) throws InterruptedException {
        log("Selected set size: " + SET_SIZE);
        log("Number of threads: " + NUM_OF_THREADS);
        log("Generating sets...");

        long[] setA = generateSet(SET_SIZE);
        long[] setB = generateSet(SET_SIZE);
        long[] setC = generateSet(SET_SIZE);

        log("Generating sets complete.");

        if (NEED_PRINT_ARRAYS) {
            log("A = " + Arrays.toString(setA));
            log("B = " + Arrays.toString(setB));
            log("C = " + Arrays.toString(setC));
        }

        log("Searching intersection...");

        createWorkers(setA, setB, setC, NUM_OF_THREADS);

        timeStartInMillis = System.currentTimeMillis();

        startWorkers();
        joinWorkers();
        long[] intersect = mergeResults();

        timeFinishInMillis = System.currentTimeMillis();

        log("Searching intersection complete.");

        if (NEED_PRINT_ARRAYS) {
            log("Intersection = " + Arrays.toString(intersect));
        }

        log("Work time: " + (timeFinishInMillis - timeStartInMillis) + " milliseconds");

        if (CLEAR_LOG_FOR_TEST)
            System.out.println(timeFinishInMillis - timeStartInMillis);
    }

    private static void log(String message) {
        if (!CLEAR_LOG_FOR_TEST) {
            System.out.println(message);
        }
    }

    private static void createWorkers(long[] setA, long[] setB, long[] setC, int numOfWorkers) {
        workers.clear();
        if (numOfWorkers > setA.length) numOfWorkers = setA.length;

        int subSetLength = setA.length / numOfWorkers;
        int beginIndex = 0;
        int endIndex = Math.min(beginIndex + subSetLength, setA.length);

        while (endIndex <= setA.length) {
            workers.add(new Worker(setA, setB, setC, beginIndex, endIndex));

            beginIndex = endIndex;
            endIndex = beginIndex + subSetLength;
            if (Math.abs(setA.length - endIndex) < subSetLength) {
                endIndex = setA.length;
            }
        }
    }

    private static void startWorkers() {
        for (Worker worker : workers) {
            worker.start();
        }
    }

    private static void joinWorkers() throws InterruptedException {
        for (Worker worker : workers) {
            worker.join();
        }
    }

    private static long[] mergeResults() {
        List<Long> intersect = new ArrayList<>();
        for (Worker worker : workers) {
            long[] results = worker.getResult();
            for (long i : results) {
                intersect.add(i);
            }
        }
        return intersect.stream().mapToLong(i -> i).toArray();
    }

    private static long[] intersection(long[] setA, long[] setB, long[] setC, int indexStart, int indexFinish) {
        List<Long> intersect = new ArrayList<>(setA.length);

        for (int i = indexStart; i < indexFinish; i++) {
            if (contains(setA[i], setB) && contains(setA[i], setC)) {
                intersect.add(setA[i]);
            }
        }

        return intersect.stream().mapToLong(i -> i).toArray();
    }

    private static boolean contains(long element, long[] array) {
        for (long i : array) {
            if (i == element)
                return true;
        }
        return false;
    }

    static class Worker extends Thread {
        private long[] setA;
        private long[] setB;
        private long[] setC;
        private int indexStart;
        private int indexFinish;
        private long[] intersect;

        public Worker(long[] setA, long[] setB, long[] setC, int indexStart, int indexFinish) {
            super();
            this.setA = setA;
            this.setB = setB;
            this.setC = setC;
            this.indexStart = indexStart;
            this.indexFinish = indexFinish;
        }

        @Override
        public void run() {
            intersect = intersection(setA, setB, setC, indexStart, indexFinish);
        }

        public long[] getResult() {
            return intersect;
        }
    }
}
