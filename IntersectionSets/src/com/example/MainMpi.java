package com.example;

import mpi.Intracomm;
import mpi.MPI;

import java.util.*;

public class MainMpi {

    private static int SET_SIZE = 100_000;
    private static boolean NEED_PRINT_ARRAYS = false;
    private static boolean CLEAR_LOG_FOR_TEST = true;

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

    public static void main(String[] args) {

        MPI.Init(args);

        final Intracomm comm = MPI.COMM_WORLD;   //перессылка сообщений процессам
        final int processIndex = comm.Rank();
        final int numOfProcesses = comm.Size();

        if (processIndex == 0) {
            log("Selected set size: " + SET_SIZE);
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

            log("Number of processes: " + (numOfProcesses - 1));
            log("Searching intersection...");

            createWorkers(comm, setA.length, numOfProcesses - 1, setA, setB, setC);

            timeStartInMillis = System.currentTimeMillis();

            startWorkers();
            long[] intersect = receiveResults();

            timeFinishInMillis = System.currentTimeMillis();

            log("Searching intersection complete.");

            if (NEED_PRINT_ARRAYS) {
                log("Intersection = " + Arrays.toString(intersect));
            }

            log("Work time: " + (timeFinishInMillis - timeStartInMillis) + " milliseconds");

            if (CLEAR_LOG_FOR_TEST)
                System.out.println(timeFinishInMillis - timeStartInMillis);

        } else {                                    // если это второстипенный процесс

            int[] nums = new int[3];
            comm.Recv(nums, 0, 3, MPI.INT, 0, 41);
            int size = nums[0];
            int indexStart = nums[1];
            int indexFinish = nums[2];

            long[] setA = new long[size];
            comm.Recv(setA, 0, size, MPI.LONG, 0, 42);

            long[] setB = new long[size];
            comm.Recv(setB, 0, size, MPI.LONG, 0, 43);

            long[] setC = new long[size];
            comm.Recv(setC, 0, size, MPI.LONG, 0, 44);

            long[] result = intersection(setA, setB, setC, indexStart, indexFinish);
            comm.Send(new int[] {result.length}, 0, 1, MPI.INT, 0, 45);
            comm.Send(result, 0, result.length, MPI.LONG, 0, 46);
        }

        MPI.Finalize();
    }

    private static void log(String message) {
        if (!CLEAR_LOG_FOR_TEST) {
            System.out.println(message);
        }
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

    private static void createWorkers(Intracomm intracomm, int setLength, int numOfWorkers, long[] setA, long[] setB, long[] setC) {
        workers.clear();
        if (numOfWorkers > setLength) numOfWorkers = setLength;

        int subSetLength = setLength / numOfWorkers;
        int beginIndex = 0;
        int endIndex = Math.min(beginIndex + subSetLength, setLength);

        int destProcIndex = 0;
        while (endIndex <= setLength) {
            destProcIndex++;
            workers.add(new Worker(intracomm, destProcIndex, beginIndex, endIndex, setA, setB, setC));

            beginIndex = endIndex;
            endIndex = beginIndex + subSetLength;
            if (Math.abs(setLength - endIndex) < subSetLength) {
                endIndex = setLength;
            }
        }
    }

    private static void startWorkers() {
        for (Worker worker : workers) {
            worker.send();
        }
    }

    private static long[] receiveResults() {
        List<Long> intersect = new ArrayList<>();
        for (Worker worker : workers) {
            long[] results = worker.receive();
            for (long i : results) {
                intersect.add(i);
            }
        }
        return intersect.stream().mapToLong(i -> i).toArray();
    }

    static class Worker {
        private Intracomm intracomm;
        private int destProcIndex;
        private int indexStart;
        private int indexFinish;
        private long[] setA;
        private long[] setB;
        private long[] setC;

        public Worker(Intracomm intracomm, int destProcIndex, int indexStart, int indexFinish, long[] setA, long[] setB, long[] setC) {
            this.intracomm = intracomm;
            this.destProcIndex = destProcIndex;
            this.indexStart = indexStart;
            this.indexFinish = indexFinish;
            this.setA = setA;
            this.setB = setB;
            this.setC = setC;
        }

        public void send() {
            intracomm.Send(new int[] {setA.length, indexStart, indexFinish}, 0, 3, MPI.INT, destProcIndex, 41);
            intracomm.Send(setA, 0, setA.length, MPI.LONG, destProcIndex, 42);
            intracomm.Send(setB, 0, setB.length, MPI.LONG, destProcIndex, 43);
            intracomm.Send(setC, 0, setC.length, MPI.LONG, destProcIndex, 44);
        }

        public long[] receive() {
            int[] response = new int[1];
            intracomm.Recv(response, 0, 1, MPI.INT, destProcIndex, 45);
            int size = response[0];
            long[] result = new long[size];
            intracomm.Recv(result, 0, size, MPI.LONG, destProcIndex, 46);
            return result;
        }
    }
}
