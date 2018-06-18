package com.example;

import java.util.*;

public class Main {
    private static int SET_SIZE = 100_000; //размер множества
    private static boolean NEED_PRINT_ARRAYS = false; //отключаем печать
    private static boolean CLEAR_LOG_FOR_TEST = true; //включаем печать

    private static long timeStartInMillis;
    private static long timeFinishInMillis;

    private static long[] generateSet(int size) {
        Random random = new Random();
        Set<Integer> hashSet = new HashSet<>();
        while (hashSet.size() < size)
            hashSet.add(random.nextInt(size*3));
        return hashSet.stream().mapToLong(i -> i).toArray();
    }

    public static void main(String[] args) {
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

        log("Searching intersection...");

        long[] intersect = intersection(setA, setB, setC); //поиск пересечений, возвращает массив пересечений

        log("Searching intersection complete.");

        if (NEED_PRINT_ARRAYS) {
            log("Intersection = " + Arrays.toString(intersect)); //выводит результат
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

    private static long[] intersection(long[] setA, long[] setB, long[] setC) {
        List<Long> intersect = new ArrayList<>(setA.length);

        timeStartInMillis = System.currentTimeMillis();

        for (long i : setA) {
            if (contains(i, setB) && contains(i, setC)) {
                intersect.add(i);
            }
        }

        timeFinishInMillis = System.currentTimeMillis();

        return intersect.stream().mapToLong(i -> i).toArray(); //преобразует list в обычный массив (LONG)
    }

    private static boolean contains(long element, long[] array) { //проверяем содержится ли эл-т в массиве
        for (long i : array) {
            if (i == element)
                return true;
        }
        return false;
    }
}
