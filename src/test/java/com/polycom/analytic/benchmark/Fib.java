package com.polycom.analytic.benchmark;

public class Fib
{

    // private volatile double l;

    public void doTest()
    {
        double l;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 3; i++)
        {
            l = fibImpl(40);
        }
        long now = System.currentTimeMillis();
        System.out.println("Elasped time: " + (now - start));
    }

    private double fibImpl(int n)
    {
        if (n < 0)
            throw new IllegalArgumentException("Must be > 0");
        if (n == 0)
            return 0d;
        if (n == 1)
            return 1d;
        double d = fibImpl(n - 2) + fibImpl(n - 1);
        if (Double.isInfinite(d))
            throw new ArithmeticException("Overflow");
        return d;
    }

    public static void main(String[] args)
    {
        new Fib().doTest();

    }

}
