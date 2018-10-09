package study;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lijingxiao on 2018/10/8.
 */
public class TreadPoolDemo {
    public static void main(String[] args) {
        //单线程池
//        ExecutorService pool = Executors.newSingleThreadExecutor();

        //固定大小的线程池
//        ExecutorService pool = Executors.newFixedThreadPool(5);

        //可缓冲的线程池
        ExecutorService pool = Executors.newCachedThreadPool();

        for (int i=0; i<10; i++){
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + "is over");
                }
            });
        }

        //这句为主线程执行
        System.out.println("all task is submitted");
//        pool.shutdownNow();
    }
}
