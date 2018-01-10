
/*
 * Copyright Notice:
 *      Copyright  1998-2009, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */
package com.omm.fms.service.manager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.wcc.framework.AppProperties;
import org.wcc.framework.log.AppLogger;

import com.omm.fms.service.alarm.util.AlarmConstant;


public final class ThreadPoolManager
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(ThreadPoolManager.class);
    
    /**
     * 线程池单例
     */
    private static ThreadPoolManager instance = new ThreadPoolManager();
    
    private ScheduledThreadPoolExecutor scheduledPool;
    
    private ThreadPoolExecutor threadPool;
    
    private List<Runnable> threadPoolTasks;
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    private ThreadPoolManager()
    {
        LOGGER.info("[fms] ThreadPoolManager initiate");
        int minThreads = AlarmConstant.DEFAUST_MINTHREADS;
        int maxThreads = AlarmConstant.DEFAULT_MAXTHREADS;
        int scheduledThreads = AlarmConstant.DEFAULT_SCHEDULEDTHREADS;
        int keepAliveTime = AlarmConstant.DEFAULT_KEEPALIVETIME;
        
        minThreads = AppProperties.getAsInt("minThreads", minThreads);
        maxThreads = AppProperties.getAsInt("maxThreads", maxThreads);
        scheduledThreads = AppProperties.getAsInt("scheduledThreads", scheduledThreads);
        keepAliveTime = AppProperties.getAsInt("keepAliveTime", keepAliveTime);
        
        this.threadPoolTasks = new CopyOnWriteArrayList();
        this.scheduledPool = new ScheduledThreadPoolExecutor(scheduledThreads);
        
        this.threadPool =
            new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue())
            {
                protected void beforeExecute(Thread t, Runnable r)
                {
                    ThreadPoolManager.this.threadPoolTasks.add(r);
                    super.beforeExecute(t, r);
                }
                
                protected void afterExecute(Runnable r, Throwable t)
                {
                    ThreadPoolManager.this.threadPoolTasks.remove(r);
                    super.afterExecute(r, t);
                }
                
                protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value)
                {
                    return new CustomizedFutureTask(runnable, value);
                }
            };
        LOGGER.info("[fms] ThreadPoolManager is ready");
    }
    
    public int getThreadPoolActiveTaskCount()
    {
        return this.threadPoolTasks.size();
    }
    
    /**
     * 
     * 活动线程总数取得
    
     * 活动线程总数取得
     * @return  int 总活动线程数
     */
    public int getScheduledActiveTaskCount()
    {
        this.scheduledPool.purge();
        return this.scheduledPool.getQueue().size();
    }
    
    public static ThreadPoolManager getInstance()
    {
        return instance;
    }
    
    public ScheduledExecutorService getScheduledThreadPool()
    {
        return this.scheduledPool;
    }
    
    public ThreadPoolExecutor getThreadPool()
    {
        return this.threadPool;
    }
    
    /**
     * 
     * 线程池关闭 
     * 〈功能详细描述〉
     */
    public void shutdown()
    {
        LOGGER.info("ThreadPoolManager is shutdowning");
        this.threadPoolTasks.clear();
        this.threadPool.shutdown();
        try
        {
            if (!(this.threadPool.awaitTermination(AlarmConstant.THREAD_POOL_TIMEOUT, TimeUnit.SECONDS)))
            {
                this.threadPool.shutdownNow();
            }
        }
        catch (InterruptedException ie)
        {
            this.threadPool.shutdownNow();
        }
        this.scheduledPool.shutdown();
        try
        {
            if (!(this.scheduledPool.awaitTermination(AlarmConstant.THREAD_POOL_TIMEOUT, TimeUnit.SECONDS)))
            {
                this.scheduledPool.shutdownNow();
            }
        }
        catch (InterruptedException ie)
        {
            this.scheduledPool.shutdownNow();
        }
        LOGGER.info("[fms] ThreadPoolManager is shutdowned");
    }
    
    /**
     * 
     * 取得线程池信息 
     * 〈功能详细描述〉]
     * @return  String 线程池信息
     */
    public String getThreadPoolInfo()
    {
        StringBuffer buf = new StringBuffer();
        buf.append("{threadPoolTasks:[");
        for (int i = 0; i < this.threadPoolTasks.size(); ++i)
        {
            if (i > 0)
            {
                buf.append(" ,");
            }
            CustomizedFutureTask task = (CustomizedFutureTask) this.threadPoolTasks.get(i);
            buf.append(task.getTaskClass());
        }
        buf.append("]}");
        return buf.toString();
    }
}
