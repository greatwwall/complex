/**
 * 流水线加工模型任务
 * 加工数据并流向下一道工序，依次从一个队列取出内容进行加工，然后将加工后的数据放入下一个队列
 * 注意：任务一旦停止，无法再次启动
 */
public abstract class ConnectRunnable<E1, E2> implements Runnable
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(ConnectRunnable.class);
    
    /**
     * 线程状态是否异常的标志
     */
    private int status = AlarmConstant.STATUS_NORMAL;
    
    /**
     * 来料待加工队列，FIFO（先进先出）的线程安全队列
     */
    private transient SerializeQueue<E1> inQueue = null;
    
    /**
     * 已加工队列，FIFO（先进先出）的线程安全队列
     */
    private transient SerializeQueue<E2> outQueue = null;
    
    /**
     * 已加工队列，FIFO（先进先出）的线程安全队列 
     * 向云管理系统主动上报的所有告警
     */
    private transient SerializeQueue<E2> queue = null;
    
    /**
     * 结束条件
     */
    private transient boolean threadRunning = false;
    
    /**
     * Future 表示异步计算的结果
     */
    private Future< ? > future = null;
    
    /**
     * 构造函数
     * @param inQueue 来料加工队列
     * @param outQueue 加工后队列
     */
    public ConnectRunnable(SerializeQueue<E1> inQueue, SerializeQueue<E2> outQueue)
    {
        this.inQueue = inQueue;
        this.outQueue = outQueue;
        
        // 设置线程开始标识
        threadRunning = true;
    }
    
    /**
     * 线程是否正在运行
     * @return boolean 正在运行返回true，否则返回false
     */
    public boolean isRunning()
    {
        return threadRunning;
    }
    
    /**
     * 消息处理
     */
    @Override
    public void run()
    {
        // 设置线程开始
        while (threadRunning)
        {
            try
            {
                setStatus(AlarmConstant.STATUS_NORMAL);
                processRun();
            }
            catch (Exception e)
            {
                LOGGER.error("[fms] run while failed!", e);
            }
        }
        
        // 释放引用，确保不再使用,
        inQueue = null;
        outQueue = null;
    }
    
    /**
     * 开始运行任务
     */
    public final void start()
    {
        LOGGER.debug("[fms] enter method: start..");
        
        // 判断线程是否可运行
        if (!threadRunning)
        {
            LOGGER.warn("[fms] thread not allow to running!");
            return;
        }
        
        synchronized (this)
        {
            if (null != future)
            {
                LOGGER.warn("[fms] thread is running already!");
                return;
            }
            
            // 获取框架线程池管理器
            ThreadPoolManager threadPoolManager = ThreadPoolManager.getInstance();
            if (null == threadPoolManager)
            {
                LOGGER.error("[fms] threadPoolManager is null, please check properties");
                return;
            }
            ThreadPoolExecutor threadExecutor = threadPoolManager.getThreadPool();
            if (null == threadExecutor)
            {
                LOGGER.error("[fms] threadExecutor is null, please check properties");
                return;
            }
            
            // 开始执行线程任务
            future = threadExecutor.submit(this);
        }
        
        LOGGER.debug("[fms] leave method: start..");
    }
    
    /**
     * 停止消息处理线程
     */
    public final void stop()
    {
        LOGGER.debug("[fms] enter method: stop..");
        
        // 设置线程停止
        threadRunning = false;
        
        // 同步避免重复stop
        synchronized (this)
        {
            // cancel future
            if (null != future)
            {
                future.cancel(true);
                future = null;
            }
        }
        
        LOGGER.debug("[fms] leave method: stop..");
    }
    
    /**
     * 休眠
     * @param sleepTime 休眠时间
     */
    public final void sleep(long sleepTime)
    {
        try
        {
            /**
             * Fority:
             * 问题:J2EE Bad Practices: Threads
             * 描述:J2EE 标准禁止在某些环境下使用 Web 应用程序中的线程管理，因为此时使用线程管理非常容易出错。
             * 线程管理起来很困难，并且可能还会以不可预知的方式干扰应用程序容器。即使容器没有受到干扰，
             * 线程管理通常还会导致各种难以发现的错误，如死锁、race condition 及其他同步错误等。
             * 不整改的原因:这是基本的JAVA应用，不是Web应用，使用线程不会出现问题描述的场景
             */
            Thread.sleep(sleepTime);
        }
        catch (InterruptedException e)
        {
            LOGGER.info("[fms] thread sleep error.", e);
        }
    }
    
    /**
     * 获取线程状态
     * @return  线程状态
     */
    public int getStatus()
    {
        synchronized (this)
        {
            return status;
        }
    }
    
    /**
     * 设置线程状态
     * @param status     线程状态
     */
    public void setStatus(int status)
    {
        synchronized (this)
        {
            this.status = status;
        }
    }
    
    
    
    public SerializeQueue<E1> getInQueue()
    {
        return inQueue;
    }

    public void setInQueue(SerializeQueue<E1> inQueue)
    {
        this.inQueue = inQueue;
    }

    public SerializeQueue<E2> getOutQueue()
    {
        return outQueue;
    }

    public void setOutQueue(SerializeQueue<E2> outQueue)
    {
        this.outQueue = outQueue;
    }

    public SerializeQueue<E2> getQueue()
    {
        return queue;
    }

    public void setQueue(SerializeQueue<E2> queue)
    {
        this.queue = queue;
    }

    public boolean isThreadRunning()
    {
        return threadRunning;
    }

    public void setThreadRunning(boolean threadRunning)
    {
        this.threadRunning = threadRunning;
    }

    /**
     * 加工
     */
    protected abstract void processRun();
}
