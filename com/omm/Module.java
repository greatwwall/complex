public class FmsModule
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(FmsModule.class);
    
    /**
     * 线程实例对象
     */
    private static FmsModule instance = null;
    
    /**
     * 单实例标示
     */
    private static boolean instanceFlag = false;
    
    /**
     * 创建定时删除已清除告警的定时器
     */
    private Future< ? > timerOfDeleteAlarm = null;
    
    /**
     * FMS返回给FMA的ACK消息队列
     */
    private SerializeQueue<AlarmAckModel> fmaAckQueue = null;
    
    /**
     * 告警缓存队列，接收来自fma的上报告警，等待预处理
     */
    private SerializeQueue<AlarmHandleModel> cacheAlarmQueue = null;
    
    /**
     * 告警待入库队列，预处理后告警队列
     */
    private SerializeQueue<AlarmModel> beStoreAlarmQueue = null;
    
    /**
     * 告警批入库队列，准备一次批处理入库的告警队列
     */
    private SerializeQueue<AlarmModel> storeAlarmQueue = null;
    
    /**
     * 告警上报队列
     */
    private SerializeQueue<AlarmModel> reportAlarmQueue = null;
    
    /**
     * 告警ack发送线程对象
     */
    private ProcessAckAlarm ackAlarmRunnable = null;
    
    /**
     * 告警预处理线程对象
     */
    private ProcessPretreatAlarm pretreatAlarmRunnable = null;
    
    /**
     * 告警入库线程对象
     */
    private ProcessStoreAlarm storeAlarmRunnable = null;
    
    /**
     * 告警上报线程对象
     */
    private ProcessReportAlarm reportAlarmRunnable = null;
    
    /**
     * 访问数据库锁，目前在入库和手动清除的时候加锁，使用UUID保证唯一性
     */
    private String accessDbLock = "7e44c083-fafa-4ebf-8366-d704ac60f744";
    
    /**
     * 数据库记录计数器 
     */
    private DbCounter dbCounter = null;
    
    /**
     * FMS运行的根目录
     */
    private String rootDir = null;
    
    /**
     * 滑窗大小
     */
    private int maxSlipWindow = 0;
    
    /**
     * 滑窗管理类
     */
    private SlipWindowManager slipWindowManager = null;
    
    /**
     * 是否支持更新告警的开关
     */
    private boolean enableUpdateAlarm = true;
    
    /**
     * 单例模式获取对象
     * @return 本类对象
     * Coverity-Unguarded read (GUARDED_BY_VIOLATION)
     * 描述：getInstance()方法同时被多个方法,可能产生多个对象
     * 不修改原因：该方法内部已经加锁,不会产生多个对象。
     */
    public static FmsModule getInstance()
    {
        if (!instanceFlag)
        {
            synchronized (FmsModule.class)
            {
                if (null == instance)
                {
                    instance = new FmsModule();
                    instanceFlag = true;
                }
            }
        }
        
        return instance;
    }
    
    /**
     * 线程初始化
     * 提供告警解析，告警入库，告警上报线程的初始化
     * @return true:初始化成功  false：初始化失败
     */
    public boolean init()
    {
        LOGGER.info("[fms] Start initialize FMS module.");
        
        /**
         * Fority:
         * 问题:FORTIFY.Poor_Error_Handling--Overly_Broad_Catch，
         * 描述:使用一个“简约”的 catch 块捕获高级别的异常类（如 Exception），
         *     可能会混淆那些需要特殊处理的异常，或是捕获了不应在程序中这一点捕获的异常。
         * 不整改的原因:代码不会出现需要特殊处理的异常，为防止代码中有不可预知的异常，因此捕获一个大异常即可。
         */
        try
        {
            //创建数据表记录计数器
            dbCounter = new DbCounter();
            
            //获取告警历史表恢复告警记录数
            if (!dbCounter.readFromAlarmLog())
            {
                LOGGER.error("[fms] init:read from alarmLogTable failed.");
                return false;
            }
            
        }
        catch (Exception e)
        {
            LOGGER.error("[fms] init:read db exception.", e);
            return false;
        }
        
        LOGGER.info("[fms] successful to get the dbcounter.");
        
        //获取FMS运行根目录
        rootDir = FmsUtil.getRootDir();
        
        // 初始化队列
        initQueue();
        
        try
        {
            // 创建并启动线程，包括ack线程、告警入库线程、告警预处理线程、事件入库线程、告警上报线程
            initThread();
            
            //创建定时删除已删除告警的定时器
            AutoClearAlarmTask deleteAlarmJob = new AutoClearAlarmTask();
            setTimerOfDeleteAlarm(deleteAlarmJob.getFuture());
            
            //创建不同源告警管理实例
            DiffSourceAlarmManager.getInstance();
            
            //读取配置文件中滑窗大小
            maxSlipWindow = AppProperties.getAsInt("max_sipWindow", AlarmConstant.DEFAULT_MAX_WINDOW);
            LOGGER.info("[fms] maxSlipWindow:{}.", maxSlipWindow);
            
            //读取配置文件中的是否支持更新告警开关，默认为true，支持更新告警机制
            enableUpdateAlarm =
                AppProperties.getAsBoolean("enableUpdateAlarm", AlarmConstant.DEFAULT_ENABLE_UPDATE_ALARM);
            
            LOGGER.info("[fms] enableUpdateAlarm:{}.", enableUpdateAlarm);
        }
        catch (Exception e)
        {
            LOGGER.error("[fms] init failed.", e);
            return false;
        }
        
        //创建滑窗管理类实例
        slipWindowManager = new SlipWindowManager();
        
        return true;
    }
    
    private void initThread()
    {
        
        // 创建告警入库线程
        storeAlarmRunnable = new ProcessStoreAlarm(beStoreAlarmQueue, storeAlarmQueue);
        
        // 创建告警预处理线程
        pretreatAlarmRunnable = new ProcessPretreatAlarm(cacheAlarmQueue, beStoreAlarmQueue);
        
        // 启动告警入库线程
        storeAlarmRunnable.start();
        LOGGER.info("[fms] Alarm storage thread start success.");
        
        // 启动告警预处理线程
        pretreatAlarmRunnable.start();
        LOGGER.info("[fms] Alarm pretreatment thread start success.");
        
        // 启动fms上报线程
        startReportAlarmRunnable();
    }
    
    /**
     * 停止线程方法
     */
    public void stop()
    {
        //取消定时删除已清除告警的定时任务
        if (null != timerOfDeleteAlarm)
        {
            LOGGER.info("[fms]cancel DeleteClearedAlarm.");
            timerOfDeleteAlarm.cancel(true);
            timerOfDeleteAlarm = null;
        }
        
        //取消定时检查不同源告警缓存的定时器
        if (null != DiffSourceAlarmManager.getInstance().getTimerOfDiffSourceAlarm())
        {
            LOGGER.info("[fms]cancel timerOfDiffSourceAlarm.");
            DiffSourceAlarmManager.getInstance().getTimerOfDiffSourceAlarm().cancel(true);
        }
        
        //取消告警预处理线程
        if (null != pretreatAlarmRunnable)
        {
            LOGGER.info("Stop alarm pretreat thread.");
            pretreatAlarmRunnable.stop();
            pretreatAlarmRunnable = null;
        }
        
        //取消告警存储线程
        if (null != storeAlarmRunnable)
        {
            LOGGER.info("Stop alarm storage thread.");
            storeAlarmRunnable.stop();
            storeAlarmRunnable = null;
        }
        
        //取消告警上报线程
        if (null != reportAlarmRunnable)
        {
            LOGGER.info("Stop alarm report thread.");
            reportAlarmRunnable.stop();
            reportAlarmRunnable = null;
        }
    }
    
    /**
     * 
     * 仅启动报告处理线程
     * 
     */
    public synchronized void startReportAlarmRunnable()
    {
        if (null == reportAlarmRunnable)
        {
            // 创建告警上报队列，队列长度500
            reportAlarmQueue = new SerializeQueue<AlarmModel>(AlarmConstant.MAX_BATCH_REPORT_SIZE);
            LOGGER.info("[fms] Create alarm retpor queue succuss.maxSize:{}", AlarmConstant.MAX_BATCH_REPORT_SIZE);
            
            reportAlarmRunnable = new ProcessReportAlarm(reportAlarmQueue, null);
            reportAlarmRunnable.start();
            LOGGER.info("[fms] Alarm report thread start success.");
        }
        else
        {
            LOGGER.info("[fms] report thread is already started.");
        }
    }
    
    /**
     * 
     * 仅停止报告处理线程
     * 
     */
    public void stopReportAlarmRunnable()
    {
        //取消告警上报线程
        if (null != reportAlarmRunnable)
        {
            LOGGER.info("Stop alarm report thread.");
            reportAlarmRunnable.stop();
            reportAlarmRunnable = null;
        }
    }
    
    /**
     * 获取ACK返回队列
     * @return  ACK返回队列
     */
    public SerializeQueue<AlarmAckModel> getFmaAckQueue()
    {
        return fmaAckQueue;
    }
    
    /**
     * 
     * 加入ACK队列
     * @param alarmAckModel     返回ACK的对象
     * @return  true 成功 false 失败
     */
    public synchronized boolean offerToAckQueue(AlarmAckModel alarmAckModel)
    {
        return fmaAckQueue.offer(alarmAckModel);
    }
    
    /**
     * 获取告警缓存队列
     * @return  告警缓存队列
     */
    public SerializeQueue<AlarmHandleModel> getAlarmCacheQueue()
    {
        return cacheAlarmQueue;
    }
    
    public SerializeQueue<AlarmModel> getBeStoreAlarmQueue()
    {
        return beStoreAlarmQueue;
    }
    
    public SerializeQueue<AlarmModel> getStoreAlarmQueue()
    {
        return storeAlarmQueue;
    }
    
    /**
     * 获取告警上报队列
     * @return  告警上报队列
     */
    public synchronized SerializeQueue<AlarmModel> getReportAlarmQueue()
    {
        return reportAlarmQueue;
    }
    
    public Future< ? > getTimerOfDeleteAlarm()
    {
        return timerOfDeleteAlarm;
    }
    
    public ProcessPretreatAlarm getPretreatAlarmRunnable()
    {
        return pretreatAlarmRunnable;
    }
    
    public ProcessStoreAlarm getStoreAlarmRunnable()
    {
        return storeAlarmRunnable;
    }
    
    public ProcessReportAlarm getReportAlarmRunnable()
    {
        return reportAlarmRunnable;
    }
    
    public ProcessAckAlarm getAckAlarmRunnable()
    {
        return ackAlarmRunnable;
    }
    
    public void setTimerOfDeleteAlarm(Future< ? > timerOfDeleteAlarm)
    {
        this.timerOfDeleteAlarm = timerOfDeleteAlarm;
    }
    
    /**
     * 获取数据库访问锁
     * @return  数据库访问锁
     */
    public String getAccessDbLock()
    {
        return accessDbLock;
    }
    
    /**
     * 初始化队列
     */
    private void initQueue()
    {
        // ACK返回队列，队列长度为6000
        fmaAckQueue = new SerializeQueue<AlarmAckModel>(AlarmConstant.MAX_ALARM_ACK_SIZE);
        LOGGER.info("[fms] Create alarm Ack queue succuss.maxSize:{}", AlarmConstant.MAX_ALARM_ACK_SIZE);
        
        // 创建告警缓存队列，队列长度6000
        cacheAlarmQueue = new SerializeQueue<AlarmHandleModel>(AlarmConstant.MAX_CACHE_QUEUE_SIZE);
        LOGGER.info("[fms] Create alarm cache queue succuss.maxSize:{}", AlarmConstant.MAX_CACHE_QUEUE_SIZE);
        
        // 创建告警待存储队列，队列长度1000
        beStoreAlarmQueue = new SerializeQueue<AlarmModel>(AlarmConstant.MAX_BESTORE_QUEUE_SIZE);
        LOGGER.info("[fms] Create alarm beStore queue succuss.maxSize:{}", AlarmConstant.MAX_BESTORE_QUEUE_SIZE);
        
        // 创建告警批处理入库队列，队列长度500
        storeAlarmQueue = new SerializeQueue<AlarmModel>(AlarmConstant.MAX_BATCH_STORE_SIZE);
        LOGGER.info("[fms] Create alarm batch store queue succuss.maxSize:{}", AlarmConstant.MAX_BATCH_STORE_SIZE);
    }
    
    public DbCounter getDbCounter()
    {
        return dbCounter;
    }
    
    public void setDbCounter(DbCounter dbCounter)
    {
        this.dbCounter = dbCounter;
    }
    
    public String getRootDir()
    {
        return rootDir;
    }
    
    public void setRootDir(String rootDir)
    {
        this.rootDir = rootDir;
    }
    
    public SlipWindowManager getSlipWindowManager()
    {
        return slipWindowManager;
    }
    
    public void setSlipWindowManager(SlipWindowManager slipWindowManager)
    {
        this.slipWindowManager = slipWindowManager;
    }
    
    public int getMaxSlipWindow()
    {
        return maxSlipWindow;
    }
    
    public void setMaxSlipWindow(int maxSlipWindow)
    {
        this.maxSlipWindow = maxSlipWindow;
    }
    
    public boolean isEnableUpdateAlarm()
    {
        return enableUpdateAlarm;
    }
    
    public void setEnableUpdateAlarm(boolean enableUpdateAlarm)
    {
        this.enableUpdateAlarm = enableUpdateAlarm;
    }
}
