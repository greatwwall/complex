/**
 * 
 * 上报告警线程类
 * 上报入库成功的告警至云管理
 */
public class ProcessReportAlarm extends ConnectRunnable<AlarmModel, AlarmModel>
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(ProcessReportAlarm.class);
    
    /**
     * 上报云管理默认端口
     */
    private static final String DEFAULT_REPORT_GM_PORT = "80";
    
    /**
     * 一次批上报的告警
     */
    private List<AlarmModel> batchReportAlarmList = new ArrayList<AlarmModel>();
    
    /**
     * 构造函数
     * @param inQueue 来料加工队列
     * @param outQueue 加工后队列
     */
    public ProcessReportAlarm(SerializeQueue<AlarmModel> inQueue, SerializeQueue<AlarmModel> outQueue)
    {
        super(inQueue, outQueue);
    }
    
    /**
     * 
     * 主动上报处理方法
     * 此方法中从上报队列中取得上报告警进行封装上报给云管理
     */
    @Override
    protected void processRun()
    {
        LOGGER.debug("[fms] ProcessReportAlarm: report alarm processRun enter.");
        
        // Step 1: 队列为空，说明队列初始化有问题，进程直接退出
        /**
         * Fority:
         * 问题:FORTIFY.J2EE_Bad_Practices--System.exit
         * 描述:让 Web 应用程序试图关闭自身的容器并不是什么好主意。调用 System.exit() 的操作可能包含在
         *      leftover debug code 中，或者包含在从非 J2EE 应用程序导入的代码中。
         * 不整改的原因:进程启动时为保证功能完整，如果入队列为空，说明队列初始化有问题，进程直接退出
         */
        SerializeQueue<AlarmModel> inQueue = getInQueue();
        if (null == inQueue)
        {
            LOGGER.error("[fms] inQueue is null!");
            System.exit(1);
        }
        
        // 检查来料队列中是否有内容
        if (inQueue.isEmpty())
        {
            LOGGER.debug("[fms] inQueue is empty,nothing to report!");
            sleep(AlarmConstant.WAIT_TIME);
            return;
        }
        
        // step 2: 从输入队列中获取一次加工的内容,直接扔到输出队列
        batchReportAlarmList.clear();
        inQueue.drainTo(batchReportAlarmList, AlarmConstant.MAX_BATCH_REPORT_SIZE);
        
        /**
         * 上报WS的告警数据类，对于GM的IP和端口，由于是接口约定，所以FMS上报时带默认值
         * FMWS会从安装模块和配置文件中获取真实的GM的IP和端口
         */
        AlarmReportModel alarmReportModel = new AlarmReportModel(AlarmConstant.DEFAULT_SERVER_IP,
            Integer.valueOf(DEFAULT_REPORT_GM_PORT), batchReportAlarmList);
        
        //发送告警到hermes
        reportToHermes(alarmReportModel);
        
        LOGGER.debug("[fms] ProcessReportAlarm: report alarm processRun leave.");
    }
    
    /**
     * 发送告警到hermes
     *    
     * RPC将告警发送到hermes
     * @param alarmReportModel     转换为JSON字符串的告警信息
     */
    private void reportToHermes(AlarmReportModel alarmReportModel)
    {
        if (null == alarmReportModel)
        {
            LOGGER.error("[fms] alarmReportModel is null.");
            return;
        }
        LOGGER.info("[fms] report alarm to local hermes.");
        IFmwsRpcService service = ServiceProxyFactory.lookup(IFmwsRpcService.class);
        
        /**
         * Fority:
         * 问题:FORTIFY.Poor_Error_Handling--Overly_Broad_Catch，
         * 描述:使用一个“简约”的 catch 块捕获高级别的异常类（如 Exception），
         *     可能会混淆那些需要特殊处理的异常，或是捕获了不应在程序中这一点捕获的异常。
         * 不整改的原因:代码不会出现需要特殊处理的异常，为防止代码中有不可预知的异常，因此捕获一个大异常即可。
         */
        boolean result = true;
        try
        {
            result = service.receiveAlarmFromFMS(alarmReportModel);
        }
        catch (Exception exception)
        {
            LOGGER.error("[fms] reportToHermes RPCClient error!", exception);
        }
        
        LOGGER.warn("[fms] reportToHermes alarmReportModel:{}", alarmReportModel);
        LOGGER.info("[fms] Rpc result is {}.", result);
    }
}
