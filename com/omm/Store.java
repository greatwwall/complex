/**
 * 
 * 告警入库线程类 
 * 用批处理一次将多条告警处理入库
 */
public class ProcessStoreAlarm extends ConnectRunnable<AlarmModel, AlarmModel>
{
    private static final int PARAM_MATCH_FLAG = 1;
    
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(ProcessStoreAlarm.class);
    
    private static FmsModule instance = FmsModule.getInstance();
    
    /**
     * 告警数据访问对象
     */
    private IAlarmDAO alarmDAO = new AlarmDAOImpl();
    
    /**
     * 数据库操作对象,用户查询数据库数量
     */
    private IAlarmCommandDao querryDB = new AlarmCommandDaoImpl();
    
    /**
     * 当前活动告警队列，用于保存告警的最新状态，批量入活动告警表
     */
    private List<AlarmModel> addActiveAlarmList = new ArrayList<AlarmModel>();
    
    /**
     * 更新告警队列，用于保存原始告警及其更新告警，批量入历史告警表
     */
    private List<AlarmModel> updateAlarmList = new ArrayList<AlarmModel>();
    
    /**
     * 恢复告警队列，用于保存恢复告警，批量入历史告警表
     */
    private List<AlarmModel> clearAlarmList = new ArrayList<AlarmModel>();
    
    /**
     * 同步号队列，用于保存数据库活动表中在缓存中被更新或被清除的活动告警记录的同步号，批量入库时更新数据库活动表中相应同步号的告警状态
     */
    private List<Integer> syncNoList = new ArrayList<Integer>();
    
    /**
     * 构造函数
     * @param inQueue 来料加工队列
     * @param outQueue 加工后队列
     */
    public ProcessStoreAlarm(SerializeQueue<AlarmModel> inQueue, SerializeQueue<AlarmModel> outQueue)
    {
        super(inQueue, outQueue);
    }
    
    /**
     * 加工处理
     * 
     * 从入队列取出一批原料，直接放到出队列，然后执行批存储动作
     */
    @Override
    protected void processRun()
    {
        LOGGER.debug("[fms] store alarm processRun enter.");
        
        boolean bSuccess = false;
        
        // 数据库访问锁，此锁代码固定写的"7e44c083-fafa-4ebf-8366-d704ac60f744"，告警入库、事件入库、告警手动清除都使用此锁
        synchronized (instance.getAccessDbLock())
        {
            // 批量入库
            bSuccess = batchInsertAlarm();
            
            // 清除缓存
            clear();
        }
        
        // 失败则休息一段时间后再重试
        if (!bSuccess)
        {
            sleep(AlarmConstant.WAIT_TIME);
        }
        
        LOGGER.debug("[fms] store alarm processRun leave.");
    }
    
    /**
     * 
     * 实现告警的批量入库
     * 实现告警的批量入库 
     * @return  true:入库成功 false:入库失败
     */
    private boolean batchInsertAlarm()
    {
        LOGGER.debug("[fms] batchInsertAlarm enter.");
        
        SerializeQueue<AlarmModel> inQueue = getInQueue();
        SerializeQueue<AlarmModel> outQueue = getOutQueue();
        
        // Step 1: 检查来料队列和出队列中是否有内容，如果都为空，则直接返回false,不做处理
        if (inQueue.isEmpty() && outQueue.isEmpty())
        {
            LOGGER.debug("[fms] inQueue and outQueue are all empty!");
            return false;
        }
        
        // Step 2: 如果来料队列非空，则从来料队列中获取一次加工的内容,直接仍往输出队列做批存储
        if (!inQueue.isEmpty())
        {
            inQueue.drainTo(outQueue, outQueue.remainingCapacity());
        }
        
        LOGGER.info("[fms] begin store:{}", outQueue.size());
        
        // Step 3: 读取同步号、流水号
        if (!NoAllocator.getInstance().readAllocNo())
        {
            // 初始化后，如果读取同步号、流水号失败,数据库异常的场景，返回等待下次运行。
            // 场景说明：在此保留outQueue队列中的告警，否则会导致该队列中告警在fms的内存中丢失
            //         由于这些告警在滑窗中已经注册，oma周期再上报这些告警时fms不再接收，直接丢弃，导致这些告警无法入库
            LOGGER.error("[fms] Read AllocNo failed, allocInfo:{}", NoAllocator.getInstance());
            return false;
        }
        // Step 4:执行存储 
        // 场景说明： 告警入库成功，则清空出队列outQueue，否则会导致重复告警的死循环入库 
        //         告警入库失败或入库抛异常时，在此保留outQueue队列中的告警，否则会导致该队列中告警在fms的内存中丢失
        //         由于这些告警在滑窗中已经注册，oma周期再上报这些告警时fms不再接收，直接丢弃，导致这些告警无法入库
        boolean ret = false;
        try
        {
            ret = storeAlarm();
            //如果告警入库成功，则清空出队列，否则会导致重复告警的死循环入库 
            if (ret)
            {
                outQueue.clear();
            }
        }
        catch (Exception e)
        {
            LOGGER.error("[fms] Store alarm exception:{}.", e);
        }
        
        LOGGER.debug("[fms] batchInsertAlarm leave.");
        return ret;
    }
    
    /**
     * 
     * 告警入库流程
     * 执行告警入库的功能 
     * @return  true:入库成功 false:入库失败
     */
    private boolean storeAlarm()
    {
        LOGGER.debug("[fms] storeAlarm enter.");
        
        // Step 1: 初始化临时队列等后续能用到变量
        clear();
        
        // Step 2: 分发告警，即处理告警重复抑制，并放入相应的缓存队列，使用数据库长连接
        if (!dispatchAlarm())
        {
            // 分发告警失败，只在数据库异常时会出现
            return false;
        }
        
        // Step 3: 统一处理最后活动告警表,日志告警表的入库操作,失败会一直进行
        if (!storeToDb())
        {
            // 告警入库失败，只在数据库异常时会出现
            return false;
        }
        
        // Step 4: 成功入库的告警加入ACK队列，由ACK线程处理
        SerializeQueue<AlarmModel> outQueue = getOutQueue();
        AckModelConvertor.offerToAlarmAckQueue(outQueue);
        
        // Step 5: 告警上报IES
        addToReportAlarm();
        
        // Step 6: 判断活动库/历史库是否超出规格，需要清理
        clearOverflowAlarm();
        
        // Step 7: 历史表超出规格进行告警转储
        if (instance.getDbCounter().getTblAlarmLogCount() >= AlarmConstant.ALARM_LOG_MAX_CONTAIN)
        {
            //启动告警转储
            DepositeOperateTask.getInstance().depositeLogRecord();
        }
        
        LOGGER.debug("[fms] storeAlarm enter.");
        return true;
    }
    
    /**
     * 
     * 分发告警
     * 根据告警的种类（恢复告警 原始告警还是更新告警）进行分别处理 
     * @return  true:分发处理成功 false:分发处理失败
     */
    private boolean dispatchAlarm()
    {
        LOGGER.debug("[fms] treatAlarm enter.");
        
        try
        {
            // 循环处理每一条AlarmMsgModel
            SerializeQueue<AlarmModel> outQueue = getOutQueue();
            Iterator<AlarmModel> iterator = outQueue.iterator();
            AlarmModel alarm = null;
            while (iterator.hasNext())
            {
                alarm = iterator.next();
                
                boolean treatFlag = dealWithAlarnStatus(alarm);
                
                if (treatFlag)
                {
                    continue;
                }
                
                //获取不同源告警管理类
                DiffSourceAlarmManager diffSourceAlarmManager = DiffSourceAlarmManager.getInstance();
                
                //对分发失败的不同源告警的处理，不同源告警处理完成则在缓存中删除此条告警
                if (diffSourceAlarmManager.getDiffSourceAlarmMap().containsKey(alarm.getSvAlarmId()))
                {
                    diffSourceAlarmManager.removeFromCache(alarm);
                }
                
                //直接删除
                iterator.remove();
            }
        }
        catch (DBOperatorException e)
        {
            //在告警活动表中匹配活动告警失败，数据库异常场景
            LOGGER.error("[fms] treatAlarm Exception.", e);
            return false;
        }
        
        LOGGER.debug("[fms] treatAlarm leave.");
        return true;
    }
    
    private boolean dealWithAlarnStatus(AlarmModel alarm)
    {
        boolean treatFlag = false;
        switch (AlarmCategory.findByValue(alarm.getIAlarmCategory()))
        {
            // 原始告警
            case ALARM:
                treatFlag = treatActiveAlarm(alarm);
                break;
            
            // 清除告警
            case CLEAR:
                treatFlag = treatClearAlarm(alarm);
                break;
            
            // 告警类型不合法
            default:
                treatFlag = false;
                break;
        }
        return treatFlag;
    }
    
    private void setActAlarmMask(AlarmModel alarm)
    {
        // 内部告警和外部告警（上报给GM的告警号）转换
        AlarmDefinition alarmDefinition = AlarmStaticDataManager.getInstance()
                .getAlarmDefinition(StringUtils.trim(alarm.getSvAlarmId()));
        
        if (null == alarmDefinition)
        {
            return;
        }
        
        // 获取告警规则队列
        List<String> maskExtAlarmLst = AlarmStaticDataManager.getInstance().getMaskExtAlarmLst();
        
        // 需要过滤，则设置过滤字段为1
        if (maskExtAlarmLst.contains(alarmDefinition.getSvExtAlarmId()))
        {
            alarm.setIDisplay(AlarmConstant.ALARM_NDISPLAY_WEBUI);
        }
    }
    
    /**
     * 执行批存储入库
     * 失败则不断重试，直到入库成功
     * @return boolean 成功返回true,失败返回false
     */
    private boolean storeToDb()
    {
        LOGGER.debug("[fms] storeToDb enter.");
        
        SerializeQueue<AlarmModel> outQueue = this.getOutQueue();
        if (outQueue.isEmpty())
        {
            // 出队列为空，直接返回成功
            LOGGER.debug("[fms] storeAlarm outQueue is empty.");
            return true;
        }
        
        AlarmBatchStoreModel batchModel = new AlarmBatchStoreModel();
        batchModel.setAddActiveAlarmList(addActiveAlarmList);
        batchModel.setUpdateAlarmList(updateAlarmList);
        batchModel.setClearAlarmList(clearAlarmList);
        batchModel.setSyncNoList(syncNoList);
        batchModel.setAlarmSerialNo(NoAllocator.getInstance().getAlarmSerialNo().get());
        batchModel.setAlarmSyncNo(NoAllocator.getInstance().getAlarmSyncNo().get());
        
        //事务调用批量入库操作
        IAlarmDAO alarmDAOWithTrans = ServiceProxyFactory.localLookup(IAlarmDAO.class);
        boolean isSucess = alarmDAOWithTrans.batchStore(batchModel);
        if (isSucess)
        {
            LOGGER.info("[fms] Store alarm to database success. batchModel:{}", batchModel);
            
            instance.getDbCounter()
                    .setTblAlarmLogCount(instance.getDbCounter().getTblAlarmLogCount() + clearAlarmList.size());
        }
        else
        {
            LOGGER.error("[fms] Store alarm to database failed.");
        }
        
        batchModel.setAddActiveAlarmList(null);
        batchModel.setUpdateActiveAlarmList(null);
        batchModel.setUpdateAlarmList(null);
        batchModel.setClearAlarmList(null);
        
        //获取不同源告警管理类，对入库成功的不同源告警，在缓存中删除
        DiffSourceAlarmManager.getInstance().removeFromCache(outQueue);
        
        LOGGER.debug("[fms] storeToDb leave.");
        return isSucess;
    }
    
    /**
     * 分发原始告警
     * 
     * 查询活动告警库进行重复告警的抑制操作，最后放入入库队列
     * 
     * @param activeAlarm 被分发的原始告警
     * @return boolean 成功返回true,失败返回false
     */
    private boolean treatActiveAlarm(AlarmModel activeAlarm)
    {
        LOGGER.info("[fms] 1_0 treatActiveAlarm enter {}.", activeAlarm);
        // 克隆被分发的活动告警，防止对象引用导致回ack的sequenceNo被改变
        AlarmModel curActAlarm = activeAlarm.cloneAlarmModel();
        if (null == curActAlarm)
        {
            LOGGER.warn("[fms] active alarm clone failed. discard alarm:{}", activeAlarm);
            return false;
        }
        
        // 设置上报的活动告警的过滤字段
        setActAlarmMask(curActAlarm);
        
        // 在临时队列里查找匹配的活动告警
        AlarmModel matchCacheAlarm = matchActiveAlarm(curActAlarm);
        LOGGER.info("[fms] 1_1 matchActiveAlarm {}.", matchCacheAlarm);
        
        // 在数据库中查找匹配的活动告警
        if (null == matchCacheAlarm)
        {
            if (!doBranch(curActAlarm))
            {
                return false;
            }
        }
        else
        {
            //在缓存中匹配到活动告警，说明该告警肯定没有被恢复，则进行重复告警、更新告警的处理
            if (!handleAlarmFromCache(curActAlarm, matchCacheAlarm))
            {
                return false;
            }
        }
        
        LOGGER.debug("[fms] treatActiveAlarm leave.");
        //matchCacheAlarm = null;
        return true;
    }
    
    private boolean doBranch(AlarmModel curActAlarm)
    {
        AlarmModel matchDbAlarm = matchDbAlarmParams(curActAlarm);
        LOGGER.info("[fms] 1_2 matchDbAlarmParams {}.", matchDbAlarm);
        
        if (null == matchDbAlarm)
        {
            // 在数据库中没找着匹配的活动告警，则被分发的是一条新的活动告警，加入addActiveAlarmList、updateAlarmList
            if (!addNewActiveAlarm(curActAlarm))
            {
                return false;
            }
        }
        else
        {
            // 在数据库中匹配到活动告警，则判断是否该在内存中被恢复
            if (!isClearedAlarm(matchDbAlarm) && !handleAlarmFromDB(curActAlarm, matchDbAlarm))
            {
                // 告警在内存中没有被恢复，则进行重复告警、更新告警的处理
                return false;
            }
            
            // 匹配到的活动告警已被恢复，被分发的是一条新的活动告警，加入addActiveAlarmList、updateAlarmList
            if (isClearedAlarm(matchDbAlarm) && !addNewActiveAlarm(curActAlarm))
            {
                return false;
            }
        }
        
        return true;
    }
    
    private AlarmModel matchDbAlarmParams(AlarmModel curActAlarm)
    {
        List<AlarmModel> matchDbAlarms = alarmDAO.querySelfActiveAlarms(curActAlarm);
        
        if (matchDbAlarms == null || matchDbAlarms.isEmpty())
        {
            return null;
        }
        
        return matchDBAlarmParams(curActAlarm, matchDbAlarms);
    }
    
    private AlarmModel matchDbAlarmParamsById(AlarmModel curActAlarm)
    {
        List<AlarmModel> matchDbAlarms = alarmDAO.querySelfActiveAlarmsById(curActAlarm);
        
        if (matchDbAlarms == null || matchDbAlarms.isEmpty())
        {
            return null;
        }
        
        LOGGER.info("[fms] before params match the size is {}.", matchDbAlarms.size());
        
        return matchDBAlarmParams(curActAlarm, matchDbAlarms);
    }
    
    /**
     * 此方法对DB查询的告警进行参数匹配，获取唯一匹配的告警
     * @param curActAlarm 匹配的原告警
     * @param matchAlarms 待匹配的目标高级集合
     * @return
     */
    private AlarmModel matchDBAlarmParams(AlarmModel curActAlarm, List<AlarmModel> matchAlarms)
    {
        String lcoationParams = curActAlarm.getSvLocationInfo();
        
        // 如果当前告警无定位信息，则不做参数匹配
        if (lcoationParams == null || lcoationParams.equals(""))
        {
            LOGGER.warn("[fms] alarm {} lcoationParams is null.", curActAlarm.getSvAlarmId());
            return alarmDAO.querySelfActiveAlarm(curActAlarm);
        }
        
        List<AlarmLocationInfo> alarmLocInfos = AlarmStaticDataManager.getInstance()
                .getAlarmLocationInfo(curActAlarm.getSvAlarmId());
        
        // 获取告警定位信息失败，则不做参数匹配
        if (null == alarmLocInfos || alarmLocInfos.isEmpty())
        {
            LOGGER.warn("[fms] Can not get the alarmDefinition by {}.", curActAlarm.getSvAlarmId());
            return alarmDAO.querySelfActiveAlarm(curActAlarm);
        }
        
        int matchParamsNum = getMatchParamsNum(alarmLocInfos);
        // 如果参数都是需要匹配的，则直接完全匹配
        if (matchParamsNum == alarmLocInfos.size())
        {
            return alarmDAO.querySelfActiveAlarm(curActAlarm);
        }
        
        List<String> curLocList = AlarmProcessUtil.processParams(curActAlarm.getSvLocationInfo());
        
        // 如果告警参数个数与定义相同，说明是活动告警，参数索引的滑动处理相同
        boolean isCurrentAlarm = false;
        if (alarmLocInfos.size() == curLocList.size())
        {
            isCurrentAlarm = true;
        }
        
        // 如果当前告警参数个数少于匹配参数要求，匹配失败, 按完全匹配
        if (curLocList.size() < matchParamsNum)
        {
            LOGGER.warn("[fms] alarm location size: {} is not match location define num: {}.",
                    new Object[] {curLocList.size(), matchParamsNum});
            return alarmDAO.querySelfActiveAlarm(curActAlarm);
        }
        
        // 修改checkstyle缩进问题，使用标记避免使用外层循环flag
        boolean isMatched;
        for (AlarmModel alarmModel : matchAlarms)
        {
            isMatched = false;
            int indexDbAlarm = 0;
            int indexCurAlarm = 0;
            List<String> dbLocList = AlarmProcessUtil.processParams(alarmModel.getSvLocationInfo());
            
            for (AlarmLocationInfo alarmLocInfo : alarmLocInfos)
            {
                // 如果是不匹配参数，只将DB的参数索引要加1
                if (alarmLocInfo.getiMatchParam() != PARAM_MATCH_FLAG)
                {
                    indexDbAlarm++;
                    
                    if (isCurrentAlarm)
                    {
                        indexCurAlarm++;
                    }
                    
                    continue;
                }
                
                // 如果有参数不匹配则匹配下一条告警
                if (!curLocList.get(indexCurAlarm).equals(dbLocList.get(indexDbAlarm)))
                {
                    isMatched = false;
                    break;
                }
                
                isMatched = true;
                indexDbAlarm++;
                indexCurAlarm++;
            }
            
            if (isMatched)
            {
                return alarmModel;
            }
        }
        
        return null;
    }
    
    /**
     * 此方法对缓存的告警进行参数匹配，获取唯一匹配的告警，此方法用于清除告警匹配场景
     * @param curActAlarm 匹配的原告警
     * @param matchAlarms 待匹配的目标高级集合
     * @return
     */
    private AlarmModel matchCacheAlarmParams(AlarmModel curActAlarm, List<AlarmModel> matchAlarms)
    {
        String lcoationParams = curActAlarm.getSvLocationInfo();
        
        // 如果当前告警无定位信息，则不做参数匹配
        if (lcoationParams == null || lcoationParams.equals(""))
        {
            LOGGER.warn("[fms] alarm {} lcoationParams is null.", curActAlarm.getSvAlarmId());
            return matchAlarm(curActAlarm);
        }
        
        List<AlarmLocationInfo> alarmLocInfos = AlarmStaticDataManager.getInstance()
                .getAlarmLocationInfo(curActAlarm.getSvAlarmId());
        
        // 获取告警定位信息失败，则不做参数匹配
        if (null == alarmLocInfos || alarmLocInfos.isEmpty())
        {
            LOGGER.warn("[fms] Can not get the alarmDefinition by {}.", curActAlarm.getSvAlarmId());
            return matchAlarm(curActAlarm);
        }
        
        int matchParamsNum = getMatchParamsNum(alarmLocInfos);
        
        // 如果参数都是需要匹配的，则直接完全匹配
        if (matchParamsNum == alarmLocInfos.size())
        {
            return matchAlarm(curActAlarm);
        }
        
        List<String> curLocList = AlarmProcessUtil.processParams(curActAlarm.getSvLocationInfo());
        
        // 如果告警参数个数与定义相同，说明是活动告警，参数索引的滑动处理相同
        boolean isCurrentAlarm = false;
        if (alarmLocInfos.size() == curLocList.size())
        {
            isCurrentAlarm = true;
        }
        
        // 如果当前告警参数个数少于匹配参数要求，匹配失败, 按完全匹配
        if (curLocList.size() < matchParamsNum)
        {
            LOGGER.warn("[fms] alarm location size: {} is not match location define num: {}.",
                    new Object[] {curLocList.size(), matchParamsNum});
            return matchAlarm(curActAlarm);
        }
        // end 可靠性处理结束
        
        // 以下流程需要做严格的参数匹配，如果活动告警的参数个数与定位信息定义个数不一致则认为不能匹配，而不会做完全匹配尝试
        // 修改checkstyle缩进问题，使用标记避免使用外层循环flag
        boolean isMatched;
        for (AlarmModel alarmModel : matchAlarms)
        {
            //告警ID不匹配，直接进行下一条告警匹配
            if (!alarmModel.getSvAlarmId().equals(curActAlarm.getSvAlarmId()))
            {
                continue;
            }
            isMatched = false;
            int indexDbAlarm = 0;
            int indexCurAlarm = 0;
            List<String> cacheLocList = AlarmProcessUtil.processParams(alarmModel.getSvLocationInfo());
            
            for (AlarmLocationInfo alarmLocInfo : alarmLocInfos)
            {
                // 如果是不匹配参数，将参数索引加1
                if (alarmLocInfo.getiMatchParam() != PARAM_MATCH_FLAG)
                {
                    indexDbAlarm++;
                    
                    if (isCurrentAlarm)
                    {
                        indexCurAlarm++;
                    }
                    
                    continue;
                }
                
                if (curLocList.size()<=indexCurAlarm||cacheLocList.size()<=indexDbAlarm)
                {
                    isMatched = false;
                    break;
                }
				
                // 如果有参数不匹配则匹配下一条告警
                if (!curLocList.get(indexCurAlarm).equals(cacheLocList.get(indexDbAlarm)))
                {
                    isMatched = false;
                    break;
                }
                
                isMatched = true;
                indexDbAlarm++;
                indexCurAlarm++;
            }
            
            if (isMatched)
            {
                return alarmModel;
            }
        }
        
        return null;
    }
    
    private int getMatchParamsNum(List<AlarmLocationInfo> alarmLocInfos)
    {
        int matchParamsNum = 0;
        for (AlarmLocationInfo alarmLocationInfo : alarmLocInfos)
        {
            if (alarmLocationInfo.getiMatchParam() == PARAM_MATCH_FLAG)
            {
                matchParamsNum++;
            }
        }
        return matchParamsNum;
    }
    
    private boolean handleAlarmFromCache(AlarmModel curActAlarm, AlarmModel matchCacheAlarm)
    {
        //是否支持更新告警处理的开关：1、true支持更新告警，当被分发的告警与匹配到的告警的关键参数+level+addition相同时，为重复告警，否则当level或addition变化时为更新告警
        //2、false不支持更新告警，当被分发的告警与匹配到的告警的关键参数+level相同时，为重复告警，否则为新告警；符合核心网老产品的告警规范。
        if (!instance.isEnableUpdateAlarm())
        {
            // 不支持更新告警处理，在临时队列能找到匹配的活动告警，说明该告警肯定没有被恢复，则判断被分发的告警是否是重复告警(ID、Location、Moc、ResourceID、Level)
            if (curActAlarm.equals(matchCacheAlarm))
            {
                // 被分发的是重复告警
                LOGGER.warn("[fms] active alarm is duplication. discard alarm:{}", curActAlarm);
                return false;
            }
            
            // 匹配的活动告警不是重复告警，则被分发的是一条新的活动告警，加入addActiveAlarmList、updateAlarmList
            return addNewActiveAlarm(curActAlarm);
        }
        
        // 支持更新告警处理，在临时队列能找到匹配的活动告警，说明该告警肯定没有被恢复，则判断被分发的告警是否是重复告警(ID、Location、Moc、ResourceID、Level、Addition)
        if (curActAlarm.equals(matchCacheAlarm)
                && curActAlarm.getSvAdditionalInfo().trim().equals(matchCacheAlarm.getSvAdditionalInfo().trim()))
        {
            // 被分发的是重复告警
            LOGGER.info("[fms] active alarm is duplication. discard alarm:{}", curActAlarm);
            return false;
        }
        
        // 被分发的是更新告警，则更新匹配到的活动告警，更新level、addition、同步号、告警类型和发生时间
        matchCacheAlarm.updateAlarmFromCache(curActAlarm);
        
        //创建加入updateAlarmList的对象，避免对象引用导致改变了原有的数据
        AlarmModel alarmModel = matchCacheAlarm.cloneAlarmModel();
        if (null == alarmModel)
        {
            LOGGER.warn("[fms] active alarm clone failed. discard alarm:{}", curActAlarm);
            return false;
        }
        
        //加入更新队列updateAlarmList
        updateAlarmList.add(alarmModel);
        LOGGER.debug("[fms] treatActiveAlarm addActiveAlarmList : {}, updateAlarmList : {}",
                new Object[] {addActiveAlarmList, updateAlarmList});
        return true;
    }
    
    private boolean addNewActiveAlarm(AlarmModel curActAlarm)
    {
        curActAlarm.allocNo();
        addActiveAlarmList.add(curActAlarm);
        
        //创建加入updateAlarmList的对象，避免对象引用导致改变了原有的数据
        AlarmModel alarmModel = curActAlarm.cloneAlarmModel();
        if (null == alarmModel)
        {
            LOGGER.warn("[fms] active alarm clone failed. discard alarm:{}", curActAlarm);
            return false;
        }
        
        updateAlarmList.add(alarmModel);
        LOGGER.info("[fms] 1_3 treatActiveAlarm addActiveAlarmList : {}, updateAlarmList : {}",
                new Object[] {addActiveAlarmList, updateAlarmList});
        return true;
    }
    
    private boolean handleAlarmFromDB(AlarmModel curActAlarm, AlarmModel matchDbAlarm)
    {
        //是否支持更新告警处理的开关：1、true支持更新告警，当被分发的告警与匹配到的告警的关键参数+level+addition相同时，为重复告警，否则当level或addition变化时为更新告警
        //2、false不支持更新告警，当被分发的告警与匹配到的告警的关键参数+level相同时，为重复告警，否则为新告警；符合核心网老产品的告警规范。
        if (!instance.isEnableUpdateAlarm())
        {
            // 不支持更新告警处理，在数据库中匹配到的活动告警没有被恢复，则判断被分发的告警是否是重复告警(ID、Location、Moc、ResourceID、Level)，不考虑Addition
            if (curActAlarm.equals(matchDbAlarm))
            {
                // 被分发的是重复告警
                LOGGER.warn("[fms] 1_5 active alarm is duplication. discard alarm:{}", curActAlarm);
                return false;
            }
            
            // 匹配的活动告警不是重复告警，则被分发的是一条新的活动告警，加入addActiveAlarmList、updateAlarmList
            return addNewActiveAlarm(curActAlarm);
        }
        
        // 支持更新告警处理，在数据库中匹配到的活动告警没有被恢复，判断被分发的告警是否是重复告警(ID、Location、Moc、ResourceID、Level、Addition)
        if (curActAlarm.equals(matchDbAlarm)
                && curActAlarm.getSvAdditionalInfo().trim().equals(matchDbAlarm.getSvAdditionalInfo().trim()))
        {
            // 被分发的是重复告警
            LOGGER.info("[fms] active alarm is duplication. discard alarm:{}", curActAlarm);
            return false;
        }
        
        // 被分发的是更新告警，设置和活动告警相同的流水号、分配同步号，告警类型为4，插入addActiveAlarmList
        curActAlarm.updateAlarmFromDB(matchDbAlarm);
        addActiveAlarmList.add(curActAlarm);
        
        // 将数据库中匹配到的活动告警告警的同步号加入同步号列表
        syncNoList.add(matchDbAlarm.getISyncNo());
        
        //创建加入updateAlarmList的对象，避免对象引用导致改变了原有的数据
        AlarmModel alarmModel = curActAlarm.cloneAlarmModel();
        if (null == alarmModel)
        {
            LOGGER.warn("[fms] active alarm clone failed. discard alarm:{}", curActAlarm);
            return false;
        }
        
        //加入更新队列updateAlarmList
        updateAlarmList.add(alarmModel);
        LOGGER.debug("[fms] treatActiveAlarm addActiveAlarmList : {}, updateAlarmList : {}",
                new Object[] {addActiveAlarmList, updateAlarmList});
        
        return true;
        
    }
    
    /**
     * 判断该告警在内存中是否被恢复
     * 此方法只会匹配告警id
     * 
     * @param matchDbAlarm     从数据库中匹配到的活动告警
     * @return  被恢复返回true，否则返回false
     */
    private boolean isClearedAlarm(AlarmModel matchDbAlarm)
    {
        LOGGER.debug("[fms] matchClearedAlarm enter.");
        
        AlarmModel matchedAlarm = matchCacheAlarmParams(matchDbAlarm, addActiveAlarmList);
        
        // 查找匹配到的告警是否在内存中已被恢复 clearType=0
        if (matchedAlarm != null && matchedAlarm.isClearedAlarm(matchDbAlarm))
        {
            LOGGER.info("[fms] 1_4 isClearedAlarm {}.", matchDbAlarm);
            return true;
        }
        
        LOGGER.info("[fms] 1_4 matchClearedAlarm leave {}.", matchDbAlarm);
        return false;
    }
    
    /**
     * 分发清除告警
     * 
     * @param clearAlarm 被分发的清除告警
     * @return boolean 成功返回true,失败返回false 
     */
    private boolean treatClearAlarm(AlarmModel clearAlarm)
    {
        LOGGER.debug("[fms] treatClearAlarm enter.");
        // 克隆被分发的恢复告警，防止对象引用导致回ack的sequenceNo被改变
        AlarmModel curActAlarm = clearAlarm.cloneAlarmModel();
        if (null == curActAlarm)
        {
            LOGGER.warn("[fms] clear alarm clone failed. discard alarm:{}", clearAlarm);
            return false;
        }
        
        // 在临时队列里查找恢复告警匹配的活动告警
        // 这里的内存匹配恢复告警需要做参数匹配
        AlarmModel matchCacheAlarm = matchActiveAlarmForClear(curActAlarm);
        LOGGER.info("[fms] 2_1 {} matchActiveAlarmForClear is: {}", new Object[] {curActAlarm, matchCacheAlarm});
        
        // 在数据库中查找匹配的活动告警
        if (null == matchCacheAlarm)
        {
            // 通过id查询告警，并做参数匹配
            AlarmModel matchDbAlarm = matchDbAlarmParamsById(curActAlarm);
            if (null == matchDbAlarm)
            {
                // 在数据库中没找着匹配的活动告警，将分发的清除告警丢弃
                LOGGER.warn("[fms] 2_2 The clearAlarm did not find a match activeAlarm. discard alarm:{}", curActAlarm);
                return false;
            }
            
            // 在数据库中匹配到活动告警，则判断该是否在内存中被恢复
            if (isClearedAlarm(matchDbAlarm))
            {
                // 匹配的活动告警在内存中已经被清除，说明被分发的清除告警是重复的清除告警，应丢弃
                LOGGER.warn("[fms] 2_3 The matching activeAlarm had been cleared: {}. ", matchDbAlarm);
                return false;
            }
            
            LOGGER.info("[fms] 2_4 add clear alarm to cache: {}", matchDbAlarm);
            // 匹配的活动告警在内存中没有被恢复，将此活动告警的同步号加入同步号列表
            syncNoList.add(matchDbAlarm.getISyncNo());
            
            // 将匹配到的活动告警进行恢复，设置清除类型clearType=0，加入addActiveAlarmList
            matchDbAlarm.setDtClearTime(curActAlarm.getDtOccurTime());
            matchDbAlarm.setIClearType(ClearType.NORMAL.getValue());
            addActiveAlarmList.add(matchDbAlarm);
            
            // 将清除告警加入"临时清除告警队列"
            curActAlarm.updateClearAlarm(matchDbAlarm);
            clearAlarmList.add(curActAlarm);
        }
        else
        {
            LOGGER.info("[fms] 2_2 clear the cache alarm: {}", matchCacheAlarm);
            // 将匹配到的活动告警进行恢复，设置发生时间，清除类型clearType=0
            matchCacheAlarm.setDtClearTime(curActAlarm.getDtOccurTime());
            matchCacheAlarm.setIClearType(ClearType.NORMAL.getValue());
            
            // 将清除告警加入"临时清除告警队列"
            curActAlarm.updateClearAlarm(matchCacheAlarm);
            clearAlarmList.add(curActAlarm);
        }
        
        LOGGER.debug("[fms] treatClearAlarm leave.");
        return true;
    }
    
    /**
     * 获取匹配的活动告警，现在都是自研设备
     * 
     * @param alarm 待匹配的告警对象
     * @return 找到返回对应的活动告警，否则返回null
     */
    private AlarmModel matchActiveAlarmForClear(AlarmModel alarm)
    {
        LOGGER.debug("[fms] matchActiveAlarmForClear enter.");
        AlarmModel alarmModel = matchCacheAlarmParams(alarm, addActiveAlarmList);
        
        if (alarmModel == null)
        {
            LOGGER.info("[fms] matchActiveAlarmForClear is not exist in cache, alarmSequence:{}.",
                    alarm.getSvSequenceNo());
            LOGGER.debug("[fms] matchSelfActiveAlarm leave.");
            return null;
        }
        
        // 如果不是活动告警则返回空
        if (alarmModel.getIClearType() != ClearType.DEFAULT.getValue())
        {
            return null;
        }
        
        return alarmModel;
    }
    
    /**
     * 获取匹配的活动告警，现在都是自研设备
     * 
     * @param alarm 待匹配的告警对象
     * @return 找到返回对应的活动告警，否则返回null
     */
    private AlarmModel matchActiveAlarm(AlarmModel alarm)
    {
        LOGGER.debug("[fms] matchActiveAlarm enter.");
        
        AlarmModel matchAlarm = matchSelfActiveAlarm(alarm);
        
        LOGGER.debug("[fms] matchActiveAlarm leave.");
        return matchAlarm;
    }
    
    /**
     * 查找自研模块的告警信息
     * 
     * 查看被查询告警是否已经被入库了
     * 
     * @param alarm 查询对象
     * @return AlarmModel 如果找到则返回已经被保存的告警信息
     */
    private AlarmModel matchSelfActiveAlarm(AlarmModel alarm)
    {
        LOGGER.debug("[fms] matchSelfActiveAlarm enter.");
        
        // 自研设备告警按ID、Location、Moc、ResourceID和clearType=-1,查找对应的活动告警
        for (AlarmModel alarmModel : addActiveAlarmList)
        {
            if (alarmModel.matchActiveAlarm(alarm))
            {
                return alarmModel;
            }
        }
        
        LOGGER.info("[fms] matchSelfActiveAlarm is not exist in cache, alarmSequence:{}.", alarm.getSvSequenceNo());
        LOGGER.debug("[fms] matchSelfActiveAlarm leave.");
        return null;
    }
    
    /**
     * 查找告警，返回ID和定位信息匹配的告警
     * 
     * @param alarm 查询对象
     * @return AlarmModel 如果找到则返回已经被保存的告警信息
     */
    private AlarmModel matchAlarm(AlarmModel alarm)
    {
        LOGGER.debug("[fms] matchSelfActiveAlarm enter.");
        
        // 自研设备告警按ID、Location、Moc、ResourceID和clearType=-1,查找对应的活动告警
        for (AlarmModel alarmModel : addActiveAlarmList)
        {
            if (alarmModel.matchAlarm(alarm))
            {
                return alarmModel;
            }
        }
        
        LOGGER.info("[fms] matchAlarm is not exist in cache, alarmSequence:{}.", alarm.getSvSequenceNo());
        LOGGER.debug("[fms] matchAlarm leave.");
        return null;
    }
    
    /**
     * 初始化临时队列状态
     * 
     */
    private void clear()
    {
        addActiveAlarmList.clear();
        updateAlarmList.clear();
        clearAlarmList.clear();
        syncNoList.clear();
    }
    
    /**
     * 清除活动表溢出的告警
     * 清除历史库溢出的告警
     */
    private void clearOverflowAlarm()
    {
        LOGGER.debug("[fms] clearOverflowAlarm enter.");
        List<Object> params = new ArrayList<Object>();
        
        int overAtvCount = querryDB.queryAlarmDBCount(SqlConstant.QUERY_COUNT_ALARMACTIVE, params)
                - AlarmConstant.MAX_ACTIVE_TBL_SIZE;
        if (overAtvCount > 0)
        {
            // 删除活动告警表已经清除的告警
            alarmDAO.deleteClearedAlarm(overAtvCount);
        }
        
        LOGGER.debug("[fms] clearOverflowAlarm leave.");
    }
    
    /**
     * 将新增的原始告警、更新告警、清除告警加入到上报队列
     */
    private boolean addToReportAlarm()
    {
        LOGGER.debug("[fms] reportAlarm enter.");
        
        addQueueToReportAlarm(updateAlarmList);
        addQueueToReportAlarm(clearAlarmList);
        
        LOGGER.debug("[fms] reportAlarm leave.");
        return true;
    }
    
    /**
     * 将已经被入库的告警放入上报队列
     * 
     * 上报队列中的数据将在上报线程中取得并被上报给云管理
     * @param list 已经被入库的告警列表
     * @return true:队列放入成功 false:队列放置失败
     */
    private boolean addQueueToReportAlarm(List<AlarmModel> list)
    {
        LOGGER.debug("[fms] addQueueToReportAlarm enter.");
        boolean ret = true;
        if ((null == list) || (list.isEmpty()))
        {
            LOGGER.debug("[fms] list is null or empty");
            return ret;
        }
        
        //获取上报队列
        SerializeQueue<AlarmModel> reportQueue = instance.getReportAlarmQueue();
        if (null == reportQueue)
        {
            //上报队列为空，返回内部处理错误
            LOGGER.error("[fms] report thread is not start. reportQueue is null");
            ret = false;
            return ret;
        }
        
        // 遍历要上报的告警队列，对于需要过滤的告警不上报
        Iterator<AlarmModel> iterator = list.iterator();
        while (iterator.hasNext())
        {
            AlarmModel alarmModel = iterator.next();
            // 参数校验，不合法则直接丢弃，并将此告警ACK信息返回给FMA侧
            if (AlarmConstant.ALARM_NDISPLAY_WEBUI == alarmModel.getIDisplay())
            {
                continue;
            }
            
            if (!reportQueue.offer(alarmModel))
            {
                LOGGER.error("[fms] reportQueue is full");
                ret = false;
                break;
            }
        }
        
        LOGGER.debug("[fms] addQueueToReportAlarm leave.");
        return ret;
    }
}
