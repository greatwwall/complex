public final class FmsMain
{
    /**
     * 日志打印对象
     */
    private static final AppLogger LOGGER = AppLogger.getInstance(FmsMain.class);
    
    private FmsMain()
    {
    }

    /**
     * 启动启动 RPC 服务 ，thrift服务
     */
    private boolean startService(String[] args)
    {
        //初始化数据库连接
        if (!initDBConnection())
        {
            //初始化数据库连接失败，进程退出
            return false;
        }
        
        //业务处理线程启动
        if (!FmsModule.getInstance().init())
        {
            //业务处理线程启动失败，进程退出
            return false;
        }
        
        //启动框架rpc服务
        FmsRpcServer.getInstance().startService(args);
        LOGGER.info("[fms] Fms frm rpc is started...");
        
        /**
         * Fority:
         * 问题:FORTIFY.Poor_Error_Handling--Overly_Broad_Catch，
         * 描述:使用一个“简约”的 catch 块捕获高级别的异常类（如 Exception），
         *     可能会混淆那些需要特殊处理的异常，或是捕获了不应在程序中这一点捕获的异常。
         * 不整改的原因:代码不会出现需要特殊处理的异常，为防止代码中有不可预知的异常，因此捕获一个大异常即可。
         */
        //启动Thrift服务
        try
        {
            FmsThriftServer fmsThriftServer = null;
            fmsThriftServer = FmsThriftServer.getInstance();
            if (null == fmsThriftServer)
            {
                LOGGER.error("[fms] Fms thrift start failed.");
                return false;
            }
            fmsThriftServer.startService();
            
        }
        catch (Exception e)
        {
            LOGGER.error("[fms] Fms thrift start failed.", e);
            return false;
        }
        
        return true;
    }
    
    /**
     * 程序退出钩子类
     * 
     * Fortify:
     * 问题:Bad_Practices--Threads，J2EE标准禁止在某些环境下使用 Web应用程序中的线程管理，因为此时使用线程管理非常容易出错。
     *    线程管理起来很困难，并且可能还会以不可预知的方式干扰应用程序容器。即使容器没有受到干扰，线程管理通常还会导致各种难以发现的错误，如死锁、race condition 及其他同步错误等。
     * 不整改的原因:这是基本的JAVA应用，不是Web应用，使用线程不会出现问题描述的场景。
     */
    private static class ShutdownHook extends Thread
    {   
        /**
         * 外界调用stop指令或执行到代码里的System.exit(1)会调用钩子线程的run方法
         */
        @Override
        public void run()
        {
            LOGGER.debug("[fms] fms ShutDownHook.run enter.");
            try
            {
                // 关闭thrift
                FmsThriftServer.getInstance().stop();
                
                // 关闭定时器和线程
                FmsModule.getInstance().stop();
                
                // 关闭进程前缓存数据入库处理
                DiffSourceAlarmManager.getInstance().putCacheToDB();
            }
            catch (Exception e)
            {
                LOGGER.error("[fms] putCacheAlarmToDB exception: ", e);
            }
            
            LOGGER.debug("[fms] fms ShutDownHook.run exit.");
        }
    }
    
    private boolean initDBConnection()
    {
        try
        {
          //初始化数据源
            ConnectionFactory.initializeAllDataSources();
            
            //初始化dao对象
            DaoFactory.initialize();
        }
        catch (AppRuntimeException e)
        {
            //初始化数据库连接失败
            LOGGER.error("init DataBase Connection Exception.", e);
            return false;
        }
        return true;
    }
    
    /**
     * 
     * FMS模块入口方法
     * @param args 函数入口参数
     * 
     * Fortify:
     * 问题:Bad_Practices--Threads，J2EE标准禁止在某些环境下使用 Web应用程序中的线程管理，因为此时使用线程管理非常容易出错。
     *    线程管理起来很困难，并且可能还会以不可预知的方式干扰应用程序容器。即使容器没有受到干扰，线程管理通常还会导致各种难以发现的错误，如死锁、race condition 及其他同步错误等。
     * 不整改的原因:这是基本的JAVA应用，不是Web应用，使用线程不会出现问题描述的场景。
     * 
     * Fority:
     * 问题:J2EE Bad Practices: Leftover Debug Code
     * 描述:代码中遗留有调试代码 
     * 不整改的原因:正常功能的程序入口main函数，不是遗留的调试代码
     */
    public static void main(String[] args)
    {
        FmsMain fmsMain = new FmsMain();
        
        //启动相关服务 
        /**
         * Fority:
         * 问题:FORTIFY.J2EE_Bad_Practices--System.exit
         * 描述:让 Web 应用程序试图关闭自身的容器并不是什么好主意。调用 System.exit() 的操作可能包含在
         *      leftover debug code 中，或者包含在从非 J2EE 应用程序导入的代码中。
         * 不整改的原因:进程启动时为保证功能完整，如果RPC、Thrift服务启动失败或异常，进程直接退出
         */
        try
        {
            if (!fmsMain.startService(args))
            {
                LOGGER.error("[fms] FmsMain start Failed, It will be killed!");
                System.exit(1);
            }
        }
        catch (Exception e)
        {
            LOGGER.error("[fms] FmsMain start has exception, It will be killed.", e);
            System.exit(1);
        }
        
        LOGGER.info("[fms] Fms RPC & thrift is started...");
        
        //注册钩子线程
        /**
         * Fority:
         * 问题:J2EE Bad Practices: Threads
         * 描述:J2EE 标准禁止在某些环境下使用 Web 应用程序中的线程管理，因为此时使用线程管理非常容易出错。
         * 线程管理起来很困难，并且可能还会以不可预知的方式干扰应用程序容器。即使容器没有受到干扰，
         * 线程管理通常还会导致各种难以发现的错误，如死锁、race condition 及其他同步错误等。
         * 不整改的原因:这是基本的JAVA应用，不是Web应用，使用线程不会出现问题描述的场景
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        LOGGER.info("[fms] Fms ShutdownHook is successful...");
    }

}
