//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;
using System.Runtime.InteropServices;
using PRISM;

// Required for call to GetDiskFreeSpaceEx

namespace CaptureTaskManager
{
    public class clsMainProgram
    {
        //*********************************************************************************************************
        // Main program execution loop for application
        //**********************************************************************************************************

        #region "Enums"

        private enum LoopExitCode
        {
            NoTaskFound,
            ConfigChanged,
            ExceededMaxTaskCount,
            DisabledMC,
            DisabledLocally,
            ExcessiveRequestErrors,
            InvalidWorkDir,
            ShutdownCmdReceived,
            UpdateRequired,
            FlagFile,
            NeedToAbortProcessing
        }

        #endregion

        #region "Constants"

        private const int MAX_ERROR_COUNT = 4;

        private const string CUSTOM_LOG_SOURCE_NAME = "Capture Task Manager";
        public const string CUSTOM_LOG_NAME = "DMSCapTaskMgr";

        #endregion

        #region "Class variables"

        private clsMgrSettings m_MgrSettings;
        private clsCaptureTask m_Task;
        private FileSystemWatcher m_FileWatcher;
        private IToolRunner m_CapTool;
        private bool m_ConfigChanged;
        private int m_TaskRequestErrorCount;
        private IStatusFile m_StatusFile;

        private clsMessageHandler m_MsgHandler;
        private bool m_MsgQueueInitSuccess;

        private LoopExitCode m_LoopExitCode;

        private string m_MgrName = "Unknown";
        private string m_StepTool = "Unknown";
        private string m_Job = "Unknown";
        private string m_Dataset = "Unknown";

        /// <summary>
        /// DebugLevel of 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
        /// </summary>
        private int m_DebugLevel = 4;

        private bool m_Running;
        private System.Timers.Timer m_StatusTimer;
        private DateTime m_DurationStart;
        private bool m_ManagerDeactivatedLocally;

        private readonly bool m_TraceMode;

        #endregion

        #region "Delegates"

        #endregion

        #region "Events"

        #endregion

        #region "Properties"

        public bool ManagerDeactivatedLocally
        {
            get { return m_ManagerDeactivatedLocally; }
        }

        #endregion

        #region "Constructors"

        /// <summary>
        /// Constructor
        /// </summary>
        public clsMainProgram(bool traceMode)
        {
            m_TraceMode = traceMode;
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Evaluates the LoopExitCode to determine whether or not manager can request another task
        /// </summary>
        /// <param name="eLoopExitCode"></param>
        /// <returns>True if OK to request another task</returns>
        private bool EvaluateLoopExitCode(LoopExitCode eLoopExitCode)
        {
            string msg;
            var restartOK = true;

            // Determine cause of loop exit and respond accordingly
            switch (eLoopExitCode)
            {
                case LoopExitCode.ConfigChanged:
                    // Reload the manager config
                    msg = "Reloading configuration and restarting manager";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    // Unsubscribe message handler events and close msssage handler
                    if (m_MsgQueueInitSuccess)
                    {
                        m_MsgHandler.BroadcastReceived -= OnBroadcastReceived;
                        m_MsgHandler.CommandReceived -= OnCommandReceived;
                        m_MsgHandler.Dispose();
                    }
                    restartOK = true;
                    break;

                case LoopExitCode.DisabledMC:
                    // Manager is disabled via manager control db
                    msg = "Manager disabled in manager control DB";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.UpdateDisabled(false);
                    restartOK = false;
                    break;

                case LoopExitCode.DisabledLocally:
                    // Manager disabled locally
                    msg = "Manager disabled locally";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.UpdateDisabled(true);
                    restartOK = false;
                    break;

                case LoopExitCode.ExcessiveRequestErrors:
                    // Too many errors
                    msg = "Excessive errors requesting task; closing manager";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    // Do not create a flag file; intermittent network connectivity is likely resulting in failure to request a task
                    // This will likely clear up eventually

                    m_StatusFile.UpdateStopped(true);

                    restartOK = false;
                    break;

                case LoopExitCode.InvalidWorkDir:
                    // Working directory not valid
                    msg = "Working directory problem, disabling manager";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.CreateStatusFlagFile();
                    m_StatusFile.UpdateStopped(true);

                    restartOK = false;
                    break;

                case LoopExitCode.NoTaskFound:
                    // No capture task found
                    msg = "No capture tasks found";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.ShutdownCmdReceived:
                    // Shutdown command received
                    msg = "Shutdown command received, closing manager";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.ExceededMaxTaskCount:
                    // Max number of consecutive jobs reached
                    msg = "Exceeded maximum job count, closing manager";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.UpdateRequired:
                    // Manager update required
                    msg = "Manager update is required, closing manager";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_MgrSettings.AckManagerUpdateRequired();
                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.FlagFile:
                    // Flag file is present
                    msg = "Flag file exists - unable to continue analysis";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.UpdateStopped(true);
                    restartOK = false;
                    break;

                case LoopExitCode.NeedToAbortProcessing:
                    // Step tool set flag NeedToAbortProcessing to true
                    msg = "NeedToAbortProcessing = true, closing manager";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                default:
                    // Should never get here
                    break;
            } // End switch

            return restartOK;
        }

        /// <summary>
        /// Initializes the manager
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool InitMgr()
        {
            // Create a database logger connected to DMS5
            // Once the initial parameters have been successfully read, 
            // we remove this logger than make a new one using the connection string read from the Manager Control DB
            var defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;

            clsLogTools.CreateDbLogger(defaultDmsConnectionString, "CaptureTaskMan: " + System.Net.Dns.GetHostName(),
                                       true);

            // Get the manager settings
            // If you get an exception here while debugging in Visual Studio, be sure 
            //  that "UsingDefaults" is set to False in CaptureTaskManager.exe.config               
            try
            {
                m_MgrSettings = new clsMgrSettings();
            }
            catch (Exception ex)
            {
                if (string.Equals(ex.Message, clsMgrSettings.DEACTIVATED_LOCALLY))
                {
                    m_ManagerDeactivatedLocally = true;
                }
                else
                {
                    // Failures are logged by clsMgrSettings to application event logs;
                    //  this includes MgrActive_Local = False
                    // 
                    // If the DMSCapTaskMgr application log does not exist yet, the Log4Net SysLogger will create it (see file Logging.config)
                    // However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
                    // Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

                    Console.WriteLine();
                    Console.WriteLine(@"===============================================================");
                    Console.WriteLine(@"Exception instantiating clsMgrSettings: " + ex.Message);
                    Console.WriteLine(@"===============================================================");
                    Console.WriteLine();
                    Console.WriteLine(
                        @"You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " +
                        CUSTOM_LOG_NAME + @" application log");
                    Console.WriteLine();
                    System.Threading.Thread.Sleep(500);
                }

                return false;
            }

            // Update the cached values for this manager and job
            m_MgrName = m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_NAME);
            if (m_TraceMode)
                ShowTraceMessage("Manager name is " + m_MgrName);

            m_StepTool = "Unknown";
            m_Job = "Unknown";
            m_Dataset = "Unknown";

            // Confirm that the application event log exists
            if (!EventLog.SourceExists(CUSTOM_LOG_SOURCE_NAME))
            {
                var sourceData = new EventSourceCreationData(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME);
                EventLog.CreateEventSource(sourceData);
            }

            // Setup the loggers
            var logFileName = m_MgrSettings.GetParam("logfilename");
            m_DebugLevel = m_MgrSettings.GetParam("debuglevel", 4);
            clsLogTools.CreateFileLogger(logFileName, m_DebugLevel);

            if (m_MgrSettings.GetBooleanParam("ftplogging"))
                clsLogTools.CreateFtpLogFileLogger("Dummy.txt");

            var logCnStr = m_MgrSettings.GetParam("connectionstring");

            clsLogTools.RemoveDefaultDbLogger();
            clsLogTools.CreateDbLogger(logCnStr, "CaptureTaskMan: " + m_MgrName, false);

            // Make initial log entry
            if (m_TraceMode)
                ShowTraceMessage("Initializing log file " + logFileName);

            var msg = "=== Started Capture Task Manager V" + Application.ProductVersion + " ===== ";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

            // Setup the message queue
            m_MsgQueueInitSuccess = false;
            m_MsgHandler = new clsMessageHandler();
            m_MsgHandler.BrokerUri = m_MsgHandler.BrokerUri = m_MgrSettings.GetParam("MessageQueueURI");
            m_MsgHandler.CommandQueueName = m_MgrSettings.GetParam("ControlQueueName");
            m_MsgHandler.BroadcastTopicName = m_MgrSettings.GetParam("BroadcastQueueTopic");
            m_MsgHandler.StatusTopicName = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");
            m_MsgHandler.MgrSettings = m_MgrSettings;

            // Initialize the message queue
            // Start this in a separate thread so that we can abort the initialization if necessary
            InitializeMessageQueue();

            if (m_MsgQueueInitSuccess)
            {
                //Connect message handler events
                m_MsgHandler.CommandReceived += OnCommandReceived;
                m_MsgHandler.BroadcastReceived += OnBroadcastReceived;
            }

            var configFileName = m_MgrSettings.GetParam("configfilename");
            if (string.IsNullOrEmpty(configFileName))
            {
                // Manager parameter error; log an error and exit
                var errMsg =
                    "Manager parameter 'configfilename' is undefined; this likely indicates a problem retrieving manager parameters.  Shutting down the manager";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errMsg);

                if (m_TraceMode)
                    ShowTraceMessage(errMsg);
                return false;
            }

            // Setup a file watcher for the config file
            var fInfo = new FileInfo(Application.ExecutablePath);
            m_FileWatcher = new FileSystemWatcher
            {
                Path = fInfo.DirectoryName,
                IncludeSubdirectories = false,
                Filter = configFileName,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };

            // Subscribe to the file watcher Changed event
            m_FileWatcher.Changed += FileWatcherChanged;

            // Set up the tool for getting tasks
            m_Task = new clsCaptureTask(m_MgrSettings);

            // Set up the status file class
            if (fInfo.DirectoryName == null)
            {
                var errMsg = "Error determining the parent path for the executable, " + Application.ExecutablePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errMsg);

                if (m_TraceMode)
                    ShowTraceMessage(errMsg);
                return false;
            }

            var statusFileNameLoc = Path.Combine(fInfo.DirectoryName, "Status.xml");
            m_StatusFile = new clsStatusFile(statusFileNameLoc)
            {
                LogToMsgQueue = m_MgrSettings.GetBooleanParam("LogStatusToMessageQueue"),
                MgrName = m_MgrName,
                MgrStatus = EnumMgrStatus.Running
            };
            m_StatusFile.MonitorUpdateRequired += OnStatusMonitorUpdateReceived;
            m_StatusFile.WriteStatusFile();

            // Set up the status reporting time, with an interval of 1 minute
            m_StatusTimer = new System.Timers.Timer
            {
                Enabled = false,
                Interval = 60 * 1000
            };
            m_StatusTimer.Elapsed += m_StatusTimer_Elapsed;

            // Get the most recent job history
            var historyFile = Path.Combine(m_MgrSettings.GetParam("ApplicationPath"), "History.txt");
            if (File.Exists(historyFile))
            {
                try
                {
                    // Create an instance of StreamReader to read from a file.
                    // The using statement also closes the StreamReader.
                    using (var sr = new StreamReader(historyFile))
                    {
                        String line;
                        // Read and display lines from the file until the end of 
                        // the file is reached.
                        while ((line = sr.ReadLine()) != null)
                        {
                            if (line.Contains("RecentJob: "))
                            {
                                var tmpStr = line.Replace("RecentJob: ", "");
                                m_StatusFile.MostRecentJobInfo = tmpStr;
                                break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    var errMsg = "Exception readining status history file";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errMsg, ex);

                    if (m_TraceMode)
                        ShowTraceMessage(errMsg + ": " + ex.Message);
                }
            }

            // Everything worked!
            return true;
        }

        private bool InitializeMessageQueue()
        {
            const int MAX_WAIT_TIME_SECONDS = 60;

            var worker = new System.Threading.Thread(InitializeMessageQueueWork);
            worker.Start();

            var dtWaitStart = DateTime.UtcNow;

            // Wait a maximum of 60 seconds
            if (!worker.Join(MAX_WAIT_TIME_SECONDS * 1000))
            {
                worker.Abort();
                m_MsgQueueInitSuccess = false;
                var warnMsg = "Unable to initialize the message queue (timeout after " + MAX_WAIT_TIME_SECONDS +
                              " seconds)";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warnMsg);

                if (m_TraceMode)
                    ShowTraceMessage(warnMsg);
                return m_MsgQueueInitSuccess;
            }

            var elaspedTime = DateTime.UtcNow.Subtract(dtWaitStart).TotalSeconds;

            if (elaspedTime > 25)
            {
                var warnMsg = "Connection to the message queue was slow, taking " + (int)elaspedTime + " seconds";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, warnMsg);
                if (m_TraceMode)
                    ShowTraceMessage(warnMsg);
            }

            return m_MsgQueueInitSuccess;
        }

        private void InitializeMessageQueueWork()
        {
            if (!m_MsgHandler.Init())
            {
                // Most error messages provided by .Init method, but debug message is here for program tracking
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     "Message handler init error");
                m_MsgQueueInitSuccess = false;
                if (m_TraceMode)
                    ShowTraceMessage("m_MsgQueueInitSuccess = false: Message handler init error");
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     "Message handler initialized");
                m_MsgQueueInitSuccess = true;
                if (m_TraceMode)
                    ShowTraceMessage("m_MsgQueueInitSuccess = true");
            }
        }

        /// <summary>
        /// Main loop for task performance
        /// </summary>
        /// <returns>TRUE if loop exits and manager restart is OK, FALSE otherwise</returns>
        public bool PerformMainLoop()
        {
            var taskCount = 1;

            var dtLastConfigDBUpdate = DateTime.UtcNow;

            m_Running = true;

            // Begin main execution loop
            while (m_Running)
            {
                try
                {
                    //Verify that an error hasn't left the the system in an odd state
                    if (StatusFlagFileError())
                    {
                        m_LoopExitCode = LoopExitCode.FlagFile;
                        break;
                    }

                    // Check for configuration change
                    // This variable will be true if the CaptureTaskManager.exe.config file has been updated
                    if (m_ConfigChanged)
                    {
                        // Local config file has changed
                        m_LoopExitCode = LoopExitCode.ConfigChanged;
                        break;
                    }

                    // Reload the manager control DB settings in case they have changed
                    // However, only reload every 2 minutes
                    if (!UpdateMgrSettings(ref dtLastConfigDBUpdate, 2))
                    {
                        // Error updating manager settings
                        m_LoopExitCode = LoopExitCode.UpdateRequired;
                        break;
                    }

                    // Check to see if manager is still active
                    if (!m_MgrSettings.GetBooleanParam("mgractive"))
                    {
                        // Disabled via manager control db
                        m_LoopExitCode = LoopExitCode.DisabledMC;
                        break;
                    }

                    if (!m_MgrSettings.GetBooleanParam(clsMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL))
                    {
                        m_LoopExitCode = LoopExitCode.DisabledLocally;
                        break;
                    }

                    if (m_MgrSettings.GetBooleanParam("ManagerUpdateRequired"))
                    {
                        m_LoopExitCode = LoopExitCode.UpdateRequired;
                        break;
                    }

                    // Check for excessive number of errors
                    if (m_TaskRequestErrorCount > MAX_ERROR_COUNT)
                    {
                        m_LoopExitCode = LoopExitCode.ExcessiveRequestErrors;
                        break;
                    }

                    // Check working directory
                    if (!ValidateWorkingDir())
                    {
                        m_LoopExitCode = LoopExitCode.InvalidWorkDir;
                        break;
                    }

                    // Check whether the computer is likely to install the monthly Windows Updates within the next few hours
                    // Do not request a task between 12 am and 6 am on Thursday in the week with the second Tuesday of the month
                    // Do not request a task between 2 am and 4 am or between 9 am and 11 am on Sunday in the week with the second Tuesday of the month
                    string pendingWindowsUpdateMessage;
                    if (clsWindowsUpdateStatus.UpdatesArePending(out pendingWindowsUpdateMessage))
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                             pendingWindowsUpdateMessage);
                        m_LoopExitCode = LoopExitCode.NoTaskFound;
                        if (m_TraceMode)
                            ShowTraceMessage(pendingWindowsUpdateMessage);
                        break;
                    }


                    // Delete temp files between 1:00 am and 1:30 am, or after every 50 tasks
                    if (taskCount == 1 && DateTime.Now.Hour == 1 && DateTime.Now.Minute < 30 || taskCount % 50 == 0)
                    {
                        RemoveOldTempFiles();
                        RemoveOldFTPLogFiles();
                    }

                    // Attempt to get a capture task
                    var taskReturn = m_Task.RequestTask();
                    switch (taskReturn)
                    {
                        case EnumRequestTaskResult.NoTaskFound:
                            m_Running = false;
                            m_LoopExitCode = LoopExitCode.NoTaskFound;
                            break;

                        case EnumRequestTaskResult.ResultError:
                            // Problem with task request; Errors are logged by request method
                            m_TaskRequestErrorCount++;
                            break;

                        case EnumRequestTaskResult.TaskFound:

                            EnumCloseOutType eTaskCloseout;
                            PerformTask(out eTaskCloseout);

                            // Increment and test the task counter
                            taskCount++;
                            if (taskCount > m_MgrSettings.GetParam("maxrepetitions", 1))
                            {
                                m_Running = false;
                                m_LoopExitCode = LoopExitCode.ExceededMaxTaskCount;
                            }

                            if (eTaskCloseout == EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING)
                            {
                                m_Running = false;
                                m_LoopExitCode = LoopExitCode.NeedToAbortProcessing;
                            }

                            break;

                        default:
                            //Shouldn't ever get here!
                            break;
                    }
                }
                catch (Exception ex)
                {
                    var msg = "Error in PerformMainLoop";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                    if (m_TraceMode)
                        ShowTraceMessage(msg + ": " + ex.Message);
                }
            } // End while

            m_Running = false;

            // Write the recent job history file				
            try
            {
                var historyFile = Path.Combine(m_MgrSettings.GetParam("ApplicationPath"), "History.txt");

                using (var sw = new StreamWriter(historyFile, false))
                {
                    sw.WriteLine("RecentJob: " + m_StatusFile.MostRecentJobInfo);
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception writing job history file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                if (m_TraceMode)
                    ShowTraceMessage(msg + ": " + ex.Message);
            }

            // Evaluate the loop exit code
            var restartOK = EvaluateLoopExitCode(m_LoopExitCode);

            if (!restartOK)
            {
                const string msg = "===== Closing Capture Task Manager =====";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            }

            return restartOK;
        }


        private void PerformTask(out EnumCloseOutType eTaskCloseout)
        {
            string msg;
            eTaskCloseout = EnumCloseOutType.CLOSEOUT_NOT_READY;

            try
            {
                // Cache the job parameters
                m_StepTool = m_Task.GetParam("StepTool");
                m_Job = m_Task.GetParam("Job");
                m_Dataset = m_Task.GetParam("Dataset");
                var stepNumber = m_Task.GetParam("Step");

                msg = "Job " + m_Job + ", step " + stepNumber + " assigned";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                if (m_TraceMode)
                    ShowTraceMessage(msg);

                // Update the status
                m_StatusFile.JobNumber = int.Parse(m_Job);
                m_StatusFile.Dataset = m_Dataset;
                m_StatusFile.MgrStatus = EnumMgrStatus.Running;
                m_StatusFile.Tool = m_StepTool;
                m_StatusFile.TaskStatus = EnumTaskStatus.Running;
                m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.Running_Tool;
                m_StatusFile.MostRecentJobInfo = DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss tt") +
                                                 ", Job " + m_Job + ", Step " + stepNumber +
                                                 ", Tool " + m_StepTool;

                m_StatusFile.WriteStatusFile();

                // Create the tool runner object
                if (!SetToolRunnerObject(m_StepTool))
                {
                    msg = m_MgrName + ": Unable to SetToolRunnerObject, job " + m_Job
                          + ", Dataset " + m_Dataset;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

                    msg = "Unable to SetToolRunnerObject";
                    if (m_TraceMode)
                        ShowTraceMessage(msg);

                    m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, msg);

                    m_StatusFile.UpdateIdle();
                    return;
                }


                // Make sure we have enough free space on the drive with the dataset folder
                if (!ValidateFreeDiskSpace(out msg))
                {
                    if (string.IsNullOrEmpty(msg))
                        msg = "Insufficient free space (location undefined)";

                    m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, msg);
                    m_StatusFile.UpdateIdle();
                    return;
                }

                // Run the tool plugin
                m_DurationStart = DateTime.UtcNow;
                m_StatusTimer.Enabled = true;
                var toolResult = m_CapTool.RunTool();
                m_StatusTimer.Enabled = false;

                eTaskCloseout = toolResult.CloseoutType;
                string sCloseoutMessage;

                switch (eTaskCloseout)
                {
                    case EnumCloseOutType.CLOSEOUT_FAILED:
                        msg = m_MgrName + ": Failure running tool " + m_StepTool
                              + ", job " + m_Job + ", Dataset " + m_Dataset;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                            sCloseoutMessage = toolResult.CloseoutMsg;
                        else
                            sCloseoutMessage = "Failure running tool " + m_StepTool;

                        if (m_TraceMode)
                            ShowTraceMessage(msg);
                        m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NOT_READY:
                        if (m_StepTool == "ArchiveVerify" || m_StepTool == "ArchiveStatusCheck")
                        {
                            msg = "Dataset not ready, tool " + m_StepTool + ", job " + m_Job + ": " + toolResult.CloseoutMsg;
                        }
                        else
                        {
                            msg = "Dataset not ready, tool " + m_StepTool + ", job " + m_Job + ", Dataset " + m_Dataset;
                        }
                        
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);

                        sCloseoutMessage = "Dataset not ready";

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                            sCloseoutMessage += ": " + toolResult.CloseoutMsg;

                        if (m_TraceMode)
                            ShowTraceMessage(msg);
                        m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_SUCCESS:
                        msg = m_MgrName + ": Step complete, tool " + m_StepTool + ", job " + m_Job + ", Dataset " +
                              m_Dataset;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                        if (m_TraceMode)
                            ShowTraceMessage(msg);

                        m_Task.CloseTask(eTaskCloseout, toolResult.CloseoutMsg, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING:
                        msg = m_MgrName + ": Failure running tool " + m_StepTool
                              + ", job " + m_Job + ", Dataset " + m_Dataset
                              + "; CloseOut = NeedToAbortProcessing";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                        sCloseoutMessage = "Error: NeedToAbortProcessing";
                        if (m_TraceMode)
                            ShowTraceMessage(msg);

                        m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    default:
                        // Should never get here
                        break;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running task",
                                     ex);

                msg = m_MgrName + ": Failure running tool " + m_StepTool
                      + ", job " + m_Job + ", Dataset " + m_Dataset
                      + "; CloseOut = Exception";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);

                msg = "Exception: " + ex.Message;
                if (m_TraceMode)
                    ShowTraceMessage(msg);
                m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, msg, EnumEvalCode.EVAL_CODE_FAILED,
                                 "Exception running tool");
            }


            // Update the status
            m_StatusFile.ClearCachedInfo();

            m_StatusFile.MgrStatus = EnumMgrStatus.Running;
            m_StatusFile.TaskStatus = EnumTaskStatus.No_Task;
            m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.No_Task;
            m_StatusFile.WriteStatusFile();
        }

        public void PostTestLogMessage()
        {
            try
            {
                var sMessage = "Test log message: " + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt");
                Console.WriteLine(@"Posting test log message to the " + CUSTOM_LOG_NAME + @" Windows event log");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.INFO, sMessage);
                Console.WriteLine(@" ... Success!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(@"Error writing to event log: " + ex.Message);
            }
        }

        /// <summary>
        /// Look for and remove FTPLog_ files that were created over 64 days ago in the application folder
        /// </summary>
        protected void RemoveOldFTPLogFiles()
        {
            const int iAgedLogFileDays = 64;
            RemoveOldFTPLogFiles(iAgedLogFileDays);
        }

        /// <summary>
        /// Look for and remove FTPLog_ files that were created over iAgedLogFileDays days ago in the application folder
        /// </summary>
        /// <remarks>Also removes zero-byte FTPLog_ files</remarks>
        protected void RemoveOldFTPLogFiles(int iAgedLogFileDays)
        {
            if (iAgedLogFileDays < 7)
                iAgedLogFileDays = 7;

            try
            {
                var fiApplication = new FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location);

                if (fiApplication.Directory == null)
                    return;

                foreach (var fiFile in fiApplication.Directory.GetFiles("FTPlog_*"))
                {
                    try
                    {
                        if (DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalDays > iAgedLogFileDays ||
                            fiFile.Length == 0)
                        {
                            fiFile.Delete();
                        }
                    }
                    // ReSharper disable once EmptyGeneralCatchClause
                    catch
                    {
                        // Ignore exceptions
                    }
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception removing old FTP log files";
                if (m_TraceMode)
                    ShowTraceMessage(msg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     msg + ": " + ex.Message);
            }
        }

        /// <summary>
        /// Look for and remove old .tmp and .zip files
        /// </summary>
        protected void RemoveOldTempFiles()
        {
            // Remove .tmp and .zip files over 12 hours old in the Windows Temp folder
            const int iAgedTempFilesHours = 12;
            var sTempFolderPath = Path.GetTempPath();
            RemoveOldTempFiles(iAgedTempFilesHours, sTempFolderPath);
        }

        protected void RemoveOldTempFiles(int iAgedTempFilesHours, string sTempFolderPath)
        {
            // This list tracks the file specs to search for in folder sTempFolderPath
            var lstSearchSpecs = new List<string>
            {
                "*.tmp",
                "*.zip"
            };

            RemoveOldTempFiles(iAgedTempFilesHours, sTempFolderPath, lstSearchSpecs);
        }

        /// <summary>
        /// Look for and remove files
        /// </summary>
        /// <param name="iAgedTempFilesHours">Files more than this many hours old will be deleted</param>
        /// <param name="sTempFolderPath">Path to the folder to look for and delete old files</param>
        /// <param name="lstSearchSpecs">File specs to search for in folder sTempFolderPath, e.g. "*.txt"</param>
        protected void RemoveOldTempFiles(int iAgedTempFilesHours, string sTempFolderPath, List<string> lstSearchSpecs)
        {
            try
            {
                var iTotalDeleted = 0;

                if (iAgedTempFilesHours < 2)
                    iAgedTempFilesHours = 2;

                var diFolder = new DirectoryInfo(sTempFolderPath);
                string msg;
                if (!diFolder.Exists)
                {
                    msg = "Folder not found: " + sTempFolderPath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);
                    return;
                }

                // Process each entry in lstSearchSpecs
                foreach (var sSpec in lstSearchSpecs)
                {
                    var iDeleteCount = 0;
                    foreach (var fiFile in diFolder.GetFiles(sSpec))
                    {
                        try
                        {
                            if (DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours > iAgedTempFilesHours)
                            {
                                fiFile.Delete();
                                iDeleteCount += 1;
                            }
                        }
                        // ReSharper disable once EmptyGeneralCatchClause
                        catch
                        {
                            // Ignore exceptions
                        }
                    }

                    iTotalDeleted += iDeleteCount;
                }

                if (iTotalDeleted > 0)
                {
                    msg = "Deleted " + iTotalDeleted + " temp file";
                    if (iTotalDeleted > 1)
                        msg += "s";

                    msg += " over " + iAgedTempFilesHours + " hours old in folder " + sTempFolderPath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    if (m_TraceMode)
                        ShowTraceMessage(msg);
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception removing old temp files";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     msg + ": " + ex.Message);
                if (m_TraceMode)
                    ShowTraceMessage(msg);
            }
        }


        /// <summary>
        /// Sets the tool runner object for this job
        /// </summary>
        /// <returns></returns>
        private bool SetToolRunnerObject(string stepToolName)
        {
            string msg;

            // Load the tool runner
            m_CapTool = clsPluginLoader.GetToolRunner(stepToolName);
            if (m_CapTool == null)
            {
                msg = "Unable to load tool runner for StepTool " + stepToolName + ": " + clsPluginLoader.ErrMsg;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                if (m_TraceMode)
                    ShowTraceMessage(msg);
                return false;
            }

            msg = "Loaded tool runner for Step Tool " + stepToolName;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            if (m_TraceMode)
                ShowTraceMessage(msg);

            try
            {
#if MyEMSL_OFFLINE
    // When this Conditional Compilation Constant is defined, then the DatasetArchive plugin will set debugMode 
    // to Pacifica.Core.EasyHttp.eDebugMode.MyEMSLOfflineMode when calling UploadToMyEMSLWithRetry()
    // This in turn results in writeToDisk becoming True in SendFileListToDavAsTar
    m_Task.AddAdditionalParameter("MyEMSLOffline", "true");
    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Adding job parameter MyEMSLOffline=true");
#endif

#if MyEMSL_TEST_TAR
    m_Task.AddAdditionalParameter("DebugTestTar", "true");
    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Adding job parameter DebugTestTar=true");
#endif
                if (m_TraceMode)
                {
                    m_MgrSettings.SetParam("TraceMode", "True");
                }

                // Setup the new tool runner
                m_CapTool.Setup(m_MgrSettings, m_Task, m_StatusFile);
            }
            catch (Exception ex)
            {
                msg = "Exception calling CapTool.Setup(): " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                if (m_TraceMode)
                    ShowTraceMessage(msg);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        public static void ShowTraceMessage(string message)
        {
            clsToolRunnerBase.ShowTraceMessage(message);
        }

        /// <summary>
        /// Looks for flag file; auto cleans if ManagerErrorCleanupMode is >= 1
        /// </summary>
        /// <returns>True if a flag file exists and it was not auto-cleaned; false if no problems</returns>
        private bool StatusFlagFileError()
        {
            if (!m_StatusFile.DetectStatusFlagFile())
            {
                return false;
            }

            bool blnMgrCleanupSuccess;
            try
            {
                var objCleanupMgrErrors = new clsCleanupMgrErrors(
                    m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING),
                    m_MgrName,
                    m_MgrSettings.GetParam("WorkDir"),
                    m_StatusFile);

                var cleanupModeVal = m_MgrSettings.GetParam("ManagerErrorCleanupMode", 0);
                blnMgrCleanupSuccess = objCleanupMgrErrors.AutoCleanupManagerErrors(cleanupModeVal);
            }
            catch (Exception ex)
            {
                var msg = "Error calling AutoCleanupManagerErrors from StatusFlagFileError";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     msg + ": " + ex.Message);
                if (m_TraceMode)
                    ShowTraceMessage(msg);
                blnMgrCleanupSuccess = false;
            }

            if (blnMgrCleanupSuccess)
            {
                var msg = "Flag file found; automatically cleaned the work directory and deleted the flag file(s)";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);

                if (m_TraceMode)
                    ShowTraceMessage(msg);

                // No error; return false
                return false;
            }

            // Error removing flag file; return true
            return true;
        }

        /// <summary>
        /// Reloads the manager settings from the manager control database 
        /// if at least MinutesBetweenUpdates minutes have elapsed since the last update
        /// </summary>
        /// <param name="dtLastConfigDBUpdate"></param>
        /// <param name="MinutesBetweenUpdates"></param>
        /// <returns></returns>
        private bool UpdateMgrSettings(ref DateTime dtLastConfigDBUpdate, double MinutesBetweenUpdates)
        {
            if (!(DateTime.UtcNow.Subtract(dtLastConfigDBUpdate).TotalMinutes >= MinutesBetweenUpdates))
            {
                return true;
            }

            var bSuccess = true;

            dtLastConfigDBUpdate = DateTime.UtcNow;

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                 "Updating manager settings using Manager Control database");

            if (!m_MgrSettings.LoadMgrSettingsFromDB())
            {
                // Error retrieving settings from the manager control DB
                string msg;

                if (string.IsNullOrEmpty(m_MgrSettings.ErrMsg))
                    msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";
                else
                    msg = m_MgrSettings.ErrMsg;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                if (m_TraceMode)
                    ShowTraceMessage(msg);

                bSuccess = false;
            }
            else
            {
                // Update the log level
                m_DebugLevel = m_MgrSettings.GetParam("debuglevel", 4);
                clsLogTools.SetFileLogLevel(m_DebugLevel);
            }

            return bSuccess;
        }

        [DllImport("kernel32", CharSet = CharSet.Auto)]
        static extern int GetDiskFreeSpaceEx(
            string lpDirectoryName,
            out ulong lpFreeBytesAvailable,
            out ulong lpTotalNumberOfBytes,
            out ulong lpTotalNumberOfFreeBytes);

        protected bool GetDiskFreeSpace(string directoryPath, out long freeBytesAvailableToUser,
                                        out long totalDriveCapacityBytes, out long totalNumberOfFreeBytes)
        {
            ulong freeAvailableUser;
            ulong totalDriveCapacity;
            ulong totalFree;

            var iResult = GetDiskFreeSpaceEx(directoryPath, out freeAvailableUser, out totalDriveCapacity, out totalFree);

            if (iResult == 0)
            {
                freeBytesAvailableToUser = 0;
                totalDriveCapacityBytes = 0;
                totalNumberOfFreeBytes = 0;

                return false;
            }

            freeBytesAvailableToUser = (long)freeAvailableUser;
            totalDriveCapacityBytes = (long)totalDriveCapacity;
            totalNumberOfFreeBytes = (long)totalFree;

            return true;
        }

        protected string GetStoragePathBase()
        {
            var storagePath = m_Task.GetParam("Storage_Path");

            // Make sure storagePath only contains the root folder, not several folders
            // In other words, if storagePath = "VOrbiETD03\2011_4" change it to just "VOrbiETD03"
            var slashLoc = storagePath.IndexOf(Path.DirectorySeparatorChar);
            if (slashLoc > 0)
                storagePath = storagePath.Substring(0, slashLoc);

            // Always use the UNC path defined by Storage_Vol_External when checking drive free space
            // Example path is: \\Proto-7\
            var datasetStoragePathBase = m_Task.GetParam("Storage_Vol_External");

            datasetStoragePathBase = Path.Combine(datasetStoragePathBase, storagePath);

            return datasetStoragePathBase;
        }

        /// <summary>
        /// Validates that the dataset storage drive has sufficient free space
        /// </summary>
        /// <param name="errMsg"></param>
        /// <returns>True if OK; false if not enough free space</returns>
        protected bool ValidateFreeDiskSpace(out string errMsg)
        {
            const int DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB = 30;

            var datasetStoragePath = string.Empty;
            errMsg = string.Empty;

            try
            {
                var stepToolLCase = m_StepTool.ToLower();

                if (stepToolLCase.Contains("archiveupdate") ||
                    stepToolLCase.Contains("datasetarchive") ||
                    stepToolLCase.Contains("sourcefilerename"))
                {
                    // We don't need to validate free space with these step tools
                    return true;
                }

                datasetStoragePath = GetStoragePathBase();

                long freeBytesAvailableToUser;
                long totalDriveCapacityBytes;
                long totalNumberOfFreeBytes;
                if (GetDiskFreeSpace(datasetStoragePath, out freeBytesAvailableToUser, out totalDriveCapacityBytes,
                                     out totalNumberOfFreeBytes))
                {
                    var freeSpaceGB = totalNumberOfFreeBytes / 1024.0 / 1024.0 / 1024.0;

                    if (freeSpaceGB < DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB)
                    {
                        errMsg = "Dataset directory drive has less than " +
                                 DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB.ToString("0") + "GB free: " +
                                 freeSpaceGB.ToString("0.00") + " GB available";

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                             errMsg + ": " + datasetStoragePath);
                        if (m_TraceMode)
                            ShowTraceMessage(errMsg);

                        return false;
                    }
                }
                else
                {
                    errMsg = "Error validating dataset storage free drive space: " + datasetStoragePath +
                             " (GetDiskFreeSpaceEx returned false)";
                    if (Environment.MachineName.ToLower().StartsWith("monroe"))
                    {
                        Console.WriteLine(@"Warning: " + errMsg);
                        return true;
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errMsg);
                    if (m_TraceMode)
                        ShowTraceMessage(errMsg);

                    return false;
                }
            }
            catch (Exception ex)
            {
                errMsg = "Exception validating dataset storage free drive space: " + datasetStoragePath;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     errMsg + "; " + ex.Message);
                if (m_TraceMode)
                    ShowTraceMessage(errMsg);

                return false;
            }

            return true;
        }

        /// <summary>
        /// Verifies working directory is properly specified
        /// </summary>
        /// <returns>TRUE for success, FALSE otherwise</returns>
        private bool ValidateWorkingDir()
        {
            var workingDir = m_MgrSettings.GetParam("WorkDir");

            if (Directory.Exists(workingDir))
            {
                return true;
            }

            const string alternateWorkDir = @"E:\CapMan_WorkDir";

            if (Directory.Exists(alternateWorkDir))
            {
                // Auto-update the working directory
                m_MgrSettings.SetParam("WorkDir", alternateWorkDir);

                var msg = "Invalid working directory: " + workingDir + "; automatically switched to " + alternateWorkDir;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
                if (m_TraceMode)
                    ShowTraceMessage(msg);
            }
            else
            {
                var msg = "Invalid working directory: " + workingDir;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, msg);
                if (m_TraceMode)
                    ShowTraceMessage(msg);

                return false;
            }

            // No problem found
            return true;
        }

        #endregion

        #region "Event handlers"

        private void FileWatcherChanged(object sender, FileSystemEventArgs e)
        {
            const string msg = "clsMainProgram.FileWatcherChanged event received";
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            m_ConfigChanged = true;
            m_FileWatcher.EnableRaisingEvents = false;
        }

        private void OnBroadcastReceived(string cmdText)
        {
            var msg = "clsMainProgram.OnBroadcasetReceived event; message = " + cmdText;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

            clsBroadcastCmd recvCmd;

            // Parse the received message
            try
            {
                recvCmd = clsXMLTools.ParseBroadcastXML(cmdText);
            }
            catch (Exception ex)
            {
                msg = "Exception while parsing broadcast data";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                if (m_TraceMode)
                    ShowTraceMessage(msg);

                return;
            }

            // Determine if the message applies to this machine
            if (!recvCmd.MachineList.Contains(m_MgrName))
            {
                // Received command doesn't apply to this manager
                msg = "Received command not applicable to this manager instance";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

                return;
            }

            // Get the command and take appropriate action
            switch (recvCmd.MachCmd.ToLower())
            {
                case "shutdown":
                    m_LoopExitCode = LoopExitCode.ShutdownCmdReceived;
                    m_Running = false;
                    break;
                case "readconfig":
                    msg = "Reload config message received";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
                    m_ConfigChanged = true;
                    m_Running = false;
                    break;
                default:
                    // Invalid command received; do nothing except log it
                    msg = "Invalid broadcast command received: " + cmdText;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                    break;
            }
        }

        private void OnCommandReceived(string cmdText)
        {
            //TODO: (Future)
        }

        void OnStatusMonitorUpdateReceived(string msg)
        {
            if (m_MsgQueueInitSuccess)
                m_MsgHandler.SendMessage(msg);
        }

        /// <summary>
        /// Updates the status at m_StatusTimer interval
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void m_StatusTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            var duration = DateTime.UtcNow - m_DurationStart;
            m_StatusFile.Duration = (Single)duration.TotalHours;
            m_StatusFile.WriteStatusFile();
        }

        #endregion
    } // End class
} // End namespace