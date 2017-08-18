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

namespace CaptureTaskManager
{
    public class clsMainProgram : clsLoggerBase
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

        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

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
        private bool m_ManagerDeactivatedLocally;

        private readonly bool m_TraceMode;

        #endregion

        #region "Delegates"

        #endregion

        #region "Events"

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, the manager is deactivated locally
        /// </summary>
        public bool ManagerDeactivatedLocally => m_ManagerDeactivatedLocally;

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
            var restartOK = true;

            // Determine cause of loop exit and respond accordingly
            switch (eLoopExitCode)
            {
                case LoopExitCode.ConfigChanged:
                    // Reload the manager config
                    LogMessage("Reloading configuration and restarting manager");

                    // Unsubscribe message handler events and close msssage handler
                    if (m_MsgQueueInitSuccess)
                    {
                        m_MsgHandler.Dispose();
                    }
                    restartOK = true;
                    break;

                case LoopExitCode.DisabledMC:
                    // Manager is disabled via manager control db
                    LogMessage("Manager disabled in manager control DB");

                    m_StatusFile.UpdateDisabled(false);
                    restartOK = false;
                    break;

                case LoopExitCode.DisabledLocally:
                    // Manager disabled locally
                    LogMessage("Manager disabled locally");

                    m_StatusFile.UpdateDisabled(true);
                    restartOK = false;
                    break;

                case LoopExitCode.ExcessiveRequestErrors:
                    // Too many errors
                    LogError("Excessive errors requesting task; closing manager");

                    // Do not create a flag file; intermittent network connectivity is likely resulting in failure to request a task
                    // This will likely clear up eventually

                    m_StatusFile.UpdateStopped(true);

                    restartOK = false;
                    break;

                case LoopExitCode.InvalidWorkDir:
                    // Working directory not valid
                    LogError("Working directory problem, disabling manager");

                    m_StatusFile.CreateStatusFlagFile();
                    m_StatusFile.UpdateStopped(true);

                    restartOK = false;
                    break;

                case LoopExitCode.NoTaskFound:
                    // No capture task found
                    LogDebug("No capture tasks found");

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.ShutdownCmdReceived:
                    // Shutdown command received
                    LogMessage("Shutdown command received, closing manager");

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.ExceededMaxTaskCount:
                    // Max number of consecutive jobs reached
                    LogMessage("Exceeded maximum job count, closing manager");

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.UpdateRequired:
                    // Manager update required
                    LogMessage("Manager update is required, closing manager");

                    m_MgrSettings.AckManagerUpdateRequired();
                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.FlagFile:
                    // Flag file is present
                    LogError("Flag file exists - unable to continue analysis");

                    m_StatusFile.UpdateStopped(true);
                    restartOK = false;
                    break;

                case LoopExitCode.NeedToAbortProcessing:
                    // Step tool set flag NeedToAbortProcessing to true
                    LogMessage("NeedToAbortProcessing = true, closing manager");

                    m_StatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                default:
                    // Should never get here
                    break;
            }

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

            // LogLevel is 1 to 5: 1 for Fatal errors only, 4 for Fatal, Error, Warning, and Info, and 5 for everything including Debug messages
            m_DebugLevel = m_MgrSettings.GetParam("debuglevel", 4);
            clsLogTools.CreateFileLogger(logFileName, m_DebugLevel);

            if (m_MgrSettings.GetBooleanParam("ftplogging"))
                clsLogTools.CreateFtpLogFileLogger();

            var logCnStr = m_MgrSettings.GetParam("connectionstring");

            clsLogTools.RemoveDefaultDbLogger();
            clsLogTools.CreateDbLogger(logCnStr, "CaptureTaskMan: " + m_MgrName, false);

            // Make initial log entry
            if (m_TraceMode)
                ShowTraceMessage("Initializing log file " + logFileName);

            LogMessage("=== Started Capture Task Manager V" + Application.ProductVersion + " ===== ");

            // Setup the message queue
            m_MsgQueueInitSuccess = false;
            m_MsgHandler = new clsMessageHandler();
            m_MsgHandler.BrokerUri = m_MsgHandler.BrokerUri = m_MgrSettings.GetParam("MessageQueueURI");

            // Typically "Manager.Status"
            m_MsgHandler.StatusTopicName = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");

            m_MsgHandler.MgrSettings = m_MgrSettings;

            // Initialize the message queue
            // Start this in a separate thread so that we can abort the initialization if necessary
            InitializeMessageQueue();

            var configFileName = m_MgrSettings.GetParam("configfilename");
            if (string.IsNullOrEmpty(configFileName))
            {
                // Manager parameter error; log an error and exit
                LogError("Manager parameter 'configfilename' is undefined; this likely indicates a problem retrieving manager parameters. " +
                         "Shutting down the manager");
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
                LogError("Error determining the parent path for the executable, " + Application.ExecutablePath);
                return false;
            }

            var statusFileNameLoc = Path.Combine(fInfo.DirectoryName, "Status.xml");
            m_StatusFile = new clsStatusFile(statusFileNameLoc)
            {
                MgrName = m_MgrName,
                MgrStatus = EnumMgrStatus.Running
            };

            RegisterEvents((clsEventNotifier)m_StatusFile);

            m_StatusFile.MonitorUpdateRequired += OnStatusMonitorUpdateReceived;

            var logStatusToMessageQueue = m_MgrSettings.GetBooleanParam("LogStatusToMessageQueue");
            var messageQueueUri = m_MgrSettings.GetParam("MessageQueueURI");
            var messageQueueTopicMgrStatus = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");

            m_StatusFile.ConfigureMessageQueueLogging(logStatusToMessageQueue, messageQueueUri, messageQueueTopicMgrStatus);

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
                    LogError("Exception reading status history file", ex);
                }
            }

            // Everything worked!
            return true;
        }

        private void InitializeMessageQueue()
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
                LogWarning("Unable to initialize the message queue (timeout after " + MAX_WAIT_TIME_SECONDS + " seconds)");
                return;
            }

            var elaspedTime = DateTime.UtcNow.Subtract(dtWaitStart).TotalSeconds;

            if (elaspedTime > 25)
            {
                LogWarning("Connection to the message queue was slow, taking " + (int)elaspedTime + " seconds");
            }
        }

        private void InitializeMessageQueueWork()
        {
            if (!m_MsgHandler.Init())
            {
                // Most error messages provided by .Init method, but debug message is here for program tracking
                LogDebug("Message handler init error");
                m_MsgQueueInitSuccess = false;
                if (m_TraceMode)
                    ShowTraceMessage("m_MsgQueueInitSuccess = false: Message handler init error");
            }
            else
            {
                LogDebug("Message handler initialized");
                m_MsgQueueInitSuccess = true;
                if (m_TraceMode)
                    ShowTraceMessage("m_MsgQueueInitSuccess = true");
            }
        }

        private Dictionary<string, DateTime> LoadCachedLogMessages(FileSystemInfo messageCacheFile)
        {
            var cachedMessages = new Dictionary<string, DateTime>();

            using (var reader = new StreamReader(new FileStream(messageCacheFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                var lineCount = 0;
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    lineCount += 1;

                    // Assume that the first line is the header line, which we'll skip
                    if (lineCount == 1 || string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var lineParts = dataLine.Split(new[] { '\t' }, 2);

                    var timeStampText = lineParts[0];
                    var message = lineParts[1];

                    if (DateTime.TryParse(timeStampText, out var timeStamp))
                    {
                        // Valid message; store it

                        if (cachedMessages.TryGetValue(message, out var cachedTimeStamp))
                        {
                            if (timeStamp > cachedTimeStamp)
                                cachedMessages[message] = timeStamp;
                        }
                        else
                        {
                            cachedMessages.Add(message, timeStamp);
                        }
                    }

                }
            }

            return cachedMessages;
        }

        private void LogErrorToDatabasePeriodically(string errorMessage, int logIntervalHours)
        {
            const string PERIODIC_LOG_FILE = "Periodic_ErrorMessages.txt";

            try
            {
                Dictionary<string, DateTime> cachedMessages;

                var messageCacheFile = new FileInfo(Path.Combine(clsUtilities.GetAppFolderPath(), PERIODIC_LOG_FILE));

                if (messageCacheFile.Exists)
                {
                    cachedMessages = LoadCachedLogMessages(messageCacheFile);
                    System.Threading.Thread.Sleep(150);
                }
                else
                {
                    cachedMessages = new Dictionary<string, DateTime>();
                }

                if (cachedMessages.TryGetValue(errorMessage, out var timeStamp))
                {
                    if (DateTime.UtcNow.Subtract(timeStamp).TotalHours < logIntervalHours)
                    {
                        // Do not log to the database
                        return;
                    }
                    cachedMessages[errorMessage] = DateTime.UtcNow;
                }
                else
                {
                    cachedMessages.Add(errorMessage, DateTime.UtcNow);
                }

                LogError(errorMessage, true);

                // Update the message cache file
                using (var writer = new StreamWriter(new FileStream(messageCacheFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    writer.WriteLine("{0}\t{1}", "TimeStamp", "Message");
                    foreach (var message in cachedMessages)
                    {
                        writer.WriteLine("{0}\t{1}", message.Value.ToString(DATE_TIME_FORMAT), message.Key);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in LogErrorToDatabasePeriodically", ex);
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
                    // Verify that an error hasn't left the the system in an odd state
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
                    // Do not request a task between 12 am and 6 am on Thursday in the week with the third Tuesday of the month
                    // Do not request a task between 2 am and 4 am or between 9 am and 11 am on Sunday following the week with the second Tuesday of the month
                    if (clsWindowsUpdateStatus.UpdatesArePending(out var pendingWindowsUpdateMessage))
                    {
                        LogMessage(pendingWindowsUpdateMessage);
                        m_LoopExitCode = LoopExitCode.NoTaskFound;
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

                            PerformTask(out var eTaskCloseout);

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
                            // Should never get here
                            break;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Error in PerformMainLoop", ex);
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
                LogError("Exception writing job history file", ex);
            }

            // Evaluate the loop exit code
            var restartOK = EvaluateLoopExitCode(m_LoopExitCode);

            if (!restartOK)
            {
                LogDebug("===== Closing Capture Task Manager =====", writeToLog: false);
            }

            return restartOK;
        }

        private void PerformTask(out EnumCloseOutType eTaskCloseout)
        {
            eTaskCloseout = EnumCloseOutType.CLOSEOUT_NOT_READY;

            try
            {
                // Cache the job parameters
                m_StepTool = m_Task.GetParam("StepTool");
                m_Job = m_Task.GetParam("Job");
                m_Dataset = m_Task.GetParam("Dataset");
                var stepNumber = m_Task.GetParam("Step");

                LogDebug("Job " + m_Job + ", step " + stepNumber + " assigned");

                // Update the status
                m_StatusFile.JobNumber = int.Parse(m_Job);
                m_StatusFile.Dataset = m_Dataset;
                m_StatusFile.MgrStatus = EnumMgrStatus.Running;
                m_StatusFile.Tool = m_StepTool;
                m_StatusFile.TaskStartTime = DateTime.UtcNow;
                m_StatusFile.TaskStatus = EnumTaskStatus.Running;
                m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.Running_Tool;
                m_StatusFile.MostRecentJobInfo = DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss tt") +
                                                 ", Job " + m_Job + ", Step " + stepNumber +
                                                 ", Tool " + m_StepTool;

                m_StatusFile.WriteStatusFile();

                // Create the tool runner object
                if (!SetToolRunnerObject(m_StepTool))
                {
                    var errMsg = "Unable to SetToolRunnerObject";
                    LogError(m_MgrName + ": " + errMsg + ", job " + m_Job + ", Dataset " + m_Dataset, true);

                    m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, errMsg);

                    m_StatusFile.UpdateIdle();
                    return;
                }

                // Make sure we have enough free space on the drive with the dataset folder
                if (!ValidateFreeDiskSpace(out var diskSpaceMsg))
                {
                    if (string.IsNullOrEmpty(diskSpaceMsg))
                    {
                        diskSpaceMsg = "Insufficient free space (location undefined)";
                        LogError(diskSpaceMsg);
                    }
                    m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, diskSpaceMsg);
                    m_StatusFile.UpdateIdle();
                    return;
                }

                // Run the tool plugin
                m_StatusTimer.Enabled = true;
                var toolResult = m_CapTool.RunTool();
                m_StatusTimer.Enabled = false;

                eTaskCloseout = toolResult.CloseoutType;
                string sCloseoutMessage;

                switch (eTaskCloseout)
                {
                    case EnumCloseOutType.CLOSEOUT_FAILED:
                        LogError(m_MgrName + ": Failure running tool " + m_StepTool + ", job " + m_Job + ", Dataset " + m_Dataset);

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                            sCloseoutMessage = toolResult.CloseoutMsg;
                        else
                            sCloseoutMessage = "Failure running tool " + m_StepTool;

                        m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NOT_READY:
                        string msg;
                        if (m_StepTool == "ArchiveVerify" || m_StepTool == "ArchiveStatusCheck")
                        {
                            msg = "Dataset not ready, tool " + m_StepTool + ", job " + m_Job + ": " + toolResult.CloseoutMsg;
                        }
                        else
                        {
                            msg = "Dataset not ready, tool " + m_StepTool + ", job " + m_Job + ", Dataset " + m_Dataset;
                        }

                        LogWarning(msg);

                        sCloseoutMessage = "Dataset not ready";

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                            sCloseoutMessage += ": " + toolResult.CloseoutMsg;

                        m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_SUCCESS:
                        LogDebug(m_MgrName + ": Step complete, tool " + m_StepTool + ", job " + m_Job + ", Dataset " + m_Dataset);

                        m_Task.CloseTask(eTaskCloseout, toolResult.CloseoutMsg, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING:
                        LogError(m_MgrName + ": Failure running tool " + m_StepTool
                              + ", job " + m_Job + ", Dataset " + m_Dataset
                              + "; CloseOut = NeedToAbortProcessing");

                        sCloseoutMessage = "Error: NeedToAbortProcessing";

                        m_Task.CloseTask(eTaskCloseout, sCloseoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    default:
                        // Should never get here
                        break;
                }
            }
            catch (Exception ex)
            {
                LogError("Error running task", ex);

                LogError(m_MgrName + ": Failure running tool " + m_StepTool
                      + ", job " + m_Job + ", Dataset " + m_Dataset
                      + "; CloseOut = Exception", true);

                LogError("Tool runner exception", ex);
                m_Task.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, "Exception: " + ex.Message, EnumEvalCode.EVAL_CODE_FAILED,
                                 "Exception running tool");
            }

            // Update the status
            m_StatusFile.ClearCachedInfo();

            m_StatusFile.MgrStatus = EnumMgrStatus.Running;
            m_StatusFile.TaskStatus = EnumTaskStatus.No_Task;
            m_StatusFile.TaskStatusDetail = EnumTaskStatusDetail.No_Task;
            m_StatusFile.WriteStatusFile();
        }

        /// <summary>
        /// Post a test log message
        /// </summary>
        public void PostTestLogMessage()
        {
            try
            {
                var sMessage = "Test log message: " + DateTime.Now.ToString(DATE_TIME_FORMAT);
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
                var appFolder = clsUtilities.GetAppFolderPath();

                if (string.IsNullOrWhiteSpace(appFolder))
                {
                    LogWarning("GetAppFolderPath returned an empty directory path to RemoveOldFTPLogFiles");
                    return;
                }

                var appFolderInfo = new DirectoryInfo(appFolder);

                foreach (var fiFile in appFolderInfo.GetFiles("FTPlog_*"))
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
                LogError("Exception removing old FTP log files", ex);
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
                if (!diFolder.Exists)
                {
                    LogWarning("Folder not found: " + sTempFolderPath);
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
                    var msg = "Deleted " + iTotalDeleted + " temp file";
                    if (iTotalDeleted > 1)
                        msg += "s";

                    msg += " over " + iAgedTempFilesHours + " hours old in folder " + sTempFolderPath;
                    LogMessage(msg);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception removing old temp files", ex);
            }
        }

        /// <summary>
        /// Sets the tool runner object for this job
        /// </summary>
        /// <returns></returns>
        private bool SetToolRunnerObject(string stepToolName)
        {

            // Load the tool runner
            m_CapTool = clsPluginLoader.GetToolRunner(stepToolName);
            if (m_CapTool == null)
            {
                LogError("Unable to load tool runner for StepTool " + stepToolName + ": " + clsPluginLoader.ErrMsg);
                return false;
            }

            LogDebug("Loaded tool runner for Step Tool " + stepToolName);

            try
            {
#if MyEMSL_OFFLINE
    // When this Conditional Compilation Constant is defined, then the DatasetArchive plugin will set debugMode
    // to Pacifica.Core.EasyHttp.eDebugMode.MyEMSLOfflineMode when calling UploadToMyEMSLWithRetry()
    // This in turn results in writeToDisk becoming True in SendFileListToDavAsTar
    m_Task.AddAdditionalParameter("MyEMSLOffline", "true");
    LogMessage("Adding job parameter MyEMSLOffline=true");
#endif

#if MyEMSL_TEST_TAR
    m_Task.AddAdditionalParameter("DebugTestTar", "true");
    LogMessage("Adding job parameter DebugTestTar=true");
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
                LogError("Exception calling CapTool.Setup", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        private static void ShowTraceMessage(string message)
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
                // No flag file
                return false;
            }

            string errorMessage;
            try
            {
                var objCleanupMgrErrors = new clsCleanupMgrErrors(
                    m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING),
                    m_MgrName,
                    m_MgrSettings.GetParam("WorkDir"),
                    m_StatusFile);

                var cleanupModeVal = m_MgrSettings.GetParam("ManagerErrorCleanupMode", 0);
                var cleanupSuccess = objCleanupMgrErrors.AutoCleanupManagerErrors(cleanupModeVal);

                if (cleanupSuccess)
                {
                    LogWarning("Flag file found; automatically cleaned the work directory and deleted the flag file(s)");

                    // No error; return false
                    return false;
                }

                var flagFile = new FileInfo(m_StatusFile.FlagFilePath);
                if (flagFile.Directory == null)
                    errorMessage = "Flag file exists in the manager folder";
                else
                    errorMessage = "Flag file exists in folder " + flagFile.Directory.Name;
            }
            catch (Exception ex)
            {
                errorMessage = "Error calling AutoCleanupManagerErrors from StatusFlagFileError";
                LogError(errorMessage, ex);
            }

            // Flag file was not removed (either an error removing it or the manager is not set to auto-remove it)

            // Post a log entry to the database every 4 hours
            LogErrorToDatabasePeriodically(errorMessage, 4);

            // Flag file exists; return true
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

            LogDebug("Updating manager settings using Manager Control database");

            if (!m_MgrSettings.LoadMgrSettingsFromDB())
            {
                // Error retrieving settings from the manager control DB
                string msg;

                if (string.IsNullOrEmpty(m_MgrSettings.ErrMsg))
                    msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";
                else
                    msg = m_MgrSettings.ErrMsg;

                LogError(msg);

                bSuccess = false;
            }
            else
            {
                // Update the log level
                // 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
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

            var iResult = GetDiskFreeSpaceEx(directoryPath, out var freeAvailableUser, out var totalDriveCapacity, out var totalFree);

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

                var success = GetDiskFreeSpace(
                    datasetStoragePath,
                    out _,
                    out _,
                    out var totalNumberOfFreeBytes);

                if (success)
                {
                    var freeSpaceGB = totalNumberOfFreeBytes / 1024.0 / 1024.0 / 1024.0;

                    if (freeSpaceGB < DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB)
                    {
                        errMsg = "Dataset directory drive has less than " +
                                 DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB.ToString("0") + "GB free: " +
                                 freeSpaceGB.ToString("0.00") + " GB available";

                        LogError(errMsg + ": " + datasetStoragePath);

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

                    LogError(errMsg);
                    return false;
                }
            }
            catch (Exception ex)
            {
                errMsg = "Exception validating dataset storage free drive space: " + datasetStoragePath;
                LogError(errMsg, ex);
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

            var alternateWorkDirs = new List<string> {
                @"C:\CapMan_WorkDir",
                @"E:\CapMan_WorkDir",
                @"D:\CapMan_WorkDir" };

            foreach (var alternateWorkDir in alternateWorkDirs)
            {
                if (!Directory.Exists(alternateWorkDir))
                    continue;

                // Auto-update the working directory
                m_MgrSettings.SetParam("WorkDir", alternateWorkDir);

                LogWarning("Invalid working directory: " + workingDir + "; automatically switched to " + alternateWorkDir);
                return true;
            }

            LogError("Invalid working directory: " + workingDir, true);
            return false;
        }

        #endregion

        #region "clsEventNotifier events"

        private void RegisterEvents(clsEventNotifier oProcessingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                oProcessingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                oProcessingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            oProcessingClass.StatusEvent += StatusEventHandler;
            oProcessingClass.ErrorEvent += ErrorEventHandler;
            oProcessingClass.WarningEvent += WarningEventHandler;
            oProcessingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogDebug(statusMessage, writeToLog: false);
        }

        private void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        private void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
        }

        private void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }

        private void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            m_StatusFile.CurrentOperation = progressMessage;
            m_StatusFile.UpdateAndWrite(percentComplete);
        }
        #endregion

        #region "Event handlers"

        private void FileWatcherChanged(object sender, FileSystemEventArgs e)
        {
            LogDebug("clsMainProgram.FileWatcherChanged event received");

            m_ConfigChanged = true;
            m_FileWatcher.EnableRaisingEvents = false;
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
            m_StatusFile.WriteStatusFile();
        }

        #endregion
    }
}