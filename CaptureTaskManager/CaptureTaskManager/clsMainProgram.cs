//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using PRISM;
using PRISM.Logging;
using PRISMWin;

namespace CaptureTaskManager
{
    /// <summary>
    /// Capture Task Manager main program execution loop
    /// </summary>
    public class clsMainProgram : clsLoggerBase
    {

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

        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        private const string DEFAULT_BASE_LOGFILE_NAME = @"Logs\CapTaskMan";

        private const bool ENABLE_LOGGER_TRACE_MODE = false;

        #endregion

        #region "Class variables"

        private clsMgrSettings m_MgrSettings;

        private readonly string m_MgrExeName;
        private readonly string m_MgrDirectoryPath;

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
        private BaseLogger.LogLevels m_DebugLevel = BaseLogger.LogLevels.DEBUG;

        private bool m_Running;
        private System.Timers.Timer m_StatusTimer;

        #endregion

        #region "Delegates"

        #endregion

        #region "Events"

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        #endregion

        #region "Constructors"

        /// <summary>
        /// Constructor
        /// </summary>
        public clsMainProgram(bool traceMode)
        {
            TraceMode = traceMode;

            var exeInfo = new FileInfo(PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath());
            m_MgrExeName = exeInfo.Name;
            m_MgrDirectoryPath = exeInfo.DirectoryName;
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

                    // Unsubscribe message handler events and close the message handler
                    if (m_MsgQueueInitSuccess)
                    {
                        m_MsgHandler.Dispose();
                    }
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
                    LogDebug("No capture tasks found for " + m_MgrName);

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
            var hostName = System.Net.Dns.GetHostName();

            // Define the default logging info
            // This will get updated below
            LogTools.CreateFileLogger(DEFAULT_BASE_LOGFILE_NAME, BaseLogger.LogLevels.DEBUG);

            // Create a database logger connected to DMS5
            // Once the initial parameters have been successfully read,
            // we update the dbLogger to use the connection string read from the Manager Control DB
            string defaultDmsConnectionString;

            // Open CaptureTaskManager.exe.config to look for setting DefaultDMSConnString, so we know which server to log to by default
            var dmsConnectionStringFromConfig = GetXmlConfigDefaultConnectionString();

            if (string.IsNullOrWhiteSpace(dmsConnectionStringFromConfig))
            {
                // Use the hard-coded default that points to Gigasax
                defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;
            }
            else
            {
                // Use the connection string from CaptureTaskManager.exe.config
                defaultDmsConnectionString = dmsConnectionStringFromConfig;
            }

            ShowTrace("Instantiate a DbLogger using " + defaultDmsConnectionString);

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            LogTools.CreateDbLogger(defaultDmsConnectionString, "CaptureTaskMan: " + hostName, TraceMode && ENABLE_LOGGER_TRACE_MODE);

            LogTools.MessageLogged += MessageLoggedHandler;

            // Get the manager settings
            // If you get an exception here while debugging in Visual Studio, be sure
            //  that "UsingDefaults" is set to False in AppName.exe.config
            try
            {
                ShowTrace("Reading application config file");

                m_MgrSettings = new clsMgrSettings(TraceMode);
            }
            catch (Exception ex)
            {
                if (string.Equals(ex.Message, clsMgrSettings.DEACTIVATED_LOCALLY))
                {
                    // Manager is deactivated locally
                }
                else
                {
                    ConsoleMsgUtils.ShowError("Exception instantiating clsMgrSettings: " + ex.Message);
                    Thread.Sleep(500);
                }

                return false;
            }

            // Update the cached values for this manager and job
            m_MgrName = m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_NAME);
            ShowTrace("Manager name is " + m_MgrName);

            m_StepTool = "Unknown";
            m_Job = "Unknown";
            m_Dataset = "Unknown";

            // Setup the loggers
            var logFileNameBase = m_MgrSettings.GetParam("LogFileName", "CapTaskMan");

            UpdateLogLevel(m_MgrSettings);

            LogTools.CreateFileLogger(logFileNameBase, m_DebugLevel);

            var logCnStr = m_MgrSettings.GetParam("ConnectionString");

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            LogTools.CreateDbLogger(logCnStr, "CaptureTaskMan: " + m_MgrName, TraceMode && ENABLE_LOGGER_TRACE_MODE);

            // Make the initial log entry
            var relativeLogFilePath = LogTools.CurrentLogFilePath;
            var logFile = new FileInfo(relativeLogFilePath);
            ShowTrace("Initializing log file " + clsPathUtils.CompactPathString(logFile.FullName, 60));

            var appVersion = Assembly.GetEntryAssembly().GetName().Version;
            var startupMsg = "=== Started Capture Task Manager V" + appVersion + " ===== ";
            LogMessage(startupMsg);

            var configFileName = m_MgrSettings.GetParam("ConfigFileName");
            if (string.IsNullOrEmpty(configFileName))
            {
                // Manager parameter error; log an error and exit
                LogError("Manager parameter 'ConfigFileName' is undefined; this likely indicates a problem retrieving manager parameters. " +
                         "Shutting down the manager");
                return false;
            }

            // Setup a file watcher for the config file

            m_FileWatcher = new FileSystemWatcher
            {
                Path = m_MgrDirectoryPath,
                IncludeSubdirectories = false,
                Filter = configFileName,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };

            // Subscribe to the file watcher Changed event
            m_FileWatcher.Changed += FileWatcherChanged;

            // Make sure that the manager name matches the machine name (with a few exceptions)
            if (!hostName.StartsWith("EMSLMQ", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("EMSLPub", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("WE27676", StringComparison.OrdinalIgnoreCase))
            {
                if (!m_MgrName.StartsWith(hostName, StringComparison.OrdinalIgnoreCase))
                {
                    LogError("Manager name does not match the host name: " + m_MgrName + " vs. " + hostName + "; update " + configFileName);
                    return false;
                }
            }

            // Setup the message queue
            m_MsgQueueInitSuccess = false;
            m_MsgHandler = new clsMessageHandler
            {
                BrokerUri = m_MgrSettings.GetParam("MessageQueueURI"),
                StatusTopicName = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus"),    // Typically "Manager.Status"
                MgrSettings = m_MgrSettings
            };

            // Initialize the message queue
            // Start this in a separate thread so that we can abort the initialization if necessary
            InitializeMessageQueue();

            // Set up the tool for getting tasks
            m_Task = new clsCaptureTask(m_MgrSettings) {
                TraceMode = TraceMode
            };

            // Set up the status file class
            if (string.IsNullOrWhiteSpace(m_MgrDirectoryPath))
            {
                LogError("Error determining the parent path for the executable, " + m_MgrExeName);
                return false;
            }

            var statusFileNameLoc = Path.Combine(m_MgrDirectoryPath, "Status.xml");
            m_StatusFile = new clsStatusFile(statusFileNameLoc)
            {
                MgrName = m_MgrName,
                MgrStatus = EnumMgrStatus.Running
            };

            RegisterEvents((clsEventNotifier)m_StatusFile);

            m_StatusFile.MonitorUpdateRequired += OnStatusMonitorUpdateReceived;

            var logStatusToMessageQueue = m_MgrSettings.GetParam("LogStatusToMessageQueue", false);
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
            if (!File.Exists(historyFile))
            {
                return true;
            }

            try
            {
                // Create an instance of StreamReader to read from a file.
                // The using statement also closes the StreamReader.
                using (var sr = new StreamReader(historyFile))
                {
                    string line;
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

            return true;
        }

        private void InitializeMessageQueue()
        {
            const int MAX_WAIT_TIME_SECONDS = 60;

            var worker = new Thread(InitializeMessageQueueWork);
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

            var elapsedTime = DateTime.UtcNow.Subtract(dtWaitStart).TotalSeconds;

            if (elapsedTime > 25)
            {
                LogWarning("Connection to the message queue was slow, taking " + (int)elapsedTime + " seconds");
            }
        }

        private void InitializeMessageQueueWork()
        {
            if (!m_MsgHandler.Init())
            {
                // Most error messages provided by .Init method, but debug message is here for program tracking
                LogDebug("Message handler init error");
                m_MsgQueueInitSuccess = false;
                ShowTrace("m_MsgQueueInitSuccess = false: Message handler init error");
            }
            else
            {
                LogDebug("Message handler initialized");
                m_MsgQueueInitSuccess = true;
                ShowTrace("m_MsgQueueInitSuccess = true");
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

                var messageCacheFile = new FileInfo(Path.Combine(clsUtilities.GetAppDirectoryPath(), PERIODIC_LOG_FILE));

                if (messageCacheFile.Exists)
                {
                    cachedMessages = LoadCachedLogMessages(messageCacheFile);
                    Thread.Sleep(150);
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

        private void MessageLoggedHandler(string message, BaseLogger.LogLevels logLevel)
        {
            var timeStamp = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss");

            // Update the status file data
            clsStatusData.MostRecentLogMessage = timeStamp + "; " + message + "; " + logLevel;

            if (logLevel <= BaseLogger.LogLevels.ERROR)
            {
                clsStatusData.AddErrorMessage(timeStamp + "; " + message + "; " + logLevel);
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

            var clearWorkDirectory = true;

            // Begin main execution loop
            while (m_Running)
            {
                try
                {
                    // Verify that an error hasn't left the the system in an odd state
                    if (StatusFlagFileError(clearWorkDirectory))
                    {
                        m_LoopExitCode = LoopExitCode.FlagFile;
                        break;
                    }

                    clearWorkDirectory = false;

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
                    if (!m_MgrSettings.GetParam("MgrActive", false))
                    {
                        // Disabled via manager control db
                        m_LoopExitCode = LoopExitCode.DisabledMC;
                        break;
                    }

                    if (!m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, false))
                    {
                        m_LoopExitCode = LoopExitCode.DisabledLocally;
                        break;
                    }

                    if (m_MgrSettings.GetParam("ManagerUpdateRequired", false))
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

                            ShowTrace("Task found for " + m_MgrName);

                            PerformTask(out var eTaskCloseout);

                            // Increment and test the task counter
                            taskCount++;
                            if (taskCount > m_MgrSettings.GetParam("MaxRepetitions", 1))
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
    // When this Conditional Compilation Constant is defined, the DatasetArchive plugin will set debugMode
    // to Pacifica.Core.EasyHttp.eDebugMode.MyEMSLOfflineMode when calling UploadToMyEMSLWithRetry()
    // This in turn results in writeToDisk becoming True in SendFileListToDavAsTar
    m_Task.AddAdditionalParameter("MyEMSLOffline", "true");
    LogMessage("Adding job parameter MyEMSLOffline=true");
#endif

#if MyEMSL_TEST_TAR
    m_Task.AddAdditionalParameter("DebugTestTar", "true");
    LogMessage("Adding job parameter DebugTestTar=true");
#endif
                if (TraceMode)
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

        private void ShowTrace(string message)
        {
            if (TraceMode)
                ShowTraceMessage(message);
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
        private bool StatusFlagFileError(bool clearWorkDirectory)
        {
            var cleanupModeVal = m_MgrSettings.GetParam("ManagerErrorCleanupMode", 0);
            clsCleanupMgrErrors.eCleanupModeConstants cleanupMode;

            if (Enum.IsDefined(typeof(clsCleanupMgrErrors.eCleanupModeConstants), cleanupModeVal))
            {
                cleanupMode = (clsCleanupMgrErrors.eCleanupModeConstants)cleanupModeVal;
            }
            else
            {
                cleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled;
            }

            if (!m_StatusFile.DetectStatusFlagFile())
            {
                // No flag file

                if (clearWorkDirectory && cleanupMode == clsCleanupMgrErrors.eCleanupModeConstants.CleanupAlways)
                {
                    // Delete all files in the working directory (but ignore errors)
                    // Delete all folders and subfolders in work folder
                    var workingDir = m_MgrSettings.GetParam("WorkDir");
                    clsToolRunnerBase.CleanWorkDir(workingDir, 1, out _);
                }

                return false;
            }

            string errorMessage;
            try
            {
                var cleanupMgrErrors = new clsCleanupMgrErrors(
                    m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING),
                    m_MgrName,
                    m_MgrSettings.GetParam("WorkDir"),
                    m_StatusFile);

                var cleanupSuccess = cleanupMgrErrors.AutoCleanupManagerErrors(cleanupMode);

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

        private void UpdateLogLevel(IMgrParams mgrSettings)
        {
            try
            {
                // LogLevel is 1 to 5: 1 for Fatal errors only, 4 for Fatal, Error, Warning, and Info, and 5 for everything including Debug messages
                var debugLevel = mgrSettings.GetParam("DebugLevel", 4);

                m_DebugLevel = (BaseLogger.LogLevels)debugLevel;

                LogTools.SetFileLogLevel(m_DebugLevel);
            }
            catch (Exception ex)
            {
                LogError("Could not convert manager parameter debugLevel to enum BaseLogger.LogLevels", ex);
            }

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
                UpdateLogLevel(m_MgrSettings);
            }

            return bSuccess;
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
        /// Extract the value DefaultDMSConnString from CaptureTaskManager.exe.config
        /// </summary>
        /// <returns></returns>
        private string GetXmlConfigDefaultConnectionString()
        {
            return GetXmlConfigFileSetting(clsMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING);
        }

        /// <summary>
        /// Extract the value for the given setting from CaptureTaskManager.exe.config
        /// </summary>
        /// <returns>Setting value if found, otherwise an empty string</returns>
        /// <remarks>Uses a simple text reader in case the file has malformed XML</remarks>
        private string GetXmlConfigFileSetting(string settingName)
        {

            if (string.IsNullOrWhiteSpace(settingName))
                throw new ArgumentException("Setting name cannot be blank", nameof(settingName));

            try
            {
                var configFilePath = Path.Combine(m_MgrDirectoryPath, m_MgrExeName + ".config");
                var configFile = new FileInfo(configFilePath);

                if (!configFile.Exists)
                {
                    LogError("File not found: " + configFilePath);
                    return string.Empty;
                }

                var configXml = new StringBuilder();

                // Open CaptureTaskManager.exe.config using a simple text reader in case the file has malformed XML

                ShowTrace(string.Format("Extracting setting {0} from {1}", settingName, configFile.FullName));

                using (var reader = new StreamReader(new FileStream(configFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        configXml.Append(dataLine);
                    }
                }

                var matcher = new Regex(settingName + ".+?<value>(?<ConnString>.+?)</value>", RegexOptions.IgnoreCase);

                var match = matcher.Match(configXml.ToString());

                if (match.Success)
                    return match.Groups["ConnString"].Value;

                LogError(settingName + " setting not found in " + configFilePath);
                return string.Empty;
            }
            catch (Exception ex)
            {
                LogError("Exception reading setting " + settingName + " in CaptureTaskManager.exe.config", ex);
                return string.Empty;
            }

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

                if (stepToolLCase.Contains("ArchiveUpdate".ToLower()) ||
                    stepToolLCase.Contains("DatasetArchive".ToLower()) ||
                    stepToolLCase.Contains("SourceFileRename".ToLower()))
                {
                    // We don't need to validate free space with these step tools
                    return true;
                }

                datasetStoragePath = GetStoragePathBase();

                var targetFilePath = Path.Combine(datasetStoragePath, "DummyFile.txt");

                var success = clsDiskInfo.GetDiskFreeSpace(
                    targetFilePath, out var totalNumberOfFreeBytes, out var errorMessage, reportFreeSpaceAvailableToUser: false);

                if (!string.IsNullOrWhiteSpace(errorMessage))
                {
                    LogWarning("clsDiskInfo.GetDiskFreeSpace: " + errorMessage);
                }

                if (success)
                {
                    var freeSpaceGB = clsUtilities.BytesToGB(totalNumberOfFreeBytes);

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
                             " (GetDiskFreeSpace returned false)";

                    if (Environment.MachineName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase))
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