//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMWin;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

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

        private clsCaptureTaskMgrSettings mMgrSettings;

        private readonly string mMgrExeName;
        private readonly string mMgrDirectoryPath;

        private clsCaptureTask mTask;
        private FileSystemWatcher mFileWatcher;
        private IToolRunner mCapTool;
        private bool mConfigChanged;
        private int mTaskRequestErrorCount;
        private IStatusFile mStatusFile;

        private clsMessageHandler mMsgHandler;
        private bool mMsgQueueInitSuccess;

        private LoopExitCode mLoopExitCode;

        private string mMgrName = "Unknown";
        private string mStepTool = "Unknown";
        private string mJob = "Unknown";
        private string mDataset = "Unknown";

        /// <summary>
        /// DebugLevel of 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
        /// </summary>
        private BaseLogger.LogLevels mDebugLevel = BaseLogger.LogLevels.DEBUG;

        private bool mRunning;
        private System.Timers.Timer mStatusTimer;

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

            var exeInfo = new FileInfo(PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath());
            mMgrExeName = exeInfo.Name;
            mMgrDirectoryPath = exeInfo.DirectoryName;
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
                    if (mMsgQueueInitSuccess)
                    {
                        mMsgHandler.Dispose();
                    }
                    break;

                case LoopExitCode.DisabledMC:
                    // Manager is disabled via manager control db
                    LogMessage("Manager disabled in manager control DB");

                    mStatusFile.UpdateDisabled(false);
                    restartOK = false;
                    break;

                case LoopExitCode.DisabledLocally:
                    // Manager disabled locally
                    LogMessage("Manager disabled locally");

                    mStatusFile.UpdateDisabled(true);
                    restartOK = false;
                    break;

                case LoopExitCode.ExcessiveRequestErrors:
                    // Too many errors
                    LogError("Excessive errors requesting task; closing manager");

                    // Do not create a flag file; intermittent network connectivity is likely resulting in failure to request a task
                    // This will likely clear up eventually

                    mStatusFile.UpdateStopped(true);

                    restartOK = false;
                    break;

                case LoopExitCode.InvalidWorkDir:
                    // Working directory not valid
                    LogError("Working directory problem, disabling manager");

                    mStatusFile.CreateStatusFlagFile();
                    mStatusFile.UpdateStopped(true);

                    restartOK = false;
                    break;

                case LoopExitCode.NoTaskFound:
                    // No capture task found
                    LogDebug("No capture tasks found for " + mMgrName);

                    mStatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.ShutdownCmdReceived:
                    // Shutdown command received
                    LogMessage("Shutdown command received, closing manager");

                    mStatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.ExceededMaxTaskCount:
                    // Max number of consecutive jobs reached
                    LogMessage("Exceeded maximum job count, closing manager");

                    mStatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.UpdateRequired:
                    // Manager update required
                    LogMessage("Manager update is required, closing manager");

                    mMgrSettings.AckManagerUpdateRequired();
                    mStatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                case LoopExitCode.FlagFile:
                    // Flag file is present
                    LogError("Flag file exists - unable to continue analysis");

                    mStatusFile.UpdateStopped(true);
                    restartOK = false;
                    break;

                case LoopExitCode.NeedToAbortProcessing:
                    // Step tool set flag NeedToAbortProcessing to true
                    LogMessage("NeedToAbortProcessing = true, closing manager");

                    mStatusFile.UpdateStopped(false);
                    restartOK = false;
                    break;

                default:
                    throw new Exception("Unrecognized enum in EvaluateLoopExitCode: " + eLoopExitCode);
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

                try
                {
                    mMgrSettings = new clsCaptureTaskMgrSettings(TraceMode);

                    // Load settings from config file CaptureTaskManager.exe.config
                    var configFileSettings = LoadMgrSettingsFromFile();

                    var settingsClass = mMgrSettings;
                    if (settingsClass != null)
                    {
                        RegisterEvents(settingsClass);
                        settingsClass.CriticalErrorEvent += CriticalErrorEvent;
                    }

                    var success = mMgrSettings.LoadSettings(configFileSettings);
                    if (!success)
                    {
                        if (!string.IsNullOrEmpty(mMgrSettings.ErrMsg))
                        {
                            throw new ApplicationException("Unable to initialize manager settings class: " + mMgrSettings.ErrMsg);
                        }

                        throw new ApplicationException("Unable to initialize manager settings class: unknown error");
                    }

                    ShowTrace("Initialized MgrParams");

                }
                catch (Exception ex)
                {
                    ConsoleMsgUtils.ShowError("Exception instantiating clsCaptureTaskMgrSettings", ex);
                    ConsoleMsgUtils.SleepSeconds(0.5);
                    return false;
                }


            }
            catch (Exception ex)
            {
                if (string.Equals(ex.Message, MgrSettings.DEACTIVATED_LOCALLY))
                {
                    // Manager is deactivated locally
                }
                else
                {
                    ConsoleMsgUtils.ShowError("Exception loading settings from CaptureTaskManager.exe.config", ex);
                    ConsoleMsgUtils.SleepSeconds(0.5);
                }

                return false;
            }

            // Update the cached values for this manager and job
            mMgrName = mMgrSettings.ManagerName;
            ShowTrace("Manager name is " + mMgrName);

            mStepTool = "Unknown";
            mJob = "Unknown";
            mDataset = "Unknown";

            // Setup the loggers
            var logFileNameBase = mMgrSettings.GetParam("LogFileName", "CapTaskMan");

            UpdateLogLevel(mMgrSettings);

            LogTools.CreateFileLogger(logFileNameBase, mDebugLevel);

            // Give the file logger a chance to zip old log files by year
            FileLogger.ArchiveOldLogFilesNow();

            var logCnStr = mMgrSettings.GetParam("ConnectionString");

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            LogTools.CreateDbLogger(logCnStr, "CaptureTaskMan: " + mMgrName, TraceMode && ENABLE_LOGGER_TRACE_MODE);

            // Make the initial log entry
            var relativeLogFilePath = LogTools.CurrentLogFilePath;
            var logFile = new FileInfo(relativeLogFilePath);
            ShowTrace("Initializing log file " + PathUtils.CompactPathString(logFile.FullName, 60));

            var entryAssembly = Assembly.GetEntryAssembly();
            string startupMsg;

            if (entryAssembly == null)
            {
                startupMsg = "=== Started Capture Task Manager (unknown version) ===== ";
            }
            else
            {
                var appVersion = entryAssembly.GetName().Version;
                startupMsg = "=== Started Capture Task Manager V" + appVersion + " ===== ";
            }

            LogMessage(startupMsg);

            var configFileName = mMgrSettings.GetParam("ConfigFileName");
            if (string.IsNullOrEmpty(configFileName))
            {
                // Manager parameter error; log an error and exit
                LogError("Manager parameter 'ConfigFileName' is undefined; this likely indicates a problem retrieving manager parameters. " +
                         "Shutting down the manager");
                return false;
            }

            // Setup a file watcher for the config file

            mFileWatcher = new FileSystemWatcher
            {
                Path = mMgrDirectoryPath,
                IncludeSubdirectories = false,
                Filter = configFileName,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };

            // Subscribe to the file watcher Changed event
            mFileWatcher.Changed += FileWatcherChanged;

            // Make sure that the manager name matches the machine name (with a few exceptions)
            if (!hostName.StartsWith("EMSLMQ", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("EMSLPub", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("WE27676", StringComparison.OrdinalIgnoreCase))
            {
                if (!mMgrName.StartsWith(hostName, StringComparison.OrdinalIgnoreCase))
                {
                    LogError("Manager name does not match the host name: " + mMgrName + " vs. " + hostName + "; update " + configFileName);
                    return false;
                }
            }

            // Setup the message queue
            mMsgQueueInitSuccess = false;
            mMsgHandler = new clsMessageHandler
            {
                BrokerUri = mMgrSettings.GetParam("MessageQueueURI"),
                StatusTopicName = mMgrSettings.GetParam("MessageQueueTopicMgrStatus"),    // Typically "Manager.Status"
                MgrSettings = mMgrSettings
            };

            // Initialize the message queue
            // Start this in a separate thread so that we can abort the initialization if necessary
            InitializeMessageQueue();

            // Set up the tool for getting tasks
            mTask = new clsCaptureTask(mMgrSettings)
            {
                TraceMode = TraceMode
            };

            // Set up the status file class
            if (string.IsNullOrWhiteSpace(mMgrDirectoryPath))
            {
                LogError("Error determining the parent path for the executable, " + mMgrExeName);
                return false;
            }

            var statusFileNameLoc = Path.Combine(mMgrDirectoryPath, "Status.xml");
            mStatusFile = new clsStatusFile(statusFileNameLoc)
            {
                MgrName = mMgrName,
                MgrStatus = EnumMgrStatus.Running
            };

            RegisterEvents((EventNotifier)mStatusFile);

            mStatusFile.MonitorUpdateRequired += OnStatusMonitorUpdateReceived;

            var logStatusToMessageQueue = mMgrSettings.GetParam("LogStatusToMessageQueue", false);
            var messageQueueUri = mMgrSettings.GetParam("MessageQueueURI");
            var messageQueueTopicMgrStatus = mMgrSettings.GetParam("MessageQueueTopicMgrStatus");

            mStatusFile.ConfigureMessageQueueLogging(logStatusToMessageQueue, messageQueueUri, messageQueueTopicMgrStatus);

            mStatusFile.WriteStatusFile();

            // Set up the status reporting time, with an interval of 1 minute
            mStatusTimer = new System.Timers.Timer
            {
                Enabled = false,
                Interval = 60 * 1000
            };
            mStatusTimer.Elapsed += StatusTimer_Elapsed;

            // Get the most recent job history
            var historyFilePath = Path.Combine(mMgrSettings.GetParam("ApplicationPath"), "History.txt");
            if (!File.Exists(historyFilePath))
            {
                return true;
            }

            try
            {
                // Read the History.txt file
                using (var reader = new StreamReader(new FileStream(historyFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.Contains("RecentJob: "))
                        {
                            var tmpStr = dataLine.Replace("RecentJob: ", "");
                            mStatusFile.MostRecentJobInfo = tmpStr;
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
                mMsgQueueInitSuccess = false;
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
            if (!mMsgHandler.Init())
            {
                // Most error messages provided by .Init method, but debug message is here for program tracking
                LogDebug("Message handler init error");
                mMsgQueueInitSuccess = false;
                ShowTrace("mMsgQueueInitSuccess = false: Message handler init error");
            }
            else
            {
                LogDebug("Message handler initialized");
                mMsgQueueInitSuccess = true;
                ShowTrace("mMsgQueueInitSuccess = true");
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

        private Dictionary<string, string> LoadMgrSettingsFromFile()
        {
            // Note: When you are editing this project using the Visual Studio IDE, if you edit the values
            //  ->Properties>Settings.settings, when you run the program (from within the IDE), it
            //  will update file CaptureTaskManager.exe.config with your settings

            // Construct the path to the config document
            var configFilePath = Path.Combine(mMgrDirectoryPath, mMgrExeName + ".config");

            var mgrSettings = mMgrSettings.LoadMgrSettingsFromFile(configFilePath);

            // Manager Config DB connection string
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING, Properties.Settings.Default.MgrCnfgDbConnectStr);
            }

            // Manager active flag
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, "False");
            }

            // Manager name
            // The manager name may contain $ComputerName$
            // If it does, InitializeMgrSettings in MgrSettings will replace "$ComputerName$ with the local host name
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_MGR_NAME))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_MGR_NAME, "LoadMgrSettingsFromFile__Undefined_manager_name");
            }

            // Default settings in use flag
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_USING_DEFAULTS))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_USING_DEFAULTS, Properties.Settings.Default.UsingDefaults.ToString());
            }

            // Default connection string for logging errors to the database
            // Will get updated later when manager settings are loaded from the manager control database
            if (!mgrSettings.ContainsKey(clsCaptureTaskMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING))
            {
                mgrSettings.Add(clsCaptureTaskMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING, Properties.Settings.Default.DefaultDMSConnString);
            }

            if (TraceMode)
            {
                ShowTrace("Settings loaded from " + PathUtils.CompactPathString(configFilePath, 60));
                MgrSettings.ShowDictionaryTrace(mgrSettings);
            }

            return mgrSettings;
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
                    ProgRunner.SleepMilliseconds(150);
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

            var lastConfigDBUpdate = DateTime.UtcNow;

            mRunning = true;

            var clearWorkDirectory = true;

            // Begin main execution loop
            while (mRunning)
            {
                try
                {
                    // Verify that an error hasn't left the the system in an odd state
                    if (StatusFlagFileError(clearWorkDirectory))
                    {
                        mLoopExitCode = LoopExitCode.FlagFile;
                        break;
                    }

                    clearWorkDirectory = false;

                    // Check for configuration change
                    // This variable will be true if the CaptureTaskManager.exe.config file has been updated
                    if (mConfigChanged)
                    {
                        // Local config file has changed
                        mLoopExitCode = LoopExitCode.ConfigChanged;
                        break;
                    }

                    // Reload the manager control DB settings in case they have changed
                    // However, only reload every 2 minutes
                    if (!ReloadManagerSettings(ref lastConfigDBUpdate, 2))
                    {
                        // Error updating manager settings
                        mLoopExitCode = LoopExitCode.UpdateRequired;
                        break;
                    }

                    // Check to see if manager is still active
                    if (!mMgrSettings.GetParam("MgrActive", false))
                    {
                        // Disabled via manager control db
                        mLoopExitCode = LoopExitCode.DisabledMC;
                        break;
                    }

                    if (!mMgrSettings.GetParam(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, false))
                    {
                        mLoopExitCode = LoopExitCode.DisabledLocally;
                        break;
                    }

                    if (mMgrSettings.GetParam("ManagerUpdateRequired", false))
                    {
                        mLoopExitCode = LoopExitCode.UpdateRequired;
                        break;
                    }

                    // Check for excessive number of errors
                    if (mTaskRequestErrorCount > MAX_ERROR_COUNT)
                    {
                        mLoopExitCode = LoopExitCode.ExcessiveRequestErrors;
                        break;
                    }

                    // Check working directory
                    if (!ValidateWorkingDir())
                    {
                        mLoopExitCode = LoopExitCode.InvalidWorkDir;
                        break;
                    }

                    // Check whether the computer is likely to install the monthly Windows Updates within the next few hours
                    // Do not request a task between 12 am and 6 am on Thursday in the week with the third Tuesday of the month
                    // Do not request a task between 2 am and 4 am or between 9 am and 11 am on Sunday following the week with the second Tuesday of the month
                    if (WindowsUpdateStatus.UpdatesArePending(out var pendingWindowsUpdateMessage))
                    {
                        LogMessage(pendingWindowsUpdateMessage);
                        mLoopExitCode = LoopExitCode.NoTaskFound;
                        break;
                    }

                    // Delete temp files between 1:00 am and 1:30 am, or after every 50 tasks
                    if (taskCount == 1 && DateTime.Now.Hour == 1 && DateTime.Now.Minute < 30 || taskCount % 50 == 0)
                    {
                        RemoveOldTempFiles();
                    }

                    // Attempt to get a capture task
                    var taskReturn = mTask.RequestTask();
                    switch (taskReturn)
                    {
                        case EnumRequestTaskResult.NoTaskFound:
                            mRunning = false;
                            mLoopExitCode = LoopExitCode.NoTaskFound;
                            break;

                        case EnumRequestTaskResult.ResultError:
                            // Problem with task request; Errors are logged by request method
                            mTaskRequestErrorCount++;
                            break;

                        case EnumRequestTaskResult.TaskFound:

                            ShowTrace("Task found for " + mMgrName);

                            PerformTask(out var eTaskCloseout);

                            // Increment and test the task counter
                            taskCount++;
                            if (taskCount > mMgrSettings.GetParam("MaxRepetitions", 1))
                            {
                                mRunning = false;
                                mLoopExitCode = LoopExitCode.ExceededMaxTaskCount;
                            }

                            if (eTaskCloseout == EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING)
                            {
                                mRunning = false;
                                mLoopExitCode = LoopExitCode.NeedToAbortProcessing;
                            }

                            break;

                        default:
                            throw new Exception("Unrecognized enum in PerformMainLoop: " + taskReturn);
                    }
                }
                catch (Exception ex)
                {
                    LogError("Error in PerformMainLoop", ex);
                }
            } // End while

            mRunning = false;

            // Write the recent job history file
            try
            {
                var historyFile = Path.Combine(mMgrSettings.GetParam("ApplicationPath"), "History.txt");

                using (var sw = new StreamWriter(historyFile, false))
                {
                    sw.WriteLine("RecentJob: " + mStatusFile.MostRecentJobInfo);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception writing job history file", ex);
            }

            // Evaluate the loop exit code
            var restartOK = EvaluateLoopExitCode(mLoopExitCode);

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
                mStepTool = mTask.GetParam("StepTool");
                mJob = mTask.GetParam("Job");
                mDataset = mTask.GetParam("Dataset");
                var stepNumber = mTask.GetParam("Step");

                LogDebug("Job " + mJob + ", step " + stepNumber + " assigned");

                // Update the status
                mStatusFile.JobNumber = int.Parse(mJob);
                mStatusFile.Dataset = mDataset;
                mStatusFile.MgrStatus = EnumMgrStatus.Running;
                mStatusFile.Tool = mStepTool;
                mStatusFile.TaskStartTime = DateTime.UtcNow;
                mStatusFile.TaskStatus = EnumTaskStatus.Running;
                mStatusFile.TaskStatusDetail = EnumTaskStatusDetail.Running_Tool;
                mStatusFile.MostRecentJobInfo = DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss tt") +
                                                 ", Job " + mJob + ", Step " + stepNumber +
                                                 ", Tool " + mStepTool;

                mStatusFile.WriteStatusFile();

                // Create the tool runner object
                if (!SetToolRunnerObject(mStepTool))
                {
                    var errMsg = "Unable to SetToolRunnerObject";
                    LogError(mMgrName + ": " + errMsg + ", job " + mJob + ", Dataset " + mDataset, true);

                    mTask.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, errMsg);

                    mStatusFile.UpdateIdle();
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
                    mTask.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, diskSpaceMsg);
                    mStatusFile.UpdateIdle();
                    return;
                }

                // Run the tool plugin
                mStatusTimer.Enabled = true;
                var toolResult = mCapTool.RunTool();
                mStatusTimer.Enabled = false;

                eTaskCloseout = toolResult.CloseoutType;
                string closeoutMessage;

                switch (eTaskCloseout)
                {
                    case EnumCloseOutType.CLOSEOUT_FAILED:
                        LogError(mMgrName + ": Failure running tool " + mStepTool + ", job " + mJob + ", Dataset " + mDataset);

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                            closeoutMessage = toolResult.CloseoutMsg;
                        else
                            closeoutMessage = "Failure running tool " + mStepTool;

                        mTask.CloseTask(eTaskCloseout, closeoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NOT_READY:
                        string msg;
                        if (mStepTool == "ArchiveVerify" || mStepTool == "ArchiveStatusCheck")
                        {
                            msg = "Dataset not ready, tool " + mStepTool + ", job " + mJob + ": " + toolResult.CloseoutMsg;
                        }
                        else
                        {
                            msg = "Dataset not ready, tool " + mStepTool + ", job " + mJob + ", Dataset " + mDataset;
                        }

                        LogWarning(msg);

                        closeoutMessage = "Dataset not ready";

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                            closeoutMessage += ": " + toolResult.CloseoutMsg;

                        mTask.CloseTask(eTaskCloseout, closeoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_SUCCESS:
                        LogDebug(mMgrName + ": Step complete, tool " + mStepTool + ", job " + mJob + ", Dataset " + mDataset);

                        mTask.CloseTask(eTaskCloseout, toolResult.CloseoutMsg, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING:
                        LogError(mMgrName + ": Failure running tool " + mStepTool
                              + ", job " + mJob + ", Dataset " + mDataset
                              + "; CloseOut = NeedToAbortProcessing");

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                            closeoutMessage = toolResult.CloseoutMsg;
                        else
                            closeoutMessage = "Error: NeedToAbortProcessing";

                        mTask.CloseTask(eTaskCloseout, closeoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    default:
                        throw new Exception("Unrecognized enum in PerformTask: " + eTaskCloseout);
                }

                if (toolResult.CloseoutMsg.Contains(clsToolRunnerBase.EXCEPTION_CREATING_OUTPUT_DIRECTORY))
                {
                    if (eTaskCloseout != EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING ||
                        System.Net.Dns.GetHostName().StartsWith("monroe", StringComparison.OrdinalIgnoreCase))
                    {
                        LogWarning("Exiting the main loop since this user cannot write to the output directory");
                        eTaskCloseout = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;

                        ConsoleMsgUtils.SleepSeconds(3);
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error running task", ex);

                LogError(string.Format("{0}: Failure running tool {1}, job {2}, Dataset {3}; CloseOut = Exception",
                                       mMgrName, mStepTool, mJob, mDataset),
                         true);

                LogError("Tool runner exception", ex);
                mTask.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED, "Exception: " + ex.Message, EnumEvalCode.EVAL_CODE_FAILED,
                                 "Exception running tool");
            }

            // Update the status
            mStatusFile.ClearCachedInfo();

            mStatusFile.MgrStatus = EnumMgrStatus.Running;
            mStatusFile.TaskStatus = EnumTaskStatus.No_Task;
            mStatusFile.TaskStatusDetail = EnumTaskStatusDetail.No_Task;
            mStatusFile.WriteStatusFile();
        }

        /// <summary>
        /// Look for and remove old .tmp and .zip files
        /// </summary>
        protected void RemoveOldTempFiles()
        {
            // Remove .tmp and .zip files over 12 hours old in the Windows Temp folder
            const int agedTempFilesHours = 12;
            var tempFolderPath = Path.GetTempPath();
            RemoveOldTempFiles(agedTempFilesHours, tempFolderPath);
        }

        protected void RemoveOldTempFiles(int agedTempFilesHours, string tempFolderPath)
        {
            // This list tracks the file specs to search for in folder tempFolderPath
            var searchSpecs = new List<string>
            {
                "*.tmp",
                "*.zip"
            };

            RemoveOldTempFiles(agedTempFilesHours, tempFolderPath, searchSpecs);
        }

        /// <summary>
        /// Look for and remove files
        /// </summary>
        /// <param name="agedTempFilesHours">Files more than this many hours old will be deleted</param>
        /// <param name="tempFolderPath">Path to the folder to look for and delete old files</param>
        /// <param name="searchSpecs">File specs to search for in folder tempFolderPath, e.g. "*.txt"</param>
        protected void RemoveOldTempFiles(int agedTempFilesHours, string tempFolderPath, List<string> searchSpecs)
        {
            try
            {
                var totalDeleted = 0;

                if (agedTempFilesHours < 2)
                    agedTempFilesHours = 2;

                var diFolder = new DirectoryInfo(tempFolderPath);
                if (!diFolder.Exists)
                {
                    LogWarning("Folder not found: " + tempFolderPath);
                    return;
                }

                // Process each entry in searchSpecs
                foreach (var spec in searchSpecs)
                {
                    var deleteCount = 0;
                    foreach (var fiFile in diFolder.GetFiles(spec))
                    {
                        try
                        {
                            if (DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours > agedTempFilesHours)
                            {
                                fiFile.Delete();
                                deleteCount += 1;
                            }
                        }
                        catch
                        {
                            // Ignore exceptions
                        }
                    }

                    totalDeleted += deleteCount;
                }

                if (totalDeleted > 0)
                {
                    var msg = "Deleted " + totalDeleted + " temp file";
                    if (totalDeleted > 1)
                        msg += "s";

                    msg += " over " + agedTempFilesHours + " hours old in folder " + tempFolderPath;
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
            mCapTool = clsPluginLoader.GetToolRunner(stepToolName);
            if (mCapTool == null)
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
    mTask.AddAdditionalParameter("MyEMSLOffline", "true");
    LogMessage("Adding job parameter MyEMSLOffline=true");
#endif

#if MyEMSL_TEST_TAR
    mTask.AddAdditionalParameter("DebugTestTar", "true");
    LogMessage("Adding job parameter DebugTestTar=true");
#endif
                if (TraceMode)
                {
                    mMgrSettings.SetParam("TraceMode", "True");
                }

                // Setup the new tool runner
                mCapTool.Setup(mMgrSettings, mTask, mStatusFile);
            }
            catch (Exception ex)
            {
                LogError("Exception calling CapTool.Setup", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// If TraceMode is True, display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        /// <param name="emptyLinesBeforeMessage"></param>
        private void ShowTrace(string message, int emptyLinesBeforeMessage= 1)
        {
            if (TraceMode)
                ShowTraceMessage(message, emptyLinesBeforeMessage);
        }

        /// <summary>
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        /// <param name="emptyLinesBeforeMessage"></param>
        public static void ShowTraceMessage(string message, int emptyLinesBeforeMessage = 1)
        {
            clsToolRunnerBase.ShowTraceMessage(message, false, emptyLinesBeforeMessage);
        }

        /// <summary>
        /// Looks for flag file; auto cleans if ManagerErrorCleanupMode is >= 1
        /// </summary>
        /// <returns>True if a flag file exists and it was not auto-cleaned; false if no problems</returns>
        private bool StatusFlagFileError(bool clearWorkDirectory)
        {
            var cleanupModeVal = mMgrSettings.GetParam("ManagerErrorCleanupMode", 0);
            clsCleanupMgrErrors.eCleanupModeConstants cleanupMode;

            if (Enum.IsDefined(typeof(clsCleanupMgrErrors.eCleanupModeConstants), cleanupModeVal))
            {
                cleanupMode = (clsCleanupMgrErrors.eCleanupModeConstants)cleanupModeVal;
            }
            else
            {
                cleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled;
            }

            if (!mStatusFile.DetectStatusFlagFile())
            {
                // No flag file

                if (clearWorkDirectory && cleanupMode == clsCleanupMgrErrors.eCleanupModeConstants.CleanupAlways)
                {
                    // Delete all files and subdirectories in the working directory (but ignore errors)
                    var workingDir = mMgrSettings.GetParam("WorkDir");
                    clsToolRunnerBase.CleanWorkDir(workingDir, 1, out _);
                }

                return false;
            }

            string errorMessage;
            try
            {
                var cleanupMgrErrors = new clsCleanupMgrErrors(
                    mMgrSettings.GetParam(MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING),
                    mMgrName,
                    mMgrSettings.GetParam("WorkDir"),
                    mStatusFile);

                var cleanupSuccess = cleanupMgrErrors.AutoCleanupManagerErrors(cleanupMode);

                if (cleanupSuccess)
                {
                    LogWarning("Flag file found; automatically cleaned the work directory and deleted the flag file(s)");

                    // No error; return false
                    return false;
                }

                var flagFile = new FileInfo(mStatusFile.FlagFilePath);
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

                mDebugLevel = (BaseLogger.LogLevels)debugLevel;

                LogTools.SetFileLogLevel(mDebugLevel);
            }
            catch (Exception ex)
            {
                LogError("Could not convert manager parameter debugLevel to enum BaseLogger.LogLevels", ex);
            }

        }

        /// <summary>
        /// Reloads the manager settings from the manager control database
        /// if at least minutesBetweenUpdates minutes have elapsed since the last update
        /// </summary>
        /// <param name="lastConfigDBUpdate"></param>
        /// <param name="minutesBetweenUpdates"></param>
        /// <returns></returns>
        private bool ReloadManagerSettings(ref DateTime lastConfigDBUpdate, double minutesBetweenUpdates)
        {
            if (!(DateTime.UtcNow.Subtract(lastConfigDBUpdate).TotalMinutes >= minutesBetweenUpdates))
            {
                return true;
            }

            lastConfigDBUpdate = DateTime.UtcNow;

            try
            {
                ShowTrace("Reading application config file");

                // Load settings from config file CaptureTaskManager.exe.config
                var configFileSettings = LoadMgrSettingsFromFile();

                if (configFileSettings == null)
                    return false;

                LogDebug("Updating manager settings using Manager Control database");

                // Store the new settings then retrieve updated settings from the database
                if (mMgrSettings.LoadSettings(configFileSettings, true))
                {
                    UpdateLogLevel(mMgrSettings);
                    return true;
                }

                if (!string.IsNullOrWhiteSpace(mMgrSettings.ErrMsg))
                {
                    // Log the error
                    LogMessage(mMgrSettings.ErrMsg);
                }
                else
                {
                    // Unknown problem reading config file
                    LogError("Error re-reading config file in ReloadManagerSettings");
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error re-loading manager settings", ex);
                return false;
            }
        }

        protected string GetStoragePathBase()
        {
            var storagePath = mTask.GetParam("Storage_Path");

            // Make sure storagePath only contains the root folder, not several folders
            // In other words, if storagePath = "VOrbiETD03\2011_4" change it to just "VOrbiETD03"
            var slashLoc = storagePath.IndexOf(Path.DirectorySeparatorChar);
            if (slashLoc > 0)
                storagePath = storagePath.Substring(0, slashLoc);

            // Always use the UNC path defined by Storage_Vol_External when checking drive free space
            // Example path is: \\Proto-7\
            var datasetStoragePathBase = mTask.GetParam("Storage_Vol_External");

            datasetStoragePathBase = Path.Combine(datasetStoragePathBase, storagePath);

            return datasetStoragePathBase;
        }

        /// <summary>
        /// Extract the value DefaultDMSConnString from CaptureTaskManager.exe.config
        /// </summary>
        /// <returns></returns>
        private string GetXmlConfigDefaultConnectionString()
        {
            return GetXmlConfigFileSetting(clsCaptureTaskMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING);
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
                var configFilePath = Path.Combine(mMgrDirectoryPath, mMgrExeName + ".config");
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
                var stepToolLCase = mStepTool.ToLower();

                if (stepToolLCase.Contains("ArchiveUpdate".ToLower()) ||
                    stepToolLCase.Contains("DatasetArchive".ToLower()) ||
                    stepToolLCase.Contains("SourceFileRename".ToLower()))
                {
                    // We don't need to validate free space with these step tools
                    return true;
                }

                datasetStoragePath = GetStoragePathBase();

                var targetFilePath = Path.Combine(datasetStoragePath, "DummyFile.txt");

                var success = DiskInfo.GetDiskFreeSpace(
                    targetFilePath, out var totalNumberOfFreeBytes, out var errorMessage, reportFreeSpaceAvailableToUser: false);

                if (!string.IsNullOrWhiteSpace(errorMessage))
                {
                    LogWarning("DiskInfo.GetDiskFreeSpace: " + errorMessage);
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

                    if (System.Net.Dns.GetHostName().StartsWith("monroe", StringComparison.OrdinalIgnoreCase))
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
            var workingDir = mMgrSettings.GetParam("WorkDir");

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
                mMgrSettings.SetParam("WorkDir", alternateWorkDir);

                LogWarning("Invalid working directory: " + workingDir + "; automatically switched to " + alternateWorkDir);
                return true;
            }

            LogError("Invalid working directory: " + workingDir, true);
            return false;
        }

        #endregion

        #region "EventNotifier events"

        private new void RegisterEvents(EventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            base.RegisterEvents(processingClass, writeDebugEventsToLog);

            processingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private void CriticalErrorEvent(string message, Exception ex)
        {
            LogError(message, true);
        }

        private void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            mStatusFile.CurrentOperation = progressMessage;
            mStatusFile.UpdateAndWrite(percentComplete);
        }

        #endregion

        #region "Event handlers"

        private void FileWatcherChanged(object sender, FileSystemEventArgs e)
        {
            LogDebug("clsMainProgram.FileWatcherChanged event received");

            mConfigChanged = true;
            mFileWatcher.EnableRaisingEvents = false;
        }

        void OnStatusMonitorUpdateReceived(string msg)
        {
            if (mMsgQueueInitSuccess)
                mMsgHandler.SendMessage(msg);
        }

        /// <summary>
        /// Updates the status at mStatusTimer interval
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void StatusTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            mStatusFile.WriteStatusFile();
        }

        #endregion
    }
}