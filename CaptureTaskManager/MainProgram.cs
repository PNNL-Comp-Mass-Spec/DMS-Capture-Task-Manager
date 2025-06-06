﻿//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMDatabaseUtils;
using PRISMDatabaseUtils.Logging;
using PRISMWin;

namespace CaptureTaskManager
{
    /// <summary>
    /// Capture Task Manager main program execution loop
    /// </summary>
    public class MainProgram : LoggerBase
    {
        // Ignore Spelling: yyyy-MM-dd hh:mm:ss tt, Unsubscribe

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

        private const int MAX_ERROR_COUNT = 4;

        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        private const string DEFAULT_BASE_LOGFILE_NAME = @"Logs\CapTaskMan";

        private const bool ENABLE_LOGGER_TRACE_MODE = false;

        private CaptureTaskMgrSettings mMgrSettings;

        private readonly string mMgrExeName;
        private readonly string mMgrDirectoryPath;

        private CaptureTask mTask;
        private FileSystemWatcher mFileWatcher;
        private IToolRunner mCapTool;
        private bool mConfigChanged;
        private int mTaskRequestErrorCount;
        private StatusFile mStatusFile;

        private MessageSender mMessageSender;

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

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MainProgram(bool traceMode)
        {
            TraceMode = traceMode;

            var exeInfo = new FileInfo(AppUtils.GetAppPath());
            mMgrExeName = exeInfo.Name;
            mMgrDirectoryPath = exeInfo.DirectoryName;
        }

        /// <summary>
        /// Initializes the database logger in static class PRISM.Logging.LogTools
        /// </summary>
        /// <remarks>Supports both SQL Server and Postgres connection strings</remarks>
        /// <param name="connectionString">Database connection string</param>
        /// <param name="moduleName">Module name used by logger</param>
        /// <param name="traceMode">When true, show additional debug messages at the console</param>
        /// <param name="logLevel">Log threshold level</param>
        private static void CreateDbLogger(
            string connectionString,
            string moduleName,
            bool traceMode = false,
            BaseLogger.LogLevels logLevel = BaseLogger.LogLevels.INFO)
        {
            var databaseType = DbToolsFactory.GetServerTypeFromConnectionString(connectionString);

            DatabaseLogger dbLogger = databaseType switch
            {
                DbServerTypes.MSSQLServer => new PRISMDatabaseUtils.Logging.SQLServerDatabaseLogger(),
                DbServerTypes.PostgreSQL => new PostgresDatabaseLogger(),
                _ => throw new Exception("Unsupported database connection string: should be SQL Server or Postgres")
            };

            dbLogger.ChangeConnectionInfo(moduleName, connectionString);

            LogTools.SetDbLogger(dbLogger, logLevel, traceMode);
        }

        /// <summary>
        /// Evaluates the LoopExitCode to determine whether the manager can request another task
        /// </summary>
        /// <param name="loopExitCode"></param>
        /// <returns>True if OK to request another task</returns>
        private bool EvaluateLoopExitCode(LoopExitCode loopExitCode)
        {
            var restartOK = true;

            // Determine cause of loop exit and respond accordingly
            switch (loopExitCode)
            {
                case LoopExitCode.ConfigChanged:
                    // Reload the manager config
                    LogMessage("Reloading configuration and restarting manager");

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
                    throw new Exception("Unrecognized enum in EvaluateLoopExitCode: " + loopExitCode);
            }

            return restartOK;
        }

        private string GetStoragePathBase()
        {
            var storagePath = mTask.GetParam("Storage_Path");

            // Make sure storagePath only contains the root folder, not several folders
            // In other words, if storagePath = "VOrbiETD03\2011_4" change it to just "VOrbiETD03"
            var slashLoc = storagePath.IndexOf(Path.DirectorySeparatorChar);

            if (slashLoc > 0)
            {
                storagePath = storagePath.Substring(0, slashLoc);
            }

            // Always use the UNC path defined by Storage_Vol_External when checking drive free space
            // Example path is: \\Proto-7\
            var storageVolume = mTask.GetParam("Storage_Vol_External");

            return Path.Combine(storageVolume, storagePath);
        }

        /// <summary>
        /// Extract the value DefaultDMSConnString from CaptureTaskManager.exe.db.config or CaptureTaskManager.exe.config
        /// </summary>
        /// <returns>DMS connection string</returns>
        private string GetXmlConfigDefaultConnectionString()
        {
            return GetXmlConfigFileSetting(CaptureTaskMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING);
        }

        /// <summary>
        /// Extract the value for the given setting from CaptureTaskManager.exe.config
        /// If the setting name is MgrCnfgDbConnectStr or DefaultDMSConnString, first checks file CaptureTaskManager.exe.db.config
        /// </summary>
        /// <remarks>Uses a simple text reader in case the file has malformed XML</remarks>
        /// <returns>Setting value if found, otherwise an empty string</returns>
        private string GetXmlConfigFileSetting(string settingName)
        {
            if (string.IsNullOrWhiteSpace(settingName))
            {
                throw new ArgumentException("Setting name cannot be blank", nameof(settingName));
            }

            var configFilePaths = new List<string>();

            if (settingName.Equals("MgrCnfgDbConnectStr", StringComparison.OrdinalIgnoreCase) ||
                settingName.Equals("DefaultDMSConnString", StringComparison.OrdinalIgnoreCase))
            {
                configFilePaths.Add(Path.Combine(mMgrDirectoryPath, mMgrExeName + ".db.config"));
            }

            configFilePaths.Add(Path.Combine(mMgrDirectoryPath, mMgrExeName + ".config"));

            var mgrSettings = new MgrSettings();
            RegisterEvents(mgrSettings);

            var valueFound = mgrSettings.GetXmlConfigFileSetting(configFilePaths, settingName, out var settingValue);

            if (valueFound)
            {
                return settingValue;
            }

            return string.Empty;
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

            // Create a database logger connected to the DMS database on prismdb2 (previously, DMS5 on Gigasax)

            // Once the initial parameters have been successfully read,
            // we update the dbLogger to use the connection string read from the Manager Control DB
            string defaultDmsConnectionString;

            // Open CaptureTaskManager.exe.config to look for setting DefaultDMSConnString, so we know which server to log to by default
            var dmsConnectionStringFromConfig = GetXmlConfigDefaultConnectionString();

            if (string.IsNullOrWhiteSpace(dmsConnectionStringFromConfig))
            {
                LogError("Did not find setting {0} in {1} or {2}",
                    CaptureTaskMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING,
                    "CaptureTaskManager.exe.db.config",
                    "CaptureTaskManager.exe.config");

                // Use the hard-coded default that points to the DMS database on prismdb2 (previously, DMS5 on Gigasax)
                defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;
            }
            else
            {
                // Use the connection string from CaptureTaskManager.exe.config
                defaultDmsConnectionString = dmsConnectionStringFromConfig;
            }

            var applicationDirectory = new DirectoryInfo(mMgrDirectoryPath);

            var defaultDbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(defaultDmsConnectionString, applicationDirectory.Name);

            ShowTrace("Instantiate a DbLogger using " + defaultDbLoggerConnectionString);

            CreateDbLogger(defaultDbLoggerConnectionString, "CaptureTaskMan: " + hostName, TraceMode && ENABLE_LOGGER_TRACE_MODE);

            LogTools.MessageLogged += MessageLoggedHandler;

            // Get the manager settings
            // If you get an exception here while debugging in Visual Studio, be sure
            //  that "UsingDefaults" is set to False in AppName.exe.config
            try
            {
                ShowTrace("Reading application config file");

                try
                {
                    mMgrSettings = new CaptureTaskMgrSettings(TraceMode);

                    // Load settings from config file CaptureTaskManager.exe.config
                    var configFileSettings = LoadMgrSettingsFromFile();

                    var settingsClass = mMgrSettings;

                    if (settingsClass != null)
                    {
                        RegisterEvents(settingsClass);
                        settingsClass.CriticalErrorEvent += CriticalErrorEvent;
                    }

                    // Store the loaded settings, then retrieve manager parameters from the database

                    Console.WriteLine();
                    mMgrSettings.ValidatePgPass(configFileSettings);

                    var success = mMgrSettings.LoadSettings(configFileSettings);

                    if (!success)
                    {
                        if (!string.IsNullOrEmpty(mMgrSettings.ErrMsg))
                        {
                            throw new ApplicationException("Unable to initialize manager settings class: " + mMgrSettings.ErrMsg);
                        }

                        throw new ApplicationException("Unable to initialize manager settings class: unknown error");
                    }

                    if (!mMgrSettings.HasParam("DMSConnectionString"))
                    {
                        LogError("Manager parameters loaded from the database are missing parameter {0}", "DMSConnectionString");
                        return false;
                    }

                    ShowTrace("Initialized MgrParams");
                }
                catch (Exception ex)
                {
                    ConsoleMsgUtils.ShowError("Exception instantiating CaptureTaskMgrSettings", ex);
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

            // This connection string points to the DMS database on prismdb2 (previously, DMS_Capture on Gigasax)
            var logCnStr = mMgrSettings.GetParam("ConnectionString");

            var dbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(logCnStr, mMgrName);

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            CreateDbLogger(dbLoggerConnectionString, "CaptureTaskMan: " + mMgrName, TraceMode && ENABLE_LOGGER_TRACE_MODE);

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

            // Set up a file watcher for the config file

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
                !hostName.StartsWith("WE31383", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("WE27676", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("WE43320", StringComparison.OrdinalIgnoreCase))
            {
                if (!mMgrName.StartsWith(hostName, StringComparison.OrdinalIgnoreCase))
                {
                    LogError("Manager name does not match the host name: " + mMgrName + " vs. " + hostName + "; update " + configFileName);
                    return false;
                }
            }

            var brokerUri = mMgrSettings.GetParam("MessageQueueURI");
            var topicName = mMgrSettings.GetParam("MessageQueueTopicMgrStatus");    // Typically "Manager.CapTask"

            // Setup the message queue
            mMessageSender = new MessageSender(brokerUri, topicName, mMgrSettings.ManagerName);

            RegisterEvents(mMessageSender);

            // Initialize the message queue
            // Start this in a separate thread so that we can abort the initialization if necessary
            InitializeMessageQueue();

            // Set up the tool for getting tasks
            mTask = new CaptureTask(mMgrSettings)
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
            mStatusFile = new StatusFile(statusFileNameLoc)
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
                using var reader = new StreamReader(new FileStream(historyFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    if (dataLine.Contains("RecentJob: "))
                    {
                        mStatusFile.MostRecentJobInfo = dataLine.Replace("RecentJob: ", string.Empty);
                        break;
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

            var waitStart = DateTime.UtcNow;

            // Wait a maximum of 60 seconds
            if (!worker.Join(MAX_WAIT_TIME_SECONDS * 1000))
            {
                worker.Abort();
                LogWarning("Unable to initialize the message queue (timeout after " + MAX_WAIT_TIME_SECONDS + " seconds)");
                return;
            }

            var elapsedTime = DateTime.UtcNow.Subtract(waitStart).TotalSeconds;

            if (elapsedTime > 25)
            {
                LogWarning("Connection to the message queue was slow, taking " + (int)elapsedTime + " seconds");
            }
        }

        private void InitializeMessageQueueWork()
        {
            if (!mMessageSender.CreateConnection())
            {
                // Most error messages provided by .Init method, but debug message is here for program tracking
                LogDebug("Message handler init error");
                ShowTrace("mMsgQueueInitSuccess = false: Message handler init error");
            }
            else
            {
                LogDebug("Message handler initialized");
                ShowTrace("mMsgQueueInitSuccess = true");
            }
        }

        // ReSharper disable once SuggestBaseTypeForParameter
        private static Dictionary<string, DateTime> LoadCachedLogMessages(FileInfo messageCacheFile)
        {
            var cachedMessages = new Dictionary<string, DateTime>();

            using var reader = new StreamReader(new FileStream(messageCacheFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var lineCount = 0;
            var splitChar = new[] { '\t' };

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();
                lineCount++;

                // Assume that the first line is the header line, which we'll skip
                if (lineCount == 1 || string.IsNullOrWhiteSpace(dataLine))
                {
                    continue;
                }

                var lineParts = dataLine.Split(splitChar, 2);

                var timeStampText = lineParts[0];
                var message = lineParts[1];

                if (DateTime.TryParse(timeStampText, out var timeStamp))
                {
                    // Valid message; store it

                    if (cachedMessages.TryGetValue(message, out var cachedTimeStamp))
                    {
                        if (timeStamp > cachedTimeStamp)
                        {
                            cachedMessages[message] = timeStamp;
                        }
                    }
                    else
                    {
                        cachedMessages.Add(message, timeStamp);
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
            if (!mgrSettings.ContainsKey(CaptureTaskMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING))
            {
                mgrSettings.Add(CaptureTaskMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING, Properties.Settings.Default.DefaultDMSConnString);
            }

            if (TraceMode)
            {
                ShowTrace("Settings loaded from " + PathUtils.CompactPathString(configFilePath, 60));
                MgrSettings.ShowDictionaryTrace(mgrSettings);
            }

            return mgrSettings;
        }

        private static void LogErrorToDatabasePeriodically(string errorMessage, int logIntervalHours)
        {
            const string PERIODIC_LOG_FILE = "Periodic_ErrorMessages.txt";

            try
            {
                Dictionary<string, DateTime> cachedMessages;

                var messageCacheFile = new FileInfo(Path.Combine(CTMUtilities.GetAppDirectoryPath(), PERIODIC_LOG_FILE));

                if (messageCacheFile.Exists)
                {
                    cachedMessages = LoadCachedLogMessages(messageCacheFile);
                    AppUtils.SleepMilliseconds(150);
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
                using var writer = new StreamWriter(new FileStream(messageCacheFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                writer.WriteLine("{0}\t{1}", "TimeStamp", "Message");

                foreach (var message in cachedMessages)
                {
                    writer.WriteLine("{0}\t{1}", message.Value.ToString(DATE_TIME_FORMAT), message.Key);
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
            StatusData.MostRecentLogMessage = timeStamp + "; " + message + "; " + logLevel;

            if (logLevel <= BaseLogger.LogLevels.ERROR)
            {
                StatusData.AddErrorMessage(timeStamp + "; " + message + "; " + logLevel);
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
                    // Verify that an error hasn't left the system in an odd state
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

                    // To enable a step-tool for debugging, use
                    // select * from cap.enable_disable_ctm_step_tool_for_debugging('DatasetArchive', true);

                    // If you are trying to debug a specific capture task, but the database does not assign the task to processor Monroe_CTM,
                    // make sure that the storage server that the dataset resides on is associated with the machine defined in t_local_processors
                    // select * from cap.t_local_processors where processor_name = 'Monroe_CTM';

                    var taskReturn = mTask.RequestTask();

                    switch (taskReturn)
                    {
                        case EnumRequestTaskResult.NoTaskFound:
                            mRunning = false;
                            mLoopExitCode = LoopExitCode.NoTaskFound;
                            break;

                        case EnumRequestTaskResult.ResultError:
                            // Problem with task request; Errors are logged by request method

                            // Close the task if the job number and step number are known
                            var job = mTask.GetParam("Job", 0);
                            var step = mTask.GetParam("Step", -1);

                            if (job > 0 && step >= 0)
                            {
                                mTask.CloseTask(EnumCloseOutType.CLOSEOUT_FAILED,
                                                "Error retrieving or parsing job parameters",
                                                EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY);
                            }

                            mTaskRequestErrorCount++;
                            break;

                        case EnumRequestTaskResult.TaskFound:

                            ShowTrace("Task found for " + mMgrName);

                            PerformTask(out var taskResult);

                            // Increment and test the task counter
                            taskCount++;

                            if (taskCount > mMgrSettings.GetParam("MaxRepetitions", 1))
                            {
                                mRunning = false;
                                mLoopExitCode = LoopExitCode.ExceededMaxTaskCount;
                            }

                            if (taskResult == EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING)
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

                    // Increment the error count and exit out of the loop
                    mTaskRequestErrorCount++;

                    mRunning = false;
                }
            }

            mRunning = false;

            // Write the recent job history file
            try
            {
                var historyFile = Path.Combine(mMgrSettings.GetParam("ApplicationPath"), "History.txt");

                using var writer = new StreamWriter(historyFile, false);

                writer.WriteLine("RecentJob: " + mStatusFile.MostRecentJobInfo);
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

            // Unsubscribe message handler events and close the message handler
            mMessageSender.Dispose();

            return restartOK;
        }

        private void PerformTask(out EnumCloseOutType taskResult)
        {
            taskResult = EnumCloseOutType.CLOSEOUT_NOT_READY;

            try
            {
                // Cache the job parameters
                mStepTool = mTask.GetParam("StepTool");
                mJob = mTask.GetParam("Job");
                mDataset = mTask.GetParam("Dataset");
                var stepNumber = mTask.GetParam("Step");

                LogMessage("Processing job {0}, step {1}; running {2} for {3}", mJob, stepNumber, mStepTool, mDataset);

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
                    const string errMsg = "Unable to set the tool runner object";
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

                taskResult = toolResult.CloseoutType;
                string closeoutMessage;

                switch (taskResult)
                {
                    case EnumCloseOutType.CLOSEOUT_FAILED:
                        LogError(mMgrName + ": Failure running tool " + mStepTool + ", job " + mJob + ", Dataset " + mDataset);

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                        {
                            closeoutMessage = toolResult.CloseoutMsg;
                        }
                        else
                        {
                            closeoutMessage = "Failure running tool " + mStepTool;
                        }

                        mTask.CloseTask(taskResult, closeoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NOT_READY:
                        string msg;

                        if (mStepTool is "ArchiveVerify" or "ArchiveStatusCheck")
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
                        {
                            closeoutMessage += ": " + toolResult.CloseoutMsg;
                        }

                        mTask.CloseTask(taskResult, closeoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_SUCCESS:
                        LogDebug(mMgrName + ": Step complete, tool " + mStepTool + ", job " + mJob + ", Dataset " + mDataset);

                        mTask.CloseTask(taskResult, toolResult.CloseoutMsg, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    case EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING:
                        LogError(mMgrName + ": Failure running tool " + mStepTool
                              + ", job " + mJob + ", Dataset " + mDataset
                              + "; CloseOut = NeedToAbortProcessing");

                        if (!string.IsNullOrEmpty(toolResult.CloseoutMsg))
                        {
                            closeoutMessage = toolResult.CloseoutMsg;
                        }
                        else
                        {
                            closeoutMessage = "Error: NeedToAbortProcessing";
                        }

                        mTask.CloseTask(taskResult, closeoutMessage, toolResult.EvalCode, toolResult.EvalMsg);
                        break;

                    default:
                        throw new Exception("Unrecognized enum in PerformTask: " + taskResult);
                }

                if (toolResult.CloseoutMsg.Contains(ToolRunnerBase.EXCEPTION_CREATING_OUTPUT_DIRECTORY))
                {
                    if (taskResult != EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING ||
                        System.Net.Dns.GetHostName().StartsWith("WE43320", StringComparison.OrdinalIgnoreCase))
                    {
                        LogWarning("Exiting the main loop since this user cannot write to the output directory");
                        taskResult = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;

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
        private static void RemoveOldTempFiles()
        {
            // Remove .tmp and .zip files over 12 hours old in the Windows Temp folder
            const int agedTempFilesHours = 12;
            var tempFolderPath = Path.GetTempPath();
            RemoveOldTempFiles(agedTempFilesHours, tempFolderPath);
        }

        private static void RemoveOldTempFiles(int agedTempFilesHours, string tempFolderPath)
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
        /// <param name="tempDirectoryPath">Path to the folder to look for and delete old files</param>
        /// <param name="searchSpecs">File specs to search for in folder tempDirectoryPath, e.g. "*.txt"</param>
        private static void RemoveOldTempFiles(int agedTempFilesHours, string tempDirectoryPath, List<string> searchSpecs)
        {
            try
            {
                var totalDeleted = 0;

                if (agedTempFilesHours < 2)
                {
                    agedTempFilesHours = 2;
                }

                var tempDirectory = new DirectoryInfo(tempDirectoryPath);

                if (!tempDirectory.Exists)
                {
                    LogWarning("Directory not found: " + tempDirectoryPath);
                    return;
                }

                // Process each entry in searchSpecs
                foreach (var spec in searchSpecs)
                {
                    var deleteCount = 0;

                    foreach (var file in tempDirectory.GetFiles(spec))
                    {
                        try
                        {
                            if (DateTime.UtcNow.Subtract(file.LastWriteTimeUtc).TotalHours > agedTempFilesHours)
                            {
                                file.Delete();
                                deleteCount++;
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
                    LogMessage("Deleted {0} temp file{1} over {2} hours old in directory {3}",
                        totalDeleted, totalDeleted > 1 ? "s" : string.Empty, agedTempFilesHours, tempDirectoryPath);
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
        /// <returns>True if successful, false if an error</returns>
        private bool SetToolRunnerObject(string stepToolName)
        {
            // Load the tool runner
            mCapTool = PluginLoader.GetToolRunner(stepToolName);

            if (mCapTool == null)
            {
                LogError("Unable to load tool runner for StepTool " + stepToolName + ": " + PluginLoader.ErrMsg);
                return false;
            }

            LogDebug("Loaded tool runner for Step Tool " + stepToolName);

            try
            {
#if MyEMSL_OFFLINE
    // When this Conditional Compilation Constant is defined, the DatasetArchive plugin will set debugMode
    // to Pacifica.Core.EasyHttp.DebugMode.MyEMSLOfflineMode when calling UploadToMyEMSLWithRetry()
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

                // Set up the new tool runner
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
            {
                ShowTraceMessage(message, emptyLinesBeforeMessage);
            }
        }

        /// <summary>
        /// Display a timestamp and message at the console
        /// </summary>
        /// <param name="message"></param>
        /// <param name="emptyLinesBeforeMessage"></param>
        public static void ShowTraceMessage(string message, int emptyLinesBeforeMessage = 1)
        {
            ToolRunnerBase.ShowTraceMessage(message, false, emptyLinesBeforeMessage);
        }

        /// <summary>
        /// Looks for flag file; auto cleans if ManagerErrorCleanupMode is >= 1
        /// </summary>
        /// <returns>True if a flag file exists, and it was not auto-cleaned; false if no problems</returns>
        private bool StatusFlagFileError(bool clearWorkDirectory)
        {
            var cleanupModeVal = mMgrSettings.GetParam("ManagerErrorCleanupMode", 0);
            CleanupMgrErrors.CleanupModeConstants cleanupMode;

            if (Enum.IsDefined(typeof(CleanupMgrErrors.CleanupModeConstants), cleanupModeVal))
            {
                cleanupMode = (CleanupMgrErrors.CleanupModeConstants)cleanupModeVal;
            }
            else
            {
                cleanupMode = CleanupMgrErrors.CleanupModeConstants.Disabled;
            }

            if (!mStatusFile.DetectStatusFlagFile())
            {
                // No flag file

                if (clearWorkDirectory && cleanupMode == CleanupMgrErrors.CleanupModeConstants.CleanupAlways)
                {
                    // Delete all files and subdirectories in the working directory (but ignore errors)
                    var workingDir = mMgrSettings.GetParam("WorkDir");
                    ToolRunnerBase.CleanWorkDir(workingDir, 1, out _);
                }

                return false;
            }

            string errorMessage;
            try
            {
                var connectionString = mMgrSettings.GetParam(MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING);

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

                var cleanupMgrErrors = new CleanupMgrErrors(
                    connectionStringToUse,
                    mMgrName,
                    mMgrSettings.GetParam("WorkDir"),
                    mStatusFile,
                    TraceMode);

                var cleanupSuccess = cleanupMgrErrors.AutoCleanupManagerErrors(cleanupMode);

                if (cleanupSuccess)
                {
                    LogWarning("Flag file found; automatically cleaned the work directory and deleted the flag file(s)");

                    // No error; return false
                    return false;
                }

                var flagFile = new FileInfo(mStatusFile.FlagFilePath);

                if (flagFile.Directory == null)
                {
                    errorMessage = "Flag file exists in the manager folder";
                }
                else
                {
                    errorMessage = "Flag file exists in folder " + flagFile.Directory.Name;
                }
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

        private void UpdateLogLevel(MgrSettings mgrSettings)
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
        /// <returns>True if successful, false if an error</returns>
        private bool ReloadManagerSettings(ref DateTime lastConfigDBUpdate, double minutesBetweenUpdates)
        {
            if (DateTime.UtcNow.Subtract(lastConfigDBUpdate).TotalMinutes < minutesBetweenUpdates)
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
                {
                    return false;
                }

                LogDebug("Updating manager settings using Manager Control database");

                // Store the new settings, then retrieve updated settings from the database
                if (mMgrSettings.LoadSettings(configFileSettings, true))
                {
                    if (!mMgrSettings.HasParam("DMSConnectionString"))
                    {
                        LogError("After reloading manager parameters, parameter {0} is missing", "DMSConnectionString");
                        return false;
                    }

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

        /// <summary>
        /// Validates that the dataset storage drive has sufficient free space
        /// </summary>
        /// <param name="errMsg">Output: error message</param>
        /// <returns>True if OK; false if not enough free space</returns>
        private bool ValidateFreeDiskSpace(out string errMsg)
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
                    var freeSpaceGB = CTMUtilities.BytesToGB(totalNumberOfFreeBytes);

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

                    if (System.Net.Dns.GetHostName().StartsWith("WE43320", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine("Warning: " + errMsg);
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
                {
                    continue;
                }

                // Auto-update the working directory
                mMgrSettings.SetParam("WorkDir", alternateWorkDir);

                LogWarning("Invalid working directory: " + workingDir + "; automatically switched to " + alternateWorkDir);
                return true;
            }

            LogError("Invalid working directory: " + workingDir, true);
            return false;
        }

        private new void RegisterEvents(IEventNotifier processingClass, bool writeDebugEventsToLog = true)
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

        private void FileWatcherChanged(object sender, FileSystemEventArgs e)
        {
            LogDebug("MainProgram.FileWatcherChanged event received");

            mConfigChanged = true;
            mFileWatcher.EnableRaisingEvents = false;
        }

        private void OnStatusMonitorUpdateReceived(string msg)
        {
            mMessageSender.SendMessage(msg);
        }

        /// <summary>
        /// Updates the status at mStatusTimer interval
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void StatusTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            mStatusFile.WriteStatusFile();
        }
    }
}
