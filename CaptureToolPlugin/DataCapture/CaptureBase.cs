using System;
using System.IO;
using System.Linq;
using CaptureTaskManager;
using PRISM;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Abstract class for capturing datasets
    /// </summary>
    internal abstract class CaptureBase : LoggerBase
    {
        // Ignore Spelling: bionet

        /// <summary>
        /// Use copy with resume for files over 500 MB in size
        /// </summary>
        protected const int COPY_WITH_RESUME_THRESHOLD_BYTES = 500 * 1024 * 1024;

        protected readonly int mSleepInterval;

        protected readonly bool mTraceMode;

        protected readonly SharedState mToolState;
        protected readonly ShareConnection mShareConnection;

        protected readonly DatasetFileSearchTool mDatasetFileSearchTool;
        protected readonly FileTools mFileTools;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        protected CaptureBase(CaptureInitData data)
        {
            mToolState = data.ToolState;
            mTraceMode = data.TraceMode;

            mShareConnection = data.ShareConnection;

            // Sleep interval for "is dataset complete" testing
            mSleepInterval = data.MgrParams.GetParam("SleepInterval", 30);

            mFileTools = data.FileTools;

            // Events from mFileTools are being listened to at a higher level.

            mDatasetFileSearchTool = new DatasetFileSearchTool(mTraceMode);
            RegisterEvents(mDatasetFileSearchTool);
        }

        ///// <summary>
        ///// Capture a dataset directory that has an extension like .D or .Raw
        ///// </summary>
        ///// <param name="msg">Output: error message</param>
        ///// <param name="returnData">Input/output: Return data</param>
        ///// <param name="datasetInfo">Dataset info</param>
        ///// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        ///// <param name="datasetDirectoryPath">Destination directory (on storage server); datasetInfo.FileOrDirectoryName will be appended to this</param>
        ///// <param name="copyWithResume">True if using copy with resume</param>
        ///// <param name="instrumentClass">Instrument class</param>
        ///// <param name="instrumentName">Instrument name</param>
        ///// <param name="taskParams">Task parameters</param>
        //public abstract void Capture(
        //    out string msg,
        //    ToolReturnData returnData,
        //    DatasetInfo datasetInfo,
        //    string sourceDirectoryPath,
        //    string datasetDirectoryPath,
        //    bool copyWithResume,
        //    InstrumentClassInfo.InstrumentClass instrumentClass,
        //    string instrumentName,
        //    ITaskParams taskParams
        //);

        /// <summary>
        /// Creates specified directory; if the directory already exists, returns true
        /// </summary>
        /// <param name="directoryPath">Fully qualified path for directory to be created</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        protected void MakeDirectoryIfMissing(string directoryPath)
        {
            // Create specified directory
            try
            {
                var targetDirectory = new DirectoryInfo(directoryPath);

                if (!targetDirectory.Exists)
                {
                    targetDirectory.Create();
                }
            }
            catch (Exception ex)
            {
                mToolState.ErrorMessage = "Exception creating directory " + directoryPath;
                LogError(mToolState.ErrorMessage, ex);
            }
        }

        /// <summary>
        /// Checks to see if directory size is changing.
        /// If so, this is a possible sign that acquisition hasn't finished
        /// </summary>
        /// <param name="targetDirectory">Directory to examine</param>
        /// <param name="returnData">Output: return data</param>
        /// <returns>TRUE if directory size hasn't changed; FALSE otherwise</returns>
        protected bool VerifyConstantDirectorySize(DirectoryInfo targetDirectory, ToolReturnData returnData)
        {
            try
            {
                var sleepIntervalSeconds = GetSleepIntervalForDirectory(targetDirectory);

                // Sleep interval should be between 1 second and 15 minutes (900 seconds)
                if (sleepIntervalSeconds > 900)
                {
                    sleepIntervalSeconds = 900;
                }

                if (sleepIntervalSeconds < 1)
                {
                    sleepIntervalSeconds = 1;
                }

                // Get the initial size of the directory
                var initialDirectorySize = mFileTools.GetDirectorySize(targetDirectory.FullName);

                // Wait for specified sleep interval
                VerifyConstantSizeSleep(sleepIntervalSeconds, "directory " + targetDirectory.Name);

                // Get the final size of the directory and compare
                var finalDirectorySize = mFileTools.GetDirectorySize(targetDirectory.FullName);

                if (finalDirectorySize == initialDirectorySize)
                {
                    return true;
                }

                LogMessage("Directory size changed from " + initialDirectorySize + " bytes to " + finalDirectorySize + " bytes: " + targetDirectory.FullName);

                return false;
            }
            catch (Exception ex)
            {
                if (ex is IOException && (ex.Message.Contains("user name") || ex.Message.Contains("password")))
                {
                    // Note that this will call LogError and update returnData.CloseoutMsg
                    mToolState.HandleCopyException(returnData, ex);

                    LogWarning("Source directory path: " + targetDirectory.FullName);
                    return false;
                }

                returnData.CloseoutMsg = "Exception validating constant directory size";

                LogError(returnData.CloseoutMsg + ": " + targetDirectory.FullName, ex);

                mToolState.HandleCopyException(returnData, ex);
                return false;
            }
        }

        /// <summary>
        /// Checks to see if file size is changing -- possible sign acquisition hasn't finished
        /// </summary>
        /// <param name="filePath">Full path specifying file to check</param>
        /// <param name="sleepIntervalSeconds">Interval for checking (seconds)</param>
        /// <param name="returnData">Output: return data</param>
        /// <returns>TRUE if file size hasn't changed during SleepInt; FALSE otherwise</returns>
        protected bool VerifyConstantFileSize(string filePath, int sleepIntervalSeconds, ToolReturnData returnData)
        {
            try
            {
                // Sleep interval should be between 1 second and 15 minutes (900 seconds)
                if (sleepIntervalSeconds > 900)
                {
                    sleepIntervalSeconds = 900;
                }

                if (sleepIntervalSeconds < 1)
                {
                    sleepIntervalSeconds = 1;
                }

                // Get the initial size of the file
                var remoteFile = new FileInfo(filePath);
                if (!remoteFile.Exists)
                {
                    // File not found, but return true anyway
                    return true;
                }

                var initialFileSize = remoteFile.Length;

                VerifyConstantSizeSleep(sleepIntervalSeconds, "file " + remoteFile.Name);

                // Get the final size of the file and compare
                remoteFile.Refresh();
                var finalFileSize = remoteFile.Length;

                if (finalFileSize == initialFileSize)
                {
                    if (mTraceMode)
                    {
                        ToolRunnerBase.ShowTraceMessage("File size did not change");
                    }

                    return true;
                }

                LogMessage("File size changed from " + initialFileSize + " bytes to " + finalFileSize + " bytes: " + filePath);

                return false;
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception validating constant file size";
                LogError(returnData.CloseoutMsg + ": " + filePath, ex);

                mToolState.HandleCopyException(returnData, ex);
                return false;
            }
        }

        /// <summary>
        /// Wait the specified number of seconds, showing a status message every 5 seconds
        /// </summary>
        /// <param name="sleepIntervalSeconds"></param>
        /// <param name="fileOrDirectoryName"></param>
        protected void VerifyConstantSizeSleep(int sleepIntervalSeconds, string fileOrDirectoryName)
        {
            const int STATUS_MESSAGE_INTERVAL = 5;

            if (mTraceMode)
            {
                // Monitoring file DatasetName.raw for 30 seconds
                // Monitoring directory DatasetName.d for 30 seconds
                ToolRunnerBase.ShowTraceMessage("Monitoring {0} for {1} seconds", fileOrDirectoryName, sleepIntervalSeconds);
            }

            // Wait for specified sleep interval
            var verificationEndTime = DateTime.UtcNow.AddSeconds(sleepIntervalSeconds);
            var nextStatusTime = DateTime.UtcNow.AddSeconds(STATUS_MESSAGE_INTERVAL);

            while (DateTime.UtcNow < verificationEndTime)
            {
                AppUtils.SleepMilliseconds(500);

                if (DateTime.UtcNow <= nextStatusTime)
                {
                    continue;
                }

                nextStatusTime = nextStatusTime.AddSeconds(STATUS_MESSAGE_INTERVAL);
                if (mTraceMode)
                {
                    ToolRunnerBase.ShowTraceMessage("{0:0} seconds remaining", verificationEndTime.Subtract(DateTime.UtcNow).TotalSeconds);
                }
            }
        }

        /// <summary>
        /// Return the sleep interval for a file or directory that is the given days old
        /// </summary>
        /// <param name="itemAgeDays">Days before now that the file or directory was modified</param>
        /// <param name="minimumTimeSeconds">Minimum sleep time</param>
        /// <returns>
        /// mSleepInterval if less than 10 days old
        /// minimumTimeSeconds if more than 30 days old
        /// Otherwise, a value between minimumTimeSeconds and mSleepInterval
        /// </returns>
        private int GetSleepInterval(double itemAgeDays, int minimumTimeSeconds)
        {
            const int AGED_FILE_DAYS_MINIMUM = 10;
            const int AGED_FILE_DAYS_MAXIMUM = 30;

            if (itemAgeDays < AGED_FILE_DAYS_MINIMUM)
            {
                return mSleepInterval;
            }

            if (itemAgeDays > AGED_FILE_DAYS_MAXIMUM)
            {
                return minimumTimeSeconds;
            }

            var scalingMultiplier = (AGED_FILE_DAYS_MAXIMUM - itemAgeDays) /
                                    (AGED_FILE_DAYS_MAXIMUM - AGED_FILE_DAYS_MINIMUM);

            var maximumTimeSeconds = Math.Max(mSleepInterval, minimumTimeSeconds);

            var sleepTimeSeconds = scalingMultiplier * (maximumTimeSeconds - minimumTimeSeconds) + minimumTimeSeconds;

            return (int)Math.Round(sleepTimeSeconds);
        }

        /// <summary>
        /// Return the appropriate interval to wait while examining that a file's size does not change
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <returns>Sleep time, in seconds</returns>
        protected int GetSleepIntervalForFile(string sourceFilePath)
        {
            const int MINIMUM_TIME_SECONDS = 3;

            try
            {
                var sourceFile = new FileInfo(sourceFilePath);

                if (!sourceFile.Exists)
                {
                    return MINIMUM_TIME_SECONDS;
                }

                var fileAgeDays = DateTime.UtcNow.Subtract(sourceFile.LastWriteTimeUtc).TotalDays;

                return GetSleepInterval(fileAgeDays, MINIMUM_TIME_SECONDS);
            }
            catch (Exception ex)
            {
                LogError("Error in GetSleepIntervalForFile", ex);
                return mSleepInterval;
            }
        }

        /// <summary>
        /// Return the appropriate interval to wait while examining that a directory's size does not change
        /// </summary>
        /// <param name="targetDirectory"></param>
        /// <returns>Sleep time, in seconds</returns>
        protected int GetSleepIntervalForDirectory(DirectoryInfo targetDirectory)
        {
            const int MINIMUM_TIME_SECONDS = 3;

            try
            {
                if (!targetDirectory.Exists)
                {
                    return MINIMUM_TIME_SECONDS;
                }

                // Find the newest file in the directory
                var files = targetDirectory.GetFileSystemInfos("*", SearchOption.AllDirectories);

                if (files.Length == 0)
                {
                    return MINIMUM_TIME_SECONDS;
                }

                var mostRecentWriteTime = (from item in files orderby item.LastWriteTimeUtc select item.LastWriteTimeUtc).Max();

                var fileAgeDays = DateTime.UtcNow.Subtract(mostRecentWriteTime).TotalDays;

                return GetSleepInterval(fileAgeDays, MINIMUM_TIME_SECONDS);
            }
            catch (Exception ex)
            {
                LogError("Error in GetSleepIntervalForDirectory", ex);
                return mSleepInterval;
            }
        }
    }
}
