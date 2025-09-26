using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CaptureTaskManager;
using PRISM;
using PRISM.Logging;

namespace ImsDemuxPlugin
{
    /// <summary>
    /// This class demultiplexes a .D directory using the PNNL-PreProcessor
    /// </summary>
    public class AgilentDemuxTools : EventNotifier
    {
        // Ignore Spelling: cmd, demultiplexed, demultiplexer, demultiplexes, demultiplexing, demux, dest, IMS, ims_tof, PNNL, workdir

        private const string DECODED_dotD_SUFFIX = ".d.deMP.d";
        public const string ENCODED_dotD_SUFFIX = "_muxed.d";

        // Set the max runtime at 5 days
        private const int MAX_DEMUX_RUNTIME_DAYS = 5;
        private const int MAX_DEMUX_RUNTIME_MINUTES = 1440 * MAX_DEMUX_RUNTIME_DAYS;

        private string mDataset;
        private string mDatasetDirectoryPathRemote = string.Empty;
        private readonly FileTools mFileTools;
        private string mWorkDir;

        /// <summary>
        /// Full path to PNNL-Preprocessor.exe
        /// </summary>
        private readonly string mPNNLPreProcessorPath;

        /// <summary>
        /// PNNL preprocessor console output file
        /// </summary>
        private string mPNNLPreProcessorConsoleOutputFilePath;

        private DateTime mLastProgressUpdateTime;
        private DateTime mLastProgressMessageTime;

        private DateTime mDemuxStartTime;
        private float mDemuxProgressPercentComplete;

        private readonly List<string> mLoggedConsoleOutputErrors;

        private struct PreprocessorOptions
        {
            public int FramesToSum;

            /// <summary>
            /// Percentage of pulse coverage required for a data point to survive demultiplexing
            /// </summary>
            public double MinimumPulseCoverage;
        }

        // Events used for communication back to PluginMain, where the logging and status updates are handled

        public event DelDemuxProgressHandler DemuxProgress;

        public event StatusEventEventHandler CopyFileWithRetryEvent;

        public bool OutOfMemoryException { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pnnlPreProcessorPath">Full path to PNNL-Preprocessor.exe</param>
        /// <param name="fileTools"></param>
        public AgilentDemuxTools(string pnnlPreProcessorPath, FileTools fileTools)
        {
            mPNNLPreProcessorPath = pnnlPreProcessorPath;
            mFileTools = fileTools;

            mLoggedConsoleOutputErrors = new List<string>();
        }

        private void CopyDotDToWorkDir(
            string remoteDotDirName,
            ToolReturnData returnData,
            out string dotDRemoteFileNamePath,
            out string dotDLocalFileNamePath)
        {
            // Locate data file on storage server
            dotDRemoteFileNamePath = Path.Combine(mDatasetDirectoryPathRemote, remoteDotDirName);
            dotDLocalFileNamePath = Path.Combine(mWorkDir, mDataset + ".d");

            // Copy the UIMF file to working directory
            OnDebugEvent("Copying file from storage server");
            const int retryCount = 0;

            if (!CopyDirectoryWithRetry(dotDRemoteFileNamePath, dotDLocalFileNamePath, false, retryCount))
            {
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Error copying Agilent IMS .D directory to working directory");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Performs demultiplexing of IMS .D directories
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="returnData">Instance of ToolReturnData</param>
        /// <param name="remoteDotDirName">Name of the Agilent IMS .D directory</param>
        /// <param name="keepLocalOutput">If true, do not delete the local output Agilent IMS .D directory (name ends with .d.deMP.d)</param>
        public void PerformDemux(
            IMgrParams mgrParams,
            ITaskParams taskParams,
            ToolReturnData returnData,
            string remoteDotDirName,
            bool keepLocalOutput = false)
        {
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            var jobNum = taskParams.GetParam("Job");
            OnStatusEvent("Performing demultiplexing, job {0}, dataset {1}", jobNum, mDataset);

            var postProcessingError = false;

            // Default to summing 5 LC frames if this parameter is not defined
            var framesToSum = taskParams.GetParam("DemuxFramesToSum", 5);

            if (framesToSum > 1)
            {
                OnStatusEvent("Will sum " + framesToSum + " LC Frames when demultiplexing");
            }

            // Default to 62% (5 of 8) minimum pulse coverage if this parameter is not defined
            var minPulseCoverage = taskParams.GetParam("DemuxMinPulseCoverage", 62);
            OnStatusEvent("Will use " + minPulseCoverage + "% minimum pulse coverage when demultiplexing");

            // Make sure the working directory is empty
            ToolRunnerBase.CleanWorkDir(mWorkDir);

            // Copy the .D directory from the storage server to the working directory

            CopyDotDToWorkDir(remoteDotDirName, returnData, out var dotDRemoteEncodedFileNamePath, out var dotDLocalEncodedFileNamePath);

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return;
            }

            var demuxOptions = new PreprocessorOptions
            {
                FramesToSum = framesToSum,
                MinimumPulseCoverage = minPulseCoverage
            };

            // Perform demux operation
            OnDebugEvent("Calling PNNL-PreProcessor.exe");

            try
            {
                if (!DemultiplexFile(dotDLocalEncodedFileNamePath, mDataset, demuxOptions, out var errorMessage))
                {
                    if (string.IsNullOrEmpty(errorMessage))
                    {
                        errorMessage = "Error demultiplexing Agilent IMS .D directory";
                    }

                    returnData.CloseoutMsg = errorMessage;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling DemultiplexFile for dataset " + mDataset, ex);
                returnData.CloseoutMsg = "Error demultiplexing Agilent IMS .D directory";
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            // Look for the demultiplexed .D directory (name ends with .d.deMP.d)
            var localDotDDecodedFilePath = Path.Combine(mWorkDir, mDataset + DECODED_dotD_SUFFIX);

            if (!Directory.Exists(localDotDDecodedFilePath))
            {
                returnData.CloseoutMsg = "Decoded Agilent IMS .D directory not found";
                OnErrorEvent(returnData.CloseoutMsg + ": " + localDotDDecodedFilePath);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (!ValidateDotDDemultiplexed(localDotDDecodedFilePath, returnData))
            {
                if (string.IsNullOrEmpty(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "ValidateDotDDemultiplexed returned false";
                }

                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                postProcessingError = true;
            }

            if (!postProcessingError)
            {
                // Rename Agilent IMS .D directory on storage server; replaces ".d" with "_muxed.d"
                OnDebugEvent("Renaming Agilent IMS .D directory on storage server");

                // If this is a re-run, the encoded directory has already been renamed
                // This is determined by looking for "_muxed.d" in the directory name
                if (!remoteDotDirName.Contains(ENCODED_dotD_SUFFIX))
                {
                    if (!RenameDirectory(dotDRemoteEncodedFileNamePath, Path.Combine(mDatasetDirectoryPathRemote, mDataset + ENCODED_dotD_SUFFIX)))
                    {
                        returnData.CloseoutMsg = "Error renaming encoded Agilent IMS .D directory on storage server";
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        postProcessingError = true;
                    }
                }
            }

            if (!postProcessingError)
            {
                // Copy the result files to the storage server
                // Copies local directory DatasetName.d.deMP.d
                // to remote directory    Dataset.d
                if (!CopyDotDDirectoryToStorageServer(returnData, localDotDDecodedFilePath, "demultiplexed Agilent IMS .D"))
                {
                    postProcessingError = true;
                }
            }

            if (postProcessingError)
            {
                try
                {
                    // Delete the multiplexed Agilent IMS .D directory (no point in saving it); its name ends with .d.deMP.d
                    mFileTools.DeleteDirectory(dotDLocalEncodedFileNamePath);
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch
                {
                    // Ignore errors deleting the multiplexed Agilent IMS .D directory (from the local working directory)
                }

                // Try to save the demultiplexed Agilent IMS .D directory (and any other files in the working directory)
                var failedResultsCopier = new FailedResultsCopier(mgrParams, taskParams);
                failedResultsCopier.CopyFailedResultsToArchiveDirectory(mWorkDir);

                return;
            }

            // Delete local .D directories and file(s)
            OnDebugEvent("Cleaning up working directory");

            for (var i = 0; i < 3; i++)
            {
                // Retry if failed, since next step can depend on this file being deleted
                try
                {
                    RemoveReadOnlyFlag(dotDLocalEncodedFileNamePath, true);
                    mFileTools.DeleteDirectory(dotDLocalEncodedFileNamePath);
                    break;
                }
                catch (Exception ex)
                {
                    // Error deleting files; don't treat this as a fatal error
                    OnErrorEvent("Exception deleting working directory file(s): " + ex.Message);

                    if (i < 2)
                    {
                        System.Threading.Thread.Sleep(3000);
                    }
                }
            }

            try
            {
                if (keepLocalOutput)
                {
                    // renamed the output file
                    Directory.Move(localDotDDecodedFilePath, dotDLocalEncodedFileNamePath);
                }
                else
                {
                    RemoveReadOnlyFlag(localDotDDecodedFilePath, true);
                    mFileTools.DeleteDirectory(localDotDDecodedFilePath);
                }
            }
            catch (Exception ex)
            {
                // Error deleting files; don't treat this as a fatal error
                OnErrorEvent("Exception deleting working directory file(s): " + ex.Message);
            }

            // Update the return data
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            returnData.EvalMsg = "De-multiplexed";
        }

        /// <summary>
        /// Copies a file, allowing for retries
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="targetDirectoryPath">Destination directory</param>
        /// <param name="overWrite">True to overWrite existing files</param>
        /// <param name="retryCount">Number of attempts</param>
        /// <param name="backupDestDirectoryBeforeCopy">If True and if the target directory exists, renames the target directory to have _Old1 before the extension</param>
        /// <returns>True if success, false if an error</returns>
        private bool CopyDirectoryWithRetry(string sourceDirectoryPath, string targetDirectoryPath, bool overWrite, int retryCount, bool backupDestDirectoryBeforeCopy = false)
        {
            OnCopyFileWithRetry(sourceDirectoryPath, targetDirectoryPath);
            return CopyDirectoryWithRetry(sourceDirectoryPath, targetDirectoryPath, overWrite, retryCount, backupDestDirectoryBeforeCopy, mFileTools);
        }

        /// <summary>
        /// Copies a file, allowing for retries
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory</param>
        /// <param name="targetDirectoryPath">Destination directory</param>
        /// <param name="overWrite">True to overWrite existing files</param>
        /// <param name="retryCount">Number of attempts</param>
        /// <param name="backupDestDirectoryBeforeCopy">If True and if the target directory exists, renames the target directory to have _Old1 before the extension</param>
        /// <param name="fileTools">Instance of FileTools</param>
        /// <returns>True if success, false if an error</returns>
        public static bool CopyDirectoryWithRetry(
            string sourceDirectoryPath,
            string targetDirectoryPath,
            bool overWrite,
            int retryCount,
            bool backupDestDirectoryBeforeCopy,
            FileTools fileTools)
        {
            var retryingCopy = false;

            if (retryCount < 0)
            {
                retryCount = 0;
            }

            if (backupDestDirectoryBeforeCopy)
            {
                FileTools.BackupFileBeforeCopy(targetDirectoryPath);
            }

            while (retryCount >= 0)
            {
                string msg;
                try
                {
                    if (retryingCopy)
                    {
                        msg = "Retrying copy; retryCount = " + retryCount;
                        LogTools.LogMessage(msg);
                    }

                    // The parent method should call OnCopyFileWithRetry() or ResetTimestampForQueueWaitTimeLogging() prior to calling this method

                    return fileTools.CopyDirectoryWithResume(sourceDirectoryPath, targetDirectoryPath, true,
                        overWrite ? FileTools.FileOverwriteMode.AlwaysOverwrite : FileTools.FileOverwriteMode.DoNotOverwrite,
                        new List<string>());
                }
                catch (Exception ex)
                {
                    msg = "Exception copying directory " + sourceDirectoryPath + " to " + targetDirectoryPath + ": " + ex.Message;
                    LogTools.LogError(msg, ex);

                    System.Threading.Thread.Sleep(2000);
                    retryCount--;
                    retryingCopy = true;
                }
            }

            // If we get here, we were not able to successfully copy the file
            return false;
        }

        /// <summary>
        /// Copies the result files to the storage server
        /// </summary>
        /// <param name="returnData"></param>
        /// <param name="localDotDDecodedFilePath"></param>
        /// <param name="fileDescription"></param>
        /// <returns>True if success; otherwise false</returns>
        private bool CopyDotDDirectoryToStorageServer(ToolReturnData returnData, string localDotDDecodedFilePath, string fileDescription)
        {
            // Copy the demultiplexed file to the storage server, renaming as DatasetName.d in the process
            OnDebugEvent("Copying " + fileDescription + " file to storage server");

            var targetDirectoryPath = Path.Combine(mDatasetDirectoryPathRemote, mDataset + ".d");

            // Assure that files in the target directory are all writable
            var success = RemoveReadOnlyFlag(targetDirectoryPath, true);

            if (!success)
            {
                returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Error clearing the ReadOnly attribute for files at " + targetDirectoryPath);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            const int retryCount = 3;

            if (CopyDirectoryWithRetry(localDotDDecodedFilePath, targetDirectoryPath, true, retryCount))
            {
                return true;
            }

            returnData.CloseoutMsg = CTMUtilities.AppendToString(returnData.CloseoutMsg, "Error copying " + fileDescription + " file to storage server");
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            return false;
        }

        /// <summary>
        /// Performs actual demultiplexing operation
        /// </summary>
        /// <param name="inputFilePath">Input file name</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="demuxOptions">Demultiplexing options</param>
        /// <param name="errorMessage">Output: Error message</param>
        /// <returns>True if success, false if an error</returns>
        private bool DemultiplexFile(
            string inputFilePath,
            string datasetName,
            PreprocessorOptions demuxOptions,
            out string errorMessage)
        {
            string msg;
            errorMessage = string.Empty;

            var inputFile = new FileInfo(inputFilePath);
            var directoryName = inputFile.DirectoryName;

            if (string.IsNullOrEmpty(directoryName))
            {
                errorMessage = "Could not determine the parent directory for " + inputFilePath;
                OnErrorEvent(errorMessage);
                return false;
            }

            // Note: PNNL-PreProcessor does not allow setting the output file path
            var outputFilePath = Path.Combine(directoryName, datasetName + DECODED_dotD_SUFFIX);

            try
            {
                OutOfMemoryException = false;

                msg = "Starting demultiplexing, dataset " + datasetName;
                OnStatusEvent(msg);

                var success = RunPNNLPreProcessorDemultiplexer(inputFilePath, outputFilePath, demuxOptions, MAX_DEMUX_RUNTIME_MINUTES, out errorMessage);

                // Confirm that things have succeeded
                if (success && mLoggedConsoleOutputErrors.Count == 0 && !OutOfMemoryException)
                {
                    msg = "Demultiplexing complete, dataset " + datasetName;
                    OnStatusEvent(msg);
                    return true;
                }

                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Unknown error";
                }

                if (OutOfMemoryException)
                {
                    errorMessage = "OutOfMemory exception was thrown";
                }

                if (string.IsNullOrEmpty(errorMessage) && mLoggedConsoleOutputErrors.Count > 0)
                {
                    errorMessage = mLoggedConsoleOutputErrors.First();
                }

                OnErrorEvent(errorMessage);
                return false;
            }
            catch (Exception ex)
            {
                msg = "Exception demultiplexing dataset " + datasetName;
                OnErrorEvent(msg, ex);
                return false;
            }
        }

        private void OnCopyFileWithRetry(string sourceFilePath, string targetFilePath)
        {
            CopyFileWithRetryEvent?.Invoke(sourceFilePath + " -> " + targetFilePath);
        }

        private void ParseConsoleOutputFileDemux()
        {
            // ReSharper disable CommentTypo

            // Example Console output:
            //
            // Processing file 1 of 1: "TuneMix_MP_0514_Single_Pos_0001"...
            //
            // Progress: 0.0 %
            // Progress: 3.7 %
            // Progress: 7.4 %
            // Progress: 11.1 %
            // Progress: 14.8 %
            // Progress: 18.5 %
            // Progress: 22.2 %
            // Progress: 25.9 %
            // Progress: 29.6 %
            // Progress: 33.3 %
            // Progress: 37.0 %
            // Progress: 40.7 %
            // Progress: 44.4 %
            // Progress: 48.1 %
            // Progress: 51.9 %
            // Progress: 55.6 %
            // Progress: 59.3 %
            // Progress: 63.0 %
            // Progress: 66.7 %
            // Progress: 70.4 %
            // Progress: 74.1 %
            // Progress: 77.8 %
            // Progress: 81.5 %
            // Progress: 85.2 %
            // Progress: 88.9 %
            // Progress: 92.6 %
            // Progress: 96.3 %
            // Progress: 100.0 %
            // Progress: 100.0 %
            // Processing completed

            // ReSharper restore CommentTypo

            var percentCompleteMatcher = new Regex(@"Progress: (\d+\.?\d*)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                if (string.IsNullOrEmpty(mPNNLPreProcessorConsoleOutputFilePath))
                {
                    return;
                }

                using var reader = new StreamReader(new FileStream(mPNNLPreProcessorConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    if (dataLine.StartsWith("Error in") ||
                        dataLine.StartsWith("Error:") ||
                        dataLine.StartsWith("Exception"))
                    {
                        if (!mLoggedConsoleOutputErrors.Contains(dataLine))
                        {
                            OnErrorEvent(dataLine);
                            mLoggedConsoleOutputErrors.Add(dataLine);
                        }

                        if (dataLine.Contains("OutOfMemoryException"))
                        {
                            OutOfMemoryException = true;
                        }
                    }
                    else if (dataLine.StartsWith("Warning:"))
                    {
                        if (!mLoggedConsoleOutputErrors.Contains(dataLine))
                        {
                            OnWarningEvent(dataLine);
                            mLoggedConsoleOutputErrors.Add(dataLine);
                        }
                    }
                    else
                    {
                        // Compare the line against the various RegEx specs

                        // % complete (integer values only)
                        var percentCompleteMatch = percentCompleteMatcher.Match(dataLine);

                        if (percentCompleteMatch.Success)
                        {
                            if (short.TryParse(percentCompleteMatch.Groups[1].Value, out var percentComplete))
                            {
                                mDemuxProgressPercentComplete = percentComplete;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                if (!mLoggedConsoleOutputErrors.Contains(ex.Message))
                {
                    OnErrorEvent("Exception in ParseConsoleOutputFileDemux", ex);
                    mLoggedConsoleOutputErrors.Add(ex.Message);
                }
            }
        }

        private bool RemoveReadOnlyFlag(string targetDirectoryPath, bool recurse)
        {
            try
            {
                var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                if (!targetDirectory.Exists)
                {
                    // Treat this as success
                    return true;
                }

                var searchOption = recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

                foreach (var item in targetDirectory.GetFileSystemInfos("*", searchOption))
                {
                    if ((item.Attributes & FileAttributes.ReadOnly) != FileAttributes.ReadOnly)
                    {
                        continue;
                    }

                    OnDebugEvent("Removing the ReadOnly attribute from " + item.FullName);
                    item.Attributes &= ~FileAttributes.ReadOnly;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in RemoveReadOnlyFlag", ex);
                return false;
            }
        }

        /// <summary>
        /// Renames a directory
        /// </summary>
        /// <param name="sourcePath">Original directory path</param>
        /// <param name="targetPath">New directory path</param>
        /// <returns>True if successful, false if an error</returns>
        private bool RenameDirectory(string sourcePath, string targetPath)
        {
            try
            {
                var sourceDirectory = new DirectoryInfo(sourcePath);
                sourceDirectory.MoveTo(targetPath);
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception renaming directory " + sourcePath + " to " + Path.GetFileName(targetPath) + ": " + ex.Message);

                // Garbage collect, then try again to rename the directory
                System.Threading.Thread.Sleep(250);
                AppUtils.GarbageCollectNow();
                System.Threading.Thread.Sleep(250);

                try
                {
                    var sourceDirectory = new DirectoryInfo(sourcePath);
                    sourceDirectory.MoveTo(targetPath);
                    return true;
                }
                catch (Exception ex2)
                {
                    OnErrorEvent("Rename failed despite garbage collection call: " + ex2.Message);
                }

                return false;
            }
        }

        /// <summary>
        /// This function is called both by CalibrateFile and DemultiplexFile
        /// </summary>
        /// <param name="inputFilePath"></param>
        /// <param name="outputFilePath"></param>
        /// <param name="demuxOptions"></param>
        /// <param name="maxRuntimeMinutes"></param>
        /// <param name="errorMessage">Output: Error message</param>
        /// <returns>True if successful, false if an error</returns>
        private bool RunPNNLPreProcessorDemultiplexer(
            string inputFilePath,
            string outputFilePath,
            PreprocessorOptions demuxOptions,
            int maxRuntimeMinutes,
            out string errorMessage)
        {
            errorMessage = string.Empty;

            try
            {
                if (string.IsNullOrWhiteSpace(mPNNLPreProcessorPath))
                {
                    errorMessage = "Field mPNNLPreProcessorPath is undefined";
                    OnErrorEvent(errorMessage);
                    return false;
                }

                var inputFile = new FileInfo(inputFilePath);
                var outputFile = new FileInfo(outputFilePath);

                // Construct the command line arguments

                // Input file
                var arguments = " -d " + Conversion.PossiblyQuotePath(inputFilePath);

                if (!string.Equals(inputFile.DirectoryName, outputFile.DirectoryName, StringComparison.OrdinalIgnoreCase))
                {
                    // Output directory
                    arguments += " -out " + Conversion.PossiblyQuotePath(outputFile.DirectoryName);
                }

                // Demultiplexing

                // Output file name
                // Note: PNNL-PreProcessor does not allow setting the output file path
                //arguments += " /N:" + Conversion.PossiblyQuotePath(outputFile.Name);

                arguments += " -demux";
                arguments += " -demuxMA " + demuxOptions.FramesToSum;
                arguments += " -demuxSignal " + demuxOptions.MinimumPulseCoverage;
                arguments += " -overwrite ";

                // Supply demultiplexing resource use level? (default is "Medium", we may want to use "High")
                //arguments += " -demuxLoad High";

                mPNNLPreProcessorConsoleOutputFilePath = Path.Combine(mWorkDir, "PNNL-PreProcessor_ConsoleOutput.txt");

                OnStatusEvent(mPNNLPreProcessorPath + " " + arguments);
                var cmdRunner = new RunDosProgram(mWorkDir);
                mDemuxStartTime = DateTime.UtcNow;
                mLastProgressUpdateTime = DateTime.UtcNow;
                mLastProgressMessageTime = DateTime.UtcNow;

                AttachCmdRunnerEvents(cmdRunner);

                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;

                // Create a console output file
                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = mPNNLPreProcessorConsoleOutputFilePath;

                var success = cmdRunner.RunProgram(mPNNLPreProcessorPath, arguments, "PNNL-PreProcessor", true, maxRuntimeMinutes * 60);

                ParseConsoleOutputFileDemux();

                if (success)
                {
                    return true;
                }

                errorMessage = "Error running PNNL-PreProcessor Demultiplexing";
                OnErrorEvent(errorMessage);

                if (cmdRunner.ExitCode != 0)
                {
                    OnWarningEvent("PNNL-PreProcessor returned a non-zero exit code: " + cmdRunner.ExitCode);
                }
                else
                {
                    OnWarningEvent("Call to PNNL-PreProcessor failed (but exit code is 0)");
                }

                return false;
            }
            catch (Exception ex)
            {
                errorMessage = "Exception in RunPNNLPreProcessorDemultiplexer";
                OnErrorEvent(errorMessage, ex);
                return false;
            }
        }

        private void UpdateDatasetInfo(IMgrParams mgrParams, ITaskParams taskParams)
        {
            mDataset = taskParams.GetParam("Dataset");
            mWorkDir = mgrParams.GetParam("workdir");

            var svrPath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
            var datasetDirectory = taskParams.GetParam("Directory");

            mDatasetDirectoryPathRemote = Path.Combine(svrPath, datasetDirectory);
        }

        /// <summary>
        /// Examines the Log_Entries table to make sure the .D directory was demultiplexed
        /// </summary>
        /// <param name="localDotDDecodedFilePath">Local decoded directory path (name ends with .d.deMP.d)</param>
        /// <param name="returnData"></param>
        /// <param name="ignoreDemultiplexingDate">When false, require that the "Demultiplexing finished!" message was logged within the last 10 minutes</param>
        /// <returns>True if it was demultiplexed, otherwise false</returns>
        public bool ValidateDotDDemultiplexed(string localDotDDecodedFilePath, ToolReturnData returnData, bool ignoreDemultiplexingDate = false)
        {
            string msg;

            // Make sure the Preprocessor log contains entry "Demultiplexing finished!" (optionally with a timestamp within the last 10 minutes)
            var demultiplexingFinished = GetDemultiplexingFinishedFromLog(localDotDDecodedFilePath, out var message);

            if (demultiplexingFinished == DateTime.MinValue)
            {
                returnData.CloseoutMsg = "Demultiplexing finished message not found in PNNL-PreProcessorLog.txt file";
                msg = returnData.CloseoutMsg + " in " + localDotDDecodedFilePath;

                if (!string.IsNullOrEmpty(message))
                {
                    msg += "; " + message;
                }

                OnErrorEvent(msg);
                return false;
            }

            if (DateTime.Now.Subtract(demultiplexingFinished).TotalMinutes < 10 || ignoreDemultiplexingDate)
            {
                msg = "Demultiplexing finished message in PNNL-PreProcessorLog.txt file has date " + demultiplexingFinished;
                OnStatusEvent(msg);
                return true;
            }

            returnData.CloseoutMsg = "Demultiplexing finished message in PNNL-PreProcessorLog.txt file is more than 10 minutes old";
            msg = returnData.CloseoutMsg + ": " + demultiplexingFinished + "; assuming this is a demultiplexing failure";

            if (!string.IsNullOrEmpty(message))
            {
                msg += "; " + message;
            }

            OnErrorEvent(msg);
            return false;
        }

        private static DateTime GetDemultiplexingFinishedFromLog(string dotDPath, out string message)
        {
            message = string.Empty;

            const string logSubPath = @"AcqData\PNNL-PreProcessorLog.txt";
            var logPath = Path.Combine(dotDPath, logSubPath);

            if (!File.Exists(logPath))
            {
                message = $"File not found: {logPath}";
                return DateTime.MinValue;
            }

            using var sr = new StreamReader(new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var lastDate = DateTime.MinValue;
            var foundFinish = false;
            var foundEndTime = false;
            var linesAfterFinish = false;

            // ReSharper disable once MoveVariableDeclarationInsideLoopCondition
            string line;

            while ((line = sr.ReadLine()) != null)
            {
                linesAfterFinish = true;

                // Processing start time
                if (line.StartsWith("---") && DateTime.TryParse(line.Trim(' ', '-'), out var time))
                {
                    lastDate = time;
                    foundFinish = false;
                    foundEndTime = false;
                }

                // Processing end time
                if (line.StartsWith("End time:") && DateTime.TryParse(line.Replace("End time:", string.Empty).Trim(' ', '-'), out var endTime))
                {
                    lastDate = endTime;
                    foundFinish = false;
                    foundEndTime = true;
                }

                if (line.Equals("Demultiplexing finished!", StringComparison.OrdinalIgnoreCase))
                {
                    foundFinish = true;
                    linesAfterFinish = false;
                }
            }

            if (!linesAfterFinish && !foundEndTime)
            {
                lastDate = new FileInfo(logPath).LastWriteTime;
            }

            if (foundFinish)
            {
                return lastDate;
            }

            message = "Did not find message 'Demultiplexing finished!'";
            return DateTime.MinValue;
        }

        private void AttachCmdRunnerEvents(RunDosProgram cmdRunner)
        {
            try
            {
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
                cmdRunner.Timeout += CmdRunner_Timeout;
            }
            catch
            {
                // Ignore errors here
            }
        }

        private void CmdRunner_Timeout()
        {
            OnErrorEvent("CmdRunner timeout reported (the PNNL-PreProcessor has been running for over {0} days)", MAX_DEMUX_RUNTIME_DAYS);
        }

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds < 30)
            {
                return;
            }

            mLastProgressUpdateTime = DateTime.UtcNow;

            ParseConsoleOutputFileDemux();

            DemuxProgress?.Invoke(mDemuxProgressPercentComplete);

            if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds < 300)
                return;

            mLastProgressMessageTime = DateTime.UtcNow;
            OnDebugEvent("{0} running; {1:F1} minutes elapsed, {2:F1}% complete",
                "PNNL-Preprocessor",
                DateTime.UtcNow.Subtract(mDemuxStartTime).TotalMinutes,
                mDemuxProgressPercentComplete);
        }
    }
}
