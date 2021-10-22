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
    /// This class demultiplexes a .D file using the PNNL-PreProcessor
    /// </summary>
    public class AgilentDemuxTools : EventNotifier
    {
        // Ignore Spelling: demultiplexed, demultiplexes, demultiplexing, demultiplexer, demux, ims_tof, workdir, cmd
        private const string DECODED_dotD_SUFFIX = ".d.deMP.d";
        public const string ENCODED_dotD_SUFFIX = "_muxed.d";

        // Set the max runtime at 5 days
        private const int MAX_DEMUX_RUNTIME_MINUTES = 1440 * 5;

        private string mDataset;
        private string mDatasetDirectoryPathRemote = string.Empty;
        private readonly FileTools mFileTools;
        private string mWorkDir;

        private readonly string mPNNLPreProcessorPath;
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
        /// <param name="pnnlPreProcessorPath"></param>
        /// <param name="fileTools"></param>
        public AgilentDemuxTools(string pnnlPreProcessorPath, FileTools fileTools)
        {
            mPNNLPreProcessorPath = pnnlPreProcessorPath;
            mFileTools = fileTools;

            mLoggedConsoleOutputErrors = new List<string>();
        }

        private ToolReturnData CopyDotDToWorkDir(
            string dotDFileName,
            ToolReturnData returnData,
            out string dotDRemoteFileNamePath,
            out string dotDLocalFileNamePath)
        {
            // Locate data file on storage server
            dotDRemoteFileNamePath = Path.Combine(mDatasetDirectoryPathRemote, dotDFileName);
            dotDLocalFileNamePath = Path.Combine(mWorkDir, mDataset + ".d");

            // Copy the UIMF file to working directory
            OnDebugEvent("Copying file from storage server");
            const int retryCount = 0;
            if (!CopyDirectoryWithRetry(dotDRemoteFileNamePath, dotDLocalFileNamePath, false, retryCount))
            {
                returnData.CloseoutMsg = AppendToString(returnData.CloseoutMsg, "Error copying Agilent IMS .D file to working directory");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            return returnData;
        }

        /// <summary>
        /// Performs demultiplexing of IMS data files
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="taskParams">Parameters for the assigned task</param>
        /// <param name="dotDFileName">Name of the Agilent IMS .D file</param>
        /// <param name="keepLocalOutput">If true, do not delete the local output Agilent IMS .D file</param>
        /// <returns>Enum indicating task success or failure</returns>
        public ToolReturnData PerformDemux(
            IMgrParams mgrParams,
            ITaskParams taskParams,
            string dotDFileName,
            bool keepLocalOutput = false)
        {
            mLoggedConsoleOutputErrors.Clear();
            UpdateDatasetInfo(mgrParams, taskParams);

            var jobNum = taskParams.GetParam("Job");
            var msg = "Performing demultiplexing, job " + jobNum + ", dataset " + mDataset;
            OnStatusEvent(msg);

            var postProcessingError = false;

            // Default to summing 5 LC frames if this parameter is not defined
            var framesToSum = taskParams.GetParam("DemuxFramesToSum", 5);

            if (framesToSum > 1)
            {
                OnStatusEvent("Will sum " + framesToSum + " LC Frames when demultiplexing");
            }

            // Default to 62% (5 of 8) minimum pulse coverage if this parameter is not defined
            var minPulseCoverage = taskParams.GetParam("DemuxFramesToSum", 62);
            OnStatusEvent("Will use " + minPulseCoverage + "% minimum pulse coverage when demultiplexing");

            // Make sure the working directory is empty
            ToolRunnerBase.CleanWorkDir(mWorkDir);

            // Copy the UIMF file from the storage server to the working directory

            var returnData = CopyDotDToWorkDir(dotDFileName, new ToolReturnData(), out var dotDRemoteEncodedFileNamePath, out var dotDLocalEncodedFileNamePath);
            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return returnData;
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
                        errorMessage = "Error demultiplexing Agilent IMS .D file";
                    }

                    returnData.CloseoutMsg = errorMessage;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return returnData;
                }
            }
            catch (Exception ex)
            {
                msg = "Exception calling DemultiplexFile for dataset " + mDataset;
                OnErrorEvent(msg, ex);
                returnData.CloseoutMsg = "Error demultiplexing Agilent IMS .D file";
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
            }

            // Look for the demultiplexed .D directory
            var localDotDDecodedFilePath = Path.Combine(mWorkDir, mDataset + DECODED_dotD_SUFFIX);

            if (!Directory.Exists(localDotDDecodedFilePath))
            {
                returnData.CloseoutMsg = "Decoded Agilent IMS .D file not found";
                OnErrorEvent(returnData.CloseoutMsg + ": " + localDotDDecodedFilePath);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return returnData;
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
                // Rename Agilent IMS .D file on storage server
                msg = "Renaming Agilent IMS .D file on storage server";
                OnDebugEvent(msg);

                // If this is a re-run, the encoded file has already been renamed
                // This is determined by looking for "_encoded" in the UIMF file name
                if (!dotDFileName.Contains(ENCODED_dotD_SUFFIX))
                {
                    if (!RenameDirectory(dotDRemoteEncodedFileNamePath, Path.Combine(mDatasetDirectoryPathRemote, mDataset + ENCODED_dotD_SUFFIX)))
                    {
                        returnData.CloseoutMsg = "Error renaming encoded Agilent IMS .D file on storage server";
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        postProcessingError = true;
                    }
                }
            }

            if (!postProcessingError)
            {
                // Copy the result files to the storage server
                if (!CopyDotDFileToStorageServer(returnData, localDotDDecodedFilePath, "demultiplexed Agilent IMS .D"))
                {
                    postProcessingError = true;
                }
            }

            if (postProcessingError)
            {
                try
                {
                    // Delete the multiplexed Agilent IMS .D file (no point in saving it)
                    mFileTools.DeleteDirectory(dotDLocalEncodedFileNamePath);
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch
                {
                    // Ignore errors deleting the multiplexed Agilent IMS .D file
                }

                // Try to save the demultiplexed Agilent IMS .D file (and any other files in the work directory)
                var failedResultsCopier = new FailedResultsCopier(mgrParams, taskParams);
                failedResultsCopier.CopyFailedResultsToArchiveDirectory(mWorkDir);

                return returnData;
            }

            // Delete local .D directories and file(s)
            msg = "Cleaning up working directory";
            OnDebugEvent(msg);
            try
            {
                if (!keepLocalOutput)
                {
                    mFileTools.DeleteDirectory(localDotDDecodedFilePath);
                }

                mFileTools.DeleteDirectory(dotDLocalEncodedFileNamePath);
            }
            catch (Exception ex)
            {
                // Error deleting files; don't treat this as a fatal error
                msg = "Exception deleting working directory file(s): " + ex.Message;
                OnErrorEvent(msg);
            }

            // Update the return data
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            returnData.EvalMsg = "De-multiplexed";

            return returnData;
        }

        /// <summary>
        /// Copies a file, allowing for retries
        /// </summary>
        /// <param name="sourceFilePath">Source file</param>
        /// <param name="targetFilePath">Destination file</param>
        /// <param name="overWrite">True to overWrite existing files</param>
        /// <param name="retryCount">Number of attempts</param>
        /// <param name="backupDestFileBeforeCopy">If True and if the target file exists, renames the target file to have _Old1 before the extension</param>
        /// <returns>True if success, false if an error</returns>
        private bool CopyDirectoryWithRetry(string sourceFilePath, string targetFilePath, bool overWrite, int retryCount, bool backupDestFileBeforeCopy = false)
        {
            OnCopyFileWithRetry(sourceFilePath, targetFilePath);
            return CopyDirectoryWithRetry(sourceFilePath, targetFilePath, overWrite, retryCount, backupDestFileBeforeCopy, mFileTools);
        }

        /// <summary>
        /// Copies a file, allowing for retries
        /// </summary>
        /// <param name="sourceFilePath">Source file</param>
        /// <param name="targetFilePath">Destination file</param>
        /// <param name="overWrite">True to overWrite existing files</param>
        /// <param name="retryCount">Number of attempts</param>
        /// <param name="backupDestFileBeforeCopy">If True and if the target file exists, renames the target file to have _Old1 before the extension</param>
        /// <param name="fileTools">Instance of FileTools</param>
        /// <returns>True if success, false if an error</returns>
        public static bool CopyDirectoryWithRetry(
            string sourceFilePath,
            string targetFilePath,
            bool overWrite,
            int retryCount,
            bool backupDestFileBeforeCopy,
            FileTools fileTools)
        {
            var retryingCopy = false;

            if (retryCount < 0)
            {
                retryCount = 0;
            }

            if (backupDestFileBeforeCopy)
            {
                FileTools.BackupFileBeforeCopy(targetFilePath);
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

                    return fileTools.CopyDirectoryWithResume(sourceFilePath, targetFilePath, true,
                        (overWrite ? FileTools.FileOverwriteMode.AlwaysOverwrite : FileTools.FileOverwriteMode.DoNotOverwrite),
                        new List<string>());
                }
                catch (Exception ex)
                {
                    msg = "Exception copying file " + sourceFilePath + " to " + targetFilePath + ": " + ex.Message;
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
        private bool CopyDotDFileToStorageServer(ToolReturnData returnData, string localDotDDecodedFilePath, string fileDescription)
        {
            var success = true;

            // Copy the demultiplexed file to the storage server, renaming as DatasetName.d in the process
            var msg = "Copying " + fileDescription + " file to storage server";
            OnDebugEvent(msg);
            const int retryCount = 3;
            if (!CopyDirectoryWithRetry(localDotDDecodedFilePath, Path.Combine(mDatasetDirectoryPathRemote, mDataset + ".d"), true, retryCount))
            {
                returnData.CloseoutMsg = AppendToString(returnData.CloseoutMsg, "Error copying " + fileDescription + " file to storage server");
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                success = false;
            }

            return success;
        }

        public static string AppendToString(string currentText, string newText)
        {
            return AppendToString(currentText, newText, "; ");
        }

        public static string AppendToString(string currentText, string newText, string separator)
        {
            if (string.IsNullOrEmpty(currentText))
            {
                return newText;
            }

            return currentText + separator + newText;
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

            // NOTE: PNNL-PreProcessor does not allow setting the output file path.
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

        /// <summary>
        /// Renames a file
        /// </summary>
        /// <param name="sourceFilePath">Original file path</param>
        /// <param name="newFilePath">New file path</param>
        /// <returns>True if successful, false if an error</returns>
        private bool RenameDirectory(string sourceFilePath, string newFilePath)
        {
            try
            {
                var di = new DirectoryInfo(sourceFilePath);
                di.MoveTo(newFilePath);
                return true;
            }
            catch (Exception ex)
            {
                var msg = "Exception renaming file " + sourceFilePath + " to " + Path.GetFileName(newFilePath) + ": " + ex.Message;
                OnErrorEvent(msg);

                // Garbage collect, then try again to rename the file
                System.Threading.Thread.Sleep(250);
                ProgRunner.GarbageCollectNow();
                System.Threading.Thread.Sleep(250);

                try
                {
                    var di = new DirectoryInfo(sourceFilePath);
                    di.MoveTo(newFilePath);
                    return true;
                }
                catch (Exception ex2)
                {
                    msg = "Rename failed despite garbage collection call: " + ex2.Message;
                    OnErrorEvent(msg);
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
                // NOTE: PNNL-PreProcessor does not allow setting the output file path.
                //arguments += " /N:" + Conversion.PossiblyQuotePath(outputFile.Name);

                arguments += " -demux " + demuxOptions.FramesToSum;
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
            var datasetDirectory = taskParams.GetParam(taskParams.HasParam("Directory") ? "Directory" : "Folder");

            mDatasetDirectoryPathRemote = Path.Combine(svrPath, datasetDirectory);
        }

        /// <summary>
        /// Examines the Log_Entries table to make sure the .UIMF file was demultiplexed
        /// </summary>
        /// <param name="localDotDDecodedFilePath"></param>
        /// <param name="returnData"></param>
        /// <returns>True if it was demultiplexed, otherwise false</returns>
        private bool ValidateDotDDemultiplexed(string localDotDDecodedFilePath, ToolReturnData returnData)
        {
            bool dotDDemultiplexed;
            string msg;

            // Make sure the Preprocessor log contains entry "Demultiplexing finished!" (with today's date)
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
                dotDDemultiplexed = false;
            }
            else
            {
                if (DateTime.Now.Subtract(demultiplexingFinished).TotalMinutes < 5)
                {
                    msg = "Demultiplexing finished message in PNNL-PreProcessorLog.txt file has date " + demultiplexingFinished;
                    OnDebugEvent(msg);
                    dotDDemultiplexed = true;
                }
                else
                {
                    returnData.CloseoutMsg = "Demultiplexing finished message in PNNL-PreProcessorLog.txt file is more than 5 minutes old";
                    msg = returnData.CloseoutMsg + ": " + demultiplexingFinished + "; assuming this is a demultiplexing failure";
                    if (!string.IsNullOrEmpty(message))
                    {
                        msg += "; " + message;
                    }

                    OnErrorEvent(msg);
                    dotDDemultiplexed = false;
                }
            }

            return dotDDemultiplexed;
        }

        private DateTime GetDemultiplexingFinishedFromLog(string dotDPath, out string message)
        {
            message = "";

            const string logSubPath = @"AcqData\PNNL-PreProcessorLog.txt";
            var logPath = Path.Combine(dotDPath, logSubPath);

            if (!File.Exists(logPath))
            {
                message = $"File not found: {logPath}";
                return DateTime.MinValue;
            }

            using (var fs = new FileStream(logPath, FileMode.Open, FileAccess.ReadWrite))
            using (var sr = new StreamReader(fs))
            {
                var lastDate = DateTime.MinValue;
                var foundFinish = false;
                string line;

                while ((line = sr.ReadLine()) != null)
                {
                    if (line.StartsWith("---") && DateTime.TryParse(line.Trim(' ', '-'), out var time))
                    {
                        lastDate = time;
                        foundFinish = false;
                    }

                    if (line.Equals("Demultiplexing finished!", StringComparison.OrdinalIgnoreCase))
                    {
                        foundFinish = true;
                    }
                }

                if (foundFinish)
                {
                    return lastDate;
                }

                message = "Did not find message 'Demultiplexing finished!'";
                return DateTime.MinValue;
            }
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
            OnErrorEvent("CmdRunner timeout reported");
        }

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds < 30)
            {
                return;
            }

            mLastProgressUpdateTime = DateTime.UtcNow;

            var toolName = "UIMFDemultiplexer";
            ParseConsoleOutputFileDemux();

            DemuxProgress?.Invoke(mDemuxProgressPercentComplete);

            if (DateTime.UtcNow.Subtract(mLastProgressMessageTime).TotalSeconds >= 300)
            {
                mLastProgressMessageTime = DateTime.UtcNow;
                OnDebugEvent(string.Format("{0} running; {1:F1} minutes elapsed, {2:F1}% complete",
                                           toolName,
                                           DateTime.UtcNow.Subtract(mDemuxStartTime).TotalMinutes,
                                           mDemuxProgressPercentComplete));
            }
        }
    }
}
