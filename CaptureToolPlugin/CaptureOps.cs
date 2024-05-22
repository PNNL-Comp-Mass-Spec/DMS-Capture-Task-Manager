//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/25/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CaptureTaskManager;
using CaptureToolPlugin.DataCapture;
using PRISM;

namespace CaptureToolPlugin
{
    /// <summary>
    /// Dataset capture plugin
    /// </summary>
    public class CaptureOps : LoggerBase
    {
        // Ignore Spelling: acq, bio, bionet, Bruker, dotnet, fso, idx, jpg, lcMethod, mcf, na, prepend, Pwd, secfso, ser, Subfolder, Unsubscribe, Username

        private enum DatasetDirectoryState
        {
            Empty,
            NotEmpty,
            Error
        }

        private readonly IMgrParams mMgrParams;

        // True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
        // False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1
        //
        // The capture task managers running on the Proto-x servers have "perspective" = "server"
        // Capture tasks that occur on the Proto-x servers should be limited to certain instruments via table T_Processor_Instrument in the DMS_Capture DB
        // If a capture task manager running on a Proto-x server has the DatasetCapture tool enabled, yet does not have an entry in T_Processor_Instrument,
        //  then no capture tasks are allowed to be assigned to avoid drive path problems
        private bool mClientServer;

        private readonly bool mUseBioNet;
        private readonly bool mTraceMode;

        /// <summary>
        /// Username for connecting to bionet
        /// </summary>
        private readonly string mUserName = string.Empty;

        /// <summary>
        /// Encoded password for the bionet user
        /// </summary>
        private readonly string mPassword = string.Empty;

        private readonly SharedState mToolState = new();
        private readonly ShareConnection mShareConnection;

        private readonly DatasetFileSearchTool mDatasetFileSearchTool;
        private readonly FileTools mFileTools;

        private DateTime mLastProgressUpdate = DateTime.Now;

        private string mLastProgressFileName = string.Empty;
        private float mLastProgressPercent = -1;
        private bool mFileCopyEventsWired;
        private readonly bool mIsLcDataCapture;

        /// <summary>
        /// Set to true if an error occurs connecting to the source computer
        /// </summary>
        public bool NeedToAbortProcessing => mToolState.NeedToAbortProcessing;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="fileTools">Instance of FileTools</param>
        /// <param name="useBioNet">Flag to indicate if source instrument is on Bionet</param>
        /// <param name="traceMode">When true, show debug messages at the console</param>
        /// <param name="isLcCapture">When true, this is an LC data capture operation</param>
        public CaptureOps(IMgrParams mgrParams, FileTools fileTools, bool useBioNet, bool traceMode, bool isLcCapture)
        {
            mMgrParams = mgrParams;
            mTraceMode = traceMode;
            mIsLcDataCapture = isLcCapture;

            // Get client/server perspective
            //   True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
            //   False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1
            var tmpParam = mMgrParams.GetParam("perspective");
            mClientServer = string.Equals(tmpParam, "client", StringComparison.OrdinalIgnoreCase);

            // Setup for Bionet use, if applicable
            mUseBioNet = useBioNet;

            if (mUseBioNet)
            {
                mUserName = mMgrParams.GetParam("BionetUser");
                mPassword = mMgrParams.GetParam("BionetPwd");

                if (!mUserName.Contains(@"\"))
                {
                    // Prepend this computer's name to the username
                    mUserName = System.Net.Dns.GetHostName() + @"\" + mUserName;
                }
            }

            mShareConnection = new ShareConnection(mToolState, mgrParams, useBioNet);

            mFileTools = fileTools;

            // Note that all the events and methods in FileTools are static
            if (!mFileCopyEventsWired)
            {
                mFileCopyEventsWired = true;
                mFileTools.CopyingFile += OnCopyingFile;
                mFileTools.FileCopyProgress += OnFileCopyProgress;
                mFileTools.ResumingFileCopy += OnResumingFileCopy;
                mFileTools.ErrorEvent += ErrorEventHandler;
                mFileTools.WarningEvent += WarningEventHandler;
            }

            mDatasetFileSearchTool = new DatasetFileSearchTool(mTraceMode);
            RegisterEvents(mDatasetFileSearchTool);
        }

        /// <summary>
        /// Unsubscribe from events
        /// </summary>
        public void DetachEvents()
        {
            if (mFileCopyEventsWired && mFileTools != null)
            {
                mFileCopyEventsWired = false;
                mFileTools.CopyingFile -= OnCopyingFile;
                mFileTools.FileCopyProgress -= OnFileCopyProgress;
                mFileTools.ResumingFileCopy -= OnResumingFileCopy;
            }
        }

        /// <summary>
        /// Finds files and/or subdirectories at datasetDirectoryPath that need to be renamed to start with x_
        /// </summary>
        /// <remarks>Does not rename LCMethod*.xml files</remarks>
        /// <param name="datasetDirectoryPath">Dataset directory path</param>
        /// <param name="pendingRenames">Files and/or directories to rename</param>
        /// <returns>True if successful, false if an error</returns>
        private bool FindSupersededFiles(string datasetDirectoryPath, IDictionary<FileSystemInfo, string> pendingRenames)
        {
            try
            {
                var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);

                if (!datasetDirectory.Exists)
                {
                    return true;
                }

                var foundFiles = datasetDirectory.GetFiles();
                var filesToSkip = datasetDirectory.GetFiles("LCMethod*.xml").ToList();

                // Also skip files with extensions .#FilePart# or .#FilePartInfo#
                filesToSkip.AddRange(datasetDirectory.GetFiles("*.#FilePart*#"));

                // Rename superseded files (but skip LCMethod files)
                foreach (var fileToRename in foundFiles)
                {
                    // Rename the file, but only if it is not in filesToSkip
                    var skipFile = filesToSkip.Any(fileToSkip => fileToSkip.FullName == fileToRename.FullName);

                    if (fileToRename.Name.StartsWith("x_") && foundFiles.Length == 1)
                    {
                        // File was previously renamed, and it is the only file in this directory; don't rename it again
                        continue;
                    }

                    if (skipFile)
                    {
                        continue;
                    }

                    var newFilePath = Path.Combine(datasetDirectory.FullName, "x_" + fileToRename.Name);
                    pendingRenames.Add(fileToRename, newFilePath);
                }

                // Rename any superseded subdirectories
                var targetSubdirectories = datasetDirectory.GetDirectories();

                foreach (var subdirectoryToRename in targetSubdirectories)
                {
                    if (subdirectoryToRename.Name.StartsWith("x_") && targetSubdirectories.Length == 1)
                    {
                        // Subdirectory was previously renamed, and it is the only Subdirectory in this directory; don't rename it again
                        continue;
                    }

                    var newSubDirPath = Path.Combine(datasetDirectory.FullName, "x_" + subdirectoryToRename.Name);
                    pendingRenames.Add(subdirectoryToRename, newSubDirPath);
                }

                if (pendingRenames.Count == 1)
                {
                    switch (pendingRenames.Keys.First())
                    {
                        case FileInfo fileToRename:
                            LogMessage("Found 1 file to prepend with x_, {0}", fileToRename.Name);
                            break;
                        case DirectoryInfo directoryToRename:
                            LogMessage("Found 1 directory to prepend with x_, {0}", directoryToRename.Name);
                            break;
                    }
                }
                else if (pendingRenames.Count > 1)
                {
                    LogMessage("Found {0} files/directories to prepend with x_", pendingRenames.Count);
                }

                return true;
            }
            catch (Exception ex)
            {
                mToolState.ErrorMessage = "Exception finding files/directories to rename with x_";
                LogError(mToolState.ErrorMessage + " in " + datasetDirectoryPath, true);
                LogError("Stack trace", ex);
                return false;
            }
        }

        /// <summary>
        /// Renames files and subdirectories in pendingRenames to start with x_
        /// </summary>
        /// <param name="datasetDirectoryPath"></param>
        /// <param name="pendingRenames">Files and/or directories to rename</param>
        /// <returns>True if successful, false if an error</returns>
        private bool MarkSupersededFiles(string datasetDirectoryPath, IReadOnlyDictionary<FileSystemInfo, string> pendingRenames)
        {
            try
            {
                var filesRenamed = 0;
                var directoriesRenamed = 0;

                foreach (var fileOrDirectoryToRename in pendingRenames)
                {
                    switch (fileOrDirectoryToRename.Key)
                    {
                        case FileInfo fileToRename:
                            var newFilePath = fileOrDirectoryToRename.Value;

                            if (string.IsNullOrWhiteSpace(newFilePath))
                            {
                                LogWarning("New name not defined in pendingRenames for {0}; cannot mark file as superseded",
                                    fileToRename.FullName);
                                continue;
                            }

                            if (File.Exists(newFilePath))
                            {
                                // Target exists; delete it
                                LogMessage("Addition of x_ to {0} will replace an existing file; deleting {1}",
                                    fileToRename.FullName, Path.GetFileName(newFilePath));
                                File.Delete(newFilePath);
                            }
                            fileToRename.MoveTo(newFilePath);
                            filesRenamed++;
                            continue;

                        case DirectoryInfo directoryToRename:
                            var newDirectoryPath = fileOrDirectoryToRename.Value;

                            if (string.IsNullOrWhiteSpace(newDirectoryPath))
                            {
                                LogWarning("New name not defined in pendingRenames for {0}; cannot mark directory as superseded",
                                    directoryToRename.FullName);
                                continue;
                            }

                            if (Directory.Exists(newDirectoryPath))
                            {
                                // Target exists; delete it
                                LogMessage("Addition of x_ to {0} will replace an existing subdirectory; deleting {1}",
                                    directoryToRename.FullName, Path.GetFileName(newDirectoryPath));
                                Directory.Delete(newDirectoryPath, true);
                            }
                            directoryToRename.MoveTo(newDirectoryPath);
                            directoriesRenamed++;
                            continue;
                    }
                }

                if (filesRenamed > 0)
                {
                    LogMessage("Renamed {0} superseded file(s) at {1} to start with x_",
                        filesRenamed, datasetDirectoryPath);
                }

                if (directoriesRenamed > 0)
                {
                    LogMessage("Renamed {0} superseded subdirectory(s) at {1} to start with x_",
                        directoriesRenamed, datasetDirectoryPath);
                }

                return true;
            }
            catch (Exception ex)
            {
                mToolState.ErrorMessage = "Exception renaming files/directories to rename with x_";
                LogError(mToolState.ErrorMessage + " in " + datasetDirectoryPath, true);
                LogError("Stack trace", ex);
                return false;
            }
        }

        /// <summary>
        /// Checks to determine if specified directory is empty
        /// </summary>
        /// <param name="directoryPath">Full path specifying directory to be checked</param>
        /// <param name="fileCount">Output parameter: number of files</param>
        /// <param name="instrumentDataDirCount">Output parameter: number of instrument directories (typically .D directories)</param>
        /// <param name="nonInstrumentDataDirCount">Output parameter: number of directories (excluding directories included in instrumentDataDirCount)</param>
        /// <returns>
        /// 0 if directory is empty
        /// 1 if not empty
        /// 2 if an error
        /// </returns>
        private DatasetDirectoryState IsDatasetDirectoryEmpty(
            string directoryPath,
            out int fileCount,
            out int instrumentDataDirCount,
            out int nonInstrumentDataDirCount)
        {
            fileCount = 0;
            instrumentDataDirCount = 0;
            nonInstrumentDataDirCount = 0;

            try
            {
                var datasetDirectory = new DirectoryInfo(directoryPath);

                // Check for files
                fileCount = datasetDirectory.GetFiles().Length;

                // Check for .D directories
                // (Future: check for other directory extensions)
                instrumentDataDirCount = datasetDirectory.GetDirectories("*.d").Length;

                // Check for non-instrument directories
                nonInstrumentDataDirCount = datasetDirectory.GetDirectories().Length - instrumentDataDirCount;

                if (fileCount > 0)
                {
                    return DatasetDirectoryState.NotEmpty;
                }

                if (nonInstrumentDataDirCount + instrumentDataDirCount > 0)
                {
                    return DatasetDirectoryState.NotEmpty;
                }
            }
            catch (Exception ex)
            {
                // Something bad happened
                mToolState.ErrorMessage = "Error checking for empty dataset directory";

                LogError(mToolState.ErrorMessage + ": " + directoryPath, true);
                LogError("Stack trace", ex);
                return DatasetDirectoryState.Error;
            }

            // If we got to here, the directory is empty
            return DatasetDirectoryState.Empty;
        }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Performs action specified by DSFolderExistsAction manager parameter if a dataset directory already exists
        /// </summary>
        /// <remarks>
        /// If both maxFileCountToAllowResume and maxInstrumentDirCountToAllowResume are zero,
        /// will require that a minimum number of subdirectories or files be present to allow for CopyToResume to be used
        /// </remarks>
        /// <param name="datasetDirectoryPath">Full path to the dataset directory</param>
        /// <param name="copyWithResume">True when we will be using Copy with Resume to capture this instrument's data</param>
        /// <param name="maxFileCountToAllowResume">
        /// Maximum number of files that can exist in the dataset directory if we are going to allow CopyWithResume to be used</param>
        /// <param name="maxInstrumentDirCountToAllowResume">
        /// Maximum number of instrument subdirectories (at present, .D directories) that can exist in the dataset directory
        /// if we are going to allow CopyWithResume to be used
        /// </param>
        /// <param name="maxNonInstrumentDirCountToAllowResume">
        /// Maximum number of non-instrument subdirectories that can exist in the dataset directory if we are going to allow CopyWithResume to be used</param>
        /// <param name="returnData">Return data</param>
        /// <param name="pendingRenames">Files and/or directories to rename</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool PerformDSExistsActions(
            string datasetDirectoryPath,
            bool copyWithResume,
            int maxFileCountToAllowResume,
            int maxInstrumentDirCountToAllowResume,
            int maxNonInstrumentDirCountToAllowResume,
            ToolReturnData returnData,
            IDictionary<FileSystemInfo, string> pendingRenames)
        {
            var switchResult = false;

            var directoryState = IsDatasetDirectoryEmpty(datasetDirectoryPath,
                                                         out var fileCount,
                                                         out var instrumentDataDirCount,
                                                         out var nonInstrumentDataDirCount);
            switch (directoryState)
            {
                case DatasetDirectoryState.Empty:
                    // Directory is empty; all is good
                    switchResult = true;
                    break;

                case DatasetDirectoryState.Error:
                    // There was an error attempting to determine the dataset directory contents
                    // (Error reporting was handled by call to IsDatasetDirectoryEmpty above)
                    break;

                case DatasetDirectoryState.NotEmpty:
                    var directoryExistsAction = mMgrParams.GetParam("DSFolderExistsAction");

                    switch (directoryExistsAction.ToLower())
                    {
                        case "overwrite_single_item":
                            // If the directory only contains one or two files or only one subdirectory, we're likely retrying capture.
                            // Rename the one file to start with x_

                            var tooManyFilesOrDirectories = false;
                            var directoryCount = maxInstrumentDirCountToAllowResume + maxNonInstrumentDirCountToAllowResume;

                            if (maxFileCountToAllowResume > 0 || maxInstrumentDirCountToAllowResume + maxNonInstrumentDirCountToAllowResume > 0)
                            {
                                if (fileCount > maxFileCountToAllowResume ||
                                    instrumentDataDirCount > maxInstrumentDirCountToAllowResume ||
                                    nonInstrumentDataDirCount > maxNonInstrumentDirCountToAllowResume)
                                {
                                    tooManyFilesOrDirectories = true;
                                }
                            }
                            else
                            {
                                if (directoryCount == 0 && fileCount > 2 || fileCount == 0 && directoryCount > 1)
                                {
                                    tooManyFilesOrDirectories = true;
                                }
                            }

                            if (!tooManyFilesOrDirectories)
                            {
                                if (copyWithResume)
                                {
                                    // Do not rename the directory or file; leave as-is, and we'll resume the copy
                                    switchResult = true;
                                }
                                else
                                {
                                    switchResult = FindSupersededFiles(datasetDirectoryPath, pendingRenames);
                                }
                            }
                            else
                            {
                                if (directoryCount == 0 && copyWithResume)
                                {
                                    // Do not rename the files; leave as-is and we'll resume the copy
                                    switchResult = true;
                                }
                                else
                                {
                                    // Fail the capture task
                                    returnData.CloseoutMsg = "Dataset directory already exists and has multiple files or subdirectories";
                                    LogError(returnData.CloseoutMsg + ": " + datasetDirectoryPath, true);
                                }
                            }

                            break;

                        case "delete":
                            // Attempt to delete dataset directory
                            try
                            {
                                Directory.Delete(datasetDirectoryPath, true);
                                switchResult = true;
                            }
                            catch (Exception ex)
                            {
                                returnData.CloseoutMsg = "Dataset directory already exists and cannot be deleted";
                                LogError(returnData.CloseoutMsg + ": " + datasetDirectoryPath, true);
                                LogError("Stack trace", ex);

                                switchResult = false;
                            }
                            break;

                        case "rename":
                            // Attempt to rename dataset directory
                            // (If the rename fails, it should have been logged via a previous call to RenameDatasetDirectory)
                            if (RenameDatasetDirectory(datasetDirectoryPath))
                            {
                                switchResult = true;
                            }
                            break;

                        case "fail":
                            // Fail the capture task
                            returnData.CloseoutMsg = "Dataset directory already exists";
                            var directoryExists = returnData.CloseoutMsg + ": " + datasetDirectoryPath;

                            LogError(directoryExists, true);
                            break;

                        default:
                            // An invalid value for directoryExistsAction was specified

                            returnData.CloseoutMsg = "Dataset directory already exists; Invalid action " + directoryExistsAction + " specified";
                            var invalidAction = returnData.CloseoutMsg + " (" + datasetDirectoryPath + ")";

                            LogError(invalidAction, true);
                            break;
                    }   // directoryExistsAction selection
                    break;

                default:
                    throw new Exception("Unrecognized enum value in PerformDSExistsActions: " + directoryState);
            }

            return switchResult;
        }

        /// <summary>
        /// Prefixes specified directory name with "x_"
        /// </summary>
        /// <param name="directoryPath">Full path specifying directory to be renamed</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private bool RenameDatasetDirectory(string directoryPath)
        {
            try
            {
                var targetDirectory = new DirectoryInfo(directoryPath);

                if (targetDirectory.Parent == null)
                {
                    return true;
                }

                var newDirectoryPath = Path.Combine(targetDirectory.Parent.FullName, "x_" + targetDirectory.Name);
                targetDirectory.MoveTo(newDirectoryPath);

                if (Directory.Exists(newDirectoryPath))
                {
                    mToolState.ErrorMessage = "Cannot add x_ to directory; the target already exists: " + newDirectoryPath;
                    LogError(mToolState.ErrorMessage);
                    return false;
                }

                LogMessage("Added x_ to directory " + directoryPath);

                return true;
            }
            catch (Exception ex)
            {
                mToolState.ErrorMessage = "Error adding x_ to directory " + directoryPath;
                LogError(mToolState.ErrorMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Perform a single capture operation
        /// </summary>
        /// <param name="taskParams">Enum indicating status of task</param>
        /// <param name="returnData">Return data class; update CloseoutMsg or EvalMsg with text to store in T_Task_Step_Params</param>
        /// <returns>True if success or false if an error.  returnData includes addition details on errors</returns>
        public bool DoOperation(ITaskParams taskParams, ToolReturnData returnData)
        {
            var datasetName = taskParams.GetParam("Dataset");

            var instClassName = taskParams.GetParam("Instrument_Class");                    // Examples: Finnigan_Ion_Trap, LTQ_FT, Triple_Quad, IMS_Agilent_TOF, Agilent_Ion_Trap
            var instrumentClass = InstrumentClassInfo.GetInstrumentClass(instClassName);    // Enum of instrument class type
            var instrumentName = taskParams.GetParam("Instrument_Name");                    // Instrument name

            if (mIsLcDataCapture)
            {
                // Acquisition times and length; invalid and unused for MS data capture, needed for LC data capture
                var acqStartTime = taskParams.GetParamAsDate("Acq_Time_Start", DateTime.MinValue);
                var acqEndTime = taskParams.GetParamAsDate("Acq_Time_End", DateTime.MinValue);
                var acqLengthMinutes = acqEndTime.Subtract(acqStartTime).TotalMinutes;

                DateTime acqEndTimeToUse;
                double acqLengthMinutesToUse;

                if (acqStartTime > DateTime.MinValue && acqEndTime > DateTime.MinValue)
                {
                    acqEndTimeToUse = acqEndTime;
                    acqLengthMinutesToUse = acqLengthMinutes;
                }
                else
                {
                    // Examine the start and end times tracked in T_Requested_Run
                    var requestStartTime = taskParams.GetParamAsDate("Request_Run_Start", DateTime.MinValue);
                    var requestEndTime = taskParams.GetParamAsDate("Request_Run_Finish", DateTime.MinValue);
                    var requestLengthMinutes = requestEndTime.Subtract(requestStartTime).TotalMinutes;

                    if (requestStartTime == DateTime.MinValue || requestEndTime == DateTime.MinValue)
                    {
                        // Acquisition start and end times must be set for the MS dataset before running LC data capture

                        returnData.CloseoutMsg = string.Format("MS acq start or end time is invalid: start '{0}', end '{1}'",
                                                               taskParams.GetParam("Acq_Time_Start"),
                                                               taskParams.GetParam("Acq_Time_End"));

                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return false;
                    }

                    acqEndTimeToUse = requestEndTime;
                    acqLengthMinutesToUse = requestLengthMinutes;
                }

                var dateTimeThreshold = acqEndTimeToUse.AddMinutes(30 + acqLengthMinutesToUse);

                if (dateTimeThreshold > DateTime.Now)
                {
                    // Don't run yet
                    // Assumptions that aren't always correct, but are good enough for this use:
                    // - All runs require column equilibration time (infusion and some other instrument runs don't need this)
                    // - Column equilibration is run after the MS acquisition (it can be run before MS acquisition)
                    // - Column equilibration time is the same as the MS run time (it can often be shorter, but rarely longer)
                    returnData.CloseoutMsg = string.Format("Minimum post-acq delay for LC data capture not reached; wait until " +
                                                           "{0:yyyy-MM-dd hh:mm:ss tt} (MS acq length + 30 minutes)", dateTimeThreshold);
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;

                    LogMessage(returnData.CloseoutMsg);
                    return false;
                }
            }

            // Confirm that the dataset name has no spaces
            if (datasetName.IndexOf(' ') >= 0)
            {
                returnData.CloseoutMsg = "Dataset name contains a space";
                LogError(returnData.CloseoutMsg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            // Confirm that the dataset name has no invalid characters
            if (NameHasInvalidCharacter(datasetName, "Dataset name", true, returnData))
            {
                return false;
            }

            // Determine whether we will use Copy with Resume for all files for this dataset
            // This determines if we add x_ to an existing file or directory,
            // and determines whether we use CopyDirectory or CopyDirectoryWithResume/CopyFileWithResume
            bool copyWithResume;

            // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
            switch (instrumentClass)
            {
                case InstrumentClass.BrukerFT_BAF:
                case InstrumentClass.BrukerMALDI_Imaging:
                case InstrumentClass.BrukerMALDI_Imaging_V2:
                    copyWithResume = true;
                    break;

                default:
                    copyWithResume = false;
                    break;
            }

            LogDebug("Started CaptureOps.DoOperation()");

            if (!PrepareTargetDirectory(taskParams, returnData, datasetName, instrumentClass, copyWithResume, out var datasetDirectoryPath, out var pendingRenames))
            {
                return false;
            }

            if (mIsLcDataCapture && instrumentClass == InstrumentClass.LCMSNet_LC)
            {
                // No data file to copy from a remote computer (except Proto-5)
                // Capture possible LC data
                var lcCapture = new LCDataCapture(mMgrParams);
                var noError = lcCapture.CaptureLCMethodFile(datasetName, datasetDirectoryPath, out var fileCopied);

                // CaptureLCMethodFile only returns false if a matching file was found but the copy failed
                // Make sure the state is set appropriately if no matching .lcmethod file was found.
                if (noError && !fileCopied)
                {
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_SKIPPED;
                    returnData.EvalMsg = $"No .lcmethod file found for dataset {datasetName}";
                    returnData.CloseoutMsg = returnData.EvalMsg;
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }

                return noError;
            }

            if (!CheckSourceFiles(taskParams, returnData, datasetName, instrumentClass, out var sourceDirectoryPath, out var datasetInfo))
            {
                if (mIsLcDataCapture)
                {
                    // Capture possible LC data, even if no LC data file was found
                    var lcCapture = new LCDataCapture(mMgrParams);
                    lcCapture.CaptureLCMethodFile(datasetName, datasetDirectoryPath);
                }

                return false;
            }

            // Now that the source has been verified, perform any pending renames
            var renameSuccess = MarkSupersededFiles(datasetDirectoryPath, pendingRenames);

            if (!renameSuccess)
            {
                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }

                if (string.IsNullOrEmpty(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "MarkSupersededFiles returned false";
                }
                return false;
            }

            var initData = new CaptureInitData(mToolState, mMgrParams, mFileTools, mShareConnection, mTraceMode);

            // Perform copy based on source type
            CaptureBase capture = datasetInfo.DatasetType switch
            {
                InstrumentFileLayout.File => new FileSingleCapture(initData),
                InstrumentFileLayout.MultiFile => new FileMultipleCapture(initData),
                InstrumentFileLayout.DirectoryExt => new DirectoryExtCapture(initData),
                InstrumentFileLayout.DirectoryNoExt => new DirectoryNoExtCapture(initData),
                InstrumentFileLayout.BrukerImaging => new BrukerImagingCapture(initData),
                InstrumentFileLayout.BrukerSpot => new BrukerSpotCapture(initData),
                _ => new UnknownCapture(initData),
            };

            capture.Capture(out var msg, returnData, datasetInfo, sourceDirectoryPath, datasetDirectoryPath, copyWithResume, instrumentClass, instrumentName, taskParams);

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS &&
                datasetInfo.DatasetType is not (InstrumentFileLayout.BrukerImaging or InstrumentFileLayout.BrukerSpot))
            {
                // Capture possible LC data
                var lcCapture = new LCDataCapture(mMgrParams);
                var success = lcCapture.CaptureLCMethodFile(datasetInfo.DatasetName, datasetDirectoryPath);

                // If LC Method capture failed, need make sure returnData.CloseoutType is not CLOSEOUT_SUCCESS
                if (!success && returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                }
            }

            PossiblyStoreErrorMessage(returnData);

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                return true;
            }

            if (string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
            {
                if (string.IsNullOrWhiteSpace(msg))
                {
                    returnData.CloseoutMsg = "Unknown error performing capture";
                }
                else
                {
                    returnData.CloseoutMsg = msg;
                }
            }

            return false;
        }

        /// <summary>
        /// Prepare the target directory for data capture
        /// </summary>
        /// <param name="taskParams">Task parameters</param>
        /// <param name="returnData">Return data class; update CloseoutMsg or EvalMsg with text to store in T_Task_Step_Params</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="instrumentClass">Instrument class - used for <paramref name="copyWithResume"/> support</param>
        /// <param name="copyWithResume">If 'copy with resume' is supported for the instrument class</param>
        /// <param name="datasetDirectoryPath">Directory path files should be copied to</param>
        /// <param name="pendingRenames">Files and/or directories to rename</param>
        /// <returns>False if error, and processing should exit.  returnData includes addition details on errors</returns>
        public bool PrepareTargetDirectory(
            ITaskParams taskParams,
            ToolReturnData returnData,
            string datasetName,
            InstrumentClass instrumentClass,
            bool copyWithResume,
            out string datasetDirectoryPath,
            out IReadOnlyDictionary<FileSystemInfo, string> pendingRenames)
        {
            var storageVol = taskParams.GetParam("Storage_Vol").Trim();                     // Example: E:\
            var storagePath = taskParams.GetParam("Storage_Path").Trim();                   // Example: Exact04\2012_1\
            var storageVolExternal = taskParams.GetParam("Storage_Vol_External").Trim();    // Example: \\proto-5\

            var instrumentName = taskParams.GetParam("Instrument_Name");                    // Instrument name
            var datasetDirectory = taskParams.GetParam("Directory");

            var computerName = System.Net.Dns.GetHostName();

            var pendingRenamesMap = new Dictionary<FileSystemInfo, string>();
            pendingRenames = pendingRenamesMap;

            string tempVol;

            if (string.IsNullOrWhiteSpace(datasetDirectory) ||
                (!datasetDirectory.Equals(datasetName, StringComparison.OrdinalIgnoreCase) &&
                 !(datasetDirectory.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)[0]).Equals(datasetName, StringComparison.OrdinalIgnoreCase)))
            {
                // datasetDirectory should either equal datasetName or be a path that starts with datasetName
                // e.g. for datasetName 'test': 'test' and 'test\subDir' are valid, 'test2' and test2\subDir are not

                returnData.CloseoutMsg = string.Format(
                    "The Directory task parameter is invalid since it does not start with the dataset name; expecting \"{0}\" but actually \"{1}\"",
                    datasetName, datasetDirectory);

                LogError(returnData.CloseoutMsg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                datasetDirectoryPath = string.Empty;
                return false;
            }

            // Setup Destination directory based on client/server switch, mClientServer
            // True means MgrParam "perspective" =  "client" which means we will use paths like \\proto-5\Exact04\2012_1
            // False means MgrParam "perspective" = "server" which means we use paths like E:\Exact04\2012_1

            if (!mClientServer)
            {
                // Look for job parameter Storage_Server_Name in storageVolExternal
                // If mClientServer=false but storageVolExternal does not contain Storage_Server_Name then auto-switch mClientServer to true

                if (storageVolExternal.IndexOf(computerName, StringComparison.OrdinalIgnoreCase) < 0)
                {
                    var autoEnableFlag = "AutoEnableClientServer_for_" + computerName;
                    var autoEnabledParamValue = mMgrParams.GetParam(autoEnableFlag, string.Empty);

                    if (string.IsNullOrEmpty(autoEnabledParamValue))
                    {
                        // Using a Manager Parameter to assure that the following log message is only logged once per session
                        // (in case this manager captures multiple datasets in a row)
                        mMgrParams.SetParam(autoEnableFlag, "True");
                        LogMessage("Auto-changing mClientServer to True (perspective=client) " +
                                   "because " + storageVolExternal.ToLower() + " does not contain " + computerName.ToLower());
                    }

                    mClientServer = true;
                }
            }

            if (mClientServer)
            {
                // Example: \\proto-5\
                if (!ValidateStoragePath(storageVolExternal, "Parameter Storage_Vol_External", @"\\proto-5", returnData))
                {
                    datasetDirectoryPath = string.Empty;
                    return false;
                }

                tempVol = storageVolExternal;
            }
            else
            {
                // Example: E:\
                if (!ValidateStoragePath(storageVol, "Parameter Storage_Vol", @"E:\", returnData))
                {
                    datasetDirectoryPath = string.Empty;
                    return false;
                }

                tempVol = storageVol;
            }

            // Set up paths

            if (!ValidateStoragePath(storagePath, "Parameter Storage_Path", @"Lumos01\2020_3", returnData))
            {
                datasetDirectoryPath = string.Empty;
                return false;
            }

            // Validate that storagePath includes both an instrument name and subdirectory name
            // Furthermore, the instrument name should match the current instrument add exceptions in the future if this becomes required
            var storagePathParts = storagePath.Split(Path.DirectorySeparatorChar).ToList();

            if (storagePathParts.Count > 1)
            {
                if (!ValidateStoragePathInstrument(instrumentName, storagePath, storagePathParts, returnData))
                {
                    datasetDirectoryPath = string.Empty;
                    return false;
                }
            }
            else
            {
                var storagePathPartsAlt = storagePath.Split(Path.AltDirectorySeparatorChar).ToList();

                if (!ValidateStoragePathInstrument(instrumentName, storagePath, storagePathPartsAlt, returnData))
                {
                    datasetDirectoryPath = string.Empty;
                    return false;
                }
            }

            // Directory on storage server where dataset directory goes
            var storageDirectoryPath = Path.Combine(tempVol, storagePath);

            // Confirm that the storage share has no invalid characters
            if (NameHasInvalidCharacter(storageDirectoryPath, "Storage share path", false, returnData))
            {
                datasetDirectoryPath = string.Empty;
                return false;
            }

            if (!ValidateStoragePath(storageDirectoryPath, "Path.Combine(tempVol, storagePath)", @"\\proto-8\Eclipse01\2020_3\", returnData))
            {
                datasetDirectoryPath = string.Empty;
                return false;
            }

            if (!string.IsNullOrWhiteSpace(taskParams.GetParam("Storage_Folder_Name")))
            {
                // Storage_Folder_Name is defined, use it instead of datasetName
                // e.g., HPLC run directory storage path
                datasetDirectoryPath = Path.Combine(storageDirectoryPath, taskParams.GetParam("Storage_Folder_Name"));
            }
            else
            {
                // Dataset directory complete path
                datasetDirectoryPath = Path.Combine(storageDirectoryPath, datasetDirectory);
            }

            // Confirm that the target dataset directory path has no invalid characters
            if (NameHasInvalidCharacter(datasetDirectoryPath, "Dataset directory path", false, returnData))
            {
                return false;
            }

            // Verify that the storage share on the storage server exists; e.g. \\proto-9\VOrbiETD02\2011_2
            if (!ValidateDirectoryPath(storageDirectoryPath))
            {
                LogMessage("Storage directory '" + storageDirectoryPath + "' does not exist; will auto-create");

                try
                {
                    Directory.CreateDirectory(storageDirectoryPath);
                    LogDebug("Successfully created " + storageDirectoryPath);
                }
                catch
                {
                    returnData.CloseoutMsg = "Error creating missing storage directory";
                    LogError(returnData.CloseoutMsg + ": " + storageDirectoryPath, true);

                    if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                    {
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    return false;
                }
            }

            // Verify that dataset directory path doesn't already exist or is empty
            // Example: \\proto-9\VOrbiETD02\2011_2\PTO_Na_iTRAQ_2_17May11_Owl_11-05-09

            if (!ValidateDirectoryPath(datasetDirectoryPath))
            {
                // The directory does not exist
                return true;
            }

            int maxFileCountToAllowResume;
            int maxInstrumentDirCountToAllowResume;
            int maxNonInstrumentDirCountToAllowResume;

            if (instrumentClass is InstrumentClass.BrukerMALDI_Imaging or InstrumentClass.BrukerMALDI_Imaging_V2)
            {
                maxFileCountToAllowResume = 20;
                maxInstrumentDirCountToAllowResume = 20;
                maxNonInstrumentDirCountToAllowResume = 1;
            }
            else
            {
                maxFileCountToAllowResume = 0;
                maxInstrumentDirCountToAllowResume = 0;
                maxNonInstrumentDirCountToAllowResume = 0;
            }

            // Dataset directory exists, so take action specified in configuration
            if (PerformDSExistsActions(
                    datasetDirectoryPath,
                    copyWithResume,
                    maxFileCountToAllowResume,
                    maxInstrumentDirCountToAllowResume,
                    maxNonInstrumentDirCountToAllowResume,
                    returnData,
                    pendingRenamesMap))
            {
                return true;
            }

            PossiblyStoreErrorMessage(returnData);

            if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            if (string.IsNullOrEmpty(returnData.CloseoutMsg))
            {
                returnData.CloseoutMsg = "PerformDSExistsActions returned false";
            }
            return false;
        }

        /// <summary>
        /// Check the source files/directory in preparation for copying
        /// </summary>
        /// <param name="taskParams">Task parameters</param>
        /// <param name="returnData">Return data class; update CloseoutMsg or EvalMsg with text to store in T_Task_Step_Params</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="instrumentClass">Instrument class to determine what the data format should be</param>
        /// <param name="sourceDirectoryPath">The determined path for the file/directory that should be copied</param>
        /// <param name="datasetInfo">Details on the files to be copied</param>
        /// <returns>False if error, and processing should exit.  returnData includes addition details on errors</returns>
        public bool CheckSourceFiles(ITaskParams taskParams, ToolReturnData returnData, string datasetName, InstrumentClass instrumentClass, out string sourceDirectoryPath, out DatasetInfo datasetInfo)
        {
            var jobNum = taskParams.GetParam("Job", 0);

            datasetInfo = null;

            if (!GetCapturePaths(taskParams, returnData, out var captureSubdirectory, out sourceDirectoryPath))
            {
                return false;
            }

            // If Source_Folder_Name is non-blank, use it. Otherwise, use dataset name
            var sourceFolderName = taskParams.GetParam("Source_Folder_Name");

            if (!string.IsNullOrWhiteSpace(sourceFolderName))
            {
                // Confirm that the source folder name has no invalid characters
                if (NameHasInvalidCharacter(sourceFolderName, "Job param Source_Folder_Name", true, returnData))
                {
                    return false;
                }
            }

            // Now that we've had a chance to connect to the share, possibly append a subdirectory to the source path
            if (!string.IsNullOrWhiteSpace(captureSubdirectory))
            {
                var sourceFolderOrDatasetName = string.IsNullOrWhiteSpace(sourceFolderName) ? datasetName : sourceFolderName;

                // However, if the subdirectory name matches the dataset name, this was probably an error on the operator's part,
                // and we likely do not want to use the subfolder name
                if (captureSubdirectory.EndsWith(Path.DirectorySeparatorChar + sourceFolderOrDatasetName, StringComparison.OrdinalIgnoreCase) ||
                    captureSubdirectory.Equals(sourceFolderOrDatasetName, StringComparison.OrdinalIgnoreCase))
                {
                    var candidateDirectoryPath = Path.Combine(sourceDirectoryPath, captureSubdirectory);

                    if (!Directory.Exists(candidateDirectoryPath))
                    {
                        // Leave sourceDirectoryPath unchanged
                        // Dataset Capture_Directory ends with the dataset name. Gracefully ignoring because this appears to be a data entry error; directory not found:
                        LogWarning("Dataset Capture_Subdirectory ends with the dataset name. Gracefully ignoring " +
                                   "because this appears to be a data entry error; directory not found: " + candidateDirectoryPath, true);
                    }
                    else
                    {
                        if (captureSubdirectory.Equals(sourceFolderOrDatasetName, StringComparison.OrdinalIgnoreCase))
                        {
                            LogWarning("Dataset Capture_Subdirectory is the dataset name; leaving the capture path as {0} " +
                                       "so that the entire dataset directory will be copied", sourceFolderOrDatasetName);
                        }
                        else
                        {
                            if (candidateDirectoryPath.EndsWith(Path.DirectorySeparatorChar + sourceFolderOrDatasetName, StringComparison.OrdinalIgnoreCase))
                            {
                                var candidateDirectoryPathTrimmed = candidateDirectoryPath.Substring(0, candidateDirectoryPath.Length - sourceFolderOrDatasetName.Length - 1);
                                LogMessage("Appending captureSubdirectory to sourceDirectoryPath, but removing SourceFolderName, giving: {0} (removed {1})",
                                    candidateDirectoryPathTrimmed, sourceFolderOrDatasetName);

                                sourceDirectoryPath = candidateDirectoryPathTrimmed;
                            }
                            else
                            {
                                LogMessage("Appending captureSubdirectory to sourceDirectoryPath, giving: " + candidateDirectoryPath);
                                sourceDirectoryPath = candidateDirectoryPath;
                            }
                        }
                    }
                }
                else
                {
                    sourceDirectoryPath = Path.Combine(sourceDirectoryPath, captureSubdirectory);
                }

                // Confirm that the source directory has no invalid characters
                if (NameHasInvalidCharacter(sourceDirectoryPath, "Source directory path with captureSubdirectory optionally added", false, returnData))
                {
                    return false;
                }
            }

            datasetInfo = mDatasetFileSearchTool.FindDatasetFileOrDirectory(sourceDirectoryPath, datasetName, instrumentClass);

            if (!string.Equals(datasetInfo.DatasetName, datasetName))
            {
                LogWarning("DatasetName in the datasetInfo object is {0}; changing to {1}",
                    datasetInfo.DatasetName, datasetName);

                datasetInfo.DatasetName = datasetName;
            }

            // Set the closeout type to Failed for now
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            bool sourceIsValid;

            if (datasetInfo.DatasetType == InstrumentFileLayout.None)
            {
                // No dataset file or directory found

                if (mUseBioNet)
                {
                    returnData.CloseoutMsg = "Dataset data file not found on Bionet at " + sourceDirectoryPath;
                }
                else
                {
                    returnData.CloseoutMsg = "Dataset data file not found at " + sourceDirectoryPath;
                }

                string directoryStatsMsg;

                if (string.IsNullOrWhiteSpace(sourceFolderName))
                {
                    directoryStatsMsg = ReportDirectoryStats(sourceDirectoryPath);
                    returnData.CloseoutMsg += "; empty SourceFolderName";
                }
                else
                {
                    directoryStatsMsg = ReportDirectoryStats(Path.Combine(sourceDirectoryPath, sourceFolderName));
                    returnData.CloseoutMsg += "; SourceFolderName: " + sourceFolderName;
                }

                LogError(returnData.CloseoutMsg + " (" + datasetName + ", job " + jobNum + "); " + directoryStatsMsg);

                if (mIsLcDataCapture)
                {
                    // For LC Data capture, if there is no dataset file we do not fail the capture, so report 'skipped'
                    returnData.EvalCode = EnumEvalCode.EVAL_CODE_SKIPPED;
                }

                sourceIsValid = false;
            }
            else
            {
                sourceIsValid = ValidateWithInstrumentClass(datasetName, sourceDirectoryPath, instrumentClass, datasetInfo, returnData);
            }

            if (!sourceIsValid)
            {
                if (string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "Dataset type (" + datasetInfo.DatasetType + ") is not valid for the instrument class (" + instrumentClass + ")";
                }

                return false;
            }

            return true;
        }

        /// <summary>
        /// Get the source directory and capture subdirectory, with handling for LCDataCapture vs. original dataset capture share redirection
        /// </summary>
        /// <param name="taskParams"></param>
        /// <param name="returnData"></param>
        /// <param name="captureSubdirectory"></param>
        /// <param name="sourceDirectoryPath"></param>
        /// <returns></returns>
        private bool GetCapturePaths(ITaskParams taskParams, ToolReturnData returnData, out string captureSubdirectory, out string sourceDirectoryPath)
        {
            // Capture_Subdirectory is typically an empty string, but could be a partial path like: "CapDev" or "Smith\2014"
            var legacyCaptureSubfolder = taskParams.GetParam("Capture_Subfolder").Trim();
            captureSubdirectory = taskParams.GetParam("Capture_Subdirectory", legacyCaptureSubfolder);
            var captureSubdirectoryCopy = captureSubdirectory;

            if (!GetSourceDirectoryPath(taskParams, returnData, ref captureSubdirectory, out sourceDirectoryPath))
            {
                // Usually a failure is caused by a connection failure on bionet; this can be because the share name doesn't exist (at least for LcDataCapture steps)
                // for LCDataCapture jobs with a capture subdirectory that is a different share, we should try removing the share change first.
                if (!mIsLcDataCapture || !captureSubdirectoryCopy.StartsWith(".."))
                {
                    return false;
                }

                LogMessage("LCDatasetCapture with capture subdirectory starting with '..'; attempting share connection again without share redirection");

                // split the original capture subdirectory on path separator characters
                var captureSubdirectorySplit = captureSubdirectoryCopy.Split(Path.PathSeparator);

                // re-create the capture subdirectory without the first 2 items in the path
                captureSubdirectory = string.Join(Path.DirectorySeparatorChar.ToString(), captureSubdirectorySplit.Skip(2));

                // Reset return data and tool state
                returnData.CloseoutMsg = "";
                returnData.EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                mToolState.ResetAbortProcessing();

                // Try connecting again
                return GetSourceDirectoryPath(taskParams, returnData, ref captureSubdirectory, out sourceDirectoryPath);
            }

            return true;
        }

        /// <summary>
        /// Get the source directory path, and connect to the bionet share (if needed)
        /// </summary>
        /// <param name="taskParams"></param>
        /// <param name="returnData"></param>
        /// <param name="captureSubdirectory"></param>
        /// <param name="sourceDirectoryPath"></param>
        /// <returns></returns>
        private bool GetSourceDirectoryPath(ITaskParams taskParams, ToolReturnData returnData, ref string captureSubdirectory, out string sourceDirectoryPath)
        {
            var sourceVol = taskParams.GetParam("Source_Vol").Trim();                       // Example: \\exact04.bionet\
            var sourcePath = taskParams.GetParam("Source_Path").Trim();                     // Example: ProteomicsData\

            var password = CTMUtilities.DecodePassword(mPassword);

            mDatasetFileSearchTool.VerifyRelativeSourcePath(sourceVol, ref sourcePath, ref captureSubdirectory);

            // Construct the path to the dataset on the instrument
            // Determine if source dataset exists, and if it is a file or a directory
            sourceDirectoryPath = Path.Combine(sourceVol, sourcePath);

            // Confirm that the source directory has no invalid characters
            if (NameHasInvalidCharacter(sourceDirectoryPath, "Source directory path", false, returnData))
            {
                return false;
            }

            // Connect to Bionet if necessary
            if (mUseBioNet)
            {
                LogDebug("Bionet connection required for " + sourceVol);

                if (!mShareConnection.ConnectToShare(mUserName, password, sourceDirectoryPath, out var closeoutType, out var evalCode))
                {
                    returnData.CloseoutType = closeoutType;
                    returnData.EvalCode = evalCode;

                    PossiblyStoreErrorMessage(returnData);

                    if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                    {
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    }

                    if (string.IsNullOrEmpty(returnData.CloseoutMsg))
                    {
                        returnData.CloseoutMsg = "Error connecting to Bionet share";
                    }
                    return false;
                }
            }
            else
            {
                LogDebug("Bionet connection not required for " + sourceVol);
            }

            return true;
        }

        /// <summary>
        /// Return true if the file or path has any invalid characters
        /// </summary>
        /// <param name="fileOrPath">Filename or full file/directory path</param>
        /// <param name="itemDescription">Description of fileOrPath; included in CloseoutMsg if there is a problem</param>
        /// <param name="isFile">True for a file; false for a path</param>
        /// <param name="returnData">Return data object</param>
        /// <returns>True if an error; false if no problems</returns>
        private static bool NameHasInvalidCharacter(string fileOrPath, string itemDescription, bool isFile, ToolReturnData returnData)
        {
            var invalidCharIndex = fileOrPath.IndexOfAny(isFile ? Path.GetInvalidFileNameChars() : Path.GetInvalidPathChars());

            if (invalidCharIndex < 0)
            {
                return false;
            }

            returnData.CloseoutMsg = string.IsNullOrWhiteSpace(itemDescription) ? fileOrPath : itemDescription;
            returnData.CloseoutMsg += " contains an invalid character at index " + invalidCharIndex + ": " + fileOrPath[invalidCharIndex];
            LogError(returnData.CloseoutMsg, true);
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            return true;
        }

        /// <summary>
        /// Store mToolState.ErrorMessage in returnData.CloseoutMsg if an error exists yet returnData.CloseoutMsg is empty
        /// </summary>
        /// <param name="returnData"></param>
        private void PossiblyStoreErrorMessage(ToolReturnData returnData)
        {
            if (!string.IsNullOrWhiteSpace(mToolState.ErrorMessage) && string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
            {
                returnData.CloseoutMsg = mToolState.ErrorMessage;

                if (mTraceMode)
                {
                    ToolRunnerBase.ShowTraceMessage(mToolState.ErrorMessage);
                }
            }
        }

        /// <summary>
        /// Report some stats on the given directory, including the number of files and the largest file
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <returns>String describing the directory; if a problem, reports Error: ErrorMsg </returns>
        private string ReportDirectoryStats(string directoryPath)
        {
            try
            {
                var targetDirectory = new DirectoryInfo(directoryPath);

                if (!targetDirectory.Exists)
                {
                    return "Error: directory not found, " + directoryPath;
                }

                var filesInDirectory = targetDirectory.GetFiles();
                float totalSizeKB = 0;
                var largestFileInfo = new KeyValuePair<long, string>(0, string.Empty);

                foreach (var file in filesInDirectory)
                {
                    totalSizeKB += file.Length / 1024.0f;

                    if (file.Length > largestFileInfo.Key)
                    {
                        largestFileInfo = new KeyValuePair<long, string>(file.Length, file.Name);
                    }
                }

                return string.Format("{0} files, {1:F1} KB total, largest file is {2}",
                                     filesInDirectory.Length, totalSizeKB, largestFileInfo.Value);
            }
            catch (Exception ex)
            {
                LogError("Error in ReportDirectoryStats", ex);
                return "Error: " + ex.Message;
            }
        }

        /// <summary>
        /// Verifies specified directory path exists
        /// </summary>
        /// <param name="directoryPath">Directory path to test</param>
        /// <returns>True if directory was found, otherwise false</returns>
        private bool ValidateDirectoryPath(string directoryPath)
        {
            return Directory.Exists(directoryPath);
        }

        /// <summary>
        /// Validates that the specified storage path is not an empty string or \ or /
        /// </summary>
        /// <param name="storagePathRoot"></param>
        /// <param name="rootPathDescription"></param>
        /// <param name="exampleRootPath"></param>
        /// <param name="returnData"></param>
        /// <returns>True if valid, otherwise false</returns>
        private bool ValidateStoragePath(string storagePathRoot, string rootPathDescription, string exampleRootPath, ToolReturnData returnData)
        {
            if (!string.IsNullOrWhiteSpace(storagePathRoot) && !storagePathRoot.Equals("\\") && !storagePathRoot.Equals(" /"))
            {
                return true;
            }

            returnData.CloseoutMsg = string.Format(
                "{0} is invalid ({1}); it should be {2} or similar",
                rootPathDescription, storagePathRoot, exampleRootPath);

            LogError(returnData.CloseoutMsg);
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            return false;
        }

        /// <summary>
        /// Verifies that the storage path starts with the instrument name and has two or more directories
        /// </summary>
        /// <param name="instrumentName"></param>
        /// <param name="storagePath"></param>
        /// <param name="storagePathParts"></param>
        /// <param name="returnData"></param>
        /// <returns>True if valid, otherwise false</returns>
        private bool ValidateStoragePathInstrument(
            string instrumentName,
            string storagePath,
            IReadOnlyCollection<string> storagePathParts,
            ToolReturnData returnData)
        {
            if (mIsLcDataCapture)
            {
                // Check will generally fail for LC data capture.
                return true;
            }

            if (storagePathParts.Count >= 2 && storagePathParts.First().Equals(instrumentName))
            {
                return true;
            }

            var exampleStoragePath = Path.Combine(instrumentName, "2020_3");

            returnData.CloseoutMsg = string.Format(
                "Parameter Storage_Path is invalid ({0}); it must start with the instrument name and should thus be {1} or similar",
                storagePath, exampleStoragePath);

            LogError(returnData.CloseoutMsg);
            returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            return false;
        }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Make sure that we matched a file for instruments that save data as a file, or a directory for instruments that save data to a directory
        /// </summary>
        /// <remarks>
        /// This method will update datasetInfo.DatasetType if it is MultiFile and we matched two files, where one of the files is a .sld file.
        /// It will also remove the .sld file from datasetInfo.FileList
        /// </remarks>
        /// <param name="dataset"></param>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="instrumentClass">Instrument class</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="returnData"></param>
        /// <returns>True if the file or directory is appropriate for the instrument class, otherwise false</returns>
        private bool ValidateWithInstrumentClass(
            string dataset,
            string sourceDirectoryPath,
            InstrumentClass instrumentClass,
            DatasetInfo datasetInfo,
            ToolReturnData returnData)
        {
            returnData.CloseoutMsg = string.Empty;

            var entityDescription = datasetInfo.DatasetType switch
            {
                InstrumentFileLayout.File => "a file",
                InstrumentFileLayout.DirectoryNoExt => "a directory",
                InstrumentFileLayout.DirectoryExt => "a directory",
                InstrumentFileLayout.BrukerImaging => "a directory",
                InstrumentFileLayout.BrukerSpot => "a directory",
                InstrumentFileLayout.MultiFile => "multiple files",
                _ => "an unknown entity"
            };

            // Make sure we are capturing the correct entity type (file or directory) based on instrumentClass
            // See table T_Instrument_Class for allowed types
            switch (instrumentClass)
            {
                case InstrumentClass.Finnigan_Ion_Trap:
                case InstrumentClass.GC_QExactive:
                case InstrumentClass.LTQ_FT:
                case InstrumentClass.Thermo_Exactive:
                case InstrumentClass.Triple_Quad:
                case InstrumentClass.Shimadzu_GC:
                case InstrumentClass.Thermo_SII_LC:
                    if (datasetInfo.DatasetType != InstrumentFileLayout.File)
                    {
                        if (datasetInfo.DatasetType == InstrumentFileLayout.DirectoryNoExt)
                        {
                            // ReSharper disable once CommentTypo
                            // Datasets from LAESI-HMS datasets will have a directory named after the dataset, and inside that directory will be a single .raw file
                            // Confirm that this is the case

                            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
                            var foundFiles = sourceDirectory.GetFiles("*.raw").ToList();

                            if (foundFiles.Count == 1)
                            {
                                break;
                            }

                            if (foundFiles.Count > 1)
                            {
                                // Dataset name matched a directory with multiple .raw files; there must be only one .raw file
                                returnData.CloseoutMsg = "Dataset name matched " + entityDescription +
                                                         " with multiple .raw files; there must be only one .raw file";

                                var fileNames = foundFiles.ConvertAll(file => file.Name);
                                LogWarning("Multiple .raw files found in directory " + sourceDirectory.FullName + ": " + string.Join(", ", fileNames.Take(5)));
                            }
                            else
                            {
                                // Dataset name matched a directory, but it does not have a .raw file
                                returnData.CloseoutMsg = "Dataset name matched " + entityDescription + " but it does not have a .raw file";
                            }

                            break;
                        }

                        if (datasetInfo.DatasetType == InstrumentFileLayout.MultiFile)
                        {
                            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath));
                            var foundFiles = sourceDirectory.GetFiles(datasetInfo.FileOrDirectoryName + ".*").ToList();

                            if (foundFiles.Count == 2)
                            {
                                // On the 21T each .raw file can have a corresponding .tsv file
                                // Allow for this during capture

                                // Also, on Thermo instruments, there might be a sequence file (extension .sld) with the same name as the .raw file; ignore it

                                var rawFound = false;
                                var tsvFound = false;
                                var sldFound = false;

                                foreach (var file in foundFiles)
                                {
                                    if (string.Equals(Path.GetExtension(file.Name), ".raw", StringComparison.OrdinalIgnoreCase))
                                    {
                                        rawFound = true;
                                    }

                                    if (string.Equals(Path.GetExtension(file.Name), ".tsv", StringComparison.OrdinalIgnoreCase))
                                    {
                                        tsvFound = true;
                                    }

                                    if (string.Equals(Path.GetExtension(file.Name), ".sld", StringComparison.OrdinalIgnoreCase))
                                    {
                                        sldFound = true;
                                    }
                                }

                                if (rawFound && tsvFound)
                                {
                                    LogMessage("Capturing a .raw file with a corresponding .tsv file");
                                    break;
                                }

                                if (rawFound && sldFound)
                                {
                                    LogMessage("Ignoring sequence file " + datasetInfo.FileOrDirectoryName + ".sld");

                                    datasetInfo.DatasetType = InstrumentFileLayout.File;
                                    datasetInfo.FileOrDirectoryName = datasetInfo.DatasetName + ".raw";

                                    datasetInfo.FileList.Clear();
                                    datasetInfo.FileList.Add(new FileInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName)));
                                    break;
                                }
                            }

                            var fileNames = foundFiles.ConvertAll(file => file.Name);
                            LogWarning(
                                "Dataset name matched multiple files in directory " + sourceDirectory.FullName + ": " +
                                string.Join(", ", fileNames.Take(5)));
                        }

                        // Dataset name matched multiple files; must be a .raw file
                        returnData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .raw file";
                    }
                    break;

                case InstrumentClass.BrukerMALDI_Imaging_V2:
                    if (datasetInfo.DatasetType != InstrumentFileLayout.DirectoryNoExt)
                    {
                        // Dataset name matched a file; must be a directory with the dataset name, and inside the directory is a .D directory (and typically some jpg files)
                        returnData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a directory with the dataset name, and inside the directory is a .D directory (and typically some jpg files)";
                    }
                    break;

                case InstrumentClass.Bruker_Amazon_Ion_Trap:
                case InstrumentClass.BrukerFT_BAF:
                case InstrumentClass.BrukerTOF_BAF:
                case InstrumentClass.BrukerTOF_TDF:
                case InstrumentClass.Agilent_Ion_Trap:
                case InstrumentClass.Agilent_TOF_V2:
                case InstrumentClass.PrepHPLC:

                    if (datasetInfo.DatasetType != InstrumentFileLayout.DirectoryExt)
                    {
                        // Dataset name matched a file; must be a .d directory
                        returnData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .d directory";
                    }
                    break;

                case InstrumentClass.BrukerMALDI_Imaging:
                case InstrumentClass.BrukerMALDI_Spot:
                case InstrumentClass.FT_Booster_Data:

                    if (datasetInfo.DatasetType != InstrumentFileLayout.DirectoryNoExt)
                    {
                        // Dataset name matched a file; must be a directory with the dataset name
                        returnData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a directory with the dataset name";
                    }
                    break;

                case InstrumentClass.Sciex_TripleTOF:
                    if (datasetInfo.DatasetType != InstrumentFileLayout.File)
                    {
                        // Dataset name matched a directory; must be a file
                        // Dataset name matched multiple files; must be a file
                        returnData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a file";
                    }
                    break;

                case InstrumentClass.IMS_Agilent_TOF_UIMF:
                case InstrumentClass.IMS_Agilent_TOF_DotD:
                    if (datasetInfo.DatasetType != InstrumentFileLayout.File)
                    {
                        if (datasetInfo.DatasetType == InstrumentFileLayout.DirectoryExt)
                        {
                            // IMS08_AgQTOF05 collects data as .D directories, which the capture pipeline will then convert to a .uimf file
                            // Make sure the matched directory is a .d file
                            if (datasetInfo.FileOrDirectoryName.EndsWith(".d", StringComparison.OrdinalIgnoreCase))
                            {
                                break;
                            }
                        }

                        if (datasetInfo.DatasetType == InstrumentFileLayout.DirectoryNoExt)
                        {
                            // IMS04_AgTOF05 and similar instruments collect data into a directory named after the dataset
                            // The directory contains a .UIMF file plus several related files
                            // Make sure the directory contains just one .UIMF file

                            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
                            var foundFiles = sourceDirectory.GetFiles("*.uimf").ToList();

                            if (foundFiles.Count == 1)
                            {
                                break;
                            }

                            if (foundFiles.Count > 1)
                            {
                                // Dataset name matched a directory with multiple .uimf files; there must be only one .uimf file
                                returnData.CloseoutMsg = "Dataset name matched " + entityDescription +
                                                      " with multiple .uimf files; there must be only one .uimf file";

                                var fileNames = foundFiles.ConvertAll(file => file.Name);
                                LogWarning("Multiple .uimf files found in directory " + sourceDirectory.FullName + ": " + string.Join(", ", fileNames).Take(5));
                            }
                            else
                            {
                                // Dataset name matched a directory, but it does not have a .uimf file
                                returnData.CloseoutMsg = "Dataset name matched " + entityDescription + " but it does not have a .uimf file";
                                LogWarning("Directory  " + sourceDirectory.FullName + " does not have any .uimf files");
                            }

                            break;
                        }

                        if (datasetInfo.DatasetType != InstrumentFileLayout.DirectoryExt &&
                            datasetInfo.DatasetType != InstrumentFileLayout.DirectoryNoExt &&
                            datasetInfo.DatasetType != InstrumentFileLayout.MultiFile)
                        {
                            LogWarning("datasetInfo.DatasetType was not DirectoryExt, DirectoryNoExt, or MultiFile; this is unexpected: " + datasetInfo.DatasetType);
                        }

                        // Dataset name matched multiple files; must be a .uimf file, .d directory, or directory with a single .uimf file
                        returnData.CloseoutMsg = "Dataset name matched " + entityDescription + "; must be a .uimf file, .d directory, or directory with a single .uimf file";
                    }
                    break;
            }

            if (string.IsNullOrEmpty(returnData.CloseoutMsg))
            {
                // We are capturing the right item for the instrument class of this dataset
                return true;
            }

            LogError(returnData.CloseoutMsg + ": " + dataset, true);

            return false;
        }

        private void OnCopyingFile(string filename)
        {
            LogDebug("Copying file " + filename);
        }

        private void OnResumingFileCopy(string filename)
        {
            LogMessage("Resuming copy of file " + filename);
        }

        private void OnFileCopyProgress(string filename, float percentComplete)
        {
            var showProgress = DateTime.Now.Subtract(mLastProgressUpdate).TotalSeconds >= 20 ||
                               percentComplete >= 100 && filename == mLastProgressFileName;

            if (!showProgress)
                return;

            if (mLastProgressFileName == filename && Math.Abs(mLastProgressPercent - percentComplete) < float.Epsilon)
            {
                // Don't re-display this progress
                return;
            }

            mLastProgressUpdate = DateTime.Now;
            mLastProgressFileName = filename;
            mLastProgressPercent = percentComplete;

            LogMessage("  copying {0}: {1:0.0}% complete", Path.GetFileName(filename), percentComplete);
        }
    }
}
