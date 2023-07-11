using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Abstract class for capturing non-directory-based dataset files
    /// </summary>
    internal abstract class CaptureFileBase : CaptureBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        protected CaptureFileBase(CaptureInitData data) : base(data)
        { }

        /// <summary>
        /// Dataset found, and it's either a single file or multiple files with the same name but different extensions
        /// Capture the file (or files) specified by fileNames
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="fileNames">List of file names</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument)</param>
        /// <param name="datasetDirectoryPath">Destination directory</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        protected void CaptureOneOrMoreFiles(
            out string msg,
            ToolReturnData returnData,
            string datasetName,
            ICollection<string> fileNames,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume)
        {
            msg = string.Empty;
            var validFiles = new List<string>();
            var errorMessages = new List<string>();

            Parallel.ForEach(fileNames, fileName =>
            {
                // First, verify constant file size (indicates acquisition is actually finished)
                var sourceFilePath = Path.Combine(sourceDirectoryPath, fileName);

                var retDataValidateConstant = new ToolReturnData();

                var sleepIntervalSeconds = GetSleepIntervalForFile(sourceFilePath);

                if (VerifyConstantFileSize(sourceFilePath, sleepIntervalSeconds, retDataValidateConstant))
                {
                    validFiles.Add(fileName);
                }
                else
                {
                    errorMessages.Add(retDataValidateConstant.CloseoutMsg);
                }
            });

            if (validFiles.Count < fileNames.Count)
            {
                LogWarning("Dataset '" + datasetName + "' not ready; source file's size changed (or authentication error)");
                mShareConnection.DisconnectShareIfRequired();
                if (errorMessages.Count > 0)
                {
                    returnData.CloseoutMsg = errorMessages[0];
                    LogMessage(returnData.CloseoutMsg);
                }
                else
                {
                    returnData.CloseoutMsg = "File size changed";
                }

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Make a dataset directory (it's OK if it already exists)
            try
            {
                MakeDirectoryIfMissing(datasetDirectoryPath);
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception creating dataset directory";
                msg = returnData.CloseoutMsg + " at " + datasetDirectoryPath;

                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return;
            }

            var success = false;

            // Copy the data file (or files) to the dataset directory
            // If any of the source files have an invalid character (space, % or period),
            // replace with the default replacement string if doing so will match the dataset name
            try
            {
                foreach (var fileName in fileNames)
                {
                    var sourceFilePath = Path.Combine(sourceDirectoryPath, fileName);
                    var sourceFileName = Path.GetFileName(sourceFilePath);

                    var targetFileName = mDatasetFileSearchTool.AutoFixFilename(datasetName, fileName);
                    var targetFilePath = Path.Combine(datasetDirectoryPath, targetFileName);

                    if (!string.Equals(sourceFileName, targetFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        LogMessage("Renaming '" + sourceFileName + "' to '" + targetFileName + "' to remove spaces");
                    }

                    var sourceFile = new FileInfo(sourceFilePath);
                    if (!File.Exists(sourceFilePath))
                    {
                        msg = "source file not found at " + sourceFilePath;
                        LogError("  " + msg + mShareConnection.GetConnectionDescription());
                        break;
                    }

                    if (copyWithResume || sourceFile.Length > COPY_WITH_RESUME_THRESHOLD_BYTES)
                    {
                        // Copy the file using 1 MB chunks, thus allowing for resuming
                        // In addition, allow the file to be copied even if another program has it open for writing
                        // (as is often the case with instrument acquisition software, even after data acquisition is complete)
                        success = mFileTools.CopyFileWithResume(sourceFile, targetFilePath, out _, true);
                    }
                    else
                    {
                        File.Copy(sourceFilePath, targetFilePath);
                        success = true;
                    }

                    if (success)
                    {
                        LogMessage("  copied file " + sourceFilePath + " to " + targetFilePath + mShareConnection.GetConnectionDescription());
                    }
                    else
                    {
                        msg = "file copy failed for " + sourceFilePath + " to " + targetFilePath;
                        LogError("  " + msg + mShareConnection.GetConnectionDescription());
                    }
                }
            }
            catch (Exception ex)
            {
                msg = "Copy exception for dataset " + datasetName;
                LogError(msg + mShareConnection.GetConnectionDescription(), ex);

                mToolState.HandleCopyException(returnData, ex);
                return;
            }
            finally
            {
                mShareConnection.DisconnectShareIfRequired();
            }

            if (success)
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
