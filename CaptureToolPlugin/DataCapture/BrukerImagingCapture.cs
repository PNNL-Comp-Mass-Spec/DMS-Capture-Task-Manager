using System;
using System.IO;
using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Capture implementation for Bruker MALDI Imaging files
    /// </summary>
    internal class BrukerImagingCapture : CaptureDirectoryBase
    {
        // Ignore Spelling: Bruker

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public BrukerImagingCapture(CaptureInitData data) : base(data)
        { }

        /// <summary>
        /// Capture a Bruker imaging directory
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="datasetDirectoryPath">Destination directory; datasetInfo.FileOrDirectoryName will not be appended to this (contrast with CaptureDirectoryExt)</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        public void CaptureBrukerImaging(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume)
        {
            bool success;

            // First, verify the directory size is constant (indicates acquisition is actually finished)
            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
            var targetDirectory = new DirectoryInfo(datasetDirectoryPath);

            // Check to see if the directories have been zipped
            var zipFileList = Directory.GetFiles(sourceDirectory.FullName, "*.zip");
            if (zipFileList.Length < 1)
            {
                // Data files haven't been zipped, so throw error
                returnData.CloseoutMsg = "No zip files found in dataset directory";
                msg = returnData.CloseoutMsg + " at " + sourceDirectory.FullName;
                LogError(msg);
                mShareConnection.DisconnectShareIfRequired();

                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (!VerifyConstantDirectorySize(sourceDirectory, returnData))
            {
                msg = "Dataset '" + datasetInfo.DatasetName + "' not ready";
                LogWarning(msg);
                mShareConnection.DisconnectShareIfRequired();

                if (string.IsNullOrWhiteSpace(returnData.CloseoutMsg))
                {
                    returnData.CloseoutMsg = "directory size changed";
                }

                if (returnData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
                {
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }

                return;
            }

            // Make a dataset directory
            try
            {
                MakeDirectoryIfMissing(targetDirectory.FullName);
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception creating dataset directory";
                msg = returnData.CloseoutMsg + " at " + targetDirectory.FullName;
                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

                mToolState.HandleCopyException(returnData, ex);
                return;
            }

            // Copy only the files in the dataset directory to the storage server. Do not copy directories
            try
            {
                if (copyWithResume)
                {
                    const bool recurse = false;
                    success = CopyDirectoryWithResume(sourceDirectory.FullName, targetDirectory.FullName, recurse, returnData);
                }
                else
                {
                    foreach (var fileToCopy in Directory.GetFiles(sourceDirectory.FullName))
                    {
                        var fi = new FileInfo(fileToCopy);
                        fi.CopyTo(Path.Combine(targetDirectory.FullName, fi.Name));
                    }
                    success = true;
                }

                if (success)
                {
                    msg = "Copied files in directory " + sourceDirectory.FullName + " to " + targetDirectory.FullName + mShareConnection.GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, targetDirectory);
                }
                else
                {
                    msg = "Unknown error copying the dataset files";
                }
            }
            catch (Exception ex)
            {
                returnData.CloseoutMsg = "Exception copying files from dataset directory";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                mShareConnection.DisconnectShareIfRequired();

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
