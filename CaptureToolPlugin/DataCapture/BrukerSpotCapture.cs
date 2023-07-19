using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Capture implementation for Bruker MALDI Spot files
    /// </summary>
    internal class BrukerSpotCapture : CaptureDirectoryBase
    {
        // Ignore Spelling: Bruker

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public BrukerSpotCapture(CaptureInitData data) : base(data)
        { }

        /// <inheritdoc />
        public override void Capture(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClass instrumentClass,
            string instrumentName,
            ITaskParams taskParams
        )
        {
            CaptureBrukerSpot(out msg, returnData, datasetInfo, sourceDirectoryPath, datasetDirectoryPath);
        }

        /// <summary>
        /// Capture a directory from a Bruker_Spot instrument
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="datasetDirectoryPath">Destination directory; datasetInfo.FileOrDirectoryName will not be appended to this (contrast with CaptureDirectoryExt)</param>
        private void CaptureBrukerSpot(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath)
        {
            // Verify that the directory size is constant (indicates acquisition is actually finished)
            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
            var targetDirectory = new DirectoryInfo(datasetDirectoryPath);

            // Verify the dataset directory doesn't contain any .zip files
            var zipFiles = sourceDirectory.GetFiles("*.zip");

            if (zipFiles.Length > 0)
            {
                returnData.CloseoutMsg = "Zip files found in dataset directory";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                LogError(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            // Check whether the dataset directory contains just one data directory or multiple data directories
            var dataDirectories = sourceDirectory.GetDirectories().ToList();

            if (dataDirectories.Count < 1)
            {
                returnData.CloseoutMsg = "No subdirectories were found in the dataset directory ";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                LogError(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (dataDirectories.Count > 1)
            {
                // Make sure the subdirectories match the naming convention for MALDI spot directories
                // Example directory names:
                //  0_D4
                //  0_E10
                //  0_N4

                const string MALDI_SPOT_DIRECTORY_REGEX = @"^\d_[A-Z]\d+$";
                var maldiSpotDirectoryMatcher = new Regex(MALDI_SPOT_DIRECTORY_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                foreach (var directory in dataDirectories)
                {
                    LogDebug("Test directory " + directory + " against RegEx " + maldiSpotDirectoryMatcher);

                    if (!maldiSpotDirectoryMatcher.IsMatch(directory.Name, 0))
                    {
                        returnData.CloseoutMsg = "Dataset directory contains multiple subdirectories, but directory " + directory.Name + " does not match the expected pattern";
                        msg = returnData.CloseoutMsg + " (" + maldiSpotDirectoryMatcher + "); see " + sourceDirectory.FullName;
                        LogError(msg);
                        returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                        return;
                    }
                }
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

            // Copy the dataset directory (and all subdirectories) to the storage server
            try
            {
                mFileTools.CopyDirectory(sourceDirectory.FullName, targetDirectory.FullName);
                msg = "Copied directory " + sourceDirectory.FullName + " to " + targetDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogMessage(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                msg = "Exception copying dataset directory " + sourceDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

                mToolState.HandleCopyException(returnData, ex);
            }
            finally
            {
                mShareConnection.DisconnectShareIfRequired();
            }
        }
    }
}
