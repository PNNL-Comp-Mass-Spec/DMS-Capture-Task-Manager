using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Capture implementation for "directory with no extension" dataset files
    /// </summary>
    internal class DirectoryNoExtCapture : CaptureDirectoryBase
    {
        // Ignore Spelling: Bruker, ser

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public DirectoryNoExtCapture(CaptureInitData data) : base(data)
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
            CaptureDirectoryNoExt(out msg, returnData, datasetInfo, sourceDirectoryPath, datasetDirectoryPath, copyWithResume, instrumentClass);
        }

        /// <summary>
        /// Capture a directory with no extension on the name (the directory name is nearly always the dataset name)
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument); datasetInfo.FileOrDirectoryName will be appended to this</param>
        /// <param name="datasetDirectoryPath">Destination directory; datasetInfo.FileOrDirectoryName will not be appended to this (contrast with CaptureDirectoryExt)</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        /// <param name="instrumentClass">Instrument class</param>
        private void CaptureDirectoryNoExt(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClass instrumentClass)
        {
            // List of file names to skip (not full paths)
            var filesToSkip = new SortedSet<string>();

            bool success;

            var sourceDirectory = new DirectoryInfo(Path.Combine(sourceDirectoryPath, datasetInfo.FileOrDirectoryName));
            var targetDirectory = new DirectoryInfo(datasetDirectoryPath);

            // Look for a zero-byte .UIMF file or a .UIMF journal file
            // Abort the capture if either is present
            if (IsIncompleteUimfFound(sourceDirectory.FullName, out msg, returnData))
            {
                return;
            }

            // Verify the directory doesn't contain a group of ".d" directories
            var dotDDirectories = sourceDirectory.GetDirectories("*.d", SearchOption.TopDirectoryOnly);

            if (dotDDirectories.Length > 1)
            {
                var allowMultipleDirectories = false;

                if (dotDDirectories.Length == 2)
                {
                    // If one directory contains a ser file and the other directory contains an analysis.baf, we'll allow it
                    // This is sometimes the case for the 15T_FTICR_Imaging
                    var serCount = 0;
                    var bafCount = 0;

                    foreach (var directory in dotDDirectories)
                    {
                        if (directory.GetFiles("ser", SearchOption.TopDirectoryOnly).Length == 1)
                        {
                            serCount++;
                        }

                        if (directory.GetFiles("analysis.baf", SearchOption.TopDirectoryOnly).Length == 1)
                        {
                            bafCount++;
                        }
                    }

                    if (bafCount == 1 && serCount == 1)
                    {
                        allowMultipleDirectories = true;
                    }
                }

                if (!allowMultipleDirectories && instrumentClass is InstrumentClass.BrukerMALDI_Imaging_V2 or InstrumentClass.TimsTOF_MALDI_Imaging)
                {
                    // Bruker Imaging datasets can have multiple .d subdirectories; they typically each have their own ser file
                    // timsTOF Imaging datasets can have multiple .d subdirectories (though this is not always the case)
                    allowMultipleDirectories = true;
                }

                if (!allowMultipleDirectories)
                {
                    returnData.CloseoutMsg = "Multiple .d subdirectories found in dataset directory";
                    msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                    LogError(msg);
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }

            // Verify the directory doesn't contain ".IMF" files
            if (sourceDirectory.GetFiles("*.imf", SearchOption.TopDirectoryOnly).Length > 0)
            {
                returnData.CloseoutMsg = "Dataset directory contains a series of .IMF files -- upload a .UIMF file instead";
                msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                LogError(msg);
                returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                return;
            }

            if (instrumentClass == InstrumentClass.IMS_Agilent_TOF_UIMF)
            {
                // Possibly skip the Fragmentation_Profile.txt file
                var fragProfileFile = new FileInfo(Path.Combine(sourceDirectory.FullName, "Fragmentation_Profile.txt"));

                if (fragProfileFile.Exists && FragmentationProfileFileIsDefault(fragProfileFile))
                {
                    filesToSkip.Add(fragProfileFile.Name);
                }
            }

            if (instrumentClass == InstrumentClass.FT_Booster_Data)
            {
                // Skip Thermo .Raw files
                foreach (var thermoRawFile in sourceDirectory.GetFiles("*.raw", SearchOption.AllDirectories))
                {
                    filesToSkip.Add(thermoRawFile.Name);
                }

                // Skip chunk .bin files
                foreach (var thermoRawFile in sourceDirectory.GetFiles("chunk*.bin", SearchOption.AllDirectories))
                {
                    filesToSkip.Add(thermoRawFile.Name);
                }
            }

            if (instrumentClass == InstrumentClass.Sciex_QTrap)
            {
                // Make sure that it doesn't have more than 2 subdirectories (it typically won't have any, but we'll allow 2)
                if (sourceDirectory.GetDirectories("*", SearchOption.TopDirectoryOnly).Length > 2)
                {
                    returnData.CloseoutMsg = "Dataset directory has more than 2 subdirectories";
                    msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                    LogError(msg);
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }

                // Verify that the directory has a .wiff or a .wiff.scan file
                if (sourceDirectory.GetFiles("*.wiff*", SearchOption.TopDirectoryOnly).Length == 0)
                {
                    returnData.CloseoutMsg = "Dataset directory does not contain any .wiff files";
                    msg = returnData.CloseoutMsg + " " + sourceDirectory.FullName;
                    LogError(msg);
                    returnData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                    return;
                }
            }

            // Verify the directory size is constant (indicates acquisition is actually finished)
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

            // Copy the dataset directory to the storage server
            try
            {
                if (copyWithResume)
                {
                    const bool recurse = true;
                    success = CopyDirectoryWithResume(sourceDirectory.FullName, targetDirectory.FullName, recurse, returnData, filesToSkip);
                }
                else
                {
                    mFileTools.CopyDirectory(sourceDirectory.FullName, targetDirectory.FullName, filesToSkip.ToList());
                    success = true;
                }

                if (success)
                {
                    msg = "Copied directory " + sourceDirectory.FullName + " to " + targetDirectory.FullName + mShareConnection.GetConnectionDescription();
                    LogMessage(msg);

                    AutoFixFilesWithInvalidChars(datasetInfo.DatasetName, targetDirectory);
                }
                else
                {
                    msg = "Unknown error copying the dataset directory";
                }
            }
            catch (Exception ex)
            {
                msg = "Exception copying dataset directory " + sourceDirectory.FullName + mShareConnection.GetConnectionDescription();
                LogError(msg, true);
                LogError("Stack trace", ex);

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

        private bool FragmentationProfileFileIsDefault(FileSystemInfo fragProfileFile)
        {
            try
            {
                // RegEx to match lines of the form:
                // 0, 0, 0, 0, 0
                var zeroLineMatcher = new Regex("^[0, ]+$", RegexOptions.Compiled);

                using var reader = new StreamReader(new FileStream(fragProfileFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var dataLineCount = 0;
                var lineAllZeroes = false;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    dataLineCount++;

                    lineAllZeroes = zeroLineMatcher.IsMatch(dataLine);
                }

                if (dataLineCount == 1 && lineAllZeroes)
                {
                    LogMessage("Skipping capture of default fragmentation profile file, " + fragProfileFile.FullName);
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception examining the Fragmentation_Profile.txt file", ex);
            }
            return false;
        }
    }
}
