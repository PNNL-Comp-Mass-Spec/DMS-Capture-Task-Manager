using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;

// ReSharper disable UnusedMember.Global

namespace CaptureTaskManager
{
    public class DatasetFileSearchTool : EventNotifier
    {

        private readonly bool mTraceMode;

        /// <summary>
        /// List of characters that should be automatically replaced if doing so makes the filename match the dataset name
        /// </summary>
        public IReadOnlyDictionary<char, string> FilenameAutoFixes { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DatasetFileSearchTool(bool traceMode)
        {
            FilenameAutoFixes = new Dictionary<char, string> {
                { ' ', "_"},
                { '%', "pct"},
                { '.', "pt"}};

            mTraceMode = traceMode;
        }

        /// <summary>
        /// If the filename contains any of the characters in mFilenameAutoFixes, replace the character with the given replacement string
        /// Next compare to datasetName.  If a match, return the updated filename, otherwise return the original filename
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="fileName">File name</param>
        /// <returns>Optimal filename to use</returns>
        /// <remarks>When searching for a period, only the base filename is examined</remarks>
        public string AutoFixFilename(string datasetName, string fileName)
        {
            return AutoFixFilename(datasetName, fileName, FilenameAutoFixes);
        }

        /// <summary>
        /// If the filename contains any of the characters in charsToFind, replace the character with the given replacement string
        /// Next compare to datasetName.  If a match, return the updated filename, otherwise return the original filename
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="fileName">File name</param>
        /// <param name="charsToFind">Keys are characters to find; values are the replacement text</param>
        /// <returns>Optimal filename to use</returns>
        /// <remarks>When searching for a period, only the base filename is examined</remarks>
        public string AutoFixFilename(string datasetName, string fileName, IReadOnlyDictionary<char, string> charsToFind)
        {
            var matchFound = charsToFind.Keys.Any(item => fileName.IndexOf(item) >= 0);
            if (!matchFound)
                return fileName;

            var fileExtension = Path.GetExtension(fileName);
            var updatedFileName = string.Copy(fileName);

            foreach (var item in charsToFind)
            {
                var baseName = Path.GetFileNameWithoutExtension(updatedFileName);

                if (baseName.IndexOf(item.Key) < 0)
                    continue;

                updatedFileName = baseName.Replace(item.Key.ToString(), item.Value) + fileExtension;
            }

            if (string.Equals(Path.GetFileNameWithoutExtension(updatedFileName), datasetName, StringComparison.OrdinalIgnoreCase))
            {
                return updatedFileName;
            }

            return fileName;
        }

        /// <summary>
        /// Looks for the dataset file in the source directory
        /// </summary>
        /// <param name="sourceDirectoryPath">Full path to the directory to search</param>
        /// <param name="datasetName">Dataset name</param>
        /// <returns>DatasetInfo object containing info on found dataset file</returns>
        /// <remarks>
        /// First looks for an exact match to a file or directory
        /// If no match, replaces spaces with underscores before comparing file and directory names to the dataset name
        /// </remarks>
        public DatasetInfo FindDatasetFile(
            string sourceDirectoryPath,
            string datasetName)
        {
            const bool checkForFilesFirst = true;
            var datasetInfo = FindDatasetFileOrDirectory(sourceDirectoryPath, datasetName, checkForFilesFirst, out var matchedDirectory);

            if (!matchedDirectory)
                return datasetInfo;

            // Matched a directory, but this method is used to match files
            datasetInfo.DatasetType = DatasetInfo.RawDSTypes.None;
            datasetInfo.FileOrDirectoryName = string.Empty;

            if (datasetInfo.FileList == null)
            {
                OnDebugEvent("datasetInfo.FileList is null in FindDatasetFile; this should not be possible");
            }
            else
            {
                datasetInfo.FileList.Clear();
            }

            return datasetInfo;
        }

        /// <summary>
        /// Determines if the dataset exists as a single file, a directory with same name as dataset,
        /// or a directory with dataset name and an extension
        /// </summary>
        /// <param name="sourceDirectoryPath">Full path to source directory on the instrument</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="instrumentClass">Instrument class for dataset to be located</param>
        /// <returns>DatasetInfo object containing info on found dataset file or directory</returns>
        /// <remarks>
        /// First looks for an exact match to a file or directory
        /// If no match, replaces spaces with underscores before comparing file and directory names to the dataset name
        /// </remarks>
        public DatasetInfo FindDatasetFileOrDirectory(
            string sourceDirectoryPath,
            string datasetName,
            clsInstrumentClassInfo.eInstrumentClass instrumentClass)
        {
            bool checkForFilesFirst;

            switch (instrumentClass)
            {
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging_V2:
                case clsInstrumentClassInfo.eInstrumentClass.IMS_Agilent_TOF:
                case clsInstrumentClassInfo.eInstrumentClass.Micromass_QTOF:
                case clsInstrumentClassInfo.eInstrumentClass.Waters_IMS:
                    // Preferentially capture dataset directories
                    // If a directory is not found, will instead look for a dataset file
                    checkForFilesFirst = false;
                    break;
                default:
                    // First look for a file with name datasetName, if not found, look for a directory
                    checkForFilesFirst = true;
                    break;
            }

            var datasetInfo = FindDatasetFileOrDirectory(sourceDirectoryPath, datasetName, checkForFilesFirst, out var matchedDirectory);

            if (!matchedDirectory)
            {
                return datasetInfo;
            }

            // Possibly update datasetInfo.DatasetType, based on instrument class

            // ReSharper disable once SwitchStatementMissingSomeCases
            switch (instrumentClass)
            {
                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Imaging:
                    datasetInfo.DatasetType = DatasetInfo.RawDSTypes.BrukerImaging;
                    break;

                case clsInstrumentClassInfo.eInstrumentClass.BrukerMALDI_Spot:
                    datasetInfo.DatasetType = DatasetInfo.RawDSTypes.BrukerSpot;
                    break;
            }

            return datasetInfo;
        }

        /// <summary>
        /// Determines if the dataset exists as a single file, a directory with same name as dataset,
        /// or a directory with dataset name and an extension
        /// </summary>
        /// <param name="sourceDirectoryPath">Full path to the directory to search</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="checkForFilesFirst">
        /// When true, first look for a file, then look for a directory.  When false, first look for a directory.
        /// </param>
        /// <param name="matchedDirectory">Output: true if we matched a directory</param>
        /// <returns>DatasetInfo object containing info on found dataset file or directory</returns>
        /// <remarks>
        /// First looks for an exact match to a file or directory
        /// If no match, replaces spaces with underscores before comparing file and directory names to the dataset name
        /// </remarks>
        public DatasetInfo FindDatasetFileOrDirectory(
            string sourceDirectoryPath,
            string datasetName,
            bool checkForFilesFirst,
            out bool matchedDirectory)
        {

            var datasetInfo = new DatasetInfo(datasetName);

            var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);

            // Verify that the source directory exists
            if (!sourceDirectory.Exists)
            {
                OnErrorEvent("Source directory not found: [" + sourceDirectory.FullName + "]");

                datasetInfo.DatasetType = DatasetInfo.RawDSTypes.None;
                matchedDirectory = false;
                return datasetInfo;
            }

            var lookForDatasetFile = checkForFilesFirst;

            // If lookForDatasetFile is true, the following logic is followed:
            // When i is 1, search for remote files
            // When i is 2, search for remote files, but replace spaces with underscores
            // When i is 3, search for remote directories
            // When i is 4, search for remote directories, but replace spaces with underscores

            // If lookForDatasetFile is false, we first look for directories

            for (var i = 1; i <= 4; i++)
            {
                if (i == 3)
                {
                    // Switch from files to directories (or vice versa)
                    lookForDatasetFile = !lookForDatasetFile;
                }

                var replaceInvalidCharacters = i % 2 == 0;

                if (lookForDatasetFile)
                {
                    // Get all files that match the dataset name
                    var foundFiles = new List<FileInfo>();

                    if (mTraceMode)
                    {
                        clsToolRunnerBase.ShowTraceMessage(
                            string.Format("Looking for a dataset file, replaceInvalidCharacters is {0}", replaceInvalidCharacters));
                    }

                    if (replaceInvalidCharacters)
                    {
                        foreach (var candidateItem in sourceDirectory.GetFiles("*"))
                        {
                            var updatedName = ReplaceInvalidChars(Path.GetFileNameWithoutExtension(candidateItem.Name));

                            if (updatedName.Equals(datasetName, StringComparison.OrdinalIgnoreCase))
                            {
                                foundFiles.Add(candidateItem);
                            }
                        }
                    }
                    else
                    {
                        foreach (var remoteFile in sourceDirectory.GetFiles(datasetName + ".*"))
                        {
                            foundFiles.Add(remoteFile);
                        }
                    }

                    if (foundFiles.Count <= 0)
                        continue;

                    datasetInfo.FileList.AddRange(foundFiles);

                    if (datasetInfo.FileCount == 1)
                    {
                        datasetInfo.FileOrDirectoryName = datasetInfo.FileList[0].Name;
                        datasetInfo.DatasetType = DatasetInfo.RawDSTypes.File;
                    }
                    else
                    {
                        datasetInfo.FileOrDirectoryName = datasetName;
                        datasetInfo.DatasetType = DatasetInfo.RawDSTypes.MultiFile;
                        var fileNames = foundFiles.Select(file => file.Name).ToList();
                        OnWarningEvent(string.Format(
                                           "Dataset name matched multiple files for iteration {0} in directory {1}: {2}",
                                           i,
                                           sourceDirectory.FullName,
                                           string.Join(", ", fileNames.Take(5))));
                    }

                    if (mTraceMode)
                    {
                        clsToolRunnerBase.ShowTraceMessage(
                            string.Format("Matched file {0}; DatasetType = {1}",
                                          datasetInfo.FileOrDirectoryName, datasetInfo.DatasetType.ToString()));
                    }

                    matchedDirectory = false;
                    return datasetInfo;
                }

                if (mTraceMode)
                {

                    clsToolRunnerBase.ShowTraceMessage(
                        string.Format("Looking for a dataset directory, replaceInvalidCharacters is {0}", replaceInvalidCharacters));
                }

                // Get all directories that match the dataset name
                var subdirectories = sourceDirectory.GetDirectories();
                foreach (var subdirectory in subdirectories)
                {
                    var directoryNameWithoutExtension = Path.GetFileNameWithoutExtension(subdirectory.Name);
                    string directoryNameToCheck;

                    if (replaceInvalidCharacters)
                    {
                        directoryNameToCheck = ReplaceInvalidChars(directoryNameWithoutExtension);
                    }
                    else
                    {
                        directoryNameToCheck = directoryNameWithoutExtension;
                    }

                    if (!string.Equals(directoryNameToCheck, datasetName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (string.IsNullOrEmpty(Path.GetExtension(subdirectory.Name)))
                    {
                        // Found a directory that has no extension
                        datasetInfo.FileOrDirectoryName = subdirectory.Name;
                        datasetInfo.DatasetType = DatasetInfo.RawDSTypes.DirectoryNoExt;
                    }
                    else
                    {
                        // Directory name has an extension
                        datasetInfo.FileOrDirectoryName = subdirectory.Name;
                        datasetInfo.DatasetType = DatasetInfo.RawDSTypes.DirectoryExt;
                    }

                    if (mTraceMode)
                    {
                        clsToolRunnerBase.ShowTraceMessage(
                            string.Format("Matched directory {0}; DatasetType = {1}",
                                          datasetInfo.FileOrDirectoryName, datasetInfo.DatasetType.ToString()));
                    }

                    if (checkForFilesFirst)
                    {
                        OnStatusEvent(string.Format(
                                          "Dataset name did not match a file, but it did match directory {0}, dataset type is {1}",
                                          datasetInfo.FileOrDirectoryName,
                                          datasetInfo.DatasetType));
                    }

                    matchedDirectory = true;
                    return datasetInfo;
                }

            }

            // If we got to here, the raw dataset wasn't found (either as a file or a directory), so there was a problem
            datasetInfo.DatasetType = DatasetInfo.RawDSTypes.None;
            matchedDirectory = true;
            return datasetInfo;
        }

        /// <summary>
        /// Replace invalid characters with substitutes
        /// </summary>
        /// <param name="searchText"></param>
        /// <returns></returns>
        private string ReplaceInvalidChars(string searchText)
        {
            var updatedText = string.Copy(searchText);

            foreach (var item in FilenameAutoFixes)
            {
                updatedText = updatedText.Replace(item.Key.ToString(), item.Value);
            }

            return updatedText;
        }

    }
}
