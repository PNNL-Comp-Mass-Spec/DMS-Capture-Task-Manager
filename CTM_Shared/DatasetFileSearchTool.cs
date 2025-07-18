﻿using System;
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
        /// <remarks>When searching for a period, only the base filename is examined</remarks>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="fileName">File name</param>
        /// <returns>Optimal filename to use</returns>
        public string AutoFixFilename(string datasetName, string fileName)
        {
            return AutoFixFilename(datasetName, fileName, FilenameAutoFixes);
        }

        /// <summary>
        /// If the filename contains any of the characters in charsToFind, replace the character with the given replacement string
        /// Next compare to datasetName.  If a match, return the updated filename, otherwise return the original filename
        /// </summary>
        /// <remarks>When searching for a period, only the base filename is examined</remarks>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="fileName">File name</param>
        /// <param name="charsToFind">Keys are characters to find; values are the replacement text</param>
        /// <returns>Optimal filename to use</returns>
        public string AutoFixFilename(string datasetName, string fileName, IReadOnlyDictionary<char, string> charsToFind)
        {
            var matchFound = charsToFind.Keys.Any(item => fileName.IndexOf(item) >= 0);

            if (!matchFound)
            {
                return fileName;
            }

            var fileExtension = Path.GetExtension(fileName);
            var updatedFileName = fileName;

            foreach (var item in charsToFind)
            {
                var baseName = Path.GetFileNameWithoutExtension(updatedFileName);

                if (baseName.IndexOf(item.Key) < 0)
                {
                    continue;
                }

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
        /// <remarks>
        /// First looks for an exact match to a file or directory
        /// If no match, replaces spaces with underscores before comparing file and directory names to the dataset name
        /// </remarks>
        /// <param name="sourceDirectoryPath">Full path to the directory to search</param>
        /// <param name="datasetName">Dataset name</param>
        /// <returns>DatasetInfo object containing info on found dataset file</returns>
        public DatasetInfo FindDatasetFile(
            string sourceDirectoryPath,
            string datasetName)
        {
            const bool checkForFilesFirst = true;
            var datasetInfo = FindDatasetFileOrDirectory(sourceDirectoryPath, datasetName, checkForFilesFirst, out var matchedDirectory);

            if (!matchedDirectory)
            {
                return datasetInfo;
            }

            // Matched a directory, but this method is used to match files
            datasetInfo.DatasetType = InstrumentFileLayout.None;
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
        /// <remarks>
        /// First looks for an exact match to a file or directory
        /// If no match, replaces spaces with underscores before comparing file and directory names to the dataset name
        /// </remarks>
        /// <param name="sourceDirectoryPath">Full path to source directory on the instrument</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="instrumentClass">Instrument class for dataset to be located</param>
        /// <returns>DatasetInfo object containing info on found dataset file or directory</returns>
        public DatasetInfo FindDatasetFileOrDirectory(
            string sourceDirectoryPath,
            string datasetName,
            InstrumentClass instrumentClass)
        {
            bool checkForFilesFirst;

            // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
            switch (instrumentClass)
            {
                case InstrumentClass.BrukerMALDI_Imaging:
                case InstrumentClass.BrukerMALDI_Imaging_V2:
                case InstrumentClass.IMS_Agilent_TOF_UIMF:
                case InstrumentClass.IMS_Agilent_TOF_DotD:
                case InstrumentClass.Waters_TOF:
                case InstrumentClass.Waters_IMS:
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
                case InstrumentClass.BrukerMALDI_Imaging:
                    datasetInfo.DatasetType = InstrumentFileLayout.BrukerImaging;
                    break;

                case InstrumentClass.BrukerMALDI_Spot:
                    datasetInfo.DatasetType = InstrumentFileLayout.BrukerSpot;
                    break;
            }

            return datasetInfo;
        }

        /// <summary>
        /// Determines if the dataset exists as a single file, a directory with same name as dataset,
        /// or a directory with dataset name and an extension
        /// </summary>
        /// <remarks>
        /// First looks for an exact match to a file or directory
        /// If no match, replaces spaces with underscores before comparing file and directory names to the dataset name
        /// </remarks>
        /// <param name="sourceDirectoryPath">Full path to the directory to search</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="checkForFilesFirst">
        /// When true, first look for a file, then look for a directory.  When false, first look for a directory.
        /// </param>
        /// <param name="matchedDirectory">Output: true if we matched a directory</param>
        /// <returns>DatasetInfo object containing info on found dataset file or directory</returns>
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

                datasetInfo.DatasetType = InstrumentFileLayout.None;
                matchedDirectory = false;
                return datasetInfo;
            }

            var lookForDatasetFile = checkForFilesFirst;

            // If lookForDatasetFile is true, the following logic is followed:
            // When i=1, search for remote files
            // When i=2, search for remote files, but replace spaces with underscores
            // When i=3, search for remote directories
            // When i=4, search for remote directories, but replace spaces with underscores

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
                        ToolRunnerBase.ShowTraceMessage(
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
                        foundFiles.AddRange(sourceDirectory.GetFiles(datasetName + ".*"));
                    }

                    if (foundFiles.Count == 0)
                    {
                        continue;
                    }

                    datasetInfo.FileList.AddRange(foundFiles);

                    if (datasetInfo.FileCount == 1)
                    {
                        datasetInfo.FileOrDirectoryName = datasetInfo.FileList[0].Name;
                        datasetInfo.DatasetType = InstrumentFileLayout.File;

                        // Look for special use-case files related to the dataset file, in particular, realtime search .tsv files

                        var realTimeSearchFiles = sourceDirectory.GetFiles(
                            string.Format("{0}_*_realtimesearch.tsv", Path.GetFileNameWithoutExtension(datasetInfo.FileOrDirectoryName)));

                        var realTimeLibSearchFiles = sourceDirectory.GetFiles(
                            string.Format("{0}_*_realtimelibsearch.tsv", Path.GetFileNameWithoutExtension(datasetInfo.FileOrDirectoryName)));

                        if (realTimeSearchFiles.Length > 0 || realTimeLibSearchFiles.Length > 0)
                        {
                            datasetInfo.RelatedFiles.AddRange(realTimeSearchFiles);
                            datasetInfo.RelatedFiles.AddRange(realTimeLibSearchFiles);

                            var fileNames = datasetInfo.RelatedFiles.ConvertAll(file => file.Name);
                            OnStatusEvent("Dataset has realtime search files in directory {0}: {1}",
                                sourceDirectory.FullName, string.Join(", ", fileNames.Take(5)));
                        }
                    }
                    else
                    {
                        datasetInfo.FileOrDirectoryName = datasetName;
                        datasetInfo.DatasetType = InstrumentFileLayout.MultiFile;
                        var fileNames = foundFiles.ConvertAll(file => file.Name);
                        OnWarningEvent("Dataset name matched multiple files for iteration {0} in directory {1}: {2}",
                            i, sourceDirectory.FullName, string.Join(", ", fileNames.Take(5)));
                    }

                    if (mTraceMode)
                    {
                        ToolRunnerBase.ShowTraceMessage("Matched file {0}; DatasetType = {1}",
                            datasetInfo.FileOrDirectoryName, datasetInfo.DatasetType.ToString());
                    }

                    matchedDirectory = false;
                    return datasetInfo;
                }

                if (mTraceMode)
                {
                    ToolRunnerBase.ShowTraceMessage(
                        string.Format("Looking for a dataset directory, replaceInvalidCharacters is {0}", replaceInvalidCharacters));
                }

                // Get all directories that match the dataset name
                foreach (var subdirectory in sourceDirectory.GetDirectories())
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
                        datasetInfo.DatasetType = InstrumentFileLayout.DirectoryNoExt;
                    }
                    else
                    {
                        // Directory name has an extension
                        datasetInfo.FileOrDirectoryName = subdirectory.Name;
                        datasetInfo.DatasetType = InstrumentFileLayout.DirectoryExt;
                    }

                    if (mTraceMode)
                    {
                        ToolRunnerBase.ShowTraceMessage("Matched directory {0}; DatasetType = {1}",
                            datasetInfo.FileOrDirectoryName, datasetInfo.DatasetType.ToString());
                    }

                    if (checkForFilesFirst)
                    {
                        OnStatusEvent("Dataset name did not match a file, but it did match directory {0}, dataset type is {1}",
                            datasetInfo.FileOrDirectoryName, datasetInfo.DatasetType);
                    }

                    matchedDirectory = true;
                    return datasetInfo;
                }
            }

            // If we got to here, the raw dataset wasn't found (either as a file or a directory), so there was a problem
            datasetInfo.DatasetType = InstrumentFileLayout.None;
            matchedDirectory = true;
            return datasetInfo;
        }

        /// <summary>
        /// Replace invalid characters with substitutes
        /// </summary>
        /// <param name="searchText"></param>
        /// <returns>Updated text</returns>
        private string ReplaceInvalidChars(string searchText)
        {
            var updatedText = searchText;

            foreach (var item in FilenameAutoFixes)
            {
                updatedText = updatedText.Replace(item.Key.ToString(), item.Value);
            }

            return updatedText;
        }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// If captureSubdirectory starts with "..", possibly update sourcePath and captureSubdirectory to account for an alternate share name
        /// </summary>
        /// <remarks>
        /// <para>On Lumos01, C:\ProteomicsData is shared as ProteomicsData2</para>
        /// <para>
        /// The trigger file created by Buzzard will have:
        /// sourceVol = "\\lumos01.bionet\", sourcePath = "ProteomicsData\", and captureSubdirectory = "..\ProteomicsData2"
        /// </para>
        /// <para>
        /// Combining those gives "\\lumos01.bionet\ProteomicsData\..\ProteomicsData2"
        /// </para>
        /// <para>
        /// This method updates the variables to instead have sourcePath = "ProteomicsData2", and captureSubdirectory = ""
        /// </para>
        /// <para>
        /// Combining those gives "\\lumos01.bionet\ProteomicsData2"
        /// </para>
        /// </remarks>
        /// <param name="sourceVol"></param>
        /// <param name="sourcePath"></param>
        /// <param name="captureSubdirectory"></param>
        /// <returns>True if the paths were updated, otherwise false</returns>
        public bool VerifyRelativeSourcePath(string sourceVol, ref string sourcePath, ref string captureSubdirectory)
        {
            if (!captureSubdirectory.TrimStart('\\').StartsWith("..") || !sourceVol.StartsWith(@"\\"))
            {
                // Update not required
                return false;
            }

            OnStatusEvent($"Updating Share Path, Old: '{sourceVol}' '{sourcePath}' '{captureSubdirectory}'");

            var sourcePathParts = sourcePath.Trim('\\', '.').Split('\\');

            if (sourcePathParts.Length == 1)
            {
                var captureSubWork = captureSubdirectory.TrimStart('\\', '.');
                sourcePath = captureSubWork.Split('\\')[0];
                captureSubdirectory = captureSubWork.Substring(sourcePath.Length).TrimStart('\\');

                OnStatusEvent($"Updating Share Path, New: '{sourceVol}' '{sourcePath}' '{captureSubdirectory}'");
                return true;
            }

            var sourceParts = sourcePathParts.ToList();
            var captureSubParts = captureSubdirectory.Trim('\\').Split('\\');
            var firstCaptureSub = string.Empty;

            foreach (var part in captureSubParts)
            {
                if (part == ".." && sourceParts.Count > 0)
                {
                    sourceParts.RemoveAt(sourceParts.Count - 1);
                }
                else
                {
                    firstCaptureSub = part;
                    break;
                }
            }

            if (sourceParts.Count == 0)
            {
                sourcePath = firstCaptureSub;
                captureSubdirectory = captureSubdirectory.TrimStart('\\', '.').Substring(sourcePath.Length).TrimStart('\\');
            }
            else
            {
                sourcePath = Path.Combine(sourceParts.ToArray());
                captureSubdirectory = captureSubdirectory.TrimStart('\\', '.');
            }

            OnStatusEvent($"Updating Share Path, New: '{sourceVol}' '{sourcePath}' '{captureSubdirectory}'");
            return true;
        }
    }
}
