//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 12/03/2010
//*********************************************************************************************************

using System.Collections.Generic;
using System.IO;

namespace CaptureTaskManager
{
    /// <summary>
    /// Holds info for a dataset
    /// </summary>
    /// <remarks>
    /// Used by the Capture plugin for finding datasets to capture
    /// Used by the Source File Rename plugin for finding dataset files or directories to rename
    /// </remarks>
    public class DatasetInfo
    {
        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; set; }

        /// <summary>
        /// Dataset type
        /// </summary>
        public InstrumentFileLayout DatasetType { get; set; }

        /// <summary>
        /// File or directory name
        /// </summary>
        public string FileOrDirectoryName { get; set; }

        /// <summary>
        /// Dataset files
        /// </summary>
        public List<FileInfo> FileList { get; }

        /// <summary>
        /// Number of files in FileList
        /// </summary>
        public int FileCount => FileList?.Count ?? 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public DatasetInfo(string datasetName)
        {
            DatasetName = datasetName;
            FileList = new List<FileInfo>();
        }
    }

    public enum InstrumentFileLayout
    {
        // Ignore Spelling: Bruker

        /// <summary>
        /// Unknown type
        /// </summary>
        None,

        /// <summary>
        /// Instrument file
        /// </summary>
        File,

        /// <summary>
        /// Instrument directory without an extension
        /// </summary>
        DirectoryNoExt,

        /// <summary>
        /// Instrument directory with an extension, like .D or .Raw
        /// </summary>
        DirectoryExt,

        /// <summary>
        /// Bruker imaging data
        /// </summary>
        BrukerImaging,

        /// <summary>
        /// Bruker spot data
        /// </summary>
        BrukerSpot,

        /// <summary>
        /// Mix of file types
        /// </summary>
        MultiFile
    }
}
