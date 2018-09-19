//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 12/03/2010
//*********************************************************************************************************

using System.IO;

namespace CaptureToolPlugin
{
    /// <summary>
    /// Holds info for a dataset to be captured
    /// </summary>
    class clsDatasetInfo
    {

        public string DatasetName { get; set; }

        public clsCaptureOps.RawDSTypes DatasetType { get; set; }

        public string FileOrDirectoryName { get; set; }

        public FileInfo[] FileList { get; set; }

        public int FileCount => FileList?.Length ?? 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public clsDatasetInfo(string datasetName)
        {
            DatasetName = datasetName;
        }

    }
}
