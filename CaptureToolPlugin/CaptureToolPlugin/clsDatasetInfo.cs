//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 12/03/2010
//*********************************************************************************************************

using System.IO;

namespace CaptureToolPlugin
{
    class clsDatasetInfo
    {
        //*********************************************************************************************************
        // Class to hold info for a dataset to be captured
        //**********************************************************************************************************

        #region "Properties"

        public string DatasetName { get; set; }

        public clsCaptureOps.RawDSTypes DatasetType { get; set; }

        public string FileOrFolderName { get; set; }

        public FileInfo[] FileList { get; set; }

        public int FileCount => FileList?.Length ?? 0;

        #endregion

        #region "Constructor"

        public clsDatasetInfo(string datasetName)
        {
            DatasetName = datasetName;
        }

        public clsDatasetInfo(clsCaptureOps.RawDSTypes dsType)
        {
            DatasetType = dsType;
        }

        #endregion
    }
}
