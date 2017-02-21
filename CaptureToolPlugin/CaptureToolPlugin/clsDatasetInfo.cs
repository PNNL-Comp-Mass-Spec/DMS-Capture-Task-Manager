//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 12/03/2010
//*********************************************************************************************************

namespace CaptureToolPlugin
{
    class clsDatasetInfo
    {
        //*********************************************************************************************************
        // Class to hold info for a dataset to be captured
        //**********************************************************************************************************

        #region "Class variables"

        string[] m_FileList;

        #endregion

        #region "Properties"

        public string DatasetName { get; set; }

        public clsCaptureOps.RawDSTypes DatasetType { get; set; }

        public string FileOrFolderName { get; set; }

        public string[] FileList
        {
            get { return m_FileList; }
            set { m_FileList = value; }
        }

        public int FileCount
        {
            get
            {
                if (m_FileList == null)
                {
                    return 0;
                }
                return m_FileList.Length;
            }
        }

        #endregion

        #region "Constructor"

        public clsDatasetInfo(string datasetName)
        {
            DatasetName = datasetName;
            m_FileList = null;
        }

        public clsDatasetInfo(clsCaptureOps.RawDSTypes dsType)
        {
            DatasetType = dsType;
            m_FileList = null;
        }

        #endregion
    }
}
