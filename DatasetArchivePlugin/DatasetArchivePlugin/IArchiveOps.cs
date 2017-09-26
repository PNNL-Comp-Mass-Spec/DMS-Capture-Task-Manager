//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/19/2009
//*********************************************************************************************************

namespace DatasetArchivePlugin
{
    /// <summary>
    /// Interface for archive or archive update classes
    /// </summary>
    public interface IArchiveOps
    {

        #region "Properties"
        /// <summary>
        /// Error message from archive ops result
        /// </summary>
        string ErrMsg { get; }
        string WarningMsg { get; }
        #endregion

        #region "Methods"
        /// <summary>
        /// Performs an archive or update operation
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        bool PerformTask();
        #endregion

        #region "Event Delegates and Classes"

        event MyEMSLUploadEventHandler MyEMSLUploadComplete;

        #endregion

    }

}
