//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/16/2009
//*********************************************************************************************************

using System.Collections.Generic;

namespace CaptureTaskManager
{
    /// <summary>
    /// Interface for manager params storage class
    /// </summary>
    public interface IMgrParams
    {

        #region "Properties"

        Dictionary<string, string> TaskDictionary { get; }

        #endregion

        #region "Methods"

        bool GetBooleanParam(string itemKey);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <returns>String value associated with specified key</returns>
        string GetParam(string itemKey);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        string GetParam(string itemKey, string valueIfMissing);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        bool GetParam(string itemKey, bool valueIfMissing);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        int GetParam(string itemKey, int valueIfMissing);

        /// <summary>
        /// Sets a parameter in the parameters string dictionary
        /// </summary>
        /// <param name="itemKey">Key name for the item</param>
        /// <param name="itemValue">Value to assign to the key</param>
        /// <remarks></remarks>
        void SetParam(string itemKey, string itemValue);

        /// <summary>
        /// Retrieves the manager and global settings from various databases
        /// </summary>
        /// <returns></returns>
        bool LoadMgrSettingsFromDB(bool logConnectionErrors = true);

        #endregion
    }
}