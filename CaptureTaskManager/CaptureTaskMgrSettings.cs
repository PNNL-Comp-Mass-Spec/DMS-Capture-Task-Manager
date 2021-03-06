﻿//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using PRISMDatabaseUtils;
using PRISMDatabaseUtils.AppSettings;

namespace CaptureTaskManager
{
    /// <summary>
    /// Class for loading, storing and accessing manager parameters.
    /// </summary>
    /// <remarks>
    /// Loads initial settings from local config file, then checks to see if remainder of settings
    /// should be loaded or manager set to inactive. If manager active, retrieves remainder of settings
    /// from manager control database.
    /// </remarks>
    public class CaptureTaskMgrSettings : MgrSettingsDB, IMgrParams
    {
        // Ignore Spelling: Utils, Ack, Seqs

        #region "Constants"

        private const string SP_NAME_ACK_MANAGER_UPDATE = "AckManagerUpdateRequired";

        /// <summary>
        /// Connection string to DMS5
        /// </summary>
        public const string MGR_PARAM_DEFAULT_DMS_CONN_STRING = "DefaultDMSConnString";

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="traceMode"></param>
        /// <remarks></remarks>
        public CaptureTaskMgrSettings(bool traceMode)
        {
            TraceMode = traceMode;
        }

        /// <summary>
        /// Calls stored procedure AckManagerUpdateRequired to acknowledge that the manager has exited so that an update can be applied
        /// </summary>
        public void AckManagerUpdateRequired()
        {
            try
            {
                // Data Source=ProteinSeqs;Initial Catalog=manager_control
                var connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING);

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    if (CTMUtilities.OfflineMode)
                    {
                        OnDebugEvent("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since offline");
                    }
                    else
                    {
                        OnDebugEvent("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since the Manager Control connection string is empty");
                    }

                    return;
                }

                ShowTrace("AckManagerUpdateRequired using " + connectionString);

                var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: TraceMode);
                RegisterEvents(dbTools);

                // Set up the command object prior to SP execution
                var cmd = dbTools.CreateCommand(SP_NAME_ACK_MANAGER_UPDATE, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, ManagerName);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);

                // Execute the SP
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling " + SP_NAME_ACK_MANAGER_UPDATE, ex);
            }
        }

        /// <summary>
        /// Check for the existence of a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <returns>True if the parameter is defined, false if not</returns>
        public bool HasParam(string name)
        {
            return MgrParams.ContainsKey(name);
        }

        /// <summary>
        /// Updates manager settings, then loads settings from the database
        /// </summary>
        /// <param name="configFileSettings">Manager settings loaded from file AppName.exe.config</param>
        /// <returns>True if successful; False on error</returns>
        /// <remarks>
        /// Settings are read by method LoadMgrSettingsFromDBWork in PRISMDatabaseUtils\AppSettings\MgrSettingsDB.cs
        /// using the query
        ///   SELECT ParameterName, ParameterValue FROM V_Mgr_Params WHERE ManagerName = 'MgrName';
        /// If one of the manager settings is named 'MgrSettingGroupName', LoadMgrSettingsFromDBWork will be called again to load the group settings,
        /// which are then stored with
        ///   StoreParameters(mgrGroupSettingsFromDB, mgrSettingsGroup, skipExistingParameters: true);
        /// </remarks>
        public bool LoadSettings(Dictionary<string, string> configFileSettings)
        {
            var success = LoadSettings(configFileSettings, true);
            return success;
        }

        #endregion
    }
}
