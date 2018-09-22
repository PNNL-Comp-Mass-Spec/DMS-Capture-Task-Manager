//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Linq;
using PRISM;

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
    public class clsMgrSettings : clsLoggerBase, IMgrParams
    {

        #region "Constants"

        const string SP_NAME_ACK_MANAGER_UPDATE = "AckManagerUpdateRequired";

        /// <summary>
        /// Status message for when the manager is deactivated locally
        /// </summary>
        /// <remarks>Used when MgrActive_Local is False in AppName.exe.config</remarks>
        public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";

        /// <summary>
        /// Manager parameter: config database connection string
        /// </summary>
        public const string MGR_PARAM_MGR_CFG_DB_CONN_STRING = "MgrCnfgDbConnectStr";
        /// <summary>
        /// Manager parameter: manager active
        /// </summary>
        /// <remarks>Defined in AppName.exe.config</remarks>
        public const string MGR_PARAM_MGR_ACTIVE_LOCAL = "MgrActive_Local";

        /// <summary>
        /// Manager parameter: manager name
        /// </summary>
        public const string MGR_PARAM_MGR_NAME = "MgrName";

        /// <summary>
        /// Manager parameter: using defaults flag
        /// </summary>
        public const string MGR_PARAM_USING_DEFAULTS = "UsingDefaults";

        /// <summary>
        /// Connection string to DMS5
        /// </summary>
        public const string MGR_PARAM_DEFAULT_DMS_CONN_STRING = "DefaultDMSConnString";

        #endregion
        #region "Class variables"

        private bool mMCParamsLoaded;

        private string mErrMsg = string.Empty;

        #endregion

        #region "Properties"

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrMsg => mErrMsg;

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName => GetParam(MGR_PARAM_MGR_NAME, Environment.MachineName + "_Undefined-Manager");

        /// <summary>
        /// Manager parameters dictionary
        /// </summary>
        public Dictionary<string, string> ParamDictionary { get; }

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="traceMode"></param>
        /// <remarks></remarks>
        public clsMgrSettings(bool traceMode)
        {
            TraceMode = traceMode;

            ParamDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var success = LoadSettings();

            if (TraceMode)
            {
                ShowTraceMessage("Initialized clsMgrSettings");
                ShowDictionaryTrace(ParamDictionary);
            }

            if (!success)
            {
                if (string.Equals(ErrMsg, DEACTIVATED_LOCALLY))
                    throw new ApplicationException(DEACTIVATED_LOCALLY);

                throw new ApplicationException("Unable to initialize manager settings class: " + ErrMsg);
            }
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
                    if (clsUtilities.OfflineMode)
                        LogDebug("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since offline");
                    else
                        LogError("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since the Manager Control connection string is empty");

                    return;
                }

                if (TraceMode)
                    ShowTraceMessage("AckManagerUpdateRequired using " + connectionString);

                var conn = new SqlConnection(connectionString);
                conn.Open();

                // Set up the command object prior to SP execution
                var spCmd = new SqlCommand(SP_NAME_ACK_MANAGER_UPDATE, conn) {
                    CommandType = CommandType.StoredProcedure
                };

                spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                spCmd.Parameters.Add(new SqlParameter("@managerName", SqlDbType.VarChar, 128)).Value = ManagerName;
                spCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                // Execute the SP
                spCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                LogError("Exception calling " + SP_NAME_ACK_MANAGER_UPDATE, ex);
            }
        }

        /// <summary>
        /// Updates manager settings, then loads settings from the database or from ManagerSettingsLocal.xml if clsUtilities.OfflineMode is true
        /// </summary>
        /// <returns>True if successful; False on error</returns>
        /// <remarks></remarks>
        public bool LoadSettings()
        {
            // Get settings from config file
            var configFileSettings = LoadMgrSettingsFromFile();

            return LoadSettings(configFileSettings);
        }

        /// <summary>
        /// Updates manager settings, then loads settings from the database
        /// </summary>
        /// <param name="configFileSettings">Manager settings loaded from file AppName.exe.config</param>
        /// <returns>True if successful; False on error</returns>
        /// <remarks></remarks>
        public bool LoadSettings(Dictionary<string, string> configFileSettings)
        {
            mErrMsg = string.Empty;

            ParamDictionary.Clear();

            foreach (var item in configFileSettings)
            {
                ParamDictionary.Add(item.Key, item.Value);
            }

            // Get directory for main executable
            var appPath = PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath();
            var fi = new FileInfo(appPath);
            ParamDictionary.Add("ApplicationPath", fi.DirectoryName);

            // Test the settings retrieved from the config file
            if (!CheckInitialSettings(ParamDictionary))
            {
                // Error logging handled by CheckInitialSettings
                return false;
            }

            // Determine if manager is deactivated locally
            if (!ParamDictionary.TryGetValue(MGR_PARAM_MGR_ACTIVE_LOCAL, out var activeLocalText))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_MGR_ACTIVE_LOCAL + " is missing from file " + Path.GetFileName(GetConfigFilePath());
                LogError(mErrMsg);
            }

            if (!bool.TryParse(activeLocalText, out var activeLocal) || !activeLocal)
            {
                LogWarning(DEACTIVATED_LOCALLY);
                mErrMsg = DEACTIVATED_LOCALLY;
                return false;
            }

            // Get remaining settings from database
            if (!LoadMgrSettingsFromDB())
            {
                // Error logging handled by LoadMgrSettingsFromDB
                return false;
            }

            // Set flag indicating manager parameters have been loaded
            mMCParamsLoaded = true;

            // No problems found
            return true;
        }

        private Dictionary<string, string> LoadMgrSettingsFromFile()
        {
            // Note: When you are editing this project using the Visual Studio IDE, if you edit the values
            //  ->Properties>Settings.settings, when you run the program (from within the IDE), it
            //  will update file CaptureTaskManager.exe.config with your settings

            // Load initial settings into string dictionary for return
            var mgrSettingsFromFile = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Manager config DB connection string
            var mgrCfgDBConnString = Properties.Settings.Default.MgrCnfgDbConnectStr;
            mgrSettingsFromFile.Add(MGR_PARAM_MGR_CFG_DB_CONN_STRING, mgrCfgDBConnString);

            // Manager active flag
            var mgrActiveLocal = Properties.Settings.Default.MgrActive_Local.ToString();
            mgrSettingsFromFile.Add(MGR_PARAM_MGR_ACTIVE_LOCAL, mgrActiveLocal);

            // Manager name
            // If the MgrName setting in the AppName.exe.config file contains the text $ComputerName$
            // that text is replaced with this computer's domain name
            // This is a case-sensitive comparison
            //
            var managerName = Properties.Settings.Default.MgrName;
            var autoDefinedName = managerName.Replace("$ComputerName$", Environment.MachineName);

            if (!string.Equals(managerName, autoDefinedName))
            {
                ShowTraceMessage("Auto-defining the manager name as " + autoDefinedName);
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, autoDefinedName);
            }
            else
            {
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, managerName);
            }

            // Default settings in use flag
            var usingDefaults = Properties.Settings.Default.UsingDefaults.ToString();
            mgrSettingsFromFile.Add(MGR_PARAM_USING_DEFAULTS, usingDefaults);

            // Default connection string for logging errors to the database
            // Will get updated later when manager settings are loaded from the manager control database
            var defaultDMSConnectionString = Properties.Settings.Default.DefaultDMSConnString;
            mgrSettingsFromFile.Add(MGR_PARAM_DEFAULT_DMS_CONN_STRING, defaultDMSConnectionString);

            if (TraceMode)
            {

                var exePath = PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath();
                var configFilePath = exePath + ".config";
                ShowTraceMessage("Settings loaded from " + PathUtils.CompactPathString(configFilePath, 60));
                ShowDictionaryTrace(mgrSettingsFromFile);
            }

            return mgrSettingsFromFile;
        }

        /// <summary>
        /// Tests initial settings retrieved from config file
        /// </summary>
        /// <param name="paramDictionary"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool CheckInitialSettings(IReadOnlyDictionary<string, string> paramDictionary)
        {
            // Verify manager settings dictionary exists
            if (paramDictionary == null)
            {
                mErrMsg = "CheckInitialSettings: Manager parameter string dictionary not found";
                LogError(mErrMsg, true);
                return false;
            }

            // Verify intact config file was found
            if (!paramDictionary.TryGetValue(MGR_PARAM_USING_DEFAULTS, out var usingDefaultsText))
            {
                mErrMsg = "CheckInitialSettings: 'UsingDefaults' entry not found in Config file";
                LogError(mErrMsg, true);
            }
            else
            {
                if (bool.TryParse(usingDefaultsText, out var usingDefaults) && usingDefaults)
                {
                    mErrMsg = "CheckInitialSettings: Config file problem, contains UsingDefaults=True";
                    LogError(mErrMsg, true);
                    return false;
                }
            }

            // No problems found
            return true;
        }

        private string GetGroupNameFromSettings(DataTable dtSettings)
        {
            foreach (DataRow currentRow in dtSettings.Rows)
            {
                // Add the column heading and value to the dictionary
                var paramKey = DbCStr(currentRow[dtSettings.Columns["ParameterName"]]);

                if (string.Equals(paramKey, "MgrSettingGroupName", StringComparison.OrdinalIgnoreCase))
                {
                    var groupName = DbCStr(currentRow[dtSettings.Columns["ParameterValue"]]);
                    if (!string.IsNullOrWhiteSpace(groupName))
                    {
                        return groupName;
                    }

                    return string.Empty;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Gets manager config settings from manager control DB (Manager_Control)
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Performs retries if necessary.</remarks>
        public bool LoadMgrSettingsFromDB(bool logConnectionErrors = true)
        {

            var managerName = GetParam(MGR_PARAM_MGR_NAME, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_MGR_NAME + " is missing from file " + Path.GetFileName(GetConfigFilePath());
                LogError(mErrMsg);
                return false;
            }

            var success = LoadMgrSettingsFromDBWork(managerName, out var dtSettings, logConnectionErrors, returnErrorIfNoParameters: true);
            if (!success)
            {
                return false;
            }

            success = StoreParameters(dtSettings, skipExistingParameters: false, managerName: managerName);

            if (!success)
                return false;

            while (success)
            {
                var mgrSettingsGroup = GetGroupNameFromSettings(dtSettings);
                if (string.IsNullOrEmpty(mgrSettingsGroup))
                {
                    break;
                }

                // This manager has group-based settings defined; load them now

                success = LoadMgrSettingsFromDBWork(mgrSettingsGroup, out dtSettings, logConnectionErrors, returnErrorIfNoParameters: false);

                if (success)
                {
                    success = StoreParameters(dtSettings, skipExistingParameters: true, managerName: mgrSettingsGroup);
                }
            }

            return success;
        }

        private bool LoadMgrSettingsFromDBWork(
            string managerName,
            out DataTable dtSettings,
            bool logConnectionErrors,
            bool returnErrorIfNoParameters)
        {
            const short retryCount = 6;

            // Data Source=ProteinSeqs;Initial Catalog=manager_control
            var connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                mErrMsg = "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; " +
                          "it should be defined in the " + Path.GetFileName(GetConfigFilePath()) + " file";

                if (TraceMode)
                    ShowTraceMessage("LoadMgrSettingsFromDBWork: " + mErrMsg);

                dtSettings = null;
                return false;
            }

            if (string.IsNullOrEmpty(connectionString))
            {
                mErrMsg = MGR_PARAM_MGR_CFG_DB_CONN_STRING +
                           " parameter not found in ParamDictionary; it should be defined in the " + Path.GetFileName(GetConfigFilePath()) + " file";
                WriteErrorMsg(mErrMsg);
                dtSettings = null;
                return false;
            }

            if (TraceMode)
                ShowTraceMessage("LoadMgrSettingsFromDBWork using [" + connectionString + "] for manager " + managerName);

            var sqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + managerName + "'";

            // Get a table to hold the results of the query
            var success = clsUtilities.GetDataTableByQuery(sqlStr, connectionString, "LoadMgrSettingsFromDBWork", retryCount, out dtSettings);

            // If unable to retrieve the data, return false
            if (!success)
            {
                // Log the message to the DB if the monthly Windows updates are not pending
                var allowLogToDB = !WindowsUpdateStatus.ServerUpdatesArePending();

                mErrMsg = "LoadMgrSettingsFromDBWork; Excessive failures attempting to retrieve manager settings from database " +
                          "for manager '" + managerName + "'";
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg, allowLogToDB);
                dtSettings?.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (dtSettings.Rows.Count < 1 && returnErrorIfNoParameters)
            {
                // No data was returned
                mErrMsg = "LoadMgrSettingsFromDBWork; Manager '" + managerName + "' not defined in the manager control database; using " + connectionString;
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg);
                dtSettings?.Dispose();
                return false;
            }

            return true;
        }

        /// <summary>
        /// Update ParamDictionary with settings in dtSettings, optionally skipping existing parameters
        /// </summary>
        /// <param name="dtSettings"></param>
        /// <param name="skipExistingParameters"></param>
        /// <param name="managerName"></param>
        /// <returns></returns>
        private bool StoreParameters(DataTable dtSettings, bool skipExistingParameters, string managerName)
        {
            bool success;

            try
            {
                foreach (DataRow currentRow in dtSettings.Rows)
                {
                    // Add the column heading and value to the dictionary
                    var paramKey = DbCStr(currentRow[dtSettings.Columns["ParameterName"]]);
                    var paramVal = DbCStr(currentRow[dtSettings.Columns["ParameterValue"]]);

                    if (paramKey.ToLower() == "perspective" && Environment.MachineName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase))
                    {
                        if (paramVal.ToLower() == "server")
                        {
                            paramVal = "client";
                            Console.WriteLine(
                                @"StoreParameters: Overriding manager perspective to be 'client' because impersonating a server-based manager from an office computer");
                        }
                    }

                    if (ParamDictionary.ContainsKey(paramKey))
                    {
                        if (!skipExistingParameters)
                        {
                            ParamDictionary[paramKey] = paramVal;
                        }
                    }
                    else
                    {
                        ParamDictionary.Add(paramKey, paramVal);
                    }
                }
                success = true;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsAnalysisMgrSettings.StoreParameters; Exception filling string dictionary from table for manager " +
                          "'" + managerName + "': " + ex.Message;
                WriteErrorMsg(mErrMsg);
                success = false;
            }
            finally
            {
                dtSettings?.Dispose();
            }

            return success;
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <returns>String value associated with specified key</returns>
        /// <remarks>Returns empty string if key isn't found</remarks>
        public string GetParam(string itemKey)
        {
            if (ParamDictionary == null)
                return string.Empty;

            if (!ParamDictionary.TryGetValue(itemKey, out var value))
                return string.Empty;

            return string.IsNullOrWhiteSpace(value) ? string.Empty : value;
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public bool GetParam(string itemKey, bool valueIfMissing)
        {
            return clsConversion.CBoolSafe(GetParam(itemKey), valueIfMissing);
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public int GetParam(string itemKey, int valueIfMissing)
        {
            return clsConversion.CIntSafe(GetParam(itemKey), valueIfMissing);
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public string GetParam(string itemKey, string valueIfMissing)
        {
            var value = GetParam(itemKey);
            if (string.IsNullOrEmpty(value))
            {
                return valueIfMissing;
            }

            return value;
        }

        /// <summary>
        /// Sets a parameter in the parameters string dictionary
        /// </summary>
        /// <param name="itemKey">Key name for the item</param>
        /// <param name="itemValue">Value to assign to the key</param>
        /// <remarks></remarks>
        public void SetParam(string itemKey, string itemValue)
        {
            if (ParamDictionary.ContainsKey(itemKey))
            {
                ParamDictionary[itemKey] = itemValue;
            }
            else
            {
                ParamDictionary.Add(itemKey, itemValue);
            }
        }

        /// <summary>
        /// Show contents of a dictionary
        /// </summary>
        /// <param name="settings"></param>
        public static void ShowDictionaryTrace(IReadOnlyDictionary<string, string> settings)
        {
            Console.ForegroundColor = ConsoleMsgUtils.DebugFontColor;
            foreach (var key in from item in settings.Keys orderby item select item)
            {
                var value = settings[key];
                var keyWidth = Math.Max(30, Math.Ceiling(key.Length / 15.0) * 15);
                var formatString = "  {0,-" + keyWidth + "} {1}";
                Console.WriteLine(formatString, key, value);
            }
            Console.ResetColor();
        }

        private static void ShowTraceMessage(string message)
        {
            clsMainProgram.ShowTraceMessage(message);
        }

        /// <summary>
        /// Converts a database output object that could be dbNull to a string
        /// </summary>
        /// <param name="inpObj"></param>
        /// <returns>String equivalent of object; empty string if object is dbNull</returns>
        /// <remarks></remarks>
        private string DbCStr(object inpObj)
        {
            // If input object is DbNull, returns "", otherwise returns String representation of object
            if (ReferenceEquals(inpObj, DBNull.Value))
            {
                return string.Empty;
            }

            return Convert.ToString(inpObj);
        }

        /// <summary>
        /// Writes an error message to the application log and the database
        /// </summary>
        /// <param name="errorMessage">Message to write</param>
        /// <param name="allowLogToDB"></param>
        /// <remarks></remarks>
        private void WriteErrorMsg(string errorMessage, bool allowLogToDB = true)
        {
            var logToDb = !mMCParamsLoaded && allowLogToDB;
            LogError(errorMessage, logToDb);

            if (TraceMode)
            {
                ShowTraceMessage(errorMessage);
            }
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        private string GetConfigFilePath()
        {
            var configFilePath = PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath() + ".config";
            return configFilePath;
        }

        #endregion
    }
}
