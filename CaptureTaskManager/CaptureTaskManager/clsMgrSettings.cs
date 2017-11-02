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
using System.Xml;
using System.Windows.Forms;
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

        private readonly Dictionary<string, string> mParamDictionary;

        private bool mMCParamsLoaded;
        private string mErrMsg = "";

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

        public Dictionary<string, string> TaskDictionary => mParamDictionary;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsMgrSettings()
        {
            mParamDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (!LoadSettings())
            {
                if (string.Equals(mErrMsg, DEACTIVATED_LOCALLY))
                    throw new ApplicationException(DEACTIVATED_LOCALLY);

                throw new ApplicationException("Unable to initialize manager settings class: " + mErrMsg);
            }
        }

        /// <summary>
        /// Updates manager settings, then loads settings from the database or from ManagerSettingsLocal.xml if clsGlobal.OfflineMode is true
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
        /// Updates manager settings, then loads settings from the database or from ManagerSettingsLocal.xml if clsGlobal.OfflineMode is true
        /// </summary>
        /// <param name="configFileSettings">Manager settings loaded from file AnalysisManagerProg.exe.config</param>
        /// <returns>True if successful; False on error</returns>
        /// <remarks></remarks>
        public bool LoadSettings(Dictionary<string, string> configFileSettings)
        {
            mErrMsg = string.Empty;

            mParamDictionary.Clear();

            foreach (var item in configFileSettings)
            {
                mParamDictionary.Add(item.Key, item.Value);
            }

            // Get directory for main executable
            var appPath = Application.ExecutablePath;
            var fi = new FileInfo(appPath);
            mParamDictionary.Add("ApplicationPath", fi.DirectoryName);

            // Test the settings retrieved from the config file
            if (!CheckInitialSettings(mParamDictionary))
            {
                // Error logging handled by CheckInitialSettings
                return false;
            }

            // Determine if manager is deactivated locally
            if (!mParamDictionary.TryGetValue(MGR_PARAM_MGR_ACTIVE_LOCAL, out var activeLocalText))
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

            // Set flag indicating params have been loaded from manger config db
            mMCParamsLoaded = true;

            // No problems found
            return true;
        }

        private Dictionary<string, string> LoadMgrSettingsFromFile()
        {
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
            var mgrName = Properties.Settings.Default.MgrName;
            if (mgrName.Contains("$ComputerName$"))
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, mgrName.Replace("$ComputerName$", Environment.MachineName));
            else
                mgrSettingsFromFile.Add(MGR_PARAM_MGR_NAME, mgrName);

            // Default settings in use flag
            var usingDefaults = Properties.Settings.Default.UsingDefaults.ToString();
            mgrSettingsFromFile.Add(MGR_PARAM_USING_DEFAULTS, usingDefaults);

            // Default connection string for logging errors to the database
            // Will get updated later when manager settings are loaded from the manager control database
            var defaultDMSConnectionString = Properties.Settings.Default.DefaultDMSConnString;
            mgrSettingsFromFile.Add(MGR_PARAM_DEFAULT_DMS_CONN_STRING, defaultDMSConnectionString);

            return mgrSettingsFromFile;
        }

        /// <summary>
        /// Calls stored procedure AckManagerUpdateRequired to acknowledge that the manager has exited so that an update can be applied
        /// </summary>
        public void AckManagerUpdateRequired()
        {
            const string SP_NAME_ACKMANAGERUPDATE = "AckManagerUpdateRequired";

            try
            {
                var ConnectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING);

                var conn = new SqlConnection(ConnectionString);
                conn.Open();

                // Set up the command object prior to SP execution
                var cmd = conn.CreateCommand();
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandText = SP_NAME_ACKMANAGERUPDATE;

                    cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

                    cmd.Parameters.Add(new SqlParameter("@managerName", SqlDbType.VarChar, 128)).Value = GetParam(MGR_PARAM_MGR_NAME);

                    cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;
                }

                // Execute the SP
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                LogError("Exception calling " + SP_NAME_ACKMANAGERUPDATE, ex);
            }
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
            foreach (DataRow oRow in dtSettings.Rows)
            {
                // Add the column heading and value to the dictionary
                var paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);

                if (string.Equals(paramKey, "MgrSettingGroupName", StringComparison.OrdinalIgnoreCase))
                {
                    var groupName = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);
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
        /// Retrieves the manager and global settings from various databases
        /// </summary>
        /// <returns></returns>
        public bool LoadMgrSettingsFromDB()
        {
            return LoadMgrSettingsFromDB(logConnectionErrors: true);
        }

        /// <summary>
        /// Gets manager config settings from manager control DB (Manager_Control)
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Performs retries if necessary.</remarks>
        public bool LoadMgrSettingsFromDB(bool logConnectionErrors)
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

        private bool LoadMgrSettingsFromDBWork(string managerName, out DataTable dtSettings, bool logConnectionErrors,
                                               bool returnErrorIfNoParameters, int retryCount = 3)
        {
            var DBConnectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING, "");
            dtSettings = null;

            if (string.IsNullOrEmpty(DBConnectionString))
            {
                mErrMsg = MGR_PARAM_MGR_CFG_DB_CONN_STRING +
                           " parameter not found in mParamDictionary; it should be defined in the " + Path.GetFileName(GetConfigFilePath()) + " file";
                WriteErrorMsg(mErrMsg);
                return false;
            }

            var sqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + managerName + "'";

            // Get a table holding the parameters for this manager
            while (retryCount >= 0)
            {
                try
                {
                    using (var cn = new SqlConnection(DBConnectionString))
                    {
                        var cmd = new SqlCommand
                        {
                            CommandType = CommandType.Text,
                            CommandText = sqlStr,
                            Connection = cn,
                            CommandTimeout = 30
                        };

                        using (var da = new SqlDataAdapter(cmd))
                        {
                            using (var ds = new DataSet())
                            {
                                da.Fill(ds);
                                dtSettings = ds.Tables[0];
                            }
                        }
                    }
                    break;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var msg = string.Format("LoadMgrSettingsFromDB; Exception getting manager settings from database: {0}; " +
                                            "ConnectionString: {1}, RetryCount = {2}",
                                            ex.Message, DBConnectionString, retryCount);

                    if (logConnectionErrors)
                        WriteErrorMsg(msg, allowLogToDB: false);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

            // If loop exited due to errors, return false
            if (retryCount < 0)
            {
                // Log the message to the DB if the monthly Windows updates are not pending
                var allowLogToDB = !clsWindowsUpdateStatus.ServerUpdatesArePending();

                mErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg, allowLogToDB);

                return false;
            }

            // Validate that the data table object is initialized
            if (dtSettings == null)
            {
                // Data table not initialized
                mErrMsg = "LoadMgrSettingsFromDB; dtSettings datatable is null; using " +
                           DBConnectionString;
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg);

                return false;
            }

            // Verify at least one row returned
            if (dtSettings.Rows.Count < 1 && returnErrorIfNoParameters)
            {
                // Wrong number of rows returned
                mErrMsg = "LoadMgrSettingsFromDB; Manager " + managerName + " not defined in the manager control database; using " + DBConnectionString;
                WriteErrorMsg(mErrMsg);
                dtSettings.Dispose();
                return false;
            }

            return true;
        }

        /// <summary>
        /// Update mParamDictionary with settings in dtSettings, optionally skipping existing parameters
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
                foreach (DataRow oRow in dtSettings.Rows)
                {
                    // Add the column heading and value to the dictionary
                    var paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);
                    var paramVal = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);

                    if (paramKey.ToLower() == "perspective" && Environment.MachineName.ToLower().StartsWith("monroe"))
                    {
                        if (paramVal.ToLower() == "server")
                        {
                            paramVal = "client";
                            Console.WriteLine(
                                @"StoreParameters: Overriding manager perspective to be 'client' because impersonating a server-based manager from an office computer");
                        }
                    }

                    if (mParamDictionary.ContainsKey(paramKey))
                    {
                        if (!skipExistingParameters)
                        {
                            mParamDictionary[paramKey] = paramVal;
                        }
                    }
                    else
                    {
                        mParamDictionary.Add(paramKey, paramVal);
                    }
                }
                success = true;
            }
            catch (Exception ex)
            {
                mErrMsg = "LoadMgrSettingsFromDB: Exception filling string dictionary from table for manager " +
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
        /// Lookup the value of a boolean parameter
        /// </summary>
        /// <param name="itemKey"></param>
        /// <returns>True/false for the given parameter; false if the parameter is not present</returns>
        public bool GetBooleanParam(string itemKey)
        {
            var itemValue = GetParam(itemKey, string.Empty);

            if (string.IsNullOrWhiteSpace(itemValue))
                return false;

            if (bool.TryParse(itemValue, out var itemBool))
                return itemBool;

            return false;
        }

        public string GetParam(string itemKey)
        {
            return GetParam(itemKey, string.Empty);
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public string GetParam(string itemKey, string valueIfMissing)
        {
            if (mParamDictionary.TryGetValue(itemKey, out var itemValue))
            {
                return itemValue ?? string.Empty;
            }

            return valueIfMissing ?? string.Empty;
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public bool GetParam(string itemKey, bool valueIfMissing)
        {
            if (mParamDictionary.TryGetValue(itemKey, out var valueText))
            {
                var value = clsConversion.CBoolSafe(valueText, valueIfMissing);
                return value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a manager parameter
        /// </summary>
        /// <param name="itemKey">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public int GetParam(string itemKey, int valueIfMissing)
        {
            if (mParamDictionary.TryGetValue(itemKey, out var valueText))
            {
                var value = clsConversion.CIntSafe(valueText, valueIfMissing);
                return value;
            }

            return valueIfMissing;
        }

        public void SetParam(string itemKey, string itemValue)
        {
            if (mParamDictionary.ContainsKey(itemKey))
            {
                mParamDictionary[itemKey] = itemValue;
            }
            else
            {
                mParamDictionary.Add(itemKey, itemValue);
            }
        }

        /// <summary>
        /// Converts a database output object that could be dbNull to a string
        /// </summary>
        /// <param name="inpObj"></param>
        /// <returns>String equivalent of object; empty string if object is dbNull</returns>
        /// <remarks></remarks>
        private string DbCStr(object inpObj)
        {
            if (inpObj == null)
            {
                return "";
            }

            return inpObj.ToString();
        }

        /// <summary>
        /// Writes specfied value to an application config file.
        /// </summary>
        /// <param name="key">Name for parameter (case sensitive)</param>
        /// <param name="value">New value for parameter</param>
        /// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
        /// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
        public bool WriteConfigSetting(string key, string value)
        {
            mErrMsg = "";

            // Load the config document
            var doc = LoadConfigDocument();
            if (doc == null)
            {
                // Error message has already been produced by LoadConfigDocument
                return false;
            }

            // Retrieve the settings node
            var appSettingsNode = doc.SelectSingleNode("//applicationSettings");

            if (appSettingsNode == null)
            {
                mErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
                return false;
            }

            try
            {
                // Select the element containing the value for the specified key containing the key
                var matchingElement = (XmlElement)appSettingsNode.SelectSingleNode(string.Format("//setting[@name='{0}']/value", key));
                if (matchingElement != null)
                {
                    // Set key to specified value
                    matchingElement.InnerText = value;
                }
                else
                {
                    // Key was not found
                    mErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                doc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
                return false;
            }
        }

        private void WriteErrorMsg(string errorMessage, bool allowLogToDB = true)
        {
            var logToDb = !mMCParamsLoaded && allowLogToDB;
            LogError(errorMessage, logToDb);
        }

        /// <summary>
        /// Loads an app config file for changing parameters
        /// </summary>
        /// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
        private XmlDocument LoadConfigDocument()
        {
            try
            {
                var doc = new XmlDocument();
                doc.Load(GetConfigFilePath());
                return doc;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        private string GetConfigFilePath()
        {
            return Application.ExecutablePath + ".config";
        }

        #endregion
    }
}
