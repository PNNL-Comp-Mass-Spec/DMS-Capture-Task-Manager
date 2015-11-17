
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
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
	public class clsMgrSettings : IMgrParams
	{
		//*********************************************************************************************************
		//	Class for loading, storing and accessing manager parameters.
		//	Loads initial settings from local config file, then checks to see if remainder of settings should be
		//		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
		//		parameters database.
		//**********************************************************************************************************

		#region "Class variables"
		public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";
		Dictionary<string, string> m_ParamDictionary;
		bool m_MCParamsLoaded;
		string m_ErrMsg = "";
		#endregion

		#region "Properties"
		public string ErrMsg {
			get { return m_ErrMsg; }
		}

		public Dictionary<string, string> TaskDictionary
		{
			get { return m_ParamDictionary; }
		}
		#endregion

		#region "Methods"
		public clsMgrSettings()
		{
			if (!LoadSettings())
			{
			    if (String.Equals(m_ErrMsg, DEACTIVATED_LOCALLY))
					throw new ApplicationException(DEACTIVATED_LOCALLY);
			    
                throw new ApplicationException("Unable to initialize manager settings class: " + m_ErrMsg);
			}
		}

		public bool LoadSettings()
		{
			m_ErrMsg = "";

			// If the param dictionary exists, it needs to be cleared out
			if (m_ParamDictionary != null)
			{
				m_ParamDictionary.Clear();
				m_ParamDictionary = null;
			}


			// Get settings from config file
			m_ParamDictionary = LoadMgrSettingsFromFile();

			// Get directory for main executable
			var appPath = Application.ExecutablePath;
			var fi = new FileInfo(appPath);
			m_ParamDictionary.Add("ApplicationPath", fi.DirectoryName);

			//Test the settings retrieved from the config file
			if (!CheckInitialSettings(m_ParamDictionary))
			{
				//Error logging handled by CheckInitialSettings
				return false;
			}

			//Determine if manager is deactivated locally
			if (!bool.Parse(GetParam("MgrActive_Local", "false")))
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.WARN, DEACTIVATED_LOCALLY);
				m_ErrMsg = DEACTIVATED_LOCALLY;
				return false;
			}

			//Get remaining settings from database
			if (!LoadMgrSettingsFromDB(ref m_ParamDictionary))
			{
				//Error logging handled by LoadMgrSettingsFromDB
				return false;
			}

			// Set flag indicating params have been loaded from manger config db
			m_MCParamsLoaded = true;

			//No problems found
			return true;
		}

		private Dictionary<string, string> LoadMgrSettingsFromFile()
		{
			// Load initial settings into string dictionary for return
			var RetDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

		    //	 My.Settings.Reload()
			//Manager config db connection string
			var TempStr = Properties.Settings.Default.MgrCnfgDbConnectStr;
			RetDict.Add("MgrCnfgDbConnectStr", TempStr);

			//Manager active flag
			TempStr = Properties.Settings.Default.MgrActive_Local.ToString();
			RetDict.Add("MgrActive_Local", TempStr);

			//Manager name
			// If the MgrName setting in the CaptureTaskManager.exe.config file contains the text $ComputerName$
			// then that text is replaced with this computer's domain name
			// This is a case-sensitive comparison
			//
			TempStr = Properties.Settings.Default.MgrName;
			TempStr = TempStr.Replace("$ComputerName$", Environment.MachineName);
			RetDict.Add("MgrName", TempStr);

			//Default settings in use flag
			TempStr = Properties.Settings.Default.UsingDefaults.ToString();
			RetDict.Add("UsingDefaults", TempStr);

			return RetDict;
		}

		/// <summary>
		/// Calls stored procedure AckManagerUpdateRequired to acknowledge that the manager has exited so that an update can be applied
		/// </summary>
		public void AckManagerUpdateRequired()
		{
			const string SP_NAME_ACKMANAGERUPDATE = "AckManagerUpdateRequired";

		    try
			{
				var ConnectionString = GetParam("MgrCnfgDbConnectStr");

				var MyConnection = new SqlConnection(ConnectionString);
				MyConnection.Open();

				//Set up the command object prior to SP execution
				var MyCmd = MyConnection.CreateCommand();
				{
					MyCmd.CommandType = CommandType.StoredProcedure;
					MyCmd.CommandText = SP_NAME_ACKMANAGERUPDATE;

					MyCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
					MyCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

					MyCmd.Parameters.Add(new SqlParameter("@managerName", SqlDbType.VarChar, 128));
					MyCmd.Parameters["@managerName"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@managerName"].Value = GetParam("MgrName");

					MyCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512));
					MyCmd.Parameters["@message"].Direction = ParameterDirection.Output;
					MyCmd.Parameters["@message"].Value = "";
				}

				//Execute the SP
				MyCmd.ExecuteNonQuery();

			}
			catch (Exception ex)
			{
			    const string strErrorMessage = "Exception calling " + SP_NAME_ACKMANAGERUPDATE;
			    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage + ex.Message);
			}
		}

		private bool CheckInitialSettings(Dictionary<string, string> InpDict)
		{
			// Verify manager settings dictionary exists
			if (InpDict == null)
			{
				m_ErrMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_ErrMsg);
				return false;
			}

			// Verify intact config file was found
			string strValue;
			if (!InpDict.TryGetValue("UsingDefaults", out strValue))
			{
				m_ErrMsg = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file";
				Console.WriteLine(m_ErrMsg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_ErrMsg);
			}
			else
			{
				bool blnValue;

				if (bool.TryParse(strValue, out blnValue))
				{
					if (blnValue)
					{
						m_ErrMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True";
						Console.WriteLine(m_ErrMsg);
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_ErrMsg);
						return false;
					}
				}
			}

			//No problems found
			return true;
		}

        private string GetGroupNameFromSettings(DataTable dtSettings)
        {

            foreach (DataRow oRow in dtSettings.Rows)
            {
                //Add the column heading and value to the dictionary
                var paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);

                if (string.Equals(paramKey, "MgrSettingGroupName", StringComparison.CurrentCultureIgnoreCase))
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

		public bool LoadMgrSettingsFromDB()
		{
		    const bool logConnectionErrors = true;
		    return LoadMgrSettingsFromDB(ref m_ParamDictionary, logConnectionErrors);
		}

	    public bool LoadMgrSettingsFromDB(bool logConnectionErrors)
		{
			return LoadMgrSettingsFromDB(ref m_ParamDictionary, logConnectionErrors);
		}

		public bool LoadMgrSettingsFromDB(ref Dictionary<string, string> MgrSettingsDict)
		{
		    const bool logConnectionErrors = true;
		    return LoadMgrSettingsFromDB(ref m_ParamDictionary, logConnectionErrors);
		}

	    public bool LoadMgrSettingsFromDB(ref Dictionary<string, string> MgrSettingsDict, bool logConnectionErrors)
		{
			//Requests manager parameters from database. Input string specifies view to use. Performs retries if necessary.

	        DataTable dtSettings;

	        var managerName = GetParam("MgrName", "");

			if (string.IsNullOrEmpty(managerName))
			{
				m_ErrMsg = "MgrName parameter not found in m_ParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
				WriteErrorMsg(m_ErrMsg);
				return false;	
			}

			var returnErrorIfNoParameters = true;
			var success = LoadMgrSettingsFromDBWork(managerName, out dtSettings, logConnectionErrors, returnErrorIfNoParameters);
			if (!success)
			{
				return false;
			}

			var skipExistingParameters = false;
			success = StoreParameters(dtSettings, skipExistingParameters, managerName);

	        if (!success)
	            return false;

            while (success)
            {
                var strMgrSettingsGroup = GetGroupNameFromSettings(dtSettings);
                if (string.IsNullOrEmpty(strMgrSettingsGroup))
                {
                    break; 
                }

                // This manager has group-based settings defined; load them now

                returnErrorIfNoParameters = false;
                success = LoadMgrSettingsFromDBWork(strMgrSettingsGroup, out dtSettings, logConnectionErrors, returnErrorIfNoParameters);

                if (success)
                {
                    skipExistingParameters = true;
                    success = StoreParameters(dtSettings, skipExistingParameters, managerName);
                }

            }

			return success;

		}

		private bool LoadMgrSettingsFromDBWork(string managerName, out DataTable dtSettings, bool logConnectionErrors, bool returnErrorIfNoParameters)
		{

			short retryCount = 3;
			var DBConnectionString = GetParam("MgrCnfgDbConnectStr", "");
			dtSettings = null;

			if (string.IsNullOrEmpty(DBConnectionString))
			{
				m_ErrMsg = "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
				WriteErrorMsg(m_ErrMsg);
				return false;
			}

			var sqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + managerName + "'";

			// Get a datatable holding the parameters for this manager
			while (retryCount > 0)
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
					var myMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " + ex.Message;
					myMsg = myMsg + ", RetryCount = " + retryCount;
					if (logConnectionErrors)
						WriteErrorMsg(myMsg, allowLogToDB: false);
					//Delay for 5 seconds before trying again
					System.Threading.Thread.Sleep(5000);
				}
			}

			// If loop exited due to errors, return false
			if (retryCount < 1)
			{
                // Log the message to the DB if the monthly Windows updates are not pending
                var allowLogToDB = !(clsWindowsUpdateStatus.ServerUpdatesArePending());

				m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
				if (logConnectionErrors)
					WriteErrorMsg(m_ErrMsg, allowLogToDB);
				return false;
			}

			// Validate that the data table object is initialized
			if (dtSettings == null)
			{
				// Data table not initialized
				m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; dtSettings datatable is null; using " + DBConnectionString;
				if (logConnectionErrors)
					WriteErrorMsg(m_ErrMsg);
				return false;
			}

			// Verify at least one row returned
			if (dtSettings.Rows.Count < 1 && returnErrorIfNoParameters)
			{
				//Wrong number of rows returned
				m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Manager " + managerName + " not defined in the manager control database; using " + DBConnectionString;
				WriteErrorMsg(m_ErrMsg);
				dtSettings.Dispose();
				return false;
			}

			return true;

		}

		public bool StoreParameters(DataTable dtSettings, bool skipExistingParameters, string managerName)
		{
		    bool success;

			// Fill a string dictionary with the manager parameters that have been found
			try
			{
				foreach (DataRow oRow in dtSettings.Rows)
				{
					//Add the column heading and value to the dictionary
					var paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);
					var paramVal = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);

					if (paramKey.ToLower() == "perspective" && Environment.MachineName.ToLower().StartsWith("monroe"))
					{
						if (paramVal.ToLower() == "server")
						{
							paramVal = "client";
							Console.WriteLine(@"StoreParameters: Overriding manager perspective to be 'client' because impersonating a server-based manager from an office computer");
						}
					}

					if (m_ParamDictionary.ContainsKey(paramKey))
					{
						if (!skipExistingParameters)
						{
							m_ParamDictionary[paramKey] = paramVal;
						}
					}
					else
					{
						m_ParamDictionary.Add(paramKey, paramVal);
					}
				}
				success = true;
			}
			catch (Exception ex)
			{
				m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table for manager '" + managerName + "': " + ex.Message;
				WriteErrorMsg(m_ErrMsg);
				success = false;
			}
			finally
			{
				if (dtSettings != null)
					dtSettings.Dispose();
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

            bool itemBool;
            if (bool.TryParse(itemValue, out itemBool))
                return itemBool;

            return false;

        }

		public string GetParam(string itemKey)
		{
			return GetParam(itemKey, string.Empty);
		}

		/// <summary>
		/// Gets a stored parameter
		/// </summary>
        /// <param name="itemKey">Parameter name</param>
		/// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
		/// <returns>Parameter value if found, otherwise empty string</returns>
		public string GetParam(string itemKey, string valueIfMissing)
		{
			string itemValue;
			if (m_ParamDictionary.TryGetValue(itemKey, out itemValue))
			{
				return itemValue ?? string.Empty;
			}
		    
            return valueIfMissing ?? string.Empty;
		}

		public void SetParam(string itemKey, string itemValue)
		{
			if (m_ParamDictionary.ContainsKey(itemKey))
			{
				m_ParamDictionary[itemKey] = itemValue;
			}
			else
			{
				m_ParamDictionary.Add(itemKey, itemValue);
			}
		}

		/// <summary>
		/// Writes specfied value to an application config file.
		/// </summary>
		/// <param name="Key">Name for parameter (case sensitive)</param>
		/// <param name="Value">New value for parameter</param>
		/// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
		/// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
		public bool WriteConfigSetting(string Key, string Value)
		{

			m_ErrMsg = "";

			//Load the config document
			var MyDoc = LoadConfigDocument();
			if (MyDoc == null)
			{
				//Error message has already been produced by LoadConfigDocument
				return false;
			}

			//Retrieve the settings node
			var MyNode = MyDoc.SelectSingleNode("//applicationSettings");

			if (MyNode == null)
			{
				m_ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
				return false;
			}

			try
			{
				//Select the eleement containing the value for the specified key containing the key
				var MyElement = (XmlElement)MyNode.SelectSingleNode(string.Format("//setting[@name='{0}']/value", Key));
				if (MyElement != null)
				{
					//Set key to specified value
					MyElement.InnerText = Value;
				}
				else
				{
					//Key was not found
					m_ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + Key;
					return false;
				}
				MyDoc.Save(GetConfigFilePath());
				return true;
			}
			catch (Exception ex)
			{
				m_ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
				return false;

			}
		} // End sub

		/// <summary>
		/// Loads an app config file for changing parameters
		/// </summary>
		/// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
		private XmlDocument LoadConfigDocument()
		{
		    try
			{
				var MyDoc = new XmlDocument();
				MyDoc.Load(GetConfigFilePath());
				return MyDoc;
			}
			catch (Exception ex)
			{
				m_ErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
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

		private string DbCStr(object InpObj)
		{
		    if (InpObj == null)
			{
				return "";
			}
		    
            return InpObj.ToString();
		}

	    private void WriteErrorMsg(string errorMessage, bool allowLogToDB = true)
		{
			if (m_MCParamsLoaded || !allowLogToDB)
			{
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);
			}
			else
			{
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, errorMessage);
			}
		}
		#endregion
	}	// End class
}	// End namespace
