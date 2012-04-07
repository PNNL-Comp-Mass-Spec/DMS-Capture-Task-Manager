
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
// Last modified 09/10/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Collections.Specialized;
using System.Xml;
using System.Configuration;
using System.Windows.Forms;

namespace CaptureTaskManager {
	public class clsMgrSettings : IMgrParams {
		//*********************************************************************************************************
		//	Class for loading, storing and accessing manager parameters.
		//	Loads initial settings from local config file, then checks to see if remainder of settings should be
		//		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
		//		parameters database.
		//**********************************************************************************************************

		#region "Class variables"
		private const string DEACTIVATED_LOCALLY = "Manager deactivated locally";
		System.Collections.Generic.Dictionary<string, string> m_ParamDictionary = null;
		bool m_MCParamsLoaded = false;
		#endregion

		#region "Properties"
		public string ErrMsg { get; set; }
			public System.Collections.Generic.Dictionary<string,string> TaskDictionary {
				get { return m_ParamDictionary; }
			}
		#endregion

		#region "Methods"
		public clsMgrSettings() {
			if (!LoadSettings()) {
				if (String.Equals(ErrMsg, DEACTIVATED_LOCALLY))
					throw new ApplicationException(DEACTIVATED_LOCALLY);
				else
					throw new ApplicationException("Unable to initialize manager settings class: " + ErrMsg);
			}
		}	// End sub

		public bool LoadSettings() {
			ErrMsg = "";

			// If the param dictionary exists, it needs to be cleared out
			if (m_ParamDictionary != null) {
				m_ParamDictionary.Clear();
				m_ParamDictionary = null;
			}


			// Get settings from config file
			m_ParamDictionary = LoadMgrSettingsFromFile();

			// Get directory for main executable
			string appPath = Application.ExecutablePath;
			FileInfo fi = new FileInfo(appPath);
			m_ParamDictionary.Add("ApplicationPath", fi.DirectoryName);

			//Test the settings retrieved from the config file
			if (!CheckInitialSettings(m_ParamDictionary)) {
				//Error logging handled by CheckInitialSettings
				return false;
			}

			//Determine if manager is deactivated locally
			if (!bool.Parse(GetParam("MgrActive_Local", "false"))) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.WARN, DEACTIVATED_LOCALLY);
				ErrMsg = DEACTIVATED_LOCALLY;
				return false;
			}

			//Get remaining settings from database
			if (!LoadMgrSettingsFromDB(ref m_ParamDictionary)) {
				//Error logging handled by LoadMgrSettingsFromDB
				return false;
			}

			// Set flag indicating params have been loaded from manger config db
			m_MCParamsLoaded = true;

			//No problems found
			return true;
		}	// End sub

		private System.Collections.Generic.Dictionary<string, string> LoadMgrSettingsFromFile() {
			// Load initial settings into string dictionary for return
			Dictionary<string, string> RetDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
			string TempStr;

			//				My.Settings.Reload()
			//Manager config db connection string
			TempStr = Properties.Settings.Default.MgrCnfgDbConnectStr;
			RetDict.Add("MgrCnfgDbConnectStr", TempStr);

			//Manager active flag
			TempStr = Properties.Settings.Default.MgrActive_Local.ToString();
			RetDict.Add("MgrActive_Local", TempStr);

			//Manager name
			TempStr = Properties.Settings.Default.MgrName;
			RetDict.Add("MgrName", TempStr);

			//Default settings in use flag
			TempStr = Properties.Settings.Default.UsingDefaults.ToString();
			RetDict.Add("UsingDefaults", TempStr);

			return RetDict;
		}	// End sub

		/// <summary>
		/// Calls stored procedure AckManagerUpdateRequired to acknowledge that the manager has exited so that an update can be applied
		/// </summary>
		public void AckManagerUpdateRequired() {
			const string SP_NAME_ACKMANAGERUPDATE = "AckManagerUpdateRequired";
			int RetVal = 0;
			string ConnectionString = null;

			try {
				ConnectionString = this.GetParam("MgrCnfgDbConnectStr");

				System.Data.SqlClient.SqlConnection MyConnection = new System.Data.SqlClient.SqlConnection(ConnectionString);
				MyConnection.Open();

				//Set up the command object prior to SP execution
				System.Data.SqlClient.SqlCommand MyCmd = MyConnection.CreateCommand();
				{
					MyCmd.CommandType = CommandType.StoredProcedure;
					MyCmd.CommandText = SP_NAME_ACKMANAGERUPDATE;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", SqlDbType.Int));
					MyCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@managerName", SqlDbType.VarChar, 128));
					MyCmd.Parameters["@managerName"].Direction = ParameterDirection.Input;
					MyCmd.Parameters["@managerName"].Value = this.GetParam("MgrName");

					MyCmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512));
					MyCmd.Parameters["@message"].Direction = ParameterDirection.Output;
					MyCmd.Parameters["@message"].Value = "";
				}

				//Execute the SP
				RetVal = MyCmd.ExecuteNonQuery();

			} catch (System.Exception ex) {
				string strErrorMessage = "Exception calling " + SP_NAME_ACKMANAGERUPDATE;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage + ex.Message);
			}

		}

		/// <summary>
		/// Convert string to bool; default false if an error
		/// </summary>
		/// <param name="Value"></param>
		/// <returns></returns>
		public static bool CBoolSafe(string Value) {
			return CBoolSafe(Value, false);
		}

		public static bool CBoolSafe(string Value, bool DefaultValue) {
			bool blnValue = DefaultValue;

			if (string.IsNullOrEmpty(Value))
				return DefaultValue;
			else {
				if (bool.TryParse(Value, out blnValue))
					return blnValue;
				else
					return DefaultValue;
			}
		}

		public static int CIntSafe(string Value, int DefaultValue) {
			int intValue = DefaultValue;

			if (string.IsNullOrEmpty(Value))
				return DefaultValue;
			else {
				if (int.TryParse(Value, out intValue))
					return intValue;
				else
					return DefaultValue;
			}
		}

		public static float CSngSafe(string Value, float DefaultValue) {
			float fValue = DefaultValue;

			if (string.IsNullOrEmpty(Value))
				return fValue;
			else {
				if (float.TryParse(Value, out fValue))
					return fValue;
				else
					return fValue;
			}
		}

		private bool CheckInitialSettings(System.Collections.Generic.Dictionary<string, string> InpDict) {
			//Verify manager settings dictionary exists
			if (InpDict == null) {
				ErrMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, ErrMsg);
				return false;
			}

			//Verify intact config file was found
			string strValue = string.Empty;
			if (!InpDict.TryGetValue("UsingDefaults", out strValue)) {
				ErrMsg = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file";
				Console.WriteLine(ErrMsg);
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, ErrMsg);
			} else {
				bool blnValue;

				if (bool.TryParse(strValue, out blnValue)) {
					if (blnValue) {
						ErrMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True";
						Console.WriteLine(ErrMsg);
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, ErrMsg);
						return false;
					}
				}
			}

			//No problems found
			return true;
		}	// End sub

		public bool LoadMgrSettingsFromDB() {
			return LoadMgrSettingsFromDB(ref m_ParamDictionary);
		}	// End sub

		public bool LoadMgrSettingsFromDB(ref System.Collections.Generic.Dictionary<string, string> MgrSettingsDict) {
			//Requests manager parameters from database. Input string specifies view to use. Performs retries if necessary.
			short RetryCount = 3;
			string MyMsg = null;
			string ParamKey = null;
			string ParamVal = null;

			string ManagerName = string.Empty;
			string DBConnectionString = string.Empty;

			try {
				ManagerName = m_ParamDictionary["MgrName"];
			} catch {
				ErrMsg = "MgrName parameter not found in m_ParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
				WriteErrorMsg(ErrMsg);
				return false;
			}

			try {
				DBConnectionString = MgrSettingsDict["MgrCnfgDbConnectStr"];
			} catch {
				ErrMsg = "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; it should be defined in the CaptureTaskManager.exe.config file";
				WriteErrorMsg(ErrMsg);
				return false;
			}

			string SqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '"
										+ ManagerName + "'";

			//Get a table containing data for job
			DataTable Dt = null;

			//Get a datatable holding the parameters for one manager
			while (RetryCount > 0) {
				try {
					using (SqlConnection Cn = new SqlConnection(DBConnectionString)) {
						using (SqlDataAdapter Da = new SqlDataAdapter(SqlStr, Cn)) {
							using (DataSet Ds = new DataSet()) {
								Da.Fill(Ds);
								Dt = Ds.Tables[0];
								//Ds
							}
							//Da
						}
					}
					//Cn
					break;
				} catch (System.Exception ex) {
					RetryCount -= 1;
					MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " + ex.Message;
					MyMsg = MyMsg + ", RetryCount = " + RetryCount.ToString();
					WriteErrorMsg(MyMsg);
					//Delay for 5 seconds before trying again
					System.Threading.Thread.Sleep(5000);
				}
			}

			//If loop exited due to errors, return false
			if (RetryCount < 1) {
				ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database";
				WriteErrorMsg(ErrMsg);
				Dt.Dispose();
				return false;
			}

			//Verify at least one row returned
			if (Dt.Rows.Count < 1) {
				//Wrong number of rows returned
				ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Manager " + ManagerName + " not defined in the manager control database; using " + DBConnectionString;
				WriteErrorMsg(ErrMsg);
				Dt.Dispose();
				return false;
			}

			//Fill a string dictionary with the manager parameters that have been found
			try {
				foreach (DataRow TestRow in Dt.Rows) {
					//Add the column heading and value to the dictionary
					ParamKey = DbCStr(TestRow[Dt.Columns["ParameterName"]]);
					ParamVal = DbCStr(TestRow[Dt.Columns["ParameterValue"]]);
					if (m_ParamDictionary.ContainsKey(ParamKey)) {
						m_ParamDictionary[ParamKey] = ParamVal;
					} else {
						m_ParamDictionary.Add(ParamKey, ParamVal);
					}
				}
				return true;
			} catch (System.Exception ex) {
				ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " + ex.Message;
				WriteErrorMsg(ErrMsg);
				return false;
			} finally {
				Dt.Dispose();
			}
		}	// End sub

		public string GetParam(string ItemKey) {
			return GetParam(ItemKey, string.Empty);
		}

		/// <summary>
		/// Gets a stored parameter
		/// </summary>
		/// <param name="name">Parameter name</param>
		/// <param name="valueIfMissing">Value to retrun if the parameter does not exist</param>
		/// <returns>Parameter value if found, otherwise empty string</returns>
		public string GetParam(string ItemKey, string valueIfMissing)
		{
			string ItemValue;
			if (m_ParamDictionary.TryGetValue(ItemKey, out ItemValue))
			{
				return ItemValue ?? string.Empty;
			}
			else
			{
				return valueIfMissing ?? string.Empty;
			}
		}

		public void SetParam(string ItemKey, string ItemValue) {
			if (m_ParamDictionary.ContainsKey(ItemKey)) {
				m_ParamDictionary[ItemKey] = ItemValue;
			} else {
				m_ParamDictionary.Add(ItemKey, ItemValue);
			}
		}

		/// <summary>
		/// Writes specfied value to an application config file.
		/// </summary>
		/// <param name="Key">Name for parameter (case sensitive)</param>
		/// <param name="Value">New value for parameter</param>
		/// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
		/// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
		public bool WriteConfigSetting(string Key, string Value) {

			ErrMsg = "";

			//Load the config document
			XmlDocument MyDoc = LoadConfigDocument();
			if (MyDoc == null) {
				//Error message has already been produced by LoadConfigDocument
				return false;
			}

			//Retrieve the settings node
			XmlNode MyNode = MyDoc.SelectSingleNode("//applicationSettings");

			if (MyNode == null) {
				ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
				return false;
			}

			try {
				//Select the eleement containing the value for the specified key containing the key
				XmlElement MyElement = (XmlElement)MyNode.SelectSingleNode(string.Format("//setting[@name='{0}']/value", Key));
				if (MyElement != null) {
					//Set key to specified value
					MyElement.InnerText = Value;
				} else {
					//Key was not found
					ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + Key;
					return false;
				}
				MyDoc.Save(GetConfigFilePath());
				return true;
			} catch (System.Exception ex) {
				ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
				return false;

			}
		} // End sub

		/// <summary>
		/// Loads an app config file for changing parameters
		/// </summary>
		/// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
		private XmlDocument LoadConfigDocument() {
			XmlDocument MyDoc = null;

			try {
				MyDoc = new XmlDocument();
				MyDoc.Load(GetConfigFilePath());
				return MyDoc;
			} catch (System.Exception ex) {
				ErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
				return null;
			}
		}	// End sub

		/// <summary>
		/// Specifies the full name and path for the application config file
		/// </summary>
		/// <returns>String containing full name and path</returns>
		private string GetConfigFilePath() {
			return Application.ExecutablePath + ".config";
		}	// End sub

		private string DbCStr(object InpObj) {
			if (InpObj == null) {
				return "";
			} else {
				return InpObj.ToString();
			}
		}

		private void WriteErrorMsg(string ErrMsg) {
			if (m_MCParamsLoaded) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg);
			} else {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.ERROR, ErrMsg);
			}
		}
		#endregion
	}	// End class
}	// End namespace
