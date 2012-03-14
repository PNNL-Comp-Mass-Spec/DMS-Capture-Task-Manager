
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/22/2009
//
// Last modified 09/22/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;

namespace CaptureTaskManager
{
	public class clsPluginLoader
	{
		//*********************************************************************************************************
		// Handles creation of plugin objects
		//**********************************************************************************************************

		#region "Class variables"
			private static string m_pluginConfigFile = "plugin_info.xml";
		#endregion

		#region "Properties"
			public static string FileName
			{
				get
				{
					return m_pluginConfigFile;
				}
				set
				{
					m_pluginConfigFile = value;
				}
			}

			public static string ErrMsg { get; set; }
		#endregion

		#region "Methods"

            /// <summary>
            /// Set the following to True if debugging
            /// </summary>
            /// <remarks>Also uncomment the appropriate case statements in the following two functions</remarks>

            private const bool PLUGIN_DEBUG_MODE_ENABLED = true;

            private static IToolRunner DebugModeGetToolRunner(string className)
            {

                IToolRunner myToolRunner = null;

                switch (className)
                {
					//case "ImsDemuxPlugin.clsPluginMain":
					//    myToolRunner = (IToolRunner)new ImsDemuxPlugin.clsPluginMain();
					//    break;
                
                    default:
                        break;
                }

                return myToolRunner;
            }

			/// <summary>
			/// Loads a tool runner object
			/// </summary>
			/// <param name="toolName">Name of tool</param>
			/// <returns>An object meeting the IToolRunner interface</returns>
			public static IToolRunner GetToolRunner(string toolName)
			{
				string msg;
				string xPath = "//ToolRunners/ToolRunner[@Tool='" + toolName.ToLower() + "']";
				string className = "";
				string assyName = "";
				IToolRunner myToolRunner = null;

				if (GetPluginInfo(xPath, ref className, ref assyName))
				{
                    if (PLUGIN_DEBUG_MODE_ENABLED)
                    {
                        myToolRunner = DebugModeGetToolRunner(className);
                        if ((myToolRunner != null))
                        {
                            return myToolRunner;
                        }
                    }

					object obj = LoadObject(className, assyName);
					if (obj != null)
					{
						try
						{
							myToolRunner = (IToolRunner)obj;
							msg = "Loaded tool runner: " + className + " from " + assyName;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
						}
						catch (Exception ex)
						{
							ErrMsg = ex.Message;
						}
					}
					else
					{
						msg = "Unable to load tool runner: " + className + " from " + assyName;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					}
				}
				return myToolRunner;
			}	// End sub

			/// <summary>
			/// Retrieves data for specified plugin from plugin info config file
			/// </summary>
			/// <param name="xPath">XPath spec for specified plugin</param>
			/// <param name="className">Name of class for plugin (return value)</param>
			/// <param name="assyName">Name of assembly for plugin (return value)</param>
			/// <returns>TRUE for success, FALSE for failure</returns>
			private static bool GetPluginInfo(string xPath, ref string className, ref string assyName)
			{
				XmlDocument doc = new XmlDocument();
				XmlNodeList nodeList;
				string strPluginInfo = string.Empty;

				try
				{
					if (xPath == null) xPath = string.Empty;
					if (className == null) className = string.Empty;
					if (assyName == null) assyName = string.Empty;

					strPluginInfo = "XPath=\"" + xPath + "\"; className=\"" + className + "\"; assyName=" + assyName + "\"";

					// Read the tool runner info file
					doc.Load(GetPluginInfoFilePath(m_pluginConfigFile));
					XmlElement root = doc.DocumentElement;

					// Find the element that matches the tool name
					nodeList = root.SelectNodes(xPath);

					// Make sure exactly 1 element found and retrieve its information
					if (nodeList.Count == 1)
					{
						foreach (XmlElement el in nodeList)
						{
							className = el.GetAttribute("Class");
							assyName = el.GetAttribute("AssemblyFile");
						}
					}
					else
					{
						throw new Exception("Could not resolve tool name; " + strPluginInfo);
					}
					return true;
				}
				catch (Exception ex)
				{
					ErrMsg = "Error in GetPluginInfo:" + ex.Message + "; " + strPluginInfo;
					return false;
				}
			}	// End sub

			/// <summary>
			/// Gets the path to the plugin info config file
			/// </summary>
			/// <param name="PluginInfoFileName">Name of plugin info file</param>
			/// <returns>Path to plugin info file</returns>
			private static string GetPluginInfoFilePath(string PluginInfoFileName)
			{
				FileInfo fi = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
				return Path.Combine(fi.DirectoryName, PluginInfoFileName);
			}	// End sub

			/// <summary>
			/// Loads the specifed dll
			/// </summary>
			/// <param name="className">Name of class to load (from GetPluginInfo)</param>
			/// <param name="assyName">Name of assembly to load (from GetPluginInfo)</param>
			/// <returns>An object referencing the specified dll</returns>
			private static object LoadObject(string className, string assyName)
			{
				object obj = null;
				try
				{
					// Build instance of tool runner class from class and assembly names
					System.Reflection.Assembly assem;
					assem = System.Reflection.Assembly.LoadFrom(GetPluginInfoFilePath(assyName));
					Type dllType = assem.GetType(className, false, true);
					obj = Activator.CreateInstance(dllType);
				}
				catch (Exception ex)
				{
					ErrMsg = "clsPluginLoader.LoadObject(), exception: " + ex.Message;
				}
				return obj;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
