//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/22/2009
//*********************************************************************************************************

using System;
using System.Reflection;
using System.Xml;
using System.IO;

namespace CaptureTaskManager
{
    /// <summary>
    /// Handles creation of plugin objects
    /// </summary>
    public class clsPluginLoader : clsLoggerBase
    {

        #region "Properties"

        public static string FileName { get; set; } = "plugin_info.xml";

        public static string ErrMsg { get; set; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Set the following to True if debugging
        /// </summary>
        /// <remarks>Also uncomment the appropriate case statements in the following two functions</remarks>
#if PLUGIN_DEBUG_MODE_ENABLED
        private static IToolRunner DebugModeGetToolRunner(string className)
        {
            IToolRunner myToolRunner = null;

            switch (className)
            {
                //case "ImsDemuxPlugin.clsPluginMain":
                //    myToolRunner = (IToolRunner)new ImsDemuxPlugin.clsPluginMain();
                //    break;

                //case "DatasetArchivePlugin.clsPluginMain":
                //    myToolRunner = (IToolRunner)new DatasetArchivePlugin.clsPluginMain();
                //    break;

                //case "ArchiveStatusCheckPlugin.clsPluginMain":
                //    myToolRunner = (IToolRunner)new ArchiveStatusCheckPlugin.clsPluginMain();
                //    break;

                //case "ArchiveVerifyPlugin.clsPluginMain":
                //    myToolRunner = (IToolRunner)new ArchiveVerifyPlugin.clsPluginMain();
                //    break;

                //case "DatasetIntegrityPlugin.clsPluginMain":
                //    myToolRunner = (IToolRunner)new DatasetIntegrityPlugin.clsPluginMain();
                //    break;

                //case "DatasetInfoPlugin.clsPluginMain":
                //    myToolRunner = (IToolRunner)new DatasetInfoPlugin.clsPluginMain();
                //    break;

                default:
                    break;
            }

            // ReSharper disable once ExpressionIsAlwaysNull
            return myToolRunner;
        }
#endif

        /// <summary>
        /// Loads a tool runner object
        /// </summary>
        /// <param name="toolName">Name of tool</param>
        /// <returns>An object meeting the IToolRunner interface</returns>
        public static IToolRunner GetToolRunner(string toolName)
        {
            var xPath = "//ToolRunners/ToolRunner[@Tool='" + toolName.ToLower() + "']";
            var className = "";
            var assemblyName = "";
            IToolRunner myToolRunner = null;

            if (GetPluginInfo(xPath, ref className, ref assemblyName))
            {
#if PLUGIN_DEBUG_MODE_ENABLED
                myToolRunner = DebugModeGetToolRunner(className);
                if (myToolRunner != null)
                {
                    return myToolRunner;
                }
#endif

                var newInstance = LoadObject(className, assemblyName);

                if (newInstance != null)
                {
                    try
                    {
                        myToolRunner = (IToolRunner)newInstance;
                        LogDebug("Loaded tool runner: " + className + " from " + assemblyName);
                    }
                    catch (Exception ex)
                    {
                        ErrMsg = ex.Message;
                    }
                }
                else
                {
                    LogError("Unable to load tool runner: " + className + " from " + assemblyName);
                }
            }
            return myToolRunner;
        }

        /// <summary>
        /// Retrieves data for specified plugin from plugin info config file
        /// </summary>
        /// <param name="xPath">XPath spec for specified plugin</param>
        /// <param name="className">Name of class for plugin (return value)</param>
        /// <param name="assemblyName">Name of assembly for plugin (return value)</param>
        /// <returns>TRUE for success, FALSE for failure</returns>
        private static bool GetPluginInfo(string xPath, ref string className, ref string assemblyName)
        {
            var doc = new XmlDocument();
            var strPluginInfo = string.Empty;

            try
            {
                if (xPath == null) xPath = string.Empty;
                if (className == null) className = string.Empty;
                if (assemblyName == null) assemblyName = string.Empty;

                strPluginInfo = "XPath=\"" + xPath + "\"; className=\"" + className + "\"; assemblyName=" + assemblyName + "\"";

                // Read the tool runner info file
                doc.Load(GetPluginInfoFilePath(FileName));
                var root = doc.DocumentElement;

                if (root == null)
                {
                    ErrMsg = "Error in GetPluginInfo: root element not found in " + FileName;
                    return false;
                }

                // Find the element that matches the tool name
                var nodeList = root.SelectNodes(xPath);

                // Make sure exactly 1 element found and retrieve its information
                if (nodeList != null && nodeList.Count == 1)
                {
                    foreach (XmlElement el in nodeList)
                    {
                        className = el.GetAttribute("Class");
                        assemblyName = el.GetAttribute("AssemblyFile");
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
                ErrMsg = "Error in GetPluginInfo: " + ex.Message + "; " + strPluginInfo;
                return false;
            }
        }

        /// <summary>
        /// Gets the path to the plugin info config file
        /// </summary>
        /// <param name="PluginInfoFileName">Name of plugin info file</param>
        /// <returns>Path to plugin info file</returns>
        private static string GetPluginInfoFilePath(string PluginInfoFileName)
        {
            var fi = new FileInfo(PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath());
            if (fi.DirectoryName == null)
                throw new DirectoryNotFoundException("Could not determine parent folder path for the exe");

            return Path.Combine(fi.DirectoryName, PluginInfoFileName);
        }

        /// <summary>
        /// Loads the specified dll
        /// </summary>
        /// <param name="className">Name of class to load (from GetPluginInfo)</param>
        /// <param name="assemblyName">Name of assembly to load (from GetPluginInfo)</param>
        /// <returns>An object referencing the specified dll</returns>
        private static object LoadObject(string className, string assemblyName)
        {
            object obj = null;
            try
            {
                // Build instance of tool runner class from class and assembly names
                var assembly = Assembly.LoadFrom(GetPluginInfoFilePath(assemblyName));
                var dllType = assembly.GetType(className, false, true);
                obj = Activator.CreateInstance(dllType);
            }
            catch (Exception ex)
            {
                ErrMsg = "clsPluginLoader.LoadObject(), exception: " + ex.Message;
            }
            return obj;
        }

        #endregion
    }
}