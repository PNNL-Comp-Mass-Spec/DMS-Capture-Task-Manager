//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/29/2009
//*********************************************************************************************************

using System;
using CaptureTaskManager;
using System.Xml;
using System.IO;
using PRISM.Logging;

namespace DatasetQualityPlugin
{
    /// <summary>
    /// Creates a metadata file in the dataset folder
    /// </summary>
    public class clsMetaDataFile
    {

        #region "Constants"
        private const string META_FILE_NAME = "metadata.xml";
        #endregion

        #region "Methods"
        /// <summary>
        /// Creates an XML metadata file for a dataset
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="TaskParams">Task parameters</param>
        public static bool CreateMetadataFile(IMgrParams mgrParams, ITaskParams TaskParams)
        {
            string xmlText;

            // Create a memory stream to write the metadata document to
            var memStream = new MemoryStream();
            using (var xWriter = new XmlTextWriter(memStream, System.Text.Encoding.UTF8))
            {
                xWriter.Formatting = Formatting.Indented;
                xWriter.Indentation = 2;

                // Create the document
                xWriter.WriteStartDocument(true);
                // Root level element
                xWriter.WriteStartElement("Root");

                // Loop through the task parameters, selecting only the ones beginning with "Meta_"
                foreach (var testKey in TaskParams.TaskDictionary.Keys)
                {
                    if (testKey.StartsWith("Meta_"))
                    {
                        // This parameter is metadata, so write it out
                        var tmpStr = testKey.Replace("Meta_", "");
                        xWriter.WriteElementString(tmpStr, TaskParams.GetParam(testKey));
                    }
                }

                xWriter.WriteEndElement();  // Close root element

                // Close the document, but don't close the writer
                xWriter.WriteEndDocument();
                xWriter.Flush();

                // Use a streamreader to copy the XML text to a string variable
                memStream.Seek(0, SeekOrigin.Begin);
                var memStreamReader = new StreamReader(memStream);
                xmlText = memStreamReader.ReadToEnd();

                memStreamReader.Close();
                memStream.Close();

                // Since the document is now a string, we can get rid of the XMLWriter
                xWriter.Close();
            }

            // Write the string to the output file
            var svrPath = Path.Combine(TaskParams.GetParam("Storage_Vol_External"), TaskParams.GetParam("Storage_Path"));
            var dsPath = Path.Combine(svrPath, TaskParams.GetParam("Folder"));
            var metaFileNamePath = Path.Combine(dsPath, META_FILE_NAME);
            try
            {
                File.WriteAllText(metaFileNamePath, xmlText);
                var msg = "Metadata file created for dataset " + TaskParams.GetParam("Dataset");
                LogTools.LogDebug(msg);
                return true;
            }
            catch (Exception ex)
            {
                var msg = "Exception creating metadata file for dataset " + TaskParams.GetParam("Dataset");
                LogTools.LogError(msg, ex);
                return false;
            }
        }
        #endregion
    }
}
