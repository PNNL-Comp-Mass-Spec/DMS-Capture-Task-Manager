//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/29/2009
//*********************************************************************************************************

using System;
using System.IO;
using System.Xml;
using CaptureTaskManager;
using PRISM.Logging;

namespace DatasetQualityPlugin
{
    /// <summary>
    /// Creates a metadata file in the dataset folder
    /// </summary>
    public static class MetaDataFile
    {
        private const string META_FILE_NAME = "metadata.xml";
        /// <summary>
        /// Creates an XML metadata file for a dataset
        /// </summary>
        /// <param name="taskParams">Task parameters</param>
        public static bool CreateMetadataFile(ITaskParams taskParams)
        {
            string xmlText;

            // Create a memory stream to write the metadata document to
            var memStream = new MemoryStream();
            using (var writer = new XmlTextWriter(memStream, System.Text.Encoding.UTF8))
            {
                writer.Formatting = Formatting.Indented;
                writer.Indentation = 2;

                // Create the document
                writer.WriteStartDocument(true);
                // Root level element
                writer.WriteStartElement("Root");

                // Loop through the task parameters, selecting only the ones beginning with "Meta_"
                // These parameters are included in the table returned by procedure cap.request_ctm_step_task
                // That procedure queries procedure (or function) get_task_step_params to get the parameters
                // Additionally, if the step tool is 'DatasetInfo' or 'DatasetQuality', get_task_step_params uses get_metadata_for_dataset,
                // which adds several items, including Meta_Dataset_Name and Meta_Dataset_ID
                foreach (var taskParam in taskParams.TaskDictionary.Keys)
                {
                    if (taskParam.StartsWith("Meta_"))
                    {
                        // This parameter is metadata, so write it out
                        var tmpStr = taskParam.Replace("Meta_", string.Empty);
                        writer.WriteElementString(tmpStr, taskParams.GetParam(taskParam));
                    }
                }

                writer.WriteEndElement();  // Close root element

                // Close the document, but don't close the writer
                writer.WriteEndDocument();
                writer.Flush();

                // Use a StreamReader to copy the XML text to a string variable
                memStream.Seek(0, SeekOrigin.Begin);
                var memStreamReader = new StreamReader(memStream);
                xmlText = memStreamReader.ReadToEnd();

                memStreamReader.Close();
                memStream.Close();

                // Since the document is now a string, we can get rid of the XMLWriter
                writer.Close();
            }

            // Write the string to the output file
            var remoteSharePath = Path.Combine(taskParams.GetParam("Storage_Vol_External"), taskParams.GetParam("Storage_Path"));
            var datasetDirectory = taskParams.GetParam("Directory");

            var datasetDirectoryPath = Path.Combine(remoteSharePath, datasetDirectory);
            var metadataFile = new FileInfo(Path.Combine(datasetDirectoryPath, META_FILE_NAME));

            try
            {
                if (metadataFile.Exists)
                {
                    LogTools.LogMessage("Replacing metadata file at " + metadataFile.FullName);
                    metadataFile.Delete();
                }

                File.WriteAllText(metadataFile.FullName, xmlText);

                LogTools.LogDebug("Metadata file created for dataset " + taskParams.GetParam("Dataset"));
                return true;
            }
            catch (Exception ex)
            {
                LogTools.LogError("Exception creating metadata file at " + metadataFile.FullName, ex);
                return false;
            }
        }
    }
}
