using System.Collections.Generic;
using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Capture implementation for multi-file datasets (with no packaging directory)
    /// </summary>
    internal class FileMultipleCapture : CaptureFileBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public FileMultipleCapture(CaptureInitData data) : base(data)
        { }

        /// <summary>
        /// Capture multiple files, each with the same name but a different extension
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument)</param>
        /// <param name="datasetDirectoryPath">Destination directory</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        public void CaptureMultiFile(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume)
        {
            // Dataset found, and it has multiple files
            // Each has the same name but a different extension

            var fileNames = new List<string>();

            foreach (var remoteFile in datasetInfo.FileList)
            {
                fileNames.Add(remoteFile.Name);
            }

            CaptureOneOrMoreFiles(out msg, returnData, datasetInfo.DatasetName,
                fileNames, sourceDirectoryPath, datasetDirectoryPath, copyWithResume);
        }
    }
}
