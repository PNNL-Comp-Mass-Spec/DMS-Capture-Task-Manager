using System.Collections.Generic;
using CaptureTaskManager;

namespace CaptureToolPlugin.DataCapture
{
    /// <summary>
    /// Capture implementation for a single dataset file
    /// </summary>
    internal class FileSingleCapture : CaptureFileBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">Initialization data object</param>
        public FileSingleCapture(CaptureInitData data) : base(data)
        { }

        /// <inheritdoc />
        public override void Capture(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume,
            InstrumentClass instrumentClass,
            string instrumentName,
            ITaskParams taskParams
        )
        {
            CaptureFile(out msg, returnData, datasetInfo, sourceDirectoryPath, datasetDirectoryPath, copyWithResume);
        }

        /// <summary>
        /// Capture a single file
        /// </summary>
        /// <param name="msg">Output: error message</param>
        /// <param name="returnData">Input/output: Return data</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryPath">Source directory (on instrument)</param>
        /// <param name="datasetDirectoryPath">Destination directory</param>
        /// <param name="copyWithResume">True if using copy with resume</param>
        private void CaptureFile(
            out string msg,
            ToolReturnData returnData,
            DatasetInfo datasetInfo,
            string sourceDirectoryPath,
            string datasetDirectoryPath,
            bool copyWithResume)
        {
            var fileNames = new List<string>
            {
                datasetInfo.FileOrDirectoryName
            };

            CaptureOneOrMoreFiles(out msg, returnData, datasetInfo.DatasetName,
                fileNames, sourceDirectoryPath, datasetDirectoryPath, copyWithResume);
        }
    }
}
