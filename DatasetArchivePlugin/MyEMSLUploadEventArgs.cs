﻿using Pacifica.DataUpload;
using System;

namespace DatasetArchivePlugin
{
    public class MyEMSLUploadEventArgs : EventArgs
    {
        // Ignore Spelling: EUS

        public readonly int FileCountNew;
        public readonly int FileCountUpdated;
        public readonly long BytesUploaded;
        public readonly double UploadTimeSeconds;
        public readonly string StatusURI;
        public readonly Upload.EUSInfo EUSInfo;
        public readonly int ErrorCode;
        public readonly bool UsedTestInstance;

        public MyEMSLUploadEventArgs(
            int fileCountNew,
            int fileCountUpdated,
            long bytes,
            double uploadTimeSeconds,
            string statusURI,
            Upload.EUSInfo eusInfo,
            int errorCode,
            bool usedTestInstance)
        {
            FileCountNew = fileCountNew;
            FileCountUpdated = fileCountUpdated;
            BytesUploaded = bytes;
            UploadTimeSeconds = uploadTimeSeconds;
            StatusURI = statusURI;
            EUSInfo = eusInfo;
            ErrorCode = errorCode;
            UsedTestInstance = usedTestInstance;
        }
    }
}
