using System;
using System.IO;

namespace Pacifica.Core
{
    public class ProgressReportingInfo
    {
        public enum ProgressTasksEnum
        {
            ZippingFiles,
            CreateZipFileHash,
            ManifestCreation,
            UploadZipFile,
        }

        #region Private Members

        private string _message;
        private long _uploadedBytes = 0;
        private int _taskProgressPercent = 0;
        private TimeSpan _elapsedTime;


        #endregion

        #region Constructor

        //TODO - remove percentDone, it is not being used...
        public ProgressReportingInfo(string bundleIdentifier, string message, int percentDone,
            long totalBytesSent, long totalBytesToSend, TimeSpan elapsedTime)
        {
            this.BundleId = bundleIdentifier;
            this.TaskType = ProgressTasksEnum.UploadZipFile;
            this.Message = message;
            this.TaskProgressPercent = 0;
            this._uploadedBytes = totalBytesSent;
            this.TotalBytesToSend = totalBytesToSend;

            //TODO - The StopWatch class is very handy for this sort of thing.
            if (elapsedTime == null)
            {
                this._elapsedTime = new TimeSpan(10);
            }
            else
            {
                this._elapsedTime = elapsedTime;
            }
        }

        // TODO : ARP : Remove this constructor (never called)
        //public ProgressReportingInfo(string bundleIdentifier, ZipProgressEventArgs zipProgressStatus)
        //{
        //    this.TaskType = ProgressTasksEnum.ZippingFiles;
        //    if (this.ZipProgressStatus.CurrentEntry != null)
        //    {
        //        this.CurrentFile = Path.GetFileName(ZipProgressStatus.CurrentEntry.FileName);
        //    }
        //    else
        //    {
        //        this.CurrentFile = string.Empty;
        //    }

        //    this.CurrentStepTask = this.CurrentFile + " (" + this.TaskProgressPercent.ToString() + "% Completed)";
        //    this.TotalBytesToSend = this.ZipProgressStatus.TotalBytesToTransfer;
        //    this._uploadedBytes = this.ZipProgressStatus.BytesTransferred;
        //}

        #endregion

        #region Properties

        //TODO - this is either wrong or needs replacement since converting to #ZipLib
        private ZipProgressData ZipProgressStatus { get; set; }

        private int TaskProgressCount { get; set; }

        public int CurrentTaskCount { get; private set; }

        public int TotalTaskCount { get; internal set; }

        public string BundleId { get; private set; }

        public long TotalBytesSent { get; private set; }

        public long TotalBytesToSend { get; private set; }

        public string CurrentFile { get; private set; }

        public string CurrentStepTask { get; private set; }

        public ProgressTasksEnum TaskType { get; private set; }

        public string Message
        {
            get
            {
                if (string.IsNullOrEmpty(_message))
                {
                    _message = "Compressing Files (Entry " +
                        TaskProgressCount.ToString() + " of "
                        + this.TotalTaskCount.ToString() + ")";
                }
                return _message;
            }
            internal set
            {
                _message = value;
            }
        }

        public int TaskProgressPercent
        {
            get
            {
                int outVal = 0;
                switch (this.TaskType)
                {
                    case ProgressTasksEnum.ZippingFiles:
                        if (this.ZipProgressStatus.TotalBytesToTransfer > 0 &&
                            this.ZipProgressStatus.BytesTransferred > 0)
                        {
                            outVal = (int)Math.Round(
                                this.ZipProgressStatus.BytesTransferred / this.ZipProgressStatus.TotalBytesToTransfer * 100.0, 0);
                        }
                        break;
                    case ProgressTasksEnum.UploadZipFile:
                        if (this._taskProgressPercent > 0)
                        {
                            outVal = this._taskProgressPercent;
                        }
                        else
                        {
                            if (this.TotalBytesToSend > 0 && this._uploadedBytes > 0)
                            {
                                outVal = this.TotalBytesToSend > 0 ?
                                    (int)Math.Round(this._uploadedBytes / this.TotalBytesToSend * 100.0, 0) : 0;
                            }
                        }
                        break;
                    default:
                        outVal = 100;
                        break;
                }
                return outVal;
            }
            set
            {
                this._taskProgressPercent = (int)Math.Round(value * 100.0, 0);
            }
        }


        public string AverageUploadSpeed
        {
            get
            {
                return CalculateAverageUploadSpeed();
            }
        }

        public int OverallTaskProgressPercent
        {
            get
            {
                int outVal = 0;
                switch (this.TaskType)
                {
                    case ProgressTasksEnum.ZippingFiles:
                        if (this._uploadedBytes > 0 && this.TotalBytesToSend > 0)
                        {
                            outVal = (int)Math.Round(this.TaskProgressCount / this.TotalTaskCount * 100.0 * 0.5, 0);
                        }
                        break;
                    case ProgressTasksEnum.CreateZipFileHash:
                        outVal = 60;
                        break;
                    case ProgressTasksEnum.ManifestCreation:
                        outVal = 70;
                        break;
                    case ProgressTasksEnum.UploadZipFile:
                        if (this._uploadedBytes > 0 && this.TotalBytesToSend > 0)
                        {
                            outVal = (int)Math.Round(this._uploadedBytes / this.TotalBytesToSend * 0.3, 0) + 70;
                        }

                        break;
                    default:
                        outVal = 100;
                        break;
                }
                return outVal;
            }
        }

        #endregion

        #region Private Methods

        private string CalculateAverageUploadSpeed()
        {
            long speed = (long)Math.Round(TotalBytesSent / _elapsedTime.TotalSeconds);
            return Utilities.ByteFormat(speed) + "/s";
        }

        #endregion

    }
}
