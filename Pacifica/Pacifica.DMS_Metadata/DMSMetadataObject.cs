using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Pacifica.Core;
using MyEMSLReader;
using PRISM.Files;
using MessageEventArgs = MyEMSLReader.MessageEventArgs;
using ProgressEventArgs = Pacifica.Core.ProgressEventArgs;

namespace Pacifica.DMS_Metadata
{
    public class DMSMetadataObject
    {

        /// <summary>
        /// Maximum number of files to archive
        /// </summary>
        /// <remarks>
        /// If uploading an entire dataset folder and all of its subfolders via a DatasetArchive operation, 
        ///   then this value applies to all files in the dataset folder (and subfolders)
        /// If uploading just one dataset subfolder via an ArchiveUpdate operation, 
        ///   then this value applies to all files in that subfolder
        /// </remarks>
        public const int MAX_FILES_TO_ARCHIVE = 500;

        /// <summary>
        /// Object that tracks the upload details, including the files to upload
        /// </summary>
        /// <remarks>
        /// The information in this dictionary is translated to JSON; 
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects 
        /// </remarks>
        private Dictionary<string, object> mMetadataObject;

        // List of remote files that were found using CacheInfo files
        private readonly List<string> mRemoteCacheInfoFilesToRetrieve;

        // Keys in this dictionary are lock folder share paths (for example \\proto-6\DMS_LockFiles)
        // Values are the corresponding lock file info object
        private readonly Dictionary<string, FileInfo> mRemoteCacheInfoLockFiles;

        private readonly clsFileTools mFileTools;

        public enum ArchiveModes
        {
            archive, update
        }

        #region "Properties"

        public string DatasetName
        {
            get;
            private set;
        }

        public string ManagerName
        {
            get;
            private set;
        }

        public Dictionary<string, object> MetadataObject
        {
            get { return mMetadataObject; }
        }

        public long TotalFileSizeToUpload
        {
            get;
            set;
        }

        public int TotalFileCountNew
        {
            get;
            set;
        }

        public int TotalFileCountUpdated
        {
            get;
            set;
        }

        public bool UseTestInstance
        {
            get;
            set;
        }

        public string MetadataObjectJSON
        {
            get
            {
                return Utilities.ObjectToJson(mMetadataObject);
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DMSMetadataObject()
            : this("")
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public DMSMetadataObject(string managerName)
        {
            mRemoteCacheInfoFilesToRetrieve = new List<string>();
            mRemoteCacheInfoLockFiles = new Dictionary<string, FileInfo>(StringComparer.CurrentCultureIgnoreCase);

            if (string.IsNullOrWhiteSpace(managerName))
                managerName = "DMSMetadataObject";

            this.ManagerName = managerName;
            mFileTools = new clsFileTools(managerName, 1);

            mFileTools.WaitingForLockQueue += mFileTools_WaitingForLockQueue;
        }

        public void SetupMetadata(Dictionary<string, string> taskParams, Dictionary<string, string> mgrParams, EasyHttp.eDebugMode debugMode)
        {
            Upload.udtUploadMetadata uploadMetadata;

            uploadMetadata.EUSInstrumentID = Utilities.GetDictionaryValue(taskParams, "EUS_Instrument_ID", "");
            uploadMetadata.EUSProposalID = Utilities.GetDictionaryValue(taskParams, "EUS_Proposal_ID", "");

            this.DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "UnknownDataset");

            var lstDatasetFilesToArchive = FindDatasetFilesToArchive(taskParams, mgrParams, out uploadMetadata);

            // Calculate the "year_quarter" code used for subfolders within an instrument folder
            // This value is based on the date the dataset was created in DMS
            uploadMetadata.DateCodeString = GetDatasetYearQuarter(taskParams);

            // Find the files that are new or need to be updated
            var lstUnmatchedFiles = CompareDatasetContentsElasticSearch(lstDatasetFilesToArchive, uploadMetadata, debugMode);

            mMetadataObject = Upload.CreateMetadataObject(uploadMetadata, lstUnmatchedFiles);

        }

        private bool AddUsingCacheInfoFile(
            FileInfo fiCacheInfoFile,
            List<FileInfoObject> fileCollection,
            string baseDSPath,
            out string remoteFilePath)
        {

            remoteFilePath = string.Empty;

            using (var srCacheInfoFile = new StreamReader(new FileStream(fiCacheInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                if (!srCacheInfoFile.EndOfStream)
                {
                    remoteFilePath = srCacheInfoFile.ReadLine();
                }
            }

            if (string.IsNullOrWhiteSpace(remoteFilePath))
            {
                OnError("AddUsingCacheInfoFile", "Warning: Cache info file did not contain a file path; see " + fiCacheInfoFile.FullName);
                return false;
            }

            var fiRemoteFile = new FileInfo(remoteFilePath);
            if (!fiRemoteFile.Exists)
            {
                // This is not a fatal error; the file may have been purged
                Console.WriteLine("Note: Remote file referred to by the cache info file was not found: " + fiRemoteFile.FullName);
                return false;
            }

            if (fiCacheInfoFile.Directory == null)
            {
                OnError("AddUsingCacheInfoFile", "Unable to determine the parent directory of the cache info file (this should never happen)");
                return false;
            }

            var relativeDestinationDirectory = FileInfoObject.GenerateRelativePath(fiCacheInfoFile.Directory.FullName, baseDSPath);

            // This constructor will auto-compute the Sha-1 hash value for the file
            var fio = new FileInfoObject(fiRemoteFile.FullName, relativeDestinationDirectory, sha1Hash: "");
            fileCollection.Add(fio);

            return true;

        }

        /// <summary>
        /// Find all of the files in the path to be archived
        /// </summary>
        /// <param name="pathToBeArchived">Folder path to be archived</param>
        /// <param name="baseDSPath">Base dataset folder path</param>
        /// <param name="recurse">True to recurse</param>
        /// <returns></returns>
        private List<FileInfoObject> CollectFileInformation(
            string pathToBeArchived,
            string baseDSPath,
            bool recurse
        )
        {
            var fileCollection = new List<FileInfoObject>();

            var archiveDir = new DirectoryInfo(pathToBeArchived);
            if (!archiveDir.Exists)
            {
                throw new DirectoryNotFoundException("Source directory not found: " + archiveDir);
            }

            SearchOption eSearchOption;
            if (recurse)
                eSearchOption = SearchOption.AllDirectories;
            else
                eSearchOption = SearchOption.TopDirectoryOnly;

            var fileList = archiveDir.GetFiles("*", eSearchOption).ToList();

            if (fileList.Count >= MAX_FILES_TO_ARCHIVE)
            {
                throw new ArgumentOutOfRangeException("Source directory has over " + MAX_FILES_TO_ARCHIVE + " files; files must be zipped before upload to MyEMSL");
            }

            var fracCompleted = 0.0;

            // Generate file size sum for status purposes
            long totalFileSize = 0;				// how much data is there to crunch?
            long runningFileSize = 0;			// how much data we've crunched so far
            foreach (var fi in fileList)
            {
                totalFileSize += fi.Length;
            }

            mRemoteCacheInfoFilesToRetrieve.Clear();
            mRemoteCacheInfoLockFiles.Clear();

            foreach (var fiFile in fileList)
            {
                runningFileSize += fiFile.Length;

                if (totalFileSize > 0)
                    fracCompleted = (runningFileSize / (double)totalFileSize);

                ReportProgress(fracCompleted * 100.0, "Hashing files: " + fiFile.Name);

                // This constructor will auto-compute the Sha-1 hash value for the file
                var fio = new FileInfoObject(fiFile.FullName, baseDSPath);
                fileCollection.Add(fio);

                if (fio.FileName.EndsWith("_CacheInfo.txt"))
                {
                    // This is a cache info file that likely points to a .mzXML or .mzML file (possibly gzipped)
                    // Auto-include that file in the .tar to be uploaded

                    string remoteFilePath;

                    var success = AddUsingCacheInfoFile(fiFile, fileCollection, baseDSPath, out remoteFilePath);
                    if (!success)
                        throw new Exception("Error reported by AddUsingCacheInfoFile for " + fiFile.FullName);

                    mRemoteCacheInfoFilesToRetrieve.Add(remoteFilePath);

                }
            }

            ReportProgress(100);

            return fileCollection;
        }

        /// <summary>
        /// Query server for files and hash codes
        /// </summary>
        /// <param name="fileList">List of local files</param>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="debugMode">Debugging options</param>
        /// <returns></returns>
        private List<FileInfoObject> CompareDatasetContentsElasticSearch(
            List<FileInfoObject> fileList,
            Upload.udtUploadMetadata uploadMetadata,
            EasyHttp.eDebugMode debugMode)
        {

            TotalFileSizeToUpload = 0;

            // Find all files in MyEMSL for this dataset
            var reader = new Reader
            {
                IncludeAllRevisions = false
            };

            // Attach events
            reader.ErrorEvent += reader_ErrorEvent;
            reader.MessageEvent += reader_MessageEvent;
            reader.ProgressEvent += reader_ProgressEvent;

            if (UseTestInstance)
                reader.UseTestInstance = true;

            List<ArchivedFileInfo> lstFilesInMyEMSL;

            if (debugMode == EasyHttp.eDebugMode.MyEMSLOfflineMode)
                lstFilesInMyEMSL = new List<ArchivedFileInfo>();
            else
                lstFilesInMyEMSL = reader.FindFilesByDatasetID(uploadMetadata.DatasetID, uploadMetadata.SubFolder);

            if (lstFilesInMyEMSL.Count == 0)
            {
                // This dataset doesn't have any files in MyEMSL; upload everything in fileList
                foreach (var localFile in fileList)
                {
                    TotalFileSizeToUpload += localFile.FileSizeInBytes;
                }
                TotalFileCountNew = fileList.Count;
                TotalFileCountUpdated = 0;

                return fileList;
            }

            // Keys in this dictionary are relative file paths
            // Values are the sha-1 hash values for the file
            var dctFilesInMyEMSLSha1Hash = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            foreach (var archiveFile in lstFilesInMyEMSL)
            {
                if (dctFilesInMyEMSLSha1Hash.ContainsKey(archiveFile.RelativePathUnix))
                    Console.WriteLine("Warning: dctFilesInMyEMSLSha1Hash already contains " + archiveFile.RelativePathUnix);
                else
                    dctFilesInMyEMSLSha1Hash.Add(archiveFile.RelativePathUnix, archiveFile.Sha1Hash);
            }

            var lstUnmatchedFiles = new List<FileInfoObject>();

            foreach (var localFile in fileList)
            {
                string itemAddress;
                if (localFile.RelativeDestinationDirectory != string.Empty)
                {
                    itemAddress = localFile.RelativeDestinationDirectory + "/" + localFile.FileName;
                }
                else
                {
                    itemAddress = localFile.FileName;
                }

                var fileHashMyEMSL = Utilities.GetDictionaryValue(dctFilesInMyEMSLSha1Hash, itemAddress, string.Empty);

                if (localFile.Sha1HashHex != fileHashMyEMSL)
                {
                    lstUnmatchedFiles.Add(localFile);
                    TotalFileSizeToUpload += localFile.FileSizeInBytes;
                    if (string.IsNullOrEmpty(fileHashMyEMSL))
                    {
                        TotalFileCountNew++;
                    }
                    else
                    {
                        TotalFileCountUpdated++;
                    }
                }
            }

            return lstUnmatchedFiles;
        }


        public void CreateLockFiles()
        {
            const int MAX_LOCKFILE_WAIT_TIME_MINUTES = 20;

            if (mRemoteCacheInfoFilesToRetrieve.Count == 0)
                return;

            mRemoteCacheInfoLockFiles.Clear();

            foreach (var remoteFilePath in mRemoteCacheInfoFilesToRetrieve)
            {
                // Construct a list of first file required from each distinct server
                var fiSource = new FileInfo(remoteFilePath);
                var strLockFolderPathSource = mFileTools.GetLockFolder(fiSource);

                if (string.IsNullOrWhiteSpace(strLockFolderPathSource))
                    continue;

                if (mRemoteCacheInfoLockFiles.ContainsKey(strLockFolderPathSource))
                    continue;

                var sourceFileSizeMB = Convert.ToInt32(fiSource.Length / 1024.0 / 1024.0);
                if (sourceFileSizeMB < clsFileTools.LOCKFILE_MININUM_SOURCE_FILE_SIZE_MB)
                {
                    // Do not use a lock file for this remote file
                    continue;
                }

                var lockFileTimestamp = mFileTools.GetLockFileTimeStamp();

                var diLockFolderSource = new DirectoryInfo(strLockFolderPathSource);

                var strTargetFilePath = Path.Combine(@"\\MyEMSL\", this.DatasetName, fiSource.Name);

                var strLockFilePathSource = mFileTools.CreateLockFile(diLockFolderSource, lockFileTimestamp, fiSource, strTargetFilePath, this.ManagerName);

                if (string.IsNullOrEmpty(strLockFilePathSource))
                {
                    // Do not use a lock file for this remote file
                    continue;
                }

                mRemoteCacheInfoLockFiles.Add(strLockFolderPathSource, new FileInfo(strLockFilePathSource));

                mFileTools.WaitForLockFileQueue(lockFileTimestamp, diLockFolderSource, fiSource, MAX_LOCKFILE_WAIT_TIME_MINUTES);

            }
        }

        public void DeleteLockFiles()
        {

            if (mRemoteCacheInfoLockFiles.Count == 0)
                return;

            foreach (var remoteLockFile in mRemoteCacheInfoLockFiles)
            {
                try
                {
                    if (remoteLockFile.Value.Exists)
                        remoteLockFile.Value.Delete();
                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch (Exception)
                {
                    // Ignore errors here
                }

            }

        }

        public List<FileInfoObject> FindDatasetFilesToArchive(
            Dictionary<string, string> taskParams,
            Dictionary<string, string> mgrParams,
            out Upload.udtUploadMetadata uploadMetadata)
        {

            uploadMetadata = new Upload.udtUploadMetadata();
            uploadMetadata.Clear();

            // Translate values from task/mgr params into usable variables
            var perspective = Utilities.GetDictionaryValue(mgrParams, "perspective", "client");
            string driveLocation;

            // Determine the drive location based on perspective 
            // (client perspective means running on a Proto storage server; server perspective means running on another computer)
            if (perspective == "client")
                driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol_External", "");
            else
                driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol", "");

            // Construct the dataset folder path
            var pathToArchive = Utilities.GetDictionaryValue(taskParams, "Folder", "");
            pathToArchive = Path.Combine(Utilities.GetDictionaryValue(taskParams, "Storage_Path", ""), pathToArchive);
            pathToArchive = Path.Combine(driveLocation, pathToArchive);

            uploadMetadata.DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "");
            uploadMetadata.DMSInstrumentName = Utilities.GetDictionaryValue(taskParams, "Instrument_Name", "");
            uploadMetadata.DatasetID = Utilities.ToIntSafe(Utilities.GetDictionaryValue(taskParams, "Dataset_ID", ""), 0);
            var baseDSPath = pathToArchive;
            uploadMetadata.SubFolder = string.Empty;

            ArchiveModes archiveMode;
            if (Utilities.GetDictionaryValue(taskParams, "StepTool", "").ToLower() == "datasetarchive")
                archiveMode = ArchiveModes.archive;
            else
                archiveMode = ArchiveModes.update;

            if (archiveMode == ArchiveModes.update)
            {
                uploadMetadata.SubFolder = Utilities.GetDictionaryValue(taskParams, "OutputFolderName", "");

                if (!string.IsNullOrWhiteSpace(uploadMetadata.SubFolder))
                    pathToArchive = Path.Combine(pathToArchive, uploadMetadata.SubFolder);
                else
                    uploadMetadata.SubFolder = string.Empty;
            }

            var recurse = true;
            string sValue;

            if (taskParams.TryGetValue(MyEMSLUploader.RECURSIVE_UPLOAD, out sValue))
            {
                bool.TryParse(sValue, out recurse);
            }

            // Grab file information from this dataset directory
            // This process will also compute the Sha-1 hash value for each file
            var lstDatasetFilesToArchive = CollectFileInformation(pathToArchive, baseDSPath, recurse);
            return lstDatasetFilesToArchive;
        }

        public static string GetDatasetYearQuarter(Dictionary<string, string> taskParams)
        {
            var datasetDate = Utilities.GetDictionaryValue(taskParams, "Created", "");
            var date_code = DateTime.Parse(datasetDate);
            var yq = date_code.Month / 12.0 * 4.0;
            var yearQuarter = (int)Math.Ceiling(yq);
            var datasetDateCodeString = date_code.Year + "_" + yearQuarter;

            return datasetDateCodeString;
        }

        protected void ReportProgress(double percentComplete)
        {
            ReportProgress(percentComplete, string.Empty);
        }

        protected void ReportProgress(double percentComplete, string currentTask)
        {
            OnProgressUpdate(new ProgressEventArgs(percentComplete, currentTask));
        }


        #region "Event Delegates and Classes"

        public event ProgressEventHandler ProgressEvent;
        public event Pacifica.Core.MessageEventHandler ErrorEvent;

        public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);

        #endregion

        #region "Event Functions"

        public void OnProgressUpdate(ProgressEventArgs e)
        {
            if (ProgressEvent != null)
                ProgressEvent(this, e);
        }

        public void OnError(string callingFunction, string errorMessage)
        {
            if (ErrorEvent != null)
            {
                ErrorEvent(this, new Pacifica.Core.MessageEventArgs(callingFunction, errorMessage));
            }
        }

        void reader_ErrorEvent(object sender, MessageEventArgs e)
        {
            OnError("MyEMSLReader", e.Message);
        }

        void reader_MessageEvent(object sender, MessageEventArgs e)
        {
            Console.WriteLine("MyEMSLReader: " + e.Message);
        }

        void reader_ProgressEvent(object sender, MyEMSLReader.ProgressEventArgs e)
        {
            // Console.WriteLine("MyEMSLReader Percent complete: " + e.PercentComplete.ToString("0.0") + "%");
        }

        void mFileTools_WaitingForLockQueue(string SourceFilePath, string TargetFilePath, int MBBacklogSource, int MBBacklogTarget)
        {
            Console.WriteLine("mFileTools_WaitingForLockQueue for " + SourceFilePath);
        }


        #endregion

    }

}