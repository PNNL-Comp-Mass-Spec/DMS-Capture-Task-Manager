using System;
using System.Net;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Pacifica.Core;
using MyEMSLReader;
using PRISM;
using Jayrock;
using Jayrock.Json.Conversion;
using ProgressEventArgs = Pacifica.Core.ProgressEventArgs;
using Utilities = Pacifica.Core.Utilities;

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
        /// Error message thrown when the dataset's instrument operator does not have an EUS person ID
        /// </summary>
        public const string UNDEFINED_EUS_OPERATOR_ID = "Operator does not have an EUS person ID in DMS";

        /// <summary>
        /// URL of the EUS website
        /// </summary>
        public const string EUS_PORTAL_URL = "https://eusi.emsl.pnl.gov/Portal/";

        /// <summary>
        /// Object that tracks the upload details, including the files to upload
        /// </summary>
        /// <remarks>
        /// The information in this dictionary is translated to JSON; 
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects 
        /// </remarks>
        private List<Dictionary<string, object>> mMetadataObject;

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

        /// <summary>
        /// EUS Info
        /// </summary>
        public Upload.udtEUSInfo EUSInfo
        {
            get;
            private set;
        }

        public string ManagerName
        {
            get;
            private set;
        }

        public List<Dictionary<string, object>> MetadataObject => mMetadataObject;

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

        public string MetadataObjectJSON => Utilities.ObjectToJson(mMetadataObject);

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DMSMetadataObject()
            : this(string.Empty)
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
            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // This hack was added in March 2014 because Proto-10 reported error
            //   "Could not establish trust relationship for the SSL/TLS secure channel"
            //   when accessing https://my.emsl.pnl.gov/
            // This workaround requires these two using statements:
            //   using System.Net.Security;
            //   using System.Security.Cryptography.X509Certificates;

            // Could use this to ignore all certificates (not wise)
            // System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;

            // Instead, only allow certain domains, as defined by ValidateRemoteCertificate
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;
            this.DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "Unknown_Dataset");

            Upload.udtUploadMetadata uploadMetadata;
            var lstDatasetFilesToArchive = FindDatasetFilesToArchive(taskParams, mgrParams, out uploadMetadata);

            // Calculate the "year_quarter" code used for subfolders within an instrument folder
            // This value is based on the date the dataset was created in DMS
            uploadMetadata.DateCodeString = GetDatasetYearQuarter(taskParams);

            // Find the files that are new or need to be updated
            var lstUnmatchedFiles = CompareDatasetContentsWithMyEMSLMetadata(lstDatasetFilesToArchive, uploadMetadata, debugMode);

            Upload.udtEUSInfo eusInfo;
            mMetadataObject = Upload.CreatePacificaMetadataObject(uploadMetadata, lstUnmatchedFiles, out eusInfo);
            string mdJSON = Utilities.ObjectToJson(mMetadataObject);
            if (!CheckMetadataValidity(mdJSON)){

            }

            var metadataDescription = Upload.GetMetadataObjectDescription(mMetadataObject);
            RaiseDebugEvent("SetupMetadata", metadataDescription);

            EUSInfo = eusInfo;

        }

        private bool CheckMetadataValidity(string mdJSON)
        {
            bool mdIsValid = false;
            string policyURL = Configuration.PolicyServerUri + "/ingest";
            HttpStatusCode responseStatusCode;
            try
            {
                
                string response = EasyHttp.Send(policyURL, null, out responseStatusCode, mdJSON, EasyHttp.HttpMethod.Post, 100, "application/json");
                if (response.Contains("success"))
                {
                    mdIsValid = true;
                }
            }
            catch
            {
                mdIsValid = false;
            }
            return mdIsValid;
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
            var fio = new FileInfoObject(fiRemoteFile.FullName, relativeDestinationDirectory, sha1Hash: string.Empty);
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
        private List<FileInfoObject> CompareDatasetContentsWithMyEMSLMetadata(
            List<FileInfoObject> fileList,
            Upload.udtUploadMetadata uploadMetadata,
            EasyHttp.eDebugMode debugMode)
        {
            TotalFileSizeToUpload = 0;

            var currentTask = "Looking for existing files in MyEMSL for DatasetID " + uploadMetadata.DatasetID;

            if (!string.IsNullOrWhiteSpace(uploadMetadata.SubFolder))
                currentTask += ", subfolder " + uploadMetadata.SubFolder;

            RaiseDebugEvent("CompareDatasetContentsWithMyEMSLMetadata", currentTask);

            int datasetID = uploadMetadata.DatasetID;
            string metadataURL = Configuration.MetadataServerUri + "/fileinfo/files_for_keyvalue/";
            metadataURL += "omics.dms.dataset_id/" + datasetID;

            HttpStatusCode responseStatusCode;

            string fileInfoListJSON = EasyHttp.Send(metadataURL, out responseStatusCode);
            var jsa = (Jayrock.Json.JsonArray)JsonConvert.Import(fileInfoListJSON);
            var fileInfoList = Utilities.JsonArrayToDictionaryList(jsa);
            List<FileInfoObject> returnList = new List<FileInfoObject>();
            Dictionary<string, string> hashList = new Dictionary<string, string>();
            foreach (Dictionary<string, object> fileObj in fileInfoList)
            {
                hashList.Add((string)fileObj["hashsum"], (string)fileObj["subdir"]);
            }

            TotalFileCountNew = 0;
            TotalFileCountUpdated = 0;

            foreach (FileInfoObject fileObj in fileList)
            {
                if (!hashList.Keys.Contains(fileObj.Sha1HashHex) || fileObj.RelativeDestinationDirectory != hashList[fileObj.Sha1HashHex])
                {
                    returnList.Add(fileObj);
                    TotalFileCountNew++;
                    TotalFileSizeToUpload += fileObj.FileSizeInBytes;
                }
                else
                {
                    TotalFileCountUpdated++;
                }
                
            }

            return returnList;

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
                driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol_External", string.Empty);
            else
                driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol", string.Empty);

            // Construct the dataset folder path
            var pathToArchive = Utilities.GetDictionaryValue(taskParams, "Folder", string.Empty);
            pathToArchive = Path.Combine(Utilities.GetDictionaryValue(taskParams, "Storage_Path", string.Empty), pathToArchive);
            pathToArchive = Path.Combine(driveLocation, pathToArchive);

            uploadMetadata.DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", string.Empty);
            uploadMetadata.DMSInstrumentName = Utilities.GetDictionaryValue(taskParams, "Instrument_Name", string.Empty);
            uploadMetadata.DatasetID = Utilities.GetDictionaryValue(taskParams, "Dataset_ID", 0);

            var baseDSPath = pathToArchive;
            uploadMetadata.SubFolder = string.Empty;

            ArchiveModes archiveMode;
            if (Utilities.GetDictionaryValue(taskParams, "StepTool", string.Empty).ToLower() == "datasetarchive")
                archiveMode = ArchiveModes.archive;
            else
                archiveMode = ArchiveModes.update;

            if (archiveMode == ArchiveModes.update)
            {
                uploadMetadata.SubFolder = Utilities.GetDictionaryValue(taskParams, "OutputFolderName", string.Empty);

                if (!string.IsNullOrWhiteSpace(uploadMetadata.SubFolder))
                    pathToArchive = Path.Combine(pathToArchive, uploadMetadata.SubFolder);
                else
                    uploadMetadata.SubFolder = string.Empty;
            }

            uploadMetadata.EUSInstrumentID = Utilities.GetDictionaryValue(taskParams, "EUS_Instrument_ID", string.Empty);
            uploadMetadata.EUSProposalID = Utilities.GetDictionaryValue(taskParams, "EUS_Proposal_ID", string.Empty);

            var operatorUsername = Utilities.GetDictionaryValue(taskParams, "Operator_PRN", "Unknown_Operator");
            uploadMetadata.EUSOperatorID = Utilities.GetDictionaryValue(taskParams, "EUS_Operator_ID", 0);

            if (uploadMetadata.EUSOperatorID == 0)
            {
                var jobNumber = Utilities.GetDictionaryValue(taskParams, "Job", string.Empty);

                var errorMessage =
                    UNDEFINED_EUS_OPERATOR_ID + ". " +
                    operatorUsername + " needs to login at " + EUS_PORTAL_URL + " to be assigned an ID, " +
                    "then DMS needs to update T_EUS_Users (occurs daily via UpdateEUSUsersFromEUSImports), then the job parameters must be updated with: EXEC UpdateParametersForJob " + jobNumber;

                throw new Exception(errorMessage);
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
            var datasetDate = Utilities.GetDictionaryValue(taskParams, "Created", string.Empty);
            var date_code = DateTime.Parse(datasetDate);
            var yq = date_code.Month / 12.0 * 4.0;
            var yearQuarter = (int)Math.Ceiling(yq);
            var datasetDateCodeString = date_code.Year + "_" + yearQuarter;

            return datasetDateCodeString;
        }

        private void ReportProgress(double percentComplete)
        {
            ReportProgress(percentComplete, string.Empty);
        }

        private void ReportProgress(double percentComplete, string currentTask)
        {
            OnProgressUpdate(new ProgressEventArgs(percentComplete, currentTask));
        }


        #region "Event Delegates and Classes"

        public event ProgressEventHandler ProgressEvent;
        public event Pacifica.Core.MessageEventHandler DebugEvent;
        public event Pacifica.Core.MessageEventHandler ErrorEvent;

        public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);

        #endregion

        #region "Event Functions"

        private void OnProgressUpdate(ProgressEventArgs e)
        {
            ProgressEvent?.Invoke(this, e);
        }

        private void OnError(string callingFunction, string errorMessage)
        {
            ErrorEvent?.Invoke(this, new Pacifica.Core.MessageEventArgs(callingFunction, errorMessage));
        }

        private void RaiseDebugEvent(string callingFunction, string currentTask)
        {
            DebugEvent?.Invoke(this, new Pacifica.Core.MessageEventArgs(callingFunction, currentTask));
        }

        void reader_ErrorEvent(string message, Exception ex)
        {
            OnError("MyEMSLReader", message);
        }

        void reader_MessageEvent(string message)
        {
            Console.WriteLine("MyEMSLReader: " + message);
        }

        void reader_ProgressEvent(string progressMessage, float percentComplete)
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