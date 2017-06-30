using System;
using System.Net;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Pacifica.Core;
using PRISM;
using Jayrock.Json.Conversion;
using ProgressEventArgs = Pacifica.Core.ProgressEventArgs;
using Utilities = Pacifica.Core.Utilities;
using System.Data.SqlClient;

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
        public Upload.EUSInfo EUSInfo
        {
            get;
            private set;
        }

        public string ManagerName
        {
            get;
        }

        public List<Dictionary<string, object>> MetadataObject => mMetadataObject;

        /// <summary>
        /// Number of bytes to upload
        /// </summary>
        public long TotalFileSizeToUpload
        {
            get;
            set;
        }

        /// <summary>
        /// Number of new files pushed to MyEMSL
        /// </summary>
        public int TotalFileCountNew
        {
            get;
            set;
        }

        /// <summary>
        /// Number of files updated in MyEMSL
        /// </summary>
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

            ManagerName = managerName;
            mFileTools = new clsFileTools(managerName, 1);

            mFileTools.WaitingForLockQueue += mFileTools_WaitingForLockQueue;
        }

        /// <summary>
        /// Construct the metadata that will be included with the ingested data
        /// </summary>
        /// <param name="taskParams"></param>
        /// <param name="mgrParams"></param>
        /// <param name="debugMode"></param>
        /// <param name="criticalError"></param>
        /// <returns>True if success, otherwise false</returns>
        public bool SetupMetadata(
            Dictionary<string, string> taskParams,
            Dictionary<string, string> mgrParams,
            EasyHttp.eDebugMode debugMode,
            out bool criticalError)
        {

            // Could use this to ignore all certificates (not wise)
            // System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;

            // Instead, only allow certain domains, as defined by ValidateRemoteCertificate
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

            DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "Unknown_Dataset");

            var lstDatasetFilesToArchive = FindDatasetFilesToArchive(taskParams, mgrParams, out Upload.UploadMetadata uploadMetadata);

            // DMS5 database
            mgrParams.TryGetValue("DefaultDMSConnString", out string connectionString);

            // DMS_Capture database
            mgrParams.TryGetValue("ConnectionString", out string captureDbConnectionString);

            taskParams.TryGetValue("Dataset_ID", out string datasetID);

            var supplementalDataSuccess = GetSupplementalDMSMetadata(connectionString, datasetID, uploadMetadata);

            // Calculate the "year_quarter" code used for subfolders within an instrument folder
            // This value is based on the date the dataset was created in DMS
            uploadMetadata.DateCodeString = GetDatasetYearQuarter(taskParams);

            // Find the files that are new or need to be updated
            var lstUnmatchedFiles = CompareDatasetContentsWithMyEMSLMetadata(
                captureDbConnectionString,
                lstDatasetFilesToArchive,
                uploadMetadata,
                out criticalError);

            if (criticalError)
                return false;

            mMetadataObject = Upload.CreatePacificaMetadataObject(uploadMetadata, lstUnmatchedFiles, out Upload.EUSInfo eusInfo);

            if (lstUnmatchedFiles.Count > 0)
            {
                var mdJSON = Utilities.ObjectToJson(mMetadataObject);
                if (!CheckMetadataValidity(mdJSON, out string validityMessage))
                {
                    OnError("CheckMetadataValidity", validityMessage);
                    return false;
                }
            }

            var metadataDescription = Upload.GetMetadataObjectDescription(mMetadataObject);
            RaiseDebugEvent("SetupMetadata", metadataDescription);

            EUSInfo = eusInfo;
            return true;

        }

        private bool GetSupplementalDMSMetadata(
            string dmsConnectionString,
            string datasetID,
            Upload.UploadMetadata uploadMetadata,
            int retryCount = 3)
        {

            var queryString = "SELECT * FROM V_MyEMSL_Supplemental_Metadata WHERE [omics.dms.dataset_id] = " + datasetID;

            while (retryCount >= 0)
            {
                try
                {

                    using (var connection = new SqlConnection(dmsConnectionString))
                    {
                        var command = new SqlCommand(queryString, connection);
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {

                            if (reader.HasRows && reader.Read())
                            {
                                uploadMetadata.CampaignID = GetDbValue(reader, "omics.dms.campaign_id", 0);
                                uploadMetadata.CampaignName = GetDbValue(reader, "omics.dms.campaign_name", string.Empty);
                                uploadMetadata.ExperimentID = GetDbValue(reader, "omics.dms.experiment_id", 0);
                                uploadMetadata.ExperimentName = GetDbValue(reader, "omics.dms.experiment_name", string.Empty);
                                uploadMetadata.OrganismName = GetDbValue(reader, "organism_name", string.Empty);
                                uploadMetadata.NCBITaxonomyID = GetDbValue(reader, "ncbi_taxonomy_id", 0);
                                uploadMetadata.OrganismID = GetDbValue(reader, "omics.dms.organism_id", 0);
                                uploadMetadata.AcquisitionTime = GetDbValue(reader, "omics.dms.acquisition_time", string.Empty);
                                uploadMetadata.AcquisitionLengthMin = GetDbValue(reader, "omics.dms.acquisition_length_min", 0);
                                uploadMetadata.NumberOfScans = GetDbValue(reader, "omics.dms.number_of_scans", 0);
                                uploadMetadata.SeparationType = GetDbValue(reader, "omics.dms.separation_type", string.Empty);
                                uploadMetadata.DatasetType = GetDbValue(reader, "omics.dms.dataset_type", string.Empty);
                                uploadMetadata.RequestedRunID = GetDbValue(reader, "omics.dms.requested_run_id", 0);
                            }

                        }

                        uploadMetadata.UserOfRecordList = GetRequestedRunUsers(connection, uploadMetadata.RequestedRunID);

                    }

                    return true;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var msg = string.Format("Exception retrieving supplemental DMS metadata for Dataset ID {0}: {1}; " +
                                            "ConnectionString: {2}, RetryCount = {3}",
                                            datasetID, ex.Message, dmsConnectionString, retryCount);

                    OnError("GetSupplementalDMSMetadata", msg);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

            return false;
        }

        private List<int> GetRequestedRunUsers(SqlConnection connection, int requestedRunID, int retryCount = 3)
        {
            var queryString = "SELECT EUS_Person_ID FROM V_Requested_Run_EUS_Users_Export WHERE Request_ID = " + requestedRunID;

            while (retryCount >= 0)
            {
                try
                {

                    var command = new SqlCommand(queryString, connection);
                    using (var reader = command.ExecuteReader())
                    {
                        if (!reader.HasRows)
                            return new List<int>();

                        var personList = new List<int>();

                        while (reader.Read())
                        {
                            var personId = GetDbValue(reader, "EUS_Person_ID", 0, out bool isNull);
                            if (!isNull)
                                personList.Add(personId);
                        }

                        return personList;
                    }

                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var msg = string.Format("Exception retrieving requested run users for Requested Run ID {0}: {1}; " +
                                            "ConnectionString: {2}, RetryCount = {3}",
                                            requestedRunID, ex.Message, connection.ConnectionString, retryCount);

                    OnError("GetRequestedRunUsers", msg);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

            return new List<int>();
        }

        private bool CheckMetadataValidity(string mdJSON, out string validityMessage)
        {
            var mdIsValid = false;
            var policyURL = Configuration.PolicyServerUri + "/ingest";
            validityMessage = string.Empty;

            try
            {
                var response = EasyHttp.Send(policyURL, null, out HttpStatusCode responseStatusCode, mdJSON, EasyHttp.HttpMethod.Post, 100, "application/json");
                if (response.Contains("success"))
                {
                    validityMessage = response;
                    mdIsValid = true;
                }
            }
            catch (Exception ex)
            {
                validityMessage = ex.Message;
                mdIsValid = false;
            }

            return mdIsValid;
        }

        private bool AddUsingCacheInfoFile(
            FileInfo fiCacheInfoFile,
            ICollection<FileInfoObject> fileCollection,
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
            long totalFileSize = 0;             // how much data is there to crunch?
            long runningFileSize = 0;           // how much data we've crunched so far
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

                    var success = AddUsingCacheInfoFile(fiFile, fileCollection, baseDSPath, out string remoteFilePath);
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
        /// <param name="captureDbConnectionString">DMS_Capture connection string</param>
        /// <param name="candidateFilesToUpload">List of local files</param>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="criticalError">Output: set to true if the job should be failed</param>
        /// <returns>List of files that need to be uploaded</returns>
        private List<FileInfoObject> CompareDatasetContentsWithMyEMSLMetadata(
            string captureDbConnectionString,
            IEnumerable<FileInfoObject> candidateFilesToUpload,
            Upload.UploadMetadata uploadMetadata,
            out bool criticalError)
        {
            TotalFileCountNew = 0;
            TotalFileCountUpdated = 0;
            TotalFileSizeToUpload = 0;

            var currentTask = "Looking for existing files in MyEMSL for DatasetID " + uploadMetadata.DatasetID;

            if (!string.IsNullOrWhiteSpace(uploadMetadata.SubFolder))
                currentTask += ", subfolder " + uploadMetadata.SubFolder;

            RaiseDebugEvent("CompareDatasetContentsWithMyEMSLMetadata", currentTask);

            var datasetID = uploadMetadata.DatasetID;

            var remoteFiles = GetDatasetFilesInMyEMSL(datasetID);

            // Make sure that the number of files reported by MyEMSL for this dataset agrees with what we expect
            var expectedRemoteFileCount = GetDatasetFileCountExpectedInMyEMSL(captureDbConnectionString, datasetID);

            if (expectedRemoteFileCount < 0)
            {
                OnError("CompareDatasetContentsWithMyEMSLMetadata", "Aborting upload since GetDatasetFileCountExpectedInMyEMSL returned -1");
                criticalError = true;
                return new List<FileInfoObject>();
            }

            if (expectedRemoteFileCount > 0 && remoteFiles.Count < expectedRemoteFileCount * 0.95)
            {
                OnError("CompareDatasetContentsWithMyEMSLMetadata",
                    string.Format("MyEMSL reported {0} files for Dataset ID {1}; it should be tracking at least {2} files",
                    remoteFiles.Count, datasetID, expectedRemoteFileCount));

                criticalError = true;
                return new List<FileInfoObject>();
            }

            // Compare the files in remoteFileInfoList to those in candidateFilesToUpload
            // Note that two files in the same directory could have the same hash value, so we cannot simply compare file hashes

            var missingFiles = new List<FileInfoObject>();

            foreach (var fileObj in candidateFilesToUpload)
            {
                var relativeFilePath = Path.Combine(fileObj.RelativeDestinationDirectory, fileObj.FileName);

                if (remoteFiles.TryGetValue(relativeFilePath, out var fileVersions))
                {
                    if (FileHashExists(fileVersions, fileObj.Sha1HashHex))
                    {
                        // File found
                        continue;
                    }

                    TotalFileCountUpdated++;
                }
                else
                {
                    TotalFileCountNew++;
                }

                missingFiles.Add(fileObj);

                TotalFileSizeToUpload += fileObj.FileSizeInBytes;
            }

            criticalError = false;
            return missingFiles;

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

                var strTargetFilePath = Path.Combine(@"\\MyEMSL\", DatasetName, fiSource.Name);

                var strLockFilePathSource = mFileTools.CreateLockFile(diLockFolderSource, lockFileTimestamp, fiSource, strTargetFilePath, ManagerName);

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

        /// <summary>
        /// Return true if fileVersions has a file with the given hash
        /// </summary>
        /// <param name="fileVersions">List of files in MyEMSL</param>
        /// <param name="fileHash">Sha-1 hash to find</param>
        /// <returns>True if a match is found, otherwise false</returns>
        private bool FileHashExists(IEnumerable<MyEMSLFileInfo> fileVersions, string fileHash)
        {
            return (from item in fileVersions where string.Equals(item.HashSum, fileHash) select item).Any();
        }

        public List<FileInfoObject> FindDatasetFilesToArchive(
            Dictionary<string, string> taskParams,
            Dictionary<string, string> mgrParams,
            out Upload.UploadMetadata uploadMetadata)
        {

            uploadMetadata = new Upload.UploadMetadata();
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

            if (taskParams.TryGetValue(MyEMSLUploader.RECURSIVE_UPLOAD, out string sValue))
            {
                bool.TryParse(sValue, out recurse);
            }

            // Grab file information from this dataset directory
            // This process will also compute the Sha-1 hash value for each file
            var lstDatasetFilesToArchive = CollectFileInformation(pathToArchive, baseDSPath, recurse);

            return lstDatasetFilesToArchive;
        }

        /// <summary>
        /// Query the DMS_Capture database to determine the number of files that MyEMSL should be tracking for this dataset
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="datasetID"></param>
        /// <param name="retryCount">Number of times to try again if the data cannot be retrieved</param>
        /// <returns>Number of files that should be in MyEMSL for this dataset; -1 if an error</returns>
        private int GetDatasetFileCountExpectedInMyEMSL(string connectionString, int datasetID, int retryCount = 3)
        {
            var queryString = string.Format(
                "SELECT SUM(FileCountNew) AS Files " +
                "FROM V_MyEMSL_Uploads " +
                "WHERE Dataset_ID = {0} AND (Verified > 0 OR Ingest_Steps_Completed >= 7)",
                datasetID);

            while (retryCount >= 0)
            {
                try
                {

                    using (var connection = new SqlConnection(connectionString))
                    {
                        var command = new SqlCommand(queryString, connection);
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                var filesForDatasetInMyEMSL = GetDbValue(reader, "Files", 0);
                                return filesForDatasetInMyEMSL;
                            }

                        }
                    }

                    return 0;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var msg = string.Format("Exception looking up expected file count in MyEMSL for Dataset ID {0}: {1}; " +
                                            "ConnectionString: {2}, RetryCount = {3}",
                                            datasetID, ex.Message, connectionString, retryCount);

                    OnError("GetDatasetFileCountExpectedInMyEMSL", msg);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

            return -1;
        }

        /// <summary>
        /// Find files in MyEMSL associated with the given dataset ID
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        /// <param name="subDir">Optional subdiretory (subfolder) to filter on</param>
        /// <returns>List of files</returns>
        public Dictionary<string, List<MyEMSLFileInfo>> GetDatasetFilesInMyEMSL(int datasetID, string subDir = "")
        {
            // Example URL:
            // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/265031
            var metadataURL = Configuration.MetadataServerUri + "/fileinfo/files_for_keyvalue/omics.dms.dataset_id/" + datasetID;

            // Retrieve a list of files already in MyEMSL for this dataset
            var fileInfoListJSON = EasyHttp.Send(metadataURL, out HttpStatusCode responseStatusCode);

            // Convert the response to a dictionary
            var jsa = (Jayrock.Json.JsonArray)JsonConvert.Import(fileInfoListJSON);
            var remoteFileInfoList = Utilities.JsonArrayToDictionaryList(jsa);

            // Keys in this dictionary are relative file paths; values are file info details
            // A given remote file could have multiple hash values if multiple versions of the file have been uploaded
            var remoteFiles = new Dictionary<string, List<MyEMSLFileInfo>>();

            // Note that two files in the same directory could have the same hash value (but different names),
            // so we cannot simply compare file hashes

            foreach (var fileObj in remoteFileInfoList)
            {
                var fileName = Utilities.GetDictionaryValue(fileObj, "name");
                var fileId = Utilities.GetDictionaryValue(fileObj, "_id", 0);
                var fileHash = Utilities.GetDictionaryValue(fileObj, "hashsum");
                var subFolder = Utilities.GetDictionaryValue(fileObj, "subdir");

                if (!string.IsNullOrWhiteSpace(subDir))
                {
                    if (!string.Equals(subDir, subFolder, StringComparison.OrdinalIgnoreCase))
                        continue;
                }

                var relativeFilePath = Path.Combine(subFolder, fileName);

                if (remoteFiles.TryGetValue(relativeFilePath, out var fileVersions))
                {
                    if (FileHashExists(fileVersions, fileHash))
                    {
                        OnError("CompareDatasetContentsWithMyEMSLMetadata",
                                "Remote file listing reports the same file with the same hash more than once; ignoring: " + relativeFilePath +
                                " with hash " + fileHash);
                        continue;
                    }

                    // Add the file to fileVersions
                }
                else
                {
                    fileVersions = new List<MyEMSLFileInfo>();
                    remoteFiles.Add(relativeFilePath, fileVersions);
                }

                var remoteFileInfo = new MyEMSLFileInfo(fileName, fileId, fileHash)
                {
                    HashType = Utilities.GetDictionaryValue(fileObj, "hashtype"),
                    SubDir = subFolder,
                    Size = Utilities.GetDictionaryValue(fileObj, "size", 0),
                    TransactionId = Utilities.GetDictionaryValue(fileObj, "transaction_id", 0)
                };

                var createdInMyEMSL = Utilities.GetDictionaryValue(fileObj, "created");
                var updatedInMyEMSL = Utilities.GetDictionaryValue(fileObj, "updated");
                var deletedInMyEMSL = Utilities.GetDictionaryValue(fileObj, "deleted");

                remoteFileInfo.UpdateRemoteFileTimes(createdInMyEMSL, updatedInMyEMSL, deletedInMyEMSL);

                var creationTime = Utilities.GetDictionaryValue(fileObj, "ctime");
                var lastWriteTime = Utilities.GetDictionaryValue(fileObj, "mtime");

                remoteFileInfo.UpdateSourceFileTimes(creationTime, lastWriteTime);

                fileVersions.Add(remoteFileInfo);

            }

            return remoteFiles;
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

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">Integer to return if null</param>
        /// <returns>Integer</returns>
        private static int GetDbValue(IDataRecord reader, string fieldName, int valueIfNull)
        {
            return GetDbValue(reader, fieldName, valueIfNull, out _);
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">Integer to return if null</param>
        /// <param name="isNull">True if the value is null</param>
        /// <returns>Integer</returns>
        private static int GetDbValue(IDataRecord reader, string fieldName, int valueIfNull, out bool isNull)
        {
            if (Convert.IsDBNull(reader[fieldName]))
            {
                isNull = true;
                return valueIfNull;
            }

            isNull = false;
            return (int)reader[fieldName];
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">String to return if null</param>
        /// <returns>String</returns>
        private static string GetDbValue(IDataRecord reader, string fieldName, string valueIfNull)
        {
            return GetDbValue(reader, fieldName, valueIfNull, out _);
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">String to return if null</param>
        /// <param name="isNull">True if the value is null</param>
        /// <returns>String</returns>
        private static string GetDbValue(IDataRecord reader, string fieldName, string valueIfNull, out bool isNull)
        {
            if (Convert.IsDBNull(reader[fieldName]))
            {
                isNull = true;
                return valueIfNull;
            }

            isNull = false;

            // Use .ToString() and not a string cast to allow for DateTime fields to convert to strings
            return reader[fieldName].ToString();
        }


        #region "Event Delegates and Classes"

        public event ProgressEventHandler ProgressEvent;
        public event MessageEventHandler DebugEvent;
        public event MessageEventHandler ErrorEvent;

        public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);

        #endregion

        #region "Event Functions"

        private void OnProgressUpdate(ProgressEventArgs e)
        {
            ProgressEvent?.Invoke(this, e);
        }

        private void OnError(string callingFunction, string errorMessage)
        {
            ErrorEvent?.Invoke(this, new MessageEventArgs(callingFunction, errorMessage));
        }

        private void RaiseDebugEvent(string callingFunction, string currentTask)
        {
            DebugEvent?.Invoke(this, new MessageEventArgs(callingFunction, currentTask));
        }

        void mFileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int MBBacklogSource, int MBBacklogTarget)
        {
            Console.WriteLine("mFileTools_WaitingForLockQueue for " + sourceFilePath);
        }


        #endregion

    }

}
