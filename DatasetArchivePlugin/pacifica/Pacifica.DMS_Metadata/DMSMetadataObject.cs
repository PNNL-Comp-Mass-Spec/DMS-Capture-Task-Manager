using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Pacifica.Core;
using MyEMSLReader;

namespace Pacifica.DMS_Metadata
{
	public class DMSMetadataObject
	{
		private List<FileInfoObject> _bundledFileInfo;
		private Dictionary<string, string> _taskParams;
		private Dictionary<string, string> _mgrParams;
		private Dictionary<string, object> _metadataObject;

		// List of new files.
		private List<FileInfoObject> _newFilesObject;

		private string _serverSearchString = string.Empty;
		private System.ComponentModel.BackgroundWorker _bgw;
		private string _ingestServerName;
		private string _myEmslServerName;

		/* August 2013: To be deleted
		 * 
		 * private PRISM.DataBase.clsDBTools dbTool;
		*/

		public enum ArchiveModes
		{
			archive, update
		}

		#region "Properties"

		public Dictionary<string, object> metadataObject
		{
			get { return this._metadataObject; }
			private set
			{
				this._metadataObject = value;
			}
		}

		public string bundleName
		{
			get { return this._metadataObject["bundleName"].ToString(); }
		}

		public long totalFileSizeToUpload
		{
			get;
			set;
		}

		public int totalFileCountNew
		{
			get;
			set;
		}

		public int totalFileCountUpdated
		{
			get;
			set;
		}
		public string serverSearchString
		{
			get { return this._serverSearchString; }
			private set { this._serverSearchString = value; }
		}

		public List<FileInfoObject> newFilesObject
		{
			get { return this._newFilesObject; }
			private set
			{
				this._newFilesObject = value;
			}
		}

		public string metadataObjectJSON
		{
			get
			{
				return Utilities.ObjectToJson(this._metadataObject);
			}
		}

		#endregion

		public DMSMetadataObject(System.Collections.Generic.Dictionary<string, string> taskParams, System.Collections.Generic.Dictionary<string, string> mgrParams, System.ComponentModel.BackgroundWorker backgrounder)
		{
			this._bgw = backgrounder;
			this._ingestServerName = Pacifica.Core.Configuration.ServerHostName;


			this._myEmslServerName = "my.emsl.pnl.gov";

			/* August 2013: To be deleted
			 * 
			 * this.dbTool = new PRISM.DataBase.clsDBTools(null, "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI");
			
			 * No longer necessary since taskParams contains EUS_Instrument_ID
			string instLookupSQL = "SELECT EUS_Instrument_ID FROM [V_EUS_Instrument_ID_Lookup] WHERE Instrument_Name = '" + taskParams["Instrument_Name"] + "';";
			System.Data.DataSet dmsDS = new System.Data.DataSet();
			Int32 rowCount = 0;

			Boolean dbSuccess = this.dbTool.GetDiscDataSet(instLookupSQL, ref dmsDS, ref rowCount);
			*/

			if (backgrounder != null)
			{
				this._bgw.DoWork += new System.ComponentModel.DoWorkEventHandler(this.SetupMetadataHandler);
				this._bgw.WorkerReportsProgress = true;
				this._bgw.RunWorkerAsync();
			}
			else
			{
				SetupMetadata(taskParams, mgrParams, null);
			}

		}

		private void SetupMetadataHandler(object sender, System.ComponentModel.DoWorkEventArgs e)
		{
			System.ComponentModel.BackgroundWorker worker = (System.ComponentModel.BackgroundWorker)sender;
			this.SetupMetadata(this._taskParams, this._mgrParams, worker);
			e.Result = this._metadataObject;
		}

		private void SetupMetadata(Dictionary<string, string> taskParams, Dictionary<string, string> mgrParams, System.ComponentModel.BackgroundWorker bgw)
		{

			//translate values from task/mgr params into usable variables
			string perspective = mgrParams["perspective"];
			string driveLocation;

			// Determine the drive location based on perspective 
			// (client perspective means running on a Proto storage server; server perspective means running on another computer)
			if (perspective == "client")
				driveLocation = taskParams["Storage_Vol_External"];
			else
				driveLocation = taskParams["Storage_Vol"];

			// Construct the dataset folder path
			string pathToArchive = Path.Combine(driveLocation, taskParams["Storage_Path"], taskParams["Folder"]);

			string datasetName = taskParams["Dataset"];
			string datasetInstrument = taskParams["Instrument_Name"];
			int datasetID = Utilities.ToIntSafe(taskParams["Dataset_ID"], 0);
			string baseDSPath = pathToArchive;
			string subFolder = string.Empty;

			ArchiveModes archiveMode;
			if (taskParams["StepTool"].ToLower() == "datasetarchive")
				archiveMode = ArchiveModes.archive;
			else
				archiveMode = ArchiveModes.update;

			if (archiveMode == ArchiveModes.update)
			{
				subFolder = taskParams["OutputFolderName"].ToString();

				if (!string.IsNullOrWhiteSpace(subFolder))
					pathToArchive = Path.Combine(pathToArchive, subFolder);
				else
					subFolder = string.Empty;
			}

			// Calculate the "year_quarter" code used for subfolders within an instrument folder
			// This value is based on the date the dataset was created in DMS
			DateTime date_code = DateTime.Parse(taskParams["Created"]);
			double yq = (double)date_code.Month / 12.0 * 4.0;
			int yearQuarter = (int)Math.Ceiling(yq);
			string datasetDateCodeString = date_code.Year.ToString() + "_" + yearQuarter.ToString();

			bool recurse = true;
			string sValue;

			if (taskParams.TryGetValue(MyEMSLUploader.RECURSIVE_UPLOAD, out sValue))
			{
				bool.TryParse(sValue, out recurse);
			}

			// Grab file information from this dataset directory
			// This process will also compute the Sha-1 hash value for each file
			this._bundledFileInfo = this.CollectFileInformation(pathToArchive, baseDSPath, recurse, bgw);

			// Keys in this object are key names; values are either strings or dictionary objects or even a list of dictionary objects
			var metadataObject = new Dictionary<string, object>();
			var groupObject = new List<Dictionary<string, string>>();

			// Set up the MyEMSL tagging information

			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetInstrument }, { "type", "omics.dms.instrument" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetDateCodeString }, { "type", "omics.dms.date_code" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetName }, { "type", "omics.dms.dataset" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", datasetID.ToString() }, { "type", "omics.dms.dataset_id" }
			});

			var eusInfo = new Dictionary<string, object>();

			eusInfo.Add("groups", groupObject);

			string eusInstrumentID = taskParams["EUS_Instrument_ID"];
			if (string.IsNullOrWhiteSpace(eusInstrumentID))
			{
				// This instrument does not have an EUS_Instrument_ID
				// Use 34127, which is VOrbiETD04
				eusInfo.Add("instrumentId", "34127");
			}
			else
			{
				eusInfo.Add("instrumentId", eusInstrumentID);
			}

			eusInfo.Add("instrumentName", datasetInstrument);

			string eusProposalID = taskParams["EUS_Proposal_ID"];
			if (string.IsNullOrWhiteSpace(eusProposalID))
			{
				// This dataset does not have an EUS_Proposal_ID
				// Use 17797, which is "Development of High Throughput Proteomic Production Operations"
				eusInfo.Add("proposalID", "17797");
			}
			else
			{
				eusInfo.Add("proposalID", eusProposalID);
			}

			metadataObject.Add("bundleName", "omics_dms");
			metadataObject.Add("creationDate", Pacifica.Core.ExtensionMethods.ToUnixTime(DateTime.UtcNow).ToString());
			metadataObject.Add("eusInfo", eusInfo);

			// Start generating the file-level information for inclusion in the metadata file
			var fileListing = new List<FileInfoObject>();

			/*
			 * August 2013: To be deleted
			 * //Dictionary<string,string> hashListing = new Dictionary<string,string>();
			*/

			foreach (FileInfoObject fio in this._bundledFileInfo)
			{
				fileListing.Add(fio);
				/*
				 * August 2013: To be deleted
				 * //  hashListing.Add(fio.RelativeDestinationFullPath, fio.Sha1HashHex);
				*/
			}

			// TODO: Remove the following code once Elastic Search is in use
			//

			System.Text.StringBuilder sbDatasetSearchPath = new System.Text.StringBuilder();
			System.Text.StringBuilder sbSearchPathSpecifier = new System.Text.StringBuilder();

			Dictionary<string, string> datasetPathPartList = new Dictionary<string, string>() { 
				{"omics.dms.instrument", datasetInstrument}, 
				{"omics.dms.date_code", datasetDateCodeString}, 
				{"omics.dms.dataset", datasetName }
			};

			sbSearchPathSpecifier.Append("myemsl/query/");
			sbDatasetSearchPath.Append("data/");
			foreach (string pathPartIdentifier in datasetPathPartList.Keys)
			{
				sbSearchPathSpecifier.Append("group/");
				sbSearchPathSpecifier.Append(pathPartIdentifier + "/");
				sbSearchPathSpecifier.Append("-later-/");

				sbDatasetSearchPath.Append(datasetPathPartList[pathPartIdentifier] + "/");
			}

			string datasetSearchPath = sbSearchPathSpecifier.ToString() + sbDatasetSearchPath.ToString();

			// Returns a list of the files that are new or modified
			// Information for each file is stored using a dictionary with these keys: 
			// "sha1Hash", "destinationDirectory", "localFilePath", "fileName", "sizeInBytes", "creationDate"
			fileListing = this.CompareDatasetContents(datasetSearchPath, fileListing, subFolder);
			
			//
			// TODO: Delete code from here on up (and delete sbDatasetSearchPath and sbSearchPathSpecifier)

			fileListing = this.CompareDatasetContentsElasticSearch(fileListing, subFolder, datasetID, datasetInstrument);

			metadataObject.Add("file", fileListing);

			/* August 2013: To be deleted
			 * 
			 * metadataObject.Add("type", "single");
			*/

			metadataObject.Add("version", "1.0.0");

			this._metadataObject = metadataObject;
			this._newFilesObject = fileListing;

			//string jsonString = Utilities.ObjectToJson(metadataObject);


		}

		/// <summary>
		/// Query server for files and hash codes
		/// </summary>
		/// <param name="datasetSearchPath">Path to query, must end in a forward slash</param>
		/// <param name="fileList">List of local files</param>
		/// <param name="subFolder">Optional subfolder to limit the search to (can be empty)</param>
		/// <returns></returns>
		private List<FileInfoObject> CompareDatasetContents(string datasetSearchPath, List<FileInfoObject> fileList, string subFolder)
		{
			string readServerName = "a3.my.emsl.pnl.gov";
			this.serverSearchString = Pacifica.Core.Configuration.Scheme + readServerName + "/" + datasetSearchPath;

			List<KeyValuePair<string, string>> lstFilesInMyEMSL;
			if (string.IsNullOrEmpty(subFolder))
			{
				lstFilesInMyEMSL = this.RecurseDirectoryTreeNodes(datasetSearchPath, "");
			}
			else
			{
				lstFilesInMyEMSL = this.RecurseDirectoryTreeNodes(datasetSearchPath + subFolder, subFolder);
				this.serverSearchString += subFolder;
			}

			if (lstFilesInMyEMSL.Count == 0)
			{
				// No files already exist in MyEMSL, so just upload all of them
				foreach (var localFile in fileList)
				{
					totalFileSizeToUpload += localFile.FileSizeInBytes;
				}
				totalFileCountNew = fileList.Count;
				totalFileCountUpdated = 0;
				return fileList;
			}

			// Must have been something already tagged like this dataset, so find the diffs and report them back
			// Keys are the file paths and values are the Sha-1 Hashes
			Dictionary<string, string> hashList;

			hashList = this.RetrieveItemHashSums(lstFilesInMyEMSL);

			var unmatchedList = new List<FileInfoObject>();
			
			foreach (var localFile in fileList)
			{
				string itemAddress = string.Empty;
				if (!string.IsNullOrWhiteSpace(localFile.RelativeDestinationDirectory))
				{
					itemAddress = localFile.RelativeDestinationDirectory + "/" + localFile.FileName;
				}
				else
				{
					itemAddress = localFile.FileName;
				}
				string itemHashFromServer = hashList.ContainsKey(itemAddress) ? hashList[itemAddress] : string.Empty;

				if (localFile.Sha1HashHex != itemHashFromServer)
				{
					unmatchedList.Add(localFile);
					totalFileSizeToUpload += localFile.FileSizeInBytes;
					if (string.IsNullOrEmpty(itemHashFromServer))
					{
						totalFileCountNew++;
					}
					else
					{
						totalFileCountUpdated++;
					}
				}
			}

			return unmatchedList;
		}
		
		// August 2013 ToDo: Delete this function once it is no longer needed
		private List<KeyValuePair<string, string>> RecurseDirectoryTreeNodes(string datasetSearchPath, string parentFolderName)
		{

			// Make sure parentFolderName ends in a forward slash
			if (!string.IsNullOrWhiteSpace(parentFolderName) && !parentFolderName.EndsWith("/"))
				parentFolderName += "/";

			string topFolderName = string.Empty;

			// Keys are file paths, values are item_id values
			var lstFilesInMyEMSL = new List<KeyValuePair<string, string>>();

			// get the xml listing of the dataset directory
			// Example URL: http://a3.my.emsl.pnl.gov/DatasetName
			string readServerName = "a3.my.emsl.pnl.gov";
			string URL = Pacifica.Core.Configuration.Scheme + readServerName + "/" + datasetSearchPath;
			//this.serverSearchString = URL;
			string xmlString = string.Empty;
			bool retrievalSuccess = false;
			int retrievalAttempts = 0;
			int maxAttempts = 3;

			while (!retrievalSuccess && retrievalAttempts < maxAttempts)
			{
				try
				{
					retrievalAttempts++;
					System.Net.HttpStatusCode responseStatusCode;
					string postData = "";
					xmlString = EasyHttp.Send(URL, out responseStatusCode, postData, EasyHttp.HttpMethod.Get);
					if (!string.IsNullOrEmpty(xmlString))
					{
						retrievalSuccess = true;
					}
				}
				catch
				{
					if (retrievalAttempts >= maxAttempts)
					{
						xmlString = string.Empty;
					}
					else
					{
						//wait 5 seconds, then retry
						System.Threading.Thread.Sleep(5000);
						continue;
					}
				}
			}

			System.Xml.XmlDocument xmlDoc = new System.Xml.XmlDocument();
			xmlDoc.LoadXml(xmlString);

			System.Xml.XmlDocument dsDocument = new System.Xml.XmlDocument();
			System.Xml.XmlNode dsDirectoryNode = dsDocument.CreateNode("element", "dir", "");
			System.Xml.XmlAttribute nameAttr = dsDocument.CreateAttribute("name");
			nameAttr.Value = Path.GetFileName(datasetSearchPath.TrimEnd('/'));
			dsDirectoryNode.Attributes.Append(nameAttr);
			System.Xml.XmlAttribute typeAttr = dsDocument.CreateAttribute("type");
			typeAttr.Value = "2";
			dsDirectoryNode.Attributes.Append(typeAttr);
			dsDocument.AppendChild(dsDirectoryNode);

			dsDirectoryNode.InnerXml = xmlDoc.ChildNodes[1].InnerXml;

			List<KeyValuePair<string, string>> recursedDirContents;
			foreach (System.Xml.XmlNode entry in dsDocument.FirstChild.ChildNodes)
			{
				topFolderName = parentFolderName + entry.Attributes["name"].Value;
				if (entry.Name == "dir")
				{
					recursedDirContents = this.RecurseDirectoryTreeNodes(datasetSearchPath.TrimEnd('/') + "/" + entry.Attributes["name"].Value, topFolderName);
					lstFilesInMyEMSL.AddRange(recursedDirContents);
				}
				else if (entry.Name == "file")
				{
					lstFilesInMyEMSL.Add(new KeyValuePair<string, string>(topFolderName, entry.Attributes["itemid"].Value));
				}
			}


			return lstFilesInMyEMSL;
		}

		// August 2013 ToDo: Delete this function once it is no longer needed

		/// <summary>
		/// 
		/// </summary>
		/// <param name="fileList">List of MyEMSL items where keys are the file paths and values are the item_ID values</param>
		/// <returns>Dictionary where keys are the file paths and values are the Sha-1 Hashes</returns>
		private Dictionary<string, string> RetrieveItemHashSums(List<KeyValuePair<string, string>> fileList)
		{
			// Keys in this list are file paths, values are the Sha-1 Hash
			Dictionary<string, string> hashList = new Dictionary<string, string>();
			string itemID = string.Empty;
			string itemName = string.Empty;
			string uriBase = Pacifica.Core.Configuration.Scheme + this._ingestServerName + "/myemsl/iteminfo/";
			string uriTail = "/xml";
			string uri = string.Empty;
			string itemXmlString = string.Empty;
			System.Xml.XmlDocument itemXml;
			System.Xml.XmlNode hashNode;
			string hashSum = string.Empty;

			System.DateTime startTime;
			System.DateTime endTime;
			System.TimeSpan elapsed;
			System.DateTime opStart = System.DateTime.Now;

			Boolean retrievalSuccess;
			int retrievalAttempts;
			int maxAttempts = 3;

			foreach (var item in fileList)
			{
				retrievalSuccess = false;
				retrievalAttempts = 0;

				startTime = System.DateTime.Now;
				itemName = item.Key;
				itemID = item.Value;
				uri = uriBase + itemID + uriTail;

				while (!retrievalSuccess && retrievalAttempts < maxAttempts)
				{
					try
					{
						retrievalAttempts++;
						System.Net.HttpStatusCode responseStatusCode;
						string postData = "";
						itemXmlString = EasyHttp.Send(uri, out responseStatusCode, postData, EasyHttp.HttpMethod.Get);
						if (!string.IsNullOrEmpty(itemXmlString))
						{
							retrievalSuccess = true;
						}
					}
					catch
					{
						if (retrievalAttempts >= maxAttempts)
						{
							itemXmlString = string.Empty;
						}
						else
						{
							//wait 5 seconds, then retry
							System.Threading.Thread.Sleep(5000);
							continue;
						}
					}
				}

				itemXml = new System.Xml.XmlDocument();
				if (!string.IsNullOrEmpty(itemXmlString))
				{
					itemXml.LoadXml(itemXmlString);
				}
				hashNode = itemXml.SelectSingleNode("/myemsl/checksum/sha1");
				if (hashNode != null)
				{
					hashSum = hashNode.FirstChild.Value.ToString();
					hashList.Add(itemName, hashSum);
				}
				endTime = System.DateTime.Now;
				elapsed = endTime.Subtract(startTime);
				Console.WriteLine("item:" + itemName + " (" + itemID + ") => " + elapsed.Milliseconds + "ms");
			}
			Console.WriteLine("item:" + itemName + " (" + itemID + ") => " + System.DateTime.Now.Subtract(opStart).Seconds + "sec");
			return hashList;
		}

		/// <summary>
		/// Find all of the files in the path to be archived
		/// </summary>
		/// <param name="pathToBeArchived">Folder path to be archived</param>
		/// <param name="baseDSPath">Base dataset folder path</param>
		/// <param name="recurse">True to recurse</param>
		/// <param name="worker">Background worker for reporting progress (can be null)</param>
		/// <returns></returns>
		private List<FileInfoObject> CollectFileInformation(
			string pathToBeArchived,
			string baseDSPath,
			bool recurse,
			System.ComponentModel.BackgroundWorker worker
		)
		{
			var fileCollection = new List<FileInfoObject>();

			DirectoryInfo archiveDir = new DirectoryInfo(pathToBeArchived);
			if (!archiveDir.Exists)
			{
				throw new DirectoryNotFoundException("Source directory not found: " + archiveDir);
			}

			SearchOption eSearchOption;
			if (recurse)
				eSearchOption = SearchOption.AllDirectories;
			else
				eSearchOption = SearchOption.TopDirectoryOnly;

			List<FileInfo> fileList = archiveDir.GetFiles("*.*", eSearchOption).ToList<FileInfo>();
			FileInfoObject fio;

			double fracCompleted = 0.0;
			int percentCompleted = 0;
			int fileCount = fileList.Count;

			if (worker != null)
			{
				worker.ReportProgress(0);
			}

			// Generate file size sum for status purposes
			long totalFileSize = 0;				// how much data is there to crunch?
			long runningFileSize = 0;			// how much data we've crunched so far
			foreach (FileInfo fi in fileList)
			{
				totalFileSize += fi.Length;
			}

			foreach (FileInfo fi in fileList)
			{
				runningFileSize += fi.Length;
				fracCompleted = ((double)runningFileSize / (double)totalFileSize);
				percentCompleted = (int)Math.Ceiling(fracCompleted * 100.0);
				if (worker != null)
					worker.ReportProgress(percentCompleted, "Hashing files: " + fi.Name);

				fio = new FileInfoObject(fi.FullName, baseDSPath);
				fileCollection.Add(fio);
			}

			if (worker != null)
				worker.ReportProgress(100);

			return fileCollection;
		}


		/// <summary>
		/// Query server for files and hash codes
		/// </summary>
		/// <param name="fileList">List of local files</param>
		/// <param name="subFolder">Optional subfolder to limit the search to (can be empty)</param>
		/// <returns></returns>
		private List<FileInfoObject> CompareDatasetContentsElasticSearch(
			List<FileInfoObject> fileList,
			string subFolder,
			int datasetID,
			string datasetInstrument)
		{

			// Find all files in MyEMSL for this dataset

			var reader = new MyEMSLReader.Reader();
			List<ArchivedFileInfo> lstFilesInMyEMSL;

			lstFilesInMyEMSL = reader.FindFilesByDatasetID(datasetID, subFolder);

			// Keys in this dictionary are relative file paths
			// Values are the sha-1 hash values for the file
			Dictionary<string, string> hashList = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

			var unmatchedList = new List<FileInfoObject>();


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
				string itemHashFromServer = hashList.ContainsKey(itemAddress) ? hashList[itemAddress] : string.Empty;

				if (localFile.Sha1HashHex != itemHashFromServer)
				{
					unmatchedList.Add(localFile);
					totalFileSizeToUpload += localFile.FileSizeInBytes;
					if (string.IsNullOrEmpty(itemHashFromServer))
					{
						totalFileCountNew++;
					}
					else
					{
						totalFileCountUpdated++;
					}
				}
			}


			return unmatchedList;
		}

	}
}