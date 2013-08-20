using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Pacifica.Core;

namespace Pacifica.DMS_Metadata
{
	public class DMSMetadataObject
	{
		private List<IFileInfoObject> _bundledFileInfo;
		private Dictionary<string, string> _taskParams;
		private Dictionary<string, string> _mgrParams;
		private Dictionary<string, object> _metadataObject;
		private List<Dictionary<string,object>> _newFilesObject;
		private string _serverSearchString = string.Empty;
		private string _basePath;
		private string _datasetName;
		private string _pathToArchive;
		private ArchiveModes _archiveMode;
		private System.ComponentModel.BackgroundWorker _bgw;
		private string _ingestServerName;
		private string _readServerName;

		/* August 2013: To be deleted
		 * 
		 * private PRISM.DataBase.clsDBTools dbTool;
		*/

		public enum ArchiveModes
		{
			archive, update
		}

		#region "Properties"
		
		public Dictionary<string, object> metadataObject {
			get { return this._metadataObject; } 
			private set {
				this._metadataObject = value;	
			}
		}

		public string bundleName {
			get { return this._metadataObject["bundleName"].ToString(); }
		}

		public long totalFileSizeToUpload {
			get;
			set;
		}

		public int totalFileCountNew {
			get;
			set;
		}

		public int totalFileCountUpdated {
			get;
			set;
		}
		public string serverSearchString {
			get { return this._serverSearchString;  }
			private set { this._serverSearchString = value; }
		}

		public List<Dictionary<string, object>> newFilesObject {
			get { return this._newFilesObject; }
			private set {
				this._newFilesObject = value;
			}
		}

		public string metadataObjectJSON {
			get {
				return Utilities.ObjectToJson(this._metadataObject);
			}
		}
		
		#endregion

		public DMSMetadataObject(System.Collections.Generic.Dictionary<string, string> taskParams, System.Collections.Generic.Dictionary<string, string> mgrParams, System.ComponentModel.BackgroundWorker backgrounder) {
			this._bgw = backgrounder;
			this._ingestServerName = Pacifica.Core.Configuration.ServerHostName;
			this._readServerName = "a3.my.emsl.pnl.gov";

			/* August 2013: To be deleted
			 * 
			 * this.dbTool = new PRISM.DataBase.clsDBTools(null, "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI");
			
			 * No longer necessary since taskParams containst EUS_Instrument_ID
			string instLookupSQL = "SELECT EUS_Instrument_ID FROM [V_EUS_Instrument_ID_Lookup] WHERE Instrument_Name = '" + taskParams["Instrument_Name"] + "';";
			System.Data.DataSet dmsDS = new System.Data.DataSet();
			Int32 rowCount = 0;

			Boolean dbSuccess = this.dbTool.GetDiscDataSet(instLookupSQL, ref dmsDS, ref rowCount);
			*/

			if (backgrounder != null) {
				this._bgw.DoWork += new System.ComponentModel.DoWorkEventHandler(this.SetupMetadataHandler);
				this._bgw.WorkerReportsProgress = true;
				this._bgw.RunWorkerAsync();
			}
			else {
				SetupMetadata(taskParams, mgrParams, null);
			}
			
		}

		private void SetupMetadataHandler(object sender, System.ComponentModel.DoWorkEventArgs e)
		{
			System.ComponentModel.BackgroundWorker worker = (System.ComponentModel.BackgroundWorker)sender;
			this.SetupMetadata(this._taskParams, this._mgrParams, worker);
			e.Result = this._metadataObject;
		}

		private void SetupMetadata(Dictionary<string, string> taskParams, Dictionary<string, string> mgrParams, System.ComponentModel.BackgroundWorker bgw) {
			
			//translate values from task/mgr params into usable variables
			string perspective = mgrParams["perspective"];
			string subFolder = string.Empty;
			string driveLocation;

			// Determine the drive location based on perspective 
			// (client perspective means running on a Proto storage server; server perspective means running on another computer)
			if (perspective == "client")
				driveLocation = taskParams["Storage_Vol_External"];
			else
				driveLocation = taskParams["Storage_Vol"];

			this._pathToArchive = Path.Combine(driveLocation, taskParams["Storage_Path"], taskParams["Folder"]);
			
			if (taskParams["StepTool"].ToLower() == "datasetarchive")
				this._archiveMode = ArchiveModes.archive;
			else
				this._archiveMode = ArchiveModes.update;

			this._datasetName = taskParams["Dataset"];		
			this._basePath = this._pathToArchive;

			if (this._archiveMode == ArchiveModes.update)
			{
				subFolder = taskParams["OutputFolderName"].ToString();
				if (!string.IsNullOrWhiteSpace(subFolder))
				{
					this._pathToArchive = Path.Combine(this._pathToArchive, subFolder);
				}
				else
				{
					subFolder = string.Empty;
				}
			}

			//Calculate the "year_quarter" code used for subfolders within an instrument folder
			DateTime date_code = DateTime.Parse(taskParams["Created"]);
			double yq = (double)date_code.Month / 12.0 * 4.0;
			int yearQuarter = (int)Math.Ceiling(yq);
			string date_code_string = date_code.Year.ToString() + "_" + yearQuarter.ToString();
			bool recurse = true;
			string sValue;

			if (taskParams.TryGetValue(MyEMSLUploader.RECURSIVE_UPLOAD, out sValue))
			{
				bool.TryParse(sValue, out recurse);
			}

			//grab file information from this dataset directory
			this._bundledFileInfo = this.CollectFileInformation(this._pathToArchive, this._archiveMode, this._basePath, recurse, bgw);

			Dictionary<string, object> metadataObject = new Dictionary<string, object>();
			List<Dictionary<string, string>> groupObject = new List<Dictionary<string, string>>();

			//Set up the MyEMSL tagging information
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", taskParams["Instrument_Name"] }, { "type", "omics.dms.instrument" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", date_code_string }, { "type", "omics.dms.date_code" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", taskParams["Dataset"] }, { "type", "omics.dms.dataset" }
			});
			groupObject.Add(new Dictionary<string, string>() {
				{ "name", taskParams["Dataset_ID"] }, { "type", "omics.dms.dataset_id" }
			});

			Dictionary<string, object> eusInfo = new Dictionary<string, object>();

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

			eusInfo.Add("instrumentName", taskParams["Instrument_Name"]);
			eusInfo.Add("proposalID", "17797");
			eusInfo.Add("proposalTitle", "");
			
			metadataObject.Add("bundleName", "omics_dms");
			metadataObject.Add("creationDate", Pacifica.Core.ExtensionMethods.ToUnixTime(DateTime.UtcNow).ToString());
			metadataObject.Add("eusInfo", eusInfo);

			//Start generating the file-level information for inclusion in the metadata file
			List<Dictionary<string,object>> fileListing = new List<Dictionary<string,object>>();
			//Dictionary<string,string> hashListing = new Dictionary<string,string>();

			foreach (IFileInfoObject fio in this._bundledFileInfo) {
				fileListing.Add((Dictionary<string,object>)fio.SerializeToDictionaryObject());
			//  hashListing.Add(fio.RelativeDestinationFullPath, fio.Sha1HashHex);
			}

			System.Text.StringBuilder datasetSearchPathSB = new System.Text.StringBuilder();
			System.Text.StringBuilder searchPathSpecifier = new System.Text.StringBuilder();

			Dictionary<string,string> datasetPathPartList = new Dictionary<string,string>() { 
				{"omics.dms.instrument", taskParams["Instrument_Name"]}, 
				{"omics.dms.date_code", date_code_string}, 
				{"omics.dms.dataset", taskParams["Dataset"] }
			};

			searchPathSpecifier.Append("myemsl/query/");
			datasetSearchPathSB.Append("data/");
			foreach (string pathPartIdentifier in datasetPathPartList.Keys) {
				searchPathSpecifier.Append("group/");
				searchPathSpecifier.Append(pathPartIdentifier + "/");
				searchPathSpecifier.Append("-later-/");

				datasetSearchPathSB.Append(datasetPathPartList[pathPartIdentifier] + "/");
			}

			string datasetSearchPath = searchPathSpecifier.ToString() + datasetSearchPathSB.ToString();

			//returns a dictionary with full relative filepath and sha-1 hex hash
			fileListing = this.CompareDatasetContents(datasetSearchPath, fileListing, subFolder);

			metadataObject.Add("file", fileListing);
			metadataObject.Add("type", "single");
			metadataObject.Add("version", "1.0");

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
		private List<Dictionary<string, object>> CompareDatasetContents(string datasetSearchPath, List<Dictionary<string, object>> fileList, string subFolder)
		{
			this.serverSearchString = Pacifica.Core.Configuration.Scheme + this._readServerName + "/" + datasetSearchPath;
			
			List<Dictionary<string, object>> newFileList;
			if (string.IsNullOrEmpty(subFolder)) {
				newFileList = this.RecurseDirectoryTreeNodes(datasetSearchPath, "");
			}
			else {
				newFileList = this.RecurseDirectoryTreeNodes(datasetSearchPath + subFolder, subFolder);
				this.serverSearchString += subFolder;
			}

			if(newFileList.Count == 0) {
				//no files already exist in MyEMSL, so just upload the lot
				foreach(Dictionary<string, object> localFile in fileList) {
					totalFileSizeToUpload += (long)localFile["sizeInBytes"];
				}
				totalFileCountNew = fileList.Count;
				totalFileCountUpdated = 0;
				return fileList;
			}
			//Must have been something already tagged like this dataset, so find the diffs and report them back
			Dictionary<string, string> hashList;

			hashList = this.RetrieveItemHashSums(newFileList);

			List<Dictionary<string, object>> unmatchedList = new List<Dictionary<string, object>>();
			string itemAddress = string.Empty;
			string itemHashFromServer = string.Empty;
			foreach (Dictionary<string, object> localFile in fileList) {
				if (localFile["destinationDirectory"].ToString() != string.Empty) {
					itemAddress = localFile["destinationDirectory"].ToString() + "/" + localFile["fileName"].ToString();
				}else{
					itemAddress = localFile["fileName"].ToString();
				}
				itemHashFromServer = hashList.ContainsKey(itemAddress) ? hashList[itemAddress].ToString() : string.Empty;

				if (localFile["sha1Hash"].ToString() != itemHashFromServer) {
					unmatchedList.Add(localFile);
					totalFileSizeToUpload += (long)localFile["sizeInBytes"];
					if(string.IsNullOrEmpty(itemHashFromServer)) {
						totalFileCountNew++;
					} else {
						totalFileCountUpdated++;
					}
				}
			}

			return unmatchedList;
		}

		private List<Dictionary<string, object>> RecurseDirectoryTreeNodes(string datasetSearchPath, string parentFolderName) {

			//Path.AltDirectorySeparatorChar
			parentFolderName = parentFolderName == string.Empty ? parentFolderName : parentFolderName + "/";
			string topFolderName = string.Empty;

			List<Dictionary<string, object>> newFileList = new List<Dictionary<string, object>>();

			//get the xml listing of the dataset directory
			string URL = Pacifica.Core.Configuration.Scheme + this._readServerName + "/" + datasetSearchPath;
			//this.serverSearchString = URL;
			string xmlString = string.Empty;
			bool retrievalSuccess = false;
			int retrievalAttempts = 0;
			int maxAttempts = 3;

			while(!retrievalSuccess && retrievalAttempts < maxAttempts) {
				try {
					retrievalAttempts++;
					xmlString = EasyHttp.Send(URL, "", EasyHttp.HttpMethod.Get);
					if(!string.IsNullOrEmpty(xmlString)) {
						retrievalSuccess = true;
					}
				} catch {
					if(retrievalAttempts >= maxAttempts) {
						xmlString = string.Empty;
					} else {
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

			List<Dictionary<string, object>> recursedDirContents;
			foreach (System.Xml.XmlNode entry in dsDocument.FirstChild.ChildNodes)
			{
				topFolderName = parentFolderName + entry.Attributes["name"].Value;
				if (entry.Name == "dir") {
					recursedDirContents = this.RecurseDirectoryTreeNodes(datasetSearchPath.TrimEnd('/') + "/" + entry.Attributes["name"].Value,topFolderName);
					newFileList.AddRange(recursedDirContents);
				}
				else if(entry.Name == "file")
				{
					newFileList.Add(new Dictionary<string,object>() { {"name", topFolderName }, { "item_id", entry.Attributes["itemid"].Value}});
				}
			}


			return newFileList;
		}

		private Dictionary<string, string> RetrieveItemHashSums(List<Dictionary<string, object>> fileList) {
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

			foreach (Dictionary<string, object> item in fileList) {
				retrievalSuccess = false;
				retrievalAttempts = 0;

				startTime = System.DateTime.Now;
				itemName = item["name"].ToString();
				itemID = item["item_id"].ToString();
				uri = uriBase + itemID.ToString() + uriTail;

				while(!retrievalSuccess && retrievalAttempts < maxAttempts) {
					try {
						retrievalAttempts++;
						itemXmlString = EasyHttp.Send(uri, "", EasyHttp.HttpMethod.Get);
						if(!string.IsNullOrEmpty(itemXmlString)) {
							retrievalSuccess = true;
						}
					} catch {
						if(retrievalAttempts >= maxAttempts) {
							itemXmlString = string.Empty;
						} else {
							//wait 5 seconds, then retry
							System.Threading.Thread.Sleep(5000);
							continue;
						}
					}
				}
				
				itemXml = new System.Xml.XmlDocument();
				if(!string.IsNullOrEmpty(itemXmlString)) {
					itemXml.LoadXml(itemXmlString);
				}
				hashNode = itemXml.SelectSingleNode("/myemsl/checksum/sha1");
				if (hashNode != null) {
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

		private List<IFileInfoObject> CollectFileInformation(
			string pathToBeArchived, 
			ArchiveModes archiveOrUpdateMode, 
			string baseDSPath,
			bool recurse,
			System.ComponentModel.BackgroundWorker worker
		)
		{
			List<IFileInfoObject> fileCollection = new List<IFileInfoObject>();

			DirectoryInfo archiveDir = new DirectoryInfo(pathToBeArchived);
			if (!archiveDir.Exists) {
				throw new DirectoryNotFoundException("Source directory not found: " + archiveDir);
			}

			SearchOption eSearchOption;
			if (recurse)
				eSearchOption = SearchOption.AllDirectories;
			else
				eSearchOption = SearchOption.TopDirectoryOnly;

			FileInfo[] fileList = archiveDir.GetFiles("*.*", eSearchOption);
			IFileInfoObject fio;

			double fracCompleted = 0.0;
			int percentCompleted = 0;
			int fileCount = fileList.Length;

			if (worker != null) { worker.ReportProgress(0); }

			//generate file size sum for status purposes
			long totalFileSize = 0;				// how much data is there to crunch?
			long runningFileSize = 0;			// how much data we've crunched so far
			foreach (FileInfo fi in fileList) {
				totalFileSize += fi.Length;
			}

			foreach (FileInfo fi in fileList) {
				runningFileSize += fi.Length;
				fracCompleted = ((double)runningFileSize / (double)totalFileSize);
				percentCompleted = (int)Math.Ceiling(fracCompleted * 100.0);
				if (worker != null)
					worker.ReportProgress(percentCompleted, "Hashing: " + fi.Name);

				fio = new FileInfoObject(fi.FullName, FileInfoObject.GenerateRelativePath(fi.Directory.FullName, baseDSPath));
				fileCollection.Add(fio);
			}

			if (worker != null) 
				worker.ReportProgress(100);

			return fileCollection;
		}
	}
}