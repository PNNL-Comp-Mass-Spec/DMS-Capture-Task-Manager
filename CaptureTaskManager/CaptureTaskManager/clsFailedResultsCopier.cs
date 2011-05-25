using System;
using CaptureTaskManager;

namespace CaptureTaskManager
{
    public class clsFailedResultsCopier
    {
        protected const string FAILED_RESULTS_FOLDER_INFO_TEXT = "FailedResultsFolderInfo_";
        protected const int FAILED_RESULTS_FOLDER_RETAIN_DAYS = 31;

        protected IMgrParams m_mgrParams;
        protected ITaskParams m_taskParams;

        // Constructor
        public clsFailedResultsCopier(IMgrParams mgrParams, ITaskParams taskParams)
        {
            m_mgrParams = mgrParams;
            m_taskParams = taskParams;
        }

        public void CopyFailedResultsToArchiveFolder(string ResultsFolderPath)
        {
            System.IO.DirectoryInfo diSourceFolder;
            System.IO.DirectoryInfo diFailedResultsFolder;
            System.IO.DirectoryInfo diTargetFolder;

            string strFailedResultsFolderPath = string.Empty;
            string strFolderInfoFilePath = string.Empty;

            try
            {
                strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");

                if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                {
                    // Failed results folder path is not defined; don't try to copy the results anywhere
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "FailedResultsFolderPath is not defined for this manager; cannot copy results");
                    return;
                }

                diSourceFolder = new System.IO.DirectoryInfo(ResultsFolderPath);
                diFailedResultsFolder = new System.IO.DirectoryInfo(strFailedResultsFolderPath);
                
                // Make sure the failed results folder exists
                if (!diFailedResultsFolder.Exists)
                    diFailedResultsFolder.Create();

                // Define the target folder name to be Dataset_Job_Step

                string strTargetFolder;

                strTargetFolder = m_taskParams.GetParam("Dataset");
                if (string.IsNullOrEmpty(strTargetFolder))
                    strTargetFolder = "Unknown_Dataset";

                strTargetFolder += "_Job" + m_taskParams.GetParam("Job") + "_Step" + m_taskParams.GetParam("Step");

                strTargetFolder = System.IO.Path.Combine(diFailedResultsFolder.FullName, strTargetFolder);
                diTargetFolder = new System.IO.DirectoryInfo(strTargetFolder);

                // Create an info file that describes the saved results
                try
                {
                    strFolderInfoFilePath = System.IO.Path.Combine(diFailedResultsFolder.FullName, FAILED_RESULTS_FOLDER_INFO_TEXT + diTargetFolder.Name + ".txt");
                    CreateInfoFile(strFolderInfoFilePath, diTargetFolder.Name);
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating the results folder info file '" + strFolderInfoFilePath + "': " + ex.Message);
                }

                // Make sure the source folder exists
                if (!diSourceFolder.Exists)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Source folder not found; cannot copy results: " + ResultsFolderPath);
                }
                else
                {
                    // Look for failed results folders that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                    DeleteOldFailedResultsFolders(diFailedResultsFolder);

                    // Create the target folder
                    if (!diTargetFolder.Exists)
                        diTargetFolder.Create();

                    // Actually copy files from the source folder to the target folder
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying data files to failed results archive: " + ResultsFolderPath);

                    foreach (System.IO.FileInfo fiFileInfo in diSourceFolder.GetFiles())
                    {
                        try
                        {
                            fiFileInfo.CopyTo(System.IO.Path.Combine(diTargetFolder.FullName, fiFileInfo.Name), true);
                        }
                        catch (Exception ex2)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying file from " + ResultsFolderPath + " to " + strFailedResultsFolderPath + ": " + ex2.Message);
                        }
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copy complete");
                }

            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying results from " + ResultsFolderPath + " to " + strFailedResultsFolderPath + ": " + ex.Message);
            }

        }


        private void CreateInfoFile(string strFolderInfoFilePath, string strResultsFolderName)
        {
            System.IO.StreamWriter swArchivedFolderInfoFile;
            swArchivedFolderInfoFile = new System.IO.StreamWriter(new System.IO.FileStream(strFolderInfoFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read));

            {
                swArchivedFolderInfoFile.WriteLine("Date\t" + System.DateTime.Now.ToString());
                swArchivedFolderInfoFile.WriteLine("ResultsFolderName\t" + strResultsFolderName);
                swArchivedFolderInfoFile.WriteLine("Manager\t" + m_mgrParams.GetParam("MgrName"));
                if ((m_taskParams != null))
                {
                    swArchivedFolderInfoFile.WriteLine("Job\t" + m_taskParams.GetParam("Job"));
                    swArchivedFolderInfoFile.WriteLine("Step\t" + m_taskParams.GetParam("Step"));
                    swArchivedFolderInfoFile.WriteLine("StepTool\t" + m_taskParams.GetParam("StepTool"));
                    swArchivedFolderInfoFile.WriteLine("Dataset\t" + m_taskParams.GetParam("Dataset"));
                }
                swArchivedFolderInfoFile.WriteLine("Date\t" + System.DateTime.Now.ToString());
            }

            swArchivedFolderInfoFile.Close();

        }


        private void DeleteOldFailedResultsFolders(System.IO.DirectoryInfo diFailedResultsFolder)
        {
	        System.IO.DirectoryInfo diOldResultsFolder;

	        string strOldResultsFolderName = null;
	        string strTargetFilePath = "";

	        // Determine the folder archive time by reading the modification times on the ResultsFolderInfo_ files
            foreach (System.IO.FileInfo fiFileInfo in diFailedResultsFolder.GetFileSystemInfos(FAILED_RESULTS_FOLDER_INFO_TEXT + "*"))
            {
		        if (System.DateTime.Now.Subtract(fiFileInfo.LastWriteTime).TotalDays > FAILED_RESULTS_FOLDER_RETAIN_DAYS) {
			        // File was modified before the threshold; delete the results folder, then rename this file

			        try {
				        strOldResultsFolderName = System.IO.Path.GetFileNameWithoutExtension(fiFileInfo.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length);
				        diOldResultsFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(fiFileInfo.DirectoryName, strOldResultsFolderName));

				        if (diOldResultsFolder.Exists) {
					        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Deleting old failed results folder: " + diOldResultsFolder.FullName);

					        diOldResultsFolder.Delete(true);
				        }

				        try {
					        strTargetFilePath = System.IO.Path.Combine(fiFileInfo.DirectoryName, "x_" + fiFileInfo.Name);
					        fiFileInfo.CopyTo(strTargetFilePath, true);
					        fiFileInfo.Delete();
				        } catch (Exception ex) {
					        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error renaming failed results info file to " + strTargetFilePath + ": " + ex.Message);
				        }

			        } catch (Exception ex) {
				        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting old failed results folder: " + ex.Message);

			        }

		        }
	        }

        }

    }
}
