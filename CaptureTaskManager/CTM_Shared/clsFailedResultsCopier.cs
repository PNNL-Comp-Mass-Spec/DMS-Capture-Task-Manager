using System;
using System.IO;

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
            var strFailedResultsFolderPath = string.Empty;
            var strFolderInfoFilePath = string.Empty;

            try
            {
                strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");

                if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                {
                    // Failed results folder path is not defined; don't try to copy the results anywhere
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                         "FailedResultsFolderPath is not defined for this manager; cannot copy results");
                    return;
                }

                var diSourceFolder = new DirectoryInfo(ResultsFolderPath);
                var diFailedResultsFolder = new DirectoryInfo(strFailedResultsFolderPath);

                // Make sure the failed results folder exists
                if (!diFailedResultsFolder.Exists)
                    diFailedResultsFolder.Create();

                // Define the target folder name to be Dataset_Job_Step

                var strTargetFolder = m_taskParams.GetParam("Dataset");
                if (string.IsNullOrEmpty(strTargetFolder))
                    strTargetFolder = "Unknown_Dataset";

                strTargetFolder += "_Job" + m_taskParams.GetParam("Job") + "_Step" + m_taskParams.GetParam("Step");

                strTargetFolder = Path.Combine(diFailedResultsFolder.FullName, strTargetFolder);
                var diTargetFolder = new DirectoryInfo(strTargetFolder);

                // Create an info file that describes the saved results
                try
                {
                    strFolderInfoFilePath = Path.Combine(diFailedResultsFolder.FullName,
                                                         FAILED_RESULTS_FOLDER_INFO_TEXT + diTargetFolder.Name + ".txt");
                    CreateInfoFile(strFolderInfoFilePath, diTargetFolder.Name);
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                         "Error creating the results folder info file '" + strFolderInfoFilePath + "': " +
                                         ex.Message);
                }

                // Make sure the source folder exists
                if (!diSourceFolder.Exists)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                         "Source folder not found; cannot copy results: " + ResultsFolderPath);
                }
                else
                {
                    // Look for failed results folders that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                    DeleteOldFailedResultsFolders(diFailedResultsFolder);

                    // Create the target folder
                    if (!diTargetFolder.Exists)
                        diTargetFolder.Create();

                    // Actually copy files from the source folder to the target folder
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                         "Copying data files to failed results archive: " + ResultsFolderPath);

                    foreach (var fiFileInfo in diSourceFolder.GetFiles())
                    {
                        try
                        {
                            fiFileInfo.CopyTo(Path.Combine(diTargetFolder.FullName, fiFileInfo.Name), true);
                        }
                        catch (Exception ex2)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                                 "Error copying file from " + ResultsFolderPath + " to " +
                                                 strFailedResultsFolderPath + ": " + ex2.Message);
                        }
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copy complete");
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     "Error copying results from " + ResultsFolderPath + " to " +
                                     strFailedResultsFolderPath + ": " + ex.Message);
            }
        }


        private void CreateInfoFile(string strFolderInfoFilePath, string strResultsFolderName)
        {
		    var swArchivedFolderInfoFile = new StreamWriter(new FileStream(strFolderInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

            {
                swArchivedFolderInfoFile.WriteLine("Date\t" + DateTime.Now);
                swArchivedFolderInfoFile.WriteLine("ResultsFolderName\t" + strResultsFolderName);
                swArchivedFolderInfoFile.WriteLine("Manager\t" + m_mgrParams.GetParam("MgrName"));
                if ((m_taskParams != null))
                {
                    swArchivedFolderInfoFile.WriteLine("Job\t" + m_taskParams.GetParam("Job"));
                    swArchivedFolderInfoFile.WriteLine("Step\t" + m_taskParams.GetParam("Step"));
                    swArchivedFolderInfoFile.WriteLine("StepTool\t" + m_taskParams.GetParam("StepTool"));
                    swArchivedFolderInfoFile.WriteLine("Dataset\t" + m_taskParams.GetParam("Dataset"));
                }
                swArchivedFolderInfoFile.WriteLine("Date\t" + DateTime.Now);
            }

            swArchivedFolderInfoFile.Close();
        }


        private void DeleteOldFailedResultsFolders(DirectoryInfo diFailedResultsFolder)
        {
            var strTargetFilePath = "";

            // Determine the folder archive time by reading the modification times on the ResultsFolderInfo_ files
			foreach (var fileSystemInfo in diFailedResultsFolder.GetFileSystemInfos(FAILED_RESULTS_FOLDER_INFO_TEXT + "*"))
            {
                var fiFileInfo = (FileInfo)fileSystemInfo;
			    if (!(DateTime.UtcNow.Subtract(fiFileInfo.LastWriteTimeUtc).TotalDays > FAILED_RESULTS_FOLDER_RETAIN_DAYS))
                {
                    continue;
                }

                // File was modified before the threshold; delete the results folder, then rename this file

                try
                {
			        var strOldResultsFolderName = Path.GetFileNameWithoutExtension(fiFileInfo.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length);
			        var diOldResultsFolder = new DirectoryInfo(Path.Combine(fiFileInfo.DirectoryName, strOldResultsFolderName));

                    if (diOldResultsFolder.Exists)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                             "Deleting old failed results folder: " + diOldResultsFolder.FullName);

                        diOldResultsFolder.Delete(true);
                    }

                    try
                    {
                        strTargetFilePath = Path.Combine(fiFileInfo.DirectoryName, "x_" + fiFileInfo.Name);
                        fiFileInfo.CopyTo(strTargetFilePath, true);
                        fiFileInfo.Delete();
                    }
                    catch (Exception ex)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                             "Error renaming failed results info file to " + strTargetFilePath + ": " +
                                             ex.Message);
                    }
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                         "Error deleting old failed results folder: " + ex.Message);
                }
            }
        }
    }
}